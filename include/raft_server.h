#pragma once

#include <brpc/controller.h>             // brpc::Controller
#include <brpc/server.h>                 // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include <rocksdb/db.h>
#include <future>
#include <shared_mutex>
#include <chrono>
#include <regex>
#include <algorithm>
#include <set>

#include "http_data.h"
#include "threadpool.h"
#include "http_server.h"
#include "batched_indexer.h"
#include "raft_dns_cache.h"
#include "cached_resource_stat.h"
#include "string_utils.h"

class Store;
class ReplicationState;

/**
 * Represents a parsed node configuration with separate collections for hostnames and IPs.
 * This provides clear separation between hostname-based and IP-based peer configurations.
 * Includes versioning for safe configuration changes (see TLA+ specs).
 */
struct NodeConfiguration {
    std::vector<std::string> hostname_nodes;  // e.g., "node1.example.com:8107:8108"
    std::vector<std::string> ip_nodes;        // e.g., "192.168.1.1:8107:8108"
    
    // Configuration versioning for safe changes (TLA+ pattern)
    uint64_t config_version = 1;              // Incremented on each config change
    uint64_t config_term = 0;                 // Term when this config was created
    std::chrono::steady_clock::time_point created_at = std::chrono::steady_clock::now();
    
    bool has_hostnames() const { return !hostname_nodes.empty(); }
    bool has_ips() const { return !ip_nodes.empty(); }
    bool empty() const { return hostname_nodes.empty() && ip_nodes.empty(); }
    size_t total_nodes() const { return hostname_nodes.size() + ip_nodes.size(); }
    
    /**
     * Get all nodes as a unified set for intersection calculations.
     * Used by validate_new_config_quorum() for joint consensus.
     */
    std::set<std::string> get_node_set() const {
        std::set<std::string> node_set;
        for (const auto& node : hostname_nodes) {
            node_set.insert(node);
        }
        for (const auto& node : ip_nodes) {
            node_set.insert(node);
        }
        return node_set;
    }
    
    /**
     * Check if this configuration is newer than another (TLA+ pattern).
     * Compares by (config_term, config_version) tuple with uninitialized term handling.
     * 
     * TLA+ Reference: TypesenseRaft.tla -> IsNewerConfig()
     */
    bool is_newer_than(const NodeConfiguration& other) const {
        const int64_t uninitialized_term = -1; // Equivalent to TLA+ Nil
        
        // TLA+ Pattern: If either term is uninitialized (Nil), ignore terms and compare versions only
        // This allows force reconfigs to override other configs using high version numbers
        // TLA+: newTerm = Nil \/ oldTerm = Nil => newVersion > oldVersion
        if (config_term == uninitialized_term || other.config_term == uninitialized_term) {
            return config_version > other.config_version;
        }
        
        // Standard TLA+ ordering: term first, then version
        // TLA+: newTerm > oldTerm \/ (newTerm = oldTerm /\ newVersion > oldVersion)
        return config_term > other.config_term || 
               (config_term == other.config_term && config_version > other.config_version);
    }
    
    /**
     * Create a new configuration version for a single-node change.
     * This implements a safe single-node membership change pattern.
     */
    NodeConfiguration create_single_node_change(const std::string& node_to_add, 
                                               const std::string& node_to_remove,
                                               uint64_t current_term) const {
        NodeConfiguration new_config = *this;
        new_config.config_version++;
        new_config.config_term = current_term;
        new_config.created_at = std::chrono::steady_clock::now();
        
        // Remove node if specified
        if (!node_to_remove.empty()) {
            auto& hostname_nodes = new_config.hostname_nodes;
            auto& ip_nodes = new_config.ip_nodes;
            
            hostname_nodes.erase(
                std::remove(hostname_nodes.begin(), hostname_nodes.end(), node_to_remove),
                hostname_nodes.end());
            ip_nodes.erase(
                std::remove(ip_nodes.begin(), ip_nodes.end(), node_to_remove),
                ip_nodes.end());
        }
        
        // Add node if specified
        if (!node_to_add.empty()) {
            // Determine if it's a hostname or IP and add to appropriate collection
            if (is_hostname_node(node_to_add)) {
                new_config.hostname_nodes.push_back(node_to_add);
            } else {
                new_config.ip_nodes.push_back(node_to_add);
            }
        }
        
        return new_config;
    }
    
    /**
     * Validate that a configuration change is safe (single node only).
     * Implements a single-node change safety rule using set symmetric difference.
     * 
     * TLA+ Reference: TypesenseRaft.tla -> ValidateConfigChange()
     */
    bool validate_config_change(const NodeConfiguration& new_config) const {
        // TLA+ Pattern: Use set symmetric difference to validate single-node changes
        // This ensures that exactly one voting member is added or removed
        // TLA+: Cardinality(added) + Cardinality(removed) = 1
        
        // Create sets of all nodes (both hostname and IP nodes are voting members)
        std::set<std::string> old_nodes_set, new_nodes_set;
        
        // Add all current nodes to old set
        for (const auto& node : hostname_nodes) {
            old_nodes_set.insert(node);
        }
        for (const auto& node : ip_nodes) {
            old_nodes_set.insert(node);
        }
        
        // Add all new nodes to new set  
        for (const auto& node : new_config.hostname_nodes) {
            new_nodes_set.insert(node);
        }
        for (const auto& node : new_config.ip_nodes) {
            new_nodes_set.insert(node);
        }
        
        // Calculate symmetric difference
        // The symmetric difference is the set of elements that are in either set but not in both
        std::vector<std::string> symmetric_diff;
        symmetric_diff.reserve(old_nodes_set.size() + new_nodes_set.size());
        
        std::set_symmetric_difference(
            old_nodes_set.begin(), old_nodes_set.end(),
            new_nodes_set.begin(), new_nodes_set.end(),
            std::back_inserter(symmetric_diff)
        );
        
        // TLA+ Rule: Single-node change means symmetric difference size must be exactly 1
        // This implements: Cardinality(added) + Cardinality(removed) = 1
        if (symmetric_diff.size() != 1) {
            return false;
        }
        
        // Additional safety: Ensure we don't go below minimum viable cluster size
        size_t new_total_nodes = new_config.total_nodes();
        if (new_total_nodes < 1) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Serialize back to the original format for persistence and logging.
     * Includes version metadata in comments for debugging.
     */
    std::string serialize() const {
        std::vector<std::string> all_nodes;
        all_nodes.insert(all_nodes.end(), hostname_nodes.begin(), hostname_nodes.end());
        all_nodes.insert(all_nodes.end(), ip_nodes.begin(), ip_nodes.end());
        return StringUtils::join(all_nodes, ",");
    }
    
    /**
     * Serialize with version metadata for debugging and monitoring.
     */
    std::string serialize_with_metadata() const {
        return serialize() + " # version=" + std::to_string(config_version) + 
               " term=" + std::to_string(config_term);
    }

private:
    /**
     * Helper to determine if a node string represents a hostname or IP.
     */
    bool is_hostname_node(const std::string& node_str) const {
        if (node_str.find('[') == 0) return false;  // IPv6
        
        std::vector<std::string> parts;
        StringUtils::split(node_str, parts, ":");
        if (parts.size() != 3) return false;
        
        const std::string& host = parts[0];
        // Check if it's an IPv4 address
        return !std::regex_match(host, std::regex("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$"));
    }
};

// Implements the callback for the state machine
class ReplicationClosure : public braft::Closure {
private:
    const std::shared_ptr<http_req> request;
    const std::shared_ptr<http_res> response;

public:
    ReplicationClosure(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response): request(request), response(response) {

    }

    ~ReplicationClosure() {
        //LOG(INFO) << "~ReplicationClosure req use count " << request.use_count();
    }

    const std::shared_ptr<http_req>& get_request() const {
        return request;
    }

    const std::shared_ptr<http_res>& get_response() const {
        return response;
    }

    void Run();
};

// Closure that fires when refresh nodes operation finishes
class RefreshNodesClosure : public braft::Closure {
public:

    RefreshNodesClosure() {}

    ~RefreshNodesClosure() {}

    void Run() {
        // Auto delete this after Run()
        std::unique_ptr<RefreshNodesClosure> self_guard(this);

        if(status().ok()) {
            LOG(INFO) << "Peer refresh succeeded!";
        } else {
            LOG(ERROR) << "Peer refresh failed, error: " << status().error_str();
        }
    }
};

// Closure that fires when requested
class OnDemandSnapshotClosure : public braft::Closure {
private:
    ReplicationState* replication_state;
    const std::shared_ptr<http_req> req;
    const std::shared_ptr<http_res> res;
    const std::string ext_snapshot_path;
    const std::string state_dir_path;

public:

    OnDemandSnapshotClosure(ReplicationState *replication_state, const std::shared_ptr<http_req>& req,
                            const std::shared_ptr<http_res>& res, const std::string& ext_snapshot_path,
                            const std::string& state_dir_path) :
        replication_state(replication_state), req(req), res(res), ext_snapshot_path(ext_snapshot_path),
        state_dir_path(state_dir_path) {}

    ~OnDemandSnapshotClosure() {}

    void Run();
};

class TimedSnapshotClosure : public braft::Closure {
private:
    ReplicationState* replication_state;

public:

    TimedSnapshotClosure(ReplicationState *replication_state) : replication_state(replication_state){}

    ~TimedSnapshotClosure() {}

    void Run();
};

// Implements braft::StateMachine.
class ReplicationState : public braft::StateMachine {
private:
    static constexpr const char* db_snapshot_name = "db_snapshot";
    static constexpr const char* analytics_db_snapshot_name = "analytics_db_snapshot";
    static constexpr const char* BATCHED_INDEXER_STATE_KEY = "$BI";

    mutable std::shared_mutex node_mutex;

    braft::Node* volatile node;
    butil::atomic<int64_t> leader_term;

    HttpServer* server;
    BatchedIndexer* batched_indexer;

    Store* store;
    Store* analytics_store;

    ThreadPool* thread_pool;
    http_message_dispatcher* message_dispatcher;

    const bool api_uses_ssl;

    const Config* config;

    const size_t num_collections_parallel_load;
    const size_t num_documents_parallel_load;

    std::atomic<bool> read_caught_up;
    std::atomic<bool> write_caught_up;

    std::string raft_dir_path;

    std::string ext_snapshot_path;

    int election_timeout_interval_ms;

    std::mutex mcv;
    std::condition_variable cv;
    bool ready;

    std::atomic<bool> shutting_down;
    std::atomic<size_t> pending_writes;

    std::atomic<size_t> snapshot_in_progress;

    const uint64_t snapshot_interval_s;     // frequency of actual snapshotting
    uint64_t last_snapshot_ts;              // when last snapshot ran

    butil::EndPoint peering_endpoint;

    // DNS failure handling and immediate re-resolution
    mutable std::shared_mutex current_config_mutex;
    NodeConfiguration current_node_config;
    std::string current_nodes_config_str;
    std::atomic<bool> immediate_refresh_requested;
    
    // DNS cache for hostname resolution
    std::unique_ptr<RaftDNSCache> dns_cache_;
    
    // TLA+ ConfigIsSafe state tracking
    mutable std::shared_mutex safety_state_mutex;
    std::atomic<uint64_t> last_term_quorum_check;
    std::atomic<uint64_t> last_config_quorum_check;
    std::chrono::steady_clock::time_point last_safety_validation;

public:

    static constexpr const char* log_dir_name = "log";
    static constexpr const char* meta_dir_name = "meta";
    static constexpr const char* snapshot_dir_name = "snapshot";

    ReplicationState(HttpServer* server, BatchedIndexer* batched_indexer, Store* store, Store* analytics_store,
                     ThreadPool* thread_pool, http_message_dispatcher* message_dispatcher,
                     bool api_uses_ssl, const Config* config,
                     size_t num_collections_parallel_load, size_t num_documents_parallel_load);

    // Starts this node
    int start(const butil::EndPoint & peering_endpoint, int api_port,
              int election_timeout_ms, int snapshot_max_byte_count_per_rpc,
              const std::string & raft_dir, const std::string & nodes,
              const std::atomic<bool>& quit_abruptly);

    // Generic write method for synchronizing all writes
    void write(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response);

    // Generic read method for consistent reads, not used for now
    void read(const std::shared_ptr<http_res>& response);

    // updates cluster membership
    void refresh_nodes(const std::string & nodes, const size_t raft_counter,
                       const std::atomic<bool>& reset_peers_on_error);

    void refresh_catchup_status(bool log_msg);

    bool trigger_vote();

    bool reset_peers();

    bool has_leader_term() const {
        return leader_term.load(butil::memory_order_acquire) > 0;
    }

    bool is_read_caught_up() const {
        return read_caught_up;
    }

    bool is_write_caught_up() const {
        return write_caught_up;
    }

    bool is_alive() const;

    uint64_t node_state() const;

    // Shut this node down.
    void shutdown();

    int init_db();

    Store* get_store();

    // for manual / external snapshots
    void do_snapshot(const std::string& snapshot_path, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);

    static std::string to_nodes_config(const butil::EndPoint &peering_endpoint, const int api_port,
                                       const std::string &nodes_config);

    void set_ext_snapshot_path(const std::string &snapshot_path);

    void set_snapshot_in_progress(const bool snapshot_in_progress);

    // for timed snapshots
    void do_snapshot(const std::string& nodes);

    void persist_applying_index();

    http_message_dispatcher* get_message_dispatcher() const;

    void wait() {
        auto lk = std::unique_lock<std::mutex>(mcv);
        cv.wait(lk, [&] { return ready; });
        ready = false;
    }

    void notify() {
        std::lock_guard<std::mutex> lk(mcv);
        ready = true;
        cv.notify_all();
    }

    /**
     * Resolves a hostname to an IP string.
     *
     * @param hostname The hostname to resolve.
     * @return A string representation of the resolved IP address.
     *         For IPv6 addresses, the IP will be enclosed in square brackets.
     *         Returns empty string if resolution fails or hostname is invalid.
     */
    static std::string hostname2ipstr(const std::string& hostname);

    /**
     * Parse node configuration string into separate hostname and IP collections.
     * This replaces resolve_node_hosts() with a cleaner separation of concerns.
     */
    static NodeConfiguration parse_node_configuration(const std::string& nodes_config);

    /**
     * Convert NodeConfiguration to braft::Configuration with DNS resolution.
     * This resolves hostnames to IPs and creates braft peer objects.
     */
    static braft::Configuration node_config_to_braft(const NodeConfiguration& node_config);

    /**
     * Handle peer connection failure with immediate DNS re-resolution for hostname peers.
     * This dramatically reduces disaster recovery time by not waiting for the 10s refresh cycle.
     */
    void handle_peer_failure(const braft::PeerId& failed_peer_id);

    /**
     * Extract hostname from a hostname-based node string (e.g., "node1.example.com:8107:8108" -> "node1.example.com")
     */
    static std::string extract_hostname_from_node(const std::string& node_str);

    /**
     * Check if a braft::PeerId corresponds to a hostname-based node by comparing resolved IPs
     */
    bool peer_matches_hostname_node(const braft::PeerId& peer_id, const std::string& hostname_node);

    /**
     * Trigger immediate cluster configuration refresh (bypasses the 10s timer)
     */
    void trigger_immediate_config_refresh();

    /**
     * Safely add a single node to the cluster (TLA+ pattern).
     * This prevents dangerous multi-node changes that could split quorums.
     */
    bool add_node_safe(const std::string& node_to_add);

    /**
     * Safely remove a single node from the cluster (TLA+ pattern).
     * This prevents dangerous multi-node changes that could split quorums.
     */
    bool remove_node_safe(const std::string& node_to_remove);

    /**
     * Check if the current configuration is safe for reconfiguration.
     * Basic safety checks: leader status, stable term, committed entries.
     */
    bool is_config_safe_for_reconfig() const;

    /**
     * TLA+ ConfigIsSafe - comprehensive safety validation.
     * Combines three critical safety checks: term quorum, config quorum, and committed operations.
     * This is the master safety check from the TLA+ specifications.
     * 
     * TLA+ Reference: TypesenseSafetyProperties.tla -> ConfigIsSafe()
     */
    bool config_is_safe() const;

    /**
     * TLA+ HasValidTermQuorum - ensures leader authority in current term.
     * Verifies that the current leader has established authority with a quorum
     * in the current term before allowing configuration changes.
     * 
     * TLA+ Reference: TypesenseSafetyProperties.tla -> HasValidTermQuorum()
     */
    bool has_valid_term_quorum() const;

    /**
     * TLA+ HasValidConfigQuorum - ensures current config is acknowledged by quorum.
     * Validates that the current configuration has been properly committed
     * and acknowledged by a majority of nodes before allowing changes.
     * 
     * TLA+ Reference: TypesenseSafetyProperties.tla -> HasValidConfigQuorum()
     */
    bool has_valid_config_quorum() const;

    /**
     * TLA+ ArePreviousOpsCommitted - prevents data loss during config changes.
     * Ensures that operations committed in previous configurations remain committed
     * in the current configuration, preventing data loss during reconfiguration.
     * 
     * TLA+ Reference: TypesenseSafetyProperties.tla -> ArePreviousOpsCommitted()
     */
    bool are_previous_ops_committed() const;

    /**
     * TLA+ HasQuorumOverlap - validate joint consensus safety for configuration changes.
     * Implements intersection-based safety check to prevent split-brain
     * scenarios during configuration transitions. The intersection of old and new
     * node sets must be able to satisfy quorum requirements for both configurations.
     * 
     * Examples:
     * ✅ Safe: [A,B,C] → [A,B,D] (intersection [A,B] = 2 ≥ quorum 2)
     * ❌ Unsafe: [A,B,C] → [D,E,F] (intersection [] = 0 < quorum 2) - Split-brain risk!
     * ❌ Unsafe: [A,B,C] → [A,D,E] (intersection [A] = 1 < quorum 2) - Insufficient overlap!
     * 
     * TLA+ Reference: TypesenseRaft.tla -> HasQuorumOverlap()
     */
    bool has_quorum_overlap(const NodeConfiguration& new_config) const;

    /**
     * Get the current Raft term for configuration versioning.
     */
    uint64_t get_current_term() const;

    int64_t get_num_queued_writes();

    bool is_leader();

    nlohmann::json get_status();

    std::string get_leader_url() const;

    static Option<bool> handle_gzip(const std::shared_ptr<http_req>& request);

    void decr_pending_writes();

private:

    friend class ReplicationClosure;

    // actual application of writes onto the WAL
    void on_apply(braft::Iterator& iter);

    struct SnapshotArg {
        ReplicationState* replication_state;
        braft::SnapshotWriter* writer;
        std::string state_dir_path;
        std::string db_snapshot_path;
        std::string analytics_db_snapshot_path;
        std::string ext_snapshot_path;
        braft::Closure* done;
    };

    static void *save_snapshot(void* arg);

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);

    int on_snapshot_load(braft::SnapshotReader* reader);

    void on_leader_start(int64_t term) {
        leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader, term: " << term;
    }

    void on_leader_stop(const butil::Status& status) {
        leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() {
        LOG(INFO) << "This node is down";
    }

    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Met peering error " << e;
        
        // Check if this is a peer connection failure that might benefit from DNS re-resolution
        if (e.type() == ::braft::ERROR_TYPE_LOG_REPLICATION || 
            e.type() == ::braft::ERROR_TYPE_INSTALL_SNAPSHOT) {
            // Extract peer information from error if possible
            // Note: braft::Error doesn't always provide peer info directly,
            // but we can still trigger a general refresh check
            LOG(INFO) << "Peer communication error detected, checking for hostname-based peers";
            trigger_immediate_config_refresh();
        }
    }

    void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }

    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        refresh_catchup_status(true);
        LOG(INFO) << "Node starts following " << ctx;
    }

    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }

    void write_to_leader(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response);

    void do_dummy_write();

    std::string get_node_url_path(const braft::PeerId& peer_id, const std::string& path,
                                  const std::string& protocol) const;

    // Safety Validator Methods (TLA+ patterns)
    void handle_peer_failure(const braft::PeerId& failed_peer_id);
    void trigger_immediate_config_refresh();
    bool add_node_safe(const std::string& node_to_add);
    bool remove_node_safe(const std::string& node_to_remove);
    bool is_config_safe_for_reconfig() const;
    bool config_is_safe() const;
    bool has_valid_term_quorum() const;
    bool has_valid_config_quorum() const;
    bool are_previous_ops_committed() const;
    bool has_quorum_overlap(const NodeConfiguration& new_config) const;
    
    // Helper methods
    bool is_self_node(const std::string& node_spec) const;

    // Config Manager Methods (DNS resolution and node parsing)
    static NodeConfiguration parse_node_configuration(const std::string& nodes_config);
    std::string hostname2ipstr(const std::string& hostname);
    static braft::Configuration node_config_to_braft(const NodeConfiguration& config);
    static std::string extract_hostname_from_node(const std::string& node_spec);
    bool peer_matches_hostname_node(const braft::PeerId& peer_id, const std::string& hostname_node);
    std::string to_nodes_config(const butil::EndPoint& peering_endpoint, const int api_port, const std::string& nodes);
    
    // DNS cache management for production use
    void clear_dns_cache();
    void clear_dns_cache_for_hostname(const std::string& hostname);
    size_t get_dns_cache_size() const;
    
    // Access to DNS cache for advanced operations
    RaftDNSCache& get_dns_cache() { return *dns_cache_; }
    const RaftDNSCache& get_dns_cache() const { return *dns_cache_; }
};
