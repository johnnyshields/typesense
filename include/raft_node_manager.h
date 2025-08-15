#pragma once

#include <string>
#include <memory>
#include <atomic>
#include <shared_mutex>
#include <nlohmann/json.hpp>
#include <braft/raft.h>
#include "http_data.h"

class ReplicationState;
class Config;
class Store;
class BatchedIndexer;
class HttpServer;
class ThreadPool;
class http_message_dispatcher;

/**
 * RaftNodeManager handles Raft node operations, status monitoring,
 * and snapshot management for the cluster.
 */
class RaftNodeManager {
private:
    ReplicationState* replication_state;
    const Config* config;
    Store* store;
    BatchedIndexer* batched_indexer;
    ThreadPool* thread_pool;
    http_message_dispatcher* message_dispatcher;
    bool api_uses_ssl;
    std::string raft_dir_path;
    butil::EndPoint peering_endpoint;
    int election_timeout_interval_ms;

    // Node synchronization
    braft::Node* volatile* node_ptr;
    mutable std::shared_mutex* node_mutex_ptr;

    // State management
    std::atomic<bool>* read_caught_up_ptr;
    std::atomic<bool>* write_caught_up_ptr;
    std::atomic<size_t>* pending_writes_ptr;
    std::atomic<bool>* snapshot_in_progress_ptr;

    // Snapshot timing
    uint64_t* last_snapshot_ts_ptr;
    const uint64_t snapshot_interval_s;

    // External snapshot path
    std::string* ext_snapshot_path_ptr;

public:
    /**
     * Constructor for RaftNodeManager
     */
    RaftNodeManager(ReplicationState* state, 
                   const Config* config, 
                   Store* store,
                   BatchedIndexer* batched_indexer, 
                   ThreadPool* thread_pool,
                   http_message_dispatcher* dispatcher, 
                   bool api_uses_ssl,
                   const std::string& raft_dir_path,
                   const butil::EndPoint& peering_endpoint,
                   int election_timeout_interval_ms,
                   uint64_t snapshot_interval_s,
                   std::shared_mutex* node_mutex,
                   braft::Node* volatile* node,
                   std::atomic<bool>* read_caught_up,
                   std::atomic<bool>* write_caught_up,
                   std::atomic<size_t>* pending_writes,
                   std::atomic<bool>* snapshot_in_progress,
                   uint64_t* last_snapshot_ts,
                   std::string* ext_snapshot_path);

    /**
     * Refresh cluster node configuration
     */
    void refresh_nodes(const std::string& nodes, 
                      size_t raft_counter,
                      const std::atomic<bool>& reset_peers_on_error);

    /**
     * Update catchup status for reads and writes
     */
    void refresh_catchup_status(bool log_msg);

    /**
     * Check if node is alive and caught up for reads
     */
    bool is_alive() const;

    /**
     * Get current node state
     */
    uint64_t node_state() const;

    /**
     * Trigger a leader election vote
     */
    bool trigger_vote();

    /**
     * Reset peer configuration (unsafe operation)
     */
    bool reset_peers();

    /**
     * Get number of queued write operations
     */
    int64_t get_num_queued_writes();

    /**
     * Check if this node is the leader
     */
    bool is_leader();

    /**
     * Get node status as JSON
     */
    nlohmann::json get_status();

    /**
     * Get leader's URL
     */
    std::string get_leader_url() const;

    /**
     * Persist the current applying index
     */
    void persist_applying_index();

    /**
     * Decrement pending writes counter
     */
    void decr_pending_writes();

    /**
     * Trigger on-demand snapshot with optional external path
     */
    void do_snapshot(const std::string& snapshot_path, 
                    const std::shared_ptr<http_req>& req,
                    const std::shared_ptr<http_res>& res);

    /**
     * Trigger timed snapshot if conditions are met
     */
    void do_snapshot(const std::string& nodes);

    /**
     * Perform a dummy write operation
     */
    void do_dummy_write();

    /**
     * Set external snapshot path
     */
    void set_ext_snapshot_path(const std::string& snapshot_path);

    /**
     * Set snapshot in progress flag
     */
    void set_snapshot_in_progress(bool snapshot_in_progress);

private:
    /**
     * Get URL path for a given peer
     */
    std::string get_node_url_path(const braft::PeerId& peer_id, 
                                  const std::string& path,
                                  const std::string& protocol) const;
};
