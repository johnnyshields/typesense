#include "raft_server.h"
#include "store.h"
#include "raft_config_manager.h"
#include "raft_http_handler.h"
#include "raft_node_manager.h"
#include <butil/files/file_enumerator.h>
#include <thread>
#include <algorithm>
#include <string_utils.h>
#include <file_utils.h>
#include <collection_manager.h>
#include <http_client.h>
#include <conversation_model_manager.h>
#include "rocksdb/utilities/checkpoint.h"
#include "thread_local_vars.h"
#include "core_api.h"
#include "personalization_model_manager.h"
#include <logger.h>

// Raft Server - Slim Coordinator
// This file now coordinates the extracted modules:
// - RaftConfigManager: DNS & Configuration (class)
// - RaftHttpHandler: HTTP Processing (class)
// - raft_lifecycle_manager.cpp: Raft Lifecycle & Snapshots (implementation file)
// - RaftNodeManager: Node Management & Status (class)

namespace braft {
    DECLARE_int32(raft_do_snapshot_min_index_gap);
    DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
    DECLARE_bool(raft_enable_append_entries_cache);
    DECLARE_int32(raft_max_append_entries_cache_size);
    DECLARE_int32(raft_max_byte_count_per_rpc);
    DECLARE_int32(raft_rpc_channel_connect_timeout_ms);
}

void ReplicationClosure::Run() {
    // Auto delete `this` after Run() - handled by coordination layer
    std::unique_ptr<ReplicationClosure> self_guard(this);
}

// Constructor - Initialize the coordination layer with manager classes
ReplicationState::ReplicationState(HttpServer* server, BatchedIndexer* batched_indexer,
                                 Store *store, Store* analytics_store, ThreadPool* thread_pool,
                                 http_message_dispatcher *message_dispatcher,
                                 bool api_uses_ssl, const Config* config,
                                 size_t num_collections_parallel_load, size_t num_documents_parallel_load):
        node(nullptr), leader_term(-1), server(server), batched_indexer(batched_indexer),
        store(store), analytics_store(analytics_store),
        thread_pool(thread_pool), message_dispatcher(message_dispatcher), api_uses_ssl(api_uses_ssl),
        config(config),
        num_collections_parallel_load(num_collections_parallel_load),
        num_documents_parallel_load(num_documents_parallel_load),
        read_caught_up(false), write_caught_up(false),
        ready(false), shutting_down(false), pending_writes(0), snapshot_in_progress(false),
        last_snapshot_ts(std::time(nullptr)), snapshot_interval_s(config->get_snapshot_interval_seconds()) {
    
    // Note: Manager classes will be initialized after raft_dir_path is set in start()
    LOG(INFO) << "ReplicationState coordinator initialized";
}

// Core coordination method - delegates to lifecycle manager
int ReplicationState::start(const butil::EndPoint & peering_endpoint, const int api_port,
                            int election_timeout_ms, int snapshot_max_byte_count_per_rpc,
                            const std::string & raft_dir, const std::string & nodes,
                            const std::atomic<bool>& quit_abruptly) {

    LOG(INFO) << "Starting Raft coordination layer";
    
    // Set coordinator state
    this->election_timeout_interval_ms = election_timeout_ms;
    this->raft_dir_path = raft_dir;
    this->peering_endpoint = peering_endpoint;
    this->read_caught_up = false;
    this->write_caught_up = false;

    // Initialize manager classes now that we have all required parameters
    http_handler = std::make_unique<RaftHttpHandler>(
        this, server, store, batched_indexer, thread_pool, message_dispatcher,
        config, api_uses_ssl, raft_dir_path,
        &node_mutex, &node, &shutting_down, &pending_writes, &leader_term
    );

    node_manager = std::make_unique<RaftNodeManager>(
        this, config, store, batched_indexer, thread_pool, message_dispatcher,
        api_uses_ssl, raft_dir_path, peering_endpoint, election_timeout_interval_ms,
        snapshot_interval_s, &node_mutex, &node,
        &read_caught_up, &write_caught_up, &pending_writes, &snapshot_in_progress,
        &last_snapshot_ts, &ext_snapshot_path
    );

    // Configure braft flags
    braft::FLAGS_raft_do_snapshot_min_index_gap = 1;
    braft::FLAGS_raft_max_parallel_append_entries_rpc_num = 1;
    braft::FLAGS_raft_enable_append_entries_cache = false;
    braft::FLAGS_raft_max_append_entries_cache_size = 8;
    braft::FLAGS_raft_max_byte_count_per_rpc = snapshot_max_byte_count_per_rpc;
    braft::FLAGS_raft_rpc_channel_connect_timeout_ms = 2000;

    // Delegate actual raft node startup to lifecycle manager
    int result = start_raft_node(peering_endpoint, api_port, election_timeout_ms, 
                                snapshot_max_byte_count_per_rpc, raft_dir, nodes, quit_abruptly);
    
    if (result == 0) {
        LOG(INFO) << "Raft coordination layer start completed";
    }
    
    return result;
}

// Delegation methods to manager classes

void ReplicationState::write(const std::shared_ptr<http_req>& request, 
                            const std::shared_ptr<http_res>& response) {
    http_handler->write(request, response);
}

void ReplicationState::write_to_leader(const std::shared_ptr<http_req>& request,
                                      const std::shared_ptr<http_res>& response) {
    http_handler->write_to_leader(request, response);
}

void ReplicationState::read(const std::shared_ptr<http_res>& response) {
    http_handler->read(response);
}

std::string ReplicationState::get_node_url_path(const braft::PeerId& peer_id, 
                                               const std::string& path,
                                               const std::string& protocol) const {
    return http_handler->get_node_url_path(peer_id, path, protocol);
}

// Static delegation to RaftConfigManager
std::string ReplicationState::to_nodes_config(const butil::EndPoint& peering_endpoint,
                                             const int api_port,
                                             const std::string& nodes_config) {
    return RaftConfigManager::to_nodes_config(peering_endpoint, api_port, nodes_config);
}

std::string ReplicationState::hostname2ipstr(const std::string& hostname) {
    return RaftConfigManager::hostname2ipstr(hostname);
}

std::string ReplicationState::resolve_node_hosts(const std::string& nodes_config) {
    return RaftConfigManager::resolve_node_hosts(nodes_config);
}

// Static delegation to RaftHttpHandler
Option<bool> ReplicationState::handle_gzip(const std::shared_ptr<http_req>& request) {
    return RaftHttpHandler::handle_gzip(request);
}

// Delegation to RaftNodeManager
void ReplicationState::refresh_nodes(const std::string& nodes, const size_t raft_counter,
                                    const std::atomic<bool>& reset_peers_on_error) {
    node_manager->refresh_nodes(nodes, raft_counter, reset_peers_on_error);
}

void ReplicationState::refresh_catchup_status(bool log_msg) {
    node_manager->refresh_catchup_status(log_msg);
}

bool ReplicationState::trigger_vote() {
    return node_manager->trigger_vote();
}

bool ReplicationState::reset_peers() {
    return node_manager->reset_peers();
}

bool ReplicationState::is_alive() const {
    return node_manager->is_alive();
}

uint64_t ReplicationState::node_state() const {
    return node_manager->node_state();
}

int64_t ReplicationState::get_num_queued_writes() {
    return node_manager->get_num_queued_writes();
}

bool ReplicationState::is_leader() {
    return node_manager->is_leader();
}

nlohmann::json ReplicationState::get_status() {
    return node_manager->get_status();
}

std::string ReplicationState::get_leader_url() const {
    return node_manager->get_leader_url();
}

void ReplicationState::persist_applying_index() {
    node_manager->persist_applying_index();
}

void ReplicationState::decr_pending_writes() {
    node_manager->decr_pending_writes();
}

void ReplicationState::do_snapshot(const std::string& snapshot_path, 
                                  const std::shared_ptr<http_req>& req,
                                  const std::shared_ptr<http_res>& res) {
    node_manager->do_snapshot(snapshot_path, req, res);
}

void ReplicationState::do_snapshot(const std::string& nodes) {
    node_manager->do_snapshot(nodes);
}

void ReplicationState::do_dummy_write() {
    node_manager->do_dummy_write();
}

void ReplicationState::set_ext_snapshot_path(const std::string& snapshot_path) {
    node_manager->set_ext_snapshot_path(snapshot_path);
}

void ReplicationState::set_snapshot_in_progress(const bool snapshot_in_progress) {
    node_manager->set_snapshot_in_progress(snapshot_in_progress);
}

// Simple getters and utility methods
http_message_dispatcher* ReplicationState::get_message_dispatcher() const {
    return message_dispatcher;
}

Store* ReplicationState::get_store() {
    return store;
}

// Shutdown coordination
void ReplicationState::shutdown() {
    LOG(INFO) << "Set shutting_down = true";
    shutting_down = true;

    // wait for pending writes to drop to zero
    LOG(INFO) << "Waiting for in-flight writes to finish...";
    while(pending_writes.load() != 0) {
        LOG(INFO) << "pending_writes: " << pending_writes;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    LOG(INFO) << "Replication state shutdown, store sequence: " << store->get_latest_seq_number();
    std::unique_lock lock(node_mutex);

    if (node) {
        LOG(INFO) << "node->shutdown";
        node->shutdown(nullptr);

        // Blocking this thread until the node is eventually down.
        LOG(INFO) << "node->join";
        node->join();
        delete node;
        node = nullptr;
    }
}

// NOTE: The following methods remain in this file as they're core to 
// the StateMachine interface and need direct access to private members:
// - on_apply()
// - on_snapshot_save()
// - on_snapshot_load()
// - save_snapshot()
// - init_db()
// - start_raft_node()

// These are implemented in raft_lifecycle_manager.cpp as per your request

// Snapshot closure implementations
OnDemandSnapshotClosure::OnDemandSnapshotClosure(ReplicationState *replication_state, 
                                                 const std::shared_ptr<http_req>& req,
                                                 const std::shared_ptr<http_res>& res, 
                                                 const std::string& ext_snapshot_path,
                                                 const std::string& state_dir_path)
    : replication_state(replication_state), req(req), res(res), 
      ext_snapshot_path(ext_snapshot_path), state_dir_path(state_dir_path) {}

TimedSnapshotClosure::TimedSnapshotClosure(ReplicationState *replication_state)
    : replication_state(replication_state) {}

// The implementations of OnDemandSnapshotClosure::Run() and TimedSnapshotClosure::Run()
// remain in raft_node_manager.cpp (they were previously in that file)
