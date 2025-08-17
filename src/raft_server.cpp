#include "store.h"
#include "raft_server.h"
#include <butil/files/file_enumerator.h>
#include <thread>
#include <string_utils.h>
#include <file_utils.h>
#include <collection_manager.h>
#include <http_client.h>
#include <conversation_model_manager.h>
#include "rocksdb/utilities/checkpoint.h"
#include "thread_local_vars.h"
#include "core_api.h"
#include "personalization_model_manager.h"

// Raft Server - Slim Coordinator
// This file now coordinates the extracted modules:
// - raft_config_manager.cpp: DNS & Configuration
// - raft_safety_validator.cpp: TLA+ Safety
// - raft_http_handler.cpp: HTTP Processing
// - raft_lifecycle_manager.cpp: Raft Lifecycle & Snapshots
// - raft_node_manager.cpp: Node Management & Status

namespace braft {
    DECLARE_int32(raft_do_snapshot_min_index_gap);
    DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
    DECLARE_bool(raft_enable_append_entries_cache);
    DECLARE_int32(raft_max_append_entries_cache_size);
    DECLARE_int32(raft_max_byte_count_per_rpc);
    DECLARE_int32(raft_rpc_channel_connect_timeout_ms);
}

// Closure implementations
void ReplicationClosure::Run() {
    // Auto delete `this` after Run() - handled by upstream
    std::unique_ptr<ReplicationClosure> self_guard(this);
}

void TimedSnapshotClosure::Run() {
    LOG(INFO) << "Timed snapshot completed";
    delete this;
}

void OnDemandSnapshotClosure::Run() {
    if (request && response) {
        response->set_200("Snapshot completed successfully");
    }
    LOG(INFO) << "On-demand snapshot completed";
    delete this;
}

// Constructor - Initialize the coordination layer
ReplicationState::ReplicationState(HttpServer* server, BatchedIndexer* batched_indexer,
                                 const std::string& state_dir_path, const size_t raft_counter) :
    server(server),
    batched_indexer(batched_indexer),
    state_dir_path(state_dir_path),
    raft_counter(raft_counter),
    node(nullptr),
    store(nullptr),
    message_dispatcher(nullptr),
    http_client(nullptr),
    ext_snapshot_path(""),
    snapshot_in_progress(false),
    read_caught_up(false),
    write_caught_up(false),
    election_timeout_interval_ms(5000),
    pending_writes(0),
    // Initialize DNS-native and safety state
    immediate_refresh_requested(false),
    last_term_quorum_check(0),
    last_config_quorum_check(0),
    last_safety_validation(std::chrono::steady_clock::now()),
    dns_cache_(std::make_unique<RaftDNSCache>()) {
    
    LOG(INFO) << "ReplicationState coordinator initialized";
}

// Core coordination methods that delegate to appropriate modules

int ReplicationState::start(const butil::EndPoint& peering_endpoint, const int api_port,
                           int election_timeout_ms, int snapshot_max_byte_count_per_rpc,
                           const std::string& raft_dir, const std::string& nodes,
                           const std::atomic<bool>& quit_abruptly) {
    
    LOG(INFO) << "Starting Raft coordination layer";
    
    // Set coordinator state
    this->election_timeout_interval_ms = election_timeout_ms;
    this->raft_dir_path = raft_dir;
    this->peering_endpoint = peering_endpoint;
    this->read_caught_up = false;
    this->write_caught_up = false;
    
    // Configure braft flags
    braft::FLAGS_raft_do_snapshot_min_index_gap = 1;
    braft::FLAGS_raft_max_parallel_append_entries_rpc_num = 1;
    braft::FLAGS_raft_enable_append_entries_cache = false;
    braft::FLAGS_raft_max_append_entries_cache_size = 8;
    braft::FLAGS_raft_max_byte_count_per_rpc = snapshot_max_byte_count_per_rpc;
    braft::FLAGS_raft_rpc_channel_connect_timeout_ms = 2000;
    
    // Delegate to lifecycle manager for actual startup
    // Note: This calls the start() method defined in raft_lifecycle_manager.cpp
    return start(peering_endpoint, api_port, raft_dir, nodes, raft_counter, state_dir_path);
}

// Delegate HTTP operations to HTTP handler module
void ReplicationState::write(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
    // Implemented in raft_http_handler.cpp
}

void ReplicationState::read(const std::shared_ptr<http_res>& response) {
    // Implemented in raft_http_handler.cpp  
}

// Delegate node management to node manager module
void ReplicationState::refresh_nodes(const std::string& nodes, const size_t raft_counter,
                                    const std::string& state_dir_path) {
    // Implemented in raft_node_manager.cpp
}

// Delegate safety operations to safety validator module
bool ReplicationState::add_node_safe(const std::string& node_to_add) {
    // Implemented in raft_safety_validator.cpp
    return false; // Placeholder
}

bool ReplicationState::remove_node_safe(const std::string& node_to_remove) {
    // Implemented in raft_safety_validator.cpp
    return false; // Placeholder
}

// Delegate configuration operations to config manager module
NodeConfiguration ReplicationState::parse_node_configuration(const std::string& nodes_config) {
    // Implemented in raft_config_manager.cpp
    return NodeConfiguration{}; // Placeholder
}

// Coordinate immediate refresh requests (used by multiple modules)
void ReplicationState::check_immediate_refresh() {
    if (immediate_refresh_requested.load()) {
        LOG(INFO) << "Immediate refresh requested - triggering node refresh";
        
        // Get current configuration
        std::shared_lock<std::shared_mutex> lock(current_config_mutex);
        std::string current_nodes = current_nodes_config_str;
        lock.unlock();
        
        // Refresh with current configuration to trigger DNS re-resolution
        refresh_nodes(current_nodes, raft_counter, state_dir_path);
    }
}

// Simple getters and utility methods
bool ReplicationState::is_alive() const {
    // Implemented in raft_node_manager.cpp
    return node != nullptr;
}

bool ReplicationState::is_leader() {
    // Implemented in raft_node_manager.cpp
    return false; // Placeholder
}

nlohmann::json ReplicationState::get_status() {
    // Implemented in raft_node_manager.cpp
    return nlohmann::json{}; // Placeholder
}

Store* ReplicationState::get_store() {
    return store;
}

http_message_dispatcher* ReplicationState::get_message_dispatcher() const {
    return message_dispatcher;
}

void ReplicationState::set_store(Store* store) {
    this->store = store;
}

void ReplicationState::set_message_dispatcher(http_message_dispatcher* dispatcher) {
    this->message_dispatcher = dispatcher;
}

void ReplicationState::set_http_client(HttpClient* client) {
    this->http_client = client;
}

// Applying index management (used by lifecycle manager)
int64_t ReplicationState::get_applying_index() const {
    std::shared_lock<std::shared_mutex> lock(applying_index_mutex);
    return applying_index;
}

void ReplicationState::set_applying_index(int64_t index) {
    std::unique_lock<std::shared_mutex> lock(applying_index_mutex);
    applying_index = index;
}

// Coordinate shutdown across all modules
void ReplicationState::shutdown() {
    LOG(INFO) << "Coordinating shutdown across all modules";
    
    // Delegate to lifecycle manager for actual shutdown
    // This calls the shutdown() method in raft_lifecycle_manager.cpp
    
    // Clean up coordinator state
    store = nullptr;
    message_dispatcher = nullptr;
    http_client = nullptr;
    
    LOG(INFO) << "Raft coordination layer shutdown completed";
}

// Error handling coordination
void ReplicationState::on_error(const braft::Error& e) {
    LOG(ERROR) << "Raft error occurred: " << e;
    
    // Check if this is a replication error that might benefit from DNS refresh
    if (e.type() == braft::ERROR_TYPE_LOG_REPLICATION || 
        e.type() == braft::ERROR_TYPE_INSTALL_SNAPSHOT) {
        
        LOG(INFO) << "Replication error detected - triggering immediate config refresh";
        trigger_immediate_config_refresh(); // Implemented in raft_safety_validator.cpp
    }
}

// Snapshot argument structure for lifecycle manager
struct SnapshotArg {
    ReplicationState* replication_state;
    braft::SnapshotWriter* writer;
    braft::Closure* done;
};

// Main coordination loop integration point
void ReplicationState::coordinate_main_loop() {
    // This method can be called from the main server loop
    // to coordinate periodic tasks across modules
    
    check_immediate_refresh();
    
    // Additional coordination tasks can be added here
    // For example:
    // - Periodic safety validations
    // - Health checks across modules
    // - Performance metric collection
} 