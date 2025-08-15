#include "raft_server.h"
#include "raft_config.h"
#include "raft_http.h"
#include "store.h"
#include <butil/files/file_enumerator.h>
#include <thread>
#include <string_utils.h>
#include <file_utils.h>
#include <collection_manager.h>
#include <http_client.h>
#include <conversation_model_manager.h>
#include "rocksdb/utilities/checkpoint.h"
#include "core_api.h"
#include "personalization_model_manager.h"
#include "cached_resource_stat.h"
#include <logger.h>

namespace braft {
    DECLARE_int32(raft_do_snapshot_min_index_gap);
    DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
    DECLARE_bool(raft_enable_append_entries_cache);
    DECLARE_int32(raft_max_append_entries_cache_size);
    DECLARE_int32(raft_max_byte_count_per_rpc);
    DECLARE_int32(raft_rpc_channel_connect_timeout_ms);
}

// ReplicationClosure implementation
void ReplicationClosure::Run() {
    std::unique_ptr<ReplicationClosure> self_guard(this);
}

// Constructor
ReplicationState::ReplicationState(HttpServer* server, 
                                   BatchedIndexer* batched_indexer,
                                   Store* store, 
                                   Store* analytics_store,
                                   ThreadPool* thread_pool,
                                   http_message_dispatcher* message_dispatcher,
                                   bool api_uses_ssl, 
                                   const Config* config,
                                   size_t num_collections_parallel_load,
                                   size_t num_documents_parallel_load)
    : server(server), batched_indexer(batched_indexer),
      store(store), analytics_store(analytics_store),
      thread_pool(thread_pool), message_dispatcher(message_dispatcher),
      api_uses_ssl(api_uses_ssl), config(config),
      num_collections_parallel_load(num_collections_parallel_load),
      num_documents_parallel_load(num_documents_parallel_load),
      ready(false), shutting_down(false), pending_writes(0),
      snapshot_in_progress(false), last_snapshot_ts(std::time(nullptr)),
      snapshot_interval_s(config->get_snapshot_interval_seconds()) {
    
    LOG(INFO) << "ReplicationState initialized";
}

// Start the Raft node
int ReplicationState::start(const butil::EndPoint& peering_endpoint,
                           int api_port,
                           int election_timeout_ms,
                           int snapshot_max_byte_count_per_rpc,
                           const std::string& raft_dir,
                           const std::string& nodes,
                           const std::atomic<bool>& quit_abruptly) {
    
    LOG(INFO) << "Starting Raft state machine";
    
    // Store configuration
    this->election_timeout_interval_ms = election_timeout_ms;
    this->raft_dir_path = raft_dir;
    this->peering_endpoint = peering_endpoint;
    
    // Configure braft flags
    braft::FLAGS_raft_do_snapshot_min_index_gap = 1;
    braft::FLAGS_raft_max_parallel_append_entries_rpc_num = 1;
    braft::FLAGS_raft_enable_append_entries_cache = false;
    braft::FLAGS_raft_max_append_entries_cache_size = 8;
    braft::FLAGS_raft_max_byte_count_per_rpc = snapshot_max_byte_count_per_rpc;
    braft::FLAGS_raft_rpc_channel_connect_timeout_ms = 2000;
    
    // Create and initialize the node manager
    node_manager = std::make_unique<RaftNodeManager>(config, store, batched_indexer, api_uses_ssl);
    
    if(node_manager->init_node(this, peering_endpoint, api_port, 
                               election_timeout_ms, raft_dir, nodes) != 0) {
        LOG(ERROR) << "Failed to initialize Raft node";
        return -1;
    }
    
    // Wait for node to be ready
    const int WAIT_TIMEOUT_MS = 60 * 1000;
    if(!node_manager->wait_until_ready(WAIT_TIMEOUT_MS, quit_abruptly)) {
        LOG(ERROR) << "Raft node failed to become ready";
        return -1;
    }
    
    // Initialize database
    if(init_db() != 0) {
        LOG(ERROR) << "Failed to initialize database";
        return -1;
    }
    
    ready = true;
    LOG(INFO) << "Raft state machine started successfully";
    return 0;
}

// Process write requests through Raft (HTTP handling merged back)
void ReplicationState::write(const std::shared_ptr<http_req>& request,
                            const std::shared_ptr<http_res>& response) {
    if(shutting_down) {
        response->set_503("Shutting down.");
        response->final = true;
        response->is_alive = false;
        request->notify();
        return;
    }
    
    // Check resources
    auto resource_check = cached_resource_stat_t::get_instance().has_enough_resources(
        raft_dir_path,
        config->get_disk_used_max_percentage(),
        config->get_memory_used_max_percentage()
    );
    
    if(resource_check != cached_resource_stat_t::OK && request->do_resource_check()) {
        response->set_422("Rejecting write: running out of resource type: " +
                         std::string(magic_enum::enum_name(resource_check)));
        response->final = true;
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }
    
    if(config->get_skip_writes() && request->path_without_query != "/config") {
        response->set_422("Skipping writes.");
        response->final = true;
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }
    
    // Check if not leader
    if(!node_manager || !node_manager->is_leader()) {
        return write_to_leader(request, response);
    }
    
    // Handle gzip if needed
    if(((request->body.size() > 2) &&
        (31 == (int)request->body[0] && -117 == (int)request->body[1])) || 
       request->zstream_initialized) {
        
        auto res = raft_http::handle_gzip(request);
        if(!res.ok()) {
            response->set_422(res.error());
            response->final = true;
            auto req_res = new async_req_res_t(request, response, true);
            return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        }
    }
    
    // Serialize request for Raft log
    butil::IOBufBuilder bufBuilder;
    bufBuilder << request->to_json();
    
    // Create Raft task
    braft::Task task;
    task.data = &bufBuilder.buf();
    task.done = new ReplicationClosure(request, response);
    task.expected_term = node_manager->get_leader_term();
    
    // Apply to Raft
    node_manager->apply(task);
    pending_writes++;
}

// Forward writes to leader
void ReplicationState::write_to_leader(const std::shared_ptr<http_req>& request,
                                      const std::shared_ptr<http_res>& response) {
    if(!node_manager) {
        LOG(ERROR) << "Node manager not initialized";
        response->set_500("Internal error");
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }
    
    braft::PeerId leader_id = node_manager->leader_id();
    if(leader_id.is_empty()) {
        LOG(ERROR) << "No leader available";
        
        if(response->proxied_stream) {
            LOG(ERROR) << "Terminating streaming request gracefully";
            response->is_alive = false;
            request->notify();
            return;
        }
        
        response->set_500("Could not find a leader");
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }
    
    if(response->proxied_stream) {
        request->notify();
        return;
    }
    
    // Build URL for leader
    h2o_custom_generator_t* custom_generator = 
        reinterpret_cast<h2o_custom_generator_t*>(response->generator.load());
    HttpServer* http_server = custom_generator->h2o_handler->http_server;
    
    auto raw_req = request->_req;
    const std::string& path = std::string(raw_req->path.base, raw_req->path.len);
    const std::string& scheme = std::string(raw_req->scheme->name.base, raw_req->scheme->name.len);
    const std::string url = raft_config::get_node_url_path(leader_id, path, scheme);
    
    // Forward request asynchronously
    thread_pool->enqueue([request, response, http_server, path, url, this]() {
        pending_writes++;
        
        std::map<std::string, std::string> res_headers;
        
        if(request->http_method == "POST") {
            std::vector<std::string> path_parts;
            StringUtils::split(path, path_parts, "/");
            
            if(path_parts.back().rfind("import", 0) == 0) {
                // Handle imports asynchronously
                response->proxied_stream = true;
                long status = HttpClient::post_response_async(url, request, response, http_server, true);
                
                if(status == 500) {
                    response->content_type_header = res_headers["content-type"];
                    response->set_500("");
                } else {
                    return;
                }
            } else {
                std::string api_res;
                long status = HttpClient::post_response(url, request->body, api_res, res_headers, {}, 0, true);
                response->content_type_header = res_headers["content-type"];
                response->set_body(status, api_res);
            }
        } else if(request->http_method == "PUT") {
            std::string api_res;
            long status = HttpClient::put_response(url, request->body, api_res, res_headers, 0, true);
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else if(request->http_method == "DELETE") {
            std::string api_res;
            long status = HttpClient::delete_response(url, api_res, res_headers, 0, true);
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else if(request->http_method == "PATCH") {
            std::string api_res;
            long status = HttpClient::patch_response(url, request->body, api_res, res_headers, 0, true);
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else {
            const std::string& err = "Forwarding for http method not implemented: " + request->http_method;
            LOG(ERROR) << err;
            response->set_500(err);
        }
        
        auto req_res = new async_req_res_t(request, response, true);
        message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        pending_writes--;
    });
}

// Read operation (unused)
void ReplicationState::read(const std::shared_ptr<http_res>& response) {
    // Not implemented - reads don't go through Raft currently
}

// Node management operations
void ReplicationState::refresh_nodes(const std::string& nodes, 
                                    size_t raft_counter,
                                    const std::atomic<bool>& reset_peers_on_error) {
    if(node_manager) {
        bool allow_reset = (raft_counter > 0 && reset_peers_on_error);
        node_manager->refresh_nodes(nodes, allow_reset);
    }
}

void ReplicationState::refresh_catchup_status(bool log_msg) {
    if(node_manager) {
        node_manager->refresh_catchup_status(log_msg);
    }
}

bool ReplicationState::trigger_vote() {
    if(node_manager) {
        auto status = node_manager->trigger_vote();
        LOG(INFO) << "Triggered vote. Ok? " << status.ok() << ", status: " << status;
        return status.ok();
    }
    return false;
}

bool ReplicationState::reset_peers() {
    if(!node_manager) {
        return false;
    }
    
    const Option<std::string>& refreshed_nodes_op = Config::fetch_nodes_config(config->get_nodes());
    if(!refreshed_nodes_op.ok()) {
        LOG(WARNING) << "Error while fetching peer configuration: " << refreshed_nodes_op.error();
        return false;
    }
    
    const std::string& nodes_config = raft_config::to_nodes_config(
        peering_endpoint, config->get_api_port(), refreshed_nodes_op.get()
    );
    
    if(nodes_config.empty()) {
        LOG(WARNING) << "No nodes resolved from peer configuration";
        return false;
    }
    
    braft::Configuration peer_config;
    peer_config.parse_from(nodes_config);
    
    auto status = node_manager->reset_peers(peer_config);
    LOG(INFO) << "Reset peers. Ok? " << status.ok() << ", status: " << status;
    return status.ok();
}

void ReplicationState::persist_applying_index() {
    batched_indexer->persist_applying_index();
}

uint64_t ReplicationState::node_state() const {
    if(!node_manager) {
        return 0;
    }
    
    braft::NodeStatus status;
    node_manager->get_status(&status);
    return status.state;
}

// Snapshot management
void ReplicationState::do_snapshot(const std::string& snapshot_path,
                                  const std::shared_ptr<http_req>& req,
                                  const std::shared_ptr<http_res>& res) {
    if(!node_manager) {
        res->set_500("Node not initialized");
        auto req_res = new async_req_res_t(req, res, true);
        message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        return;
    }
    
    if(snapshot_in_progress) {
        res->set_409("Another snapshot is in progress");
        auto req_res = new async_req_res_t(req, res, true);
        message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        return;
    }
    
    LOG(INFO) << "Triggering on-demand snapshot" 
              << (!snapshot_path.empty() ? " with external path" : "");
    
    thread_pool->enqueue([snapshot_path, req, res, this]() {
        OnDemandSnapshotClosure* closure = new OnDemandSnapshotClosure(
            this, req, res, snapshot_path, raft_dir_path
        );
        ext_snapshot_path = snapshot_path;
        node_manager->snapshot(closure);
    });
}

void ReplicationState::do_snapshot(const std::string& nodes) {
    auto current_ts = std::time(nullptr);
    if(current_ts - last_snapshot_ts < snapshot_interval_s) {
        return;
    }
    
    LOG(INFO) << "Timed snapshot triggered";
    
    if(node_manager && node_manager->is_leader()) {
        // Check if all peers are healthy before snapshot
        std::vector<braft::PeerId> peers;
        braft::Configuration peer_config;
        peer_config.parse_from(nodes);
        peer_config.list_peers(&peers);
        
        bool all_healthy = true;
        std::string my_addr = node_manager->node_id().peer_id.to_string();
        
        for(const auto& peer : peers) {
            if(peer.to_string() == my_addr) {
                continue; // Skip self
            }
            
            const std::string protocol = api_uses_ssl ? "https" : "http";
            std::string url = raft_config::get_node_url_path(peer, "/health", protocol);
            std::string api_res;
            std::map<std::string, std::string> res_headers;
            long status_code = HttpClient::get_response(url, api_res, res_headers, {}, 5*1000, true);
            
            if(status_code != 200) {
                LOG(WARNING) << "Peer " << peer.to_string() << " unhealthy during snapshot pre-check";
                all_healthy = false;
            }
        }
        
        if(!all_healthy) {
            LOG(WARNING) << "Unable to trigger snapshot - one or more peers unhealthy";
            return;
        }
    }
    
    if(node_manager) {
        TimedSnapshotClosure* closure = new TimedSnapshotClosure(this);
        node_manager->snapshot(closure);
        last_snapshot_ts = current_ts;
    }
}

void ReplicationState::do_dummy_write() {
    if(!node_manager) {
        LOG(ERROR) << "Cannot do dummy write - node manager not initialized";
        return;
    }
    
    braft::PeerId leader_id = node_manager->leader_id();
    if(leader_id.is_empty()) {
        LOG(ERROR) << "Cannot do dummy write - no leader";
        return;
    }
    
    const std::string protocol = api_uses_ssl ? "https" : "http";
    std::string url = raft_config::get_node_url_path(leader_id, "/health", protocol);
    
    std::string api_res;
    std::map<std::string, std::string> res_headers;
    long status_code = HttpClient::post_response(url, "", api_res, res_headers, {}, 4000, true);
    
    LOG(INFO) << "Dummy write to " << url << ", status = " << status_code;
}

// Shutdown
void ReplicationState::shutdown() {
    LOG(INFO) << "Shutting down ReplicationState";
    shutting_down = true;
    
    // Wait for pending writes
    LOG(INFO) << "Waiting for pending writes to complete...";
    while(pending_writes.load() != 0) {
        LOG(INFO) << "Pending writes: " << pending_writes;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    
    // Shutdown node
    if(node_manager) {
        node_manager->shutdown();
    }
    
    LOG(INFO) << "ReplicationState shutdown complete";
}

// Initialize database
int ReplicationState::init_db() {
    LOG(INFO) << "Loading collections from disk...";
    
    Option<bool> init_op = CollectionManager::get_instance().load(
        num_collections_parallel_load, num_documents_parallel_load
    );
    
    if(!init_op.ok()) {
        LOG(ERROR) << "Failed to load collections: " << init_op.error();
        return 1;
    }
    
    LOG(INFO) << "Finished loading collections from disk";
    
    // Initialize conversation models
    auto conversation_models_init = ConversationModelManager::init(store);
    if(!conversation_models_init.ok()) {
        LOG(INFO) << "Failed to initialize conversation model manager: " << conversation_models_init.error();
    } else {
        LOG(INFO) << "Loaded " << conversation_models_init.get() << " conversation model(s)";
    }
    
    // Initialize batched indexer state
    if(batched_indexer) {
        LOG(INFO) << "Initializing batched indexer from snapshot state...";
        std::string batched_indexer_state_str;
        StoreStatus s = store->get(BATCHED_INDEXER_STATE_KEY, batched_indexer_state_str);
        if(s == FOUND) {
            nlohmann::json batch_indexer_state = nlohmann::json::parse(batched_indexer_state_str);
            batched_indexer->load_state(batch_indexer_state);
        }
    }
    
    // Initialize personalization models
    auto personalization_models_init = PersonalizationModelManager::init(store);
    if(!personalization_models_init.ok()) {
        LOG(INFO) << "Failed to initialize personalization model manager: " << personalization_models_init.error();
    } else {
        LOG(INFO) << "Loaded " << personalization_models_init.get() << " personalization model(s)";
    }
    
    return 0;
}

// StateMachine interface implementation
void ReplicationState::on_apply(braft::Iterator& iter) {
    for(; iter.valid(); iter.next()) {
        braft::AsyncClosureGuard closure_guard(iter.done());
        
        const std::shared_ptr<http_req>& request_generated = iter.done() ?
            dynamic_cast<ReplicationClosure*>(iter.done())->get_request() : 
            std::make_shared<http_req>();
        
        const std::shared_ptr<http_res>& response_generated = iter.done() ?
            dynamic_cast<ReplicationClosure*>(iter.done())->get_response() : 
            std::make_shared<http_res>(nullptr);
        
        if(!iter.done()) {
            // Log entry - deserialize request
            request_generated->load_from_json(iter.data().to_string());
        }
        
        request_generated->log_index = iter.index();
        
        // Queue for batch processing
        batched_indexer->enqueue(request_generated, response_generated);
        
        if(iter.done()) {
            pending_writes--;
        }
    }
}

void ReplicationState::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    LOG(INFO) << "on_snapshot_save";
    
    snapshot_in_progress = true;
    std::string db_snapshot_path = writer->get_path() + "/" + db_snapshot_name;
    std::string analytics_db_snapshot_path = writer->get_path() + "/" + analytics_db_snapshot_name;
    
    {
        // Lock batch indexer for clean snapshot
        std::shared_mutex& pause_mutex = batched_indexer->get_pause_mutex();
        std::unique_lock lk(pause_mutex);
        
        nlohmann::json batch_index_state;
        batched_indexer->serialize_state(batch_index_state);
        store->insert(BATCHED_INDEXER_STATE_KEY, batch_index_state.dump());
        
        batched_indexer->clear_skip_indices();
        
        // Create checkpoint
        rocksdb::Checkpoint* checkpoint = nullptr;
        rocksdb::Status status = store->create_check_point(&checkpoint, db_snapshot_path);
        std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);
        
        if(!status.ok()) {
            LOG(ERROR) << "Checkpoint creation failed: " << status.ToString();
            done->status().set_error(EIO, "Checkpoint creation failure");
        }
        
        // Analytics store checkpoint if present
        if(analytics_store) {
            analytics_store->flush();
            
            rocksdb::Checkpoint* checkpoint2 = nullptr;
            status = analytics_store->create_check_point(&checkpoint2, analytics_db_snapshot_path);
            std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint2);
            
            if(!status.ok()) {
                LOG(ERROR) << "Analytics checkpoint creation failed: " << status.ToString();
                done->status().set_error(EIO, "Analytics checkpoint creation failure");
            }
        }
    }
    
    // Create snapshot argument
    SnapshotArg* arg = new SnapshotArg;
    arg->replication_state = this;
    arg->writer = writer;
    arg->state_dir_path = raft_dir_path;
    arg->db_snapshot_path = db_snapshot_path;
    arg->done = done;
    
    if(analytics_store) {
        arg->analytics_db_snapshot_path = analytics_db_snapshot_path;
    }
    
    if(!ext_snapshot_path.empty()) {
        arg->ext_snapshot_path = ext_snapshot_path;
    }
    
    // Start background thread for snapshot
    bthread_t tid;
    bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}

void* ReplicationState::save_snapshot(void* arg) {
    LOG(INFO) << "save_snapshot called";
    
    SnapshotArg* sa = static_cast<SnapshotArg*>(arg);
    std::unique_ptr<SnapshotArg> arg_guard(sa);
    
    // Add DB snapshot files
    butil::FileEnumerator dir_enum(butil::FilePath(sa->db_snapshot_path), false, 
                                   butil::FileEnumerator::FILES);
    
    for(butil::FilePath file = dir_enum.Next(); !file.empty(); file = dir_enum.Next()) {
        std::string file_name = std::string(db_snapshot_name) + "/" + file.BaseName().value();
        if(sa->writer->add_file(file_name) != 0) {
            sa->done->status().set_error(EIO, "Failed to add file to writer");
            sa->replication_state->snapshot_in_progress = false;
            return nullptr;
        }
    }
    
    // Add analytics DB snapshot files if present
    if(!sa->analytics_db_snapshot_path.empty()) {
        butil::FileEnumerator analytics_dir_enum(butil::FilePath(sa->analytics_db_snapshot_path), 
                                                 false, butil::FileEnumerator::FILES);
        
        for(butil::FilePath file = analytics_dir_enum.Next(); !file.empty(); file = analytics_dir_enum.Next()) {
            auto file_name = std::string(analytics_db_snapshot_name) + "/" + file.BaseName().value();
            if(sa->writer->add_file(file_name) != 0) {
                sa->done->status().set_error(EIO, "Failed to add analytics file to writer");
                sa->replication_state->snapshot_in_progress = false;
                return nullptr;
            }
        }
    }
    
    sa->done->Run();
    
    // Do dummy write to ensure future snapshots can be triggered
    sa->replication_state->do_dummy_write();
    
    LOG(INFO) << "save_snapshot done";
    
    return nullptr;
}

int ReplicationState::on_snapshot_load(braft::SnapshotReader* reader) {
    LOG(INFO) << "on_snapshot_load";
    
    // Ensure reads/writes are rejected during reload
    if(node_manager) {
        // This will set read_caught_up and write_caught_up to false internally
        node_manager->refresh_catchup_status(false);
    }
    
    // Load analytics snapshot if present
    std::string analytics_snapshot_path = reader->get_path();
    analytics_snapshot_path.append(std::string("/") + analytics_db_snapshot_name);
    
    if(analytics_store && directory_exists(analytics_snapshot_path)) {
        int reload_store = analytics_store->reload(true, analytics_snapshot_path,
                                                   config->get_analytics_db_ttl());
        if(reload_store != 0) {
            LOG(ERROR) << "Failed to reload analytics db snapshot";
            return reload_store;
        }
    }
    
    // Load main DB snapshot
    std::string db_snapshot_path = reader->get_path();
    db_snapshot_path.append(std::string("/") + db_snapshot_name);
    
    int reload_store = store->reload(true, db_snapshot_path);
    if(reload_store != 0) {
        return reload_store;
    }
    
    // Reinitialize database
    return init_db();
}

// Snapshot closure implementations
OnDemandSnapshotClosure::OnDemandSnapshotClosure(ReplicationState* replication_state,
                                                 const std::shared_ptr<http_req>& req,
                                                 const std::shared_ptr<http_res>& res,
                                                 const std::string& ext_snapshot_path,
                                                 const std::string& state_dir_path)
    : replication_state(replication_state), req(req), res(res),
      ext_snapshot_path(ext_snapshot_path), state_dir_path(state_dir_path) {}

void OnDemandSnapshotClosure::Run() {
    std::unique_ptr<OnDemandSnapshotClosure> self_guard(this);
    
    bool ext_snapshot_succeeded = false;
    
    // Copy snapshot to external path if requested
    if(!ext_snapshot_path.empty()) {
        const butil::FilePath& dest_state_dir = butil::FilePath(ext_snapshot_path + "/state");
        
        if(!butil::DirectoryExists(dest_state_dir)) {
            butil::CreateDirectory(dest_state_dir, true);
        }
        
        const butil::FilePath& src_snapshot_dir = butil::FilePath(state_dir_path + "/snapshot");
        const butil::FilePath& src_meta_dir = butil::FilePath(state_dir_path + "/meta");
        
        bool snapshot_copied = butil::CopyDirectory(src_snapshot_dir, dest_state_dir, true);
        bool meta_copied = butil::CopyDirectory(src_meta_dir, dest_state_dir, true);
        
        ext_snapshot_succeeded = snapshot_copied && meta_copied;
    }
    
    // Clear external snapshot path
    replication_state->set_ext_snapshot_path("");
    replication_state->set_snapshot_in_progress(false);
    
    req->last_chunk_aggregate = true;
    res->final = true;
    
    nlohmann::json response;
    uint32_t status_code;
    
    if(!status().ok()) {
        LOG(ERROR) << "On-demand snapshot failed: " << status().error_str();
        status_code = 500;
        response["success"] = false;
        response["error"] = status().error_str();
    } else if(!ext_snapshot_succeeded && !ext_snapshot_path.empty()) {
        LOG(ERROR) << "On-demand snapshot failed: copy failed";
        status_code = 500;
        response["success"] = false;
        response["error"] = "Copy failed";
    } else {
        LOG(INFO) << "On-demand snapshot succeeded";
        status_code = 201;
        response["success"] = true;
    }
    
    res->status_code = status_code;
    res->body = response.dump();
    
    auto req_res = new async_req_res_t(req, res, true);
    replication_state->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    
    res->wait();
}

TimedSnapshotClosure::TimedSnapshotClosure(ReplicationState* replication_state)
    : replication_state(replication_state) {}

void TimedSnapshotClosure::Run() {
    std::unique_ptr<TimedSnapshotClosure> self_guard(this);
    
    if(status().ok()) {
        LOG(INFO) << "Timed snapshot succeeded";
    } else {
        LOG(ERROR) << "Timed snapshot failed: " << status().error_str();
    }
    
    replication_state->set_snapshot_in_progress(false);
}
