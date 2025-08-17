#include "raft_server.h"
#include "string_utils.h"
#include "logger.h"
#include "file_utils.h"

#include <thread>
#include <fstream>

// Raft Lifecycle and Snapshot Management Module
// Extracted from raft_server.cpp for better organization

int ReplicationState::start(const butil::EndPoint & peering_endpoint, const int api_port,
                           const std::string & data_dir, const std::string & nodes,
                           const size_t raft_counter, const std::string & state_dir_path) {

    LOG(INFO) << "Starting Raft node with peering endpoint: " << peering_endpoint
              << ", API port: " << api_port << ", data dir: " << data_dir;

    // Initialize configuration
    std::string nodes_config = to_nodes_config(peering_endpoint, api_port, nodes);
    NodeConfiguration parsed_config = parse_node_configuration(nodes_config);
    
    // Store current configuration
    {
        std::unique_lock<std::shared_mutex> lock(current_config_mutex);
        current_node_config = parsed_config;
        current_nodes_config_str = nodes_config;
    }

    // Initialize braft configuration
    braft::Configuration initial_conf = node_config_to_braft(parsed_config);

    // Set up raft options
    braft::NodeOptions node_options;
    node_options.election_timeout_ms = 5000;
    node_options.fsm = this;
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = 30;
    node_options.log_uri = data_dir + "/log";
    node_options.raft_meta_uri = data_dir + "/raft_meta";
    node_options.snapshot_uri = data_dir + "/snapshot";
    node_options.disable_cli = false;
    
    // Initialize the raft node
    node = new braft::Node("typesense_raft_group", braft::PeerId(peering_endpoint));
    
    int init_result = node->init(node_options);
    if (init_result != 0) {
        LOG(ERROR) << "Failed to initialize raft node: " << init_result;
        delete node;
        node = nullptr;
        return init_result;
    }

    // Initialize database
    int db_init_result = init_db();
    if (db_init_result != 0) {
        LOG(ERROR) << "Failed to initialize database: " << db_init_result;
        shutdown();
        return db_init_result;
    }

    // Bootstrap or join cluster
    if (initial_conf.size() == 1) {
        // Bootstrap new cluster
        LOG(INFO) << "Bootstrapping new single-node cluster";
        butil::Status bootstrap_status = node->bootstrap(initial_conf);
        if (!bootstrap_status.ok()) {
            LOG(ERROR) << "Failed to bootstrap cluster: " << bootstrap_status.error_str();
            shutdown();
            return -1;
        }
    } else {
        // Join existing cluster
        LOG(INFO) << "Joining existing cluster with " << initial_conf.size() << " nodes";
        // The node will automatically try to join the cluster
    }

    LOG(INFO) << "Raft node started successfully";
    return 0;
}

void* ReplicationState::save_snapshot(void* arg) {
    SnapshotArg* snapshot_arg = static_cast<SnapshotArg*>(arg);
    ReplicationState* replication_state = snapshot_arg->replication_state;
    braft::SnapshotWriter* writer = snapshot_arg->writer;
    braft::Closure* done = snapshot_arg->done;

    LOG(INFO) << "Starting snapshot save process";

    std::unique_ptr<SnapshotArg> arg_guard(snapshot_arg);
    brpc::ClosureGuard done_guard(done);

    // Create snapshot path
    std::string snapshot_path = writer->get_path() + "/data";
    
    try {
        // Create snapshot directory
        if (!file_utils::create_directory(snapshot_path)) {
            LOG(ERROR) << "Failed to create snapshot directory: " << snapshot_path;
            done->status().set_error(EIO, "Failed to create snapshot directory");
            return nullptr;
        }

        // Save current state to snapshot
        if (replication_state->store) {
            // Get current applying index for consistency
            int64_t applying_index = replication_state->get_applying_index();
            
            LOG(INFO) << "Saving snapshot at applying index: " << applying_index;
            
            // Create snapshot metadata
            nlohmann::json snapshot_meta;
            snapshot_meta["applying_index"] = applying_index;
            snapshot_meta["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            // Save metadata
            std::string meta_file = snapshot_path + "/meta.json";
            std::ofstream meta_stream(meta_file);
            if (!meta_stream) {
                LOG(ERROR) << "Failed to create snapshot metadata file: " << meta_file;
                done->status().set_error(EIO, "Failed to create metadata file");
                return nullptr;
            }
            meta_stream << snapshot_meta.dump(2);
            meta_stream.close();

            // Save store data (implementation depends on store type)
            std::string store_snapshot_path = snapshot_path + "/store";
            bool store_success = replication_state->store->create_snapshot(store_snapshot_path);
            
            if (!store_success) {
                LOG(ERROR) << "Failed to create store snapshot";
                done->status().set_error(EIO, "Failed to create store snapshot");
                return nullptr;
            }
        }

        // Add files to snapshot
        if (writer->add_file("data") != 0) {
            LOG(ERROR) << "Failed to add snapshot data to writer";
            done->status().set_error(EIO, "Failed to add snapshot data");
            return nullptr;
        }

        LOG(INFO) << "Snapshot save completed successfully";

    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception during snapshot save: " << e.what();
        done->status().set_error(EIO, "Exception during snapshot save");
        return nullptr;
    }

    return nullptr;
}

void ReplicationState::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    LOG(INFO) << "Snapshot save requested";

    // Create snapshot argument
    SnapshotArg* arg = new SnapshotArg;
    arg->replication_state = this;
    arg->writer = writer;
    arg->done = done;

    // Create thread for snapshot saving (non-blocking)
    std::thread snapshot_thread(save_snapshot, arg);
    snapshot_thread.detach();
}

int ReplicationState::init_db() {
    LOG(INFO) << "Initializing database for raft state machine";
    
    if (!store) {
        LOG(ERROR) << "Store not initialized";
        return -1;
    }

    // Initialize any database-specific state here
    // This could include loading indexes, validating data integrity, etc.
    
    LOG(INFO) << "Database initialization completed";
    return 0;
}

int ReplicationState::on_snapshot_load(braft::SnapshotReader* reader) {
    LOG(INFO) << "Loading snapshot";

    // Check if snapshot exists
    if (!reader->list_files().empty()) {
        std::string snapshot_path = reader->get_path() + "/data";
        
        try {
            // Load snapshot metadata
            std::string meta_file = snapshot_path + "/meta.json";
            std::ifstream meta_stream(meta_file);
            if (!meta_stream) {
                LOG(ERROR) << "Failed to open snapshot metadata file: " << meta_file;
                return -1;
            }

            nlohmann::json snapshot_meta;
            meta_stream >> snapshot_meta;
            meta_stream.close();

            int64_t snapshot_applying_index = snapshot_meta["applying_index"];
            LOG(INFO) << "Loading snapshot with applying index: " << snapshot_applying_index;

            // Load store data
            if (store) {
                std::string store_snapshot_path = snapshot_path + "/store";
                bool load_success = store->load_snapshot(store_snapshot_path);
                
                if (!load_success) {
                    LOG(ERROR) << "Failed to load store snapshot";
                    return -1;
                }
            }

            // Update applying index
            set_applying_index(snapshot_applying_index);
            
            LOG(INFO) << "Snapshot loaded successfully";

        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception during snapshot load: " << e.what();
            return -1;
        }
    } else {
        LOG(INFO) << "No snapshot to load, starting with empty state";
    }

    return 0;
}

void ReplicationState::on_apply(braft::Iterator& iter) {
    // Apply log entries to the state machine
    for (; iter.valid(); iter.next()) {
        braft::AsyncClosureGuard closure_guard(iter.done());
        
        if (iter.done()) {
            // This is a user-initiated write operation
            ReplicationClosure* repl_closure = dynamic_cast<ReplicationClosure*>(iter.done());
            if (repl_closure) {
                // Process the write request
                try {
                    // Extract request data
                    butil::IOBuf data = iter.data();
                    std::string request_body = data.to_string();
                    
                    LOG(DEBUG) << "Applying write operation: " << request_body.substr(0, 100) << "...";
                    
                    // Apply to store via batched indexer
                    if (batched_indexer) {
                        // Queue the operation for batched processing
                        batched_indexer->enqueue(request_body, repl_closure->request, repl_closure->response);
                    } else {
                        LOG(ERROR) << "Batched indexer not available";
                        repl_closure->response->set_500("Internal server error");
                    }
                    
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Exception while applying write operation: " << e.what();
                    repl_closure->response->set_500("Internal server error");
                }
            }
        } else {
            // This is a configuration change or other internal operation
            LOG(DEBUG) << "Applying internal raft operation at index: " << iter.index();
        }
        
        // Update applying index
        set_applying_index(iter.index());
    }
}

void ReplicationState::do_snapshot(const std::string& snapshot_path, const std::shared_ptr<http_req>& req,
                                  const std::shared_ptr<http_res>& res) {
    LOG(INFO) << "Manual snapshot requested to path: " << snapshot_path;

    if (!node) {
        res->set_500("Raft node not initialized");
        return;
    }

    // Set external snapshot path if provided
    if (!snapshot_path.empty()) {
        set_ext_snapshot_path(snapshot_path);
    }

    // Create on-demand snapshot closure
    OnDemandSnapshotClosure* closure = new OnDemandSnapshotClosure(req, res);
    
    // Trigger snapshot
    node->snapshot(closure);
    
    LOG(INFO) << "Manual snapshot initiated";
}

void ReplicationState::set_ext_snapshot_path(const std::string& snapshot_path) {
    ext_snapshot_path = snapshot_path;
    LOG(DEBUG) << "External snapshot path set to: " << snapshot_path;
}

void ReplicationState::set_snapshot_in_progress(const bool snapshot_in_progress) {
    this->snapshot_in_progress = snapshot_in_progress;
    LOG(DEBUG) << "Snapshot in progress flag set to: " << snapshot_in_progress;
}

void ReplicationState::do_dummy_write() {
    if (!is_leader()) {
        LOG(DEBUG) << "Not leader, skipping dummy write";
        return;
    }

    LOG(DEBUG) << "Performing dummy write to establish leadership";

    // Create a dummy log entry
    braft::IOBuf log_entry;
    log_entry.append("dummy_write");
    
    // Create task
    braft::Task task;
    task.data = &log_entry;
    task.done = nullptr; // No callback needed for dummy write
    task.expected_term = get_current_term();
    
    // Apply to raft
    if (node) {
        node->apply(task);
        LOG(DEBUG) << "Dummy write submitted to raft";
    }
}

void ReplicationState::shutdown() {
    LOG(INFO) << "Shutting down raft node";

    if (node) {
        // Stop the raft node
        node->shutdown(nullptr);
        node->join();
        
        delete node;
        node = nullptr;
    }

    // Clean up other resources
    set_snapshot_in_progress(false);
    
    LOG(INFO) << "Raft node shutdown completed";
} 