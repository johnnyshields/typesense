#include "raft_server.h"
#include "store.h"
#include <butil/files/file_enumerator.h>
#include <butil/files/file_path.h>
#include <file_utils.h>
#include <collection_manager.h>
#include <conversation_model_manager.h>
#include "rocksdb/utilities/checkpoint.h"
#include "core_api.h"
#include "personalization_model_manager.h"
#include <logger.h>

// Raft Lifecycle and Snapshot Management Module
// This file implements the braft::StateMachine interface methods for ReplicationState

// Initialize database after node is ready
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

// Apply committed entries to the state machine
void ReplicationState::on_apply(braft::Iterator& iter) {
    // NOTE: this is executed on a different thread and runs concurrent to http thread
    for(; iter.valid(); iter.next()) {
        // Guard invokes done->Run() asynchronously to avoid blocking
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

        // Queue for batch processing to avoid blocking Raft thread
        batched_indexer->enqueue(request_generated, response_generated);

        if(iter.done()) {
            pending_writes--;
        }
    }
}

// Save a snapshot of the state machine
void ReplicationState::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    LOG(INFO) << "on_snapshot_save";

    snapshot_in_progress = true;
    std::string db_snapshot_path = writer->get_path() + "/" + db_snapshot_name;
    std::string analytics_db_snapshot_path = writer->get_path() + "/" + analytics_db_snapshot_name;

    {
        // Lock batch indexer for clean snapshot
        std::shared_mutex& pause_mutex = batched_indexer->get_pause_mutex();
        std::unique_lock lk(pause_mutex);

        // Serialize batch indexer state
        nlohmann::json batch_index_state;
        batched_indexer->serialize_state(batch_index_state);
        store->insert(BATCHED_INDEXER_STATE_KEY, batch_index_state.dump());

        // Clear skip indices before snapshot
        batched_indexer->clear_skip_indices();

        // Create main DB checkpoint
        rocksdb::Checkpoint* checkpoint = nullptr;
        rocksdb::Status status = store->create_check_point(&checkpoint, db_snapshot_path);
        std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);

        if(!status.ok()) {
            LOG(ERROR) << "Failure during checkpoint creation, msg:" << status.ToString();
            done->status().set_error(EIO, "Checkpoint creation failure.");
        }

        // Analytics store checkpoint if present
        if(analytics_store) {
            analytics_store->flush();

            rocksdb::Checkpoint* checkpoint2 = nullptr;
            status = analytics_store->create_check_point(&checkpoint2, analytics_db_snapshot_path);
            std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard2(checkpoint2);

            if(!status.ok()) {
                LOG(ERROR) << "AnalyticsStore : Failure during checkpoint creation, msg:" << status.ToString();
                done->status().set_error(EIO, "AnalyticsStore : Checkpoint creation failure.");
            }
        }
    }

    // Create snapshot argument for background thread
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

    // Start background thread for adding files to snapshot
    bthread_t tid;
    bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}

// Background thread function to save snapshot files
void* ReplicationState::save_snapshot(void* arg) {
    LOG(INFO) << "save_snapshot called";

    SnapshotArg* sa = static_cast<SnapshotArg*>(arg);
    std::unique_ptr<SnapshotArg> arg_guard(sa);

    // Add main DB snapshot files
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

    // NOTE: Must do dummy write here to ensure future snapshots can be triggered
    // (snapshots cannot be triggered if no write has happened since last snapshot)
    sa->replication_state->do_dummy_write();

    LOG(INFO) << "save_snapshot done";

    return nullptr;
}

// Load a snapshot to restore state machine
int ReplicationState::on_snapshot_load(braft::SnapshotReader* reader) {
    // Critical safety check - leader should NEVER load a snapshot
    CHECK(!node_manager || !node_manager->is_leader_safe_check()) 
        << "Leader is not supposed to load snapshot";

    LOG(INFO) << "on_snapshot_load";

    // Ensure reads/writes are rejected during reload, as `store->reload()` unique locks the DB handle
    if(node_manager) {
        // This will set read_caught_up and write_caught_up to false internally
        node_manager->refresh_catchup_status(false);
    }

    // Load snapshot from leader, replacing the running StateMachine
    std::string analytics_snapshot_path = reader->get_path();
    analytics_snapshot_path.append(std::string("/") + analytics_db_snapshot_name);

    if(analytics_store && directory_exists(analytics_snapshot_path)) {
        // analytics db snapshot could be missing (older version or disabled earlier)
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

    // Reinitialize database from loaded snapshot
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
    // Auto delete this after Done()
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

    // Clear external snapshot path and mark complete
    // Order is important, because the atomic boolean guards write to the path
    replication_state->set_ext_snapshot_path("");
    replication_state->set_snapshot_in_progress(false);

    req->last_chunk_aggregate = true;
    res->final = true;

    nlohmann::json response;
    uint32_t status_code;

    if(!status().ok()) {
        // in case of internal raft error
        LOG(ERROR) << "On demand snapshot failed, error: " << status().error_str() << ", code: " << status().error_code();
        status_code = 500;
        response["success"] = false;
        response["error"] = status().error_str();
    } else if(!ext_snapshot_succeeded && !ext_snapshot_path.empty()) {
        LOG(ERROR) << "On demand snapshot failed, error: copy failed.";
        status_code = 500;
        response["success"] = false;
        response["error"] = "Copy failed.";
    } else {
        LOG(INFO) << "On demand snapshot succeeded!";
        status_code = 201;
        response["success"] = true;
    }

    res->status_code = status_code;
    res->body = response.dump();

    auto req_res = new async_req_res_t(req, res, true);
    replication_state->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);

    // Wait for response to be sent
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
