#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <shared_mutex>
#include <braft/raft.h>
#include "http_data.h"
#include "option.h"

class ReplicationState;
class HttpServer;
class Store;
class Config;
class ThreadPool;
class http_message_dispatcher;
class BatchedIndexer;

/**
 * RaftHttpHandler manages HTTP request processing for Raft operations.
 * It handles gzip compression, request forwarding to leader, and write coordination.
 */
class RaftHttpHandler {
private:
    ReplicationState* replication_state;
    HttpServer* server;
    Store* store;
    ThreadPool* thread_pool;
    http_message_dispatcher* message_dispatcher;
    BatchedIndexer* batched_indexer;
    const Config* config;
    bool api_uses_ssl;
    std::string raft_dir_path;

public:
    /**
     * Constructor for RaftHttpHandler
     */
    RaftHttpHandler(ReplicationState* state, 
                   HttpServer* server, 
                   Store* store,
                   BatchedIndexer* batched_indexer,
                   ThreadPool* thread_pool, 
                   http_message_dispatcher* dispatcher,
                   const Config* config, 
                   bool api_uses_ssl,
                   const std::string& raft_dir_path);

    /**
     * Handle gzip compression for incoming requests
     * @param request The HTTP request to process
     * @return Success or error with status code
     */
    static Option<bool> handle_gzip(const std::shared_ptr<http_req>& request);

    /**
     * Process write requests through Raft consensus
     * @param request The HTTP request to process
     * @param response The HTTP response to populate
     */
    void write(const std::shared_ptr<http_req>& request, 
              const std::shared_ptr<http_res>& response);

    /**
     * Forward write requests to the leader node
     * @param request The HTTP request to forward
     * @param response The HTTP response to populate
     */
    void write_to_leader(const std::shared_ptr<http_req>& request, 
                        const std::shared_ptr<http_res>& response);

    /**
     * Process read requests (currently not used for consistency)
     * @param response The HTTP response to populate
     */
    void read(const std::shared_ptr<http_res>& response);

    /**
     * Get URL path for a given peer
     * @param peer_id The peer identifier
     * @param path The URL path
     * @param protocol The protocol (http/https)
     * @return Complete URL string
     */
    std::string get_node_url_path(const braft::PeerId& peer_id, 
                                  const std::string& path,
                                  const std::string& protocol) const;

private:
    /**
     * Check if there's an alter operation in progress for a collection
     */
    bool get_alter_in_progress(const std::string& collection_name);
};
