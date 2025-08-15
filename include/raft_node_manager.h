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

public:
    /**
     * Constructor for RaftNodeManager
     */
    explicit RaftNodeManager(ReplicationState* state);
    
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