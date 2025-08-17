#include "raft_server.h"
#include "string_utils.h"
#include "logger.h"

// Node Management and Status Module
// Extracted from raft_server.cpp for better organization

void ReplicationState::refresh_nodes(const std::string & nodes, const size_t raft_counter,
                                    const std::string & state_dir_path) {
    LOG(INFO) << "Refreshing nodes configuration: " << nodes;

    if (!node) {
        LOG(WARNING) << "Cannot refresh nodes - raft node not initialized";
        return;
    }

    // Parse the new configuration
    NodeConfiguration parsed_config = parse_node_configuration(nodes);
    
    // Update current configuration
    {
        std::unique_lock<std::shared_mutex> lock(current_config_mutex);
        current_node_config = parsed_config;
        current_nodes_config_str = nodes;
    }

    // Convert to braft configuration with fresh DNS resolution
    braft::Configuration new_conf = node_config_to_braft(parsed_config);
    
    // Check for unreachable hostname peers and trigger refresh if needed
    bool has_unreachable_hostnames = false;
    std::shared_lock<std::shared_mutex> lock(current_config_mutex);
    for (const auto& hostname_node : current_node_config.hostname_nodes) {
        std::string hostname = extract_hostname_from_node(hostname_node);
        if (!hostname.empty()) {
            std::string resolved_ip = hostname2ipstr(hostname);
            if (resolved_ip == hostname) {
                // DNS resolution failed
                LOG(WARNING) << "DNS resolution failed for hostname: " << hostname;
                has_unreachable_hostnames = true;
            }
        }
    }
    lock.unlock();

    if (has_unreachable_hostnames && immediate_refresh_requested.load()) {
        LOG(INFO) << "Immediate refresh requested due to hostname resolution issues";
        trigger_immediate_config_refresh();
    }

    // Apply the new configuration to braft
    if (is_leader()) {
        LOG(INFO) << "Applying new configuration as leader";
        
        // Use braft's change_peers for safe reconfiguration
        braft::SynchronizedClosure done;
        node->change_peers(new_conf, &done);
        done.wait();
        
        if (done.status().ok()) {
            LOG(INFO) << "Successfully applied new node configuration";
        } else {
            LOG(ERROR) << "Failed to apply new node configuration: " << done.status().error_str();
        }
    } else {
        LOG(DEBUG) << "Not leader, configuration will be applied when we become leader";
    }

    // Reset immediate refresh flag
    immediate_refresh_requested.store(false);
}

void ReplicationState::refresh_catchup_status(bool log_msg) {
    if (!node) {
        return;
    }

    braft::NodeStatus status;
    node->get_status(&status);

    // Calculate catchup metrics
    int64_t last_index = status.last_index;
    int64_t committed_index = status.committed_index;
    int64_t applied_index = status.known_applied_index;
    
    // Update internal state
    if (log_msg) {
        LOG(INFO) << "Catchup status - Last: " << last_index 
                  << ", Committed: " << committed_index 
                  << ", Applied: " << applied_index;
    }

    // Check if we're caught up
    int64_t catchup_threshold = 100; // Consider caught up if within 100 entries
    bool is_caught_up = (last_index - applied_index) <= catchup_threshold;
    
    if (is_caught_up && log_msg) {
        LOG(INFO) << "Node is caught up with cluster";
    } else if (!is_caught_up && log_msg) {
        LOG(WARNING) << "Node is lagging behind cluster by " << (last_index - applied_index) << " entries";
    }
}

bool ReplicationState::is_alive() const {
    return node != nullptr && !node->is_leader_lease_valid();
}

uint64_t ReplicationState::node_state() const {
    if (!node) {
        return 0; // Not initialized
    }

    braft::NodeStatus status;
    node->get_status(&status);
    
    // Encode node state as bitfield
    uint64_t state = 0;
    
    // Bit 0: Is leader
    if (status.state == braft::STATE_LEADER) {
        state |= 0x1;
    }
    
    // Bit 1: Is candidate
    if (status.state == braft::STATE_CANDIDATE) {
        state |= 0x2;
    }
    
    // Bit 2: Is follower
    if (status.state == braft::STATE_FOLLOWER) {
        state |= 0x4;
    }
    
    // Bit 3: Is caught up (within 100 entries of leader)
    if ((status.last_index - status.known_applied_index) <= 100) {
        state |= 0x8;
    }
    
    // Bits 4-31: Current term (28 bits)
    state |= (status.term & 0xFFFFFFF) << 4;
    
    // Bits 32-63: Committed index (lower 32 bits)
    state |= (status.committed_index & 0xFFFFFFFF) << 32;
    
    return state;
}

bool ReplicationState::trigger_vote() {
    if (!node) {
        LOG(ERROR) << "Cannot trigger vote - raft node not initialized";
        return false;
    }

    if (is_leader()) {
        LOG(DEBUG) << "Already leader, no need to trigger vote";
        return true;
    }

    LOG(INFO) << "Triggering leader election";
    
    // Reset election timeout to trigger immediate election
    int result = node->vote(0); // 0 means immediate election
    
    if (result == 0) {
        LOG(INFO) << "Vote triggered successfully";
        return true;
    } else {
        LOG(ERROR) << "Failed to trigger vote: " << result;
        return false;
    }
}

bool ReplicationState::reset_peers() {
    if (!node) {
        LOG(ERROR) << "Cannot reset peers - raft node not initialized";
        return false;
    }

    LOG(INFO) << "Resetting peer configuration";

    // Get current configuration
    std::shared_lock<std::shared_mutex> lock(current_config_mutex);
    NodeConfiguration current_config = current_node_config;
    lock.unlock();

    // Convert to braft configuration with fresh DNS resolution
    braft::Configuration new_conf = node_config_to_braft(current_config);
    
    // Reset peers using braft
    braft::SynchronizedClosure done;
    node->reset_peers(new_conf, &done);
    done.wait();
    
    if (done.status().ok()) {
        LOG(INFO) << "Successfully reset peer configuration";
        return true;
    } else {
        LOG(ERROR) << "Failed to reset peer configuration: " << done.status().error_str();
        return false;
    }
}

http_message_dispatcher* ReplicationState::get_message_dispatcher() const {
    return message_dispatcher;
}

Store* ReplicationState::get_store() {
    return store;
}

void ReplicationState::persist_applying_index() {
    if (!node) {
        return;
    }

    braft::NodeStatus status;
    node->get_status(&status);
    
    // Persist the current applying index for crash recovery
    // This would typically write to a persistent store
    LOG(DEBUG) << "Persisting applying index: " << status.known_applied_index;
    
    // Implementation would depend on the persistence mechanism
    // For now, we just log it
}

int64_t ReplicationState::get_num_queued_writes() {
    return pending_writes.load();
}

bool ReplicationState::is_leader() {
    if (!node) {
        return false;
    }

    braft::NodeStatus status;
    node->get_status(&status);
    
    return status.state == braft::STATE_LEADER;
}

nlohmann::json ReplicationState::get_status() {
    nlohmann::json status;
    
    if (!node) {
        status["state"] = "not_initialized";
        status["error"] = "Raft node not initialized";
        return status;
    }

    braft::NodeStatus raft_status;
    node->get_status(&raft_status);
    
    // Basic raft status
    status["state"] = (raft_status.state == braft::STATE_LEADER) ? "leader" :
                      (raft_status.state == braft::STATE_CANDIDATE) ? "candidate" : "follower";
    status["term"] = raft_status.term;
    status["last_index"] = raft_status.last_index;
    status["committed_index"] = raft_status.committed_index;
    status["applied_index"] = raft_status.known_applied_index;
    status["applying_index"] = raft_status.applying_index;
    
    // Leader information
    if (!raft_status.leader_id.is_empty()) {
        status["leader_id"] = raft_status.leader_id.to_string();
    }
    
    // Peer information
    std::shared_lock<std::shared_mutex> lock(current_config_mutex);
    status["total_nodes"] = current_node_config.total_nodes();
    status["hostname_nodes"] = current_node_config.hostname_nodes.size();
    status["ip_nodes"] = current_node_config.ip_nodes.size();
    status["config_version"] = current_node_config.config_version;
    status["config_term"] = current_node_config.config_term;
    lock.unlock();
    
    // Performance metrics
    status["pending_writes"] = pending_writes.load();
    status["snapshot_in_progress"] = snapshot_in_progress;
    
    // Catchup status
    int64_t lag = raft_status.last_index - raft_status.known_applied_index;
    status["lag_entries"] = lag;
    status["is_caught_up"] = lag <= 100;
    
    // DNS-specific status
    status["immediate_refresh_requested"] = immediate_refresh_requested.load();
    
    return status;
}

void ReplicationState::do_snapshot(const std::string& nodes) {
    LOG(INFO) << "Triggering snapshot with nodes: " << nodes;

    if (!node) {
        LOG(ERROR) << "Cannot create snapshot - raft node not initialized";
        return;
    }

    // Update configuration if nodes provided
    if (!nodes.empty()) {
        refresh_nodes(nodes, 0, "");
    }

    // Create timed snapshot closure
    TimedSnapshotClosure* closure = new TimedSnapshotClosure();
    
    // Trigger snapshot
    node->snapshot(closure);
    
    LOG(INFO) << "Snapshot creation initiated";
}

std::string ReplicationState::get_leader_url() const {
    if (!node) {
        return "";
    }

    braft::PeerId leader_id = node->leader_id();
    if (leader_id.is_empty()) {
        return "";
    }

    // Generate URL for the leader
    return get_node_url_path(leader_id, "/", "");
}

void ReplicationState::decr_pending_writes() {
    pending_writes--;
    LOG(DEBUG) << "Decremented pending writes, current count: " << pending_writes.load();
} 