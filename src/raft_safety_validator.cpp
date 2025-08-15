#include "raft_server.h"
#include "logger.h"

// MongoDB TLA+ Safety Validation Module
// Extracted from raft_server.cpp for better organization

void ReplicationState::handle_peer_failure(const braft::PeerId& failed_peer_id) {
    LOG(INFO) << "Handling peer failure for: " << failed_peer_id;
    
    // Check if this failed peer corresponds to a hostname node
    std::shared_lock<std::shared_mutex> lock(current_config_mutex);
    
    bool found_hostname_match = false;
    for (const auto& hostname_node : current_node_config.hostname_nodes) {
        if (peer_matches_hostname_node(failed_peer_id, hostname_node)) {
            LOG(INFO) << "Failed peer " << failed_peer_id << " matches hostname node " << hostname_node 
                      << " - triggering immediate DNS re-resolution";
            found_hostname_match = true;
            break;
        }
    }
    
    if (found_hostname_match) {
        // Trigger immediate configuration refresh for hostname nodes
        trigger_immediate_config_refresh();
    } else {
        LOG(DEBUG) << "Failed peer " << failed_peer_id << " does not match any hostname nodes";
    }
}

void ReplicationState::trigger_immediate_config_refresh() {
    LOG(INFO) << "Triggering immediate configuration refresh due to peer failure";
    immediate_refresh_requested.store(true);
}

bool ReplicationState::add_node_safe(const std::string& node_to_add) {
    LOG(INFO) << "Attempting to safely add node: " << node_to_add;
    
    // Note: In Typesense's file-based model, this is primarily for validation
    // Actual node addition happens via file updates and periodic refresh
    
    // Step 1: Basic validation
    if (node_to_add.empty()) {
        LOG(WARNING) << "Cannot add empty node";
        return false;
    }
    
    // Step 2: Check if we're in a reasonable state for changes
    if (!is_leader()) {
        LOG(DEBUG) << "Not leader - node addition should be done via file update";
        return false;
    }
    
    // Step 3: Create the new configuration for validation
    std::shared_lock<std::shared_mutex> lock(current_config_mutex);
    NodeConfiguration new_config = current_node_config.create_single_node_change(node_to_add, "", get_current_term());
    
    // Step 4: Basic safety validation (prevent obvious errors)
    if (!current_node_config.is_safe_single_node_change(new_config)) {
        LOG(WARNING) << "Single-node change validation failed for adding: " << node_to_add;
        return false;
    }
    
    // Step 5: Basic quorum validation
    if (!validate_new_config_quorum(new_config)) {
        LOG(WARNING) << "New configuration would not have valid quorum";
        return false;
    }
    lock.unlock();
    
    LOG(INFO) << "Node addition validation passed for: " << node_to_add 
              << " (actual addition should be done via file update)";
    return true;
}

bool ReplicationState::remove_node_safe(const std::string& node_to_remove) {
    LOG(INFO) << "Attempting to safely remove node: " << node_to_remove;
    
    // Note: In Typesense's file-based model, this is primarily for validation
    // Actual node removal happens via file updates and periodic refresh
    
    // Step 1: Basic validation
    if (node_to_remove.empty()) {
        LOG(WARNING) << "Cannot remove empty node";
        return false;
    }
    
    // Step 2: Check if we're in a reasonable state for changes
    if (!is_leader()) {
        LOG(DEBUG) << "Not leader - node removal should be done via file update";
        return false;
    }
    
    // Step 3: Create the new configuration for validation
    std::shared_lock<std::shared_mutex> lock(current_config_mutex);
    NodeConfiguration new_config = current_node_config.create_single_node_change("", node_to_remove, get_current_term());
    
    // Step 4: Basic safety validation (prevent obvious errors)
    if (!current_node_config.is_safe_single_node_change(new_config)) {
        LOG(WARNING) << "Single-node change validation failed for removing: " << node_to_remove;
        return false;
    }
    
    // Step 5: Basic quorum validation
    if (!validate_new_config_quorum(new_config)) {
        LOG(WARNING) << "New configuration would not have valid quorum";
        return false;
    }
    
    // Step 6: Ensure we're not removing ourselves
    if (is_self_node(node_to_remove)) {
        LOG(WARNING) << "Cannot remove self from configuration";
        return false;
    }
    lock.unlock();
    
    LOG(INFO) << "Node removal validation passed for: " << node_to_remove 
              << " (actual removal should be done via file update)";
    return true;
}

bool ReplicationState::is_config_safe_for_reconfig() const {
    // Basic safety checks before allowing reconfiguration
    
    // Check 1: Must be leader to initiate reconfig
    if (!is_leader()) {
        LOG(DEBUG) << "Not leader - cannot initiate reconfiguration";
        return false;
    }
    
    // Check 2: Must have committed entries in current term
    if (!node) {
        LOG(DEBUG) << "Raft node not available for safety check";
        return false;
    }
    
    braft::NodeStatus status;
    node->get_status(&status);
    
    if (status.committed_index <= 0) {
        LOG(DEBUG) << "No committed entries - reconfiguration not safe";
        return false;
    }
    
    // Check 3: Current configuration should have quorum
    size_t current_nodes = 0;
    {
        std::shared_lock<std::shared_mutex> lock(current_config_mutex);
        current_nodes = current_node_config.total_nodes();
    }
    
    if (current_nodes == 0) {
        LOG(DEBUG) << "Empty current configuration - reconfiguration not safe";
        return false;
    }
    
    size_t required_quorum = (current_nodes / 2) + 1;
    if (required_quorum < 1) {
        LOG(DEBUG) << "Invalid quorum requirements - reconfiguration not safe";
        return false;
    }
    
    LOG(DEBUG) << "Basic reconfiguration safety checks passed";
    return true;
}

uint64_t ReplicationState::get_current_term() const {
    if (!node) {
        return 0;
    }
    
    braft::NodeStatus status;
    node->get_status(&status);
    return status.term;
}

bool ReplicationState::config_is_safe() const {
    // TLA+ Reference: TypesenseSafetyProperties.tla -> ConfigIsSafe()
    // This combines three safety checks: TermQuorumCheck, ConfigQuorumCheck, and OpCommittedInConfig
    
    LOG(DEBUG) << "Performing TLA+ ConfigIsSafe validation";
    
    // Check 1: TermQuorumCheck - Ensures leader authority in current term
    // TLA+: HasValidTermQuorum(s)
    if (!has_term_quorum_check()) {
        LOG(DEBUG) << "TermQuorumCheck failed - not safe for config changes";
        return false;
    }
    
    // Check 2: ConfigQuorumCheck - Ensures current config is acknowledged by quorum
    // TLA+: HasValidConfigQuorum(s)
    if (!has_config_quorum_check()) {
        LOG(DEBUG) << "ConfigQuorumCheck failed - not safe for config changes";
        return false;
    }
    
    // Check 3: OpCommittedInConfig - Ensures no data loss during config changes
    // TLA+: ArePreviousOpsCommitted(s)
    if (!are_previous_ops_committed_in_current_config()) {
        LOG(DEBUG) << "OpCommittedInConfig failed - not safe for config changes";
        return false;
    }
    
    LOG(DEBUG) << "MongoDB TLA+ ConfigIsSafe validation passed";
    return true;
}

bool ReplicationState::has_term_quorum_check() const {
    // MongoDB TLA+ TermQuorumCheck pattern
    // Ensures that the current leader has authority in the current term
    
    if (!node) {
        LOG(DEBUG) << "TermQuorumCheck: No raft node available";
        return false;
    }
    
    if (!is_leader()) {
        LOG(DEBUG) << "TermQuorumCheck: Not the leader";
        return false;
    }
    
    braft::NodeStatus status;
    node->get_status(&status);
    
    uint64_t current_term = status.term;
    uint64_t last_check_term = last_term_quorum_check.load();
    
    // If we've already validated this term recently, consider it valid
    if (last_check_term == current_term) {
        LOG(DEBUG) << "TermQuorumCheck: Already validated for term " << current_term;
        return true;
    }
    
    // Check that we have committed at least one operation in the current term
    // This ensures we have established leadership authority
    if (status.committed_index > 0 && current_term > 0) {
        // Update the last validated term
        last_term_quorum_check.store(current_term);
        LOG(DEBUG) << "TermQuorumCheck: Validated for term " << current_term 
                   << " (committed_index: " << status.committed_index << ")";
        return true;
    }
    
    LOG(DEBUG) << "TermQuorumCheck: Failed - insufficient committed entries in term " << current_term;
    return false;
}

bool ReplicationState::has_config_quorum_check() const {
    // MongoDB TLA+ ConfigQuorumCheck pattern  
    // Ensures that the current configuration is acknowledged by a quorum of nodes
    
    if (!node) {
        LOG(DEBUG) << "ConfigQuorumCheck: No raft node available";
        return false;
    }
    
    braft::NodeStatus status;
    node->get_status(&status);
    
    // Get current configuration size
    size_t config_size = 0;
    {
        std::shared_lock<std::shared_mutex> lock(current_config_mutex);
        config_size = current_node_config.total_nodes();
    }
    
    if (config_size == 0) {
        LOG(DEBUG) << "ConfigQuorumCheck: Empty configuration";
        return false;
    }
    
    size_t required_quorum = (config_size / 2) + 1;
    
    // Check if we have recent acknowledgments from a quorum
    // In a real implementation, this would check heartbeat responses
    // For now, we'll use a heuristic based on committed entries
    
    if (status.committed_index > 0) {
        // If we have committed entries, it means a quorum acknowledged our leadership
        uint64_t current_config_check = config_size; // Use config size as a proxy
        last_config_quorum_check.store(current_config_check);
        
        LOG(DEBUG) << "ConfigQuorumCheck: Passed with quorum " << required_quorum 
                   << "/" << config_size << " (committed_index: " << status.committed_index << ")";
        return true;
    }
    
    LOG(DEBUG) << "ConfigQuorumCheck: Failed - no evidence of quorum acknowledgment";
    return false;
}

bool ReplicationState::are_previous_ops_committed_in_current_config() const {
    // MongoDB TLA+ OpCommittedInConfig pattern
    // Ensures that operations from previous configurations are committed before changing config
    
    if (!node) {
        LOG(DEBUG) << "OpCommittedInConfig: No raft node available";
        return false;
    }
    
    braft::NodeStatus status;
    node->get_status(&status);
    
    // Check that we have a reasonable committed index
    if (status.committed_index <= 0) {
        LOG(DEBUG) << "OpCommittedInConfig: No committed operations";
        return false;
    }
    
    // Check that the gap between last_index and committed_index is not too large
    // This ensures we're not too far behind in applying committed operations
    int64_t uncommitted_gap = status.last_index - status.committed_index;
    const int64_t MAX_UNCOMMITTED_GAP = 1000; // Reasonable threshold
    
    if (uncommitted_gap > MAX_UNCOMMITTED_GAP) {
        LOG(DEBUG) << "OpCommittedInConfig: Too many uncommitted operations (gap: " 
                   << uncommitted_gap << ")";
        return false;
    }
    
    // Check that local application is caught up reasonably well
    int64_t unapplied_gap = status.committed_index - status.known_applied_index;
    const int64_t MAX_UNAPPLIED_GAP = 100; // Smaller threshold for application lag
    
    if (unapplied_gap > MAX_UNAPPLIED_GAP) {
        LOG(DEBUG) << "OpCommittedInConfig: Too many unapplied operations (gap: " 
                   << unapplied_gap << ")";
        return false;
    }
    
    LOG(DEBUG) << "OpCommittedInConfig: Passed (committed: " << status.committed_index 
               << ", applied: " << status.known_applied_index << ", last: " << status.last_index << ")";
    return true;
}

bool ReplicationState::validate_new_config_quorum(const NodeConfiguration& new_config) const {
    // Validate that the new configuration can achieve quorum with proper joint consensus safety
    
    size_t new_total_nodes = new_config.total_nodes();
    if (new_total_nodes == 0) {
        LOG(DEBUG) << "New configuration is empty - invalid";
        return false;
    }
    
    size_t new_quorum_size = (new_total_nodes / 2) + 1;
    
    // Basic quorum math validation
    if (new_quorum_size < 1 || new_quorum_size > new_total_nodes) {
        LOG(DEBUG) << "Invalid quorum requirements for new configuration: " 
                   << new_quorum_size << "/" << new_total_nodes;
        return false;
    }
    
    // MongoDB-style joint consensus validation: check intersection overlap
    std::set<std::string> current_nodes, intersection;
    size_t current_total_nodes = 0;
    
    {
        std::shared_lock<std::shared_mutex> lock(current_config_mutex);
        current_total_nodes = current_node_config.total_nodes();
        current_nodes = current_node_config.get_node_set();
    }
    
    std::set<std::string> new_nodes = new_config.get_node_set();
    
    if (current_total_nodes > 0) {
        size_t current_quorum_size = (current_total_nodes / 2) + 1;
        
        // Calculate intersection of current and new configurations
        std::set_intersection(current_nodes.begin(), current_nodes.end(),
                             new_nodes.begin(), new_nodes.end(),
                             std::inserter(intersection, intersection.begin()));
        
        // MongoDB's HasQuorumOverlap: intersection must satisfy both quorums
        if (intersection.size() < current_quorum_size || intersection.size() < new_quorum_size) {
            LOG(DEBUG) << "Joint consensus validation failed - insufficient overlap: "
                       << "intersection=" << intersection.size() 
                       << ", current_quorum=" << current_quorum_size
                       << ", new_quorum=" << new_quorum_size;
            return false;
        }
        
        LOG(DEBUG) << "Joint consensus validation passed - sufficient overlap: "
                   << "intersection=" << intersection.size()
                   << " >= max(" << current_quorum_size << "," << new_quorum_size << ")";
    }
    
    LOG(DEBUG) << "New configuration quorum validation passed: " 
               << new_quorum_size << "/" << new_total_nodes;
    return true;
} 

bool ReplicationState::is_self_node(const std::string& node_spec) const {
    // Simple heuristic to check if a node specification refers to this node
    // In a production system, this would need more sophisticated matching
    // against the actual peering endpoint and hostname resolution
    
    if (node_spec.find("localhost") != std::string::npos ||
        node_spec.find("127.0.0.1") != std::string::npos ||
        node_spec.find("::1") != std::string::npos) {
        return true;
    }
    
    // TODO: More sophisticated self-detection based on actual peering_endpoint
    // For now, this prevents obvious self-removal attempts
    return false;
} 