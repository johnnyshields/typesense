#include "raft_node_manager.h"
#include "raft_config.h"
#include "store.h"
#include "batched_indexer.h"
#include "config.h"
#include <http_client.h>
#include <logger.h>
#include <thread>
#include <chrono>

RaftNodeManager::RaftNodeManager(const Config* config, 
                               Store* store,
                               BatchedIndexer* batched_indexer,
                               bool api_uses_ssl)
    : node(nullptr), config(config), store(store), 
      batched_indexer(batched_indexer), api_uses_ssl(api_uses_ssl),
      leader_term(-1), read_caught_up(false), write_caught_up(false) {}

RaftNodeManager::~RaftNodeManager() {
    if (node) {
        shutdown();
    }
}

int RaftNodeManager::init_node(braft::StateMachine* fsm,
                              const butil::EndPoint& peering_endpoint,
                              int api_port,
                              int election_timeout_ms,
                              const std::string& raft_dir,
                              const std::string& nodes) {
    this->peering_endpoint = peering_endpoint;
    this->api_port = api_port;
    this->election_timeout_ms = election_timeout_ms;
    
    butil::ip_t ip;
    if (butil::str2ip(butil::endpoint2str(peering_endpoint).c_str(), &ip) < 0) {
        LOG(ERROR) << "Invalid peering endpoint: " << butil::endpoint2str(peering_endpoint).c_str();
        return -1;
    }

    braft::NodeOptions node_options;
    std::string nodes_config = raft_config::to_nodes_config(peering_endpoint, api_port, nodes);
    
    if (node_options.initial_conf.parse_from(nodes_config) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << nodes << "'";
        return -1;
    }

    node_options.election_timeout_ms = election_timeout_ms;
    node_options.fsm = fsm;
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = 0; // manual snapshots
    node_options.log_uri = raft_dir + "/log";
    node_options.raft_meta_uri = raft_dir + "/raft_meta";
    node_options.snapshot_uri = raft_dir + "/snapshot";
    node_options.disable_cli = false;

    std::unique_lock lock(node_mutex);
    node = new braft::Node(braft::GroupId("ReplicationState"), braft::PeerId(peering_endpoint, 0));

    if (node->init(node_options) != 0) {
        LOG(ERROR) << "Fail to init raft node";
        delete node;
        node = nullptr;
        return -1;
    }
    
    return 0;
}

bool RaftNodeManager::wait_until_ready(int timeout_ms, const std::atomic<bool>& quit_signal) {
    auto begin_ts = std::chrono::high_resolution_clock::now();
    
    while(true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        
        auto current_ts = std::chrono::high_resolution_clock::now();
        auto time_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_ts - begin_ts).count();
        
        if(time_elapsed > timeout_ms) {
            LOG(ERROR) << "Raft state not ready even after " << time_elapsed << " ms. Stopping.";
            return false;
        }
        
        if(quit_signal.load()) {
            LOG(ERROR) << "Server is quitting abruptly.";
            return false;
        }
        
        std::shared_lock lock(node_mutex);
        if(!node) {
            LOG(ERROR) << "Node is null during wait";
            return false;
        }
        
        braft::NodeStatus status;
        node->get_status(&status);
        
        bool is_single_node = status.peer_manager.size() == 1;
        bool ready = is_single_node || node->is_leader() || !node->leader_id().is_empty();
        
        if(ready) {
            LOG(INFO) << "Raft node is now ready. single_node=" << is_single_node;
            return true;
        } else {
            LOG(INFO) << "Waiting for raft node to come online, time_elapsed=" << time_elapsed << " ms";
        }
    }
}

void RaftNodeManager::shutdown() {
    std::unique_lock lock(node_mutex);
    if (node) {
        LOG(INFO) << "Shutting down Raft node";
        node->shutdown(nullptr);
        node->join();
        delete node;
        node = nullptr;
    }
}

void RaftNodeManager::apply(braft::Task& task) {
    std::shared_lock lock(node_mutex);
    if (node && node->is_leader()) {
        node->apply(task);
    }
}

void RaftNodeManager::snapshot(braft::Closure* done) {
    std::shared_lock lock(node_mutex);
    if (node) {
        node->snapshot(done);
    }
}

void RaftNodeManager::change_peers(const braft::Configuration& new_conf, braft::Closure* done) {
    std::shared_lock lock(node_mutex);
    if (node && node->is_leader()) {
        node->change_peers(new_conf, done);
    }
}

butil::Status RaftNodeManager::reset_peers(const braft::Configuration& new_conf) {
    std::shared_lock lock(node_mutex);
    if (node) {
        return node->reset_peers(new_conf);
    }
    return butil::Status(EINVAL, "Node not initialized");
}

butil::Status RaftNodeManager::trigger_vote() {
    std::shared_lock lock(node_mutex);
    if (node) {
        return node->vote(election_timeout_ms);
    }
    return butil::Status(EINVAL, "Node not initialized");
}

void RaftNodeManager::get_status(braft::NodeStatus* status) const {
    std::shared_lock lock(node_mutex);
    if (node) {
        node->get_status(status);
    }
}

bool RaftNodeManager::is_leader() const {
    std::shared_lock lock(node_mutex);
    return node && node->is_leader();
}

braft::PeerId RaftNodeManager::leader_id() const {
    std::shared_lock lock(node_mutex);
    if (node) {
        return node->leader_id();
    }
    return braft::PeerId();
}

braft::NodeId RaftNodeManager::node_id() const {
    std::shared_lock lock(node_mutex);
    if (node) {
        return node->node_id();
    }
    return braft::NodeId();
}

void RaftNodeManager::refresh_catchup_status(bool log_msg) {
    std::shared_lock lock(node_mutex);
    
    if(!node) {
        read_caught_up = write_caught_up = false;
        return;
    }
    
    bool is_leader = node->is_leader();
    bool has_leader = !node->leader_id().is_empty();
    
    if(!is_leader && !has_leader) {
        read_caught_up = write_caught_up = false;
        return;
    }
    
    braft::NodeStatus n_status;
    node->get_status(&n_status);
    lock.unlock();
    
    if(n_status.known_applied_index == 0) {
        LOG_IF(ERROR, log_msg) << "Node not ready yet (known_applied_index is 0).";
        read_caught_up = write_caught_up = false;
        return;
    }
    
    // Calculate lag
    int64_t current_index = (n_status.applying_index == 0) ? 
                           n_status.known_applied_index : n_status.applying_index;
    int64_t apply_lag = n_status.last_index - current_index;
    int64_t num_queued_writes = batched_indexer->get_queued_writes();
    
    int healthy_read_lag = config->get_healthy_read_lag();
    int healthy_write_lag = config->get_healthy_write_lag();
    
    // Check read lag
    if (apply_lag > healthy_read_lag || num_queued_writes > healthy_read_lag) {
        LOG_IF(ERROR, log_msg) << "Read lag unhealthy: apply_lag=" << apply_lag 
                              << ", queued=" << num_queued_writes;
        read_caught_up = false;
    } else {
        read_caught_up = true;
    }
    
    // Check write lag
    if (apply_lag > healthy_write_lag || num_queued_writes > healthy_write_lag) {
        LOG_IF(ERROR, log_msg) << "Write lag unhealthy: apply_lag=" << apply_lag 
                              << ", queued=" << num_queued_writes;
        write_caught_up = false;
    } else {
        write_caught_up = true;
    }
    
    // For followers, check with leader
    if(!is_leader && read_caught_up) {
        check_leader_health(n_status);
    }
}

void RaftNodeManager::check_leader_health(const braft::NodeStatus& local_status) {
    std::shared_lock lock(node_mutex);
    
    if(!node || node->leader_id().is_empty()) {
        return;
    }
    
    const braft::PeerId& leader_addr = node->leader_id();
    lock.unlock();
    
    const std::string protocol = api_uses_ssl ? "https" : "http";
    std::string url = raft_config::get_node_url_path(leader_addr, "/status", protocol);
    
    std::string api_res;
    std::map<std::string, std::string> res_headers;
    long status_code = HttpClient::get_response(url, api_res, res_headers, {}, 5*1000, true);
    
    if(status_code == 200) {
        try {
            nlohmann::json leader_status = nlohmann::json::parse(api_res);
            if(leader_status.contains("committed_index")) {
                int64_t leader_committed = leader_status["committed_index"].get<int64_t>();
                if(leader_committed > local_status.committed_index) {
                    int lag = leader_committed - local_status.committed_index;
                    read_caught_up = (lag < config->get_healthy_read_lag());
                }
            }
        } catch(const std::exception& e) {
            LOG(ERROR) << "Failed to parse leader status: " << e.what();
        }
    }
}

nlohmann::json RaftNodeManager::get_status() const {
    nlohmann::json status;
    
    std::shared_lock lock(node_mutex);
    if(!node) {
        status["state"] = "NOT_READY";
        status["committed_index"] = 0;
        status["queued_writes"] = 0;
        return status;
    }
    
    braft::NodeStatus node_status;
    node->get_status(&node_status);
    lock.unlock();
    
    status["state"] = braft::state2str(node_status.state);
    status["committed_index"] = node_status.committed_index;
    status["queued_writes"] = batched_indexer->get_queued_writes();
    status["is_leader"] = is_leader();
    status["read_ready"] = read_caught_up.load();
    status["write_ready"] = write_caught_up.load();
    
    return status;
}

std::string RaftNodeManager::get_leader_url() const {
    std::shared_lock lock(node_mutex);
    
    if(!node || node->leader_id().is_empty()) {
        return "";
    }
    
    const braft::PeerId& leader_addr = node->leader_id();
    lock.unlock();
    
    const std::string protocol = api_uses_ssl ? "https" : "http";
    return raft_config::get_node_url_path(leader_addr, "/", protocol);
}

void RaftNodeManager::refresh_nodes(const std::string& nodes, bool allow_single_node_reset) {
    std::shared_lock lock(node_mutex);
    
    if(!node) {
        LOG(WARNING) << "Node state is not initialized: unable to refresh nodes.";
        return;
    }
    
    braft::Configuration new_conf;
    new_conf.parse_from(nodes);
    
    braft::NodeStatus nodeStatus;
    node->get_status(&nodeStatus);
    
    LOG(INFO) << "Refreshing nodes. Term: " << nodeStatus.term
              << ", last_index: " << nodeStatus.last_index
              << ", committed: " << nodeStatus.committed_index;
    
    if(node->is_leader()) {
        RefreshNodesClosure* done = new RefreshNodesClosure;
        node->change_peers(new_conf, done);
    } else if(node->leader_id().is_empty()) {
        // Handle single node IP change scenario
        std::vector<braft::PeerId> latest_nodes;
        new_conf.list_peers(&latest_nodes);
        
        if(latest_nodes.size() == 1 || allow_single_node_reset) {
            LOG(WARNING) << "Node with no leader. Resetting peers of size: " << latest_nodes.size();
            node->reset_peers(new_conf);
        } else {
            LOG(WARNING) << "Multi-node with no leader: refusing to reset peers.";
        }
    }
}

// Helper closure for refresh_nodes
class RefreshNodesClosure : public braft::Closure {
public:
    void Run() {
        std::unique_ptr<RefreshNodesClosure> self_guard(this);
        if(status().ok()) {
            LOG(INFO) << "Peer refresh succeeded!";
        } else {
            LOG(ERROR) << "Peer refresh failed, error: " << status().error_str();
        }
    }
};
