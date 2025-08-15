#include "raft_node_manager.h"
#include "raft_server.h"
#include "raft_config_manager.h"
#include "store.h"
#include <string_utils.h>
#include <http_client.h>
#include "core_api.h"
#include "config.h"
#include <logger.h>
#include <butil/files/file_path.h>

RaftNodeManager::RaftNodeManager(ReplicationState* state)
    : replication_state(state) {}

void RaftNodeManager::refresh_nodes(const std::string& nodes, 
                                   const size_t raft_counter,
                                   const std::atomic<bool>& reset_peers_on_error) {
    std::shared_lock lock(replication_state->get_node_mutex());

    braft::Node* node = replication_state->get_node();
    if(!node) {
        LOG(WARNING) << "Node state is not initialized: unable to refresh nodes.";
        return;
    }

    braft::Configuration new_conf;
    new_conf.parse_from(nodes);

    braft::NodeStatus nodeStatus;
    node->get_status(&nodeStatus);

    LOG(INFO) << "Term: " << nodeStatus.term
              << ", pending_queue: " << nodeStatus.pending_queue_size
              << ", last_index: " << nodeStatus.last_index
              << ", committed: " << nodeStatus.committed_index
              << ", known_applied: " << nodeStatus.known_applied_index
              << ", applying: " << nodeStatus.applying_index
              << ", pending_writes: " << replication_state->get_pending_writes()
              << ", queued_writes: " << replication_state->get_batched_indexer()->get_queued_writes()
              << ", local_sequence: " << replication_state->get_store()->get_latest_seq_number();

    if(node->is_leader()) {
        RefreshNodesClosure* refresh_nodes_done = new RefreshNodesClosure;
        node->change_peers(new_conf, refresh_nodes_done);
    } else {
        if(node->leader_id().is_empty()) {
            // When node is not a leader, does not have a leader and is also a single-node cluster,
            // we forcefully reset its peers.
            // NOTE: `reset_peers()` is not a safe call to make as we give up on consistency and consensus guarantees.
            // We are doing this solely to handle single node cluster whose IP changes.
            // Examples: Docker container IP change, local DHCP leased IP change etc.

            std::vector<braft::PeerId> latest_nodes;
            new_conf.list_peers(&latest_nodes);

            if(latest_nodes.size() == 1 || (raft_counter > 0 && reset_peers_on_error)) {
                LOG(WARNING) << "Node with no leader. Resetting peers of size: " << latest_nodes.size();
                node->reset_peers(new_conf);
            } else {
                LOG(WARNING) << "Multi-node with no leader: refusing to reset peers.";
            }

            return;
        }
    }
}

void RaftNodeManager::refresh_catchup_status(bool log_msg) {
    std::shared_lock lock(replication_state->get_node_mutex());
    
    braft::Node* node = replication_state->get_node();
    if(node == nullptr) {
        replication_state->get_read_caught_up() = false;
        replication_state->get_write_caught_up() = false;
        return;
    }

    bool is_leader = node->is_leader();
    bool leader_or_follower = (is_leader || !node->leader_id().is_empty());
    if(!leader_or_follower) {
        replication_state->get_read_caught_up() = false;
        replication_state->get_write_caught_up() = false;
        return;
    }

    braft::NodeStatus n_status;
    node->get_status(&n_status);
    lock.unlock();

    // `known_applied_index` guaranteed to be atleast 1 if raft log is available (after snapshot loading etc.)
    if(n_status.known_applied_index == 0) {
        LOG_IF(ERROR, log_msg) << "Node not ready yet (known_applied_index is 0).";
        replication_state->get_read_caught_up() = false;
        replication_state->get_write_caught_up() = false;
        return;
    }

    // work around for: https://github.com/baidu/braft/issues/277#issuecomment-823080171
    int64_t current_index = (n_status.applying_index == 0) ? n_status.known_applied_index : n_status.applying_index;
    int64_t apply_lag = n_status.last_index - current_index;

    // in addition to raft level lag, we should also account for internal batched write queue
    int64_t num_queued_writes = replication_state->get_batched_indexer()->get_queued_writes();

    int healthy_read_lag = replication_state->get_config()->get_healthy_read_lag();
    int healthy_write_lag = replication_state->get_config()->get_healthy_write_lag();

    if (apply_lag > healthy_read_lag) {
        LOG_IF(ERROR, log_msg) << apply_lag << " lagging entries > healthy read lag of " << healthy_read_lag;
        replication_state->get_read_caught_up() = false;
    } else {
        if(num_queued_writes > healthy_read_lag) {
            LOG_IF(ERROR, log_msg) << num_queued_writes << " queued writes > healthy read lag of " << healthy_read_lag;
            replication_state->get_read_caught_up() = false;
        } else {
            replication_state->get_read_caught_up() = true;
        }
    }

    if (apply_lag > healthy_write_lag) {
        LOG_IF(ERROR, log_msg) << apply_lag << " lagging entries > healthy write lag of " << healthy_write_lag;
        replication_state->get_write_caught_up() = false;
    } else {
        if(num_queued_writes > healthy_write_lag) {
            LOG_IF(ERROR, log_msg) << num_queued_writes << " queued writes > healthy write lag of " << healthy_write_lag;
            replication_state->get_write_caught_up() = false;
        } else {
            replication_state->get_write_caught_up() = true;
        }
    }

    if(is_leader || !replication_state->get_read_caught_up()) {
        // no need to re-check status with leader
        return;
    }

    lock.lock();
    node = replication_state->get_node();

    if(node->leader_id().is_empty()) {
        LOG(ERROR) << "Could not get leader status, as node does not have a leader!";
        return;
    }

    const braft::PeerId& leader_addr = node->leader_id();
    lock.unlock();

    const std::string protocol = replication_state->get_api_uses_ssl() ? "https" : "http";
    std::string url = get_node_url_path(leader_addr, "/status", protocol);

    std::string api_res;
    std::map<std::string, std::string> res_headers;
    long status_code = HttpClient::get_response(url, api_res, res_headers, {}, 5*1000, true);
    if(status_code == 200) {
        // compare leader's applied log with local applied to see if we are lagging
        nlohmann::json leader_status = nlohmann::json::parse(api_res);
        if(leader_status.contains("committed_index")) {
            int64_t leader_committed_index = leader_status["committed_index"].get<int64_t>();
            if(leader_committed_index <= n_status.committed_index) {
                // this can happen due to network latency in making the /status call
                // we will refrain from changing current status
                return;
            }
            replication_state->get_read_caught_up() = ((leader_committed_index - n_status.committed_index) < healthy_read_lag);
        } else {
            // we will refrain from changing current status
            LOG(ERROR) << "Error, `committed_index` key not found in /status response from leader.";
        }
    } else {
        // we will again refrain from changing current status
        LOG(ERROR) << "Error, /status end-point returned bad status code " << status_code;
    }
}

bool RaftNodeManager::is_alive() const {
    // for general health check we will only care about the `read_caught_up` threshold
    return replication_state->get_read_caught_up();
}

uint64_t RaftNodeManager::node_state() const {
    std::shared_lock lock(replication_state->get_node_mutex());

    braft::Node* node = replication_state->get_node();
    if(node == nullptr) {
        return 0;
    }

    braft::NodeStatus node_status;
    node->get_status(&node_status);

    return node_status.state;
}

bool RaftNodeManager::trigger_vote() {
    std::shared_lock lock(replication_state->get_node_mutex());

    braft::Node* node = replication_state->get_node();
    if(node) {
        auto status = node->vote(replication_state->get_election_timeout_interval_ms());
        LOG(INFO) << "Triggered vote. Ok? " << status.ok() << ", status: " << status;
        return status.ok();
    }

    return false;
}

bool RaftNodeManager::reset_peers() {
    std::shared_lock lock(replication_state->get_node_mutex());

    braft::Node* node = replication_state->get_node();
    if(node) {
        const Option<std::string>& refreshed_nodes_op = Config::fetch_nodes_config(
            replication_state->get_config()->get_nodes()
        );
        if(!refreshed_nodes_op.ok()) {
            LOG(WARNING) << "Error while fetching peer configuration: " << refreshed_nodes_op.error();
            return false;
        }

        const std::string& nodes_config = RaftConfigManager::to_nodes_config(
            replication_state->get_peering_endpoint(),
            replication_state->get_config()->get_api_port(),
            refreshed_nodes_op.get()
        );

        if(nodes_config.empty()) {
            LOG(WARNING) << "No nodes resolved from peer configuration.";
            return false;
        }

        braft::Configuration peer_config;
        peer_config.parse_from(nodes_config);

        std::vector<braft::PeerId> peers;
        peer_config.list_peers(&peers);

        auto status = node->reset_peers(peer_config);
        LOG(INFO) << "Reset peers. Ok? " << status.ok() << ", status: " << status;
        LOG(INFO) << "New peer config is: " << peer_config;
        return status.ok();
    }

    return false;
}

int64_t RaftNodeManager::get_num_queued_writes() {
    return replication_state->get_batched_indexer()->get_queued_writes();
}

bool RaftNodeManager::is_leader() {
    std::shared_lock lock(replication_state->get_node_mutex());

    braft::Node* node = replication_state->get_node();
    if(!node) {
        return false;
    }

    return node->is_leader();
}

nlohmann::json RaftNodeManager::get_status() {
    nlohmann::json status;

    std::shared_lock lock(replication_state->get_node_mutex());
    braft::Node* node = replication_state->get_node();
    
    if(!node) {
        // `node` is not yet initialized (probably loading snapshot)
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
    status["queued_writes"] = replication_state->get_batched_indexer()->get_queued_writes();

    return status;
}

std::string RaftNodeManager::get_leader_url() const {
    std::shared_lock lock(replication_state->get_node_mutex());

    braft::Node* node = replication_state->get_node();
    if(!node) {
        LOG(ERROR) << "Could not get leader url as node is not initialized!";
        return "";
    }

    if(node->leader_id().is_empty()) {
        LOG(ERROR) << "Could not get leader url, as node does not have a leader!";
        return "";
    }

    const braft::PeerId& leader_addr = node->leader_id();
    lock.unlock();

    const std::string protocol = replication_state->get_api_uses_ssl() ? "https" : "http";
    return get_node_url_path(leader_addr, "/", protocol);
}

void RaftNodeManager::persist_applying_index() {
    std::shared_lock lock(replication_state->get_node_mutex());

    braft::Node* node = replication_state->get_node();
    if(node == nullptr) {
        return;
    }

    lock.unlock();

    replication_state->get_batched_indexer()->persist_applying_index();
}

void RaftNodeManager::decr_pending_writes() {
    replication_state->get_pending_writes()--;
}

void RaftNodeManager::do_snapshot(const std::string& snapshot_path, 
                                 const std::shared_ptr<http_req>& req,
                                 const std::shared_ptr<http_res>& res) {
    braft::Node* node = replication_state->get_node();
    if(node == nullptr) {
        res->set_500("Could not trigger a snapshot, as node is not initialized.");
        auto req_res = new async_req_res_t(req, res, true);
        replication_state->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        return;
    }

    if(replication_state->get_snapshot_in_progress()) {
        res->set_409("Another snapshot is in progress.");
        auto req_res = new async_req_res_t(req, res, true);
        replication_state->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        return;
    }

    LOG(INFO) << "Triggering an on demand snapshot"
              << (!snapshot_path.empty() ? " with external snapshot path..." : "...");

    replication_state->get_thread_pool()->enqueue([snapshot_path, req, res, this]() {
        OnDemandSnapshotClosure* snapshot_closure = new OnDemandSnapshotClosure(
            replication_state, req, res, snapshot_path, replication_state->get_raft_dir_path()
        );
        replication_state->get_ext_snapshot_path() = snapshot_path;
        std::shared_lock lock(replication_state->get_node_mutex());
        braft::Node* node = replication_state->get_node();
        node->snapshot(snapshot_closure);
    });
}

void RaftNodeManager::do_snapshot(const std::string& nodes) {
    auto current_ts = std::time(nullptr);
    if(current_ts - replication_state->get_last_snapshot_ts() < replication_state->get_snapshot_interval_s()) {
        return;
    }

    LOG(INFO) << "Snapshot timer is active, current_ts: " << current_ts 
              << ", last_snapshot_ts: " << replication_state->get_last_snapshot_ts();

    if(is_leader()) {
        // run the snapshot only if there are no other recovering followers
        std::vector<braft::PeerId> peers;
        braft::Configuration peer_config;
        peer_config.parse_from(nodes);
        peer_config.list_peers(&peers);

        std::shared_lock lock(replication_state->get_node_mutex());
        braft::Node* node = replication_state->get_node();
        std::string my_addr = node->node_id().peer_id.to_string();
        lock.unlock();

        bool all_peers_healthy = true;

        // iterate peers and check health status
        for(const auto& peer: peers) {
            const std::string& peer_addr = peer.to_string();

            if(my_addr == peer_addr) {
                // skip self
                continue;
            }

            const std::string protocol = replication_state->get_api_uses_ssl() ? "https" : "http";
            std::string url = get_node_url_path(peer, "/health", protocol);
            std::string api_res;
            std::map<std::string, std::string> res_headers;
            long status_code = HttpClient::get_response(url, api_res, res_headers, {}, 5*1000, true);
            bool peer_healthy = (status_code == 200);

            if(!peer_healthy) {
                LOG(WARNING) << "Peer " << peer_addr << " reported unhealthy during snapshot pre-check.";
            }

            all_peers_healthy = all_peers_healthy && peer_healthy;
        }

        if(!all_peers_healthy) {
            LOG(WARNING) << "Unable to trigger snapshot as one or more of the peers reported unhealthy.";
            return;
        }
    }

    TimedSnapshotClosure* snapshot_closure = new TimedSnapshotClosure(replication_state);
    std::shared_lock lock(replication_state->get_node_mutex());
    braft::Node* node = replication_state->get_node();
    node->snapshot(snapshot_closure);
    replication_state->get_last_snapshot_ts() = current_ts;
}

void RaftNodeManager::do_dummy_write() {
    std::shared_lock lock(replication_state->get_node_mutex());

    braft::Node* node = replication_state->get_node();
    if(!node || node->leader_id().is_empty()) {
        LOG(ERROR) << "Could not do a dummy write, as node does not have a leader";
        return;
    }

    const std::string& leader_addr = node->leader_id().to_string();
    lock.unlock();

    const std::string protocol = replication_state->get_api_uses_ssl() ? "https" : "http";
    std::string url = get_node_url_path(leader_addr, "/health", protocol);

    std::string api_res;
    std::map<std::string, std::string> res_headers;
    long status_code = HttpClient::post_response(url, "", api_res, res_headers, {}, 4000, true);

    LOG(INFO) << "Dummy write to " << url << ", status = " << status_code << ", response = " << api_res;
}

void RaftNodeManager::set_ext_snapshot_path(const std::string& snapshot_path) {
    replication_state->get_ext_snapshot_path() = snapshot_path;
}

void RaftNodeManager::set_snapshot_in_progress(bool snapshot_in_progress) {
    replication_state->get_snapshot_in_progress() = snapshot_in_progress;
}

std::string RaftNodeManager::get_node_url_path(const braft::PeerId& peer_id, 
                                              const std::string& path,
                                              const std::string& protocol) const {
    const std::string endpoint_str = butil::endpoint2str(peer_id.addr).c_str();
    const size_t last_colon = endpoint_str.rfind(':');
    if (last_colon == std::string::npos) {
        LOG(ERROR) << "Invalid endpoint format: " << endpoint_str;
        return "";
    }

    const std::string ip_part = endpoint_str.substr(0, last_colon);

    std::string url = protocol + "://";
    url += ip_part;
    url += ":";
    url += std::to_string(peer_id.idx);

    if(!path.empty()) {
        if(path[0] == '/') {
            url += path;
        } else {
            url += "/" + path;
        }
    }

    return url;
}
