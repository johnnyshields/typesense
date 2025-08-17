#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <butil/endpoint.h>
#include <brpc/server.h>

class RaftServer;
class Store;

class RaftServerManager {
private:
        // Server components
    brpc::Server peering_server;

    // Configuration
    std::string path_to_nodes;
    butil::EndPoint peering_endpoint;
    uint32_t api_port;
    int snapshot_interval_seconds;
    int snapshot_max_byte_count_per_rpc;
    const std::atomic<bool>* reset_peers_on_error;

    // Dependencies
    RaftServer* raft_server;

    // Monitoring intervals (in counter ticks, where each tick = 1 second)
    static constexpr size_t PEER_CONFIG_REFRESH_INTERVAL = 10;    // every 10 seconds
    static constexpr size_t CATCHUP_STATUS_REFRESH_INTERVAL = 3;  // every 3 seconds
    static constexpr size_t SNAPSHOT_INTERVAL = 60;              // every 60 seconds
    static constexpr size_t CATCHUP_LOG_INTERVAL = 9;            // log catchup every 9 seconds

    RaftServerManager() = default;
    ~RaftServerManager() = default;

public:
    static RaftServerManager& get_instance() {
        static RaftServerManager instance;
        return instance;
    }

    RaftServerManager(const RaftServerManager&) = delete;
    RaftServerManager(RaftServerManager&&) = delete;
    RaftServerManager& operator=(const RaftServerManager&) = delete;
    RaftServerManager& operator=(RaftServerManager&&) = delete;

            // Main server lifecycle method - blocks until shutdown
    int start_server(RaftServer& raft_server, Store& store,
                     const std::string& state_dir, const std::string& path_to_nodes,
                     const std::string& peering_address, uint32_t peering_port,
                     const std::string& peering_subnet, uint32_t api_port,
                     int snapshot_interval_seconds, int snapshot_max_byte_count_per_rpc,
                     const std::atomic<bool>& reset_peers_on_error,
                     const std::atomic<bool>& quit_service);

private:
    // Helper methods
    butil::EndPoint resolve_peering_endpoint(const std::string& peering_address,
                                            uint32_t peering_port,
                                            const std::string& peering_subnet);

    int setup_peering_server(const butil::EndPoint& endpoint);

    int initialize_raft_server(RaftServer& raft_server,
                               const butil::EndPoint& endpoint,
                               uint32_t api_port,
                               int snapshot_max_byte_count_per_rpc,
                               const std::string& state_dir,
                               const std::string& nodes_config);

        void refresh_peer_configuration(size_t raft_counter);
    void refresh_catchup_status(size_t raft_counter);
};
