#include "raft_server_manager.h"
#include "raft_server.h"
#include "raft_config.h"
#include "tsconfig.h"
#include "logger.h"
#include "store.h"
#include "typesense_server_utils.h"
#include <braft/raft.h>
#include <brpc/server.h>
#include <unistd.h>

extern std::atomic<bool> quit_raft_service;

int RaftServerManager::start_server(RaftServer& server, Store& store,
                                   const std::string& state_dir, const std::string& nodes_path,
                                   const std::string& peering_address, uint32_t peering_port,
                                   const std::string& peering_subnet, uint32_t port,
                                   int interval_seconds, int max_byte_count_per_rpc,
                                   const std::atomic<bool>& reset_peers_flag,
                                   const std::atomic<bool>& quit_service) {

    // Store configuration
    raft_server = &server;
    path_to_nodes = nodes_path;
    api_port = port;
    snapshot_interval_seconds = interval_seconds;
    snapshot_max_byte_count_per_rpc = max_byte_count_per_rpc;
    reset_peers_on_error = &reset_peers_flag;

    if (nodes_path.empty()) {
        LOG(INFO) << "Since no --nodes argument is provided, starting a single node Typesense cluster.";
    }

    // Fetch and validate node configuration
    const Option<std::string>& nodes_config_op = Config::fetch_nodes_config(nodes_path);
    if (!nodes_config_op.ok()) {
        LOG(ERROR) << nodes_config_op.error();
        return -1;
    }

    // Resolve peering endpoint
    peering_endpoint = resolve_peering_endpoint(peering_address, peering_port, peering_subnet);

    // Setup peering server
    int setup_result = setup_peering_server(peering_endpoint);
    if (setup_result != 0) {
        return setup_result;
    }

    // Initialize raft server
    int raft_result = initialize_raft_server(server, peering_endpoint, port,
                                           max_byte_count_per_rpc, state_dir,
                                           nodes_config_op.get());
    if (raft_result != 0) {
        peering_server.Stop(0);
        peering_server.Join();
        return raft_result;
    }

    LOG(INFO) << "Typesense peering service is running on " << peering_server.listen_address();
    LOG(INFO) << "Snapshot interval configured as: " << interval_seconds << "s";
    LOG(INFO) << "Snapshot max byte count configured as: " << max_byte_count_per_rpc;

    // Run monitoring loop synchronously - blocks until shutdown
    LOG(INFO) << "Raft monitoring started";

    size_t raft_counter = 0;
    while (!brpc::IsAskedToQuit() && !quit_service.load()) {
        // Refresh peer configuration every 10 seconds
        if (raft_counter % PEER_CONFIG_REFRESH_INTERVAL == 0) {
            refresh_peer_configuration(raft_counter);
        }

        // Refresh catchup status every 3 seconds
        if (raft_counter % CATCHUP_STATUS_REFRESH_INTERVAL == 0) {
            refresh_catchup_status(raft_counter);
        }

        raft_counter++;
        sleep(1);
    }

    LOG(INFO) << "Raft monitoring stopped";

    // Cleanup on shutdown
    LOG(INFO) << "Typesense peering service is going to quit.";

    // Stop application before server
    if (raft_server) {
        raft_server->shutdown();
    }

    // Stop peering server
    LOG(INFO) << "peering_server.stop()";
    peering_server.Stop(0);

    LOG(INFO) << "peering_server.join()";
    peering_server.Join();

    LOG(INFO) << "Typesense peering service has quit.";

    return 0;
}

butil::EndPoint RaftServerManager::resolve_peering_endpoint(const std::string& peering_address,
                                                          uint32_t peering_port,
                                                          const std::string& peering_subnet) {
    butil::EndPoint endpoint;
    int ip_conv_status = 0;

    if (!peering_address.empty()) {
        // If IPv6 address and not already wrapped in [], wrap it
        std::string normalized_addr = peering_address;
        if (peering_address.find(':') != std::string::npos &&
           peering_address.front() != '[' && peering_address.back() != ']') {
            normalized_addr = "[" + peering_address + "]";
        }

        ip_conv_status = butil::str2endpoint(normalized_addr.c_str(), peering_port, &endpoint);

        if (ip_conv_status != 0) {
            LOG(ERROR) << "Failed to parse peering address `" << normalized_addr << "`";
            // Return invalid endpoint - caller should handle
        }
    } else {
        endpoint = get_internal_endpoint(peering_subnet, peering_port);
    }

    return endpoint;
}

int RaftServerManager::setup_peering_server(const butil::EndPoint& endpoint) {
    if (braft::add_service(&peering_server, endpoint) != 0) {
        LOG(ERROR) << "Failed to add peering service";
        return -1;
    }

    if (peering_server.Start(endpoint, nullptr) != 0) {
        LOG(ERROR) << "Failed to start peering service";
        return -1;
    }

    return 0;
}

int RaftServerManager::initialize_raft_server(RaftServer& server,
                                             const butil::EndPoint& endpoint,
                                             uint32_t port,
                                             int max_byte_count_per_rpc,
                                             const std::string& state_dir,
                                             const std::string& nodes_config) {
    size_t election_timeout_ms = 5000;

    if (server.start(endpoint, port, election_timeout_ms, max_byte_count_per_rpc, state_dir,
                     nodes_config, quit_raft_service) != 0) {
        LOG(ERROR) << "Failed to start peering state";
        return -1;
    }

    return 0;
}

void RaftServerManager::refresh_peer_configuration(size_t raft_counter) {
    // Reset peer configuration periodically to identify change in cluster membership
    const Option<std::string>& refreshed_nodes_op = Config::fetch_nodes_config(path_to_nodes);

    if (!refreshed_nodes_op.ok()) {
        LOG(WARNING) << "Error while refreshing peer configuration: " << refreshed_nodes_op.error();
        return;
    }

    const std::string& nodes_config = raft::config::to_nodes_config(peering_endpoint, api_port,
                                                                    refreshed_nodes_op.get());
    if (nodes_config.empty()) {
        LOG(WARNING) << "No nodes resolved from peer configuration.";
        return;
    }

    if (raft_server) {
        raft_server->refresh_nodes(nodes_config, raft_counter, *reset_peers_on_error);

        // Perform snapshot every 60 seconds
        if (raft_counter % SNAPSHOT_INTERVAL == 0) {
            raft_server->do_snapshot(nodes_config);
        }
    }
}

void RaftServerManager::refresh_catchup_status(size_t raft_counter) {
    if (!raft_server) {
        return;
    }

    // Update node catch up status periodically, take care not to log too verbosely
    bool log_msg = (raft_counter % CATCHUP_LOG_INTERVAL == 0);
    raft_server->refresh_catchup_status(log_msg);
}
