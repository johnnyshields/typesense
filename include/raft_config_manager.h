#pragma once

#include <string>
#include <butil/endpoint.h>

class Config;

/**
 * RaftConfigManager handles DNS resolution and node configuration management
 * for the Raft cluster. All methods are static as they don't require state.
 */
class RaftConfigManager {
public:
    /**
     * Resolves a hostname to an IP string.
     * 
     * @param hostname The hostname to resolve (max 64 characters)
     * @return Resolved IP address string. IPv6 addresses are wrapped in [].
     *         Returns empty string on failure or original hostname if already an IP.
     */
    static std::string hostname2ipstr(const std::string& hostname);

    /**
     * Resolves all node hostnames in a comma-separated configuration string.
     * Expected format: "host1:port1:port2,host2:port1:port2,..."
     * 
     * @param nodes_config Comma-separated list of node configurations
     * @return Resolved configuration string with IPs instead of hostnames.
     *         Returns empty string if all DNS resolutions fail.
     */
    static std::string resolve_node_hosts(const std::string& nodes_config);

    /**
     * Converts endpoint and API port to nodes configuration string.
     * 
     * @param peering_endpoint The peering endpoint for this node
     * @param api_port The API port for this node
     * @param nodes_config Existing nodes configuration (can be empty)
     * @return Formatted nodes configuration string with resolved IPs.
     *         Can return empty string if DNS resolution fails on all nodes.
     */
    static std::string to_nodes_config(const butil::EndPoint& peering_endpoint, 
                                       int api_port,
                                       const std::string& nodes_config);
};
