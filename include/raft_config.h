#pragma once

#include <string>
#include <vector>
#include <set>
#include <chrono>
#include <butil/endpoint.h>
#include <braft/raft.h>

/**
 * Namespace for Raft configuration and DNS utilities
 */
namespace raft_config {

    /**
     * Resolves a hostname to an IP string.
     *
     * @param hostname The hostname to resolve (max 64 characters)
     * @return Resolved IP address string. IPv6 addresses are wrapped in [].
     *         Returns empty string on failure or original hostname if already an IP.
     */
    std::string hostname2ipstr(const std::string& hostname);

    /**
     * Resolves all node hostnames in a comma-separated configuration string.
     * Expected format: "host1:port1:port2,host2:port1:port2,..."
     *
     * @param nodes_config Comma-separated list of node configurations
     * @return Resolved configuration string with IPs instead of hostnames.
     *         Returns empty string if all DNS resolutions fail.
     */
    std::string resolve_node_hosts(const std::string& nodes_config);

    /**
     * Converts endpoint and API port to nodes configuration string.
     * Can return empty string if DNS resolution fails on all nodes.
     *
     * @param peering_endpoint The peering endpoint for this node
     * @param api_port The API port for this node
     * @param nodes_config Existing nodes configuration (can be empty)
     * @return Formatted nodes configuration string with resolved IPs.
     */
    std::string to_nodes_config(const butil::EndPoint& peering_endpoint,
                                int api_port,
                                const std::string& nodes_config);

    /**
     * Constructs a URL for a given peer.
     *
     * @param peer_id The peer identifier
     * @param path The URL path (e.g., "/health")
     * @param protocol The protocol ("http" or "https")
     * @return Complete URL string (e.g., "http://127.0.0.1:8108/health")
     */
    std::string get_node_url_path(const braft::PeerId& peer_id,
                                  const std::string& path,
                                  const std::string& protocol);

    // === ENHANCED DNS-NATIVE FEATURES ===

    /**
     * Enhanced node configuration structure for DNS-native operations
     */
    struct NodeConfiguration {
        std::vector<std::string> hostname_nodes;  // DNS-resolvable hostnames
        std::vector<std::string> ip_nodes;        // Static IP addresses
        uint64_t config_version;                  // Configuration version
        int64_t config_term;                      // Raft term when config was created
        std::chrono::steady_clock::time_point created_at;  // Creation timestamp

        NodeConfiguration() : config_version(0), config_term(-1), created_at(std::chrono::steady_clock::now()) {}

        // Get total number of nodes
        size_t total_nodes() const { return hostname_nodes.size() + ip_nodes.size(); }

        // Get all nodes as a set (for operations like intersection)
        std::set<std::string> get_node_set() const;

        // TLA+ safety methods
        bool is_newer_than(const NodeConfiguration& other) const;
        bool is_safe_single_node_change(const NodeConfiguration& new_config) const;
        NodeConfiguration create_single_node_change(const std::string& node_to_add,
                                                   const std::string& node_to_remove,
                                                   int64_t current_term) const;
    };

    /**
     * Parse node configuration with hostname/IP classification
     * Implements enterprise-grade configuration parsing with DNS-native support
     *
     * @param nodes_config Comma-separated node configuration string
     * @return NodeConfiguration with classified hostname and IP nodes
     */
    NodeConfiguration parse_node_configuration(const std::string& nodes_config);

    /**
     * Convert NodeConfiguration to braft::Configuration with fresh DNS resolution
     *
     * @param node_config The NodeConfiguration to convert
     * @return braft::Configuration with resolved IPs
     */
    braft::Configuration node_config_to_braft(const NodeConfiguration& node_config);

    /**
     * Extract hostname from a node string (e.g., "host:8107:8108" -> "host")
     *
     * @param node_str Node string in format "host:peering_port:api_port"
     * @return Extracted hostname or IP
     */
    std::string extract_hostname_from_node(const std::string& node_str);

    /**
     * Check if a peer matches a hostname node (for failure-triggered DNS refresh)
     *
     * @param peer_id The braft peer identifier
     * @param hostname_node The hostname node string (e.g., "host.example.com:8107:8108")
     * @return True if the peer corresponds to this hostname node
     */
    bool peer_matches_hostname_node(const braft::PeerId& peer_id, const std::string& hostname_node);

    /**
     * Enhanced DNS resolution with failure handling
     * Provides more robust DNS resolution with proper error handling
     *
     * @param hostname The hostname to resolve
     * @param force_refresh Force fresh DNS resolution (ignore cache)
     * @return Resolved IP address or original hostname on failure
     */
    std::string hostname2ipstr_enhanced(const std::string& hostname, bool force_refresh = false);

    /**
     * Validate if a string is a valid IP address (IPv4 or IPv6)
     *
     * @param str The string to validate
     * @return True if it's a valid IP address
     */
    bool is_valid_ip(const std::string& str);

    /**
     * Check if a node string represents a hostname (not IP)
     *
     * @param node_str Node string to check
     * @return True if it contains a hostname, false if IP
     */
    bool is_hostname_node(const std::string& node_str);

} // namespace raft_config
