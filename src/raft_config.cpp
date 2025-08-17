#include "raft_config.h"
#include <string_utils.h>
#include <logger.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <cstring>
#include <vector>
#include <set>
#include <algorithm>
#include <regex>
#include <chrono>

// Raft Configuration and DNS Resolution Module
// Extracted from raft_server.cpp for better organization

namespace raft_config {

std::string hostname2ipstr(const std::string& hostname) {
    if(hostname.size() > 64) {
        LOG(ERROR) << "Host name is too long (must be < 64 characters): " << hostname;
        return "";
    }

    // Check if this is already an IPv6 address by looking for []
    if(hostname.find('[') == 0) {
        return hostname;
    }

    struct addrinfo hints, *result;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;     // Allow both IPv4 and IPv6
    hints.ai_socktype = SOCK_STREAM; // TCP

    int status = getaddrinfo(hostname.c_str(), nullptr, &hints, &result);
    if (status != 0) {
        LOG(ERROR) << "Unable to resolve host: " << hostname << ", error: " << gai_strerror(status);
        return hostname; // Return original hostname on error
    }

    char ip_str[INET6_ADDRSTRLEN];
    std::string resolved_ip;

    // Get the first resolved address
    if (result->ai_family == AF_INET) {
        // IPv4
        struct sockaddr_in *addr = (struct sockaddr_in *)result->ai_addr;
        inet_ntop(AF_INET, &(addr->sin_addr), ip_str, INET_ADDRSTRLEN);
        resolved_ip = ip_str;
    } else if (result->ai_family == AF_INET6) {
        // IPv6
        struct sockaddr_in6 *addr = (struct sockaddr_in6 *)result->ai_addr;
        inet_ntop(AF_INET6, &(addr->sin6_addr), ip_str, INET6_ADDRSTRLEN);
        resolved_ip = std::string("[") + ip_str + "]";
    }

    freeaddrinfo(result);

    if(resolved_ip.empty()) {
        return hostname; // Return original hostname if resolution didn't produce a valid IP
    }

    return resolved_ip;
}

std::string resolve_node_hosts(const std::string& nodes_config) {
    std::vector<std::string> final_nodes_vec;
    std::vector<std::string> node_strings;
    StringUtils::split(nodes_config, node_strings, ",");

    for(const auto& node_str: node_strings) {
        // Check if this is already an IPv6 address node by looking for []
        if(node_str.find('[') == 0) {
            final_nodes_vec.push_back(node_str);
            continue;
        }

        // could be an IP or a hostname that must be resolved
        std::vector<std::string> node_parts;
        StringUtils::split(node_str, node_parts, ":");

        if(node_parts.size() != 3) {
            final_nodes_vec.push_back(node_str);
            continue;
        }

        std::string resolved_ip = hostname2ipstr(node_parts[0]);
        if(resolved_ip.empty()) {
            LOG(ERROR) << "Unable to resolve host: " << node_parts[0];
            continue;
        }

        final_nodes_vec.push_back(resolved_ip + ":" + node_parts[1] + ":" + node_parts[2]);
    }

    if(final_nodes_vec.empty()) {
        return "";
    }

    std::string final_nodes_config = StringUtils::join(final_nodes_vec, ",");
    return final_nodes_config;
}

std::string to_nodes_config(const butil::EndPoint& peering_endpoint, const int api_port,
                            const std::string& nodes_config) {
    if(nodes_config.empty()) {
        // endpoint2str gives us "<ip>:<peering_port>", we just need to add ":<api_port>"
        return std::string(butil::endpoint2str(peering_endpoint).c_str()) + ":" + std::to_string(api_port);
    } else {
        return resolve_node_hosts(nodes_config);
    }
}

std::string get_node_url_path(const braft::PeerId& peer_id,
                              const std::string& path,
                              const std::string& protocol) {
    const std::string endpoint_str = butil::endpoint2str(peer_id.addr).c_str();
    const size_t last_colon = endpoint_str.rfind(':');
    if (last_colon == std::string::npos) {
        LOG(ERROR) << "Invalid endpoint format: " << endpoint_str;
        return "";
    }

    // For IPv6, the IP part may contain colons and be wrapped in []
    const std::string ip_part = endpoint_str.substr(0, last_colon);

    std::string url = protocol + "://";
    url += ip_part;  // IP part (possibly with [] for IPv6)
    url += ":";
    url += std::to_string(peer_id.idx);

    // Add path ensuring there's exactly one / between URL parts
    if(!path.empty()) {
        if(path[0] == '/') {
            url += path;
        } else {
            url += "/" + path;
        }
    }

    return url;
}

// ============================================================================
// ENHANCED DNS-NATIVE FEATURES
// ============================================================================

std::set<std::string> NodeConfiguration::get_node_set() const {
    std::set<std::string> nodes_set;
    for (const auto& node : hostname_nodes) {
        nodes_set.insert(node);
    }
    for (const auto& node : ip_nodes) {
        nodes_set.insert(node);
    }
    return nodes_set;
}

bool NodeConfiguration::is_newer_than(const NodeConfiguration& other) const {
    const int64_t kUninitializedTerm = -1;

    // TLA+ pattern: Handle force reconfigs (term=-1)
    if (config_term == kUninitializedTerm || other.config_term == kUninitializedTerm) {
        return config_version > other.config_version;  // Version-only comparison
    }

    // Standard TLA+ ordering: term first, then version
    return config_term > other.config_term ||
           (config_term == other.config_term && config_version > other.config_version);
}

bool NodeConfiguration::is_safe_single_node_change(const NodeConfiguration& new_config) const {
    // TLA+ pattern: Use set symmetric difference algorithm for precise validation

    std::set<std::string> old_nodes_set = get_node_set();
    std::set<std::string> new_nodes_set = new_config.get_node_set();

    // Calculate symmetric difference
    std::vector<std::string> symmetric_diff;
    std::set_symmetric_difference(
        old_nodes_set.begin(), old_nodes_set.end(),
        new_nodes_set.begin(), new_nodes_set.end(),
        std::back_inserter(symmetric_diff)
    );

    // Rule: Single-node change means symmetric difference size must be exactly 1
    return symmetric_diff.size() == 1 && new_config.total_nodes() >= 1;
}

NodeConfiguration NodeConfiguration::create_single_node_change(const std::string& node_to_add,
                                                              const std::string& node_to_remove,
                                                              int64_t current_term) const {
    NodeConfiguration new_config = *this;
    new_config.config_version++;
    new_config.config_term = current_term;
    new_config.created_at = std::chrono::steady_clock::now();

    // Remove node if specified
    if (!node_to_remove.empty()) {
        auto it = std::find(new_config.hostname_nodes.begin(), new_config.hostname_nodes.end(), node_to_remove);
        if (it != new_config.hostname_nodes.end()) {
            new_config.hostname_nodes.erase(it);
        } else {
            auto it2 = std::find(new_config.ip_nodes.begin(), new_config.ip_nodes.end(), node_to_remove);
            if (it2 != new_config.ip_nodes.end()) {
                new_config.ip_nodes.erase(it2);
            }
        }
    }

    // Add node if specified
    if (!node_to_add.empty()) {
        if (is_hostname_node(node_to_add)) {
            new_config.hostname_nodes.push_back(node_to_add);
        } else {
            new_config.ip_nodes.push_back(node_to_add);
        }
    }

    return new_config;
}

NodeConfiguration parse_node_configuration(const std::string& nodes_config) {
    NodeConfiguration config;
    std::vector<std::string> node_strings;
    StringUtils::split(nodes_config, node_strings, ",");

    for (const auto& node_str : node_strings) {
        if (node_str.empty()) continue;

        if (is_hostname_node(node_str)) {
            config.hostname_nodes.push_back(node_str);
        } else {
            config.ip_nodes.push_back(node_str);
        }
    }

    // Set initial version and timestamp
    config.config_version = 1;
    config.config_term = -1; // Uninitialized until set by Raft
    config.created_at = std::chrono::steady_clock::now();

    return config;
}

braft::Configuration node_config_to_braft(const NodeConfiguration& node_config) {
    braft::Configuration braft_config;

    // Process hostname nodes with fresh DNS resolution
    for (const auto& hostname_node : node_config.hostname_nodes) {
        std::string resolved = resolve_node_hosts(hostname_node);
        if (!resolved.empty()) {
            std::vector<std::string> parts;
            StringUtils::split(resolved, parts, ":");
            if (parts.size() >= 2) {
                butil::EndPoint endpoint;
                if (butil::str2endpoint(parts[0].c_str(), std::stoi(parts[1]), &endpoint) == 0) {
                    braft::PeerId peer_id(endpoint, 0);
                    braft_config.add_peer(peer_id);
                }
            }
        }
    }

    // Process IP nodes directly
    for (const auto& ip_node : node_config.ip_nodes) {
        std::vector<std::string> parts;
        StringUtils::split(ip_node, parts, ":");
        if (parts.size() >= 2) {
            butil::EndPoint endpoint;
            if (butil::str2endpoint(parts[0].c_str(), std::stoi(parts[1]), &endpoint) == 0) {
                braft::PeerId peer_id(endpoint, 0);
                braft_config.add_peer(peer_id);
            }
        }
    }

    return braft_config;
}

std::string extract_hostname_from_node(const std::string& node_str) {
    // Extract hostname/IP from "host:peering_port:api_port" format
    size_t first_colon = node_str.find(':');
    if (first_colon != std::string::npos) {
        return node_str.substr(0, first_colon);
    }
    return node_str; // Return as-is if no colon found
}

bool peer_matches_hostname_node(const braft::PeerId& peer_id, const std::string& hostname_node) {
    // Extract hostname from the hostname node
    std::string hostname = extract_hostname_from_node(hostname_node);

    // Resolve the hostname to IP
    std::string resolved_ip = hostname2ipstr(hostname);
    if (resolved_ip.empty() || resolved_ip == hostname) {
        return false; // DNS resolution failed
    }

    // Compare with peer's IP
    std::string peer_ip = butil::endpoint2str(peer_id.addr).c_str();
    size_t last_colon = peer_ip.rfind(':');
    if (last_colon != std::string::npos) {
        peer_ip = peer_ip.substr(0, last_colon); // Remove port part
    }

    // Handle IPv6 brackets if needed
    if (resolved_ip.front() == '[' && resolved_ip.back() == ']') {
        resolved_ip = resolved_ip.substr(1, resolved_ip.length() - 2);
    }
    if (peer_ip.front() == '[' && peer_ip.back() == ']') {
        peer_ip = peer_ip.substr(1, peer_ip.length() - 2);
    }

    return resolved_ip == peer_ip;
}

std::string hostname2ipstr_enhanced(const std::string& hostname, bool force_refresh) {
    // For now, force_refresh is not implemented (would need DNS caching)
    // This is a placeholder for future DNS caching implementation
    return hostname2ipstr(hostname);
}

bool is_valid_ip(const std::string& str) {
    // Check for IPv4
    std::regex ipv4_pattern(R"(^(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)$)");
    if (std::regex_match(str, ipv4_pattern)) {
        return true;
    }

    // Check for IPv6 (simplified - handles most common cases)
    if (str.front() == '[' && str.back() == ']') {
        std::string ipv6_inner = str.substr(1, str.length() - 2);
        // Very basic IPv6 check - contains only hex chars and colons
        std::regex ipv6_pattern(R"(^[0-9a-fA-F:]+$)");
        return std::regex_match(ipv6_inner, ipv6_pattern) && ipv6_inner.find(':') != std::string::npos;
    }

    return false;
}

bool is_hostname_node(const std::string& node_str) {
    // Extract the host part (before first colon)
    std::string host_part = extract_hostname_from_node(node_str);

    // If it's a valid IP, it's not a hostname
    return !is_valid_ip(host_part);
}

}
