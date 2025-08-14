#include "raft_server.h"
#include "string_utils.h"
#include "logger.h"

#include <regex>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>

// DNS and Configuration Management Module
// Extracted from raft_server.cpp for better organization

std::string ReplicationState::hostname2ipstr(const std::string& hostname) {
    // IPv4 regex pattern for validation
    static const std::regex ipv4_pattern(R"(^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$)");
    
    // If it's already an IP address, return as-is
    if (std::regex_match(hostname, ipv4_pattern)) {
        LOG(DEBUG) << "Input is already an IPv4 address: " << hostname;
        return hostname;
    }
    
    LOG(DEBUG) << "Resolving hostname: " << hostname;
    
    // Perform DNS resolution
    struct addrinfo hints = {};
    hints.ai_family = AF_INET;  // IPv4 only for now
    hints.ai_socktype = SOCK_STREAM;
    
    struct addrinfo* result = nullptr;
    int status = getaddrinfo(hostname.c_str(), nullptr, &hints, &result);
    
    if (status != 0) {
        LOG(ERROR) << "DNS resolution failed for " << hostname << ": " << gai_strerror(status);
        return hostname;  // Return original if resolution fails
    }
    
    std::string ip_str;
    if (result && result->ai_family == AF_INET) {
        struct sockaddr_in* addr_in = reinterpret_cast<struct sockaddr_in*>(result->ai_addr);
        char ip_buffer[INET_ADDRSTRLEN];
        
        if (inet_ntop(AF_INET, &addr_in->sin_addr, ip_buffer, INET_ADDRSTRLEN)) {
            ip_str = ip_buffer;
            LOG(DEBUG) << "Resolved " << hostname << " to " << ip_str;
        } else {
            LOG(ERROR) << "Failed to convert resolved address to string for " << hostname;
            ip_str = hostname;  // Fallback to original
        }
    } else {
        LOG(WARNING) << "No IPv4 address found for " << hostname;
        ip_str = hostname;  // Fallback to original
    }
    
    freeaddrinfo(result);
    return ip_str;
}

NodeConfiguration ReplicationState::parse_node_configuration(const string& nodes_config) {
    NodeConfiguration config;
    
    if (nodes_config.empty()) {
        LOG(DEBUG) << "Empty nodes configuration provided";
        return config;
    }
    
    LOG(DEBUG) << "Parsing node configuration: " << nodes_config;
    
    // Split the nodes string by comma
    std::vector<std::string> nodes = StringUtils::split(nodes_config, ',');
    
    // IPv4 regex pattern for classification
    static const std::regex ipv4_pattern(R"(^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):)");
    
    for (const auto& node : nodes) {
        std::string trimmed_node = StringUtils::trim(node);
        if (trimmed_node.empty()) {
            continue;
        }
        
        // Classify as IP or hostname based on pattern
        if (std::regex_search(trimmed_node, ipv4_pattern)) {
            // Avoid duplicates
            if (std::find(config.ip_nodes.begin(), config.ip_nodes.end(), trimmed_node) == config.ip_nodes.end()) {
                config.ip_nodes.push_back(trimmed_node);
                LOG(DEBUG) << "Added IP node: " << trimmed_node;
            }
        } else {
            // Avoid duplicates
            if (std::find(config.hostname_nodes.begin(), config.hostname_nodes.end(), trimmed_node) == config.hostname_nodes.end()) {
                config.hostname_nodes.push_back(trimmed_node);
                LOG(DEBUG) << "Added hostname node: " << trimmed_node;
            }
        }
    }
    
    // Set metadata
    config.config_version = 1;  // Default version
    config.config_term = 0;     // Default term
    config.created_at = std::chrono::steady_clock::now();
    
    LOG(DEBUG) << "Parsed configuration: " << config.hostname_nodes.size() 
               << " hostname nodes, " << config.ip_nodes.size() << " IP nodes";
    
    return config;
}

braft::Configuration ReplicationState::node_config_to_braft(const NodeConfiguration& node_config) {
    braft::Configuration braft_config;
    std::vector<std::string> final_nodes_vec;
    
    LOG(DEBUG) << "Converting NodeConfiguration to braft::Configuration";
    
    // Handle hostname nodes - perform fresh DNS resolution
    for (const auto& hostname_node : node_config.hostname_nodes) {
        std::string resolved_node = hostname_node;
        
        // Extract hostname from the node string (format: hostname:port1:port2)
        std::string hostname = extract_hostname_from_node(hostname_node);
        if (!hostname.empty() && hostname != hostname_node) {
            // Perform DNS resolution
            std::string resolved_ip = hostname2ipstr(hostname);
            if (resolved_ip != hostname) {
                // Replace hostname with resolved IP in the node string
                resolved_node = StringUtils::replace_all(hostname_node, hostname, resolved_ip);
                LOG(DEBUG) << "DNS resolved: " << hostname_node << " -> " << resolved_node;
            } else {
                LOG(WARNING) << "DNS resolution failed for " << hostname << ", using original: " << hostname_node;
            }
        }
        
        final_nodes_vec.push_back(resolved_node);
    }
    
    // Handle IP nodes directly
    for (const auto& ip_node : node_config.ip_nodes) {
        final_nodes_vec.push_back(ip_node);
    }
    
    // Convert to braft configuration format
    std::string nodes_str = StringUtils::join(final_nodes_vec, ",");
    LOG(DEBUG) << "Final braft configuration string: " << nodes_str;
    
    if (braft_config.parse_from(nodes_str) != 0) {
        LOG(ERROR) << "Failed to parse braft configuration from: " << nodes_str;
    } else {
        LOG(DEBUG) << "Successfully created braft configuration with " << final_nodes_vec.size() << " nodes";
    }
    
    return braft_config;
}

std::string ReplicationState::extract_hostname_from_node(const std::string& node_str) {
    // Extract hostname from node string format: hostname:port1:port2
    // Returns the hostname part before the first colon
    
    size_t first_colon = node_str.find(':');
    if (first_colon != std::string::npos) {
        std::string hostname = node_str.substr(0, first_colon);
        LOG(DEBUG) << "Extracted hostname '" << hostname << "' from node '" << node_str << "'";
        return hostname;
    }
    
    LOG(DEBUG) << "No hostname found in node string: " << node_str;
    return "";
}

bool ReplicationState::peer_matches_hostname_node(const braft::PeerId& peer_id, const std::string& hostname_node) {
    // Extract the hostname from the hostname_node
    std::string hostname = extract_hostname_from_node(hostname_node);
    if (hostname.empty()) {
        return false;
    }
    
    // Resolve the hostname to IP
    std::string resolved_ip = hostname2ipstr(hostname);
    if (resolved_ip == hostname) {
        // DNS resolution failed, can't match
        LOG(DEBUG) << "Cannot match peer " << peer_id << " with hostname node " << hostname_node 
                   << " (DNS resolution failed)";
        return false;
    }
    
    // Extract IP from peer_id and compare
    std::string peer_ip_str = butil::ip2str(peer_id.addr.ip).c_str();
    bool matches = (peer_ip_str == resolved_ip);
    
    LOG(DEBUG) << "Peer matching: " << peer_id << " (" << peer_ip_str << ") vs hostname node " 
               << hostname_node << " (" << resolved_ip << ") = " << (matches ? "MATCH" : "NO MATCH");
    
    return matches;
}

std::string ReplicationState::to_nodes_config(const butil::EndPoint& peering_endpoint, const int api_port,
                                              const std::string& nodes) {
    std::string node_ip = butil::ip2str(peering_endpoint.ip).c_str();
    std::string self_node = node_ip + ":" + std::to_string(peering_endpoint.port) + ":" + std::to_string(api_port);
    
    if (nodes.empty()) {
        return self_node;
    }
    
    return nodes + "," + self_node;
} 