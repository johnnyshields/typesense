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
    // Enhanced DNS resolution with proper cache management
    
    if (hostname.empty()) {
        LOG(WARNING) << "Empty hostname provided for resolution";
        return "";
    }
    
    // Check cache first
    if (auto cached_ip = dns_cache_->get(hostname)) {
        return *cached_ip;
    }
    
    LOG(DEBUG) << "Resolving hostname: " << hostname;
    
    struct addrinfo hints, *result = nullptr;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;    // Allow IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_ADDRCONFIG;  // Only return addresses if we have that address type configured
    
    int status = getaddrinfo(hostname.c_str(), nullptr, &hints, &result);
    if (status != 0) {
        LOG(WARNING) << "DNS resolution failed for " << hostname << ": " << gai_strerror(status);
        return "";
    }
    
    if (!result) {
        LOG(WARNING) << "No addresses found for hostname: " << hostname;
        return "";
    }
    
    std::string ip_str;
    char ip_buffer[INET6_ADDRSTRLEN];
    
    for (struct addrinfo* addr = result; addr != nullptr; addr = addr->ai_next) {
        if (addr->ai_family == AF_INET) {
            // IPv4
            struct sockaddr_in* sockaddr_ipv4 = (struct sockaddr_in*)addr->ai_addr;
            if (inet_ntop(AF_INET, &(sockaddr_ipv4->sin_addr), ip_buffer, INET_ADDRSTRLEN)) {
                ip_str = std::string(ip_buffer);
                LOG(DEBUG) << "Resolved " << hostname << " to IPv4: " << ip_str;
                break;  // Prefer IPv4 for consistency
            }
        } else if (addr->ai_family == AF_INET6) {
            // IPv6 - only use if no IPv4 found
            if (ip_str.empty()) {
                struct sockaddr_in6* sockaddr_ipv6 = (struct sockaddr_in6*)addr->ai_addr;
                if (inet_ntop(AF_INET6, &(sockaddr_ipv6->sin6_addr), ip_buffer, INET6_ADDRSTRLEN)) {
                    ip_str = std::string(ip_buffer);
                    LOG(DEBUG) << "Resolved " << hostname << " to IPv6: " << ip_str;
                }
            }
        }
    }
    
    freeaddrinfo(result);
    
    if (ip_str.empty()) {
        LOG(WARNING) << "Failed to extract IP address for hostname: " << hostname;
        return "";
    }
    
    // Cache the successful resolution
    dns_cache_->put(hostname, ip_str);
    
    LOG(INFO) << "Successfully resolved " << hostname << " -> " << ip_str;
    return ip_str;
}

NodeConfiguration ReplicationState::parse_node_configuration(const string& nodes_config) {
    NodeConfiguration config;
    
    if (nodes_config.empty()) {
        LOG(DEBUG) << "Empty nodes configuration provided";
        return config;
    }
    
    LOG(DEBUG) << "Parsing node configuration: " << nodes_config;
    
    std::vector<std::string> nodes = StringUtils::split(nodes_config, ',');
    
    for (const std::string& node : nodes) {
        std::string trimmed_node = StringUtils::trim(node);
        
        if (trimmed_node.empty()) {
            LOG(WARNING) << "Skipping empty node specification";
            continue;
        }
        
        // Enhanced validation: check for proper format (host:peering_port:api_port)
        std::vector<std::string> parts = StringUtils::split(trimmed_node, ':');
        if (parts.size() != 3) {
            LOG(WARNING) << "Invalid node format (expected host:peering_port:api_port): " << trimmed_node;
            continue;
        }
        
        // Validate ports are numeric
        bool valid_ports = true;
        for (int i = 1; i < 3; i++) {
            try {
                int port = std::stoi(parts[i]);
                if (port <= 0 || port > 65535) {
                    LOG(WARNING) << "Invalid port number in node spec: " << trimmed_node;
                    valid_ports = false;
                    break;
                }
            } catch (const std::exception& e) {
                LOG(WARNING) << "Non-numeric port in node spec: " << trimmed_node;
                valid_ports = false;
                break;
            }
        }
        
        if (!valid_ports) {
            continue;
        }
        
        // Determine if it's IP or hostname and add to appropriate collection
        if (is_ip_address(parts[0])) {
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
    
    LOG(INFO) << "Parsed configuration: " << config.hostname_nodes.size() 
              << " hostname nodes, " << config.ip_nodes.size() << " IP nodes";
              
    // Basic sanity check
    if (config.empty()) {
        LOG(WARNING) << "No valid nodes found in configuration";
    } else if (config.total_nodes() == 1) {
        LOG(INFO) << "Single-node configuration detected";
    } else if (config.total_nodes() % 2 == 0) {
        LOG(WARNING) << "Even number of nodes (" << config.total_nodes() 
                     << ") may lead to split-brain scenarios";
    }
    
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

// DNS cache management methods
void ReplicationState::clear_dns_cache() {
    dns_cache_->clear();
}

void ReplicationState::clear_dns_cache_for_hostname(const std::string& hostname) {
    dns_cache_->clear_hostname(hostname);
}

size_t ReplicationState::get_dns_cache_size() const {
    return dns_cache_->size();
} 