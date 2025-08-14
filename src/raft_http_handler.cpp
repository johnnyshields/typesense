#include "raft_server.h"
#include "string_utils.h"
#include "logger.h"
#include "http_data.h"

#include <zlib.h>

// HTTP Request Processing Module
// Extracted from raft_server.cpp for better organization

Option<bool> ReplicationState::handle_gzip(const std::shared_ptr<http_req>& request) {
    const std::string& encoding = request->get_header("content-encoding");
    if(encoding != "gzip") {
        return Option<bool>(404);
    }

    std::string& compressed_body = request->body;
    if(compressed_body.empty()) {
        return Option<bool>(400);
    }

    z_stream stream;
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;
    stream.avail_in = compressed_body.size();
    stream.next_in = (Bytef*)compressed_body.c_str();

    int ret = inflateInit2(&stream, 16+MAX_WBITS);
    if (ret != Z_OK) {
        LOG(ERROR) << "Failed to initialize gzip decompression: " << ret;
        return Option<bool>(400);
    }

    std::string decompressed_body;
    char buffer[8192];

    do {
        stream.avail_out = sizeof(buffer);
        stream.next_out = (Bytef*)buffer;

        ret = inflate(&stream, Z_NO_FLUSH);
        if (ret != Z_OK && ret != Z_STREAM_END) {
            inflateEnd(&stream);
            LOG(ERROR) << "Failed to decompress gzip data: " << ret;
            return Option<bool>(400);
        }

        size_t decompressed_size = sizeof(buffer) - stream.avail_out;
        decompressed_body.append(buffer, decompressed_size);

    } while (stream.avail_out == 0);

    inflateEnd(&stream);

    if (ret != Z_STREAM_END) {
        LOG(ERROR) << "Incomplete gzip decompression";
        return Option<bool>(400);
    }

    request->body = decompressed_body;
    LOG(DEBUG) << "Successfully decompressed gzip request body: " 
               << compressed_body.size() << " -> " << decompressed_body.size() << " bytes";

    return Option<bool>(true);
}

void ReplicationState::write(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
    if(!is_alive()) {
        response->set_500("Raft server is not ready.");
        return;
    }

    // Handle gzip compression if present
    auto gzip_result = handle_gzip(request);
    if(!gzip_result.ok()) {
        if(gzip_result.code() == 400) {
            response->set_400("Bad gzip compression.");
        } else if(gzip_result.code() == 404) {
            // No gzip encoding, continue normally
        }
        return;
    }

    // Check if we're the leader
    if(!is_leader()) {
        // Forward to leader if we're not the leader
        write_to_leader(request, response);
        return;
    }

    // We are the leader, process the write request
    LOG(DEBUG) << "Processing write request as leader";

    // Create replication closure
    ReplicationClosure* closure = new ReplicationClosure(this, request, response);
    
    // Create log entry
    braft::IOBuf log_entry;
    log_entry.append(request->body);
    
    // Apply the log entry
    braft::Task task;
    task.data = &log_entry;
    task.done = closure;
    
    // Set expected term (for safety)
    task.expected_term = get_current_term();
    
    // Increment pending writes counter
    pending_writes++;
    
    // Apply to raft
    node->apply(task);
    
    LOG(DEBUG) << "Write request submitted to raft, pending writes: " << pending_writes.load();
}

void ReplicationState::write_to_leader(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
    if(!node) {
        response->set_500("Raft node not initialized.");
        return;
    }

    // Get current leader
    braft::PeerId leader_peer_id = node->leader_id();
    if(leader_peer_id.is_empty()) {
        response->set_503("No leader available.");
        return;
    }

    LOG(DEBUG) << "Forwarding write request to leader: " << leader_peer_id;

    // Construct leader URL
    std::string leader_url = get_node_url_path(leader_peer_id, request->get_req_path(), 
                                               request->get_req_param("X-TYPESENSE-API-KEY"));

    // Create HTTP client request
    std::map<std::string, std::string> headers;
    
    // Copy relevant headers from original request
    const auto& original_headers = request->headers;
    for(const auto& header : original_headers) {
        if(header.first != "host" && header.first != "content-length") {
            headers[header.first] = header.second;
        }
    }

    // Set content type if not already set
    if(headers.find("content-type") == headers.end()) {
        headers["content-type"] = "application/json";
    }

    // Make the request to leader
    std::string response_body;
    std::map<std::string, std::string> response_headers;
    long response_code;

    // Use HTTP client to forward request
    bool success = http_client->post_response(leader_url, request->body, response_body, 
                                            response_headers, response_code, headers, 10);

    if(!success) {
        LOG(ERROR) << "Failed to forward request to leader: " << leader_url;
        response->set_500("Failed to forward request to leader.");
        return;
    }

    // Forward the response back to client
    response->set_body(response_code, response_body);
    
    // Copy response headers
    for(const auto& header : response_headers) {
        response->set_header(header.first, header.second);
    }

    LOG(DEBUG) << "Successfully forwarded request to leader, response code: " << response_code;
}

std::string ReplicationState::get_node_url_path(const braft::PeerId& peer_id, const std::string& path,
                                                const std::string& api_key) {
    // Extract IP and port from peer_id
    std::string peer_ip = butil::ip2str(peer_id.addr.ip).c_str();
    int peer_port = peer_id.addr.port;
    
    // Find the API port for this peer
    int api_port = 8108; // Default API port
    
    // Try to find the actual API port from configuration
    std::shared_lock<std::shared_mutex> lock(current_config_mutex);
    
    // Check if this peer matches any of our configured nodes
    for(const auto& hostname_node : current_node_config.hostname_nodes) {
        if(peer_matches_hostname_node(peer_id, hostname_node)) {
            // Extract API port from hostname_node format: hostname:raft_port:api_port
            auto parts = StringUtils::split(hostname_node, ':');
            if(parts.size() >= 3) {
                api_port = std::stoi(parts[2]);
            }
            break;
        }
    }
    
    // Check IP nodes too
    for(const auto& ip_node : current_node_config.ip_nodes) {
        auto parts = StringUtils::split(ip_node, ':');
        if(parts.size() >= 3 && parts[0] == peer_ip && std::stoi(parts[1]) == peer_port) {
            api_port = std::stoi(parts[2]);
            break;
        }
    }
    
    lock.unlock();
    
    // Construct the URL
    std::string url = "http://" + peer_ip + ":" + std::to_string(api_port) + path;
    
    // Add API key if provided
    if(!api_key.empty()) {
        char separator = (path.find('?') != std::string::npos) ? '&' : '?';
        url += separator + "X-TYPESENSE-API-KEY=" + api_key;
    }
    
    LOG(DEBUG) << "Generated node URL: " << url;
    return url;
}

void ReplicationState::read(const std::shared_ptr<http_res>& response) {
    if(!is_alive()) {
        response->set_500("Raft server is not ready.");
        return;
    }

    // For read operations, we can serve from any node that's caught up
    // This is a simple implementation - more sophisticated read consistency 
    // models could be implemented here
    
    nlohmann::json status_json = get_status();
    response->set_200(status_json.dump());
    
    LOG(DEBUG) << "Served read request with current status";
} 