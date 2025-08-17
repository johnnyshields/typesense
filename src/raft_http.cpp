#include "raft_http.h"
#include <zlib.h>
#include <logger.h>
#include <http_client.h>
#include <chrono>
#include <thread>
#include <sstream>
#include <iomanip>

// Raft HTTP Request Processing Module
// Extracted from raft_server.cpp for better organization

namespace raft_http {

Option<bool> handle_gzip(const std::shared_ptr<http_req>& request) {
    if (!request->zstream_initialized) {
        request->zs.zalloc = Z_NULL;
        request->zs.zfree = Z_NULL;
        request->zs.opaque = Z_NULL;
        request->zs.avail_in = 0;
        request->zs.next_in = Z_NULL;

        if (inflateInit2(&request->zs, 16 + MAX_WBITS) != Z_OK) {
            return Option<bool>(400, "inflateInit failed while decompressing");
        }

        request->zstream_initialized = true;
    }

    std::string outbuffer;
    outbuffer.resize(10 * request->body.size());

    request->zs.next_in = (Bytef *) request->body.c_str();
    request->zs.avail_in = request->body.size();
    std::size_t size_uncompressed = 0;
    int ret = 0;
    do {
        request->zs.avail_out = static_cast<unsigned int>(outbuffer.size());
        request->zs.next_out = reinterpret_cast<Bytef *>(&outbuffer[0] + size_uncompressed);
        ret = inflate(&request->zs, Z_FINISH);
        if (ret != Z_STREAM_END && ret != Z_OK && ret != Z_BUF_ERROR) {
            std::string error_msg = request->zs.msg;
            inflateEnd(&request->zs);
            return Option<bool>(400, error_msg);
        }

        size_uncompressed += (outbuffer.size() - request->zs.avail_out);
    } while (request->zs.avail_out == 0);

    if (ret == Z_STREAM_END) {
        request->zstream_initialized = false;
        inflateEnd(&request->zs);
    }

    outbuffer.resize(size_uncompressed);

    request->body = outbuffer;
    request->chunk_len = outbuffer.size();

    return Option<bool>(true);
}

// ============================================================================
// ENHANCED HTTP FEATURES
// ============================================================================

Option<bool> forward_to_leader_advanced(const std::shared_ptr<http_req>& request,
                                       const std::shared_ptr<http_res>& response,
                                       const std::string& leader_url,
                                       int max_retries) {
    if (leader_url.empty()) {
        create_error_response(response, 503, "No leader available", true);
        return Option<bool>(503, "No leader URL provided");
    }

    // Build the target URL
    std::string target_url = build_node_url(leader_url, request->http_req_path,
                                           request->get_header_value("X-TYPESENSE-API-KEY"));

    // Attempt forwarding with retries
    for (int attempt = 1; attempt <= max_retries; attempt++) {
        try {
            LOG(DEBUG) << "Forwarding to leader (attempt " << attempt << "/" << max_retries
                      << "): " << target_url;

            long status_code;
            std::string leader_response;

            bool success = HttpClient::post_response(target_url, request->body, leader_response, status_code,
                                                   {{"Content-Type", request->get_header_value("content-type")}});

            if (success && status_code >= 200 && status_code < 300) {
                response->content_type_header = "application/json; charset=utf-8";
                response->status_code = status_code;
                response->body = leader_response;
                LOG(DEBUG) << "Successfully forwarded request to leader";
                return Option<bool>(true);
            }

            LOG(WARNING) << "Leader forwarding attempt " << attempt << " failed: "
                        << "success=" << success << ", status=" << status_code;

            // If this was the last attempt, return the error
            if (attempt == max_retries) {
                create_error_response(response, status_code > 0 ? status_code : 502,
                                    "Failed to forward request to leader after " +
                                    std::to_string(max_retries) + " attempts", true);
                return Option<bool>(false);
            }

            // Wait before retry (exponential backoff)
            std::this_thread::sleep_for(std::chrono::milliseconds(100 * attempt));

        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception during leader forwarding (attempt " << attempt << "): " << e.what();

            if (attempt == max_retries) {
                create_error_response(response, 502,
                                    "Internal error while forwarding to leader: " + std::string(e.what()), true);
                return Option<bool>(502, e.what());
            }
        }
    }

    return Option<bool>(502, "All retry attempts failed");
}

void create_error_response(const std::shared_ptr<http_res>& response,
                          int error_code,
                          const std::string& error_message,
                          bool include_debug_info) {
    nlohmann::json error_response;
    error_response["message"] = error_message;
    error_response["error_code"] = error_code;

    if (include_debug_info) {
        error_response["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        error_response["debug"] = true;
    }

    response->content_type_header = "application/json; charset=utf-8";
    response->status_code = error_code;
    response->body = error_response.dump();
}

std::string build_node_url(const std::string& base_url,
                          const std::string& path,
                          const std::string& api_key,
                          const std::string& query_params) {
    std::ostringstream url_stream;
    url_stream << base_url;

    // Add path
    if (!path.empty()) {
        if (path[0] != '/') {
            url_stream << "/";
        }
        url_stream << path;
    }

    // Add query parameters
    bool has_query = path.find('?') != std::string::npos;

    if (!api_key.empty()) {
        url_stream << (has_query ? "&" : "?");
        url_stream << "x-typesense-api-key=" << api_key;
        has_query = true;
    }

    if (!query_params.empty()) {
        url_stream << (has_query ? "&" : "?");
        url_stream << query_params;
    }

    return url_stream.str();
}

Option<bool> validate_request(const std::shared_ptr<http_req>& request) {
    // Basic validation checks
    if (!request) {
        return Option<bool>(400, "Invalid request object");
    }

    // Check for required headers
    if (request->get_header_value("content-type").empty() && !request->body.empty()) {
        return Option<bool>(400, "Content-Type header required for requests with body");
    }

    // Check for oversized requests
    const size_t MAX_REQUEST_SIZE = 100 * 1024 * 1024; // 100MB
    if (request->body.size() > MAX_REQUEST_SIZE) {
        return Option<bool>(413, "Request body too large");
    }

    // Check for valid JSON if content type suggests JSON
    std::string content_type = request->get_header_value("content-type");
    if (content_type.find("application/json") != std::string::npos && !request->body.empty()) {
        try {
            nlohmann::json::parse(request->body);
        } catch (const nlohmann::json::exception& e) {
            return Option<bool>(400, "Invalid JSON in request body: " + std::string(e.what()));
        }
    }

    return Option<bool>(true);
}

void log_request(const std::shared_ptr<http_req>& request,
                const std::string& prefix,
                bool include_body) {
    if (!request) {
        LOG(DEBUG) << prefix << "NULL request";
        return;
    }

    std::ostringstream log_stream;
    log_stream << prefix << "HTTP " << request->http_req_path;
    log_stream << " [Content-Type: " << request->get_header_value("content-type") << "]";
    log_stream << " [Body size: " << request->body.size() << " bytes]";

    if (include_body && !request->body.empty() && request->body.size() < 1000) {
        log_stream << " [Body: " << request->body.substr(0, 200);
        if (request->body.size() > 200) {
            log_stream << "...";
        }
        log_stream << "]";
    }

    LOG(DEBUG) << log_stream.str();
}

nlohmann::json extract_client_info(const std::shared_ptr<http_req>& request) {
    nlohmann::json client_info;

    if (!request) {
        return client_info;
    }

    // Extract common client information
    client_info["user_agent"] = request->get_header_value("user-agent");
    client_info["content_type"] = request->get_header_value("content-type");
    client_info["request_path"] = request->http_req_path;
    client_info["body_size"] = request->body.size();
    client_info["has_api_key"] = !request->get_header_value("x-typesense-api-key").empty();

    // Add timestamp
    client_info["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    return client_info;
}

bool requires_leader_processing(const std::shared_ptr<http_req>& request) {
    if (!request) {
        return false;
    }

    // Write operations that require leader processing
    std::string path = request->http_req_path;

    // Collection operations that modify data
    if (path.find("/collections/") != std::string::npos) {
        // POST (create/update), PUT (update), DELETE (delete) require leader
        if (request->http_req_method == "POST" ||
            request->http_req_method == "PUT" ||
            request->http_req_method == "DELETE") {
            return true;
        }

        // Specific write endpoints
        if (path.find("/documents") != std::string::npos ||
            path.find("/synonyms") != std::string::npos ||
            path.find("/overrides") != std::string::npos) {
            return request->http_req_method != "GET";
        }
    }

    // Cluster management operations
    if (path.find("/cluster/") != std::string::npos ||
        path.find("/operations/") != std::string::npos) {
        return true;
    }

    // Default: read operations can be served by followers
    return false;
}

} // namespace raft_http