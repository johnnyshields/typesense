#pragma once

#include <memory>
#include <string>
#include <nlohmann/json.hpp>
#include "http_data.h"
#include "option.h"

/**
 * Namespace for HTTP-related utilities
 */
namespace raft_http {

    /**
     * Handle gzip compression/decompression for incoming requests.
     * Modifies the request body in-place if gzip is detected.
     *
     * @param request The HTTP request to process
     * @return Success(true) or Error with status code and message
     */
    Option<bool> handle_gzip(const std::shared_ptr<http_req>& request);

    // === ENHANCED HTTP FEATURES ===

    /**
     * Advanced leader forwarding with retry logic
     * Forwards requests to leader with comprehensive error handling
     *
     * @param request The HTTP request to forward
     * @param response The HTTP response to populate
     * @param leader_url The leader's URL
     * @param max_retries Maximum number of retry attempts
     * @return Success or detailed error information
     */
    Option<bool> forward_to_leader_advanced(const std::shared_ptr<http_req>& request,
                                           const std::shared_ptr<http_res>& response,
                                           const std::string& leader_url,
                                           int max_retries = 3);

    /**
     * Enhanced error response generation
     * Creates standardized error responses with detailed information
     *
     * @param response The response to populate
     * @param error_code HTTP error code
     * @param error_message Error message
     * @param include_debug_info Whether to include debug information
     */
    void create_error_response(const std::shared_ptr<http_res>& response,
                              int error_code,
                              const std::string& error_message,
                              bool include_debug_info = false);

    /**
     * Generate URL with API key and proper encoding
     * Creates properly formatted URLs for node communication
     *
     * @param base_url Base URL (e.g., "http://node1:8108")
     * @param path URL path (e.g., "/collections/search")
     * @param api_key API key for authentication
     * @param query_params Optional query parameters
     * @return Complete URL string
     */
    std::string build_node_url(const std::string& base_url,
                              const std::string& path,
                              const std::string& api_key = "",
                              const std::string& query_params = "");

    /**
     * Validate and sanitize HTTP request
     * Performs security and format validation on incoming requests
     *
     * @param request The request to validate
     * @return Validation result with details
     */
    Option<bool> validate_request(const std::shared_ptr<http_req>& request);

    /**
     * Enhanced request logging for debugging
     * Logs request details with configurable verbosity
     *
     * @param request The request to log
     * @param prefix Log message prefix
     * @param include_body Whether to include request body
     */
    void log_request(const std::shared_ptr<http_req>& request,
                    const std::string& prefix = "",
                    bool include_body = false);

    /**
     * Extract client information from request
     * Gets client IP, user agent, and other metadata
     *
     * @param request The request to analyze
     * @return JSON object with client information
     */
    nlohmann::json extract_client_info(const std::shared_ptr<http_req>& request);

    /**
     * Check if request requires leader processing
     * Determines if request can be served by follower or needs leader
     *
     * @param request The request to check
     * @return True if leader processing is required
     */
    bool requires_leader_processing(const std::shared_ptr<http_req>& request);

} // namespace raft_http
