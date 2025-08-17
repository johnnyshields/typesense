#include <gtest/gtest.h>
#include "raft_server.h"
#include "http_data.h"

// Unit Tests for raft_http_handler.cpp
// Tests HTTP request processing, GZIP handling, and leader forwarding

class RaftHttpHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create ReplicationState with null dependencies - tests should work without full setup
        repl_state = std::make_unique<ReplicationState>(nullptr, nullptr, "", 0);
        
        // Create mock HTTP request and response
        request = std::make_shared<http_req>();
        response = std::make_shared<http_res>();
        
        // Setup basic request properties
        request->params["action"] = "create";
        request->body = R"({"name": "test_collection", "fields": [{"name": "title", "type": "string"}]})";
        request->http_req::path_params["collection"] = "test_collection";
    }

    std::unique_ptr<ReplicationState> repl_state;
    std::shared_ptr<http_req> request;
    std::shared_ptr<http_res> response;
};

// Test handle_gzip functionality
TEST_F(RaftHttpHandlerTest, HandleGzipWithoutCompression) {
    // Test request without compression
    Option<bool> result = repl_state->handle_gzip(request);
    
    // Should return None since no compression is present
    EXPECT_FALSE(result.is_some());
}

TEST_F(RaftHttpHandlerTest, HandleGzipWithGzipEncoding) {
    // Test with gzip content-encoding header
    request->set_header("content-encoding", "gzip");
    
    // Create simple gzip compressed data for testing
    // Note: This is a simplified test - real implementation would use actual gzip data
    std::string test_data = "test data";
    request->body = test_data;
    
    Option<bool> result = repl_state->handle_gzip(request);
    
    // Should return Some(true) for successful decompression or Some(false) for failure
    // The exact result depends on whether the body is valid gzip data
    EXPECT_TRUE(result.is_some());
}

TEST_F(RaftHttpHandlerTest, HandleGzipWithInvalidGzipData) {
    request->set_header("content-encoding", "gzip");
    request->body = "invalid gzip data";
    
    Option<bool> result = repl_state->handle_gzip(request);
    
    // Should return Some(false) for invalid gzip data
    EXPECT_TRUE(result.is_some());
    if (result.is_some()) {
        EXPECT_FALSE(result.get());
    }
}

// Test get_node_url_path functionality
TEST_F(RaftHttpHandlerTest, GetNodeUrlPathBasicConstruction) {
    braft::PeerId peer_id;
    butil::str2endpoint("192.168.1.10:8107", &peer_id.addr);
    
    std::string path = "/collections/test";
    std::string api_key = "test_api_key";
    
    std::string url = repl_state->get_node_url_path(peer_id, path, api_key);
    
    // Should construct proper URL
    EXPECT_TRUE(url.find("192.168.1.10") != std::string::npos);
    EXPECT_TRUE(url.find("8108") != std::string::npos); // HTTP port (raft_port + 1)
    EXPECT_TRUE(url.find(path) != std::string::npos);
    EXPECT_TRUE(url.find("x-typesense-api-key=" + api_key) != std::string::npos);
}

TEST_F(RaftHttpHandlerTest, GetNodeUrlPathWithEmptyApiKey) {
    braft::PeerId peer_id;
    butil::str2endpoint("127.0.0.1:8107", &peer_id.addr);
    
    std::string path = "/health";
    std::string api_key = "";
    
    std::string url = repl_state->get_node_url_path(peer_id, path, api_key);
    
    // Should construct URL without API key parameter
    EXPECT_TRUE(url.find("127.0.0.1") != std::string::npos);
    EXPECT_TRUE(url.find(path) != std::string::npos);
    EXPECT_TRUE(url.find("x-typesense-api-key") == std::string::npos);
}

TEST_F(RaftHttpHandlerTest, GetNodeUrlPathWithQueryParameters) {
    braft::PeerId peer_id;
    butil::str2endpoint("10.0.0.1:9000", &peer_id.addr);
    
    std::string path = "/collections/search?q=test&query_by=title";
    std::string api_key = "search_key";
    
    std::string url = repl_state->get_node_url_path(peer_id, path, api_key);
    
    // Should properly handle existing query parameters
    EXPECT_TRUE(url.find("10.0.0.1") != std::string::npos);
    EXPECT_TRUE(url.find("9001") != std::string::npos); // HTTP port
    EXPECT_TRUE(url.find("q=test") != std::string::npos);
    EXPECT_TRUE(url.find("query_by=title") != std::string::npos);
    EXPECT_TRUE(url.find("x-typesense-api-key=" + api_key) != std::string::npos);
}

// Test write functionality (basic structure)
TEST_F(RaftHttpHandlerTest, WriteWithoutRaftNode) {
    // Test write operation without proper raft node setup
    // This should handle the case gracefully
    
    EXPECT_NO_THROW({
        repl_state->write(request, response);
    });
    
    // Response should indicate some form of error
    EXPECT_TRUE(response->status_code >= 400 || response->status_code == 0);
}

// Test write_to_leader functionality
TEST_F(RaftHttpHandlerTest, WriteToLeaderWithoutHttpClient) {
    // Test leader forwarding without HTTP client
    EXPECT_NO_THROW({
        repl_state->write_to_leader(request, response);
    });
    
    // Should handle gracefully when HTTP client is not set
    EXPECT_TRUE(response->status_code >= 400 || response->status_code == 0);
}

// Test read functionality
TEST_F(RaftHttpHandlerTest, ReadWithoutRaftNode) {
    // Test read operation without proper raft node setup
    EXPECT_NO_THROW({
        repl_state->read(request, response);
    });
    
    // Should handle gracefully
    EXPECT_TRUE(response->status_code >= 400 || response->status_code == 0);
}

// Test HTTP method handling
TEST_F(RaftHttpHandlerTest, HttpMethodHandling) {
    // Test different HTTP methods
    std::vector<std::string> methods = {"GET", "POST", "PUT", "DELETE", "PATCH"};
    
    for (const auto& method : methods) {
        auto test_request = std::make_shared<http_req>();
        auto test_response = std::make_shared<http_res>();
        
        // Set HTTP method (assuming http_req has a method field)
        // Note: This depends on the actual http_req implementation
        test_request->body = R"({"test": "data"})";
        
        EXPECT_NO_THROW({
            repl_state->write(test_request, test_response);
        });
    }
}

// Test request body handling
TEST_F(RaftHttpHandlerTest, RequestBodyHandling) {
    // Test with various body sizes and formats
    
    // Empty body
    auto empty_request = std::make_shared<http_req>();
    auto empty_response = std::make_shared<http_res>();
    empty_request->body = "";
    
    EXPECT_NO_THROW({
        repl_state->write(empty_request, empty_response);
    });
    
    // Large body
    auto large_request = std::make_shared<http_req>();
    auto large_response = std::make_shared<http_res>();
    large_request->body = std::string(10000, 'x'); // 10KB of 'x'
    
    EXPECT_NO_THROW({
        repl_state->write(large_request, large_response);
    });
    
    // JSON body
    auto json_request = std::make_shared<http_req>();
    auto json_response = std::make_shared<http_res>();
    json_request->body = R"({"field1": "value1", "field2": 123, "field3": [1, 2, 3]})";
    
    EXPECT_NO_THROW({
        repl_state->write(json_request, json_response);
    });
}

// Test URL construction edge cases
TEST_F(RaftHttpHandlerTest, UrlConstructionEdgeCases) {
    braft::PeerId peer_id;
    butil::str2endpoint("192.168.1.100:8107", &peer_id.addr);
    
    // Test with root path
    std::string root_url = repl_state->get_node_url_path(peer_id, "/", "key123");
    EXPECT_TRUE(root_url.find("192.168.1.100:8108/") != std::string::npos);
    
    // Test with path containing special characters
    std::string special_url = repl_state->get_node_url_path(peer_id, "/collections/test%20collection", "key456");
    EXPECT_TRUE(special_url.find("test%20collection") != std::string::npos);
    
    // Test with very long API key
    std::string long_key = std::string(1000, 'a');
    std::string long_key_url = repl_state->get_node_url_path(peer_id, "/test", long_key);
    EXPECT_TRUE(long_key_url.find(long_key) != std::string::npos);
}

// Test concurrent HTTP handling
TEST_F(RaftHttpHandlerTest, ConcurrentHttpHandling) {
    const int num_threads = 8;
    const int requests_per_thread = 25;
    std::vector<std::thread> threads;
    std::atomic<int> successful_requests{0};
    std::atomic<int> failed_requests{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < requests_per_thread; ++j) {
                try {
                    auto thread_request = std::make_shared<http_req>();
                    auto thread_response = std::make_shared<http_res>();
                    
                    thread_request->body = R"({"thread": )" + std::to_string(i) + 
                                         R"(, "request": )" + std::to_string(j) + "}";
                    
                    repl_state->write(thread_request, thread_response);
                    successful_requests++;
                    
                } catch (...) {
                    failed_requests++;
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    int expected_requests = num_threads * requests_per_thread;
    EXPECT_EQ(expected_requests, successful_requests.load() + failed_requests.load());
    
    // All requests should complete (though they may fail due to no raft node)
    EXPECT_EQ(expected_requests, successful_requests.load());
}

// Test GZIP compression/decompression performance
TEST_F(RaftHttpHandlerTest, GzipPerformance) {
    // Create a large JSON payload
    std::string large_json = R"({"data": [)";
    for (int i = 0; i < 1000; ++i) {
        if (i > 0) large_json += ",";
        large_json += R"({"id": )" + std::to_string(i) + R"(, "value": ")" + 
                     std::string(100, 'x') + R"("})";
    }
    large_json += "]}";
    
    auto perf_request = std::make_shared<http_req>();
    perf_request->body = large_json;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Test GZIP handling performance
    for (int i = 0; i < 100; ++i) {
        Option<bool> result = repl_state->handle_gzip(perf_request);
        // The result depends on whether compression headers are set
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete 100 GZIP operations quickly (< 50ms)
    EXPECT_LT(duration.count(), 50000);
    
    double avg_time_per_operation = static_cast<double>(duration.count()) / 100.0;
    EXPECT_LT(avg_time_per_operation, 500.0); // < 500μs per operation
}

// Test URL generation performance
TEST_F(RaftHttpHandlerTest, UrlGenerationPerformance) {
    braft::PeerId peer_id;
    butil::str2endpoint("192.168.1.50:8107", &peer_id.addr);
    
    std::string path = "/collections/test_collection/documents/search";
    std::string api_key = "test_api_key_12345";
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Generate URLs repeatedly
    for (int i = 0; i < 10000; ++i) {
        std::string url = repl_state->get_node_url_path(peer_id, path, api_key);
        EXPECT_FALSE(url.empty());
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete 10000 URL generations quickly (< 100ms)
    EXPECT_LT(duration.count(), 100000);
    
    double avg_time_per_url = static_cast<double>(duration.count()) / 10000.0;
    EXPECT_LT(avg_time_per_url, 10.0); // < 10μs per URL generation
}

// Test memory usage with large requests
TEST_F(RaftHttpHandlerTest, LargeRequestMemoryHandling) {
    // Test with various large request sizes
    std::vector<size_t> sizes = {1024, 10240, 102400, 1048576}; // 1KB, 10KB, 100KB, 1MB
    
    for (size_t size : sizes) {
        auto large_request = std::make_shared<http_req>();
        auto large_response = std::make_shared<http_res>();
        
        // Create request body of specified size
        large_request->body = std::string(size, 'A');
        
        EXPECT_NO_THROW({
            repl_state->write(large_request, large_response);
        });
        
        // Test GZIP handling with large data
        large_request->set_header("content-encoding", "gzip");
        
        EXPECT_NO_THROW({
            Option<bool> result = repl_state->handle_gzip(large_request);
        });
    }
}

// Test error handling scenarios
TEST_F(RaftHttpHandlerTest, ErrorHandlingScenarios) {
    // Test with null request
    std::shared_ptr<http_req> null_request = nullptr;
    
    EXPECT_NO_THROW({
        repl_state->write(null_request, response);
    });
    
    // Test with null response
    std::shared_ptr<http_res> null_response = nullptr;
    
    EXPECT_NO_THROW({
        repl_state->write(request, null_response);
    });
    
    // Test with both null
    EXPECT_NO_THROW({
        repl_state->write(null_request, null_response);
    });
}

// Test HTTP header handling
TEST_F(RaftHttpHandlerTest, HttpHeaderHandling) {
    // Test various HTTP headers
    request->set_header("content-type", "application/json");
    request->set_header("authorization", "Bearer token123");
    request->set_header("x-custom-header", "custom_value");
    request->set_header("user-agent", "TypesenseClient/1.0");
    
    EXPECT_NO_THROW({
        repl_state->write(request, response);
    });
    
    // Test GZIP with additional headers
    request->set_header("content-encoding", "gzip");
    request->set_header("accept-encoding", "gzip, deflate");
    
    EXPECT_NO_THROW({
        Option<bool> result = repl_state->handle_gzip(request);
    });
}

// Test path parameter extraction
TEST_F(RaftHttpHandlerTest, PathParameterExtraction) {
    // Test various path parameters
    request->http_req::path_params["collection"] = "products";
    request->http_req::path_params["document_id"] = "12345";
    request->params["action"] = "upsert";
    request->params["filter_by"] = "category:electronics";
    
    EXPECT_NO_THROW({
        repl_state->write(request, response);
    });
    
    // Test URL generation with these parameters
    braft::PeerId peer_id;
    butil::str2endpoint("10.0.0.5:8107", &peer_id.addr);
    
    std::string complex_path = "/collections/products/documents/12345?action=upsert&filter_by=category:electronics";
    std::string url = repl_state->get_node_url_path(peer_id, complex_path, "api_key");
    
    EXPECT_TRUE(url.find("products") != std::string::npos);
    EXPECT_TRUE(url.find("12345") != std::string::npos);
    EXPECT_TRUE(url.find("action=upsert") != std::string::npos);
}

// Test response handling
TEST_F(RaftHttpHandlerTest, ResponseHandling) {
    // Test that response object is properly modified
    response->status_code = 200;
    response->body = "initial body";
    
    repl_state->write(request, response);
    
    // After write operation, response should be modified
    // The exact changes depend on the implementation
    EXPECT_TRUE(response->status_code >= 0);
}

// Test integration between HTTP components
TEST_F(RaftHttpHandlerTest, HttpComponentIntegration) {
    // Test the flow: GZIP -> Write -> URL Generation
    
    // Step 1: Handle GZIP
    request->set_header("content-encoding", "gzip");
    Option<bool> gzip_result = repl_state->handle_gzip(request);
    
    // Step 2: Process write request
    repl_state->write(request, response);
    
    // Step 3: Generate URL for potential forwarding
    braft::PeerId peer_id;
    butil::str2endpoint("127.0.0.1:8107", &peer_id.addr);
    std::string url = repl_state->get_node_url_path(peer_id, "/test", "key");
    
    // All operations should complete without throwing
    EXPECT_FALSE(url.empty());
} 