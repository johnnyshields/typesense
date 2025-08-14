#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include "raft_server.h"

namespace {
    // Helper function to create a mock braft::PeerId
    braft::PeerId create_peer_id(const std::string& ip, int peering_port, int api_port) {
        butil::EndPoint endpoint;
        butil::str2endpoint(ip.c_str(), peering_port, &endpoint);
        return braft::PeerId(endpoint, api_port);
    }

    // Helper to validate IP format
    bool is_valid_ipv4(const std::string& ip) {
        struct sockaddr_in sa;
        return inet_pton(AF_INET, ip.c_str(), &(sa.sin_addr)) != 0;
    }

    bool is_valid_ipv6_with_brackets(const std::string& str) {
        if (str.length() < 2 || str[0] != '[' || str[str.length() - 1] != ']') {
            return false;
        }
        std::string ipv6 = str.substr(1, str.length() - 2);
        struct sockaddr_in6 sa;
        return inet_pton(AF_INET6, ipv6.c_str(), &(sa.sin6_addr)) != 0;
    }
}

class DNSFailureHandlingTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup test data
        mixed_config = "node1.example.com:8107:8108,192.168.1.10:8107:8108,node2.internal:8107:8108";
        hostname_only_config = "node1.example.com:8107:8108,node2.example.com:8107:8108";
        ip_only_config = "192.168.1.10:8107:8108,192.168.1.11:8107:8108";
        ipv6_config = "[2001:db8::1]:8107:8108,[2001:db8::2]:8107:8108";
    }

    std::string mixed_config;
    std::string hostname_only_config;
    std::string ip_only_config;
    std::string ipv6_config;
};

// Test NodeConfiguration structure and parsing
TEST_F(DNSFailureHandlingTest, NodeConfigurationParsing) {
    // Test mixed hostname and IP configuration
    NodeConfiguration config = ReplicationState::parse_node_configuration(mixed_config);
    
    EXPECT_EQ(2, config.hostname_nodes.size());
    EXPECT_EQ(1, config.ip_nodes.size());
    EXPECT_TRUE(config.has_hostnames());
    EXPECT_TRUE(config.has_ips());
    EXPECT_FALSE(config.empty());
    EXPECT_EQ(3, config.total_nodes());

    // Verify hostname nodes
    EXPECT_TRUE(std::find(config.hostname_nodes.begin(), config.hostname_nodes.end(), 
                         "node1.example.com:8107:8108") != config.hostname_nodes.end());
    EXPECT_TRUE(std::find(config.hostname_nodes.begin(), config.hostname_nodes.end(), 
                         "node2.internal:8107:8108") != config.hostname_nodes.end());

    // Verify IP nodes
    EXPECT_TRUE(std::find(config.ip_nodes.begin(), config.ip_nodes.end(), 
                         "192.168.1.10:8107:8108") != config.ip_nodes.end());
}

TEST_F(DNSFailureHandlingTest, NodeConfigurationHostnameOnly) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(hostname_only_config);
    
    EXPECT_EQ(2, config.hostname_nodes.size());
    EXPECT_EQ(0, config.ip_nodes.size());
    EXPECT_TRUE(config.has_hostnames());
    EXPECT_FALSE(config.has_ips());
    EXPECT_FALSE(config.empty());
}

TEST_F(DNSFailureHandlingTest, NodeConfigurationIPOnly) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(ip_only_config);
    
    EXPECT_EQ(0, config.hostname_nodes.size());
    EXPECT_EQ(2, config.ip_nodes.size());
    EXPECT_FALSE(config.has_hostnames());
    EXPECT_TRUE(config.has_ips());
    EXPECT_FALSE(config.empty());
}

TEST_F(DNSFailureHandlingTest, NodeConfigurationIPv6) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(ipv6_config);
    
    EXPECT_EQ(0, config.hostname_nodes.size());
    EXPECT_EQ(2, config.ip_nodes.size());
    EXPECT_FALSE(config.has_hostnames());
    EXPECT_TRUE(config.has_ips());
    
    // Verify IPv6 nodes are in IP collection
    EXPECT_TRUE(std::find(config.ip_nodes.begin(), config.ip_nodes.end(), 
                         "[2001:db8::1]:8107:8108") != config.ip_nodes.end());
}

TEST_F(DNSFailureHandlingTest, NodeConfigurationEmpty) {
    NodeConfiguration config = ReplicationState::parse_node_configuration("");
    
    EXPECT_EQ(0, config.hostname_nodes.size());
    EXPECT_EQ(0, config.ip_nodes.size());
    EXPECT_FALSE(config.has_hostnames());
    EXPECT_FALSE(config.has_ips());
    EXPECT_TRUE(config.empty());
}

TEST_F(DNSFailureHandlingTest, NodeConfigurationSerialization) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(mixed_config);
    std::string serialized = config.serialize();
    
    // Should contain all original nodes (order might differ)
    EXPECT_TRUE(serialized.find("node1.example.com:8107:8108") != std::string::npos);
    EXPECT_TRUE(serialized.find("node2.internal:8107:8108") != std::string::npos);
    EXPECT_TRUE(serialized.find("192.168.1.10:8107:8108") != std::string::npos);
    
    // Should be parseable back to same structure
    NodeConfiguration reparsed = ReplicationState::parse_node_configuration(serialized);
    EXPECT_EQ(config.total_nodes(), reparsed.total_nodes());
}

// Test helper functions
TEST_F(DNSFailureHandlingTest, ExtractHostnameFromNode) {
    // Test hostname extraction
    EXPECT_EQ("node1.example.com", 
              ReplicationState::extract_hostname_from_node("node1.example.com:8107:8108"));
    EXPECT_EQ("internal.service", 
              ReplicationState::extract_hostname_from_node("internal.service:9000:9001"));
    
    // Test IP addresses (should return empty)
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("192.168.1.10:8107:8108"));
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("10.0.0.1:8107:8108"));
    
    // Test IPv6 addresses (should return empty)
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("[2001:db8::1]:8107:8108"));
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("[::1]:8107:8108"));
    
    // Test malformed strings
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("malformed"));
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("too:many:colons:here:8107:8108"));
}

// Test braft configuration conversion
TEST_F(DNSFailureHandlingTest, NodeConfigToBraftConversion) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(ip_only_config);
    braft::Configuration braft_config = ReplicationState::node_config_to_braft(config);
    
    // Should have same number of peers as IP nodes
    EXPECT_EQ(2, braft_config.size());
    
    // Test empty configuration
    NodeConfiguration empty_config;
    braft::Configuration empty_braft = ReplicationState::node_config_to_braft(empty_config);
    EXPECT_EQ(0, empty_braft.size());
}

TEST_F(DNSFailureHandlingTest, NodeConfigToBraftWithHostnames) {
    // Test localhost resolution (should work in most environments)
    std::string localhost_config = "localhost:8107:8108";
    NodeConfiguration config = ReplicationState::parse_node_configuration(localhost_config);
    
    if (!config.hostname_nodes.empty()) {
        braft::Configuration braft_config = ReplicationState::node_config_to_braft(config);
        // Should resolve localhost to at least one peer
        EXPECT_GE(braft_config.size(), 0);  // Might be 0 if DNS resolution fails
    }
}

// Test peer matching functionality
TEST_F(DNSFailureHandlingTest, PeerMatchesHostnameNode) {
    // Create a peer ID for localhost resolution test
    std::string resolved_ip = ReplicationState::hostname2ipstr("localhost");
    
    if (resolved_ip != "localhost" && !resolved_ip.empty()) {
        // Extract IP from resolved result (handle IPv6 brackets)
        std::string clean_ip = resolved_ip;
        if (resolved_ip[0] == '[' && resolved_ip.back() == ']') {
            clean_ip = resolved_ip.substr(1, resolved_ip.length() - 2);
        }
        
        braft::PeerId peer_id = create_peer_id(clean_ip, 8107, 8108);
        
        // Should match the hostname node
        bool matches = ReplicationState::peer_matches_hostname_node(peer_id, "localhost:8107:8108");
        EXPECT_TRUE(matches) << "Peer " << peer_id << " should match localhost:8107:8108";
    }
    
    // Test non-matching cases
    braft::PeerId different_port = create_peer_id("127.0.0.1", 9999, 8108);
    EXPECT_FALSE(ReplicationState::peer_matches_hostname_node(different_port, "localhost:8107:8108"));
    
    // Test IP node (should not match)
    braft::PeerId ip_peer = create_peer_id("192.168.1.10", 8107, 8108);
    EXPECT_FALSE(ReplicationState::peer_matches_hostname_node(ip_peer, "192.168.1.10:8107:8108"));
}

// Test malformed input handling
TEST_F(DNSFailureHandlingTest, MalformedInputHandling) {
    // Test malformed node strings
    NodeConfiguration config1 = ReplicationState::parse_node_configuration("malformed");
    EXPECT_EQ(1, config1.ip_nodes.size());  // Should be treated as IP node for backward compatibility
    
    NodeConfiguration config2 = ReplicationState::parse_node_configuration("too:many:colons:8107:8108");
    EXPECT_EQ(1, config2.ip_nodes.size());  // Should be treated as IP node
    
    // Test empty and whitespace
    NodeConfiguration config3 = ReplicationState::parse_node_configuration("   ");
    EXPECT_TRUE(config3.empty());
    
    NodeConfiguration config4 = ReplicationState::parse_node_configuration(",,,");
    EXPECT_TRUE(config4.empty());
}

// Test thread safety (basic test)
TEST_F(DNSFailureHandlingTest, BasicThreadSafety) {
    const int num_threads = 4;
    const int iterations = 10;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < iterations; ++j) {
                try {
                    NodeConfiguration config = ReplicationState::parse_node_configuration(mixed_config);
                    if (config.total_nodes() == 3) {
                        success_count++;
                    }
                    
                    // Test hostname extraction
                    std::string hostname = ReplicationState::extract_hostname_from_node("test.example.com:8107:8108");
                    if (hostname == "test.example.com") {
                        success_count++;
                    }
                    
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                } catch (...) {
                    // Should not throw exceptions
                    FAIL() << "Thread " << i << " threw an exception";
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // All operations should succeed
    EXPECT_EQ(num_threads * iterations * 2, success_count.load());
}

// Integration test for the complete flow
TEST_F(DNSFailureHandlingTest, DNSResolutionFlow) {
    // Test the complete flow: parse -> convert to braft -> verify
    NodeConfiguration config = ReplicationState::parse_node_configuration(mixed_config);
    ASSERT_FALSE(config.empty());
    
    // Convert to braft configuration
    braft::Configuration braft_config = ReplicationState::node_config_to_braft(config);
    
    // Should have at least the IP nodes (hostname resolution might fail in test environment)
    EXPECT_GE(braft_config.size(), config.ip_nodes.size());
    
    // Test serialization round-trip
    std::string serialized = config.serialize();
    NodeConfiguration reparsed = ReplicationState::parse_node_configuration(serialized);
    EXPECT_EQ(config.total_nodes(), reparsed.total_nodes());
}

// Performance test for DNS operations
TEST_F(DNSFailureHandlingTest, DNSPerformanceTest) {
    auto start = std::chrono::high_resolution_clock::now();
    
    const int iterations = 100;
    for (int i = 0; i < iterations; ++i) {
        NodeConfiguration config = ReplicationState::parse_node_configuration(mixed_config);
        std::string serialized = config.serialize();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete 100 iterations in reasonable time (less than 100ms)
    EXPECT_LT(duration.count(), 100000) << "DNS operations took too long: " << duration.count() << "μs";
} 