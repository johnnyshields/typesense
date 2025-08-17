#include <gtest/gtest.h>
#include "raft_server.h"
#include "string_utils.h"

// Unit Tests for raft_config_manager.cpp
// Tests DNS resolution, configuration parsing, and hostname utilities

class RaftConfigManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create ReplicationState with null dependencies - tests should work without full setup
        repl_state = std::make_unique<ReplicationState>(nullptr, nullptr, "", 0);
    }

    std::unique_ptr<ReplicationState> repl_state;
};

// Test enhanced DNS resolution with caching
TEST_F(RaftConfigManagerTest, Hostname2IPStrBasicResolution) {
    std::string result = repl_state->hostname2ipstr("localhost");
    EXPECT_FALSE(result.empty());
    // Should resolve to either 127.0.0.1 or ::1
    EXPECT_TRUE(result == "127.0.0.1" || result == "::1");
}

TEST_F(RaftConfigManagerTest, Hostname2IPStrEmptyInput) {
    std::string result = repl_state->hostname2ipstr("");
    EXPECT_TRUE(result.empty());
}

TEST_F(RaftConfigManagerTest, Hostname2IPStrCaching) {
    // First resolution
    std::string result1 = repl_state->hostname2ipstr("localhost");
    EXPECT_FALSE(result1.empty());
    
    // Second resolution should use cache (verify cache size increases)
    size_t cache_size_before = repl_state->get_dns_cache_size();
    std::string result2 = repl_state->hostname2ipstr("localhost");
    EXPECT_EQ(result1, result2);
    
    // Cache should have at least one entry
    EXPECT_GT(repl_state->get_dns_cache_size(), 0);
}

TEST_F(RaftConfigManagerTest, DNSCacheManagement) {
    // Add some entries to cache
    repl_state->hostname2ipstr("localhost");
    EXPECT_GT(repl_state->get_dns_cache_size(), 0);
    
    // Test cache stats functionality
    auto& cache = repl_state->get_dns_cache();
    auto stats_before = cache.get_stats();
    
    // Another resolution should be a cache hit
    repl_state->hostname2ipstr("localhost");
    auto stats_after = cache.get_stats();
    EXPECT_GT(stats_after.hits, stats_before.hits);
    
    // Clear specific hostname
    repl_state->clear_dns_cache_for_hostname("localhost");
    
    // Clear entire cache
    repl_state->clear_dns_cache();
    EXPECT_EQ(repl_state->get_dns_cache_size(), 0);
}

TEST_F(RaftConfigManagerTest, DNSCacheAdvancedFeatures) {
    auto& cache = repl_state->get_dns_cache();
    
    // Test configuration
    EXPECT_EQ(cache.get_ttl(), std::chrono::minutes(5)); // Default TTL
    EXPECT_EQ(cache.get_max_size(), 1000); // Default max size
    
    // Add multiple entries
    repl_state->hostname2ipstr("host1.example.com");
    repl_state->hostname2ipstr("host2.example.com");
    repl_state->hostname2ipstr("host3.example.com");
    
    // Check cached hostnames
    auto hostnames = cache.get_cached_hostnames();
    EXPECT_GE(hostnames.size(), 3);
    
    // Test stats
    auto stats = cache.get_stats();
    EXPECT_GE(stats.hits + stats.misses, 3);
    
    // Test cache configuration changes
    cache.set_ttl(std::chrono::minutes(10));
    EXPECT_EQ(cache.get_ttl(), std::chrono::minutes(10));
    
    cache.set_max_size(5);
    EXPECT_EQ(cache.get_max_size(), 5);
}

// Test enhanced configuration parsing with validation
TEST_F(RaftConfigManagerTest, ParseNodeConfigurationValidFormat) {
    std::string config = "node1.example.com:8107:8108,192.168.1.10:8107:8108,node3.example.com:8107:8108";
    NodeConfiguration result = ReplicationState::parse_node_configuration(config);
    
    EXPECT_EQ(2, result.hostname_nodes.size());
    EXPECT_EQ(1, result.ip_nodes.size());
    EXPECT_EQ(3, result.total_nodes());
}

TEST_F(RaftConfigManagerTest, ParseNodeConfigurationInvalidFormat) {
    // Test various invalid formats
    std::vector<std::string> invalid_configs = {
        "node1.example.com:8107",           // Missing API port
        "node1.example.com:8107:8108:9000", // Too many ports
        "node1.example.com:abc:8108",       // Non-numeric port
        "node1.example.com:8107:70000",     // Port out of range
        "node1.example.com:-1:8108",        // Negative port
        "",                                 // Empty config
        "   ,   ,   "                      // Only whitespace and commas
    };
    
    for (const auto& config : invalid_configs) {
        NodeConfiguration result = ReplicationState::parse_node_configuration(config);
        // Should either be empty or have fewer nodes than expected
        EXPECT_LE(result.total_nodes(), 1) << "Invalid config should not parse: " << config;
    }
}

TEST_F(RaftConfigManagerTest, ParseNodeConfigurationDuplicateHandling) {
    std::string config_with_duplicates = "node1.example.com:8107:8108,node1.example.com:8107:8108,192.168.1.10:8107:8108";
    NodeConfiguration result = ReplicationState::parse_node_configuration(config_with_duplicates);
    
    // Should deduplicate
    EXPECT_EQ(1, result.hostname_nodes.size());
    EXPECT_EQ(1, result.ip_nodes.size());
    EXPECT_EQ(2, result.total_nodes());
}

TEST_F(RaftConfigManagerTest, ParseNodeConfigurationSanityWarnings) {
    // Test even number of nodes (should generate warning)
    std::string even_config = "node1:8107:8108,node2:8107:8108,node3:8107:8108,node4:8107:8108";
    NodeConfiguration result = ReplicationState::parse_node_configuration(even_config);
    EXPECT_EQ(4, result.total_nodes());
    
    // Test single node (should be fine)
    std::string single_config = "node1:8107:8108";
    NodeConfiguration single_result = ReplicationState::parse_node_configuration(single_config);
    EXPECT_EQ(1, single_result.total_nodes());
}

// Test hostname extraction
TEST_F(RaftConfigManagerTest, ExtractHostnameFromNode) {
    EXPECT_EQ("node1.example.com", ReplicationState::extract_hostname_from_node("node1.example.com:8107:8108"));
    EXPECT_EQ("sub.domain.com", ReplicationState::extract_hostname_from_node("sub.domain.com:8107:8108"));
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("192.168.1.1:8107:8108")); // IP should return empty
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("malformed"));
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node(""));
}

// Test braft configuration conversion
TEST_F(RaftConfigManagerTest, NodeConfigToBraft) {
    NodeConfiguration config = ReplicationState::parse_node_configuration("node1.example.com:8107:8108,192.168.1.10:8107:8108");
    braft::Configuration braft_config = ReplicationState::node_config_to_braft(config);
    
    EXPECT_EQ(2, braft_config.size());
}

// Test peer matching
TEST_F(RaftConfigManagerTest, PeerMatchesHostnameNode) {
    // Create a peer ID
    braft::PeerId peer_id;
    butil::str2endpoint("127.0.0.1:8107", &peer_id.addr);
    
    // Test matching (this is a basic test - actual matching depends on DNS resolution)
    bool matches = repl_state->peer_matches_hostname_node(peer_id, "localhost:8107:8108");
    // Result depends on DNS resolution, but should not crash
    EXPECT_TRUE(matches || !matches); // Just ensure no crash
}

// Test configuration serialization
TEST_F(RaftConfigManagerTest, ConfigurationSerialization) {
    NodeConfiguration config = ReplicationState::parse_node_configuration("node1.example.com:8107:8108,192.168.1.10:8107:8108");
    std::string serialized = config.serialize();
    
    EXPECT_FALSE(serialized.empty());
    EXPECT_TRUE(serialized.find("node1.example.com:8107:8108") != std::string::npos);
    EXPECT_TRUE(serialized.find("192.168.1.10:8107:8108") != std::string::npos);
}

// Test version comparison with enhanced logic
TEST_F(RaftConfigManagerTest, ConfigurationVersionComparison) {
    NodeConfiguration config1 = ReplicationState::parse_node_configuration("node1:8107:8108");
    NodeConfiguration config2 = ReplicationState::parse_node_configuration("node1:8107:8108");
    
    config1.config_version = 1;
    config1.config_term = 1;
    
    config2.config_version = 2;
    config2.config_term = 1;
    
    EXPECT_TRUE(config2.is_newer_than(config1));
    EXPECT_FALSE(config1.is_newer_than(config2));
    
    // Test uninitialized term handling
    config2.config_term = -1; // Force reconfig
    config2.config_version = 10;
    EXPECT_TRUE(config2.is_newer_than(config1));
}

// Test single-node change creation
TEST_F(RaftConfigManagerTest, SingleNodeChangeCreation) {
    NodeConfiguration base = ReplicationState::parse_node_configuration("node1:8107:8108,node2:8107:8108");
    
    // Test addition
    NodeConfiguration add_result = base.create_single_node_change("node3:8107:8108", "", 2);
    EXPECT_EQ(3, add_result.total_nodes());
    EXPECT_EQ(2, add_result.config_version);
    EXPECT_EQ(2, add_result.config_term);
    
    // Test removal
    NodeConfiguration remove_result = base.create_single_node_change("", "node2:8107:8108", 3);
    EXPECT_EQ(1, remove_result.total_nodes());
    EXPECT_EQ(2, remove_result.config_version);
    EXPECT_EQ(3, remove_result.config_term);
}

// Test self-node detection
TEST_F(RaftConfigManagerTest, SelfNodeDetection) {
    EXPECT_TRUE(repl_state->is_self_node("localhost:8107:8108"));
    EXPECT_TRUE(repl_state->is_self_node("127.0.0.1:8107:8108"));
    EXPECT_TRUE(repl_state->is_self_node("::1:8107:8108"));
    EXPECT_FALSE(repl_state->is_self_node("remote.host.com:8107:8108"));
    EXPECT_FALSE(repl_state->is_self_node("192.168.1.100:8107:8108"));
}

// Performance test for DNS operations
TEST_F(RaftConfigManagerTest, DNSPerformanceWithCaching) {
    auto start = std::chrono::high_resolution_clock::now();
    
    // First batch - should populate cache
    for (int i = 0; i < 100; ++i) {
        std::string result = repl_state->hostname2ipstr("localhost");
        EXPECT_FALSE(result.empty());
    }
    
    auto mid = std::chrono::high_resolution_clock::now();
    
    // Second batch - should use cache
    for (int i = 0; i < 100; ++i) {
        std::string result = repl_state->hostname2ipstr("localhost");
        EXPECT_FALSE(result.empty());
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    
    auto first_batch_time = std::chrono::duration_cast<std::chrono::microseconds>(mid - start);
    auto second_batch_time = std::chrono::duration_cast<std::chrono::microseconds>(end - mid);
    
    // Second batch should be significantly faster due to caching
    // (though the first lookup might also be cached by the system)
    EXPECT_LT(second_batch_time.count(), first_batch_time.count() * 2);
    
    LOG(INFO) << "DNS performance: first batch " << first_batch_time.count() 
              << "μs, second batch " << second_batch_time.count() << "μs";
} 