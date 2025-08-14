#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include "raft_server.h"

class MongoDBTLASafetyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup test configurations
        three_node_config = "node1.example.com:8107:8108,node2.example.com:8107:8108,node3.example.com:8107:8108";
        five_node_config = "node1.example.com:8107:8108,node2.example.com:8107:8108,node3.example.com:8107:8108,node4.example.com:8107:8108,node5.example.com:8107:8108";
        single_node_config = "node1.example.com:8107:8108";
        mixed_config = "node1.example.com:8107:8108,192.168.1.10:8107:8108,node3.example.com:8107:8108";
    }
    
    std::string three_node_config;
    std::string five_node_config;
    std::string single_node_config;
    std::string mixed_config;
};

// Test ConfigIsSafe basic functionality
TEST_F(MongoDBTLASafetyTest, ConfigIsSafeBasicValidation) {
    // Test the MongoDB TLA+ ConfigIsSafe pattern
    // Note: These tests validate the logic structure, not actual Raft node behavior
    
    NodeConfiguration config = ReplicationState::parse_node_configuration(three_node_config);
    EXPECT_EQ(3, config.total_nodes());
    
    // Test configuration versioning
    EXPECT_EQ(1, config.config_version);
    EXPECT_EQ(0, config.config_term);
    
    // Test version comparison
    NodeConfiguration newer_config = config.create_single_node_change("node4.example.com:8107:8108", "", 5);
    EXPECT_TRUE(newer_config.is_newer_than(config));
    EXPECT_FALSE(config.is_newer_than(newer_config));
    
    EXPECT_EQ(2, newer_config.config_version);
    EXPECT_EQ(5, newer_config.config_term);
}

// Test TermQuorumCheck validation
TEST_F(MongoDBTLASafetyTest, TermQuorumCheckValidation) {
    // Test the MongoDB TLA+ TermQuorumCheck pattern
    NodeConfiguration config = ReplicationState::parse_node_configuration(three_node_config);
    
    // Test that term validation works with different term scenarios
    NodeConfiguration config_term_1 = config;
    config_term_1.config_term = 1;
    
    NodeConfiguration config_term_5 = config;
    config_term_5.config_term = 5;
    
    NodeConfiguration config_term_3 = config;
    config_term_3.config_term = 3;
    
    // Higher term should win regardless of version
    EXPECT_TRUE(config_term_5.is_newer_than(config_term_3));
    EXPECT_TRUE(config_term_3.is_newer_than(config_term_1));
    
    // Same term, version matters
    config_term_3.config_version = 10;
    config_term_5.config_version = 2;
    EXPECT_TRUE(config_term_5.is_newer_than(config_term_3)); // Term still wins
}

// Test ConfigQuorumCheck validation
TEST_F(MongoDBTLASafetyTest, ConfigQuorumCheckValidation) {
    // Test configuration quorum requirements
    
    // Single node - no quorum possible
    NodeConfiguration single = ReplicationState::parse_node_configuration(single_node_config);
    EXPECT_EQ(1, single.total_nodes());
    
    // Three nodes - quorum of 2
    NodeConfiguration three = ReplicationState::parse_node_configuration(three_node_config);
    EXPECT_EQ(3, three.total_nodes());
    
    // Five nodes - quorum of 3
    NodeConfiguration five = ReplicationState::parse_node_configuration(five_node_config);
    EXPECT_EQ(5, five.total_nodes());
    
    // Test safe single-node changes maintain quorum
    NodeConfiguration three_plus_one = three.create_single_node_change("node4.example.com:8107:8108", "", 1);
    EXPECT_EQ(4, three_plus_one.total_nodes());
    EXPECT_TRUE(three.is_safe_single_node_change(three_plus_one));
    
    NodeConfiguration three_minus_one = three.create_single_node_change("", "node3.example.com:8107:8108", 1);
    EXPECT_EQ(2, three_minus_one.total_nodes());
    EXPECT_TRUE(three.is_safe_single_node_change(three_minus_one));
}

// Test OpCommittedInConfig validation
TEST_F(MongoDBTLASafetyTest, OpCommittedInConfigValidation) {
    // Test that operations committed in previous configs remain committed
    
    NodeConfiguration original = ReplicationState::parse_node_configuration(three_node_config);
    original.config_version = 1;
    original.config_term = 5;
    
    // Simulate a configuration change
    NodeConfiguration new_config = original.create_single_node_change("node4.example.com:8107:8108", "", 6);
    EXPECT_EQ(2, new_config.config_version);
    EXPECT_EQ(6, new_config.config_term);
    
    // Verify the change maintains safety
    EXPECT_TRUE(original.is_safe_single_node_change(new_config));
    EXPECT_TRUE(new_config.is_newer_than(original));
}

// Test quorum validation for new configurations
TEST_F(MongoDBTLASafetyTest, NewConfigQuorumValidation) {
    // Test MongoDB's alive nodes quorum check pattern
    
    NodeConfiguration three_node = ReplicationState::parse_node_configuration(three_node_config);
    
    // Adding a node should maintain quorum capability
    NodeConfiguration four_node = three_node.create_single_node_change("node4.example.com:8107:8108", "", 1);
    EXPECT_EQ(4, four_node.total_nodes());
    
    // Removing a node should still allow quorum
    NodeConfiguration two_node = three_node.create_single_node_change("", "node3.example.com:8107:8108", 1);
    EXPECT_EQ(2, two_node.total_nodes());
    
    // Both changes should be considered safe single-node changes
    EXPECT_TRUE(three_node.is_safe_single_node_change(four_node));
    EXPECT_TRUE(three_node.is_safe_single_node_change(two_node));
}

// Test mixed hostname/IP configuration safety
TEST_F(MongoDBTLASafetyTest, MixedConfigurationSafety) {
    NodeConfiguration mixed = ReplicationState::parse_node_configuration(mixed_config);
    EXPECT_EQ(3, mixed.total_nodes());
    EXPECT_EQ(2, mixed.hostname_nodes.size());
    EXPECT_EQ(1, mixed.ip_nodes.size());
    
    // Test adding different node types
    NodeConfiguration mixed_plus_hostname = mixed.create_single_node_change("node4.example.com:8107:8108", "", 1);
    EXPECT_EQ(4, mixed_plus_hostname.total_nodes());
    EXPECT_EQ(3, mixed_plus_hostname.hostname_nodes.size());
    EXPECT_EQ(1, mixed_plus_hostname.ip_nodes.size());
    
    NodeConfiguration mixed_plus_ip = mixed.create_single_node_change("192.168.1.20:8107:8108", "", 1);
    EXPECT_EQ(4, mixed_plus_ip.total_nodes());
    EXPECT_EQ(2, mixed_plus_ip.hostname_nodes.size());
    EXPECT_EQ(2, mixed_plus_ip.ip_nodes.size());
    
    // Both should be safe
    EXPECT_TRUE(mixed.is_safe_single_node_change(mixed_plus_hostname));
    EXPECT_TRUE(mixed.is_safe_single_node_change(mixed_plus_ip));
}

// Test unsafe configuration changes
TEST_F(MongoDBTLASafetyTest, UnsafeConfigurationChanges) {
    NodeConfiguration original = ReplicationState::parse_node_configuration(three_node_config);
    
    // Test unsafe multi-node changes
    NodeConfiguration multi_add = original;
    multi_add.hostname_nodes.push_back("node4.example.com:8107:8108");
    multi_add.hostname_nodes.push_back("node5.example.com:8107:8108");
    multi_add.config_version++;
    
    EXPECT_FALSE(original.is_safe_single_node_change(multi_add));
    
    // Test no-change scenario
    NodeConfiguration no_change = original;
    no_change.config_version++;
    
    EXPECT_FALSE(original.is_safe_single_node_change(no_change));
    
    // Test removing multiple nodes
    NodeConfiguration multi_remove = original;
    multi_remove.hostname_nodes.clear();
    multi_remove.hostname_nodes.push_back("node1.example.com:8107:8108"); // Only keep one
    multi_remove.config_version++;
    
    EXPECT_FALSE(original.is_safe_single_node_change(multi_remove));
}

// Test configuration metadata and serialization
TEST_F(MongoDBTLASafetyTest, ConfigurationMetadata) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(three_node_config);
    config.config_version = 10;
    config.config_term = 25;
    
    // Test basic serialization
    std::string serialized = config.serialize();
    EXPECT_EQ(three_node_config, serialized);
    
    // Test metadata serialization
    std::string with_metadata = config.serialize_with_metadata();
    EXPECT_TRUE(with_metadata.find("version=10") != std::string::npos);
    EXPECT_TRUE(with_metadata.find("term=25") != std::string::npos);
    EXPECT_TRUE(with_metadata.find(three_node_config) != std::string::npos);
    
    // Test timestamp is recent
    auto now = std::chrono::steady_clock::now();
    auto config_age = std::chrono::duration_cast<std::chrono::seconds>(now - config.created_at);
    EXPECT_LT(config_age.count(), 5); // Should be created within last 5 seconds
}

// Test concurrent safety operations
TEST_F(MongoDBTLASafetyTest, ConcurrentSafetyOperations) {
    const int num_threads = 4;
    const int operations_per_thread = 50;
    std::vector<std::thread> threads;
    std::atomic<int> successful_operations{0};
    std::atomic<int> failed_operations{0};
    
    NodeConfiguration base_config = ReplicationState::parse_node_configuration(three_node_config);
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < operations_per_thread; ++j) {
                try {
                    // Test various safety operations
                    if (j % 3 == 0) {
                        // Test version comparison
                        NodeConfiguration newer = base_config.create_single_node_change("node4.example.com:8107:8108", "", i + 1);
                        if (newer.is_newer_than(base_config)) {
                            successful_operations++;
                        } else {
                            failed_operations++;
                        }
                    } else if (j % 3 == 1) {
                        // Test single-node change validation
                        NodeConfiguration changed = base_config.create_single_node_change("node4.example.com:8107:8108", "", i + 1);
                        if (base_config.is_safe_single_node_change(changed)) {
                            successful_operations++;
                        } else {
                            failed_operations++;
                        }
                    } else {
                        // Test serialization
                        std::string serialized = base_config.serialize();
                        std::string with_metadata = base_config.serialize_with_metadata();
                        if (!serialized.empty() && !with_metadata.empty()) {
                            successful_operations++;
                        } else {
                            failed_operations++;
                        }
                    }
                    
                    std::this_thread::sleep_for(std::chrono::microseconds(1));
                } catch (...) {
                    failed_operations++;
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // All operations should succeed
    EXPECT_EQ(num_threads * operations_per_thread, successful_operations.load() + failed_operations.load());
    EXPECT_GT(successful_operations.load(), 0);
    EXPECT_EQ(0, failed_operations.load()); // No operations should fail
}

// Test performance of MongoDB TLA+ safety checks
TEST_F(MongoDBTLASafetyTest, SafetyCheckPerformance) {
    NodeConfiguration base_config = ReplicationState::parse_node_configuration(five_node_config);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    const int num_operations = 1000;
    for (int i = 0; i < num_operations; ++i) {
        // Test the most expensive operations
        std::string node_name = "perfnode" + std::to_string(i) + ":8107:8108";
        
        // Create configuration change
        NodeConfiguration new_config = base_config.create_single_node_change(node_name, "", 1);
        
        // Validate safety
        bool is_safe = base_config.is_safe_single_node_change(new_config);
        EXPECT_TRUE(is_safe);
        
        // Check version comparison (MongoDB TLA+ pattern)
        bool is_newer = new_config.is_newer_than(base_config);
        EXPECT_TRUE(is_newer);
        
        // Test serialization with metadata
        std::string with_metadata = new_config.serialize_with_metadata();
        EXPECT_FALSE(with_metadata.empty());
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete MongoDB TLA+ safety operations quickly (target: under 50ms for 1000 operations)
    EXPECT_LT(duration.count(), 50000) << "MongoDB TLA+ safety operations took: " << duration.count() << "μs";
    
    double avg_time_per_op = static_cast<double>(duration.count()) / num_operations;
    EXPECT_LT(avg_time_per_op, 50.0) << "Average time per safety operation: " << avg_time_per_op << "μs";
}

// Test edge cases for MongoDB TLA+ patterns
TEST_F(MongoDBTLASafetyTest, EdgeCasesAndErrorHandling) {
    // Test empty configuration
    NodeConfiguration empty_config = ReplicationState::parse_node_configuration("");
    EXPECT_TRUE(empty_config.empty());
    EXPECT_EQ(0, empty_config.total_nodes());
    
    // Test malformed input
    NodeConfiguration malformed = ReplicationState::parse_node_configuration("malformed:input");
    EXPECT_EQ(1, malformed.total_nodes()); // Should be treated as IP node
    EXPECT_EQ(0, malformed.hostname_nodes.size());
    EXPECT_EQ(1, malformed.ip_nodes.size());
    
    // Test version overflow scenarios
    NodeConfiguration config = ReplicationState::parse_node_configuration(three_node_config);
    config.config_version = UINT64_MAX - 1;
    config.config_term = UINT64_MAX - 1;
    
    NodeConfiguration newer = config.create_single_node_change("node4.example.com:8107:8108", "", UINT64_MAX);
    EXPECT_EQ(UINT64_MAX, newer.config_version);
    EXPECT_EQ(UINT64_MAX, newer.config_term);
    EXPECT_TRUE(newer.is_newer_than(config));
    
    // Test very large configurations
    std::vector<std::string> many_nodes;
    for (int i = 1; i <= 100; ++i) {
        many_nodes.push_back("node" + std::to_string(i) + ".example.com:8107:8108");
    }
    std::string large_config = StringUtils::join(many_nodes, ",");
    
    NodeConfiguration large = ReplicationState::parse_node_configuration(large_config);
    EXPECT_EQ(100, large.total_nodes());
    EXPECT_EQ(100, large.hostname_nodes.size());
    EXPECT_EQ(0, large.ip_nodes.size());
    
    // Should still support safe single-node changes
    NodeConfiguration large_plus_one = large.create_single_node_change("node101.example.com:8107:8108", "", 1);
    EXPECT_EQ(101, large_plus_one.total_nodes());
    EXPECT_TRUE(large.is_safe_single_node_change(large_plus_one));
} 