#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include "raft_server.h"

class SafeConfigChangesTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup test configurations
        initial_config = "node1.example.com:8107:8108,node2.example.com:8107:8108,node3.example.com:8107:8108";
        mixed_config = "node1.example.com:8107:8108,192.168.1.10:8107:8108,node3.example.com:8107:8108";
        single_node_config = "node1.example.com:8107:8108";
    }
    
    std::string initial_config;
    std::string mixed_config;
    std::string single_node_config;
};

// Test NodeConfiguration versioning
TEST_F(SafeConfigChangesTest, ConfigurationVersioning) {
    NodeConfiguration config1 = ReplicationState::parse_node_configuration(initial_config);
    
    // Initial version should be 1
    EXPECT_EQ(1, config1.config_version);
    EXPECT_EQ(0, config1.config_term);
    
    // Create a new version
    NodeConfiguration config2 = config1.create_single_node_change("node4.example.com:8107:8108", "", 5);
    
    EXPECT_EQ(2, config2.config_version);
    EXPECT_EQ(5, config2.config_term);
    EXPECT_TRUE(config2.is_newer_than(config1));
    EXPECT_FALSE(config1.is_newer_than(config2));
}

TEST_F(SafeConfigChangesTest, ConfigurationVersionComparison) {
    NodeConfiguration config1 = ReplicationState::parse_node_configuration(initial_config);
    NodeConfiguration config2 = config1;
    
    // Same version and term
    EXPECT_FALSE(config1.is_newer_than(config2));
    EXPECT_FALSE(config2.is_newer_than(config1));
    
    // Higher version, same term
    config2.config_version = 2;
    EXPECT_TRUE(config2.is_newer_than(config1));
    EXPECT_FALSE(config1.is_newer_than(config2));
    
    // Lower version, higher term (term wins)
    config1.config_term = 10;
    config2.config_term = 5;
    config2.config_version = 10;
    EXPECT_TRUE(config1.is_newer_than(config2));
    EXPECT_FALSE(config2.is_newer_than(config1));
}

// Test single-node addition
TEST_F(SafeConfigChangesTest, SingleNodeAddition) {
    NodeConfiguration original = ReplicationState::parse_node_configuration(initial_config);
    ASSERT_EQ(3, original.total_nodes());
    
    // Add a hostname node
    NodeConfiguration with_hostname = original.create_single_node_change("node4.example.com:8107:8108", "", 1);
    
    EXPECT_EQ(4, with_hostname.total_nodes());
    EXPECT_EQ(4, with_hostname.hostname_nodes.size());
    EXPECT_EQ(0, with_hostname.ip_nodes.size());
    EXPECT_TRUE(original.is_safe_single_node_change(with_hostname));
    
    // Add an IP node
    NodeConfiguration with_ip = original.create_single_node_change("192.168.1.20:8107:8108", "", 1);
    
    EXPECT_EQ(4, with_ip.total_nodes());
    EXPECT_EQ(3, with_ip.hostname_nodes.size());
    EXPECT_EQ(1, with_ip.ip_nodes.size());
    EXPECT_TRUE(original.is_safe_single_node_change(with_ip));
}

// Test single-node removal
TEST_F(SafeConfigChangesTest, SingleNodeRemoval) {
    NodeConfiguration original = ReplicationState::parse_node_configuration(initial_config);
    ASSERT_EQ(3, original.total_nodes());
    
    // Remove a node
    NodeConfiguration without_node = original.create_single_node_change("", "node2.example.com:8107:8108", 1);
    
    EXPECT_EQ(2, without_node.total_nodes());
    EXPECT_EQ(2, without_node.hostname_nodes.size());
    EXPECT_EQ(0, without_node.ip_nodes.size());
    EXPECT_TRUE(original.is_safe_single_node_change(without_node));
    
    // Verify the correct node was removed
    EXPECT_TRUE(std::find(without_node.hostname_nodes.begin(), without_node.hostname_nodes.end(), 
                         "node1.example.com:8107:8108") != without_node.hostname_nodes.end());
    EXPECT_TRUE(std::find(without_node.hostname_nodes.begin(), without_node.hostname_nodes.end(), 
                         "node3.example.com:8107:8108") != without_node.hostname_nodes.end());
    EXPECT_TRUE(std::find(without_node.hostname_nodes.begin(), without_node.hostname_nodes.end(), 
                         "node2.example.com:8107:8108") == without_node.hostname_nodes.end());
}

// Test mixed hostname/IP node changes
TEST_F(SafeConfigChangesTest, MixedNodeChanges) {
    NodeConfiguration original = ReplicationState::parse_node_configuration(mixed_config);
    ASSERT_EQ(3, original.total_nodes());
    ASSERT_EQ(2, original.hostname_nodes.size());
    ASSERT_EQ(1, original.ip_nodes.size());
    
    // Remove hostname node
    NodeConfiguration without_hostname = original.create_single_node_change("", "node1.example.com:8107:8108", 1);
    EXPECT_EQ(2, without_hostname.total_nodes());
    EXPECT_EQ(1, without_hostname.hostname_nodes.size());
    EXPECT_EQ(1, without_hostname.ip_nodes.size());
    EXPECT_TRUE(original.is_safe_single_node_change(without_hostname));
    
    // Remove IP node
    NodeConfiguration without_ip = original.create_single_node_change("", "192.168.1.10:8107:8108", 1);
    EXPECT_EQ(2, without_ip.total_nodes());
    EXPECT_EQ(2, without_ip.hostname_nodes.size());
    EXPECT_EQ(0, without_ip.ip_nodes.size());
    EXPECT_TRUE(original.is_safe_single_node_change(without_ip));
}

// Test safety validation
TEST_F(SafeConfigChangesTest, SafetyValidation) {
    NodeConfiguration original = ReplicationState::parse_node_configuration(initial_config);
    
    // Safe single addition
    NodeConfiguration safe_add = original.create_single_node_change("node4.example.com:8107:8108", "", 1);
    EXPECT_TRUE(original.is_safe_single_node_change(safe_add));
    
    // Safe single removal
    NodeConfiguration safe_remove = original.create_single_node_change("", "node2.example.com:8107:8108", 1);
    EXPECT_TRUE(original.is_safe_single_node_change(safe_remove));
    
    // Unsafe: no change
    NodeConfiguration no_change = original;
    no_change.config_version++;
    EXPECT_FALSE(original.is_safe_single_node_change(no_change));
    
    // Unsafe: multiple changes (simulate by manually creating invalid config)
    NodeConfiguration multi_change = original;
    multi_change.hostname_nodes.push_back("node4.example.com:8107:8108");
    multi_change.hostname_nodes.push_back("node5.example.com:8107:8108");
    multi_change.config_version++;
    EXPECT_FALSE(original.is_safe_single_node_change(multi_change));
}

// Test hostname vs IP detection
TEST_F(SafeConfigChangesTest, HostnameVsIPDetection) {
    NodeConfiguration config = ReplicationState::parse_node_configuration("");
    
    // Test hostname addition
    NodeConfiguration with_hostname = config.create_single_node_change("node1.example.com:8107:8108", "", 1);
    EXPECT_EQ(1, with_hostname.hostname_nodes.size());
    EXPECT_EQ(0, with_hostname.ip_nodes.size());
    
    // Test IPv4 addition
    NodeConfiguration with_ipv4 = config.create_single_node_change("192.168.1.10:8107:8108", "", 1);
    EXPECT_EQ(0, with_ipv4.hostname_nodes.size());
    EXPECT_EQ(1, with_ipv4.ip_nodes.size());
    
    // Test IPv6 addition
    NodeConfiguration with_ipv6 = config.create_single_node_change("[2001:db8::1]:8107:8108", "", 1);
    EXPECT_EQ(0, with_ipv6.hostname_nodes.size());
    EXPECT_EQ(1, with_ipv6.ip_nodes.size());
}

// Test serialization with metadata
TEST_F(SafeConfigChangesTest, SerializationWithMetadata) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(initial_config);
    config.config_version = 5;
    config.config_term = 10;
    
    std::string basic_serialized = config.serialize();
    std::string metadata_serialized = config.serialize_with_metadata();
    
    // Basic serialization should not include metadata
    EXPECT_EQ(initial_config, basic_serialized);
    
    // Metadata serialization should include version info
    EXPECT_TRUE(metadata_serialized.find("version=5") != std::string::npos);
    EXPECT_TRUE(metadata_serialized.find("term=10") != std::string::npos);
    EXPECT_TRUE(metadata_serialized.find(initial_config) != std::string::npos);
}

// Test edge cases
TEST_F(SafeConfigChangesTest, EdgeCases) {
    // Empty configuration
    NodeConfiguration empty_config = ReplicationState::parse_node_configuration("");
    NodeConfiguration with_first_node = empty_config.create_single_node_change("node1.example.com:8107:8108", "", 1);
    
    EXPECT_EQ(1, with_first_node.total_nodes());
    EXPECT_TRUE(empty_config.is_safe_single_node_change(with_first_node));
    
    // Single node configuration
    NodeConfiguration single_config = ReplicationState::parse_node_configuration(single_node_config);
    
    // Can add to single node
    NodeConfiguration single_plus_one = single_config.create_single_node_change("node2.example.com:8107:8108", "", 1);
    EXPECT_EQ(2, single_plus_one.total_nodes());
    EXPECT_TRUE(single_config.is_safe_single_node_change(single_plus_one));
    
    // Can remove from single node (though this might not be wise in practice)
    NodeConfiguration empty_from_single = single_config.create_single_node_change("", "node1.example.com:8107:8108", 1);
    EXPECT_EQ(0, empty_from_single.total_nodes());
    EXPECT_TRUE(single_config.is_safe_single_node_change(empty_from_single));
}

// Test malformed input handling
TEST_F(SafeConfigChangesTest, MalformedInputHandling) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(initial_config);
    
    // Try to add malformed node
    NodeConfiguration with_malformed = config.create_single_node_change("malformed-node", "", 1);
    
    // Should still work (malformed nodes are treated as IPs for backward compatibility)
    EXPECT_EQ(4, with_malformed.total_nodes());
    EXPECT_EQ(3, with_malformed.hostname_nodes.size());
    EXPECT_EQ(1, with_malformed.ip_nodes.size());
    
    // Try to remove non-existent node
    NodeConfiguration without_nonexistent = config.create_single_node_change("", "nonexistent:8107:8108", 1);
    
    // Should be unchanged
    EXPECT_EQ(3, without_nonexistent.total_nodes());
    EXPECT_EQ(3, without_nonexistent.hostname_nodes.size());
    EXPECT_EQ(0, without_nonexistent.ip_nodes.size());
}

// Test concurrent configuration changes
TEST_F(SafeConfigChangesTest, ConcurrentConfigChanges) {
    const int num_threads = 4;
    const int changes_per_thread = 10;
    std::vector<std::thread> threads;
    std::atomic<int> successful_changes{0};
    std::atomic<int> failed_changes{0};
    
    NodeConfiguration base_config = ReplicationState::parse_node_configuration(initial_config);
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < changes_per_thread; ++j) {
                try {
                    // Alternate between additions and removals
                    if (j % 2 == 0) {
                        std::string node_to_add = "thread" + std::to_string(i) + "node" + std::to_string(j) + ":8107:8108";
                        NodeConfiguration new_config = base_config.create_single_node_change(node_to_add, "", i + 1);
                        
                        if (base_config.is_safe_single_node_change(new_config)) {
                            successful_changes++;
                        } else {
                            failed_changes++;
                        }
                    } else {
                        // Try to remove a node (this will fail for non-existent nodes, which is expected)
                        NodeConfiguration new_config = base_config.create_single_node_change("", "node2.example.com:8107:8108", i + 1);
                        
                        if (base_config.is_safe_single_node_change(new_config)) {
                            successful_changes++;
                        } else {
                            failed_changes++;
                        }
                    }
                    
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                } catch (...) {
                    failed_changes++;
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Should have processed all changes without exceptions
    EXPECT_EQ(num_threads * changes_per_thread, successful_changes.load() + failed_changes.load());
    EXPECT_GT(successful_changes.load(), 0);  // At least some should succeed
}

// Performance test for configuration changes
TEST_F(SafeConfigChangesTest, ConfigurationChangePerformance) {
    NodeConfiguration base_config = ReplicationState::parse_node_configuration(initial_config);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    const int num_operations = 1000;
    for (int i = 0; i < num_operations; ++i) {
        std::string node_name = "perfnode" + std::to_string(i) + ":8107:8108";
        
        // Create change
        NodeConfiguration new_config = base_config.create_single_node_change(node_name, "", 1);
        
        // Validate safety
        bool is_safe = base_config.is_safe_single_node_change(new_config);
        EXPECT_TRUE(is_safe);
        
        // Check version comparison
        bool is_newer = new_config.is_newer_than(base_config);
        EXPECT_TRUE(is_newer);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete operations quickly (less than 100ms for 1000 operations)
    EXPECT_LT(duration.count(), 100000) << "Config operations took: " << duration.count() << "μs";
    
    double avg_time_per_op = static_cast<double>(duration.count()) / num_operations;
    EXPECT_LT(avg_time_per_op, 100.0) << "Average time per operation: " << avg_time_per_op << "μs";
} 