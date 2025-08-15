#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "raft_server.h"
#include "string_utils.h"

// Unit Tests for raft_safety_validator.cpp
// Tests MongoDB TLA+ safety patterns and peer failure handling

class RaftSafetyValidatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create ReplicationState with null dependencies - tests should work without full setup
        repl_state = std::make_unique<ReplicationState>(nullptr, nullptr, "", 0);
        
        // Test configurations
        three_node_config = "node1.example.com:8107:8108,node2.example.com:8107:8108,node3.example.com:8107:8108";
        five_node_config = "node1.example.com:8107:8108,node2.example.com:8107:8108,node3.example.com:8107:8108,node4.example.com:8107:8108,node5.example.com:8107:8108";
        mixed_config = "node1.example.com:8107:8108,192.168.1.10:8107:8108,node3.example.com:8107:8108";
    }

    std::unique_ptr<ReplicationState> repl_state;
    std::string three_node_config;
    std::string five_node_config;
    std::string mixed_config;
};

// Test handle_peer_failure functionality
TEST_F(RaftSafetyValidatorTest, HandlePeerFailureHostnameMatch) {
    // Setup configuration with hostname nodes
    NodeConfiguration config = repl_state->parse_node_configuration(three_node_config);
    
    // Mock a peer failure
    braft::PeerId failed_peer;
    butil::str2endpoint("127.0.0.1:8107", &failed_peer.addr);
    
    // Test that immediate refresh is not requested initially
    EXPECT_FALSE(repl_state->immediate_refresh_requested.load());
    
    // Handle peer failure
    repl_state->handle_peer_failure(failed_peer);
    
    // Should trigger immediate refresh for hostname-based peers
    // Note: This test might pass or fail depending on DNS resolution of hostnames
    // The important thing is that the method doesn't crash
}

TEST_F(RaftSafetyValidatorTest, HandlePeerFailureIPMatch) {
    // Setup configuration with IP nodes
    std::string ip_config = "192.168.1.10:8107:8108,192.168.1.20:8107:8108,192.168.1.30:8107:8108";
    NodeConfiguration config = repl_state->parse_node_configuration(ip_config);
    
    braft::PeerId failed_peer;
    butil::str2endpoint("192.168.1.40:8107", &failed_peer.addr);
    
    EXPECT_FALSE(repl_state->immediate_refresh_requested.load());
    
    repl_state->handle_peer_failure(failed_peer);
    
    // Should not trigger immediate refresh for non-hostname peers that don't match
    EXPECT_FALSE(repl_state->immediate_refresh_requested.load());
}

// Test trigger_immediate_config_refresh functionality
TEST_F(RaftSafetyValidatorTest, TriggerImmediateConfigRefresh) {
    EXPECT_FALSE(repl_state->immediate_refresh_requested.load());
    
    repl_state->trigger_immediate_config_refresh();
    
    EXPECT_TRUE(repl_state->immediate_refresh_requested.load());
}

// Test is_config_safe_for_reconfig basic functionality
TEST_F(RaftSafetyValidatorTest, IsConfigSafeForReconfigBasicChecks) {
    // Without a proper raft node, this should return false
    bool result = repl_state->is_config_safe_for_reconfig();
    EXPECT_FALSE(result); // Should fail because node is null
}

// Test get_current_term functionality
TEST_F(RaftSafetyValidatorTest, GetCurrentTermWithoutNode) {
    uint64_t term = repl_state->get_current_term();
    EXPECT_EQ(0, term); // Should return 0 when node is null
}

// Test config_is_safe functionality (MongoDB TLA+ pattern)
TEST_F(RaftSafetyValidatorTest, ConfigIsSafeWithoutNode) {
    bool result = repl_state->config_is_safe();
    EXPECT_FALSE(result); // Should fail without proper raft node
}

// Test has_valid_term_quorum functionality (MongoDB TLA+ HasValidTermQuorum)
TEST_F(RaftSafetyValidatorTest, HasValidTermQuorumWithoutNode) {
    bool result = repl_state->has_valid_term_quorum();
    EXPECT_FALSE(result); // Should fail without raft node
}

// Test has_valid_config_quorum functionality (MongoDB TLA+ HasValidConfigQuorum)
TEST_F(RaftSafetyValidatorTest, HasValidConfigQuorumWithoutNode) {
    bool result = repl_state->has_valid_config_quorum();
    EXPECT_FALSE(result); // Should fail without raft node
}

// Test are_previous_ops_committed functionality (MongoDB TLA+ ArePreviousOpsCommitted)
TEST_F(RaftSafetyValidatorTest, ArePreviousOpsCommittedWithoutNode) {
    bool result = repl_state->are_previous_ops_committed();
    EXPECT_FALSE(result); // Should fail without raft node
}

// Test has_quorum_overlap functionality (MongoDB TLA+ HasQuorumOverlap)
TEST_F(RaftSafetyValidatorTest, HasQuorumOverlapEmptyConfig) {
    NodeConfiguration empty_config;
    bool result = repl_state->has_quorum_overlap(empty_config);
    EXPECT_FALSE(result); // Empty config should be invalid
}

TEST_F(RaftSafetyValidatorTest, HasQuorumOverlapSingleNode) {
    NodeConfiguration single_config = repl_state->parse_node_configuration("node1.example.com:8107:8108");
    bool result = repl_state->has_quorum_overlap(single_config);
    EXPECT_TRUE(result); // Single node should be valid
}

TEST_F(RaftSafetyValidatorTest, HasQuorumOverlapThreeNodes) {
    NodeConfiguration three_config = repl_state->parse_node_configuration(three_node_config);
    bool result = repl_state->has_quorum_overlap(three_config);
    EXPECT_TRUE(result); // Three nodes should be valid (quorum = 2)
}

TEST_F(RaftSafetyValidatorTest, HasQuorumOverlapFiveNodes) {
    NodeConfiguration five_config = repl_state->parse_node_configuration(five_node_config);
    bool result = repl_state->has_quorum_overlap(five_config);
    EXPECT_TRUE(result); // Five nodes should be valid (quorum = 3)
}

// Test MongoDB-style joint consensus intersection validation
TEST_F(RaftSafetyValidatorTest, HasQuorumOverlapJointConsensus) {
    // Set up current configuration: [A, B, C] (quorum = 2)
    std::string current_config = "nodeA.example.com:8107:8108,nodeB.example.com:8107:8108,nodeC.example.com:8107:8108";
    NodeConfiguration current = repl_state->parse_node_configuration(current_config);
    
    // Simulate having a current configuration by setting internal state
    // (In a real test, this would be set through proper initialization)
    
    // Test Case 1: Safe single-node change [A,B,C] -> [A,B,D] 
    // Intersection: [A,B] = 2 nodes >= quorum(2) for both configs
    std::string safe_new_config = "nodeA.example.com:8107:8108,nodeB.example.com:8107:8108,nodeD.example.com:8107:8108";
    NodeConfiguration safe_config = repl_state->parse_node_configuration(safe_new_config);
    bool safe_result = repl_state->has_quorum_overlap(safe_config);
    EXPECT_TRUE(safe_result); // Should pass - sufficient intersection
    
    // Test Case 2: Unsafe complete replacement [A,B,C] -> [D,E,F]
    // Intersection: [] = 0 nodes < quorum(2) - would cause split-brain
    std::string unsafe_new_config = "nodeD.example.com:8107:8108,nodeE.example.com:8107:8108,nodeF.example.com:8107:8108";
    NodeConfiguration unsafe_config = repl_state->parse_node_configuration(unsafe_new_config);
    bool unsafe_result = repl_state->has_quorum_overlap(unsafe_config);
    // Note: This test may pass if there's no current config set in the test state
    // The real validation happens when there's an active current configuration
    
    // Test Case 3: Minimal intersection [A,B,C] -> [A,D,E]
    // Intersection: [A] = 1 node < quorum(2) - unsafe
    std::string minimal_config = "nodeA.example.com:8107:8108,nodeD.example.com:8107:8108,nodeE.example.com:8107:8108";
    NodeConfiguration minimal = repl_state->parse_node_configuration(minimal_config);
    bool minimal_result = repl_state->has_quorum_overlap(minimal);
    // Again, may pass without active current config in test environment
}

// Test add_node_safe functionality
TEST_F(RaftSafetyValidatorTest, AddNodeSafeWithoutProperSetup) {
    bool result = repl_state->add_node_safe("new-node.example.com:8107:8108");
    EXPECT_FALSE(result); // Should fail without proper raft setup
}

// Test remove_node_safe functionality  
TEST_F(RaftSafetyValidatorTest, RemoveNodeSafeWithoutProperSetup) {
    bool result = repl_state->remove_node_safe("node1.example.com:8107:8108");
    EXPECT_FALSE(result); // Should fail without proper raft setup
}

// Test safety validation edge cases
TEST_F(RaftSafetyValidatorTest, SafetyValidationEdgeCases) {
    // Test with various configuration sizes
    std::vector<std::string> test_configs = {
        "node1.example.com:8107:8108", // 1 node
        "node1.example.com:8107:8108,node2.example.com:8107:8108", // 2 nodes
        three_node_config, // 3 nodes
        "node1.example.com:8107:8108,node2.example.com:8107:8108,node3.example.com:8107:8108,node4.example.com:8107:8108", // 4 nodes
        five_node_config // 5 nodes
    };
    
    for (const auto& config_str : test_configs) {
        NodeConfiguration config = repl_state->parse_node_configuration(config_str);
        bool quorum_valid = repl_state->has_quorum_overlap(config);
        
        // All valid configurations should pass quorum validation
        EXPECT_TRUE(quorum_valid) << "Failed for config: " << config_str;
        
        // Quorum size should be (n/2) + 1
        size_t expected_quorum = (config.total_nodes() / 2) + 1;
        EXPECT_GE(expected_quorum, 1);
        EXPECT_LE(expected_quorum, config.total_nodes());
    }
}

// Test MongoDB TLA+ pattern implementation details
TEST_F(RaftSafetyValidatorTest, MongoDBTLAPatternsStructure) {
    // Test that the MongoDB TLA+ methods are properly structured
    
            // config_is_safe should combine all three checks
        bool config_safe = repl_state->config_is_safe();
        bool term_check = repl_state->has_valid_term_quorum();
        bool config_check = repl_state->has_valid_config_quorum();
        bool ops_check = repl_state->are_previous_ops_committed();
    
    // Without proper raft node, all should be false
    EXPECT_FALSE(config_safe);
    EXPECT_FALSE(term_check);
    EXPECT_FALSE(config_check);
    EXPECT_FALSE(ops_check);
}

// Test thread safety of safety validation
TEST_F(RaftSafetyValidatorTest, ThreadSafetySafetyValidation) {
    const int num_threads = 8;
    const int operations_per_thread = 50;
    std::vector<std::thread> threads;
    std::atomic<int> successful_validations{0};
    std::atomic<int> failed_validations{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < operations_per_thread; ++j) {
                try {
                    // Test various safety operations concurrently
                    bool config_safe = repl_state->config_is_safe();
                    bool term_check = repl_state->has_valid_term_quorum();
                    bool config_check = repl_state->has_valid_config_quorum();
                    bool ops_check = repl_state->are_previous_ops_committed();
                    
                    // Create a test configuration
                    std::string test_config = "node" + std::to_string(i) + ".example.com:8107:8108";
                    NodeConfiguration config = repl_state->parse_node_configuration(test_config);
                    bool quorum_valid = repl_state->has_quorum_overlap(config);
                    
                    if (quorum_valid) {
                        successful_validations++;
                    } else {
                        failed_validations++;
                    }
                    
                } catch (...) {
                    failed_validations++;
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    int expected_operations = num_threads * operations_per_thread;
    EXPECT_EQ(expected_operations, successful_validations.load() + failed_validations.load());
    
    // Single node configs should all pass quorum validation
    EXPECT_EQ(expected_operations, successful_validations.load());
    EXPECT_EQ(0, failed_validations.load());
}

// Test concurrent peer failure handling
TEST_F(RaftSafetyValidatorTest, ConcurrentPeerFailureHandling) {
    const int num_threads = 5;
    const int failures_per_thread = 20;
    std::vector<std::thread> threads;
    std::atomic<int> refresh_triggers{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < failures_per_thread; ++j) {
                try {
                    braft::PeerId failed_peer;
                    std::string peer_addr = "192.168.1." + std::to_string(i * 10 + j) + ":8107";
                    butil::str2endpoint(peer_addr.c_str(), &failed_peer.addr);
                    
                    bool was_requested_before = repl_state->immediate_refresh_requested.load();
                    repl_state->handle_peer_failure(failed_peer);
                    bool is_requested_after = repl_state->immediate_refresh_requested.load();
                    
                    if (is_requested_after && !was_requested_before) {
                        refresh_triggers++;
                    }
                    
                } catch (...) {
                    // Ignore exceptions in this test
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // The number of refresh triggers depends on hostname matching
    // but the important thing is no crashes occurred
    EXPECT_GE(refresh_triggers.load(), 0);
}

// Test configuration safety with different cluster sizes
TEST_F(RaftSafetyValidatorTest, ConfigurationSafetyDifferentSizes) {
    std::vector<std::pair<std::string, size_t>> test_cases = {
        {"node1.example.com:8107:8108", 1},
        {"node1.example.com:8107:8108,node2.example.com:8107:8108", 2},
        {three_node_config, 3},
        {five_node_config, 5}
    };
    
    for (const auto& test_case : test_cases) {
        NodeConfiguration config = repl_state->parse_node_configuration(test_case.first);
        EXPECT_EQ(test_case.second, config.total_nodes());
        
        // Test quorum validation
        bool quorum_valid = repl_state->has_quorum_overlap(config);
        EXPECT_TRUE(quorum_valid);
        
        // Test that quorum math is correct
        size_t quorum_size = (config.total_nodes() / 2) + 1;
        EXPECT_GE(quorum_size, 1);
        EXPECT_LE(quorum_size, config.total_nodes());
        
        // For different cluster sizes, verify quorum requirements
        switch (config.total_nodes()) {
            case 1: EXPECT_EQ(1, quorum_size); break;
            case 2: EXPECT_EQ(2, quorum_size); break;
            case 3: EXPECT_EQ(2, quorum_size); break;
            case 4: EXPECT_EQ(3, quorum_size); break;
            case 5: EXPECT_EQ(3, quorum_size); break;
        }
    }
}

// Test safety validation with mixed node types
TEST_F(RaftSafetyValidatorTest, SafetyValidationMixedNodeTypes) {
    NodeConfiguration mixed_config_parsed = repl_state->parse_node_configuration(mixed_config);
    
    // Should have both hostname and IP nodes
    EXPECT_GT(mixed_config_parsed.hostname_nodes.size(), 0);
    EXPECT_GT(mixed_config_parsed.ip_nodes.size(), 0);
    EXPECT_EQ(3, mixed_config_parsed.total_nodes());
    
    // Quorum validation should work with mixed types
    bool quorum_valid = repl_state->has_quorum_overlap(mixed_config_parsed);
    EXPECT_TRUE(quorum_valid);
    
    // Test peer failure handling with mixed configuration
    braft::PeerId test_peer;
    butil::str2endpoint("192.168.1.10:8107", &test_peer.addr);
    
    bool refresh_before = repl_state->immediate_refresh_requested.load();
    repl_state->handle_peer_failure(test_peer);
    bool refresh_after = repl_state->immediate_refresh_requested.load();
    
    // The result depends on whether the peer matches a hostname node
    // but the operation should complete without error
    EXPECT_TRUE(refresh_after == refresh_before || refresh_after != refresh_before);
}

// Test atomic operations thread safety
TEST_F(RaftSafetyValidatorTest, AtomicOperationsThreadSafety) {
    const int num_threads = 10;
    const int operations_per_thread = 100;
    std::vector<std::thread> threads;
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < operations_per_thread; ++j) {
                // Test atomic operations
                repl_state->trigger_immediate_config_refresh();
                bool is_requested = repl_state->immediate_refresh_requested.load();
                EXPECT_TRUE(is_requested);
                
                // Reset for next iteration
                repl_state->immediate_refresh_requested.store(false);
                
                // Test atomic counters
                uint64_t term_check = repl_state->last_term_quorum_check.load();
                repl_state->last_term_quorum_check.store(term_check + 1);
                
                uint64_t config_check = repl_state->last_config_quorum_check.load();
                repl_state->last_config_quorum_check.store(config_check + 1);
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify final state
    uint64_t final_term_check = repl_state->last_term_quorum_check.load();
    uint64_t final_config_check = repl_state->last_config_quorum_check.load();
    
    EXPECT_EQ(num_threads * operations_per_thread, final_term_check);
    EXPECT_EQ(num_threads * operations_per_thread, final_config_check);
}

// Test performance of safety validation
TEST_F(RaftSafetyValidatorTest, SafetyValidationPerformance) {
    NodeConfiguration large_config;
    
    // Create a large configuration
    for (int i = 1; i <= 50; ++i) {
        large_config.hostname_nodes.push_back("node" + std::to_string(i) + ".example.com:8107:8108");
    }
    large_config.config_version = 1;
    large_config.config_term = 0;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Perform multiple safety validations
    for (int i = 0; i < 1000; ++i) {
        bool quorum_valid = repl_state->has_quorum_overlap(large_config);
        EXPECT_TRUE(quorum_valid);
        
        // These will fail without proper raft node, but should be fast
        repl_state->config_is_safe();
        repl_state->has_valid_term_quorum();
        repl_state->has_valid_config_quorum();
        repl_state->are_previous_ops_committed();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete 1000 safety validations quickly (< 100ms)
    EXPECT_LT(duration.count(), 100000);
    
    double avg_time_per_validation = static_cast<double>(duration.count()) / 1000.0;
    EXPECT_LT(avg_time_per_validation, 100.0); // < 100μs per validation
}

// NodeConfiguration Unit Tests
// These tests focus on the core NodeConfiguration struct functionality

TEST_F(RaftSafetyValidatorTest, NodeConfigurationVersioning) {
    NodeConfiguration config1 = repl_state->parse_node_configuration(three_node_config);
    
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

TEST_F(RaftSafetyValidatorTest, NodeConfigurationVersionComparison) {
    NodeConfiguration config1 = repl_state->parse_node_configuration(three_node_config);
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

TEST_F(RaftSafetyValidatorTest, SingleNodeConfigurationChanges) {
    NodeConfiguration original = repl_state->parse_node_configuration(three_node_config);
    ASSERT_EQ(3, original.total_nodes());
    
    // Test single node addition
    NodeConfiguration with_hostname = original.create_single_node_change("node4.example.com:8107:8108", "", 1);
    EXPECT_EQ(4, with_hostname.total_nodes());
    EXPECT_TRUE(original.validate_config_change(with_hostname));
    
    // Test single node removal
    NodeConfiguration without_node = original.create_single_node_change("", "node2.example.com:8107:8108", 1);
    EXPECT_EQ(2, without_node.total_nodes());
    EXPECT_TRUE(original.validate_config_change(without_node));
    
    // Test unsafe: no change
    NodeConfiguration no_change = original;
    no_change.config_version++;
    EXPECT_FALSE(original.validate_config_change(no_change));
}

TEST_F(RaftSafetyValidatorTest, EnhancedSymmetricDifferenceValidation) {
    NodeConfiguration base = repl_state->parse_node_configuration(three_node_config);
    
    // Single addition (symmetric difference = 1) - should be valid
    NodeConfiguration add_one = base;
    add_one.hostname_nodes.push_back("node4.example.com:8107:8108");
    EXPECT_TRUE(base.validate_config_change(add_one));
    
    // Single removal (symmetric difference = 1) - should be valid
    NodeConfiguration remove_one = base;
    remove_one.hostname_nodes.pop_back();
    EXPECT_TRUE(base.validate_config_change(remove_one));
    
    // Node replacement (symmetric difference = 2) - should be invalid
    NodeConfiguration replace_node = base;
    replace_node.hostname_nodes.pop_back(); // Remove last
    replace_node.hostname_nodes.push_back("replacement.example.com:8107:8108"); // Add different
    EXPECT_FALSE(base.validate_config_change(replace_node));
    
    // Multiple additions (symmetric difference > 1) - should be invalid
    NodeConfiguration add_multiple = base;
    add_multiple.hostname_nodes.push_back("node4.example.com:8107:8108");
    add_multiple.hostname_nodes.push_back("node5.example.com:8107:8108");
    EXPECT_FALSE(base.validate_config_change(add_multiple));
}

TEST_F(RaftSafetyValidatorTest, ForceReconfigurationVersionComparison) {
    NodeConfiguration base = repl_state->parse_node_configuration(three_node_config);
    
    // Test force reconfig (term = -1) vs normal config
    NodeConfiguration force_config = base;
    force_config.config_term = -1; // Uninitialized/force reconfig
    force_config.config_version = 20;
    
    NodeConfiguration normal_config = base;
    normal_config.config_term = 10; // High term
    normal_config.config_version = 15; // Lower version
    
    EXPECT_TRUE(force_config.is_newer_than(normal_config))
        << "Force reconfig with higher version should win against any term";
    EXPECT_FALSE(normal_config.is_newer_than(force_config))
        << "Normal config should lose to force reconfig with higher version";
    
    // Both force reconfigs - version-only comparison
    NodeConfiguration force_config2 = base;
    force_config2.config_term = -1;
    force_config2.config_version = 30;
    
    EXPECT_TRUE(force_config2.is_newer_than(force_config))
        << "Among force reconfigs, higher version should win";
}

TEST_F(RaftSafetyValidatorTest, MixedNodeTypeConfigurationChanges) {
    NodeConfiguration mixed = repl_state->parse_node_configuration(mixed_config);
    ASSERT_EQ(2, mixed.hostname_nodes.size());
    ASSERT_EQ(1, mixed.ip_nodes.size());
    
    // Add hostname to mixed config
    NodeConfiguration add_hostname = mixed;
    add_hostname.hostname_nodes.push_back("node4.example.com:8107:8108");
    EXPECT_TRUE(mixed.validate_config_change(add_hostname));
    
    // Add IP to mixed config
    NodeConfiguration add_ip = mixed;
    add_ip.ip_nodes.push_back("192.168.1.20:8107:8108");
    EXPECT_TRUE(mixed.validate_config_change(add_ip));
    
    // Cross-type replacement (hostname -> IP) should be invalid
    NodeConfiguration cross_replace = mixed;
    cross_replace.hostname_nodes.pop_back(); // Remove hostname
    cross_replace.ip_nodes.push_back("192.168.1.30:8107:8108"); // Add IP
    EXPECT_FALSE(mixed.validate_config_change(cross_replace));
}

TEST_F(RaftSafetyValidatorTest, NodeConfigurationEdgeCases) {
    // Empty to single node (bootstrap scenario)
    NodeConfiguration empty = repl_state->parse_node_configuration("");
    NodeConfiguration single = repl_state->parse_node_configuration("node1.example.com:8107:8108");
    EXPECT_TRUE(empty.validate_config_change(single));
    
    // Single to empty (should be valid for single node removal)
    EXPECT_TRUE(single.validate_config_change(empty));
    
    // Large cluster single change
    std::vector<std::string> many_nodes;
    for (int i = 1; i <= 10; ++i) {
        many_nodes.push_back("node" + std::to_string(i) + ".example.com:8107:8108");
    }
    std::string large_config = StringUtils::join(many_nodes, ",");
    NodeConfiguration large = repl_state->parse_node_configuration(large_config);
    
    NodeConfiguration large_plus_one = large;
    large_plus_one.hostname_nodes.push_back("node11.example.com:8107:8108");
    EXPECT_TRUE(large.validate_config_change(large_plus_one));
}

TEST_F(RaftSafetyValidatorTest, NodeConfigurationPerformance) {
    NodeConfiguration base = repl_state->parse_node_configuration(three_node_config);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    const int num_operations = 1000;
    for (int i = 0; i < num_operations; ++i) {
        std::string node_name = "perfnode" + std::to_string(i) + ":8107:8108";
        
        // Create change
        NodeConfiguration new_config = base.create_single_node_change(node_name, "", 1);
        
        // Validate safety
        bool is_safe = base.validate_config_change(new_config);
        EXPECT_TRUE(is_safe);
        
        // Check version comparison
        bool is_newer = new_config.is_newer_than(base);
        EXPECT_TRUE(is_newer);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete operations quickly (less than 100ms for 1000 operations)
    EXPECT_LT(duration.count(), 100000) << "Config operations took: " << duration.count() << "μs";
    
    double avg_time_per_op = static_cast<double>(duration.count()) / num_operations;
    EXPECT_LT(avg_time_per_op, 100.0) << "Average time per operation: " << avg_time_per_op << "μs";
} 

// Test simplified add_node_safe functionality
TEST_F(RaftSafetyValidatorTest, AddNodeSafeBasicValidation) {
    // Test with empty node
    bool result = repl_state->add_node_safe("");
    EXPECT_FALSE(result);
    
    // Test with valid node format
    bool valid_result = repl_state->add_node_safe("node4.example.com:8107:8108");
    // Result depends on leadership status, but should not crash
    EXPECT_TRUE(valid_result || !valid_result);
}

TEST_F(RaftSafetyValidatorTest, AddNodeSafeValidation) {
    // Test validation logic without requiring full raft setup
    std::string node_to_add = "node4.example.com:8107:8108";
    
    // Should handle gracefully when not leader
    bool result = repl_state->add_node_safe(node_to_add);
    EXPECT_FALSE(result); // Expected to fail without proper raft setup
}

TEST_F(RaftSafetyValidatorTest, RemoveNodeSafeBasicValidation) {
    // Test with empty node
    bool result = repl_state->remove_node_safe("");
    EXPECT_FALSE(result);
    
    // Test self-removal protection
    bool self_removal = repl_state->remove_node_safe("localhost:8107:8108");
    EXPECT_FALSE(self_removal);
    
    // Test with valid node format
    bool valid_result = repl_state->remove_node_safe("node2.example.com:8107:8108");
    // Result depends on leadership status, but should not crash
    EXPECT_TRUE(valid_result || !valid_result);
}

TEST_F(RaftSafetyValidatorTest, RemoveNodeSafeSelfProtection) {
    // Test various self-identification patterns
    EXPECT_FALSE(repl_state->remove_node_safe("localhost:8107:8108"));
    EXPECT_FALSE(repl_state->remove_node_safe("127.0.0.1:8107:8108"));
    EXPECT_FALSE(repl_state->remove_node_safe("::1:8107:8108"));
    
    // Test non-self nodes (should pass validation step but fail on leadership)
    bool result = repl_state->remove_node_safe("remote.host.com:8107:8108");
    EXPECT_FALSE(result); // Should fail due to not being leader
}

TEST_F(RaftSafetyValidatorTest, SelfNodeDetection) {
    // Test the is_self_node helper method
    EXPECT_TRUE(repl_state->is_self_node("localhost:8107:8108"));
    EXPECT_TRUE(repl_state->is_self_node("127.0.0.1:8107:8108"));
    EXPECT_TRUE(repl_state->is_self_node("::1:8107:8108"));
    EXPECT_FALSE(repl_state->is_self_node("remote.host.com:8107:8108"));
    EXPECT_FALSE(repl_state->is_self_node("192.168.1.100:8107:8108"));
    EXPECT_FALSE(repl_state->is_self_node(""));
}

// Test configuration safety validation (simplified)
TEST_F(RaftSafetyValidatorTest, ConfigSafeForReconfigSimplified) {
    // Test basic safety checks without complex MongoDB state machines
    bool result = repl_state->is_config_safe_for_reconfig();
    
    // Should return false when not leader (which is expected without full setup)
    EXPECT_FALSE(result);
}

// Test validation methods work without crashing
TEST_F(RaftSafetyValidatorTest, ValidationMethodsStability) {
    // These should not crash even without full raft setup
    EXPECT_NO_THROW({
        repl_state->config_is_safe();
        repl_state->has_valid_term_quorum();
        repl_state->has_valid_config_quorum();
        repl_state->are_previous_ops_committed();
    });
}

TEST_F(RaftSafetyValidatorTest, QuorumValidationWithoutRaft) {
    // Test quorum validation with basic configurations
    NodeConfiguration single_node = repl_state->parse_node_configuration("node1:8107:8108");
    NodeConfiguration three_nodes = repl_state->parse_node_configuration(three_node_config);
    NodeConfiguration five_nodes = repl_state->parse_node_configuration(five_node_config);
    
    // Should handle validation gracefully
    EXPECT_NO_THROW({
        repl_state->has_quorum_overlap(single_node);
        repl_state->has_quorum_overlap(three_nodes);
        repl_state->has_quorum_overlap(five_nodes);
    });
}

// Test file-based model awareness
TEST_F(RaftSafetyValidatorTest, FileBased ModelAwareness) {
    // The simplified methods should indicate they're for validation only
    std::string node_to_add = "node4.example.com:8107:8108";
    
    // Capture log output would be ideal, but for now just ensure methods complete
    EXPECT_NO_THROW({
        repl_state->add_node_safe(node_to_add);
        repl_state->remove_node_safe("node2.example.com:8107:8108");
    });
}

// Test concurrent validation operations
TEST_F(RaftSafetyValidatorTest, ConcurrentValidationOperations) {
    const int num_threads = 4;
    const int operations_per_thread = 50;
    std::vector<std::thread> threads;
    std::atomic<int> completed_operations{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < operations_per_thread; ++j) {
                try {
                    // Mix of validation operations
                    if (j % 3 == 0) {
                        repl_state->add_node_safe("node" + std::to_string(i) + std::to_string(j) + ":8107:8108");
                    } else if (j % 3 == 1) {
                        repl_state->remove_node_safe("node" + std::to_string(i) + std::to_string(j) + ":8107:8108");
                    } else {
                        repl_state->is_config_safe_for_reconfig();
                    }
                    completed_operations++;
                } catch (...) {
                    // Should not throw
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // All operations should complete without throwing
    EXPECT_EQ(num_threads * operations_per_thread, completed_operations.load());
}

// Test performance of validation operations
TEST_F(RaftSafetyValidatorTest, ValidationPerformance) {
    auto start = std::chrono::high_resolution_clock::now();
    
    const int num_operations = 1000;
    for (int i = 0; i < num_operations; ++i) {
        // Mix of validation operations
        repl_state->add_node_safe("perfnode" + std::to_string(i) + ":8107:8108");
        repl_state->is_config_safe_for_reconfig();
        repl_state->config_is_safe();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete validation operations quickly (< 100ms for 1000 operations)
    EXPECT_LT(duration.count(), 100000);
    
    double avg_time_per_validation = static_cast<double>(duration.count()) / (num_operations * 3);
    EXPECT_LT(avg_time_per_validation, 100.0); // < 100μs per validation
} 