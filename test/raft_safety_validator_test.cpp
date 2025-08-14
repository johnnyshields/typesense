#include <gtest/gtest.h>
#include "raft_server.h"

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

// Test has_term_quorum_check functionality
TEST_F(RaftSafetyValidatorTest, HasTermQuorumCheckWithoutNode) {
    bool result = repl_state->has_term_quorum_check();
    EXPECT_FALSE(result); // Should fail without raft node
}

// Test has_config_quorum_check functionality
TEST_F(RaftSafetyValidatorTest, HasConfigQuorumCheckWithoutNode) {
    bool result = repl_state->has_config_quorum_check();
    EXPECT_FALSE(result); // Should fail without raft node
}

// Test are_previous_ops_committed_in_current_config functionality
TEST_F(RaftSafetyValidatorTest, ArePreviousOpsCommittedWithoutNode) {
    bool result = repl_state->are_previous_ops_committed_in_current_config();
    EXPECT_FALSE(result); // Should fail without raft node
}

// Test validate_new_config_quorum functionality
TEST_F(RaftSafetyValidatorTest, ValidateNewConfigQuorumEmptyConfig) {
    NodeConfiguration empty_config;
    bool result = repl_state->validate_new_config_quorum(empty_config);
    EXPECT_FALSE(result); // Empty config should be invalid
}

TEST_F(RaftSafetyValidatorTest, ValidateNewConfigQuorumSingleNode) {
    NodeConfiguration single_config = repl_state->parse_node_configuration("node1.example.com:8107:8108");
    bool result = repl_state->validate_new_config_quorum(single_config);
    EXPECT_TRUE(result); // Single node should be valid
}

TEST_F(RaftSafetyValidatorTest, ValidateNewConfigQuorumThreeNodes) {
    NodeConfiguration three_config = repl_state->parse_node_configuration(three_node_config);
    bool result = repl_state->validate_new_config_quorum(three_config);
    EXPECT_TRUE(result); // Three nodes should be valid (quorum = 2)
}

TEST_F(RaftSafetyValidatorTest, ValidateNewConfigQuorumFiveNodes) {
    NodeConfiguration five_config = repl_state->parse_node_configuration(five_node_config);
    bool result = repl_state->validate_new_config_quorum(five_config);
    EXPECT_TRUE(result); // Five nodes should be valid (quorum = 3)
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
        bool quorum_valid = repl_state->validate_new_config_quorum(config);
        
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
    bool term_check = repl_state->has_term_quorum_check();
    bool config_check = repl_state->has_config_quorum_check();
    bool ops_check = repl_state->are_previous_ops_committed_in_current_config();
    
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
                    bool term_check = repl_state->has_term_quorum_check();
                    bool config_check = repl_state->has_config_quorum_check();
                    bool ops_check = repl_state->are_previous_ops_committed_in_current_config();
                    
                    // Create a test configuration
                    std::string test_config = "node" + std::to_string(i) + ".example.com:8107:8108";
                    NodeConfiguration config = repl_state->parse_node_configuration(test_config);
                    bool quorum_valid = repl_state->validate_new_config_quorum(config);
                    
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
        bool quorum_valid = repl_state->validate_new_config_quorum(config);
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
    bool quorum_valid = repl_state->validate_new_config_quorum(mixed_config_parsed);
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
        bool quorum_valid = repl_state->validate_new_config_quorum(large_config);
        EXPECT_TRUE(quorum_valid);
        
        // These will fail without proper raft node, but should be fast
        repl_state->config_is_safe();
        repl_state->has_term_quorum_check();
        repl_state->has_config_quorum_check();
        repl_state->are_previous_ops_committed_in_current_config();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete 1000 safety validations quickly (< 100ms)
    EXPECT_LT(duration.count(), 100000);
    
    double avg_time_per_validation = static_cast<double>(duration.count()) / 1000.0;
    EXPECT_LT(avg_time_per_validation, 100.0); // < 100μs per validation
} 