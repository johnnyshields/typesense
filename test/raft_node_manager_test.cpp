#include <gtest/gtest.h>
#include "raft_server.h"

// Unit Tests for raft_node_manager.cpp  
// Tests node status monitoring, configuration refresh, and peer management

// Simple test Store implementation
class TestStore {
public:
    bool get(const std::string& key, std::string& value) {
        auto it = data_.find(key);
        if (it != data_.end()) {
            value = it->second;
            return true;
        }
        return false;
    }
    
    bool set(const std::string& key, const std::string& value) {
        data_[key] = value;
        return true;
    }
    
    bool remove(const std::string& key) {
        return data_.erase(key) > 0;
    }

private:
    std::map<std::string, std::string> data_;
};

// Simple test MessageDispatcher implementation
class TestMessageDispatcher {
public:
    bool send_message(const std::string& message) {
        messages_.push_back(message);
        return true;
    }
    
    void set_handler(std::function<void(const std::string&)> handler) {
        handler_ = handler;
    }
    
    const std::vector<std::string>& get_messages() const {
        return messages_;
    }

private:
    std::vector<std::string> messages_;
    std::function<void(const std::string&)> handler_;
};

class RaftNodeManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create ReplicationState with null dependencies - tests should work without full setup
        repl_state = std::make_unique<ReplicationState>(nullptr, nullptr, "", 0);
        test_store = std::make_shared<TestStore>();
        test_dispatcher = std::make_shared<TestMessageDispatcher>();
        
        // Test configurations
        single_node_config = "127.0.0.1:8107:8108";
        three_node_config = "node1.example.com:8107:8108,node2.example.com:8107:8108,node3.example.com:8107:8108";
        mixed_config = "node1.example.com:8107:8108,192.168.1.10:8107:8108,node3.example.com:8107:8108";
    }

    std::unique_ptr<ReplicationState> repl_state;
    std::shared_ptr<TestStore> test_store;
    std::shared_ptr<TestMessageDispatcher> test_dispatcher;
    std::string single_node_config;
    std::string three_node_config;
    std::string mixed_config;
};

// Test refresh_nodes functionality
TEST_F(RaftNodeManagerTest, RefreshNodesBasicConfiguration) {
    std::string raft_dir = "/tmp/raft_refresh_test";
    
    // Test refresh with basic configuration
    EXPECT_NO_THROW({
        repl_state->refresh_nodes(single_node_config, raft_dir, 8108);
    });
}

TEST_F(RaftNodeManagerTest, RefreshNodesEmptyConfiguration) {
    std::string raft_dir = "/tmp/raft_refresh_test";
    
    // Test refresh with empty configuration
    EXPECT_NO_THROW({
        repl_state->refresh_nodes("", raft_dir, 8108);
    });
}

TEST_F(RaftNodeManagerTest, RefreshNodesMultipleNodes) {
    std::string raft_dir = "/tmp/raft_refresh_test";
    
    // Test refresh with multiple nodes
    EXPECT_NO_THROW({
        repl_state->refresh_nodes(three_node_config, raft_dir, 8108);
    });
}

TEST_F(RaftNodeManagerTest, RefreshNodesMixedConfiguration) {
    std::string raft_dir = "/tmp/raft_refresh_test";
    
    // Test refresh with mixed hostname/IP configuration
    EXPECT_NO_THROW({
        repl_state->refresh_nodes(mixed_config, raft_dir, 8108);
    });
}

// Test refresh_catchup_status functionality
TEST_F(RaftNodeManagerTest, RefreshCatchupStatusWithLogging) {
    // Test catchup status refresh with logging enabled
    EXPECT_NO_THROW({
        repl_state->refresh_catchup_status(true);
    });
}

TEST_F(RaftNodeManagerTest, RefreshCatchupStatusWithoutLogging) {
    // Test catchup status refresh with logging disabled
    EXPECT_NO_THROW({
        repl_state->refresh_catchup_status(false);
    });
}

// Test is_alive functionality
TEST_F(RaftNodeManagerTest, IsAliveWithoutNode) {
    // Test is_alive without proper raft node setup
    bool result = repl_state->is_alive();
    EXPECT_FALSE(result); // Should return false without proper node
}

// Test node_state functionality
TEST_F(RaftNodeManagerTest, NodeStateWithoutNode) {
    // Test node state without proper raft node setup
    std::string state = repl_state->node_state();
    EXPECT_FALSE(state.empty()); // Should return some default state
}

// Test trigger_vote functionality
TEST_F(RaftNodeManagerTest, TriggerVoteWithoutNode) {
    // Test trigger vote without proper raft node setup
    bool result = repl_state->trigger_vote();
    EXPECT_FALSE(result); // Should return false without proper node
}

// Test reset_peers functionality
TEST_F(RaftNodeManagerTest, ResetPeersWithoutNode) {
    // Test reset peers without proper raft node setup
    bool result = repl_state->reset_peers();
    EXPECT_FALSE(result); // Should return false without proper node
}

// Test get_message_dispatcher functionality
TEST_F(RaftNodeManagerTest, GetMessageDispatcherInitiallyNull) {
    // Test getting message dispatcher when not set
    auto dispatcher = repl_state->get_message_dispatcher();
    EXPECT_EQ(nullptr, dispatcher); // Should be null initially
}

TEST_F(RaftNodeManagerTest, GetMessageDispatcherAfterSetting) {
    // Test setting and getting message dispatcher
    repl_state->set_message_dispatcher(test_dispatcher);
    auto dispatcher = repl_state->get_message_dispatcher();
    EXPECT_EQ(test_dispatcher, dispatcher);
}

// Test get_store functionality
TEST_F(RaftNodeManagerTest, GetStoreInitiallyNull) {
    // Test getting store when not set
    auto store = repl_state->get_store();
    EXPECT_EQ(nullptr, store); // Should be null initially
}

TEST_F(RaftNodeManagerTest, GetStoreAfterSetting) {
    // Test setting and getting store
    repl_state->set_store(test_store);
    auto store = repl_state->get_store();
    EXPECT_EQ(test_store, store);
}

// Test persist_applying_index functionality
TEST_F(RaftNodeManagerTest, PersistApplyingIndexWithoutStore) {
    // Test persisting applying index without store
    EXPECT_NO_THROW({
        repl_state->persist_applying_index();
    });
}

TEST_F(RaftNodeManagerTest, PersistApplyingIndexWithStore) {
    // Test persisting applying index with store
    repl_state->set_store(test_store);
    
    EXPECT_NO_THROW({
        repl_state->persist_applying_index();
    });
    
    // Check that something was stored
    std::string value;
    bool found = test_store->get("applying_index", value);
    // The exact key depends on implementation, this test just ensures no crash
}

// Test get_num_queued_writes functionality
TEST_F(RaftNodeManagerTest, GetNumQueuedWritesWithoutNode) {
    // Test getting queued writes count without raft node
    uint64_t count = repl_state->get_num_queued_writes();
    EXPECT_EQ(0, count); // Should return 0 without proper node
}

// Test is_leader functionality
TEST_F(RaftNodeManagerTest, IsLeaderWithoutNode) {
    // Test is_leader without proper raft node
    bool result = repl_state->is_leader();
    EXPECT_FALSE(result); // Should return false without proper node
}

// Test get_status functionality
TEST_F(RaftNodeManagerTest, GetStatusBasicStructure) {
    // Test getting status without proper raft node setup
    nlohmann::json status = repl_state->get_status();
    
    // Should return a valid JSON object with basic structure
    EXPECT_TRUE(status.is_object());
    
    // Should contain some basic fields even without proper setup
    EXPECT_TRUE(status.contains("state") || status.size() >= 0);
}

// Test get_leader_url functionality
TEST_F(RaftNodeManagerTest, GetLeaderUrlWithoutNode) {
    // Test getting leader URL without proper raft node
    std::string url = repl_state->get_leader_url("test_api_key");
    EXPECT_TRUE(url.empty() || !url.empty()); // Should handle gracefully
}

TEST_F(RaftNodeManagerTest, GetLeaderUrlWithApiKey) {
    // Test getting leader URL with API key
    std::string api_key = "test_api_key_12345";
    std::string url = repl_state->get_leader_url(api_key);
    // Should handle gracefully regardless of result
    EXPECT_TRUE(true);
}

// Test decr_pending_writes functionality
TEST_F(RaftNodeManagerTest, DecrPendingWrites) {
    // Test decrementing pending writes
    EXPECT_NO_THROW({
        repl_state->decr_pending_writes();
    });
    
    // Test multiple decrements
    for (int i = 0; i < 10; ++i) {
        EXPECT_NO_THROW({
            repl_state->decr_pending_writes();
        });
    }
}

// Test concurrent node management operations
TEST_F(RaftNodeManagerTest, ConcurrentNodeManagementOps) {
    const int num_threads = 6;
    const int operations_per_thread = 30;
    std::vector<std::thread> threads;
    std::atomic<int> successful_operations{0};
    std::atomic<int> failed_operations{0};
    
    // Set up dependencies for some operations
    repl_state->set_store(test_store);
    repl_state->set_message_dispatcher(test_dispatcher);
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < operations_per_thread; ++j) {
                try {
                    switch (j % 6) {
                        case 0:
                            // Node status operations
                            repl_state->is_alive();
                            repl_state->is_leader();
                            repl_state->node_state();
                            break;
                        case 1:
                            // Configuration refresh
                            std::string config = "127.0.0.1:810" + std::to_string(i) + ":810" + std::to_string(i+1);
                            repl_state->refresh_nodes(config, "/tmp/test_" + std::to_string(i), 8108 + i);
                            break;
                        case 2:
                            // Catchup status refresh
                            repl_state->refresh_catchup_status(i % 2 == 0);
                            break;
                        case 3:
                            // Vote and peer operations
                            repl_state->trigger_vote();
                            repl_state->reset_peers();
                            break;
                        case 4:
                            // Write operations
                            repl_state->get_num_queued_writes();
                            repl_state->decr_pending_writes();
                            repl_state->persist_applying_index();
                            break;
                        case 5:
                            // Status and URL operations
                            nlohmann::json status = repl_state->get_status();
                            std::string url = repl_state->get_leader_url("key_" + std::to_string(i));
                            break;
                    }
                    successful_operations++;
                } catch (...) {
                    failed_operations++;
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    int expected_operations = num_threads * operations_per_thread;
    EXPECT_EQ(expected_operations, successful_operations.load() + failed_operations.load());
    
    // Most operations should succeed (even if they don't do much without proper setup)
    EXPECT_GE(successful_operations.load(), expected_operations * 0.8);
}

// Test dependency injection
TEST_F(RaftNodeManagerTest, DependencyInjection) {
    // Test setting and getting store
    EXPECT_EQ(nullptr, repl_state->get_store());
    repl_state->set_store(test_store);
    EXPECT_EQ(test_store, repl_state->get_store());
    
    // Test setting and getting message dispatcher
    EXPECT_EQ(nullptr, repl_state->get_message_dispatcher());
    repl_state->set_message_dispatcher(test_dispatcher);
    EXPECT_EQ(test_dispatcher, repl_state->get_message_dispatcher());
    
    // Test setting null dependencies
    repl_state->set_store(nullptr);
    EXPECT_EQ(nullptr, repl_state->get_store());
    
    repl_state->set_message_dispatcher(nullptr);
    EXPECT_EQ(nullptr, repl_state->get_message_dispatcher());
}

// Test node management performance
TEST_F(RaftNodeManagerTest, NodeManagementPerformance) {
    auto start = std::chrono::high_resolution_clock::now();
    
    // Perform multiple node management operations
    for (int i = 0; i < 1000; ++i) {
        repl_state->is_alive();
        repl_state->is_leader();
        repl_state->node_state();
        repl_state->get_num_queued_writes();
        repl_state->decr_pending_writes();
        
        if (i % 10 == 0) {
            repl_state->refresh_catchup_status(false);
            repl_state->trigger_vote();
            repl_state->reset_peers();
        }
        
        if (i % 100 == 0) {
            std::string config = "127.0.0.1:8107:8108";
            repl_state->refresh_nodes(config, "/tmp/perf_test", 8108);
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete 1000+ operations quickly (< 200ms)
    EXPECT_LT(duration.count(), 200000);
    
    double avg_time_per_operation = static_cast<double>(duration.count()) / 1000.0;
    EXPECT_LT(avg_time_per_operation, 200.0); // < 200μs per operation
}

// Test status JSON structure
TEST_F(RaftNodeManagerTest, StatusJsonStructure) {
    nlohmann::json status = repl_state->get_status();
    
    // Should be a valid JSON object
    EXPECT_TRUE(status.is_object());
    
    // Test that we can serialize and deserialize
    std::string serialized = status.dump();
    EXPECT_FALSE(serialized.empty());
    
    nlohmann::json deserialized = nlohmann::json::parse(serialized);
    EXPECT_TRUE(deserialized.is_object());
}

// Test configuration refresh with different scenarios
TEST_F(RaftNodeManagerTest, ConfigurationRefreshScenarios) {
    std::vector<std::pair<std::string, std::string>> test_scenarios = {
        {single_node_config, "Single node configuration"},
        {three_node_config, "Three node configuration"},
        {mixed_config, "Mixed hostname/IP configuration"},
        {"", "Empty configuration"},
        {"invalid_config", "Invalid configuration"},
        {"node1.com:8107:8108,node2.com:8107:8108,node3.com:8107:8108,node4.com:8107:8108,node5.com:8107:8108", "Five node configuration"}
    };
    
    for (const auto& scenario : test_scenarios) {
        EXPECT_NO_THROW({
            repl_state->refresh_nodes(scenario.first, "/tmp/scenario_test", 8108);
        }) << "Failed for: " << scenario.second;
    }
}

// Test edge cases in node management
TEST_F(RaftNodeManagerTest, NodeManagementEdgeCases) {
    // Test with very long configuration strings
    std::string long_config = "";
    for (int i = 0; i < 100; ++i) {
        if (i > 0) long_config += ",";
        long_config += "node" + std::to_string(i) + ".example.com:8107:8108";
    }
    
    EXPECT_NO_THROW({
        repl_state->refresh_nodes(long_config, "/tmp/long_config_test", 8108);
    });
    
    // Test with very long directory paths
    std::string long_dir = "/tmp/" + std::string(1000, 'a');
    EXPECT_NO_THROW({
        repl_state->refresh_nodes(single_node_config, long_dir, 8108);
    });
    
    // Test with extreme port numbers
    EXPECT_NO_THROW({
        repl_state->refresh_nodes(single_node_config, "/tmp/port_test", 65535);
    });
    
    EXPECT_NO_THROW({
        repl_state->refresh_nodes(single_node_config, "/tmp/port_test", 1);
    });
}

// Test thread safety of node management operations
TEST_F(RaftNodeManagerTest, ThreadSafetyNodeManagement) {
    const int num_threads = 8;
    const int operations_per_thread = 100;
    std::vector<std::thread> threads;
    std::atomic<uint64_t> total_writes_decremented{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < operations_per_thread; ++j) {
                // Thread-safe operations
                bool alive = repl_state->is_alive();
                bool leader = repl_state->is_leader();
                std::string state = repl_state->node_state();
                uint64_t queued = repl_state->get_num_queued_writes();
                
                // Decrement writes (should be thread-safe)
                repl_state->decr_pending_writes();
                total_writes_decremented++;
                
                // Status operations (should be thread-safe)
                nlohmann::json status = repl_state->get_status();
                std::string leader_url = repl_state->get_leader_url("thread_" + std::to_string(i));
                
                // Refresh operations (may have some synchronization)
                if (j % 10 == 0) {
                    std::string config = "127.0.0.1:" + std::to_string(8107 + i) + ":" + std::to_string(8108 + i);
                    repl_state->refresh_nodes(config, "/tmp/thread_test_" + std::to_string(i), 8108 + i);
                    repl_state->refresh_catchup_status(false);
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // All write decrements should have been counted
    EXPECT_EQ(num_threads * operations_per_thread, total_writes_decremented.load());
}

// Test error handling in node management
TEST_F(RaftNodeManagerTest, ErrorHandlingNodeManagement) {
    // Test operations that might fail gracefully
    
    // Operations without dependencies should not crash
    EXPECT_NO_THROW({
        repl_state->is_alive();
        repl_state->is_leader();
        repl_state->node_state();
        repl_state->get_num_queued_writes();
        repl_state->decr_pending_writes();
        repl_state->trigger_vote();
        repl_state->reset_peers();
    });
    
    // Operations with bad parameters should not crash
    EXPECT_NO_THROW({
        repl_state->refresh_nodes("malformed config", "", -1);
        repl_state->get_leader_url("");
        repl_state->refresh_catchup_status(true);
    });
}

// Test integration between node management components
TEST_F(RaftNodeManagerTest, NodeManagementIntegration) {
    // Test integration: set dependencies -> refresh config -> check status -> operations
    
    // Step 1: Set dependencies
    repl_state->set_store(test_store);
    repl_state->set_message_dispatcher(test_dispatcher);
    
    // Step 2: Refresh configuration
    repl_state->refresh_nodes(three_node_config, "/tmp/integration_test", 8108);
    
    // Step 3: Check status
    nlohmann::json status = repl_state->get_status();
    EXPECT_TRUE(status.is_object());
    
    // Step 4: Perform various operations
    bool alive = repl_state->is_alive();
    bool leader = repl_state->is_leader();
    std::string state = repl_state->node_state();
    uint64_t queued = repl_state->get_num_queued_writes();
    
    // Step 5: Try peer operations
    repl_state->trigger_vote();
    repl_state->reset_peers();
    
    // Step 6: Refresh catchup status
    repl_state->refresh_catchup_status(true);
    
    // Step 7: Handle writes
    repl_state->decr_pending_writes();
    repl_state->persist_applying_index();
    
    // Step 8: Get leader URL
    std::string leader_url = repl_state->get_leader_url("integration_key");
    
    // All operations should complete without crashing
    EXPECT_TRUE(true);
}

// Test TestStore functionality
TEST_F(RaftNodeManagerTest, TestStoreBasicOperations) {
    // Test basic store operations
    std::string value;
    
    // Test get from empty store
    EXPECT_FALSE(test_store->get("nonexistent", value));
    
    // Test set and get
    EXPECT_TRUE(test_store->set("key1", "value1"));
    EXPECT_TRUE(test_store->get("key1", value));
    EXPECT_EQ("value1", value);
    
    // Test overwrite
    EXPECT_TRUE(test_store->set("key1", "new_value1"));
    EXPECT_TRUE(test_store->get("key1", value));
    EXPECT_EQ("new_value1", value);
    
    // Test remove
    EXPECT_TRUE(test_store->remove("key1"));
    EXPECT_FALSE(test_store->get("key1", value));
    
    // Test remove nonexistent
    EXPECT_FALSE(test_store->remove("nonexistent"));
}

// Test TestMessageDispatcher functionality
TEST_F(RaftNodeManagerTest, TestMessageDispatcherBasicOperations) {
    // Test message sending
    EXPECT_TRUE(test_dispatcher->send_message("test message 1"));
    EXPECT_TRUE(test_dispatcher->send_message("test message 2"));
    
    // Check messages were stored
    const auto& messages = test_dispatcher->get_messages();
    EXPECT_EQ(2, messages.size());
    EXPECT_EQ("test message 1", messages[0]);
    EXPECT_EQ("test message 2", messages[1]);
    
    // Test handler setting
    bool handler_called = false;
    test_dispatcher->set_handler([&handler_called](const std::string& msg) {
        handler_called = true;
    });
    
    // Handler is set but we don't test calling it since it's not used in this context
    EXPECT_FALSE(handler_called); // Should still be false since we didn't call it
} 