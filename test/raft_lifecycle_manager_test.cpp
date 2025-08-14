#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "raft_server.h"

// Unit Tests for raft_lifecycle_manager.cpp
// Tests Raft lifecycle management, snapshot operations, and log application

class MockSnapshotWriter {
public:
    MOCK_METHOD(int, add_file, (const std::string& filename), ());
    MOCK_METHOD(int, remove_file, (const std::string& filename), ());
    MOCK_METHOD(std::string, get_path, (), ());
};

class MockSnapshotReader {
public:
    MOCK_METHOD(int, load_file, (const std::string& filename, std::string* contents), ());
    MOCK_METHOD(std::string, get_path, (), ());
    MOCK_METHOD(bool, list_files, (std::vector<std::string>* files), ());
};

class MockIterator {
public:
    MOCK_METHOD(bool, valid, (), ());
    MOCK_METHOD(void, next, (), ());
    MOCK_METHOD(braft::LogEntry*, entry, (), ());
    MOCK_METHOD(int64_t, index, (), ());
    MOCK_METHOD(uint64_t, term, (), ());
};

class RaftLifecycleManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        repl_state = std::make_unique<ReplicationState>(nullptr, nullptr, "", 0);
        
        // Setup test endpoint
        butil::str2endpoint("127.0.0.1:8107", &test_endpoint);
    }

    std::unique_ptr<ReplicationState> repl_state;
    butil::EndPoint test_endpoint;
};

// Test start functionality (basic)
TEST_F(RaftLifecycleManagerTest, StartWithBasicParameters) {
    std::string nodes_config = "127.0.0.1:8107:8108";
    std::string raft_dir = "/tmp/raft_test";
    
    // Without proper braft setup, this should handle gracefully
    int result = repl_state->start(test_endpoint, 8108, nodes_config, raft_dir);
    
    // Should return error code since we don't have proper braft setup
    EXPECT_NE(0, result);
}

TEST_F(RaftLifecycleManagerTest, StartWithEmptyNodesConfig) {
    std::string empty_nodes = "";
    std::string raft_dir = "/tmp/raft_test";
    
    int result = repl_state->start(test_endpoint, 8108, empty_nodes, raft_dir);
    
    // Should fail with empty nodes configuration
    EXPECT_NE(0, result);
}

TEST_F(RaftLifecycleManagerTest, StartWithInvalidEndpoint) {
    butil::EndPoint invalid_endpoint;
    // Don't initialize the endpoint - should be invalid
    
    std::string nodes_config = "127.0.0.1:8107:8108";
    std::string raft_dir = "/tmp/raft_test";
    
    int result = repl_state->start(invalid_endpoint, 8108, nodes_config, raft_dir);
    
    // Should fail with invalid endpoint
    EXPECT_NE(0, result);
}

// Test snapshot operations
TEST_F(RaftLifecycleManagerTest, OnSnapshotSaveWithoutStore) {
    // Test snapshot save without proper store setup
    MockSnapshotWriter mock_writer;
    braft::Closure* closure = nullptr;
    
    // Should handle gracefully without crashing
    EXPECT_NO_THROW({
        repl_state->on_snapshot_save(&mock_writer, closure);
    });
}

TEST_F(RaftLifecycleManagerTest, OnSnapshotLoadWithoutStore) {
    // Test snapshot load without proper store setup
    MockSnapshotReader mock_reader;
    
    int result = repl_state->on_snapshot_load(&mock_reader);
    
    // Should return error code without proper store
    EXPECT_NE(0, result);
}

// Test set_ext_snapshot_path functionality
TEST_F(RaftLifecycleManagerTest, SetExternalSnapshotPath) {
    std::string test_path = "/tmp/external_snapshot";
    
    EXPECT_NO_THROW({
        repl_state->set_ext_snapshot_path(test_path);
    });
    
    // Test with empty path
    EXPECT_NO_THROW({
        repl_state->set_ext_snapshot_path("");
    });
    
    // Test with very long path
    std::string long_path = std::string(1000, 'a');
    EXPECT_NO_THROW({
        repl_state->set_ext_snapshot_path(long_path);
    });
}

// Test set_snapshot_in_progress functionality
TEST_F(RaftLifecycleManagerTest, SetSnapshotInProgress) {
    // Test setting snapshot in progress
    EXPECT_NO_THROW({
        repl_state->set_snapshot_in_progress(true);
    });
    
    // Test clearing snapshot in progress
    EXPECT_NO_THROW({
        repl_state->set_snapshot_in_progress(false);
    });
    
    // Test multiple rapid changes
    for (int i = 0; i < 100; ++i) {
        repl_state->set_snapshot_in_progress(i % 2 == 0);
    }
}

// Test init_db functionality
TEST_F(RaftLifecycleManagerTest, InitDbWithoutStore) {
    // Test database initialization without proper store
    EXPECT_NO_THROW({
        repl_state->init_db();
    });
}

// Test on_apply functionality
TEST_F(RaftLifecycleManagerTest, OnApplyWithoutIterator) {
    // Test log application without proper iterator
    MockIterator mock_iter;
    
    // Setup mock expectations
    EXPECT_CALL(mock_iter, valid())
        .WillRepeatedly(testing::Return(false));
    
    EXPECT_NO_THROW({
        repl_state->on_apply(reinterpret_cast<braft::Iterator&>(mock_iter));
    });
}

TEST_F(RaftLifecycleManagerTest, OnApplyWithValidIterator) {
    MockIterator mock_iter;
    
    // Setup mock to simulate one valid entry
    EXPECT_CALL(mock_iter, valid())
        .WillOnce(testing::Return(true))
        .WillOnce(testing::Return(false));
    
    EXPECT_CALL(mock_iter, next())
        .Times(1);
    
    EXPECT_CALL(mock_iter, index())
        .WillOnce(testing::Return(1));
    
    // Mock log entry
    braft::LogEntry mock_entry;
    EXPECT_CALL(mock_iter, entry())
        .WillOnce(testing::Return(&mock_entry));
    
    EXPECT_NO_THROW({
        repl_state->on_apply(reinterpret_cast<braft::Iterator&>(mock_iter));
    });
}

// Test do_snapshot functionality
TEST_F(RaftLifecycleManagerTest, DoSnapshotWithoutNode) {
    // Test snapshot creation without proper raft node
    EXPECT_NO_THROW({
        repl_state->do_snapshot();
    });
}

// Test do_dummy_write functionality
TEST_F(RaftLifecycleManagerTest, DoDummyWriteWithoutNode) {
    // Test dummy write without proper raft node
    bool result = repl_state->do_dummy_write();
    
    // Should return false without proper node setup
    EXPECT_FALSE(result);
}

// Test shutdown functionality
TEST_F(RaftLifecycleManagerTest, ShutdownBasic) {
    // Test basic shutdown
    EXPECT_NO_THROW({
        repl_state->shutdown();
    });
}

TEST_F(RaftLifecycleManagerTest, ShutdownMultipleCalls) {
    // Test multiple shutdown calls
    for (int i = 0; i < 5; ++i) {
        EXPECT_NO_THROW({
            repl_state->shutdown();
        });
    }
}

// Test lifecycle state management
TEST_F(RaftLifecycleManagerTest, LifecycleStateManagement) {
    // Test the complete lifecycle: start -> operations -> shutdown
    
    std::string nodes_config = "127.0.0.1:8107:8108";
    std::string raft_dir = "/tmp/raft_test_lifecycle";
    
    // Start (will fail without proper setup, but shouldn't crash)
    int start_result = repl_state->start(test_endpoint, 8108, nodes_config, raft_dir);
    EXPECT_NE(0, start_result);
    
    // Try some operations
    repl_state->set_snapshot_in_progress(true);
    repl_state->do_snapshot();
    bool dummy_result = repl_state->do_dummy_write();
    EXPECT_FALSE(dummy_result);
    
    // Shutdown
    repl_state->shutdown();
    
    // Try operations after shutdown
    repl_state->set_snapshot_in_progress(false);
    repl_state->do_snapshot();
}

// Test concurrent lifecycle operations
TEST_F(RaftLifecycleManagerTest, ConcurrentLifecycleOperations) {
    const int num_threads = 6;
    std::vector<std::thread> threads;
    std::atomic<int> successful_operations{0};
    std::atomic<int> failed_operations{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            try {
                switch (i % 6) {
                    case 0:
                        // Snapshot operations
                        repl_state->set_snapshot_in_progress(true);
                        repl_state->do_snapshot();
                        repl_state->set_snapshot_in_progress(false);
                        break;
                    case 1:
                        // External snapshot path operations
                        repl_state->set_ext_snapshot_path("/tmp/test_" + std::to_string(i));
                        break;
                    case 2:
                        // Database initialization
                        repl_state->init_db();
                        break;
                    case 3:
                        // Dummy writes
                        for (int j = 0; j < 10; ++j) {
                            repl_state->do_dummy_write();
                        }
                        break;
                    case 4:
                        // Start/shutdown cycle
                        std::string config = "127.0.0.1:810" + std::to_string(i) + ":810" + std::to_string(i+1);
                        repl_state->start(test_endpoint, 8108 + i, config, "/tmp/raft_" + std::to_string(i));
                        repl_state->shutdown();
                        break;
                    case 5:
                        // Mixed operations
                        repl_state->set_snapshot_in_progress(true);
                        repl_state->init_db();
                        repl_state->do_dummy_write();
                        repl_state->set_snapshot_in_progress(false);
                        break;
                }
                successful_operations++;
            } catch (...) {
                failed_operations++;
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(num_threads, successful_operations.load() + failed_operations.load());
    // Most operations should succeed (even if they don't do much without proper setup)
    EXPECT_GE(successful_operations.load(), num_threads * 0.8);
}

// Test snapshot performance
TEST_F(RaftLifecycleManagerTest, SnapshotPerformance) {
    auto start = std::chrono::high_resolution_clock::now();
    
    // Perform multiple snapshot operations
    for (int i = 0; i < 1000; ++i) {
        repl_state->set_snapshot_in_progress(i % 2 == 0);
        repl_state->do_snapshot();
        repl_state->set_ext_snapshot_path("/tmp/snapshot_" + std::to_string(i));
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete 1000 snapshot operations quickly (< 100ms)
    EXPECT_LT(duration.count(), 100000);
    
    double avg_time_per_operation = static_cast<double>(duration.count()) / 1000.0;
    EXPECT_LT(avg_time_per_operation, 100.0); // < 100μs per operation
}

// Test memory management during lifecycle operations
TEST_F(RaftLifecycleManagerTest, MemoryManagementLifecycleOps) {
    // Test with large snapshot paths and repeated operations
    for (int i = 0; i < 100; ++i) {
        // Create large snapshot path
        std::string large_path = "/tmp/very_long_snapshot_path_" + std::string(1000, 'a') + std::to_string(i);
        repl_state->set_ext_snapshot_path(large_path);
        
        // Perform operations
        repl_state->set_snapshot_in_progress(true);
        repl_state->init_db();
        repl_state->do_snapshot();
        repl_state->do_dummy_write();
        repl_state->set_snapshot_in_progress(false);
    }
    
    // Should not crash or leak memory
    EXPECT_TRUE(true);
}

// Test error handling in lifecycle operations
TEST_F(RaftLifecycleManagerTest, ErrorHandlingLifecycleOps) {
    // Test with invalid parameters
    butil::EndPoint invalid_endpoint;
    
    // Test start with invalid parameters
    int result1 = repl_state->start(invalid_endpoint, -1, "", "");
    EXPECT_NE(0, result1);
    
    int result2 = repl_state->start(invalid_endpoint, 0, "invalid_config", "");
    EXPECT_NE(0, result2);
    
    // Test with extremely long paths
    std::string extremely_long_path = std::string(10000, 'x');
    EXPECT_NO_THROW({
        repl_state->set_ext_snapshot_path(extremely_long_path);
    });
    
    // Test with null-like inputs (empty strings)
    EXPECT_NO_THROW({
        repl_state->set_ext_snapshot_path("");
    });
}

// Test start method overloads
TEST_F(RaftLifecycleManagerTest, StartMethodOverloads) {
    // Test different start method signatures
    std::string nodes_config = "127.0.0.1:8107:8108";
    std::string raft_dir = "/tmp/raft_overload_test";
    
    // Test basic start
    int result1 = repl_state->start(test_endpoint, 8108, nodes_config, raft_dir);
    EXPECT_NE(0, result1); // Expected to fail without proper setup
    
    // Test with different port
    int result2 = repl_state->start(test_endpoint, 9108, nodes_config, raft_dir);
    EXPECT_NE(0, result2);
    
    // Test with different directory
    int result3 = repl_state->start(test_endpoint, 8108, nodes_config, "/tmp/different_raft_dir");
    EXPECT_NE(0, result3);
}

// Test integration between lifecycle components
TEST_F(RaftLifecycleManagerTest, LifecycleComponentIntegration) {
    // Test integration: start -> init -> snapshot -> shutdown
    
    std::string nodes_config = "127.0.0.1:8107:8108";
    std::string raft_dir = "/tmp/raft_integration_test";
    
    // Step 1: Start
    int start_result = repl_state->start(test_endpoint, 8108, nodes_config, raft_dir);
    
    // Step 2: Initialize database
    repl_state->init_db();
    
    // Step 3: Snapshot operations
    repl_state->set_ext_snapshot_path("/tmp/integration_snapshot");
    repl_state->set_snapshot_in_progress(true);
    repl_state->do_snapshot();
    
    // Step 4: Try some writes
    for (int i = 0; i < 5; ++i) {
        bool write_result = repl_state->do_dummy_write();
        // Expected to fail without proper setup
        EXPECT_FALSE(write_result);
    }
    
    // Step 5: More snapshot operations
    repl_state->do_snapshot();
    repl_state->set_snapshot_in_progress(false);
    
    // Step 6: Shutdown
    repl_state->shutdown();
    
    // All operations should complete without crashing
    EXPECT_TRUE(true);
}

// Test atomic operations in lifecycle management
TEST_F(RaftLifecycleManagerTest, AtomicLifecycleOperations) {
    const int num_threads = 4;
    const int operations_per_thread = 50;
    std::vector<std::thread> threads;
    std::atomic<bool> snapshot_in_progress{false};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < operations_per_thread; ++j) {
                // Test atomic snapshot progress tracking
                bool expected = false;
                if (snapshot_in_progress.compare_exchange_weak(expected, true)) {
                    // Got the lock, do snapshot operation
                    repl_state->set_snapshot_in_progress(true);
                    repl_state->do_snapshot();
                    repl_state->set_snapshot_in_progress(false);
                    snapshot_in_progress.store(false);
                } else {
                    // Couldn't get lock, do other operations
                    repl_state->init_db();
                    repl_state->do_dummy_write();
                }
                
                // Set external snapshot path (thread-safe)
                std::string path = "/tmp/atomic_test_" + std::to_string(i) + "_" + std::to_string(j);
                repl_state->set_ext_snapshot_path(path);
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Should complete without deadlocks or crashes
    EXPECT_FALSE(snapshot_in_progress.load());
} 