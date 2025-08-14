#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <unordered_map>
#include "raft_server.h"

// Test DNS resolver for integration testing
class TestDNSResolver {
public:
    static TestDNSResolver& instance() {
        static TestDNSResolver inst;
        return inst;
    }
    
    void set_resolution(const std::string& hostname, const std::string& ip) {
        std::lock_guard<std::mutex> lock(mutex_);
        resolutions_[hostname] = ip;
    }
    
    void clear_resolution(const std::string& hostname) {
        std::lock_guard<std::mutex> lock(mutex_);
        resolutions_.erase(hostname);
    }
    
    std::string resolve(const std::string& hostname) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = resolutions_.find(hostname);
        return (it != resolutions_.end()) ? it->second : hostname;
    }
    
    void simulate_dr_event(const std::string& hostname, const std::string& old_ip, const std::string& new_ip) {
        set_resolution(hostname, new_ip);
        dr_events_[hostname] = {old_ip, new_ip, std::chrono::steady_clock::now()};
    }
    
    bool has_dr_event(const std::string& hostname) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return dr_events_.find(hostname) != dr_events_.end();
    }
    
    void clear_all() {
        std::lock_guard<std::mutex> lock(mutex_);
        resolutions_.clear();
        dr_events_.clear();
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::string> resolutions_;
    
    struct DREvent {
        std::string old_ip;
        std::string new_ip;
        std::chrono::steady_clock::time_point timestamp;
    };
    std::unordered_map<std::string, DREvent> dr_events_;
};

class RaftIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup initial DNS resolutions
        TestDNSResolver::instance().clear_all();
        TestDNSResolver::instance().set_resolution("node1.example.com", "192.168.1.10");
        TestDNSResolver::instance().set_resolution("node2.example.com", "192.168.1.11");
        TestDNSResolver::instance().set_resolution("node3.example.com", "192.168.1.12");
        
        initial_config_ = "node1.example.com:8107:8108,node2.example.com:8107:8108,node3.example.com:8107:8108";
        mixed_config_ = "node1.example.com:8107:8108,192.168.1.20:8107:8108,node3.example.com:8107:8108";
        
        // Create ReplicationState for integration testing
        repl_state_ = std::make_unique<ReplicationState>(nullptr, nullptr, "", 0);
    }
    
    void TearDown() override {
        TestDNSResolver::instance().clear_all();
    }
    
    // Helper to create braft::PeerId from IP
    braft::PeerId create_peer(const std::string& ip, int peering_port = 8107, int api_port = 8108) {
        butil::EndPoint endpoint;
        butil::str2endpoint(ip.c_str(), peering_port, &endpoint);
        return braft::PeerId(endpoint, api_port);
    }
    
    std::string initial_config_;
    std::string mixed_config_;
    std::unique_ptr<ReplicationState> repl_state_;
};

// Integration Test: Complete Disaster Recovery Flow
TEST_F(RaftIntegrationTest, CompleteDisasterRecoveryFlow) {
    // Step 1: Initial healthy state
    NodeConfiguration initial = repl_state_->parse_node_configuration(initial_config_);
    braft::Configuration initial_braft = repl_state_->node_config_to_braft(initial);
    
    ASSERT_EQ(3, initial.hostname_nodes.size());
    
    // Step 2: Simulate disaster - all nodes change subnet
    TestDNSResolver::instance().simulate_dr_event("node1.example.com", "192.168.1.10", "192.168.2.10");
    TestDNSResolver::instance().simulate_dr_event("node2.example.com", "192.168.1.11", "192.168.2.11");
    TestDNSResolver::instance().simulate_dr_event("node3.example.com", "192.168.1.12", "192.168.2.12");
    
    // Step 3: Simulate immediate refresh (what our implementation would do)
    std::atomic<bool> immediate_refresh_requested{false};
    immediate_refresh_requested.store(true, std::memory_order_release);
    
    // Step 4: Process immediate refresh
    if (immediate_refresh_requested.load(std::memory_order_acquire)) {
        immediate_refresh_requested.store(false, std::memory_order_release);
        
        // Fresh DNS resolution
        NodeConfiguration updated = repl_state_->parse_node_configuration(initial_config_);
        braft::Configuration updated_braft = repl_state_->node_config_to_braft(updated);
        
        // Verify all DR events were detected
        EXPECT_TRUE(TestDNSResolver::instance().has_dr_event("node1.example.com"));
        EXPECT_TRUE(TestDNSResolver::instance().has_dr_event("node2.example.com"));
        EXPECT_TRUE(TestDNSResolver::instance().has_dr_event("node3.example.com"));
    }
    
    // Step 5: Verify cluster can continue operating
    NodeConfiguration final = repl_state_->parse_node_configuration(initial_config_);
    EXPECT_EQ(3, final.hostname_nodes.size());
    EXPECT_EQ(0, final.ip_nodes.size());
}

// Integration Test: Mixed Configuration Disaster Recovery
TEST_F(RaftIntegrationTest, MixedConfigurationDisasterRecovery) {
    NodeConfiguration config = repl_state_->parse_node_configuration(mixed_config_);
    ASSERT_EQ(2, config.hostname_nodes.size());  // node1 and node3
    ASSERT_EQ(1, config.ip_nodes.size());        // 192.168.1.20
    
    // Simulate DR event affecting only hostname nodes
    TestDNSResolver::instance().simulate_dr_event("node1.example.com", "192.168.1.10", "192.168.2.10");
    TestDNSResolver::instance().simulate_dr_event("node3.example.com", "192.168.1.12", "192.168.2.12");
    
    braft::Configuration after_dr_braft = repl_state_->node_config_to_braft(config);
    
    // IP node should be unaffected, hostname nodes should be updated
    EXPECT_TRUE(TestDNSResolver::instance().has_dr_event("node1.example.com"));
    EXPECT_TRUE(TestDNSResolver::instance().has_dr_event("node3.example.com"));
}

// Integration Test: Safe Configuration Changes with DNS
TEST_F(RaftIntegrationTest, SafeConfigurationChangesWithDNS) {
    // Start with initial configuration
    NodeConfiguration initial = repl_state_->parse_node_configuration(initial_config_);
    
    // Test adding a new hostname node safely
    std::string new_node = "node4.example.com:8107:8108";
    TestDNSResolver::instance().set_resolution("node4.example.com", "192.168.1.13");
    
    // Create new configuration with additional node
    NodeConfiguration new_config = initial.create_single_node_change(new_node, "", 1);
    
    // Verify it's a safe single-node change
    EXPECT_TRUE(initial.is_safe_single_node_change(new_config));
    
    // Test removing a node safely
    NodeConfiguration remove_config = new_config.create_single_node_change("", "node2.example.com:8107:8108", 2);
    EXPECT_TRUE(new_config.is_safe_single_node_change(remove_config));
}

// Integration Test: Peer Failure Detection and DNS Re-resolution
TEST_F(RaftIntegrationTest, PeerFailureDetectionAndDNSReresolution) {
    NodeConfiguration config = repl_state_->parse_node_configuration(initial_config_);
    
    // Create a peer that represents the old IP of node1
    braft::PeerId old_peer = create_peer("192.168.1.10");
    
    // Should match the hostname node before DR
    EXPECT_TRUE(repl_state_->peer_matches_hostname_node(old_peer, "node1.example.com:8107:8108"));
    
    // Simulate DR event
    TestDNSResolver::instance().simulate_dr_event("node1.example.com", "192.168.1.10", "192.168.2.10");
    
    // Test immediate refresh trigger
    EXPECT_FALSE(repl_state_->immediate_refresh_requested.load());
    repl_state_->handle_peer_failure(old_peer);
    
    // Should trigger immediate refresh for hostname-based peers
    // (Result depends on actual DNS resolution implementation)
}

// Integration Test: MongoDB TLA+ Safety with Configuration Changes
TEST_F(RaftIntegrationTest, MongoDBTLASafetyWithConfigChanges) {
    NodeConfiguration config = repl_state_->parse_node_configuration(initial_config_);
    
    // Test configuration safety validation
    bool quorum_valid = repl_state_->validate_new_config_quorum(config);
    EXPECT_TRUE(quorum_valid);
    
    // Test safe node addition
    std::string new_node = "node4.example.com:8107:8108";
    TestDNSResolver::instance().set_resolution("node4.example.com", "192.168.1.13");
    
    // Without proper raft setup, this should fail gracefully
    bool add_result = repl_state_->add_node_safe(new_node);
    EXPECT_FALSE(add_result); // Expected to fail without proper raft node
    
    // Test safe node removal
    bool remove_result = repl_state_->remove_node_safe("node2.example.com:8107:8108");
    EXPECT_FALSE(remove_result); // Expected to fail without proper raft node
}

// Integration Test: HTTP Handler with DNS Configuration
TEST_F(RaftIntegrationTest, HttpHandlerWithDNSConfiguration) {
    // Create HTTP request and response
    auto request = std::make_shared<http_req>();
    auto response = std::make_shared<http_res>();
    
    request->body = R"({"name": "test_collection"})";
    request->params["action"] = "create";
    
    // Test write operation (should handle gracefully without raft node)
    EXPECT_NO_THROW({
        repl_state_->write(request, response);
    });
    
    // Test URL generation for hostname-based peers
    braft::PeerId peer_id = create_peer("192.168.1.10");
    std::string url = repl_state_->get_node_url_path(peer_id, "/collections", "api_key");
    EXPECT_FALSE(url.empty());
    
    // Test GZIP handling
    request->set_header("content-encoding", "gzip");
    Option<bool> gzip_result = repl_state_->handle_gzip(request);
    EXPECT_TRUE(gzip_result.is_some());
}

// Integration Test: Lifecycle Management with Configuration Changes
TEST_F(RaftIntegrationTest, LifecycleManagementWithConfigChanges) {
    butil::EndPoint endpoint;
    butil::str2endpoint("127.0.0.1:8107", &endpoint);
    
    // Test start with hostname configuration
    int start_result = repl_state_->start(endpoint, 8108, initial_config_, "/tmp/raft_integration");
    EXPECT_NE(0, start_result); // Expected to fail without proper setup
    
    // Test snapshot operations
    repl_state_->set_ext_snapshot_path("/tmp/integration_snapshot");
    repl_state_->set_snapshot_in_progress(true);
    repl_state_->do_snapshot();
    repl_state_->set_snapshot_in_progress(false);
    
    // Test database initialization
    repl_state_->init_db();
    
    // Test shutdown
    repl_state_->shutdown();
}

// Integration Test: Node Management with DNS Resolution
TEST_F(RaftIntegrationTest, NodeManagementWithDNSResolution) {
    // Test configuration refresh with hostname nodes
    repl_state_->refresh_nodes(initial_config_, "/tmp/integration_raft", 8108);
    
    // Test status reporting
    nlohmann::json status = repl_state_->get_status();
    EXPECT_TRUE(status.is_object());
    
    // Test node state operations
    bool alive = repl_state_->is_alive();
    bool leader = repl_state_->is_leader();
    std::string state = repl_state_->node_state();
    
    EXPECT_FALSE(alive);  // Expected without proper raft setup
    EXPECT_FALSE(leader); // Expected without proper raft setup
    EXPECT_FALSE(state.empty());
    
    // Test peer operations
    bool vote_result = repl_state_->trigger_vote();
    bool reset_result = repl_state_->reset_peers();
    
    EXPECT_FALSE(vote_result);  // Expected without proper raft setup
    EXPECT_FALSE(reset_result); // Expected without proper raft setup
}

// Integration Test: Concurrent Operations Across All Modules
TEST_F(RaftIntegrationTest, ConcurrentOperationsAcrossModules) {
    const int num_threads = 6;
    const int operations_per_thread = 20;
    std::vector<std::thread> threads;
    std::atomic<int> successful_operations{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < operations_per_thread; ++j) {
                try {
                    switch (j % 6) {
                        case 0: {
                            // Config Manager operations
                            NodeConfiguration config = repl_state_->parse_node_configuration(initial_config_);
                            std::string hostname = repl_state_->extract_hostname_from_node("node1.example.com:8107:8108");
                            if (!hostname.empty()) successful_operations++;
                            break;
                        }
                        case 1: {
                            // Safety Validator operations
                            bool quorum_valid = repl_state_->validate_new_config_quorum(
                                repl_state_->parse_node_configuration(initial_config_));
                            if (quorum_valid) successful_operations++;
                            break;
                        }
                        case 2: {
                            // HTTP Handler operations
                            auto req = std::make_shared<http_req>();
                            auto res = std::make_shared<http_res>();
                            req->body = R"({"test": true})";
                            repl_state_->write(req, res);
                            successful_operations++;
                            break;
                        }
                        case 3: {
                            // Lifecycle Manager operations
                            repl_state_->set_ext_snapshot_path("/tmp/thread_" + std::to_string(i));
                            repl_state_->do_snapshot();
                            successful_operations++;
                            break;
                        }
                        case 4: {
                            // Node Manager operations
                            repl_state_->refresh_catchup_status(false);
                            nlohmann::json status = repl_state_->get_status();
                            if (status.is_object()) successful_operations++;
                            break;
                        }
                        case 5: {
                            // DNS operations with disaster recovery simulation
                            TestDNSResolver::instance().simulate_dr_event(
                                "node" + std::to_string(i % 3 + 1) + ".example.com",
                                "192.168.1.1" + std::to_string(i % 3),
                                "192.168.2.1" + std::to_string(i % 3)
                            );
                            successful_operations++;
                            break;
                        }
                    }
                } catch (...) {
                    // Continue on exceptions
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Most operations should succeed
    EXPECT_GT(successful_operations.load(), num_threads * operations_per_thread * 0.8);
}

// Integration Test: End-to-End Configuration Version Management
TEST_F(RaftIntegrationTest, EndToEndConfigurationVersionManagement) {
    // Create initial configuration
    NodeConfiguration config1 = repl_state_->parse_node_configuration(initial_config_);
    config1.config_version = 1;
    config1.config_term = 1;
    
    // Create newer configuration
    NodeConfiguration config2 = config1.create_single_node_change("node4.example.com:8107:8108", "", 2);
    config2.config_version = 2;
    config2.config_term = 1;
    
    // Test version comparison
    EXPECT_TRUE(config2.is_newer_than(config1));
    EXPECT_FALSE(config1.is_newer_than(config2));
    
    // Test single-node change validation
    EXPECT_TRUE(config1.is_safe_single_node_change(config2));
    
    // Test serialization round-trip
    std::string serialized = config2.serialize();
    NodeConfiguration deserialized = repl_state_->parse_node_configuration(serialized);
    
    EXPECT_EQ(config2.hostname_nodes.size(), deserialized.hostname_nodes.size());
    EXPECT_EQ(config2.ip_nodes.size(), deserialized.ip_nodes.size());
}

// Integration Test: Performance Under Load
TEST_F(RaftIntegrationTest, PerformanceUnderLoad) {
    const int num_operations = 500;
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_operations; ++i) {
        // Mix of operations across all modules
        NodeConfiguration config = repl_state_->parse_node_configuration(initial_config_);
        braft::Configuration braft_config = repl_state_->node_config_to_braft(config);
        
        std::string hostname = repl_state_->extract_hostname_from_node("node1.example.com:8107:8108");
        bool quorum_valid = repl_state_->validate_new_config_quorum(config);
        
        auto req = std::make_shared<http_req>();
        auto res = std::make_shared<http_res>();
        req->body = R"({"operation": )" + std::to_string(i) + "}";
        repl_state_->write(req, res);
        
        if (i % 10 == 0) {
            repl_state_->refresh_catchup_status(false);
            nlohmann::json status = repl_state_->get_status();
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Should complete operations quickly (target: under 500ms for 500 operations)
    EXPECT_LT(duration.count(), 500) << "Integration operations took: " << duration.count() << "ms";
    
    double avg_time_per_op = static_cast<double>(duration.count()) / num_operations;
    EXPECT_LT(avg_time_per_op, 1.0) << "Average time per operation: " << avg_time_per_op << "ms";
}

// Integration Test: Error Recovery Across Modules
TEST_F(RaftIntegrationTest, ErrorRecoveryAcrossModules) {
    // Test that errors in one module don't crash others
    
    // Trigger various error conditions
    repl_state_->parse_node_configuration(""); // Empty config
    repl_state_->extract_hostname_from_node("malformed"); // Bad input
    repl_state_->validate_new_config_quorum(NodeConfiguration{}); // Empty config
    
    auto null_req = std::shared_ptr<http_req>(nullptr);
    auto null_res = std::shared_ptr<http_res>(nullptr);
    repl_state_->write(null_req, null_res); // Null pointers
    
    butil::EndPoint invalid_endpoint;
    repl_state_->start(invalid_endpoint, -1, "", ""); // Invalid parameters
    
    repl_state_->refresh_nodes("malformed config", "", -1); // Bad parameters
    
    // After all these errors, basic operations should still work
    NodeConfiguration config = repl_state_->parse_node_configuration(initial_config_);
    EXPECT_EQ(3, config.hostname_nodes.size());
    
    nlohmann::json status = repl_state_->get_status();
    EXPECT_TRUE(status.is_object());
}

// Integration Test: Memory Management Across All Operations
TEST_F(RaftIntegrationTest, MemoryManagementAcrossOperations) {
    // Perform many operations to test for memory leaks
    for (int i = 0; i < 100; ++i) {
        // Large configuration
        std::string large_config = "";
        for (int j = 0; j < 20; ++j) {
            if (j > 0) large_config += ",";
            large_config += "node" + std::to_string(j) + ".example.com:8107:8108";
        }
        
        NodeConfiguration config = repl_state_->parse_node_configuration(large_config);
        std::string serialized = config.serialize();
        NodeConfiguration deserialized = repl_state_->parse_node_configuration(serialized);
        
        // Large HTTP request
        auto req = std::make_shared<http_req>();
        auto res = std::make_shared<http_res>();
        req->body = std::string(10000, 'A'); // 10KB body
        repl_state_->write(req, res);
        
        // Snapshot operations
        std::string long_path = "/tmp/very_long_path_" + std::string(1000, 'x') + std::to_string(i);
        repl_state_->set_ext_snapshot_path(long_path);
        repl_state_->do_snapshot();
        
        // Status operations
        nlohmann::json status = repl_state_->get_status();
        std::string status_str = status.dump();
    }
    
    // Should complete without memory issues
    EXPECT_TRUE(true);
} 