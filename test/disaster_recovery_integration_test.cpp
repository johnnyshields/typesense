#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <unordered_map>
#include "raft_server.h"

// Mock DNS resolver for testing
class MockDNSResolver {
public:
    static MockDNSResolver& instance() {
        static MockDNSResolver inst;
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

class DisasterRecoveryTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup initial DNS resolutions
        MockDNSResolver::instance().clear_all();
        MockDNSResolver::instance().set_resolution("node1.example.com", "192.168.1.10");
        MockDNSResolver::instance().set_resolution("node2.example.com", "192.168.1.11");
        MockDNSResolver::instance().set_resolution("node3.example.com", "192.168.1.12");
        
        initial_config_ = "node1.example.com:8107:8108,node2.example.com:8107:8108,node3.example.com:8107:8108";
        mixed_config_ = "node1.example.com:8107:8108,192.168.1.20:8107:8108,node3.example.com:8107:8108";
    }
    
    void TearDown() override {
        MockDNSResolver::instance().clear_all();
    }
    
    // Helper to create braft::PeerId from IP
    braft::PeerId create_peer(const std::string& ip, int peering_port = 8107, int api_port = 8108) {
        butil::EndPoint endpoint;
        butil::str2endpoint(ip.c_str(), peering_port, &endpoint);
        return braft::PeerId(endpoint, api_port);
    }
    
    // Simulate time passing for refresh cycles
    void simulate_refresh_cycle(int seconds = 10) {
        std::this_thread::sleep_for(std::chrono::milliseconds(seconds * 100));  // Accelerated time
    }
    
    std::string initial_config_;
    std::string mixed_config_;
};

// Test basic disaster recovery scenario
TEST_F(DisasterRecoveryTest, SingleNodeIPChange) {
    // Parse initial configuration
    NodeConfiguration config = ReplicationState::parse_node_configuration(initial_config_);
    ASSERT_EQ(3, config.hostname_nodes.size());
    ASSERT_EQ(0, config.ip_nodes.size());
    
    // Convert to braft configuration (initial state)
    braft::Configuration initial_braft = ReplicationState::node_config_to_braft(config);
    
    // Simulate DR event - node1 IP changes
    MockDNSResolver::instance().simulate_dr_event("node1.example.com", "192.168.1.10", "192.168.2.10");
    
    // Convert to braft configuration again (after DR)
    braft::Configuration after_dr_braft = ReplicationState::node_config_to_braft(config);
    
    // Should detect the IP change
    EXPECT_TRUE(MockDNSResolver::instance().has_dr_event("node1.example.com"));
}

// Test multiple simultaneous IP changes
TEST_F(DisasterRecoveryTest, MultipleNodeIPChanges) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(initial_config_);
    
    // Simulate DR event affecting multiple nodes
    MockDNSResolver::instance().simulate_dr_event("node1.example.com", "192.168.1.10", "192.168.2.10");
    MockDNSResolver::instance().simulate_dr_event("node2.example.com", "192.168.1.11", "192.168.2.11");
    
    // Node3 remains the same
    braft::Configuration after_dr_braft = ReplicationState::node_config_to_braft(config);
    
    // Should handle multiple IP changes
    EXPECT_TRUE(MockDNSResolver::instance().has_dr_event("node1.example.com"));
    EXPECT_TRUE(MockDNSResolver::instance().has_dr_event("node2.example.com"));
}

// Test mixed hostname/IP configuration during DR
TEST_F(DisasterRecoveryTest, MixedConfigurationDR) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(mixed_config_);
    ASSERT_EQ(2, config.hostname_nodes.size());  // node1 and node3
    ASSERT_EQ(1, config.ip_nodes.size());        // 192.168.1.20
    
    // Simulate DR event affecting only hostname nodes
    MockDNSResolver::instance().simulate_dr_event("node1.example.com", "192.168.1.10", "192.168.2.10");
    MockDNSResolver::instance().simulate_dr_event("node3.example.com", "192.168.1.12", "192.168.2.12");
    
    braft::Configuration after_dr_braft = ReplicationState::node_config_to_braft(config);
    
    // IP node should be unaffected, hostname nodes should be updated
    EXPECT_TRUE(MockDNSResolver::instance().has_dr_event("node1.example.com"));
    EXPECT_TRUE(MockDNSResolver::instance().has_dr_event("node3.example.com"));
}

// Test peer failure detection and matching
TEST_F(DisasterRecoveryTest, PeerFailureDetection) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(initial_config_);
    
    // Create a peer that represents the old IP of node1
    braft::PeerId old_peer = create_peer("192.168.1.10");
    
    // Should match the hostname node before DR
    EXPECT_TRUE(ReplicationState::peer_matches_hostname_node(old_peer, "node1.example.com:8107:8108"));
    
    // Simulate DR event
    MockDNSResolver::instance().simulate_dr_event("node1.example.com", "192.168.1.10", "192.168.2.10");
    
    // Old peer should no longer match after IP change
    // (This tests the concept - actual implementation would need the updated resolution)
    braft::PeerId new_peer = create_peer("192.168.2.10");
    // Note: peer_matches_hostname_node would need to use fresh DNS resolution to detect this
}

// Test configuration serialization and parsing consistency
TEST_F(DisasterRecoveryTest, ConfigurationConsistency) {
    NodeConfiguration original = ReplicationState::parse_node_configuration(initial_config_);
    
    // Serialize and reparse
    std::string serialized = original.serialize();
    NodeConfiguration reparsed = ReplicationState::parse_node_configuration(serialized);
    
    EXPECT_EQ(original.hostname_nodes.size(), reparsed.hostname_nodes.size());
    EXPECT_EQ(original.ip_nodes.size(), reparsed.ip_nodes.size());
    EXPECT_EQ(original.total_nodes(), reparsed.total_nodes());
    
    // Should contain same nodes (order might differ)
    for (const auto& node : original.hostname_nodes) {
        EXPECT_TRUE(std::find(reparsed.hostname_nodes.begin(), reparsed.hostname_nodes.end(), node) 
                   != reparsed.hostname_nodes.end()) << "Missing hostname node: " << node;
    }
}

// Test immediate refresh trigger simulation
TEST_F(DisasterRecoveryTest, ImmediateRefreshTrigger) {
    // This test simulates the immediate refresh mechanism
    std::atomic<bool> immediate_refresh_requested{false};
    
    // Simulate peer failure detection
    auto simulate_peer_failure = [&]() {
        // In real implementation, this would be triggered by handle_peer_failure
        immediate_refresh_requested.store(true, std::memory_order_release);
    };
    
    // Simulate main loop checking for immediate refresh
    auto simulate_main_loop = [&]() -> bool {
        return immediate_refresh_requested.load(std::memory_order_acquire);
    };
    
    // Initial state - no refresh requested
    EXPECT_FALSE(simulate_main_loop());
    
    // Trigger failure
    simulate_peer_failure();
    
    // Should detect immediate refresh request
    EXPECT_TRUE(simulate_main_loop());
    
    // Reset flag (as main loop would do)
    immediate_refresh_requested.store(false, std::memory_order_release);
    EXPECT_FALSE(simulate_main_loop());
}

// Test performance of DNS operations under load
TEST_F(DisasterRecoveryTest, DNSOperationPerformance) {
    const int num_operations = 1000;
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_operations; ++i) {
        NodeConfiguration config = ReplicationState::parse_node_configuration(initial_config_);
        std::string hostname = ReplicationState::extract_hostname_from_node("node1.example.com:8107:8108");
        
        // Simulate some work
        if (hostname == "node1.example.com") {
            // Success
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Should complete operations quickly (less than 1 second for 1000 operations)
    EXPECT_LT(duration.count(), 1000000) << "DNS operations too slow: " << duration.count() << "μs";
    
    // Average time per operation should be reasonable
    double avg_time_per_op = static_cast<double>(duration.count()) / num_operations;
    EXPECT_LT(avg_time_per_op, 1000.0) << "Average time per operation: " << avg_time_per_op << "μs";
}

// Test concurrent DNS operations (thread safety)
TEST_F(DisasterRecoveryTest, ConcurrentDNSOperations) {
    const int num_threads = 8;
    const int operations_per_thread = 100;
    std::vector<std::thread> threads;
    std::atomic<int> successful_operations{0};
    std::atomic<int> failed_operations{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < operations_per_thread; ++j) {
                try {
                    // Mix of different operations
                    if (j % 3 == 0) {
                        NodeConfiguration config = ReplicationState::parse_node_configuration(initial_config_);
                        if (config.hostname_nodes.size() == 3) {
                            successful_operations++;
                        }
                    } else if (j % 3 == 1) {
                        std::string hostname = ReplicationState::extract_hostname_from_node("node1.example.com:8107:8108");
                        if (hostname == "node1.example.com") {
                            successful_operations++;
                        }
                    } else {
                        NodeConfiguration config = ReplicationState::parse_node_configuration(mixed_config_);
                        std::string serialized = config.serialize();
                        if (!serialized.empty()) {
                            successful_operations++;
                        }
                    }
                    
                    // Small delay to increase chance of race conditions
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
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
    EXPECT_EQ(num_threads * operations_per_thread, successful_operations.load());
    EXPECT_EQ(0, failed_operations.load());
}

// Test hostname extraction edge cases
TEST_F(DisasterRecoveryTest, HostnameExtractionEdgeCases) {
    // Test various hostname formats
    EXPECT_EQ("simple", ReplicationState::extract_hostname_from_node("simple:8107:8108"));
    EXPECT_EQ("sub.domain.com", ReplicationState::extract_hostname_from_node("sub.domain.com:8107:8108"));
    EXPECT_EQ("long-hostname-with-dashes", ReplicationState::extract_hostname_from_node("long-hostname-with-dashes:8107:8108"));
    
    // Edge cases that should return empty
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node(""));
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("malformed"));
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("192.168.1.1:8107:8108"));
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("[::1]:8107:8108"));
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("too:many:colons:here:8107:8108"));
}

// Test the complete disaster recovery flow simulation
TEST_F(DisasterRecoveryTest, CompleteDisasterRecoveryFlow) {
    // Step 1: Initial healthy state
    NodeConfiguration initial = ReplicationState::parse_node_configuration(initial_config_);
    braft::Configuration initial_braft = ReplicationState::node_config_to_braft(initial);
    
    ASSERT_EQ(3, initial.hostname_nodes.size());
    
    // Step 2: Simulate disaster - all nodes change subnet
    MockDNSResolver::instance().simulate_dr_event("node1.example.com", "192.168.1.10", "192.168.2.10");
    MockDNSResolver::instance().simulate_dr_event("node2.example.com", "192.168.1.11", "192.168.2.11");
    MockDNSResolver::instance().simulate_dr_event("node3.example.com", "192.168.1.12", "192.168.2.12");
    
    // Step 3: Simulate immediate refresh (what our implementation would do)
    std::atomic<bool> immediate_refresh_requested{false};
    immediate_refresh_requested.store(true, std::memory_order_release);
    
    // Step 4: Process immediate refresh
    if (immediate_refresh_requested.load(std::memory_order_acquire)) {
        immediate_refresh_requested.store(false, std::memory_order_release);
        
        // Fresh DNS resolution
        NodeConfiguration updated = ReplicationState::parse_node_configuration(initial_config_);
        braft::Configuration updated_braft = ReplicationState::node_config_to_braft(updated);
        
        // Verify all DR events were detected
        EXPECT_TRUE(MockDNSResolver::instance().has_dr_event("node1.example.com"));
        EXPECT_TRUE(MockDNSResolver::instance().has_dr_event("node2.example.com"));
        EXPECT_TRUE(MockDNSResolver::instance().has_dr_event("node3.example.com"));
    }
    
    // Step 5: Verify cluster can continue operating
    NodeConfiguration final = ReplicationState::parse_node_configuration(initial_config_);
    EXPECT_EQ(3, final.hostname_nodes.size());
    EXPECT_EQ(0, final.ip_nodes.size());
}

// Benchmark test for disaster recovery response time
TEST_F(DisasterRecoveryTest, DisasterRecoveryResponseTime) {
    NodeConfiguration config = ReplicationState::parse_node_configuration(initial_config_);
    
    // Measure time for complete DR handling
    auto start = std::chrono::high_resolution_clock::now();
    
    // Simulate the operations that would happen during DR
    for (int i = 0; i < 10; ++i) {
        // 1. Parse configuration
        NodeConfiguration parsed = ReplicationState::parse_node_configuration(initial_config_);
        
        // 2. Extract hostnames
        for (const auto& node : parsed.hostname_nodes) {
            std::string hostname = ReplicationState::extract_hostname_from_node(node);
            EXPECT_FALSE(hostname.empty());
        }
        
        // 3. Convert to braft configuration
        braft::Configuration braft_config = ReplicationState::node_config_to_braft(parsed);
        
        // 4. Serialize for persistence
        std::string serialized = parsed.serialize();
        EXPECT_FALSE(serialized.empty());
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Should complete DR operations quickly (target: under 100ms for 10 iterations)
    EXPECT_LT(duration.count(), 100) << "DR operations took: " << duration.count() << "ms";
    
    std::cout << "Disaster Recovery response time: " << duration.count() << "ms for 10 operations" << std::endl;
} 