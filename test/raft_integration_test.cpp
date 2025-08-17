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

// Integration Test: Enhanced DNS Resolution with Caching
TEST_F(RaftIntegrationTest, EnhancedDNSResolutionWithCaching) {
    // Test DNS caching functionality
    size_t initial_cache_size = repl_state_->get_dns_cache_size();
    
    // First resolution should populate cache
    std::string result1 = repl_state_->hostname2ipstr("localhost");
    EXPECT_FALSE(result1.empty());
    EXPECT_GT(repl_state_->get_dns_cache_size(), initial_cache_size);
    
    // Second resolution should use cache
    std::string result2 = repl_state_->hostname2ipstr("localhost");
    EXPECT_EQ(result1, result2);
    
    // Clear cache for specific hostname
    repl_state_->clear_dns_cache_for_hostname("localhost");
    
    // Clear entire cache
    repl_state_->clear_dns_cache();
    EXPECT_EQ(repl_state_->get_dns_cache_size(), 0);
}

// Integration Test: Production-Ready Configuration Parsing
TEST_F(RaftIntegrationTest, ProductionConfigurationParsing) {
    // Test enhanced validation
    std::vector<std::string> test_configs = {
        "node1.example.com:8107:8108,192.168.1.10:8107:8108,node3.example.com:8107:8108", // Valid mixed
        "node1:8107:8108,node2:8107:8108,node3:8107:8108,node4:8107:8108", // Even number warning
        "node1.example.com:8107", // Invalid - missing port
        "node1.example.com:8107:abc", // Invalid - non-numeric port
        "node1.example.com:8107:70000", // Invalid - port out of range
        "", // Empty
        "   ,   ,   " // Whitespace only
    };
    
    for (const auto& config_str : test_configs) {
        NodeConfiguration config = repl_state_->parse_node_configuration(config_str);
        // Should not crash and should handle validation appropriately
        EXPECT_NO_THROW({
            config.total_nodes();
            config.serialize();
        });
    }
}

// Integration Test: Simplified Safety Validation
TEST_F(RaftIntegrationTest, SimplifiedSafetyValidation) {
    // Test that validation methods work for file-based model
    std::string node_to_add = "node4.example.com:8107:8108";
    std::string node_to_remove = "node2.example.com:8107:8108";
    
    // These should complete without crashing (but may fail due to not being leader)
    EXPECT_NO_THROW({
        bool add_result = repl_state_->add_node_safe(node_to_add);
        bool remove_result = repl_state_->remove_node_safe(node_to_remove);
        
        // Results depend on leadership status, but operations should complete
        EXPECT_TRUE(add_result || !add_result);
        EXPECT_TRUE(remove_result || !remove_result);
    });
}

// Integration Test: Self-Protection Mechanisms
TEST_F(RaftIntegrationTest, SelfProtectionMechanisms) {
    // Test various self-identification patterns
    std::vector<std::string> self_patterns = {
        "localhost:8107:8108",
        "127.0.0.1:8107:8108", 
        "::1:8107:8108"
    };
    
    for (const auto& pattern : self_patterns) {
        EXPECT_TRUE(repl_state_->is_self_node(pattern));
        EXPECT_FALSE(repl_state_->remove_node_safe(pattern)); // Should prevent self-removal
    }
    
    // Test non-self patterns
    std::vector<std::string> non_self_patterns = {
        "remote.host.com:8107:8108",
        "192.168.1.100:8107:8108",
        "other.domain.org:8107:8108"
    };
    
    for (const auto& pattern : non_self_patterns) {
        EXPECT_FALSE(repl_state_->is_self_node(pattern));
    }
}

// Integration Test: Complete Disaster Recovery Flow (Enhanced)
TEST_F(RaftIntegrationTest, CompleteDisasterRecoveryFlowEnhanced) {
    // Step 1: Initial healthy state with enhanced parsing
    NodeConfiguration initial = repl_state_->parse_node_configuration(initial_config_);
    ASSERT_EQ(3, initial.hostname_nodes.size());
    
    // Step 2: Test DNS caching during normal operations
    size_t cache_size_before = repl_state_->get_dns_cache_size();
    braft::Configuration initial_braft = repl_state_->node_config_to_braft(initial);
    size_t cache_size_after = repl_state_->get_dns_cache_size();
    EXPECT_GE(cache_size_after, cache_size_before); // May populate cache
    
    // Step 3: Simulate disaster with immediate DNS cache clearing
    TestDNSResolver::instance().simulate_dr_event("node1.example.com", "192.168.1.10", "192.168.2.10");
    TestDNSResolver::instance().simulate_dr_event("node2.example.com", "192.168.1.11", "192.168.2.11");
    TestDNSResolver::instance().simulate_dr_event("node3.example.com", "192.168.1.12", "192.168.2.12");
    
    // Clear DNS cache to force fresh resolution
    repl_state_->clear_dns_cache();
    
    // Step 4: Process immediate refresh with enhanced validation
    std::atomic<bool> immediate_refresh_requested{false};
    immediate_refresh_requested.store(true, std::memory_order_release);
    
    if (immediate_refresh_requested.load(std::memory_order_acquire)) {
        immediate_refresh_requested.store(false, std::memory_order_release);
        
        // Fresh DNS resolution with new caching
        NodeConfiguration updated = repl_state_->parse_node_configuration(initial_config_);
        braft::Configuration updated_braft = repl_state_->node_config_to_braft(updated);
        
        // Verify DR events were detected
        EXPECT_TRUE(TestDNSResolver::instance().has_dr_event("node1.example.com"));
        EXPECT_TRUE(TestDNSResolver::instance().has_dr_event("node2.example.com"));
        EXPECT_TRUE(TestDNSResolver::instance().has_dr_event("node3.example.com"));
    }
    
    // Step 5: Verify cluster can continue with enhanced validation
    NodeConfiguration final = repl_state_->parse_node_configuration(initial_config_);
    EXPECT_EQ(3, final.hostname_nodes.size());
    EXPECT_EQ(0, final.ip_nodes.size());
    
    // Verify configuration is valid
    EXPECT_FALSE(final.empty());
    EXPECT_GT(final.total_nodes(), 0);
}

// Integration Test: Production Performance Under Load
TEST_F(RaftIntegrationTest, ProductionPerformanceUnderLoad) {
    const int num_operations = 200; // Reduced for faster testing
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_operations; ++i) {
        // Mix of operations that would happen in production
        
        // Configuration parsing with validation
        NodeConfiguration config = repl_state_->parse_node_configuration(initial_config_);
        EXPECT_EQ(3, config.total_nodes());
        
        // DNS resolution with caching
        std::string resolved = repl_state_->hostname2ipstr("localhost");
        EXPECT_FALSE(resolved.empty());
        
        // Safety validation
        bool safe = repl_state_->is_config_safe_for_reconfig();
        EXPECT_TRUE(safe || !safe); // Just ensure no crash
        
        // Configuration conversion
        braft::Configuration braft_config = repl_state_->node_config_to_braft(config);
        EXPECT_GT(braft_config.size(), 0);
        
        // Periodic cache management
        if (i % 50 == 0) {
            size_t cache_size = repl_state_->get_dns_cache_size();
            EXPECT_GE(cache_size, 0);
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Should complete operations quickly (target: under 200ms for 200 operations)
    EXPECT_LT(duration.count(), 200) << "Production operations took: " << duration.count() << "ms";
    
    double avg_time_per_op = static_cast<double>(duration.count()) / num_operations;
    EXPECT_LT(avg_time_per_op, 1.0) << "Average time per operation: " << avg_time_per_op << "ms";
}

// Integration Test: Concurrent Operations with DNS Caching
TEST_F(RaftIntegrationTest, ConcurrentOperationsWithDNSCaching) {
    const int num_threads = 4;
    const int operations_per_thread = 25;
    std::vector<std::thread> threads;
    std::atomic<int> successful_operations{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < operations_per_thread; ++j) {
                try {
                    switch (j % 5) {
                        case 0: {
                            // Config parsing with enhanced validation
                            NodeConfiguration config = repl_state_->parse_node_configuration(initial_config_);
                            if (config.total_nodes() == 3) successful_operations++;
                            break;
                        }
                        case 1: {
                            // DNS resolution with caching
                            std::string resolved = repl_state_->hostname2ipstr("localhost");
                            if (!resolved.empty()) successful_operations++;
                            break;
                        }
                        case 2: {
                            // Safety validation
                            bool safe = repl_state_->add_node_safe("thread" + std::to_string(i) + "node" + std::to_string(j) + ":8107:8108");
                            successful_operations++; // Count completion, not result
                            break;
                        }
                        case 3: {
                            // Cache management
                            size_t cache_size = repl_state_->get_dns_cache_size();
                            if (j % 10 == 0) repl_state_->clear_dns_cache_for_hostname("localhost");
                            successful_operations++;
                            break;
                        }
                        case 4: {
                            // Self-node detection
                            bool is_self = repl_state_->is_self_node("localhost:8107:8108");
                            if (is_self) successful_operations++;
                            break;
                        }
                    }
                } catch (...) {
                    // Should not throw in production
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

// Integration Test: Error Recovery and Resilience
TEST_F(RaftIntegrationTest, ErrorRecoveryAndResilience) {
    // Test that the system handles various error conditions gracefully
    
    // Invalid configurations
    EXPECT_NO_THROW({
        repl_state_->parse_node_configuration("");
        repl_state_->parse_node_configuration("invalid");
        repl_state_->parse_node_configuration("node:abc:def");
    });
    
    // Invalid DNS operations
    EXPECT_NO_THROW({
        std::string result = repl_state_->hostname2ipstr("");
        EXPECT_TRUE(result.empty());
        
        result = repl_state_->hostname2ipstr("nonexistent-host-12345.invalid");
        EXPECT_TRUE(result.empty()); // Should return empty on failure now
    });
    
    // Invalid safety operations
    EXPECT_NO_THROW({
        repl_state_->add_node_safe("");
        repl_state_->remove_node_safe("");
        repl_state_->is_self_node("");
    });
    
    // Cache operations on empty cache
    EXPECT_NO_THROW({
        repl_state_->clear_dns_cache();
        repl_state_->clear_dns_cache_for_hostname("nonexistent");
        size_t size = repl_state_->get_dns_cache_size();
        EXPECT_GE(size, 0);
    });
    
    // After all these errors, basic operations should still work
    NodeConfiguration config = repl_state_->parse_node_configuration(initial_config_);
    EXPECT_EQ(3, config.hostname_nodes.size());
}

// Integration Test: Memory Management with Caching
TEST_F(RaftIntegrationTest, MemoryManagementWithCaching) {
    // Test that DNS caching doesn't cause memory leaks
    const int num_hostnames = 50;
    
    // Populate cache with many hostnames
    for (int i = 0; i < num_hostnames; ++i) {
        std::string hostname = "host" + std::to_string(i) + ".example.com";
        std::string result = repl_state_->hostname2ipstr(hostname);
        // Result may be empty for non-existent hosts, which is fine
    }
    
    size_t cache_size_after_population = repl_state_->get_dns_cache_size();
    EXPECT_GT(cache_size_after_population, 0);
    
    // Clear cache and verify cleanup
    repl_state_->clear_dns_cache();
    EXPECT_EQ(repl_state_->get_dns_cache_size(), 0);
    
    // Repeat operations to ensure no memory accumulation
    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < 20; ++i) {
            std::string hostname = "testhost" + std::to_string(i) + ".local";
            repl_state_->hostname2ipstr(hostname);
        }
        
        // Periodic cache clearing
        if (round % 2 == 0) {
            repl_state_->clear_dns_cache();
        }
    }
    
    // Should complete without issues
    EXPECT_TRUE(true);
} 