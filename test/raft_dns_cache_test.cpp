#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <vector>
#include <future>
#include "raft_dns_cache.h"

class RaftDNSCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use short TTL for faster testing
        cache_ = std::make_unique<RaftDNSCache>(std::chrono::minutes(1), 10);
    }
    
    void TearDown() override {
        cache_.reset();
    }
    
    std::unique_ptr<RaftDNSCache> cache_;
};

// Basic functionality tests
TEST_F(RaftDNSCacheTest, BasicPutAndGet) {
    EXPECT_TRUE(cache_->empty());
    EXPECT_EQ(cache_->size(), 0);
    
    // Put and get
    cache_->put("example.com", "192.168.1.1");
    EXPECT_EQ(cache_->size(), 1);
    EXPECT_FALSE(cache_->empty());
    
    auto result = cache_->get("example.com");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "192.168.1.1");
}

TEST_F(RaftDNSCacheTest, CacheMiss) {
    auto result = cache_->get("nonexistent.com");
    EXPECT_FALSE(result.has_value());
    
    // Stats should reflect miss
    auto stats = cache_->get_stats();
    EXPECT_EQ(stats.misses, 1);
    EXPECT_EQ(stats.hits, 0);
}

TEST_F(RaftDNSCacheTest, CacheHitStats) {
    cache_->put("test.com", "10.0.0.1");
    
    // Multiple hits
    for (int i = 0; i < 5; i++) {
        auto result = cache_->get("test.com");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, "10.0.0.1");
    }
    
    auto stats = cache_->get_stats();
    EXPECT_EQ(stats.hits, 5);
    EXPECT_EQ(stats.misses, 0);
    EXPECT_GT(stats.hit_rate(), 0.99); // Should be 100%
}

TEST_F(RaftDNSCacheTest, EmptyHostnameHandling) {
    cache_->put("", "192.168.1.1");
    EXPECT_EQ(cache_->size(), 0); // Should not cache empty hostname
    
    auto result = cache_->get("");
    EXPECT_FALSE(result.has_value());
}

TEST_F(RaftDNSCacheTest, EmptyIPHandling) {
    cache_->put("example.com", "");
    EXPECT_EQ(cache_->size(), 0); // Should not cache empty IP
}

// Cache management tests
TEST_F(RaftDNSCacheTest, ClearCache) {
    cache_->put("host1.com", "1.1.1.1");
    cache_->put("host2.com", "2.2.2.2");
    EXPECT_EQ(cache_->size(), 2);
    
    cache_->clear();
    EXPECT_EQ(cache_->size(), 0);
    EXPECT_TRUE(cache_->empty());
}

TEST_F(RaftDNSCacheTest, ClearSpecificHostname) {
    cache_->put("host1.com", "1.1.1.1");
    cache_->put("host2.com", "2.2.2.2");
    EXPECT_EQ(cache_->size(), 2);
    
    cache_->clear_hostname("host1.com");
    EXPECT_EQ(cache_->size(), 1);
    
    auto result = cache_->get("host1.com");
    EXPECT_FALSE(result.has_value());
    
    result = cache_->get("host2.com");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "2.2.2.2");
}

TEST_F(RaftDNSCacheTest, GetCachedHostnames) {
    cache_->put("alpha.com", "1.1.1.1");
    cache_->put("beta.com", "2.2.2.2");
    cache_->put("gamma.com", "3.3.3.3");
    
    auto hostnames = cache_->get_cached_hostnames();
    EXPECT_EQ(hostnames.size(), 3);
    
    std::sort(hostnames.begin(), hostnames.end());
    EXPECT_EQ(hostnames[0], "alpha.com");
    EXPECT_EQ(hostnames[1], "beta.com");
    EXPECT_EQ(hostnames[2], "gamma.com");
}

// Configuration tests
TEST_F(RaftDNSCacheTest, TTLConfiguration) {
    EXPECT_EQ(cache_->get_ttl(), std::chrono::minutes(1));
    
    cache_->set_ttl(std::chrono::minutes(10));
    EXPECT_EQ(cache_->get_ttl(), std::chrono::minutes(10));
}

TEST_F(RaftDNSCacheTest, MaxSizeConfiguration) {
    EXPECT_EQ(cache_->get_max_size(), 10);
    
    cache_->set_max_size(5);
    EXPECT_EQ(cache_->get_max_size(), 5);
    
    // Should evict entries if over new limit
    for (int i = 0; i < 8; i++) {
        cache_->put("host" + std::to_string(i) + ".com", "192.168.1." + std::to_string(i));
    }
    
    EXPECT_LE(cache_->size(), 5);
}

// LRU eviction tests
TEST_F(RaftDNSCacheTest, LRUEviction) {
    // Fill cache to capacity
    for (int i = 0; i < 10; i++) {
        cache_->put("host" + std::to_string(i) + ".com", "192.168.1." + std::to_string(i));
    }
    EXPECT_EQ(cache_->size(), 10);
    
    // Access some entries to increase their hit count
    cache_->get("host5.com");
    cache_->get("host5.com");
    cache_->get("host7.com");
    
    // Add one more entry to trigger eviction
    cache_->put("new-host.com", "10.0.0.1");
    
    EXPECT_EQ(cache_->size(), 10); // Should still be at max
    
    // Most accessed entries should still be there
    auto result5 = cache_->get("host5.com");
    auto result7 = cache_->get("host7.com");
    auto result_new = cache_->get("new-host.com");
    
    EXPECT_TRUE(result5.has_value());
    EXPECT_TRUE(result7.has_value());
    EXPECT_TRUE(result_new.has_value());
    
    // Check eviction stats
    auto stats = cache_->get_stats();
    EXPECT_GT(stats.evictions, 0);
}

// TTL and expiration tests
TEST_F(RaftDNSCacheTest, TTLExpiration) {
    // Use very short TTL for this test
    auto short_cache = std::make_unique<RaftDNSCache>(std::chrono::milliseconds(100), 10);
    
    short_cache->put("temp.com", "192.168.1.1");
    
    // Should be available immediately
    auto result1 = short_cache->get("temp.com");
    EXPECT_TRUE(result1.has_value());
    
    // Wait for expiration
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    
    // Should be expired now
    auto result2 = short_cache->get("temp.com");
    EXPECT_FALSE(result2.has_value());
    
    // Size should be reduced after expired entry is removed
    EXPECT_EQ(short_cache->size(), 0);
}

TEST_F(RaftDNSCacheTest, ExpiredHostnamesDetection) {
    auto short_cache = std::make_unique<RaftDNSCache>(std::chrono::milliseconds(100), 10);
    
    short_cache->put("temp1.com", "1.1.1.1");
    short_cache->put("temp2.com", "2.2.2.2");
    short_cache->put("permanent.com", "3.3.3.3");
    
    // Wait for some to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    
    // Add a new entry (should not be expired)
    short_cache->put("fresh.com", "4.4.4.4");
    
    auto expired = short_cache->get_expired_hostnames();
    
    // Should have at least temp1.com and temp2.com as expired
    EXPECT_GE(expired.size(), 2);
    
    // Fresh entry should not be in expired list
    auto fresh_in_expired = std::find(expired.begin(), expired.end(), "fresh.com");
    EXPECT_EQ(fresh_in_expired, expired.end());
}

TEST_F(RaftDNSCacheTest, ManualExpiredCleanup) {
    auto short_cache = std::make_unique<RaftDNSCache>(std::chrono::milliseconds(100), 10);
    
    short_cache->put("temp1.com", "1.1.1.1");
    short_cache->put("temp2.com", "2.2.2.2");
    short_cache->put("temp3.com", "3.3.3.3");
    
    EXPECT_EQ(short_cache->size(), 3);
    
    // Wait for expiration
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    
    // Manual cleanup
    size_t cleaned = short_cache->cleanup_expired();
    EXPECT_EQ(cleaned, 3);
    EXPECT_EQ(short_cache->size(), 0);
    
    // Stats should reflect cleanup
    auto stats = short_cache->get_stats();
    EXPECT_EQ(stats.expired_cleanups, 3);
}

// Thread safety tests
TEST_F(RaftDNSCacheTest, ConcurrentAccess) {
    const int num_threads = 10;
    const int operations_per_thread = 100;
    
    std::vector<std::future<void>> futures;
    
    // Launch multiple threads doing concurrent operations
    for (int t = 0; t < num_threads; t++) {
        futures.push_back(std::async(std::launch::async, [this, t, operations_per_thread]() {
            for (int i = 0; i < operations_per_thread; i++) {
                std::string hostname = "host" + std::to_string(t) + "-" + std::to_string(i) + ".com";
                std::string ip = "192.168." + std::to_string(t) + "." + std::to_string(i);
                
                // Put
                cache_->put(hostname, ip);
                
                // Get
                auto result = cache_->get(hostname);
                if (result.has_value()) {
                    EXPECT_EQ(*result, ip);
                }
                
                // Occasional clear operations
                if (i % 50 == 0) {
                    cache_->clear_hostname(hostname);
                }
            }
        }));
    }
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
    
    // Cache should be in a consistent state
    EXPECT_LE(cache_->size(), cache_->get_max_size());
    
    auto stats = cache_->get_stats();
    EXPECT_GT(stats.hits + stats.misses, 0);
}

TEST_F(RaftDNSCacheTest, ConcurrentStatsAccess) {
    const int num_threads = 5;
    std::atomic<bool> stop_flag{false};
    
    std::vector<std::future<void>> futures;
    
    // Thread that continuously adds/removes entries
    futures.push_back(std::async(std::launch::async, [this, &stop_flag]() {
        int counter = 0;
        while (!stop_flag) {
            cache_->put("dynamic" + std::to_string(counter) + ".com", "1.2.3.4");
            if (counter % 10 == 0) {
                cache_->clear();
            }
            counter++;
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }));
    
    // Threads that continuously read stats
    for (int t = 0; t < num_threads; t++) {
        futures.push_back(std::async(std::launch::async, [this, &stop_flag]() {
            while (!stop_flag) {
                auto stats = cache_->get_stats();
                auto size = cache_->size();
                auto hostnames = cache_->get_cached_hostnames();
                
                // Basic consistency checks
                EXPECT_GE(stats.hits + stats.misses, 0);
                EXPECT_LE(size, cache_->get_max_size());
                EXPECT_EQ(hostnames.size(), size);
                
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        }));
    }
    
    // Let threads run for a short time
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    stop_flag = true;
    
    // Wait for all threads
    for (auto& future : futures) {
        future.wait();
    }
}

// Statistics tests
TEST_F(RaftDNSCacheTest, StatisticsAccuracy) {
    cache_->reset_stats();
    
    // Perform known operations
    cache_->put("test1.com", "1.1.1.1");
    cache_->put("test2.com", "2.2.2.2");
    
    // 3 hits
    cache_->get("test1.com");
    cache_->get("test1.com");
    cache_->get("test2.com");
    
    // 2 misses
    cache_->get("nonexistent1.com");
    cache_->get("nonexistent2.com");
    
    auto stats = cache_->get_stats();
    EXPECT_EQ(stats.hits, 3);
    EXPECT_EQ(stats.misses, 2);
    EXPECT_NEAR(stats.hit_rate(), 0.6, 0.01); // 3/(3+2) = 0.6
    
    // Reset and verify
    cache_->reset_stats();
    stats = cache_->get_stats();
    EXPECT_EQ(stats.hits, 0);
    EXPECT_EQ(stats.misses, 0);
    EXPECT_EQ(stats.evictions, 0);
    EXPECT_EQ(stats.expired_cleanups, 0);
}

// Edge cases and error handling
TEST_F(RaftDNSCacheTest, UpdateExistingEntry) {
    cache_->put("example.com", "1.1.1.1");
    auto result1 = cache_->get("example.com");
    ASSERT_TRUE(result1.has_value());
    EXPECT_EQ(*result1, "1.1.1.1");
    
    // Update with new IP
    cache_->put("example.com", "2.2.2.2");
    auto result2 = cache_->get("example.com");
    ASSERT_TRUE(result2.has_value());
    EXPECT_EQ(*result2, "2.2.2.2");
    
    // Should still be only one entry
    EXPECT_EQ(cache_->size(), 1);
}

TEST_F(RaftDNSCacheTest, ZeroMaxSize) {
    auto zero_cache = std::make_unique<RaftDNSCache>(std::chrono::minutes(5), 0);
    
    zero_cache->put("test.com", "1.1.1.1");
    EXPECT_EQ(zero_cache->size(), 0); // Should not store anything
    
    auto result = zero_cache->get("test.com");
    EXPECT_FALSE(result.has_value());
}

TEST_F(RaftDNSCacheTest, VeryShortTTL) {
    auto micro_cache = std::make_unique<RaftDNSCache>(std::chrono::microseconds(1), 10);
    
    micro_cache->put("test.com", "1.1.1.1");
    
    // Even immediate access might miss due to very short TTL
    std::this_thread::sleep_for(std::chrono::microseconds(10));
    auto result = micro_cache->get("test.com");
    
    // The entry should be expired and removed
    EXPECT_FALSE(result.has_value());
} 