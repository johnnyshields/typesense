#pragma once

#include <string>
#include <unordered_map>
#include <optional>
#include <chrono>
#include <shared_mutex>
#include <vector>
#include <cstdint>

/**
 * Thread-safe DNS cache for Raft operations with TTL and size management.
 * Provides caching for hostname-to-IP resolution with configurable eviction policies.
 */
class RaftDNSCache {
public:
    struct CacheEntry {
        std::string ip;
        std::chrono::steady_clock::time_point timestamp;
        uint32_t hit_count = 0;
        
        CacheEntry() = default;
        CacheEntry(const std::string& ip_addr) 
            : ip(ip_addr), timestamp(std::chrono::steady_clock::now()), hit_count(0) {}
    };
    
    struct Stats {
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t evictions = 0;
        uint64_t expired_cleanups = 0;
        
        double hit_rate() const {
            uint64_t total = hits + misses;
            return total > 0 ? static_cast<double>(hits) / total : 0.0;
        }
    };

private:
    mutable std::shared_mutex cache_mutex_;
    std::unordered_map<std::string, CacheEntry> cache_;
    std::chrono::minutes ttl_;
    size_t max_size_;
    mutable Stats stats_;

    // Internal helper methods
    bool is_expired(const CacheEntry& entry) const;
    void evict_lru_if_needed();
    void cleanup_expired_internal();

public:
    /**
     * Construct DNS cache with configurable TTL and max size
     * @param ttl Time-to-live for cache entries (default: 5 minutes)
     * @param max_size Maximum number of entries (default: 1000)
     */
    explicit RaftDNSCache(std::chrono::minutes ttl = std::chrono::minutes(5), 
                          size_t max_size = 1000);

    // Core cache operations
    
    /**
     * Get cached IP for hostname if available and not expired
     * @param hostname The hostname to look up
     * @return IP address if cached and valid, nullopt otherwise
     */
    std::optional<std::string> get(const std::string& hostname);
    
    /**
     * Cache hostname-to-IP mapping
     * @param hostname The hostname to cache
     * @param ip The resolved IP address
     */
    void put(const std::string& hostname, const std::string& ip);
    
    // Cache management
    
    /**
     * Clear all cache entries
     */
    void clear();
    
    /**
     * Clear cache entry for specific hostname
     * @param hostname The hostname to remove from cache
     */
    void clear_hostname(const std::string& hostname);
    
    /**
     * Get current cache size
     * @return Number of entries in cache
     */
    size_t size() const;
    
    /**
     * Check if cache is empty
     * @return true if cache has no entries
     */
    bool empty() const;
    
    // Configuration
    
    /**
     * Update cache TTL
     * @param ttl New time-to-live duration
     */
    void set_ttl(std::chrono::minutes ttl);
    
    /**
     * Update maximum cache size
     * @param max_size New maximum number of entries
     */
    void set_max_size(size_t max_size);
    
    /**
     * Get current TTL setting
     * @return Current time-to-live duration
     */
    std::chrono::minutes get_ttl() const;
    
    /**
     * Get current max size setting
     * @return Current maximum cache size
     */
    size_t get_max_size() const;
    
    // Maintenance and monitoring
    
    /**
     * Get list of hostnames with expired entries
     * @return Vector of expired hostnames
     */
    std::vector<std::string> get_expired_hostnames() const;
    
    /**
     * Remove all expired entries from cache
     * @return Number of entries removed
     */
    size_t cleanup_expired();
    
    /**
     * Get cache statistics
     * @return Current cache performance stats
     */
    Stats get_stats() const;
    
    /**
     * Reset cache statistics
     */
    void reset_stats();
    
    /**
     * Get all cached hostnames (for debugging/monitoring)
     * @return Vector of all hostnames in cache
     */
    std::vector<std::string> get_cached_hostnames() const;
    
    // Disable copy/move for thread safety
    RaftDNSCache(const RaftDNSCache&) = delete;
    RaftDNSCache& operator=(const RaftDNSCache&) = delete;
    RaftDNSCache(RaftDNSCache&&) = delete;
    RaftDNSCache& operator=(RaftDNSCache&&) = delete;
}; 