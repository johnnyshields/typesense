#include "raft_dns_cache.h"
#include "logger.h"
#include <algorithm>
#include <iterator>

RaftDNSCache::RaftDNSCache(std::chrono::minutes ttl, size_t max_size)
    : ttl_(ttl), max_size_(max_size) {
    LOG(INFO) << "DNS cache initialized with TTL=" << ttl_.count() 
              << " minutes, max_size=" << max_size_;
}

bool RaftDNSCache::is_expired(const CacheEntry& entry) const {
    auto age = std::chrono::steady_clock::now() - entry.timestamp;
    return age >= ttl_;
}

void RaftDNSCache::evict_lru_if_needed() {
    // This method assumes we already have a unique lock
    if (cache_.size() < max_size_) {
        return;
    }
    
    // Find LRU entry (lowest hit_count, oldest timestamp as tiebreaker)
    auto lru_it = std::min_element(cache_.begin(), cache_.end(),
        [](const auto& a, const auto& b) {
            if (a.second.hit_count != b.second.hit_count) {
                return a.second.hit_count < b.second.hit_count;
            }
            return a.second.timestamp < b.second.timestamp;
        });
    
    if (lru_it != cache_.end()) {
        LOG(DEBUG) << "Evicting LRU cache entry: " << lru_it->first 
                   << " (hits: " << lru_it->second.hit_count << ")";
        cache_.erase(lru_it);
        ++stats_.evictions;
    }
}

void RaftDNSCache::cleanup_expired_internal() {
    // This method assumes we already have a unique lock
    auto now = std::chrono::steady_clock::now();
    size_t removed = 0;
    
    for (auto it = cache_.begin(); it != cache_.end();) {
        if (is_expired(it->second)) {
            LOG(DEBUG) << "Removing expired cache entry: " << it->first;
            it = cache_.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    
    if (removed > 0) {
        stats_.expired_cleanups += removed;
        LOG(DEBUG) << "Cleaned up " << removed << " expired cache entries";
    }
}

std::optional<std::string> RaftDNSCache::get(const std::string& hostname) {
    if (hostname.empty()) {
        return std::nullopt;
    }
    
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    
    auto it = cache_.find(hostname);
    if (it == cache_.end()) {
        ++stats_.misses;
        return std::nullopt;
    }
    
    // Check if expired
    if (is_expired(it->second)) {
        LOG(DEBUG) << "Cache entry expired for: " << hostname;
        cache_.erase(it);
        ++stats_.misses;
        ++stats_.expired_cleanups;
        return std::nullopt;
    }
    
    // Update hit count and stats
    ++it->second.hit_count;
    ++stats_.hits;
    
    LOG(DEBUG) << "Cache hit for " << hostname << " -> " << it->second.ip 
               << " (hits: " << it->second.hit_count << ")";
    
    return it->second.ip;
}

void RaftDNSCache::put(const std::string& hostname, const std::string& ip) {
    if (hostname.empty() || ip.empty()) {
        LOG(WARNING) << "Attempted to cache empty hostname or IP";
        return;
    }
    
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    
    // Clean up expired entries periodically (every 100 puts)
    static thread_local size_t put_counter = 0;
    if (++put_counter % 100 == 0) {
        cleanup_expired_internal();
    }
    
    // Evict LRU if at capacity
    evict_lru_if_needed();
    
    // Insert or update entry
    cache_[hostname] = CacheEntry(ip);
    
    LOG(DEBUG) << "Cached DNS resolution: " << hostname << " -> " << ip 
               << " (cache size: " << cache_.size() << ")";
}

void RaftDNSCache::clear() {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    size_t cleared = cache_.size();
    cache_.clear();
    
    LOG(INFO) << "Cleared DNS cache (" << cleared << " entries)";
}

void RaftDNSCache::clear_hostname(const std::string& hostname) {
    if (hostname.empty()) {
        return;
    }
    
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    auto it = cache_.find(hostname);
    if (it != cache_.end()) {
        cache_.erase(it);
        LOG(INFO) << "Cleared DNS cache entry for: " << hostname;
    }
}

size_t RaftDNSCache::size() const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    return cache_.size();
}

bool RaftDNSCache::empty() const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    return cache_.empty();
}

void RaftDNSCache::set_ttl(std::chrono::minutes ttl) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    ttl_ = ttl;
    LOG(INFO) << "Updated DNS cache TTL to " << ttl_.count() << " minutes";
}

void RaftDNSCache::set_max_size(size_t max_size) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    max_size_ = max_size;
    
    // Evict entries if current size exceeds new limit
    while (cache_.size() > max_size_) {
        evict_lru_if_needed();
    }
    
    LOG(INFO) << "Updated DNS cache max size to " << max_size_;
}

std::chrono::minutes RaftDNSCache::get_ttl() const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    return ttl_;
}

size_t RaftDNSCache::get_max_size() const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    return max_size_;
}

std::vector<std::string> RaftDNSCache::get_expired_hostnames() const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    std::vector<std::string> expired;
    
    for (const auto& [hostname, entry] : cache_) {
        if (is_expired(entry)) {
            expired.push_back(hostname);
        }
    }
    
    return expired;
}

size_t RaftDNSCache::cleanup_expired() {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    size_t initial_size = cache_.size();
    cleanup_expired_internal();
    return initial_size - cache_.size();
}

RaftDNSCache::Stats RaftDNSCache::get_stats() const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    return stats_;
}

void RaftDNSCache::reset_stats() {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    stats_ = Stats{};
    LOG(INFO) << "Reset DNS cache statistics";
}

std::vector<std::string> RaftDNSCache::get_cached_hostnames() const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    std::vector<std::string> hostnames;
    hostnames.reserve(cache_.size());
    
    for (const auto& [hostname, entry] : cache_) {
        hostnames.push_back(hostname);
    }
    
    return hostnames;
} 