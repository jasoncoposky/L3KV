#ifndef L3KV_ENGINE_REPLICATION_LOG_HPP
#define L3KV_ENGINE_REPLICATION_LOG_HPP

#include "clock.hpp" // For Timestamp
#include <algorithm>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

namespace l3kv {

// Represents a single change to the KV store that must be replicated
struct Mutation {
  Timestamp timestamp;
  std::string key;
  std::vector<uint8_t> value; // Empty = Delete (Tombstone)
  bool is_delete{false};
  // Future: Lane hint? (Small vs Large)
};

class ReplicationLog {
  mutable std::mutex mx_;
  std::deque<Mutation> queue_;
  size_t max_size_{10000}; // Cap to prevent memory explosion if net is down

public:
  explicit ReplicationLog(size_t max_size = 10000) : max_size_(max_size) {}

  void append(Mutation m) {
    std::lock_guard<std::mutex> lock(mx_);
    if (queue_.size() >= max_size_) {
      // Drop oldest? Or block? Or reject?
      // L3KV approach: Drop oldest is risky for consistency, but preventing
      // memory crash is better. Real sys would spill to disk.
      // For now: Drop oldest and warn (metrics).
      queue_.pop_front();
    }
    queue_.push_back(std::move(m));
  }

  std::vector<Mutation> pop_batch(size_t limit) {
    std::lock_guard<std::mutex> lock(mx_);
    std::vector<Mutation> batch;
    batch.reserve(std::min(limit, queue_.size()));

    while (!queue_.empty() && batch.size() < limit) {
      batch.push_back(std::move(queue_.front()));
      queue_.pop_front();
    }
    return batch;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(mx_);
    return queue_.size();
  }

  bool empty() const {
    std::lock_guard<std::mutex> lock(mx_);
    return queue_.empty();
  }
};

} // namespace l3kv

#endif
