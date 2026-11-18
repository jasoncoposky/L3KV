#ifndef L3KV_ENGINE_CLOCK_HPP
#define L3KV_ENGINE_CLOCK_HPP

#include <algorithm>
#include <atomic>
#include <cstdint>


namespace l3kv {

struct Timestamp {
  int64_t wall_time; // Physical time (e.g., unix micros)
  uint32_t logical;  // Logical counter
  uint32_t node_id;  // Tie-breaker

  // Comparison operators for total ordering
  bool operator==(const Timestamp &o) const {
    return wall_time == o.wall_time && logical == o.logical &&
           node_id == o.node_id;
  }
  bool operator<(const Timestamp &o) const {
    if (wall_time != o.wall_time)
      return wall_time < o.wall_time;
    if (logical != o.logical)
      return logical < o.logical;
    return node_id < o.node_id;
  }
  bool operator>(const Timestamp &o) const { return o < *this; }
  bool operator<=(const Timestamp &o) const { return !(*this > o); }
  bool operator>=(const Timestamp &o) const { return !(*this < o); }
  bool operator!=(const Timestamp &o) const { return !(*this == o); }
};

class HybridLogicalClock {
  static constexpr uint32_t kLogicalBits = 12;
  static constexpr uint64_t kLogicalMask = (1ULL << kLogicalBits) - 1ULL;
  static constexpr uint32_t kMaxLogical = static_cast<uint32_t>(kLogicalMask);

  std::atomic<uint64_t> state_{0};
  uint32_t node_id_{0};
  std::atomic<int64_t> last_warn_phys_{0};

  static constexpr uint64_t pack(int64_t wall_time, uint32_t logical) noexcept {
    return (static_cast<uint64_t>(wall_time) << kLogicalBits) |
           (static_cast<uint64_t>(logical) & kLogicalMask);
  }
  static constexpr int64_t unpack_wall(uint64_t s) noexcept {
    return static_cast<int64_t>(s >> kLogicalBits);
  }
  static constexpr uint32_t unpack_logical(uint64_t s) noexcept {
    return static_cast<uint32_t>(s & kLogicalMask);
  }

public:
  explicit HybridLogicalClock(uint32_t node_id) : node_id_(node_id) {}

  // Get NOW (Send Event)
  Timestamp now();

  // Update with incoming timestamp (Receive Event)
  void update(const Timestamp &incoming);

  // For manual/test clock skew injection
  // Reserve a batch of logical ticks for a specific physical timestamp.
  // Returns the starting logical value for the batch.
  // If 'for_phys_time' is older than current max_wall_time_, it returns -1
  // (failure, retry). If successful, max_logical_ is incremented by 'count'.
  int64_t reserve_logical(int64_t for_phys_time, int count);

  // Reserve a batch of logical ticks at effective_wall = max(min_phys_time, phys_now, max_wall_time_).
  bool reserve_batch(int64_t min_phys_time, uint32_t count,
                     int64_t &out_wall_time, uint32_t &out_start_logical);

  uint32_t get_node_id() const { return node_id_; }
};

// Thread-local wrapper for high-throughput timestamp generation
class ThreadLocalClock {
  HybridLogicalClock *global_clock_;
  int64_t cached_phys_time_{0};
  uint32_t cached_logical_next_{0};
  uint32_t cached_logical_end_{0}; // Exclusive

public:
  explicit ThreadLocalClock(HybridLogicalClock *global)
      : global_clock_(global) {}

  Timestamp now();
  void update(const Timestamp &incoming) {
    global_clock_->update(incoming);
    cached_logical_next_ = 0;
    cached_logical_end_ = 0;
  }
};

} // namespace l3kv

#endif
