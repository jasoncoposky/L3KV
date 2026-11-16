#include "clock.hpp"
#include <algorithm> // for std::max
#include <chrono>
#include <iostream>
#include <thread>

namespace l3kv {

// Helper to get physical time (micros)
static int64_t get_physical_time() {
  using namespace std::chrono;
  return duration_cast<microseconds>(system_clock::now().time_since_epoch())
      .count();
}

Timestamp HybridLogicalClock::now() {
  std::lock_guard<std::mutex> lock(mx_);
  auto phys_now = get_physical_time();

  if (phys_now > max_wall_time_) {
    max_wall_time_ = phys_now;
    max_logical_ = 0;
  } else {
    // Clock hasn't moved forward, or we are being called faster than 1us

    // Safety: Check for backwards clock jumps (skew)
    int64_t diff = max_wall_time_ - phys_now;
    if (diff > 5000000) { // 5 seconds
      // Only log periodically to avoid spamming
      if (last_warn_phys_ == 0 || phys_now - last_warn_phys_ > 5000000) {
        std::cerr << "[HLC] WARNING: Physical clock lagging logical clock by "
                  << (diff / 1000) << "ms. (System clock moved backwards?)\n";
        last_warn_phys_ = phys_now;
      }
    }

    // Safety: Overflow protection
    if (max_logical_ == UINT32_MAX) {
      // Critical Error: We exhausted 4 billion logical ticks in 1 microsecond?
      // Or (more likely) the physical clock is stuck/backwards for a LONG time.
      // We MUST wait for physical time to advance to preserve ordering.
      std::cerr << "[HLC] CRITICAL: Logical counter overflow. Blocking until "
                   "physical time advances.\n";
      while (get_physical_time() <= max_wall_time_) {
        std::this_thread::yield();
      }
      auto new_phys = get_physical_time();
      max_wall_time_ = new_phys;
      max_logical_ = 0;
    } else {
      max_logical_++;
    }
  }

  return {max_wall_time_, max_logical_, node_id_};
}

void HybridLogicalClock::update(const Timestamp &incoming) {
  std::lock_guard<std::mutex> lock(mx_);
  auto phys_now = get_physical_time();

  int64_t l_old = max_wall_time_;
  uint32_t c_old = max_logical_;

  int64_t l_msg = incoming.wall_time;
  uint32_t c_msg = incoming.logical;

  // Update local HLC to be max(local, message, physical)
  max_wall_time_ = std::max({l_old, l_msg, phys_now});

  if (max_wall_time_ == l_old && max_wall_time_ == l_msg) {
    max_logical_ = std::max(c_old, c_msg) + 1;
  } else if (max_wall_time_ == l_old) {
    max_logical_ = c_old + 1;
  } else if (max_wall_time_ == l_msg) {
    max_logical_ = c_msg + 1;
  } else {
    max_logical_ = 0;
  }
}

int64_t HybridLogicalClock::reserve_logical(int64_t for_phys_time, int count) {
  std::lock_guard<std::mutex> lock(mx_);
  int64_t phys_now = std::max(get_physical_time(), max_wall_time_);

  if (for_phys_time < phys_now) {
    return -1;
  }

  // If for_phys_time > max_wall_time_, we advance.
  if (for_phys_time > max_wall_time_) {
    max_wall_time_ = for_phys_time;
    max_logical_ = 0;
  }

  // Overflow check for batch reservation
  if (UINT32_MAX - max_logical_ < (uint32_t)count) {
    return -1; // Cannot reserve
  }

  int64_t start_logical = max_logical_ + 1;
  max_logical_ += count;
  return start_logical;
}

bool HybridLogicalClock::reserve_batch(int64_t min_phys_time, uint32_t count,
                                       int64_t &out_wall_time,
                                       uint32_t &out_start_logical) {
  if (count == 0) {
    return false;
  }
  std::lock_guard<std::mutex> lock(mx_);
  int64_t effective_wall =
      std::max({min_phys_time, get_physical_time(), max_wall_time_});

  if (effective_wall > max_wall_time_) {
    max_wall_time_ = effective_wall;
    max_logical_ = 0;
  }

  if (UINT32_MAX - max_logical_ < count) {
    return false;
  }

  out_wall_time = max_wall_time_;
  out_start_logical = max_logical_ + 1;
  max_logical_ += count;
  return true;
}

Timestamp ThreadLocalClock::now() {
  int64_t phys_now = get_physical_time();

  // 1. Serve from cached batch whenever physical time has not surpassed cached_phys_time_
  //    (covers both same-microsecond bursts and skewed max_wall_time_ > phys_now)
  if (phys_now <= cached_phys_time_) {
    if (cached_logical_next_ < cached_logical_end_) {
      return {cached_phys_time_, cached_logical_next_++,
              global_clock_->get_node_id()};
    }
  } else {
    cached_phys_time_ = phys_now;
    cached_logical_next_ = 0;
    cached_logical_end_ = 0;
  }

  // 2. Refill batch at effective_wall = max(phys_now, max_wall_time_) without spinning
  constexpr uint32_t BATCH_SIZE = 50;
  int64_t batch_wall = 0;
  uint32_t start_logical = 0;
  if (global_clock_->reserve_batch(phys_now, BATCH_SIZE, batch_wall,
                                   start_logical)) {
    cached_phys_time_ = batch_wall;
    cached_logical_next_ = start_logical;
    cached_logical_end_ = start_logical + BATCH_SIZE;
    return {cached_phys_time_, cached_logical_next_++,
            global_clock_->get_node_id()};
  }

  cached_logical_next_ = 0;
  cached_logical_end_ = 0;
  return global_clock_->now();
}

} // namespace l3kv
