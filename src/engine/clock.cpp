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
  int64_t phys_now = get_physical_time();
  uint64_t expected = state_.load(std::memory_order_relaxed);
  for (;;) {
    int64_t old_wall = unpack_wall(expected);
    uint32_t old_logical = unpack_logical(expected);
    int64_t next_wall = old_wall;
    uint32_t next_logical = 0;

    if (phys_now > old_wall) {
      next_wall = phys_now;
      next_logical = 0;
    } else {
      int64_t diff = old_wall - phys_now;
      if (diff > 5000000) {
        int64_t last_w = last_warn_phys_.load(std::memory_order_relaxed);
        if ((last_w == 0 || phys_now - last_w > 5000000) &&
            last_warn_phys_.compare_exchange_weak(last_w, phys_now,
                                                  std::memory_order_relaxed)) {
          std::cerr << "[HLC] WARNING: Physical clock lagging logical clock by "
                    << (diff / 1000) << "ms. (System clock moved backwards?)\n";
        }
      }
      if (old_logical >= kMaxLogical) {
        next_wall = old_wall + 1;
        next_logical = 0;
      } else {
        next_logical = old_logical + 1;
      }
    }

    uint64_t desired = pack(next_wall, next_logical);
    if (state_.compare_exchange_weak(expected, desired,
                                     std::memory_order_acq_rel,
                                     std::memory_order_relaxed)) {
      return {next_wall, next_logical, node_id_};
    }
  }
}

void HybridLogicalClock::update(const Timestamp &incoming) {
  int64_t phys_now = get_physical_time();
  int64_t l_msg = incoming.wall_time;
  uint32_t c_msg = incoming.logical;

  uint64_t expected = state_.load(std::memory_order_relaxed);
  for (;;) {
    int64_t l_old = unpack_wall(expected);
    uint32_t c_old = unpack_logical(expected);

    int64_t next_wall = std::max({l_old, l_msg, phys_now});
    uint32_t next_logical = 0;

    if (next_wall == l_old && next_wall == l_msg) {
      uint32_t base = std::max(c_old, c_msg);
      if (base >= kMaxLogical) {
        next_wall += 1;
        next_logical = 0;
      } else {
        next_logical = base + 1;
      }
    } else if (next_wall == l_old) {
      if (c_old >= kMaxLogical) {
        next_wall += 1;
        next_logical = 0;
      } else {
        next_logical = c_old + 1;
      }
    } else if (next_wall == l_msg) {
      if (c_msg >= kMaxLogical) {
        next_wall += 1;
        next_logical = 0;
      } else {
        next_logical = c_msg + 1;
      }
    }

    uint64_t desired = pack(next_wall, next_logical);
    if (state_.compare_exchange_weak(expected, desired,
                                     std::memory_order_acq_rel,
                                     std::memory_order_relaxed)) {
      return;
    }
  }
}

int64_t HybridLogicalClock::reserve_logical(int64_t for_phys_time, int count) {
  if (count <= 0 || static_cast<uint32_t>(count) > kMaxLogical) {
    return -1;
  }
  uint64_t expected = state_.load(std::memory_order_relaxed);
  for (;;) {
    int64_t old_wall = unpack_wall(expected);
    uint32_t old_logical = unpack_logical(expected);
    int64_t phys_now = std::max(get_physical_time(), old_wall);

    if (for_phys_time < phys_now) {
      return -1;
    }

    int64_t next_wall = old_wall;
    uint32_t base_logical = old_logical;
    if (for_phys_time > old_wall) {
      next_wall = for_phys_time;
      base_logical = 0;
    }

    if (kMaxLogical - base_logical < static_cast<uint32_t>(count)) {
      return -1;
    }

    int64_t start_logical = static_cast<int64_t>(base_logical) + 1;
    uint32_t next_logical = base_logical + static_cast<uint32_t>(count);
    uint64_t desired = pack(next_wall, next_logical);
    if (state_.compare_exchange_weak(expected, desired,
                                     std::memory_order_acq_rel,
                                     std::memory_order_relaxed)) {
      return start_logical;
    }
  }
}

bool HybridLogicalClock::reserve_batch(int64_t min_phys_time, uint32_t count,
                                       int64_t &out_wall_time,
                                       uint32_t &out_start_logical) {
  if (count == 0 || count > kMaxLogical) {
    return false;
  }
  uint64_t expected = state_.load(std::memory_order_relaxed);
  for (;;) {
    int64_t old_wall = unpack_wall(expected);
    uint32_t old_logical = unpack_logical(expected);
    int64_t effective_wall =
        std::max({min_phys_time, get_physical_time(), old_wall});

    uint32_t base_logical = (effective_wall > old_wall) ? 0 : old_logical;
    if (kMaxLogical - base_logical < count) {
      effective_wall += 1;
      base_logical = 0;
    }

    uint32_t start_logical = base_logical + 1;
    uint32_t next_logical = base_logical + count;
    uint64_t desired = pack(effective_wall, next_logical);
    if (state_.compare_exchange_weak(expected, desired,
                                     std::memory_order_acq_rel,
                                     std::memory_order_relaxed)) {
      out_wall_time = effective_wall;
      out_start_logical = start_logical;
      return true;
    }
  }
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
