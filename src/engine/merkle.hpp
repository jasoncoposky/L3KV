#ifndef L3KV_ENGINE_MERKLE_HPP
#define L3KV_ENGINE_MERKLE_HPP

/*
 * MERKLE TREE IMPLEMENTATION - LAZY UPDATES
 *
 * Update Strategy:
 * - `apply_delta()` affects only the Leaf (L4) and marks the Leaf's parent (L3)
 * as dirty.
 * - This is an O(1) operation involving a simple XOR and bitset flag.
 * - Full tree recomputation is deferred until `get_root_hash()` is called.
 * - Only dirty branches are rehashed up to the root.
 * - This ensures high write throughput (microsecond latency) without paying the
 * Full Tree Hash cost on every write.
 */

#include <array>
#include <atomic>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace l3kv {

#include <xxhash.h>

// Ultra-fast SIMD AVX-accelerated hash (XXH3 64-bit)
// Replaces FNV-1a for extreme throughput
inline uint64_t fnv1a_64(const void *data, size_t len) {
  return XXH3_64bits(data, len);
}

inline uint64_t fnv1a_64(std::string_view s) {
  return XXH3_64bits(s.data(), s.size());
}

class MerkleTree {
private:
  static constexpr size_t L4_SIZE = 65536;
  static constexpr size_t L3_SIZE = 4096;
  static constexpr size_t SHARD_COUNT = 256;

  std::vector<std::atomic<uint64_t>> leaves_; // 65536
  std::vector<uint64_t> l3_;                  // 4096
  std::vector<std::atomic<uint8_t>> l3_dirty_;

  std::vector<uint64_t> l2_; // 256
  std::vector<uint8_t> l2_dirty_;
  std::vector<uint64_t> l1_; // 16
  std::vector<uint8_t> l1_dirty_;
  std::vector<uint64_t> l0_; // 1
  std::vector<uint8_t> l0_dirty_;

  mutable std::mutex global_mx_;

public:
  MerkleTree() : leaves_(L4_SIZE), l3_dirty_(L3_SIZE) {
    for (auto &leaf : leaves_) {
      leaf.store(0, std::memory_order_relaxed);
    }
    for (auto &d : l3_dirty_) {
      d.store(0, std::memory_order_relaxed);
    }
    l3_.resize(L3_SIZE, 0);
    l2_.resize(256, 0);
    l2_dirty_.resize(256, 0);
    l1_.resize(16, 0);
    l1_dirty_.resize(16, 0);
    l0_.resize(1, 0);
    l0_dirty_.resize(1, 0);

    // Initialize tree to steady state (recursive hashes of 0)
    std::vector<uint64_t> zeros(16, 0);
    uint64_t h_l3 = fnv1a_64(zeros.data(), 16 * sizeof(uint64_t));
    std::fill(l3_.begin(), l3_.end(), h_l3);

    std::vector<uint64_t> l3_block(16, h_l3);
    uint64_t h_l2 = fnv1a_64(l3_block.data(), 16 * sizeof(uint64_t));
    std::fill(l2_.begin(), l2_.end(), h_l2);

    std::vector<uint64_t> l2_block(16, h_l2);
    uint64_t h_l1 = fnv1a_64(l2_block.data(), 16 * sizeof(uint64_t));
    std::fill(l1_.begin(), l1_.end(), h_l1);

    std::vector<uint64_t> l1_block(16, h_l1);
    uint64_t h_l0 = fnv1a_64(l1_block.data(), 16 * sizeof(uint64_t));
    l0_[0] = h_l0;
  }

  void apply_delta(std::string_view key, uint64_t hash_delta) {
    uint64_t k_hash = fnv1a_64(key);
    uint32_t bucket_idx = (k_hash >> 48) & 0xFFFF; // 0..65535

    leaves_[bucket_idx].fetch_xor(hash_delta, std::memory_order_release);
    l3_dirty_[bucket_idx >> 4].store(1, std::memory_order_release);
  }

  uint64_t get_root_hash() {
    std::lock_guard lock(global_mx_);
    recompute_dirty();
    return l0_[0];
  }

  uint64_t get_node_hash(int level, size_t index) {
    // Note: recompute_dirty() must be called via get_root_hash() first
    // to ensure consistency if used by SyncManager.
    // SyncManager calls get_root_hash once, then get_node_hash multiple times.
    // This avoids redundant recomputations.
    if (level == 0)
      return l0_[0];
    if (level == 1)
      return l1_[index];
    if (level == 2)
      return l2_[index];
    if (level == 3)
      return l3_[index];
    if (level == 4)
      return leaves_[index].load(std::memory_order_acquire);
    return 0;
  }

private:
  void recompute_dirty() {
    // Phase 1: Lock-free L3 from atomic Leaves
    for (size_t curr_l3 = 0; curr_l3 < L3_SIZE; ++curr_l3) {
      if (l3_dirty_[curr_l3].exchange(0, std::memory_order_acq_rel) != 0) {
        uint64_t child_hashes[16];
        for (int k = 0; k < 16; ++k)
          child_hashes[k] =
              leaves_[curr_l3 * 16 + k].load(std::memory_order_acquire);
        l3_[curr_l3] = fnv1a_64(child_hashes, 16 * sizeof(uint64_t));
        l2_dirty_[curr_l3 >> 4] = 1;
      }
    }

    // Phase 2-4: Serial processing for higher levels (protected by global_mx_)
    for (size_t i = 0; i < 256; ++i) {
      if (l2_dirty_[i]) {
        l2_[i] = fnv1a_64(&l3_[i * 16], 16 * sizeof(uint64_t));
        l2_dirty_[i] = 0;
        l1_dirty_[i >> 4] = 1;
      }
    }

    for (size_t i = 0; i < 16; ++i) {
      if (l1_dirty_[i]) {
        l1_[i] = fnv1a_64(&l2_[i * 16], 16 * sizeof(uint64_t));
        l1_dirty_[i] = 0;
        l0_dirty_[0] = 1;
      }
    }

    if (l0_dirty_[0]) {
      l0_[0] = fnv1a_64(&l1_[0], 16 * sizeof(uint64_t));
      l0_dirty_[0] = 0;
    }
  }
};

} // namespace l3kv

#endif
