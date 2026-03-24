#pragma once
#include "clock.hpp"
#include "merkle.hpp"
#include "replication_log.hpp"
#include "wal.hpp"
#include "KeyBuilder.hpp"

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <span>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include "../../lib/concurrentqueue/concurrentqueue.h"

#include "buffer.hpp"
#include "json.hpp"

#include <atomic>
#include <mutex>
#include <zdict.h>
#include <zstd.h>

namespace l3kv {

class EBR {
public:
  std::atomic<uint64_t> global_epoch{1};
  std::array<std::atomic<uint64_t>, 64> local_epochs; // Max 64 shards

  EBR() {
    for (auto &e : local_epochs)
      e = 0;
  }

  void enter(size_t core_id) {
    local_epochs[core_id].store(global_epoch.load(std::memory_order_acquire),
                                 std::memory_order_release);
  }

  void exit(size_t core_id) {
    local_epochs[core_id].store(0, std::memory_order_release);
  }

  uint64_t get_min_epoch() {
    uint64_t min_e = global_epoch.load();
    for (const auto &e : local_epochs) {
      uint64_t le = e.load(std::memory_order_acquire);
      if (le > 0 && le < min_e)
        min_e = le;
    }
    return min_e;
  }

  void advance() { global_epoch.fetch_add(1, std::memory_order_acq_rel); }
};

class Blob;

struct EbrGarbageList {
  uint64_t retire_epoch;
  std::vector<std::unique_ptr<Blob>> items;
};

struct CCtxWrapper {
  ZSTD_CCtx *ctx;
  CCtxWrapper() { ctx = ZSTD_createCCtx(); }
  ~CCtxWrapper() { ZSTD_freeCCtx(ctx); }
};
struct DCtxWrapper {
  ZSTD_DCtx *ctx;
  DCtxWrapper() { ctx = ZSTD_createDCtx(); }
  ~DCtxWrapper() { ZSTD_freeDCtx(ctx); }
};

class ZstdManager {
  std::mutex mx_;
  std::vector<std::string> samples_;
  size_t samples_bytes_ = 0;
  ZSTD_CDict *cdict_ = nullptr;
  ZSTD_DDict *ddict_ = nullptr;
  std::atomic<bool> active_{false};

public:
  ZstdManager() = default;

  ~ZstdManager() {
    if (cdict_)
      ZSTD_freeCDict(cdict_);
    if (ddict_)
      ZSTD_freeDDict(ddict_);
  }

  void add_sample(const std::string &data) {
    if (active_.load(std::memory_order_acquire)) // Changed to acquire
      return;

    std::lock_guard<std::mutex> lock(mx_); // Used the new mutex
    if (active_.load(std::memory_order_relaxed))
      return;

    samples_.push_back(data);
    samples_bytes_ += data.size(); // Keep original size_t type

    if (samples_.size() >= 1000 || samples_bytes_ >= 100 * 1024) {
      train_dictionary();
      active_.store(true, std::memory_order_release);
      samples_.clear();
      samples_.shrink_to_fit();
    }
  }

  void train_dictionary() {
    if (samples_.empty())
      return;

    std::vector<size_t> sizes;
    std::vector<uint8_t> buffer;
    buffer.reserve(samples_bytes_);
    for (const auto &s : samples_) {
      sizes.push_back(s.size());
      buffer.insert(buffer.end(), s.begin(), s.end());
    }

    size_t dict_cap = 100 * 1024;
    std::vector<uint8_t> dict_buffer(dict_cap);

    size_t dict_size =
        ZDICT_trainFromBuffer(dict_buffer.data(), dict_cap, buffer.data(),
                              sizes.data(), sizes.size());

    if (ZDICT_isError(dict_size)) {
      std::cerr << "ZDICT_trainFromBuffer failed: "
                << ZDICT_getErrorName(dict_size) << "\n";
      return;
    }

    cdict_ = ZSTD_createCDict(dict_buffer.data(), dict_size, 3);
    ddict_ = ZSTD_createDDict(dict_buffer.data(), dict_size);
    std::cout << "[ZstdManager] Dictionary trained on " << sizes.size()
              << " samples. Size: " << dict_size << " bytes.\n";
  }

  bool is_active() const { return active_.load(std::memory_order_acquire); }

  void compress(const std::string &src, std::string &dst) {
    if (!is_active() || !cdict_) {
      dst = src;
      return;
    }
    static thread_local CCtxWrapper cctx;
    size_t bound = ZSTD_compressBound(src.size());
    dst.resize(bound);
    size_t csize = ZSTD_compress_usingCDict(cctx.ctx, dst.data(), bound,
                                             src.data(), src.size(), cdict_);
    if (ZSTD_isError(csize)) {
      dst = src;
    } else {
      dst.resize(csize);
    }
  }

  void decompress(const std::string &src, std::string &dst,
                  size_t original_size) {
    if (!is_active() || !ddict_) {
      dst = src;
      return;
    }
    static thread_local DCtxWrapper dctx;
    dst.resize(original_size);
    size_t dsize = ZSTD_decompress_usingDDict(
        dctx.ctx, dst.data(), original_size, src.data(), src.size(), ddict_);
    if (ZSTD_isError(dsize)) {
      dst = src;
    } else {
      dst.resize(dsize);
    }
  }
};

// A wrapper around a single Lite3 buffer using PMR
class Blob {
public:
  lite3cpp::Buffer buf_;
  bool compressed_ = false;
  uint32_t original_size_ = 0;

public:
  Blob(std::pmr::memory_resource *mr, size_t cap = 1024) : buf_(cap) {
    buf_.init_object();
  }

  void compress(ZstdManager *zstd) {
    if (compressed_ || !zstd || !zstd->is_active())
      return;
    std::string src(reinterpret_cast<const char *>(buf_.data()), buf_.size());
    std::string dst;
    zstd->compress(src, dst);
    if (dst.size() < src.size()) {
      original_size_ = static_cast<uint32_t>(src.size());
      compressed_ = true;
      buf_ = lite3cpp::Buffer(std::vector<uint8_t>(dst.begin(), dst.end()));
    }
  }

  void decompress(ZstdManager *zstd) {
    if (!compressed_ || !zstd)
      return;
    std::string src(reinterpret_cast<const char *>(buf_.data()), buf_.size());
    std::string dst;
    zstd->decompress(src, dst, original_size_);
    compressed_ = false;
    buf_ = lite3cpp::Buffer(std::vector<uint8_t>(dst.begin(), dst.end()));
  }

  void overwrite(const std::string &data, ZstdManager *zstd) {
    if (zstd && !zstd->is_active()) {
      zstd->add_sample(data);
    }

    bool is_json = false;
    if (!data.empty()) {
      char first = data[0];
      if (first == '{' || first == '[') {
        is_json = true;
      }
    }

    if (is_json) {
      try {
        lite3cpp::Buffer new_buf = lite3cpp::lite3_json::from_json_string(data);
        buf_ = std::move(new_buf);
        compressed_ = false;
        if (zstd)
          compress(zstd);
        return;
      } catch (...) {
      }
    }

    // Treat as binary
    std::vector<uint8_t> vec(data.begin(), data.end());
    buf_ = lite3cpp::Buffer(std::move(vec));
    compressed_ = false;
    if (zstd)
      compress(zstd);
  }

  bool set_int(const std::string &key, int64_t val, ZstdManager *zstd) {
    if (compressed_)
      decompress(zstd);
    buf_.set_i64(0, key, val);
    if (zstd)
      compress(zstd);
    return true;
  }

  bool set_str(const std::string &key, const std::string &val,
               ZstdManager *zstd) {
    if (compressed_)
      decompress(zstd);
    buf_.set_str(0, key, val);
    if (zstd)
      compress(zstd);
    return true;
  }

  std::span<const uint8_t> view() const { return {buf_.data(), buf_.size()}; }
};

class Engine {
  static constexpr size_t SHARDS = 64;
  struct CoreMessage {
    std::function<void()> task;
  };

  struct alignas(64) Shard {
    std::pmr::unsynchronized_pool_resource pool;
    std::map<std::string, std::unique_ptr<Blob>> map;

    // EBR
    std::vector<EbrGarbageList> garbage;
    uint32_t ops_since_reclaim = 0;

    void retire(std::unique_ptr<Blob> blob, uint64_t global_ep) {
      if (garbage.empty() || garbage.back().retire_epoch != global_ep) {
        garbage.push_back({global_ep, {}});
      }
      garbage.back().items.push_back(std::move(blob));
    }

    void reclaim(uint64_t min_epoch) {
      auto it = garbage.begin();
      while (it != garbage.end() && it->retire_epoch < min_epoch) {
        it = garbage.erase(it);
      }
    }

    // Message passing
    moodycamel::ConcurrentQueue<CoreMessage> messages;
    std::atomic<bool> stop_flag{false};
    std::thread core_thread;

    Shard() : pool(std::pmr::new_delete_resource()) {}
  };

  std::vector<std::unique_ptr<Shard>> shards_;
  std::unique_ptr<WriteAheadLog> wal_;
  HybridLogicalClock clock_;
  MerkleTree merkle_;
  std::unique_ptr<ZstdManager> zstd_manager_;
  EBR ebr_;

public:
  size_t get_routing_shard(const std::string &key) {
    size_t start = key.find('{');
    if (start != std::string::npos) {
      size_t end = key.find('}', start + 1);
      if (end != std::string::npos) {
        std::string_view tag(key.data() + start + 1, end - start - 1);
        return XXH3_64bits(tag.data(), tag.size()) % SHARDS;
      }
    }
    return XXH3_64bits(key.data(), key.size()) % SHARDS;
  }

private:
  Shard &get_shard(const std::string &key) {
    return *shards_[get_routing_shard(key)];
  }

private:
  Timestamp get_local_timestamp_internal(const std::string &key) {
    return {0, 0, 0};
  }

  uint64_t hash_blob(const std::unique_ptr<Blob> &blob) {
    if (!blob)
      return 0;
    auto v = blob->view();
    return fnv1a_64(v.data(), v.size());
  }

  void apply_put(const std::string &key, const std::string &json_body) {
    auto &s = get_shard(key);

    uint64_t old_h = 0;
    if (s.map.contains(key)) {
      old_h = hash_blob(s.map[key]);
    } else {
      s.map[key] = std::make_unique<Blob>(&s.pool);
    }

    s.map[key]->overwrite(json_body, zstd_manager_.get());
    uint64_t new_h = hash_blob(s.map[key]);
    merkle_.apply_delta(key, old_h ^ new_h);
  }

  void apply_patch_int(const std::string &key, const std::string &field,
                       int64_t val) {
    auto &s = get_shard(key);
    if (!s.map.contains(key))
      s.map[key] = std::make_unique<Blob>(&s.pool);

    uint64_t old_h = hash_blob(s.map[key]);
    s.map[key]->set_int(field, val, zstd_manager_.get());
    uint64_t new_h = hash_blob(s.map[key]);
    merkle_.apply_delta(key, old_h ^ new_h);
  }

  void apply_patch_str(const std::string &key, const std::string &field,
                       const std::string &val) {
    auto &s = get_shard(key);
    if (!s.map.contains(key))
      s.map[key] = std::make_unique<Blob>(&s.pool);

    uint64_t old_h = hash_blob(s.map[key]);
    s.map[key]->set_str(field, val, zstd_manager_.get());
    uint64_t new_h = hash_blob(s.map[key]);
    merkle_.apply_delta(key, old_h ^ new_h);
  }

  bool apply_del(const std::string &key) {
    auto &s = get_shard(key);

    // Tombstone logic: Don't erase. Set to empty.
    if (!s.map.contains(key)) {
      s.map[key] = std::make_unique<Blob>(&s.pool);
    }

    uint64_t old_h = hash_blob(s.map[key]);
    s.map[key]->overwrite("", zstd_manager_.get()); // Set to empty (Tombstone)
    uint64_t new_h = hash_blob(s.map[key]);

    merkle_.apply_delta(key, old_h ^ new_h);
    return true; // Always "succeeded" in setting tombstone
  }

public:
  Engine(std::string wal_path, uint32_t node_id = 1) : clock_(node_id) {
    wal_ = std::make_unique<WriteAheadLog>(wal_path);
    zstd_manager_ = std::make_unique<ZstdManager>();
    for (size_t i = 0; i < SHARDS; ++i)
      shards_.push_back(std::make_unique<Shard>());

    wal_->recover(
        [this](WalOp op, std::string_view key, std::string_view payload) {
          try {
            if (op == WalOp::PUT) {
              apply_put(std::string(key), std::string(payload));
            } else if (op == WalOp::PATCH_I64) {
              std::string p(payload);
              size_t colon = p.find(':');
              if (colon != std::string::npos) {
                std::string field = p.substr(0, colon);
                int64_t val = std::stoll(p.substr(colon + 1));
                apply_patch_int(std::string(key), field, val);
              }
            } else if (op == WalOp::PATCH_STR) {
              std::string p(payload);
              size_t colon = p.find(':');
              if (colon != std::string::npos) {
                std::string field = p.substr(0, colon);
                std::string val = p.substr(colon + 1);
                apply_patch_str(std::string(key), field, val);
              }
            } else if (op == WalOp::DELETE_) {
              apply_del(std::string(key));
            }
          } catch (const std::exception &e) {
            std::cerr << "WAL Recovery Skip: " << e.what() << "\n";
          }
        });

    for (size_t i = 0; i < SHARDS; ++i) {
      shards_[i]->core_thread = std::thread([this, i]() {
#ifdef _WIN32
        HANDLE hThread = GetCurrentThread();
        DWORD_PTR mask = (DWORD_PTR)1
                         << (i % std::thread::hardware_concurrency());
        SetThreadAffinityMask(hThread, mask);
#endif
        auto &s = get_shard_by_index(i);
        CoreMessage msg;
        int spin = 0;
        while (!s.stop_flag.load(std::memory_order_relaxed) ||
               s.messages.size_approx() > 0) {
          if (s.messages.try_dequeue(msg)) {
            spin = 0;
            ebr_.enter(i);
            msg.task();
            s.ops_since_reclaim++;

            if (s.ops_since_reclaim >= 1000) {
              if (i == 0)
                ebr_.advance();
              s.reclaim(ebr_.get_min_epoch());
              s.ops_since_reclaim = 0;
            }
            ebr_.exit(i);
          } else {
            if (spin < 10000) {
              spin++;
            } else {
              std::this_thread::yield();
            }
          }
        }
      });
    }
  }

  ~Engine() {
    for (size_t i = 0; i < SHARDS; ++i) {
      shards_[i]->stop_flag.store(true, std::memory_order_release);
      if (shards_[i]->core_thread.joinable()) {
        shards_[i]->core_thread.join();
      }
    }
  }

  Shard &get_shard_by_index(size_t index) { return *shards_[index]; }

  template <typename Func> std::future<typename std::invoke_result_t<Func>> submit_to_shard_idx(size_t h, Func &&f) {
    using ReturnType = typename std::invoke_result_t<Func>;
    auto &s = *shards_[h];

    std::shared_ptr<std::promise<ReturnType>> p = std::make_shared<std::promise<ReturnType>>();
    auto fut = p->get_future();

    s.messages.enqueue({[p, f = std::forward<Func>(f)]() mutable {
      if constexpr (std::is_void_v<ReturnType>) {
        f();
        p->set_value();
      } else {
        p->set_value(f());
      }
    }});
    return fut;
  }

  template <typename Func> void submit_async(const std::string &key, Func &&f) {
    size_t h = get_routing_shard(key);
    auto &s = *shards_[h];
    s.messages.enqueue({[f = std::forward<Func>(f)]() mutable { f(); }});
  }

  template <typename Func> auto submit_to_shard(const std::string &key, Func &&f) {
    size_t h = get_routing_shard(key);
    return submit_to_shard_idx(h, std::forward<Func>(f)).get();
  }

  lite3cpp::Buffer get(const std::string &key) {
    return submit_to_shard(key, [&, key]() {
      auto &s = get_shard(key);
      if (auto it = s.map.find(key); it != s.map.end()) {
        if (it->second->compressed_) {
          std::string src(
              reinterpret_cast<const char *>(it->second->buf_.data()),
              it->second->buf_.size());
          std::string dst;
          zstd_manager_->decompress(src, dst, it->second->original_size_);
          return lite3cpp::Buffer(std::vector<uint8_t>(dst.begin(), dst.end()));
        }
        return it->second->buf_;
      }
      return lite3cpp::Buffer();
    });
  }

  void put(std::string key, const std::string &json_body) {
    auto now = clock_.now();
    std::string_view mkey_v = KeyBuilder::meta_key(key);
    std::string mkey_s(mkey_v);
    std::string meta_val = "{\"ts\":" + std::to_string(now.wall_time) +
                           ",\"l\":" + std::to_string(now.logical) +
                           ",\"n\":" + std::to_string(now.node_id) + "}";

    std::vector<BatchOp> batch;
    batch.push_back({WalOp::PUT, key, json_body});
    batch.push_back({WalOp::PUT, mkey_s, meta_val});

    wal_->append_batch(batch);

    submit_async(key, [this, key, json_body, mkey_s, meta_val]() {
      apply_put(key, json_body);
      apply_put(mkey_s, meta_val);
    });
  }

  void patch_int(std::string key, std::string field, int64_t val) {
    auto now = clock_.now();
    std::string_view mkey_v = KeyBuilder::meta_key(key);
    std::string mkey_s(mkey_v);
    std::string ts_str = std::to_string(now.wall_time) + ":" +
                         std::to_string(now.logical) + ":" +
                         std::to_string(now.node_id);

    std::string log_payload_int = field + ":" + std::to_string(val);
    std::string log_payload_str = field + ":" + ts_str;

    std::vector<BatchOp> batch;
    batch.push_back({WalOp::PATCH_I64, key, log_payload_int});
    batch.push_back({WalOp::PATCH_STR, mkey_s, log_payload_str});

    wal_->append_batch(batch);

    submit_async(key, [this, key, field, val, mkey_s, ts_str]() {
      apply_patch_int(key, field, val);
      apply_patch_str(mkey_s, field, ts_str);
    });
  }

  void patch_str(std::string key, std::string field, std::string val) {
    auto now = clock_.now();
    std::string_view mkey_v = KeyBuilder::meta_key(key);
    std::string mkey_s(mkey_v);
    std::string ts_str = std::to_string(now.wall_time) + ":" +
                         std::to_string(now.logical) + ":" +
                         std::to_string(now.node_id);

    std::string log_payload_str = field + ":" + val;
    std::string log_payload_meta = field + ":" + ts_str;

    std::vector<BatchOp> batch;
    batch.push_back({WalOp::PATCH_STR, key, log_payload_str});
    batch.push_back({WalOp::PATCH_STR, mkey_s, log_payload_meta});

    wal_->append_batch(batch);

    submit_async(key, [this, key, field, val, mkey_s, ts_str]() {
      apply_patch_str(key, field, val);
      apply_patch_str(mkey_s, field, ts_str);
    });
  }

  bool del(const std::string &key) {
    auto now = clock_.now();
    std::string_view mkey_v = KeyBuilder::meta_key(key);
    std::string mkey_s(mkey_v);
    std::string meta_val = "{\"ts\":" + std::to_string(now.wall_time) +
                           ",\"l\":" + std::to_string(now.logical) +
                           ",\"n\":" + std::to_string(now.node_id) +
                           ",\"tombstone\":true}";

    std::vector<BatchOp> batch;
    batch.push_back({WalOp::DELETE_, key, ""});
    batch.push_back({WalOp::PUT, mkey_s, meta_val});

    wal_->append_batch(batch);

    return submit_to_shard(key, [this, key, mkey_s, meta_val]() {
      bool existed = apply_del(key);
      apply_put(mkey_s, meta_val);
      return existed;
    });
  }

  inline void apply_mutation(const Mutation &m) {
    std::string_view meta_key_v = KeyBuilder::meta_key(m.key);
    std::string meta_key_lookup(meta_key_v);
    auto buf = get(meta_key_lookup);
    Timestamp local_ts{0, 0, 0};
    if (buf.size() > 0) {
      auto type = buf.get_type(0, "ts");
      if (type == lite3cpp::Type::Int64 || type == lite3cpp::Type::Float64) {
        int64_t w = buf.get_i64(0, "ts");
        uint32_t l = (uint32_t)buf.get_i64(0, "l");
        uint32_t n = (uint32_t)buf.get_i64(0, "n");
        local_ts = {w, l, n};
      }
    }

    if (m.timestamp <= local_ts) {
      std::cerr << "[Store] Rejecting mutation for " << m.key
                << " (Stale). Inc: " << m.timestamp.wall_time
                << " Local: " << local_ts.wall_time << "\n";
      return;
    }

    std::string meta_val = "{\"ts\":" + std::to_string(m.timestamp.wall_time) +
                           ",\"l\":" + std::to_string(m.timestamp.logical) +
                           ",\"n\":" + std::to_string(m.timestamp.node_id) +
                           (m.is_delete ? ",\"tombstone\":true" : "") + "}";

    std::vector<BatchOp> wal_batch;
    if (m.is_delete) {
      wal_batch.push_back({WalOp::DELETE_, m.key, ""});
    } else {
      std::string val_str(m.value.begin(), m.value.end());
      wal_batch.push_back({WalOp::PUT, m.key, val_str});
    }
    wal_batch.push_back({WalOp::PUT, meta_key_lookup, meta_val});

    wal_->append_batch(wal_batch);

    if (m.is_delete) {
      apply_del(m.key);
    } else {
      std::string val_str(m.value.begin(), m.value.end());
      apply_put(m.key, val_str);
    }
    apply_put(meta_key_lookup, meta_val);
  }

  void flush() { wal_->flush(); }
  void wait_all_shards() {
    std::vector<std::future<void>> futures;
    for (size_t i = 0; i < SHARDS; ++i) {
      futures.push_back(submit_to_shard_idx(i, []() {}));
    }
    for (auto &f : futures) {
      f.get();
    }
  }
  auto get_wal_stats() { return wal_->stats(); }
  uint64_t get_merkle_root_hash() { return merkle_.get_root_hash(); }
  uint64_t get_merkle_node(int level, int index) {
    return merkle_.get_node_hash(level, index);
  }

  std::vector<std::pair<std::string, uint64_t>>
  get_bucket_keys(int bucket_idx) {
    std::vector<std::pair<std::string, uint64_t>> result;
    for (size_t i = 0; i < SHARDS; ++i) {
      auto fut = submit_to_shard_idx(i, [&, i, bucket_idx]() {
        std::vector<std::pair<std::string, uint64_t>> local_res;
        auto &shard = *shards_[i];
        for (auto &[k, v] : shard.map) {
          uint64_t kh = fnv1a_64(k.c_str(), k.size());
          uint32_t b = (kh >> 48) & 0xFFFF;
          if (b == (uint32_t)bucket_idx) {
            local_res.push_back({k, hash_blob(v)});
          }
        }
        return local_res;
      });
      auto part = fut.get();
      result.insert(result.end(), part.begin(), part.end());
    }
    return result;
  }

  std::vector<std::pair<std::string, std::string>>
  get_prefix_chunk(const std::string &prefix, size_t shard_idx,
                   const std::string &start_key, size_t limit) {
    std::vector<std::pair<std::string, std::string>> chunk;
    if (shard_idx >= SHARDS)
      return chunk;

    auto &s = *shards_[shard_idx];
    return submit_to_shard_idx(shard_idx, [&, shard_idx]() {
      auto it = s.map.lower_bound(start_key);
      while (it != s.map.end() && it->first.starts_with(prefix)) {
        if (it->first > start_key ||
            (chunk.empty() && it->first >= start_key)) {
          if (it->second->buf_.size() > 0) {
            std::string vstr;
            if (it->second->compressed_) {
              std::string src(
                  reinterpret_cast<const char *>(it->second->buf_.data()),
                  it->second->buf_.size());
              zstd_manager_->decompress(src, vstr, it->second->original_size_);
            } else {
              vstr = std::string(
                  reinterpret_cast<const char *>(it->second->buf_.data()),
                  it->second->buf_.size());
            }
            chunk.push_back({it->first, std::move(vstr)});
            if (chunk.size() >= limit)
              break;
          }
        }
        ++it;
      }
      return chunk;
    }).get();
  }

  std::vector<std::string> get_prefix_keys(const std::string &prefix,
                                           size_t shard_idx,
                                           const std::string &start_key,
                                           size_t limit) {
    std::vector<std::string> chunk;
    if (shard_idx >= SHARDS)
      return chunk;

    auto &s = *shards_[shard_idx];
    return submit_to_shard_idx(shard_idx, [&, shard_idx]() {
      auto it = s.map.lower_bound(start_key);
      while (it != s.map.end() && it->first.starts_with(prefix)) {
        if (it->first > start_key ||
            (chunk.empty() && it->first >= start_key)) {
          chunk.push_back(it->first);
          if (chunk.size() >= limit)
            break;
        }
        ++it;
      }
      return chunk;
    }).get();
  }
  std::vector<std::string>
  get_prefix_keys_all_shards(const std::string &prefix,
                             const std::string &start_key,
                             size_t limit_per_shard) {
    std::vector<std::future<std::vector<std::string>>> futures;
    for (size_t i = 0; i < SHARDS; ++i) {
      futures.push_back(submit_to_shard_idx(i, [this, i, prefix, start_key, limit_per_shard]() {
        std::vector<std::string> chunk;
        auto &s = *shards_[i];
        auto it = s.map.lower_bound(start_key);
        while (it != s.map.end() && it->first.starts_with(prefix)) {
          if (it->first >= start_key) {
            chunk.push_back(it->first);
            if (chunk.size() >= limit_per_shard)
              break;
          }
          ++it;
        }
        return chunk;
      }));
    }

    std::vector<std::string> results;
    for (auto &fut : futures) {
      auto chunk = fut.get();
      results.insert(results.end(), chunk.begin(), chunk.end());
    }
    return results;
  }
};

} // namespace l3kv
