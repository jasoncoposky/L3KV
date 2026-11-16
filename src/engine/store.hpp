#pragma once
#include "KeyBuilder.hpp"
#include "clock.hpp"
#include "credential_manager.hpp"
#include "merkle.hpp"
#include "replication_log.hpp"
#include "wal.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "../../lib/concurrentqueue/blockingconcurrentqueue.h"

#include "buffer.hpp"
#include "json.hpp"

#include <zdict.h>
#include <zstd.h>

namespace l3kv {

class Blob;

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
public:
  struct TrainedDict {
    ZSTD_CDict *cdict = nullptr;
    ZSTD_DDict *ddict = nullptr;
    ~TrainedDict() {
      if (cdict) {
        ZSTD_freeCDict(cdict);
      }
      if (ddict) {
        ZSTD_freeDDict(ddict);
      }
    }
  };

private:
  mutable std::mutex mx_;
  mutable std::condition_variable cv_;
  std::vector<std::string> samples_;
  size_t samples_bytes_ = 0;
  std::atomic<bool> sampling_done_{false};
  std::atomic<bool> training_in_flight_{false};
  std::atomic<std::shared_ptr<const TrainedDict>> dict_{nullptr};
  std::thread training_thread_;

  void train_from_samples(std::vector<std::string> samples,
                          size_t total_bytes) {
    struct FlightGuard {
      ZstdManager *self;
      ~FlightGuard() {
        std::lock_guard<std::mutex> lk(self->mx_);
        self->training_in_flight_.store(false, std::memory_order_release);
        self->cv_.notify_all();
      }
    } guard{this};

    if (samples.empty()) {
      return;
    }

    std::vector<size_t> sizes;
    sizes.reserve(samples.size());
    std::vector<uint8_t> buffer;
    buffer.reserve(total_bytes);
    for (const auto &s : samples) {
      sizes.push_back(s.size());
      buffer.insert(buffer.end(), s.begin(), s.end());
    }

    size_t dict_cap = std::min(static_cast<size_t>(100 * 1024),
                               std::max(static_cast<size_t>(256),
                                        buffer.size() / 10));
    std::vector<uint8_t> dict_buffer(dict_cap);

    size_t dict_size = ZDICT_trainFromBuffer(
        dict_buffer.data(), dict_cap, buffer.data(), sizes.data(),
        static_cast<unsigned>(sizes.size()));

    if (ZDICT_isError(dict_size)) {
      std::cerr << "ZDICT_trainFromBuffer failed: "
                << ZDICT_getErrorName(dict_size) << "\n";
      return;
    }

    auto trained = std::make_shared<TrainedDict>();
    trained->cdict = ZSTD_createCDict(dict_buffer.data(), dict_size, 3);
    trained->ddict = ZSTD_createDDict(dict_buffer.data(), dict_size);
    if (!trained->cdict || !trained->ddict) {
      return;
    }

    dict_.store(std::shared_ptr<const TrainedDict>(std::move(trained)),
                std::memory_order_release);
    std::cerr << "[ZstdManager] Dictionary trained on " << sizes.size()
              << " samples. Size: " << dict_size << " bytes.\n";
  }

public:
  ZstdManager() = default;

  ~ZstdManager() {
    if (training_thread_.joinable()) {
      training_thread_.join();
    }
  }

  void add_sample(std::string_view data) {
    if (data.size() < 512) {
      return;
    }
    if (sampling_done_.load(std::memory_order_acquire)) {
      return;
    }

    std::vector<std::string> to_train;
    size_t total_bytes = 0;
    {
      std::lock_guard<std::mutex> lock(mx_);
      if (sampling_done_.load(std::memory_order_relaxed)) {
        return;
      }

      samples_.emplace_back(data);
      samples_bytes_ += data.size();

      if (samples_.size() >= 1000 || samples_bytes_ >= 100 * 1024) {
        sampling_done_.store(true, std::memory_order_release);
        training_in_flight_.store(true, std::memory_order_release);
        to_train = std::move(samples_);
        total_bytes = samples_bytes_;
        samples_.clear();
        samples_.shrink_to_fit();
        samples_bytes_ = 0;
        training_thread_ = std::thread(
            [this, s = std::move(to_train), total_bytes]() mutable {
              train_from_samples(std::move(s), total_bytes);
            });
      }
    }
  }

  std::shared_ptr<const TrainedDict> get_dict() const noexcept {
    return dict_.load(std::memory_order_acquire);
  }

  bool is_active() const {
    if (dict_.load(std::memory_order_acquire) != nullptr) {
      return true;
    }
    if (training_in_flight_.load(std::memory_order_acquire)) {
      std::unique_lock<std::mutex> lock(mx_);
      cv_.wait(lock, [this]() {
        return !training_in_flight_.load(std::memory_order_acquire);
      });
      return dict_.load(std::memory_order_acquire) != nullptr;
    }
    return false;
  }

  size_t compress_raw(const void *src, size_t src_size, void *dst,
                      size_t dst_cap) const {
    auto d = get_dict();
    if (!d || !d->cdict) {
      return 0;
    }
    static thread_local CCtxWrapper cctx;
    size_t csize = ZSTD_compress_usingCDict(cctx.ctx, dst, dst_cap, src,
                                            src_size, d->cdict);
    return ZSTD_isError(csize) ? 0 : csize;
  }

  bool decompress_into(const void *src, size_t src_size, void *dst,
                       size_t original_size) const {
    auto d = get_dict();
    if (!d || !d->ddict) {
      return false;
    }
    static thread_local DCtxWrapper dctx;
    size_t dsize = ZSTD_decompress_usingDDict(
        dctx.ctx, dst, original_size, src, src_size, d->ddict);
    return !ZSTD_isError(dsize) && dsize == original_size;
  }

  void compress(const std::string &src, std::string &dst) const {
    auto d = get_dict();
    if (!d || !d->cdict) {
      dst = src;
      return;
    }
    static thread_local CCtxWrapper cctx;
    size_t bound = ZSTD_compressBound(src.size());
    dst.resize(bound);
    size_t csize = ZSTD_compress_usingCDict(cctx.ctx, dst.data(), bound,
                                            src.data(), src.size(), d->cdict);
    if (ZSTD_isError(csize)) {
      dst = src;
    } else {
      dst.resize(csize);
    }
  }

  void decompress(const std::string &src, std::string &dst,
                  size_t original_size) const {
    auto d = get_dict();
    if (!d || !d->ddict) {
      dst = src;
      return;
    }
    static thread_local DCtxWrapper dctx;
    dst.resize(original_size);
    size_t dsize = ZSTD_decompress_usingDDict(
        dctx.ctx, dst.data(), original_size, src.data(), src.size(), d->ddict);
    if (ZSTD_isError(dsize)) {
      dst = src;
    } else {
      dst.resize(dsize);
    }
  }
};

static inline bool is_json_payload(std::string_view data) {
  size_t first_non_ws = data.find_first_not_of(" \t\r\n");
  return (first_non_ws != std::string_view::npos &&
          (data[first_non_ws] == '{' || data[first_non_ws] == '['));
}

// A wrapper around a single Lite3 buffer
class Blob {
public:
  lite3cpp::Buffer buf_;
  bool compressed_ = false;
  uint32_t original_size_ = 0;
  uint64_t content_hash_ = 0;

public:
  Blob() = default;

  explicit Blob(std::pmr::memory_resource * /*mr*/, size_t cap = 0)
      : buf_(cap) {
    if (cap > 0) {
      buf_.init_object();
      original_size_ = static_cast<uint32_t>(buf_.size());
      content_hash_ = XXH3_64bits(buf_.data(), buf_.size());
    }
  }

  explicit Blob(std::string_view data, ZstdManager *zstd = nullptr,
                std::pmr::memory_resource * /*mr*/ = nullptr,
                bool allow_compress = true) {
    overwrite(data, allow_compress ? zstd : nullptr);
  }

  uint64_t content_hash() const noexcept { return content_hash_; }

  void compress(ZstdManager *zstd) {
    if (compressed_ || buf_.size() < 512 || !zstd) {
      return;
    }
    auto dict = zstd->get_dict();
    if (!dict || !dict->cdict) {
      return;
    }

    const size_t src_size = buf_.size();
    const size_t bound = ZSTD_compressBound(src_size);
    thread_local std::vector<uint8_t> cscratch;
    cscratch.resize(bound);

    static thread_local CCtxWrapper cctx;
    size_t csize = ZSTD_compress_usingCDict(
        cctx.ctx, cscratch.data(), bound, buf_.data(), src_size, dict->cdict);
    if (!ZSTD_isError(csize) && csize + 32 < src_size) {
      original_size_ = static_cast<uint32_t>(src_size);
      compressed_ = true;
      buf_ = lite3cpp::Buffer(cscratch.data(), csize);
    }
    if (cscratch.capacity() > 4ULL * 1024 * 1024 && bound < 65536) {
      cscratch.clear();
      cscratch.shrink_to_fit();
    }
  }

  void decompress(ZstdManager *zstd) {
    if (!compressed_ || !zstd) {
      return;
    }
    std::vector<uint8_t> dst(original_size_);
    if (zstd->decompress_into(buf_.data(), buf_.size(), dst.data(),
                              dst.size())) {
      compressed_ = false;
      buf_ = lite3cpp::Buffer(std::move(dst));
    }
  }

  void overwrite(std::string_view data, ZstdManager *zstd) {
    if (data.empty()) {
      buf_ = lite3cpp::Buffer();
      compressed_ = false;
      original_size_ = 0;
      content_hash_ = 0;
      return;
    }

    if (is_json_payload(data)) {
      try {
        buf_ = lite3cpp::lite3_json::from_json_string(data);
        compressed_ = false;
        original_size_ = static_cast<uint32_t>(buf_.size());
        content_hash_ =
            buf_.size() > 0 ? XXH3_64bits(buf_.data(), buf_.size()) : 0;
        if (zstd && buf_.size() >= 512) {
          zstd->add_sample(std::string_view(
              reinterpret_cast<const char *>(buf_.data()), buf_.size()));
          compress(zstd);
        }
        return;
      } catch (...) {
      }
    }

    // Treat as binary
    buf_ = lite3cpp::Buffer(reinterpret_cast<const uint8_t *>(data.data()),
                            data.size());
    compressed_ = false;
    original_size_ = static_cast<uint32_t>(buf_.size());
    content_hash_ = buf_.size() > 0 ? XXH3_64bits(buf_.data(), buf_.size()) : 0;
    if (zstd && buf_.size() >= 512) {
      zstd->add_sample(std::string_view(
          reinterpret_cast<const char *>(buf_.data()), buf_.size()));
      compress(zstd);
    }
  }

  bool set_int(std::string_view key, int64_t val, ZstdManager *zstd) {
    if (compressed_) {
      decompress(zstd);
    }
    if (buf_.size() == 0) {
      buf_.init_object();
    }
    buf_.set_i64(0, lite3cpp::HashedKey(key), val);
    original_size_ = static_cast<uint32_t>(buf_.size());
    content_hash_ = buf_.size() > 0 ? XXH3_64bits(buf_.data(), buf_.size()) : 0;
    if (zstd && buf_.size() >= 512) {
      compress(zstd);
    }
    return true;
  }

  bool set_str(std::string_view key, std::string_view val, ZstdManager *zstd) {
    if (compressed_) {
      decompress(zstd);
    }
    if (buf_.size() == 0) {
      buf_.init_object();
    }
    buf_.set_str(0, lite3cpp::HashedKey(key), val);
    original_size_ = static_cast<uint32_t>(buf_.size());
    content_hash_ = buf_.size() > 0 ? XXH3_64bits(buf_.data(), buf_.size()) : 0;
    if (zstd && buf_.size() >= 512) {
      compress(zstd);
    }
    return true;
  }

  std::span<const uint8_t> view() const { return {buf_.data(), buf_.size()}; }
};

class ILogger {
public:
  virtual ~ILogger() = default;
  virtual void log(const std::string &msg) = 0;
};

class Engine {
  static constexpr size_t SHARDS = 8;
  std::shared_ptr<ILogger> logger_;
  struct CoreMessage {
    std::function<void()> task;
  };

  struct alignas(64) Shard {
    std::pmr::unsynchronized_pool_resource pool;
    std::map<std::string, std::shared_ptr<const Blob>, std::less<>> map;

    // Legacy async queue retained for submit_to_shard / submit_async callers
    moodycamel::BlockingConcurrentQueue<CoreMessage> messages;
    std::atomic<bool> stop_flag{false};
    std::thread core_thread;

    Shard() : pool(std::pmr::new_delete_resource()) {}
    mutable std::shared_mutex read_mu;
  };

  struct ShardPairLock {
    std::unique_lock<std::shared_mutex> lk1;
    std::unique_lock<std::shared_mutex> lk2;
    ShardPairLock(std::vector<std::unique_ptr<Shard>> &shards, size_t a,
                  size_t b) {
      if (a == b) {
        lk1 = std::unique_lock<std::shared_mutex>(shards[a]->read_mu);
      } else if (a < b) {
        lk1 = std::unique_lock<std::shared_mutex>(shards[a]->read_mu);
        lk2 = std::unique_lock<std::shared_mutex>(shards[b]->read_mu);
      } else {
        lk1 = std::unique_lock<std::shared_mutex>(shards[b]->read_mu);
        lk2 = std::unique_lock<std::shared_mutex>(shards[a]->read_mu);
      }
    }
  };

  static uint64_t next_instance_id() noexcept {
    static std::atomic<uint64_t> seq{1};
    return seq.fetch_add(1, std::memory_order_relaxed);
  }

  const uint64_t instance_id_{next_instance_id()};
  std::atomic<uint64_t> clock_epoch_{1};
  std::atomic<size_t> async_enqueued_{0};

  std::vector<std::unique_ptr<Shard>> shards_;
  std::unique_ptr<WriteAheadLog> wal_;
  HybridLogicalClock clock_;
  MerkleTree merkle_;
  std::unique_ptr<ZstdManager> zstd_manager_;
  std::unique_ptr<CredentialManager> credentials_;
  mutable std::shared_mutex compaction_mu_;

public:
  CredentialManager &credentials() { return *credentials_; }
  HybridLogicalClock &get_clock() { return clock_; }

  size_t get_routing_shard(std::string_view key) const noexcept {
    size_t start = key.find('{');
    if (start != std::string_view::npos) {
      size_t end = key.find('}', start + 1);
      if (end != std::string_view::npos) {
        std::string_view tag(key.data() + start + 1, end - start - 1);
        return XXH3_64bits(tag.data(), tag.size()) % SHARDS;
      }
    }
    if (key.size() > 5 && key.ends_with(":meta")) {
      std::string_view base = key.substr(0, key.size() - 5);
      return XXH3_64bits(base.data(), base.size()) % SHARDS;
    }
    return XXH3_64bits(key.data(), key.size()) % SHARDS;
  }

private:
  Shard &get_shard(std::string_view key) {
    return *shards_[get_routing_shard(key)];
  }

  Timestamp next_timestamp() {
    struct BoundTlClock {
      const Engine *engine = nullptr;
      uint64_t instance_id = 0;
      uint64_t epoch = 0;
      ThreadLocalClock clock{nullptr};
    };
    thread_local BoundTlClock tl;
    const uint64_t ep = clock_epoch_.load(std::memory_order_acquire);
    if (tl.engine != this || tl.instance_id != instance_id_ || tl.epoch != ep) {
      tl.engine = this;
      tl.instance_id = instance_id_;
      tl.epoch = ep;
      tl.clock = ThreadLocalClock(&clock_);
    }
    return tl.clock.now();
  }

  static lite3cpp::Buffer &format_meta_tl(const Timestamp &ts,
                                          bool tombstone = false) {
    using namespace lite3cpp::literals;
    thread_local lite3cpp::Buffer tl_mbuf;
    tl_mbuf.clear();
    tl_mbuf.reserve(160);
    tl_mbuf.init_object();
    tl_mbuf.set_i64(0, "ts"_hk, ts.wall_time);
    tl_mbuf.set_i64(0, "l"_hk, ts.logical);
    tl_mbuf.set_i64(0, "n"_hk, ts.node_id);
    if (tombstone) {
      tl_mbuf.set_bool(0, "tombstone"_hk, true);
    }
    return tl_mbuf;
  }

  static std::shared_ptr<Blob> make_meta_blob(const lite3cpp::Buffer &mbuf) {
    auto b = std::make_shared<Blob>();
    b->buf_ = lite3cpp::Buffer(mbuf.data(), mbuf.size());
    b->compressed_ = false;
    b->original_size_ = static_cast<uint32_t>(mbuf.size());
    b->content_hash_ =
        mbuf.size() > 0 ? XXH3_64bits(mbuf.data(), mbuf.size()) : 0;
    return b;
  }

  static std::string encode_wal_patch_i64(std::string_view field, int64_t val) {
    if (field.size() > UINT32_MAX - 13) {
      throw std::invalid_argument("Patch field name exceeds UINT32_MAX");
    }
    std::string out;
    out.resize(1 + 4 + field.size() + 8);
    out[0] = '\0';
    uint32_t flen = static_cast<uint32_t>(field.size());
    std::memcpy(out.data() + 1, &flen, 4);
    if (flen > 0) {
      std::memcpy(out.data() + 5, field.data(), flen);
    }
    std::memcpy(out.data() + 5 + flen, &val, 8);
    return out;
  }

  static bool decode_wal_patch_i64(std::string_view payload,
                                   std::string_view &field_out,
                                   int64_t &val_out) noexcept {
    if (!payload.empty() && payload[0] == '\0') {
      if (payload.size() < 13) {
        return false;
      }
      uint32_t flen = 0;
      std::memcpy(&flen, payload.data() + 1, 4);
      if (payload.size() != 5 + static_cast<size_t>(flen) + 8) {
        return false;
      }
      field_out = std::string_view(payload.data() + 5, flen);
      std::memcpy(&val_out, payload.data() + 5 + flen, 8);
      return true;
    }
    size_t colon = payload.rfind(':');
    if (colon == std::string_view::npos) {
      return false;
    }
    field_out = payload.substr(0, colon);
    std::string_view num_sv = payload.substr(colon + 1);
    auto [ptr, ec] =
        std::from_chars(num_sv.data(), num_sv.data() + num_sv.size(), val_out);
    return ec == std::errc{} && ptr == num_sv.data() + num_sv.size();
  }

  static std::string encode_wal_patch_str(std::string_view field,
                                          std::string_view val) {
    if (field.size() > UINT32_MAX - 5) {
      throw std::invalid_argument("Patch field name exceeds UINT32_MAX");
    }
    std::string out;
    out.resize(1 + 4 + field.size() + val.size());
    out[0] = '\0';
    uint32_t flen = static_cast<uint32_t>(field.size());
    std::memcpy(out.data() + 1, &flen, 4);
    if (flen > 0) {
      std::memcpy(out.data() + 5, field.data(), flen);
    }
    if (!val.empty()) {
      std::memcpy(out.data() + 5 + flen, val.data(), val.size());
    }
    return out;
  }

  static bool decode_wal_patch_str(std::string_view payload,
                                   std::string_view &field_out,
                                   std::string_view &val_out) noexcept {
    if (!payload.empty() && payload[0] == '\0') {
      if (payload.size() < 5) {
        return false;
      }
      uint32_t flen = 0;
      std::memcpy(&flen, payload.data() + 1, 4);
      if (5 + static_cast<size_t>(flen) > payload.size()) {
        return false;
      }
      field_out = std::string_view(payload.data() + 5, flen);
      val_out = std::string_view(payload.data() + 5 + flen,
                                 payload.size() - 5 - flen);
      return true;
    }
    size_t colon = payload.find(':');
    if (colon == std::string_view::npos) {
      return false;
    }
    field_out = payload.substr(0, colon);
    val_out = payload.substr(colon + 1);
    return true;
  }

  static std::shared_ptr<const Blob> get_empty_blob() {
    static const auto empty = std::make_shared<Blob>();
    return empty;
  }

  uint64_t hash_blob(const std::shared_ptr<const Blob> &blob) const noexcept {
    return blob ? blob->content_hash() : 0;
  }

  static bool has_complete_routing_tag(std::string_view key) noexcept {
    size_t start = key.find('{');
    if (start == std::string_view::npos) {
      return false;
    }
    return key.find('}', start + 1) != std::string_view::npos;
  }

  static bool is_expired_meta_tombstone(std::string_view key,
                                        std::string_view raw_bytes,
                                        int64_t now_wall_us,
                                        uint64_t tombstone_ttl_ms) noexcept {
    using namespace lite3cpp::literals;
    if (!key.ends_with(":meta") ||
        raw_bytes.size() < lite3cpp::config::node_size) {
      return false;
    }
    auto mbuf = lite3cpp::Buffer::from_view(
        reinterpret_cast<const uint8_t *>(raw_bytes.data()), raw_bytes.size());
    auto tomb = mbuf.try_get_bool(0, "tombstone"_hk);
    if (!tomb.has_value() || !*tomb) {
      return false;
    }
    if (tombstone_ttl_ms == 0) {
      return true;
    }
    int64_t ts_us = 0;
    if (auto ti = mbuf.try_get_i64(0, "ts"_hk)) {
      ts_us = *ti;
    } else if (auto tf = mbuf.try_get_f64(0, "ts"_hk)) {
      ts_us = static_cast<int64_t>(*tf);
    } else {
      return false;
    }
    if (now_wall_us < ts_us) {
      return false;
    }
    uint64_t age_us =
        static_cast<uint64_t>(now_wall_us) - static_cast<uint64_t>(ts_us);
    uint64_t age_ms = age_us / 1000ULL;
    return age_ms >= tombstone_ttl_ms;
  }

  Timestamp read_meta_timestamp_locked(Shard &mshard,
                                       std::string_view mkey) const {
    using namespace lite3cpp::literals;
    auto it = mshard.map.find(mkey);
    if (it == mshard.map.end() || !it->second ||
        it->second->buf_.size() == 0) {
      return {0, 0, 0};
    }

    std::vector<uint8_t> decomp;
    lite3cpp::Buffer mbuf;
    if (it->second->compressed_) {
      decomp.resize(it->second->original_size_);
      if (!zstd_manager_->decompress_into(it->second->buf_.data(),
                                          it->second->buf_.size(),
                                          decomp.data(), decomp.size())) {
        return {0, 0, 0};
      }
      mbuf = lite3cpp::Buffer::from_view(decomp.data(), decomp.size());
    } else {
      mbuf = lite3cpp::Buffer::from_view(it->second->buf_.data(),
                                         it->second->buf_.size());
    }
    if (mbuf.size() < lite3cpp::config::node_size) {
      return {0, 0, 0};
    }

    int64_t w = 0;
    if (auto wi = mbuf.try_get_i64(0, "ts"_hk)) {
      w = *wi;
    } else if (auto wf = mbuf.try_get_f64(0, "ts"_hk)) {
      w = static_cast<int64_t>(*wf);
    } else {
      return {0, 0, 0};
    }

    uint32_t l = 0;
    if (auto li = mbuf.try_get_i64(0, "l"_hk)) {
      l = static_cast<uint32_t>(*li);
    } else if (auto lf = mbuf.try_get_f64(0, "l"_hk)) {
      l = static_cast<uint32_t>(*lf);
    }

    uint32_t n = 0;
    if (auto ni = mbuf.try_get_i64(0, "n"_hk)) {
      n = static_cast<uint32_t>(*ni);
    } else if (auto nf = mbuf.try_get_f64(0, "n"_hk)) {
      n = static_cast<uint32_t>(*nf);
    }

    return {w, l, n};
  }

  void upsert_blob_locked(Shard &s, std::string_view key,
                          std::shared_ptr<const Blob> new_blob) {
    uint64_t old_h = 0;
    const uint64_t new_h = hash_blob(new_blob);
    auto it = s.map.lower_bound(key);
    if (it != s.map.end() && it->first == key) {
      old_h = hash_blob(it->second);
      it->second = std::move(new_blob);
    } else {
      s.map.emplace_hint(it, std::string(key), std::move(new_blob));
    }
    if (old_h != new_h) {
      merkle_.apply_delta(key, old_h ^ new_h);
    }
  }

  void apply_patch_int_locked(Shard &s, std::string_view key,
                              std::string_view field, int64_t val) {
    uint64_t old_h = 0;
    std::shared_ptr<Blob> new_blob;
    auto it = s.map.lower_bound(key);
    const bool found = (it != s.map.end() && it->first == key);
    if (found) {
      old_h = hash_blob(it->second);
      if (it->second && it->second->buf_.size() > 0) {
        new_blob = std::make_shared<Blob>(*it->second);
      } else {
        new_blob = std::make_shared<Blob>(&s.pool, 0);
      }
    } else {
      new_blob = std::make_shared<Blob>(&s.pool, 0);
    }

    const bool is_meta = key.ends_with(":meta");
    new_blob->set_int(field, val, is_meta ? nullptr : zstd_manager_.get());
    const uint64_t new_h = hash_blob(new_blob);
    if (found) {
      it->second = std::move(new_blob);
    } else {
      s.map.emplace_hint(it, std::string(key), std::move(new_blob));
    }
    if (old_h != new_h) {
      merkle_.apply_delta(key, old_h ^ new_h);
    }
  }

  void apply_patch_str_locked(Shard &s, std::string_view key,
                              std::string_view field, std::string_view val) {
    uint64_t old_h = 0;
    std::shared_ptr<Blob> new_blob;
    auto it = s.map.lower_bound(key);
    const bool found = (it != s.map.end() && it->first == key);
    if (found) {
      old_h = hash_blob(it->second);
      if (it->second && it->second->buf_.size() > 0) {
        new_blob = std::make_shared<Blob>(*it->second);
      } else {
        new_blob = std::make_shared<Blob>(&s.pool, 0);
      }
    } else {
      new_blob = std::make_shared<Blob>(&s.pool, 0);
    }

    const bool is_meta = key.ends_with(":meta");
    new_blob->set_str(field, val, is_meta ? nullptr : zstd_manager_.get());
    const uint64_t new_h = hash_blob(new_blob);
    if (found) {
      it->second = std::move(new_blob);
    } else {
      s.map.emplace_hint(it, std::string(key), std::move(new_blob));
    }
    if (old_h != new_h) {
      merkle_.apply_delta(key, old_h ^ new_h);
    }
  }

  bool apply_del_locked(Shard &s, std::string_view key) {
    upsert_blob_locked(s, key, get_empty_blob());
    return true;
  }

  void apply_system_hooks(std::string_view key, std::string_view raw_input,
                          const Blob &stored_blob) {
    using namespace lite3cpp::literals;
    if (key.starts_with("sys:u:")) {
      std::string_view uid_sv = key.substr(6);
      uint32_t uid = 0;
      auto [ptr, ec] =
          std::from_chars(uid_sv.data(), uid_sv.data() + uid_sv.size(), uid);
      if (ec == std::errc{} && ptr == uid_sv.data() + uid_sv.size()) {
        std::vector<uint8_t> decomp;
        lite3cpp::Buffer view_buf;
        if (stored_blob.compressed_) {
          decomp.resize(stored_blob.original_size_);
          if (zstd_manager_->decompress_into(stored_blob.buf_.data(),
                                             stored_blob.buf_.size(),
                                             decomp.data(), decomp.size())) {
            view_buf = lite3cpp::Buffer::from_view(decomp.data(), decomp.size());
          }
        } else if (stored_blob.buf_.size() >= lite3cpp::config::node_size) {
          view_buf = lite3cpp::Buffer::from_view(stored_blob.buf_.data(),
                                                 stored_blob.buf_.size());
        }
        if (view_buf.size() >= lite3cpp::config::node_size) {
          auto uname = view_buf.try_get_str(0, "name"_hk);
          auto pubkey = view_buf.try_get_str(0, "public_key"_hk);
          if (uname && !uname->empty() && pubkey && !pubkey->empty()) {
            credentials_->register_user(uid, std::string(*uname),
                                        std::string(*pubkey));
          }
        }
      }
    } else if (key.starts_with("sys:acl:")) {
      // Format: sys:acl:{uid}:{prefix}
      std::string_view sub = key.substr(8);
      size_t colon = sub.find(':');
      if (colon != std::string_view::npos) {
        std::string_view uid_sv = sub.substr(0, colon);
        uint32_t uid = 0;
        auto [ptr, ec] =
            std::from_chars(uid_sv.data(), uid_sv.data() + uid_sv.size(), uid);
        if (ec == std::errc{} && ptr == uid_sv.data() + uid_sv.size()) {
          std::string prefix(sub.substr(colon + 1));
          Permission perm = Permission::NONE;
          if (raw_input.find("READ") != std::string_view::npos) {
            perm = perm | Permission::READ;
          }
          if (raw_input.find("WRITE") != std::string_view::npos) {
            perm = perm | Permission::WRITE;
          }
          if (raw_input.find("ADMIN") != std::string_view::npos) {
            perm = perm | Permission::ADMIN;
          }
          credentials_->set_acl(uid, prefix, perm);
        }
      }
    }
  }

public:
  void apply_put(std::string_view key, std::string_view json_body) {
    const bool is_meta = key.ends_with(":meta");
    const bool is_sys = key.starts_with("sys:");
    auto new_blob = std::make_shared<Blob>(json_body, zstd_manager_.get(),
                                           nullptr, !is_meta);
    std::shared_ptr<const Blob> sys_ref;
    if (is_sys) {
      sys_ref = new_blob;
    }

    auto &s = get_shard(key);
    {
      std::unique_lock<std::shared_mutex> lock(s.read_mu);
      upsert_blob_locked(s, key, std::move(new_blob));
    }
    if (is_sys && sys_ref) {
      apply_system_hooks(key, json_body, *sys_ref);
    }
  }

  void apply_patch_int(std::string_view key, std::string_view field,
                       int64_t val) {
    auto &s = get_shard(key);
    std::unique_lock<std::shared_mutex> lock(s.read_mu);
    apply_patch_int_locked(s, key, field, val);
  }

  void apply_patch_str(std::string_view key, std::string_view field,
                       std::string_view val) {
    auto &s = get_shard(key);
    std::unique_lock<std::shared_mutex> lock(s.read_mu);
    apply_patch_str_locked(s, key, field, val);
  }

  bool apply_del(std::string_view key) {
    auto &s = get_shard(key);
    std::unique_lock<std::shared_mutex> lock(s.read_mu);
    return apply_del_locked(s, key);
  }

public:
  Engine(std::string wal_path, uint32_t node_id = 1) : clock_(node_id) {
    wal_ = std::make_unique<WriteAheadLog>(wal_path);
    zstd_manager_ = std::make_unique<ZstdManager>();
    credentials_ = std::make_unique<CredentialManager>(this);
    credentials_->register_user(ADMIN_UID, "admin", "admin-key");
    credentials_->set_acl(ADMIN_UID, "*", Permission::ADMIN);
    credentials_->register_user(INTERNAL_UID, "internal", "internal-key");
    credentials_->set_acl(INTERNAL_UID, "*", Permission::ADMIN);

    for (size_t i = 0; i < SHARDS; ++i) {
      shards_.push_back(std::make_unique<Shard>());
    }

    wal_->recover(
        [this](WalOp op, std::string_view key, std::string_view payload) {
          try {
            if (op == WalOp::PUT) {
              apply_put(key, payload);
            } else if (op == WalOp::PATCH_I64) {
              std::string_view field;
              int64_t val = 0;
              if (decode_wal_patch_i64(payload, field, val)) {
                apply_patch_int(key, field, val);
              }
            } else if (op == WalOp::PATCH_STR) {
              std::string_view field;
              std::string_view val;
              if (decode_wal_patch_str(payload, field, val)) {
                apply_patch_str(key, field, val);
              }
            } else if (op == WalOp::DELETE_) {
              apply_del(key);
            }
          } catch (const std::exception &e) {
            std::cerr << "WAL Recovery Skip: " << e.what() << "\n";
          }
        });

    {
      size_t total_live_keys = 0;
      for (size_t i = 0; i < SHARDS; ++i) {
        auto &s = *shards_[i];
        for (auto it = s.map.begin(); it != s.map.end();) {
          if (!it->second || it->second->buf_.size() == 0) {
            if (it->second) {
              uint64_t h = hash_blob(it->second);
              if (h != 0) {
                merkle_.apply_delta(it->first, h);
              }
            }
            it = s.map.erase(it);
          } else {
            ++total_live_keys;
            ++it;
          }
        }
      }
      uint64_t wal_bytes = wal_ ? wal_->file_size() : 0;
      size_t rec_ops = wal_ ? wal_->last_recovered_ops() : 0;
      if (wal_ &&
          ((wal_bytes > 32ULL * 1024 * 1024 && rec_ops > total_live_keys * 2) ||
           (wal_bytes > 256ULL * 1024 * 1024 &&
            rec_ops > total_live_keys + 50000))) {
        compact_wal_unlocked();
      }
    }

    for (size_t i = 0; i < SHARDS; ++i) {
      shards_[i]->core_thread = std::thread([this, i]() {
#ifdef _WIN32
        HANDLE hThread = GetCurrentThread();
        DWORD_PTR mask =
            (DWORD_PTR)1 << (i % std::thread::hardware_concurrency());
        SetThreadAffinityMask(hThread, mask);
#endif
        auto &s = get_shard_by_index(i);
        CoreMessage msg;
        while (!s.stop_flag.load(std::memory_order_relaxed) ||
               s.messages.size_approx() > 0) {
          if (s.messages.wait_dequeue_timed(msg,
                                            std::chrono::milliseconds(50))) {
            if (msg.task) {
              try {
                msg.task();
              } catch (...) {
              }
              async_enqueued_.fetch_sub(1, std::memory_order_release);
            }
          }
        }
      });
    }
  }

  ~Engine() {
    for (size_t i = 0; i < SHARDS; ++i) {
      shards_[i]->stop_flag.store(true, std::memory_order_release);
      shards_[i]->messages.enqueue({});
      if (shards_[i]->core_thread.joinable()) {
        shards_[i]->core_thread.join();
      }
    }
  }

  Shard &get_shard_by_index(size_t index) { return *shards_[index]; }

  template <typename Func>
  std::future<typename std::invoke_result_t<Func>>
  submit_to_shard_idx(size_t h, Func &&f) {
    using ReturnType = typename std::invoke_result_t<Func>;
    auto &s = *shards_[h];

    auto p = std::make_shared<std::promise<ReturnType>>();
    auto fut = p->get_future();

    async_enqueued_.fetch_add(1, std::memory_order_release);
    s.messages.enqueue({[p, f = std::forward<Func>(f)]() mutable {
      try {
        if constexpr (std::is_void_v<ReturnType>) {
          f();
          p->set_value();
        } else {
          p->set_value(f());
        }
      } catch (...) {
        p->set_exception(std::current_exception());
      }
    }});
    return fut;
  }

  template <typename Func> void submit_async(std::string_view key, Func &&f) {
    size_t h = get_routing_shard(key);
    auto &s = *shards_[h];
    async_enqueued_.fetch_add(1, std::memory_order_release);
    s.messages.enqueue({[f = std::forward<Func>(f)]() mutable { f(); }});
  }

  template <typename Func>
  auto submit_to_shard(const std::string &key, Func &&f) {
    size_t h = get_routing_shard(key);
    return submit_to_shard_idx(h, std::forward<Func>(f)).get();
  }

  std::shared_ptr<const Blob> get_view(std::string_view key) {
    size_t h = get_routing_shard(key);
    auto &s = *shards_[h];
    std::shared_lock<std::shared_mutex> lock(s.read_mu);
    if (auto it = s.map.find(key); it != s.map.end()) {
      return it->second;
    }
    return nullptr;
  }

  std::shared_ptr<const Blob> get_blob(std::string_view key,
                                       uint32_t principal_id = INTERNAL_UID) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::READ) && !(perm & Permission::ADMIN)) {
        return nullptr;
      }
    }

    auto view = get_view(key);
    if (!view || view->buf_.size() == 0) {
      return nullptr;
    }
    if (!view->compressed_) {
      return view;
    }

    std::vector<uint8_t> raw(view->original_size_);
    if (!zstd_manager_->decompress_into(view->buf_.data(), view->buf_.size(),
                                        raw.data(), raw.size())) {
      return nullptr;
    }
    auto out = std::make_shared<Blob>();
    out->buf_ = lite3cpp::Buffer(std::move(raw));
    out->compressed_ = false;
    out->original_size_ = static_cast<uint32_t>(out->buf_.size());
    out->content_hash_ = view->content_hash_;
    return out;
  }

  lite3cpp::Buffer get(std::string_view key,
                       uint32_t principal_id = ADMIN_UID) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::READ) && !(perm & Permission::ADMIN)) {
        return {};
      }
    }

    auto view = get_view(key);
    if (!view || view->buf_.size() == 0) {
      return {};
    }

    if (view->compressed_) {
      std::vector<uint8_t> raw(view->original_size_);
      if (!zstd_manager_->decompress_into(view->buf_.data(), view->buf_.size(),
                                          raw.data(), raw.size())) {
        return {};
      }
      return lite3cpp::Buffer(std::move(raw));
    }
    return view->buf_;
  }

  void put(std::string key, std::string json_body,
           uint32_t principal_id = ADMIN_UID) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
        throw std::runtime_error("Unauthorized: Access Denied for key " + key);
      }
    }

    const bool is_meta = key.ends_with(":meta");
    const bool is_sys = key.starts_with("sys:");
    auto new_blob = std::make_shared<Blob>(json_body, zstd_manager_.get(),
                                           nullptr, !is_meta);
    std::shared_ptr<const Blob> sys_ref;
    if (is_sys) {
      sys_ref = new_blob;
    }

    std::shared_lock<std::shared_mutex> comp_lk(compaction_mu_);

    std::string_view mkey_v = KeyBuilder::meta_key(key);
    const size_t k_idx = get_routing_shard(key);
    const size_t m_idx = get_routing_shard(mkey_v);

    {
      ShardPairLock slk(shards_, k_idx, m_idx);

      auto now = next_timestamp();
      auto &mbuf = format_meta_tl(now, false);
      std::string_view meta_sv(reinterpret_cast<const char *>(mbuf.data()),
                               mbuf.size());

      thread_local std::vector<BatchOp> batch;
      batch.clear();
      batch.push_back({WalOp::PUT, key, json_body});
      batch.push_back({WalOp::PUT, mkey_v, meta_sv});
      wal_->append_batch(batch);

      auto meta_blob = make_meta_blob(mbuf);
      upsert_blob_locked(*shards_[k_idx], key, std::move(new_blob));
      upsert_blob_locked(*shards_[m_idx], mkey_v, std::move(meta_blob));
    }

    if (is_sys && sys_ref) {
      apply_system_hooks(key, json_body, *sys_ref);
    }
  }

  void patch_int(std::string key, std::string field, int64_t val,
                 uint32_t principal_id = ADMIN_UID) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
        throw std::runtime_error("Unauthorized: Access Denied for key " + key);
      }
    }

    std::string wal_patch = encode_wal_patch_i64(field, val);
    std::shared_lock<std::shared_mutex> comp_lk(compaction_mu_);

    std::string_view mkey_v = KeyBuilder::meta_key(key);
    const size_t k_idx = get_routing_shard(key);
    const size_t m_idx = get_routing_shard(mkey_v);

    ShardPairLock slk(shards_, k_idx, m_idx);

    auto now = next_timestamp();
    auto &mbuf = format_meta_tl(now, false);
    std::string_view meta_sv(reinterpret_cast<const char *>(mbuf.data()),
                             mbuf.size());

    thread_local std::vector<BatchOp> batch;
    batch.clear();
    batch.push_back({WalOp::PATCH_I64, key, wal_patch});
    batch.push_back({WalOp::PUT, mkey_v, meta_sv});
    wal_->append_batch(batch);

    auto meta_blob = make_meta_blob(mbuf);
    apply_patch_int_locked(*shards_[k_idx], key, field, val);
    upsert_blob_locked(*shards_[m_idx], mkey_v, std::move(meta_blob));
  }

  void patch_str(std::string key, std::string field, std::string val,
                 uint32_t principal_id = ADMIN_UID) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
        throw std::runtime_error("Unauthorized: Access Denied for key " + key);
      }
    }

    std::string wal_patch = encode_wal_patch_str(field, val);
    std::shared_lock<std::shared_mutex> comp_lk(compaction_mu_);

    std::string_view mkey_v = KeyBuilder::meta_key(key);
    const size_t k_idx = get_routing_shard(key);
    const size_t m_idx = get_routing_shard(mkey_v);

    ShardPairLock slk(shards_, k_idx, m_idx);

    auto now = next_timestamp();
    auto &mbuf = format_meta_tl(now, false);
    std::string_view meta_sv(reinterpret_cast<const char *>(mbuf.data()),
                             mbuf.size());

    thread_local std::vector<BatchOp> batch;
    batch.clear();
    batch.push_back({WalOp::PATCH_STR, key, wal_patch});
    batch.push_back({WalOp::PUT, mkey_v, meta_sv});
    wal_->append_batch(batch);

    auto meta_blob = make_meta_blob(mbuf);
    apply_patch_str_locked(*shards_[k_idx], key, field, val);
    upsert_blob_locked(*shards_[m_idx], mkey_v, std::move(meta_blob));
  }

  bool del(const std::string &key, uint32_t principal_id = ADMIN_UID) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
        return false;
      }
    }

    std::shared_lock<std::shared_mutex> comp_lk(compaction_mu_);

    std::string_view mkey_v = KeyBuilder::meta_key(key);
    const size_t k_idx = get_routing_shard(key);
    const size_t m_idx = get_routing_shard(mkey_v);

    ShardPairLock slk(shards_, k_idx, m_idx);

    auto now = next_timestamp();
    auto &mbuf = format_meta_tl(now, true);
    std::string_view meta_sv(reinterpret_cast<const char *>(mbuf.data()),
                             mbuf.size());

    thread_local std::vector<BatchOp> batch;
    batch.clear();
    batch.push_back({WalOp::DELETE_, key, ""});
    batch.push_back({WalOp::PUT, mkey_v, meta_sv});
    wal_->append_batch(batch);

    auto meta_blob = make_meta_blob(mbuf);
    bool existed = apply_del_locked(*shards_[k_idx], key);
    upsert_blob_locked(*shards_[m_idx], mkey_v, std::move(meta_blob));
    return existed;
  }

  void batch_put(const lite3cpp::Buffer &batch,
                 uint32_t principal_id = ADMIN_UID) {
    if (batch.size() < lite3cpp::config::node_size) {
      return;
    }

    struct PreparedPut {
      std::string_view key;
      std::string_view val;
      std::string mkey;
      std::shared_ptr<Blob> val_blob;
      std::shared_ptr<Blob> meta_blob;
      size_t k_shard;
      size_t m_shard;
    };

    std::vector<PreparedPut> prepared;
    size_t root = 0;
    for (auto it = batch.begin(root), end = batch.end(root); it != end; ++it) {
      std::string_view key_sv = it->key;
      if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
        auto perm = credentials_->check_permission(principal_id, key_sv);
        if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
          throw std::runtime_error("Unauthorized: Access Denied for key " +
                                   std::string(key_sv));
        }
      }

      lite3cpp::HashedKey hk(key_sv);
      std::string_view val_sv;
      if (auto s = batch.try_get_str(root, hk)) {
        val_sv = *s;
      } else if (auto b = batch.try_get_bytes(root, hk)) {
        val_sv = std::string_view(reinterpret_cast<const char *>(b->data()),
                                  b->size());
      } else {
        continue;
      }

      const bool is_meta = key_sv.ends_with(":meta");
      auto vblob = std::make_shared<Blob>(val_sv, zstd_manager_.get(), nullptr,
                                          !is_meta);
      auto now = next_timestamp();
      auto &mbuf = format_meta_tl(now, false);
      auto mblob = make_meta_blob(mbuf);
      std::string mkey_s(KeyBuilder::meta_key(key_sv));
      const size_t k_idx = get_routing_shard(key_sv);
      const size_t m_idx = get_routing_shard(mkey_s);

      prepared.push_back({key_sv, val_sv, std::move(mkey_s), std::move(vblob),
                          std::move(mblob), k_idx, m_idx});
    }

    if (prepared.empty()) {
      return;
    }

    std::shared_lock<std::shared_mutex> comp_lk(compaction_mu_);

    std::vector<BatchOp> wal_batch;
    wal_batch.reserve(prepared.size() * 2);
    std::array<bool, SHARDS> touched{};
    for (const auto &item : prepared) {
      wal_batch.push_back({WalOp::PUT, item.key, item.val});
      std::string_view mval_sv(
          reinterpret_cast<const char *>(item.meta_blob->buf_.data()),
          item.meta_blob->buf_.size());
      wal_batch.push_back({WalOp::PUT, item.mkey, mval_sv});
      touched[item.k_shard] = true;
      touched[item.m_shard] = true;
    }

    std::vector<std::unique_lock<std::shared_mutex>> shard_locks;
    shard_locks.reserve(SHARDS);
    for (size_t i = 0; i < SHARDS; ++i) {
      if (touched[i]) {
        shard_locks.emplace_back(shards_[i]->read_mu);
      }
    }

    wal_->append_batch(wal_batch);

    for (auto &item : prepared) {
      const bool is_sys = item.key.starts_with("sys:");
      std::shared_ptr<const Blob> sys_ref = is_sys ? item.val_blob : nullptr;
      upsert_blob_locked(*shards_[item.k_shard], item.key,
                         std::move(item.val_blob));
      upsert_blob_locked(*shards_[item.m_shard], item.mkey,
                         std::move(item.meta_blob));
      if (is_sys && sys_ref) {
        apply_system_hooks(item.key, item.val, *sys_ref);
      }
    }
  }

  lite3cpp::Buffer batch_get(const std::vector<std::string> &keys,
                             uint32_t principal_id = ADMIN_UID) {
    lite3cpp::Buffer res;
    res.init_object();
    for (const auto &k : keys) {
      if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
        auto perm = credentials_->check_permission(principal_id, k);
        if (!(perm & Permission::READ) && !(perm & Permission::ADMIN)) {
          continue;
        }
      }

      auto view = get_view(k);
      if (!view || view->buf_.size() == 0) {
        continue;
      }

      lite3cpp::HashedKey hk(k);
      if (view->compressed_) {
        auto slot = res.set_bytes_uninitialized(0, hk, view->original_size_);
        if (!slot.empty()) {
          zstd_manager_->decompress_into(
              view->buf_.data(), view->buf_.size(),
              reinterpret_cast<char *>(slot.data()), slot.size());
        }
      } else {
        auto v = view->view();
        auto slot = res.set_bytes_uninitialized(0, hk, v.size());
        if (!slot.empty()) {
          std::memcpy(slot.data(), v.data(), v.size());
        }
      }
    }
    return res;
  }

  struct ShardMutation {
    std::vector<std::pair<std::string, std::string>> puts;
    std::vector<std::string> dels;
  };

  void apply_batch_mutations(const std::vector<BatchOp> &wal_ops,
                             std::map<size_t, ShardMutation> shard_mutations) {
    if (wal_ops.empty() && shard_mutations.empty()) {
      return;
    }

    std::shared_lock<std::shared_mutex> comp_lk(compaction_mu_);

    if (!wal_ops.empty() && wal_) {
      wal_->append_batch(wal_ops);
    }

    for (auto &[shard_idx, mut] : shard_mutations) {
      if (shard_idx >= SHARDS) {
        continue;
      }
      std::vector<std::pair<std::string, std::shared_ptr<Blob>>> prepared_puts;
      prepared_puts.reserve(mut.puts.size());
      for (auto &p : mut.puts) {
        const bool is_meta = p.first.ends_with(":meta");
        prepared_puts.emplace_back(
            std::move(p.first),
            std::make_shared<Blob>(p.second, zstd_manager_.get(), nullptr,
                                   !is_meta));
      }
      auto &s = *shards_[shard_idx];
      std::unique_lock<std::shared_mutex> lock(s.read_mu);
      for (const auto &d : mut.dels) {
        apply_del_locked(s, d);
      }
      for (auto &[pk, pblob] : prepared_puts) {
        upsert_blob_locked(s, pk, std::move(pblob));
      }
    }
  }

  inline void apply_mutation(const Mutation &m) {
    clock_.update(m.timestamp);
    clock_epoch_.fetch_add(1, std::memory_order_release);

    std::string_view val_sv;
    std::shared_ptr<Blob> prebuilt_val;
    if (!m.is_delete) {
      val_sv = std::string_view(reinterpret_cast<const char *>(m.value.data()),
                                m.value.size());
      const bool is_meta = m.key.ends_with(":meta");
      prebuilt_val = std::make_shared<Blob>(val_sv, zstd_manager_.get(),
                                            nullptr, !is_meta);
    }

    std::shared_lock<std::shared_mutex> comp_lk(compaction_mu_);

    std::string_view mkey_v = KeyBuilder::meta_key(m.key);
    const size_t k_idx = get_routing_shard(m.key);
    const size_t m_idx = get_routing_shard(mkey_v);

    std::shared_ptr<const Blob> sys_ref;
    const bool is_sys = (!m.is_delete && m.key.starts_with("sys:"));
    if (is_sys) {
      sys_ref = prebuilt_val;
    }

    {
      ShardPairLock slk(shards_, k_idx, m_idx);

      Timestamp local_ts = read_meta_timestamp_locked(*shards_[m_idx], mkey_v);
      if (m.timestamp <= local_ts) {
        std::cerr << "[Store] Rejecting mutation for " << m.key
                  << " (Stale). Inc: " << m.timestamp.wall_time
                  << " Local: " << local_ts.wall_time << "\n";
        return;
      }

      auto &mbuf = format_meta_tl(m.timestamp, m.is_delete);
      std::string_view meta_sv(reinterpret_cast<const char *>(mbuf.data()),
                               mbuf.size());

      thread_local std::vector<BatchOp> wal_batch;
      wal_batch.clear();
      if (m.is_delete) {
        wal_batch.push_back({WalOp::DELETE_, m.key, ""});
      } else {
        wal_batch.push_back({WalOp::PUT, m.key, val_sv});
      }
      wal_batch.push_back({WalOp::PUT, mkey_v, meta_sv});

      wal_->append_batch(wal_batch);

      auto meta_blob = make_meta_blob(mbuf);
      if (m.is_delete) {
        apply_del_locked(*shards_[k_idx], m.key);
      } else {
        upsert_blob_locked(*shards_[k_idx], m.key, std::move(prebuilt_val));
      }
      upsert_blob_locked(*shards_[m_idx], mkey_v, std::move(meta_blob));
    }

    if (is_sys && sys_ref) {
      apply_system_hooks(m.key, val_sv, *sys_ref);
    }
  }

  void set_logger(std::shared_ptr<ILogger> logger) { logger_ = logger; }
  ZstdManager *get_zstd_manager() const { return zstd_manager_.get(); }
  void flush() { wal_->flush(); }

  bool compact_wal_unlocked(
      uint64_t tombstone_ttl_ms = 24ULL * 3600ULL * 1000ULL) {
    if (!wal_) {
      return false;
    }
    uint64_t old_bytes = wal_->file_size();
    size_t old_ops = wal_->last_recovered_ops();
    const int64_t now_wall_us = clock_.now().wall_time;
    size_t shard_idx = 0;
    auto it = shards_[0]->map.begin();
    size_t live_keys = 0;

    using MapIter = decltype(shards_[0]->map.begin());
    struct DeferredErasure {
      size_t shard;
      MapIter it;
      uint64_t hash;
    };
    std::vector<DeferredErasure> deferred_erasures;

    bool ok = wal_->compact_streaming(
        [&](std::vector<std::pair<std::string, std::string>> &out_chunk)
            -> bool {
          out_chunk.clear();
          size_t chunk_bytes = 0;
          while (shard_idx < SHARDS) {
            auto &s = *shards_[shard_idx];
            while (it != s.map.end()) {
              if (!it->second || it->second->buf_.size() == 0) {
                uint64_t h = it->second ? hash_blob(it->second) : 0;
                deferred_erasures.push_back({shard_idx, it, h});
                ++it;
                continue;
              }
              std::string val_str;
              if (it->second->compressed_) {
                val_str.resize(it->second->original_size_);
                if (!zstd_manager_->decompress_into(
                        it->second->buf_.data(), it->second->buf_.size(),
                        val_str.data(), val_str.size())) {
                  return false;
                }
              } else {
                val_str.assign(
                    reinterpret_cast<const char *>(it->second->buf_.data()),
                    it->second->buf_.size());
              }

              if (is_expired_meta_tombstone(it->first, val_str, now_wall_us,
                                            tombstone_ttl_ms)) {
                std::string_view base_key(it->first.data(),
                                          it->first.size() - 5);
                auto &base_s = get_shard(base_key);
                auto base_it = base_s.map.find(base_key);
                bool base_live =
                    (base_it != base_s.map.end() && base_it->second &&
                     base_it->second->buf_.size() > 0);
                if (!base_live) {
                  uint64_t h = hash_blob(it->second);
                  deferred_erasures.push_back({shard_idx, it, h});
                  ++it;
                  continue;
                }
              }

              chunk_bytes += it->first.size() + val_str.size() + 16;
              out_chunk.emplace_back(it->first, std::move(val_str));
              ++live_keys;
              ++it;
              if (out_chunk.size() >= 5000 ||
                  chunk_bytes >= 16ULL * 1024 * 1024) {
                return true;
              }
            }
            ++shard_idx;
            if (shard_idx < SHARDS) {
              it = shards_[shard_idx]->map.begin();
            }
          }
          return !out_chunk.empty();
        });

    if (ok) {
      for (const auto &de : deferred_erasures) {
        if (de.hash != 0) {
          merkle_.apply_delta(de.it->first, de.hash);
        }
        shards_[de.shard]->map.erase(de.it);
      }
      std::fprintf(stderr,
                   "[WAL COMPACT] Compacted kv.wal from %llu bytes (%zu ops) "
                   "to %llu bytes (%zu live keys)\n",
                   static_cast<unsigned long long>(old_bytes), old_ops,
                   static_cast<unsigned long long>(wal_->file_size()),
                   live_keys);
    }
    return ok;
  }

  bool compact_wal(uint64_t tombstone_ttl_ms = 24ULL * 3600ULL * 1000ULL) {
    if (!wal_) {
      return false;
    }
    std::unique_lock<std::shared_mutex> comp_lk(compaction_mu_);
    wait_all_shards();
    std::vector<std::unique_lock<std::shared_mutex>> locks;
    locks.reserve(SHARDS);
    for (size_t i = 0; i < SHARDS; ++i) {
      locks.emplace_back(shards_[i]->read_mu);
    }
    return compact_wal_unlocked(tombstone_ttl_ms);
  }

  void wait_all_shards() {
    if (async_enqueued_.load(std::memory_order_acquire) == 0) {
      return;
    }
    std::vector<std::future<void>> futures;
    futures.reserve(SHARDS);
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
      auto &shard = *shards_[i];
      std::shared_lock<std::shared_mutex> lock(shard.read_mu);
      for (const auto &[k, v] : shard.map) {
        if (!v || v->buf_.size() == 0) {
          continue;
        }
        uint64_t kh = fnv1a_64(k.data(), k.size());
        uint32_t b = (kh >> 48) & 0xFFFF;
        if (b == static_cast<uint32_t>(bucket_idx)) {
          result.emplace_back(k, hash_blob(v));
        }
      }
    }
    return result;
  }

  std::vector<std::pair<std::string, std::string>>
  get_prefix_chunk(const std::string &prefix, size_t shard_idx,
                   const std::string &start_key, size_t limit) {
    std::vector<std::pair<std::string, std::string>> chunk;
    if (shard_idx >= SHARDS || limit == 0) {
      return chunk;
    }

    auto &s = *shards_[shard_idx];
    std::shared_lock<std::shared_mutex> lock(s.read_mu);
    auto it = (!start_key.empty() && start_key >= prefix)
                  ? s.map.upper_bound(start_key)
                  : s.map.lower_bound(prefix);
    while (it != s.map.end() && it->first.starts_with(prefix)) {
      if (it->first.ends_with(":meta") || !it->second ||
          it->second->buf_.size() == 0) {
        ++it;
        continue;
      }
      std::string vstr;
      if (it->second->compressed_) {
        vstr.resize(it->second->original_size_);
        if (!zstd_manager_->decompress_into(it->second->buf_.data(),
                                            it->second->buf_.size(),
                                            vstr.data(), vstr.size())) {
          ++it;
          continue;
        }
      } else {
        vstr.assign(reinterpret_cast<const char *>(it->second->buf_.data()),
                    it->second->buf_.size());
      }
      chunk.emplace_back(it->first, std::move(vstr));
      if (chunk.size() >= limit) {
        break;
      }
      ++it;
    }
    return chunk;
  }

  std::string get_max_key_with_prefix(const std::string &prefix) {
    std::string upper = prefix;
    while (!upper.empty() &&
           static_cast<unsigned char>(upper.back()) == 0xFFu) {
      upper.pop_back();
    }
    const bool has_upper = !upper.empty();
    if (has_upper) {
      upper.back() =
          static_cast<char>(static_cast<unsigned char>(upper.back()) + 1u);
    }

    std::string max_key;
    bool found = false;

    auto scan_shard = [&](Shard &s) {
      std::shared_lock<std::shared_mutex> lock(s.read_mu);
      auto it = has_upper ? s.map.lower_bound(upper) : s.map.end();
      while (it != s.map.begin()) {
        --it;
        if (!it->first.starts_with(prefix))
          break;
        if (it->first.ends_with(":meta"))
          continue;
        if (!it->second || it->second->buf_.size() == 0)
          continue;
        if (!found || it->first > max_key) {
          max_key = it->first;
          found = true;
        }
        break;
      }
    };

    if (has_complete_routing_tag(prefix)) {
      scan_shard(*shards_[get_routing_shard(prefix)]);
    } else {
      for (size_t i = 0; i < SHARDS; ++i) {
        scan_shard(*shards_[i]);
      }
    }
    return max_key;
  }

  std::vector<std::pair<std::string, std::string>>
  get_prefix_entries_all_shards(const std::string &prefix,
                                const std::string &start_key = "",
                                size_t limit = 100000) {
    if (limit == 0) {
      return {};
    }
    if (has_complete_routing_tag(prefix)) {
      return get_prefix_chunk(prefix, get_routing_shard(prefix), start_key,
                              limit);
    }

    std::vector<std::pair<std::string, std::string>> results;
    for (size_t i = 0; i < SHARDS; ++i) {
      auto chunk = get_prefix_chunk(prefix, i, start_key, limit);
      for (auto &entry : chunk) {
        results.push_back(std::move(entry));
      }
    }
    std::sort(results.begin(), results.end(),
              [](const auto &a, const auto &b) { return a.first < b.first; });
    if (results.size() > limit) {
      results.resize(limit);
    }
    return results;
  }

  std::vector<std::string> get_prefix_keys(const std::string &prefix,
                                           size_t shard_idx,
                                           const std::string &start_key,
                                           size_t limit) {
    std::vector<std::string> chunk;
    if (shard_idx >= SHARDS || limit == 0) {
      return chunk;
    }

    auto &s = *shards_[shard_idx];
    std::shared_lock<std::shared_mutex> lock(s.read_mu);
    auto it = (!start_key.empty() && start_key >= prefix)
                  ? s.map.upper_bound(start_key)
                  : s.map.lower_bound(prefix);
    while (it != s.map.end() && it->first.starts_with(prefix)) {
      if (!it->second || it->second->buf_.size() == 0 ||
          it->first.ends_with(":meta")) {
        ++it;
        continue;
      }
      chunk.push_back(it->first);
      if (chunk.size() >= limit) {
        break;
      }
      ++it;
    }
    return chunk;
  }

  std::vector<std::string>
  get_prefix_keys_all_shards(const std::string &prefix,
                             const std::string &start_key, size_t limit) {
    if (limit == 0) {
      return {};
    }
    if (has_complete_routing_tag(prefix)) {
      return get_prefix_keys(prefix, get_routing_shard(prefix), start_key,
                             limit);
    }

    std::vector<std::string> results;
    for (size_t i = 0; i < SHARDS; ++i) {
      auto chunk = get_prefix_keys(prefix, i, start_key, limit);
      for (auto &k : chunk) {
        results.push_back(std::move(k));
      }
    }
    std::sort(results.begin(), results.end());
    if (results.size() > limit) {
      results.resize(limit);
    }
    return results;
  }
};

} // namespace l3kv
