#pragma once
#include "KeyBuilder.hpp"
#include "btree_map.hpp"
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
  bool is_tombstone_ = false;
  bool has_hlc_ = false;
  uint32_t original_size_ = 0;
  uint64_t content_hash_ = 0;
  int64_t expires_at_us_ = 0;
  Timestamp hlc_{0, 0, 0};

public:
  static uint64_t non_zero_hash(const uint8_t *data, size_t len) noexcept {
    if (len == 0) {
      return 0;
    }
    uint64_t h = XXH3_64bits(data, len);
    return h == 0 ? 1 : h;
  }

  bool is_expired(int64_t now_us) const noexcept {
    return expires_at_us_ > 0 && now_us >= expires_at_us_;
  }

  bool is_tombstone_expired(int64_t now_wall_us,
                            uint64_t tombstone_ttl_ms) const noexcept {
    if (!is_tombstone_) {
      return false;
    }
    if (tombstone_ttl_ms == 0) {
      return true;
    }
    if (!has_hlc_ || now_wall_us < hlc_.wall_time) {
      return false;
    }
    uint64_t age_us = static_cast<uint64_t>(now_wall_us) -
                      static_cast<uint64_t>(hlc_.wall_time);
    return (age_us / 1000ULL) >= tombstone_ttl_ms;
  }
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
        content_hash_ = non_zero_hash(buf_.data(), buf_.size());
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
    content_hash_ = non_zero_hash(buf_.data(), buf_.size());
    if (zstd && buf_.size() >= 512) {
      zstd->add_sample(std::string_view(
          reinterpret_cast<const char *>(buf_.data()), buf_.size()));
      compress(zstd);
    }
  }

  bool set_int(std::string_view key, int64_t val, ZstdManager *zstd,
               bool recompress = true) {
    if (compressed_) {
      decompress(zstd);
    }
    if (compressed_) {
      return false;
    }
    if (buf_.size() == 0) {
      buf_.init_object();
    }
    buf_.set_i64(0, lite3cpp::HashedKey(key), val);
    original_size_ = static_cast<uint32_t>(buf_.size());
    content_hash_ = buf_.size() > 0 ? XXH3_64bits(buf_.data(), buf_.size()) : 0;
    if (recompress && zstd && buf_.size() >= 512) {
      compress(zstd);
    }
    return true;
  }

  bool set_str(std::string_view key, std::string_view val, ZstdManager *zstd,
               bool recompress = true) {
    if (compressed_) {
      decompress(zstd);
    }
    if (compressed_) {
      return false;
    }
    if (buf_.size() == 0) {
      buf_.init_object();
    }
    buf_.set_str(0, lite3cpp::HashedKey(key), val);
    original_size_ = static_cast<uint32_t>(buf_.size());
    content_hash_ = buf_.size() > 0 ? XXH3_64bits(buf_.data(), buf_.size()) : 0;
    if (recompress && zstd && buf_.size() >= 512) {
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
    BTreeMap<std::string, std::shared_ptr<const Blob>, std::less<>> map;

    // Legacy async queue retained for submit_to_shard / submit_async callers
    moodycamel::BlockingConcurrentQueue<CoreMessage> messages;
    std::atomic<bool> stop_flag{false};
    std::thread core_thread;

    Shard() : pool(std::pmr::new_delete_resource()) {
      wal_buf.reserve(64 * 1024);
    }
    mutable std::mutex write_mu;
    mutable std::shared_mutex read_mu;
    std::vector<uint8_t> wal_buf;
  };

  static constexpr size_t kShardWalFlushBytes = 64 * 1024;

  void append_wal_shard_locked(Shard &s, WalOp op, std::string_view key,
                               std::string_view payload) {
    WriteAheadLog::encode_record(s.wal_buf, op, key, payload);
    if (s.wal_buf.size() >= kShardWalFlushBytes) {
      wal_->append_prebuilt(s.wal_buf.data(), s.wal_buf.size());
      s.wal_buf.clear();
    }
  }

  void flush_shard_wal_locked(Shard &s) {
    if (!s.wal_buf.empty()) {
      wal_->append_prebuilt(s.wal_buf.data(), s.wal_buf.size());
      s.wal_buf.clear();
    }
  }

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
    if (key.size() >= 5 && key.ends_with(":meta")) {
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

  static int64_t now_epoch_us() noexcept {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  static lite3cpp::Buffer &format_meta_tl(const Timestamp &ts,
                                          bool tombstone = false,
                                          int64_t expires_at_us = 0) {
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
    if (expires_at_us > 0) {
      tl_mbuf.set_i64(0, "exp"_hk, expires_at_us);
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
    if (!blob || blob->is_tombstone_) {
      return 0;
    }
    return blob->content_hash();
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

  void upsert_blob_locked(Shard &s, std::string &&key,
                          std::shared_ptr<const Blob> new_blob) {
    uint64_t old_h = 0;
    const uint64_t new_h = hash_blob(new_blob);
    auto it = s.map.lower_bound(key);
    if (it != s.map.end() && it->first == key) {
      old_h = hash_blob(it->second);
      it->second = std::move(new_blob);
      if (old_h != new_h) {
        merkle_.apply_delta(key, old_h ^ new_h);
      }
    } else {
      auto inserted = s.map.emplace_hint(it, std::move(key), std::move(new_blob));
      if (old_h != new_h) {
        merkle_.apply_delta(inserted->first, old_h ^ new_h);
      }
    }
  }

  void apply_patch_int_locked(Shard &s, std::string_view key,
                              std::string_view field, int64_t val,
                              Timestamp hlc = {0, 0, 0},
                              bool has_hlc = false) {
    uint64_t old_h = 0;
    std::shared_ptr<Blob> new_blob;
    auto it = s.map.lower_bound(key);
    const bool found = (it != s.map.end() && it->first == key);
    if (found) {
      old_h = hash_blob(it->second);
      if (it->second && it->second->buf_.size() > 0 &&
          !it->second->is_tombstone_) {
        new_blob = std::make_shared<Blob>(*it->second);
      } else {
        new_blob = std::make_shared<Blob>(&s.pool, 0);
      }
    } else {
      new_blob = std::make_shared<Blob>(&s.pool, 0);
    }

    const bool is_meta = key.ends_with(":meta");
    if (!new_blob->set_int(field, val, zstd_manager_.get(), !is_meta)) {
      return;
    }
    new_blob->is_tombstone_ = false;
    if (has_hlc) {
      new_blob->hlc_ = hlc;
      new_blob->has_hlc_ = true;
    }
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
                              std::string_view field, std::string_view val,
                              Timestamp hlc = {0, 0, 0},
                              bool has_hlc = false) {
    uint64_t old_h = 0;
    std::shared_ptr<Blob> new_blob;
    auto it = s.map.lower_bound(key);
    const bool found = (it != s.map.end() && it->first == key);
    if (found) {
      old_h = hash_blob(it->second);
      if (it->second && it->second->buf_.size() > 0 &&
          !it->second->is_tombstone_) {
        new_blob = std::make_shared<Blob>(*it->second);
      } else {
        new_blob = std::make_shared<Blob>(&s.pool, 0);
      }
    } else {
      new_blob = std::make_shared<Blob>(&s.pool, 0);
    }

    const bool is_meta = key.ends_with(":meta");
    if (!new_blob->set_str(field, val, zstd_manager_.get(), !is_meta)) {
      return;
    }
    new_blob->is_tombstone_ = false;
    if (has_hlc) {
      new_blob->hlc_ = hlc;
      new_blob->has_hlc_ = true;
    }
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

  bool apply_del_locked(Shard &s, std::string_view key,
                        Timestamp hlc = {0, 0, 0}, bool has_hlc = false) {
    auto it = s.map.lower_bound(key);
    const bool found = (it != s.map.end() && it->first == key);
    const bool existed = (found && it->second && !it->second->is_tombstone_ &&
                          it->second->buf_.size() > 0);
    uint64_t old_h = found ? hash_blob(it->second) : 0;
    auto tomb = std::make_shared<Blob>();
    tomb->is_tombstone_ = true;
    tomb->has_hlc_ = has_hlc;
    tomb->hlc_ = hlc;
    const uint64_t new_h = hash_blob(tomb);
    if (found) {
      it->second = std::move(tomb);
    } else {
      s.map.emplace_hint(it, std::string(key), std::move(tomb));
    }
    if (old_h != new_h) {
      merkle_.apply_delta(key, old_h ^ new_h);
    }
    return existed;
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
    if (is_meta && key.size() > 5) {
      using namespace lite3cpp::literals;
      auto view_buf = lite3cpp::Buffer::from_view(
          reinterpret_cast<const uint8_t *>(json_body.data()),
          json_body.size());
      int64_t recovered_exp = 0;
      if (auto exp = view_buf.try_get_i64(0, "exp"_hk); exp && *exp > 0) {
        recovered_exp = *exp;
      }
      bool tomb = false;
      if (auto tb = view_buf.try_get_bool(0, "tombstone"_hk)) {
        tomb = *tb;
      }
      Timestamp ts{0, 0, 0};
      bool has_ts = false;
      if (auto wi = view_buf.try_get_i64(0, "ts"_hk)) {
        ts.wall_time = *wi;
        has_ts = true;
      }
      if (auto li = view_buf.try_get_i64(0, "l"_hk)) {
        ts.logical = static_cast<uint32_t>(*li);
      }
      if (auto ni = view_buf.try_get_i64(0, "n"_hk)) {
        ts.node_id = static_cast<uint32_t>(*ni);
      }
      std::string_view base = key.substr(0, key.size() - 5);
      auto &bs = get_shard(base);
      std::unique_lock<std::shared_mutex> lock(bs.read_mu);
      if (auto it = bs.map.find(base); it != bs.map.end() && it->second) {
        auto updated = std::make_shared<Blob>(*it->second);
        if (recovered_exp > 0) {
          updated->expires_at_us_ = recovered_exp;
        }
        if (has_ts) {
          updated->hlc_ = ts;
          updated->has_hlc_ = true;
        }
        if (tomb && updated->buf_.size() == 0) {
          updated->is_tombstone_ = true;
        }
        upsert_blob_locked(bs, base, std::move(updated));
      } else if (tomb) {
        auto tb = std::make_shared<Blob>();
        tb->is_tombstone_ = true;
        tb->hlc_ = ts;
        tb->has_hlc_ = has_ts;
        upsert_blob_locked(bs, base, std::move(tb));
      }
      return;
    }
    auto new_blob = std::make_shared<Blob>(json_body, zstd_manager_.get(),
                                           nullptr, true);
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
  Engine(std::string wal_path, uint32_t node_id = 1);
  ~Engine();

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

  std::shared_ptr<const Blob> get_view(std::string_view key);
  std::shared_ptr<const Blob> get_blob(std::string_view key,
                                       uint32_t principal_id = INTERNAL_UID);
  lite3cpp::Buffer get(std::string_view key,
                       uint32_t principal_id = ADMIN_UID);

  struct CasResult {
    bool ok{false};
    uint64_t new_hash{0};
    uint64_t actual_hash{0};
  };

  void put(std::string key, std::string json_body,
           uint32_t principal_id = ADMIN_UID, int64_t ttl_ms = 0);
  CasResult cas_put(std::string key, std::string payload,
                    uint64_t expected_hash, uint32_t principal_id = ADMIN_UID,
                    int64_t ttl_ms = 0);
  void patch_int(std::string key, std::string field, int64_t val,
                 uint32_t principal_id = ADMIN_UID);
  void patch_str(std::string key, std::string field, std::string val,
                 uint32_t principal_id = ADMIN_UID);
  bool del(const std::string &key, uint32_t principal_id = ADMIN_UID);
  void batch_put(const lite3cpp::Buffer &batch,
                 uint32_t principal_id = ADMIN_UID);
  lite3cpp::Buffer batch_get(const std::vector<std::string> &keys,
                             uint32_t principal_id = ADMIN_UID);

  struct ShardMutation {
    std::vector<std::pair<std::string, std::string>> puts;
    std::vector<std::string> dels;
  };

  void apply_batch_mutations(const std::vector<BatchOp> &wal_ops,
                             std::map<size_t, ShardMutation> shard_mutations);
  void apply_mutation(const Mutation &m);

  void set_logger(std::shared_ptr<ILogger> logger) { logger_ = logger; }
  ZstdManager *get_zstd_manager() const { return zstd_manager_.get(); }
  void flush();
  bool compact_wal_unlocked(
      uint64_t tombstone_ttl_ms = 24ULL * 3600ULL * 1000ULL,
      std::vector<std::shared_lock<std::shared_mutex>> *active_read_locks =
          nullptr);
  bool compact_wal(uint64_t tombstone_ttl_ms = 24ULL * 3600ULL * 1000ULL);
  void wait_all_shards();

  auto get_wal_stats() { return wal_->stats(); }
  uint64_t get_merkle_root_hash() { return merkle_.get_root_hash(); }
  uint64_t get_merkle_node(int level, int index) {
    return merkle_.get_node_hash(level, index);
  }

  std::vector<std::pair<std::string, uint64_t>> get_bucket_keys(int bucket_idx);
  std::vector<std::pair<std::string, std::string>>
  get_prefix_chunk(const std::string &prefix, size_t shard_idx,
                   const std::string &start_key, size_t limit);
  std::string get_max_key_with_prefix(const std::string &prefix);
  std::vector<std::pair<std::string, std::string>>
  get_prefix_entries_all_shards(const std::string &prefix,
                                const std::string &start_key = "",
                                size_t limit = 100000);
  std::vector<std::string> get_prefix_keys(const std::string &prefix,
                                           size_t shard_idx,
                                           const std::string &start_key,
                                           size_t limit);
  std::vector<std::string>
  get_prefix_keys_all_shards(const std::string &prefix,
                             const std::string &start_key, size_t limit);
  std::vector<std::pair<std::string, lite3cpp::Buffer>>
  scan_prefix_kv(const std::string &prefix, const std::string &start_key,
                 size_t limit, uint32_t principal_id = ADMIN_UID);
  size_t sweep_expired_keys(size_t max_per_shard = 256);
};

} // namespace l3kv
