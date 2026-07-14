#pragma once
#include "clock.hpp"
#include "merkle.hpp"
#include "replication_log.hpp"
#include "wal.hpp"
#include "KeyBuilder.hpp"
#include "credential_manager.hpp"
#include <nlohmann/json.hpp>

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <span>
#include <future>
#include <string>
#include <shared_mutex>
#include <vector>

#include "../../lib/concurrentqueue/concurrentqueue.h"

#include "buffer.hpp"
#include "json.hpp"

#include <atomic>
#include <mutex>
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

class ILogger {
public:
  virtual ~ILogger() = default;
  virtual void log(const std::string& msg) = 0;
};

class Engine {
  static constexpr size_t SHARDS = 8;
  std::shared_ptr<ILogger> logger_;
  struct CoreMessage {
    std::function<void()> task;
  };

  struct alignas(64) Shard {
    std::pmr::unsynchronized_pool_resource pool;
    std::map<std::string, std::shared_ptr<const Blob>> map;
    
    // Message passing
    moodycamel::ConcurrentQueue<CoreMessage> messages;
    std::atomic<bool> stop_flag{false};
    std::thread core_thread;

    Shard() : pool(std::pmr::new_delete_resource()) {}
    mutable std::shared_mutex read_mu;
  };

  std::vector<std::unique_ptr<Shard>> shards_;
  std::unique_ptr<WriteAheadLog> wal_;
  HybridLogicalClock clock_;
  MerkleTree merkle_;
  std::unique_ptr<ZstdManager> zstd_manager_;
  std::unique_ptr<CredentialManager> credentials_;


public:
  CredentialManager& credentials() { return *credentials_; }

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

  uint64_t hash_blob(const std::shared_ptr<const Blob> &blob) {
    if (!blob)
      return 0;
    auto v = blob->view();
    return fnv1a_64(v.data(), v.size());
  }

public:
  void apply_put(std::string_view key, std::string_view json_body) {
    if(1) std::fprintf(stderr, "[Store] apply_put: %.*s (len=%zu)\n", (int)key.size(), key.data(), json_body.size());
    auto &s = get_shard(std::string(key));
    std::unique_lock lock(s.read_mu);

     // if(0) std::fprintf(stderr, "[Store] apply_put: %.*s (body_len=%zu)\n", (int)key.size(), key.data(), json_body.size()); 

    uint64_t old_h = 0;
    auto it = s.map.find(std::string(key));
    if (it != s.map.end()) {
      old_h = hash_blob(it->second);
    }

    auto new_blob = std::make_shared<Blob>(&s.pool);
    new_blob->overwrite(std::string(json_body), zstd_manager_.get());
    uint64_t new_h = hash_blob(new_blob);
    
    s.map[std::string(key)] = std::move(new_blob);
    merkle_.apply_delta(std::string(key), old_h ^ new_h);

    // Foundational Security: Hook system keys to CredentialManager
    if (key.starts_with("sys:u:")) {
        try {
            uint32_t uid = std::stoul(std::string(key.substr(6)));
            auto j = nlohmann::json::parse(std::string(json_body));
            credentials_->register_user(uid, j.value("name", ""), j.value("public_key", ""));
        } catch (...) {}
    } else if (key.starts_with("sys:acl:")) {
        try {
            // Format: sys:acl:{uid}:{prefix}
            std::string sub = std::string(key.substr(8));
            size_t colon = sub.find(':');
            if (colon != std::string::npos) {
                uint32_t uid = std::stoul(sub.substr(0, colon));
                std::string prefix = sub.substr(colon + 1);
                
                Permission perm = Permission::NONE;
                if (json_body.find("READ") != std::string::npos) perm = perm | Permission::READ;
                if (json_body.find("WRITE") != std::string::npos) perm = perm | Permission::WRITE;
                if (json_body.find("ADMIN") != std::string::npos) perm = perm | Permission::ADMIN;
                
                credentials_->set_acl(uid, prefix, perm);
            }
        } catch (...) {}
    }
  }

  void apply_patch_int(std::string_view key, std::string_view field,
                       int64_t val) {
    auto &s = get_shard(std::string(key));
    std::unique_lock lock(s.read_mu);
    
    uint64_t old_h = 0;
    std::shared_ptr<Blob> new_blob;
    
    auto it = s.map.find(std::string(key));
    if (it != s.map.end()) {
      old_h = hash_blob(it->second);
      new_blob = std::make_shared<Blob>(*it->second);
    } else {
      new_blob = std::make_shared<Blob>(&s.pool);
    }

    new_blob->set_int(std::string(field), val, zstd_manager_.get());
    uint64_t new_h = hash_blob(new_blob);
    
    s.map[std::string(key)] = std::move(new_blob);
    merkle_.apply_delta(std::string(key), old_h ^ new_h);
  }

  void apply_patch_str(std::string_view key, std::string_view field,
                       std::string_view val) {
    auto &s = get_shard(std::string(key));
    std::unique_lock lock(s.read_mu);
    
    uint64_t old_h = 0;
    std::shared_ptr<Blob> new_blob;
    
    auto it = s.map.find(std::string(key));
    if (it != s.map.end()) {
      old_h = hash_blob(it->second);
      new_blob = std::make_shared<Blob>(*it->second);
    } else {
      new_blob = std::make_shared<Blob>(&s.pool);
    }

    new_blob->set_str(std::string(field), std::string(val), zstd_manager_.get());
    uint64_t new_h = hash_blob(new_blob);
    
    s.map[std::string(key)] = std::move(new_blob);
    merkle_.apply_delta(std::string(key), old_h ^ new_h);
  }

  bool apply_del(std::string_view key) {
    auto &s = get_shard(std::string(key));
    std::unique_lock lock(s.read_mu);

    uint64_t old_h = 0;
    auto it = s.map.find(std::string(key));
    if (it != s.map.end()) {
        old_h = hash_blob(it->second);
    }

    auto new_blob = std::make_shared<Blob>(&s.pool);
    new_blob->overwrite("", zstd_manager_.get());
    uint64_t new_h = hash_blob(new_blob);

    s.map[std::string(key)] = std::move(new_blob);
    merkle_.apply_delta(std::string(key), old_h ^ new_h);
    return true;
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

    for (size_t i = 0; i < SHARDS; ++i)
      shards_.push_back(std::make_unique<Shard>());

    wal_->recover(
        [this](WalOp op, std::string_view key, std::string_view payload) {
          try {
            if (op == WalOp::PUT) {
              apply_put(key, payload);
            } else if (op == WalOp::PATCH_I64) {
              std::string p(payload);
              size_t colon = p.find(':');
              if (colon != std::string::npos) {
                std::string field = p.substr(0, colon);
                int64_t val = std::stoll(p.substr(colon + 1));
                apply_patch_int(key, field, val);
              }
            } else if (op == WalOp::PATCH_STR) {
              std::string p(payload);
              size_t colon = p.find(':');
              if (colon != std::string::npos) {
                std::string field = p.substr(0, colon);
                std::string val = p.substr(colon + 1);
                apply_patch_str(key, field, val);
              }
            } else if (op == WalOp::DELETE_) {
              apply_del(key);
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
            msg.task();
          } else {
            if (spin < 1000) {
              spin++;
            } else {
              std::this_thread::sleep_for(std::chrono::microseconds(50));
              spin = 0;
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

  template <typename Func> void submit_async(std::string_view key, Func &&f) {
    size_t h = get_routing_shard(std::string(key));
    auto &s = *shards_[h];
    s.messages.enqueue({[f = std::forward<Func>(f)]() mutable { f(); }});
  }

  template <typename Func> auto submit_to_shard(const std::string &key, Func &&f) {
    size_t h = get_routing_shard(key);
    return submit_to_shard_idx(h, std::forward<Func>(f)).get();
  }

  std::shared_ptr<const Blob> get_view(const std::string &key) {
    size_t h = get_routing_shard(key);
    auto &s = *shards_[h];
    std::shared_ptr<const Blob> result;
    {
        std::shared_lock lock(s.read_mu);
        if (auto it = s.map.find(key); it != s.map.end()) {
            result = it->second;
        }
    }
    
    return result;
  }

  lite3cpp::Buffer get(const std::string &key, uint32_t principal_id = ADMIN_UID) {
    // Foundational Security: ACL Check
    auto perm = credentials_->check_permission(principal_id, key);
    if (!(perm & Permission::READ) && !(perm & Permission::ADMIN)) {
        return {}; // Access Denied
    }

    auto view = get_view(key);
    if (!view) return {};

    if (view->compressed_) {
        std::string src(reinterpret_cast<const char *>(view->buf_.data()), view->buf_.size());
        std::string dst;
        zstd_manager_->decompress(src, dst, view->original_size_);
        return lite3cpp::Buffer(std::vector<uint8_t>(dst.begin(), dst.end()));
    }
    return view->buf_;
  }

  void put(std::string key, std::string json_body, uint32_t principal_id = ADMIN_UID) {
    // Foundational Security: ACL Check
    auto perm = credentials_->check_permission(principal_id, key);
    if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
        throw std::runtime_error("Unauthorized: Access Denied for key " + key);
    }

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

    submit_to_shard(key, [this, key = std::move(key), json_body = std::move(json_body), mkey_s = std::move(mkey_s), meta_val = std::move(meta_val)]() mutable {
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

    submit_to_shard(key, [this, key = std::move(key), field = std::move(field), val, mkey_s = std::move(mkey_s), ts_str = std::move(ts_str)]() mutable {
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

    submit_to_shard(key, [this, key = std::move(key), field = std::move(field), val = std::move(val), mkey_s = std::move(mkey_s), ts_str = std::move(ts_str)]() mutable {
      apply_patch_str(key, field, val);
      apply_patch_str(mkey_s, field, ts_str);
    });
  }

  bool del(const std::string &key, uint32_t principal_id = ADMIN_UID) {
    // Foundational Security: ACL Check
    auto perm = credentials_->check_permission(principal_id, key);
    if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
        return false; // Access Denied
    }

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

  void batch_put(const lite3cpp::Buffer &batch, uint32_t principal_id = ADMIN_UID) {
    std::vector<BatchOp> wal_batch;
    struct ShardWork {
      std::vector<std::pair<std::string, std::string>> updates;
    };
    std::map<size_t, ShardWork> shard_works;

    size_t root = 0;
    for (auto it = batch.begin(root); it != batch.end(root); ++it) {
      std::string key(it->key);
      
      // Foundational Security: ACL Check
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
          throw std::runtime_error("Unauthorized: Access Denied for key " + key);
      }

      auto type = batch.get_type(root, key);
      std::string val;
      if (type == lite3cpp::Type::String) {
        val = batch.get_str(root, key);
      } else if (type == lite3cpp::Type::Bytes) {
        auto b = batch.get_bytes(root, key);
        val = std::string(reinterpret_cast<const char *>(b.data()), b.size());
      } else {
        continue;
      }

      auto now = clock_.now();
      std::string mkey_s(KeyBuilder::meta_key(key));
      std::string meta_val = "{\"ts\":" + std::to_string(now.wall_time) +
                             ",\"l\":" + std::to_string(now.logical) +
                             ",\"n\":" + std::to_string(now.node_id) + "}";

      wal_batch.push_back({WalOp::PUT, key, val});
      wal_batch.push_back({WalOp::PUT, mkey_s, meta_val});

      shard_works[get_routing_shard(key)].updates.push_back({key, val});
      shard_works[get_routing_shard(mkey_s)].updates.push_back({mkey_s, meta_val});
    }

    if (wal_batch.empty())
      return;

    wal_->append_batch(wal_batch);

    for (auto &[shard_idx, work] : shard_works) {
      auto &s = *shards_[shard_idx];
      s.messages.enqueue({[this, updates = std::move(work.updates)]() mutable {
        for (auto &p : updates) {
          apply_put(p.first, p.second);
        }
      }});
    }
  }

  lite3cpp::Buffer batch_get(const std::vector<std::string> &keys, uint32_t principal_id = ADMIN_UID) {
    lite3cpp::Buffer res;
    res.init_object();
    for (const auto &k : keys) {
      // Foundational Security: ACL Check
      auto perm = credentials_->check_permission(principal_id, k);
      if (!(perm & Permission::READ) && !(perm & Permission::ADMIN)) {
          continue; // Skip unauthorized key
      }

      auto view = get_view(k);
      if (view) {
        if (view->compressed_) {
          std::string src(reinterpret_cast<const char *>(view->buf_.data()), view->buf_.size());
          std::string dst;
          zstd_manager_->decompress(src, dst, view->original_size_);
          res.set_str(0, k, dst);
        } else {
          auto v = view->view();
          res.set_bytes(0, k, std::span<const std::byte>(reinterpret_cast<const std::byte *>(v.data()), v.size()));
        }
      }
    }
    return res;
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

  void set_logger(std::shared_ptr<ILogger> logger) { logger_ = logger; }
  ZstdManager* get_zstd_manager() const { return zstd_manager_.get(); }
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
    return submit_to_shard_idx(shard_idx, [&, shard_idx, prefix, start_key]() {
      auto it = s.map.lower_bound(start_key);
      while (it != s.map.end() && it->first.starts_with(prefix)) {
        if (it->first > start_key ||
            (chunk.empty() && it->first >= start_key)) {
          if (it->second->buf_.size() > 0) {
            std::string vstr;
            if (it->second->compressed_) {
              std::string src(reinterpret_cast<const char *>(it->second->buf_.data()), it->second->buf_.size());
              zstd_manager_->decompress(src, vstr, it->second->original_size_);
            } else {
              vstr = std::string(reinterpret_cast<const char *>(it->second->buf_.data()), it->second->buf_.size());
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
    return submit_to_shard_idx(shard_idx, [&, shard_idx, prefix, start_key]() {
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
        
        std::string effective_start = start_key;
        if (effective_start < prefix) effective_start = prefix;
        
        auto it = s.map.lower_bound(effective_start);
        while (it != s.map.end() && it->first.starts_with(prefix)) {
          chunk.push_back(it->first);
          if (chunk.size() >= limit_per_shard)
            break;
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

    if (results.empty()) {
        for (size_t i = 0; i < SHARDS; ++i) {
            auto &s = *shards_[i];
            std::shared_lock lock(s.read_mu);
            if (!s.map.empty()) {
                 // if(0) std::fprintf(stderr, "[Store] Shard %zu sample keys: ", i);
                int count = 0;
                for (auto it = s.map.begin(); it != s.map.end() && count < 5; ++it, ++count) {
                    if(0) std::fprintf(stderr, "[%s] ", it->first.c_str());
                }
                if(0) std::fprintf(stderr, "\n"); 
            }
        }
    }

    return results;
  }
};

} // namespace l3kv
