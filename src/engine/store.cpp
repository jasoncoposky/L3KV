#include "engine/store.hpp"

namespace l3kv {

Engine::Engine(std::string wal_path, uint32_t node_id) : clock_(node_id) {
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
          if (!it->second ||
              (!it->second->is_tombstone_ && it->second->buf_.size() == 0)) {
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

Engine::~Engine() {
    for (size_t i = 0; i < SHARDS; ++i) {
      shards_[i]->stop_flag.store(true, std::memory_order_release);
      shards_[i]->messages.enqueue({});
      if (shards_[i]->core_thread.joinable()) {
        shards_[i]->core_thread.join();
      }
    }
    if (wal_) {
      for (size_t i = 0; i < SHARDS; ++i) {
        try {
          std::lock_guard<std::mutex> wlk(shards_[i]->write_mu);
          flush_shard_wal_locked(*shards_[i]);
        } catch (...) {
        }
      }
    }
  }

std::shared_ptr<const Blob> Engine::get_view(std::string_view key) {
    size_t h = get_routing_shard(key);
    auto &s = *shards_[h];
    {
      std::shared_lock<std::shared_mutex> lock(s.read_mu);
      if (auto it = s.map.find(key); it != s.map.end()) {
        if (it->second && it->second->is_expired(now_epoch_us())) {
          return nullptr;
        }
        return it->second;
      }
    }
    if (key.size() > 5 && key.ends_with(":meta")) {
      std::string_view base = key.substr(0, key.size() - 5);
      size_t bh = get_routing_shard(base);
      auto &bs = *shards_[bh];
      std::shared_lock<std::shared_mutex> lock(bs.read_mu);
      if (auto it = bs.map.find(base);
          it != bs.map.end() && it->second &&
          (it->second->has_hlc_ || it->second->is_tombstone_ ||
           it->second->buf_.size() > 0)) {
        auto &mbuf = format_meta_tl(it->second->hlc_, it->second->is_tombstone_,
                                    it->second->expires_at_us_);
        auto out = std::make_shared<Blob>();
        out->buf_ = mbuf;
        out->compressed_ = false;
        out->original_size_ = static_cast<uint32_t>(out->buf_.size());
        out->content_hash_ =
            Blob::non_zero_hash(out->buf_.data(), out->buf_.size());
        return out;
      }
    }
    return nullptr;
  }

std::shared_ptr<const Blob> Engine::get_blob(std::string_view key, uint32_t principal_id) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::READ) && !(perm & Permission::ADMIN)) {
        return nullptr;
      }
    }

    auto view = get_view(key);
    if ((!view || view->buf_.size() == 0) && key.size() > 5 &&
        key.ends_with(":meta")) {
      std::string_view base = key.substr(0, key.size() - 5);
      auto bview = get_view(base);
      if (bview && (bview->has_hlc_ || bview->is_tombstone_ ||
                    bview->buf_.size() > 0)) {
        auto &mbuf = format_meta_tl(bview->hlc_, bview->is_tombstone_,
                                    bview->expires_at_us_);
        auto out = std::make_shared<Blob>();
        out->buf_ = mbuf;
        out->compressed_ = false;
        out->original_size_ = static_cast<uint32_t>(out->buf_.size());
        out->content_hash_ =
            Blob::non_zero_hash(out->buf_.data(), out->buf_.size());
        return out;
      }
    }
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

lite3cpp::Buffer Engine::get(std::string_view key, uint32_t principal_id) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::READ) && !(perm & Permission::ADMIN)) {
        return {};
      }
    }

    auto view = get_view(key);
    if ((!view || view->buf_.size() == 0) && key.size() > 5 &&
        key.ends_with(":meta")) {
      std::string_view base = key.substr(0, key.size() - 5);
      auto bview = get_view(base);
      if (bview && (bview->has_hlc_ || bview->is_tombstone_ ||
                    bview->buf_.size() > 0)) {
        return format_meta_tl(bview->hlc_, bview->is_tombstone_,
                              bview->expires_at_us_);
      }
    }
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

void Engine::put(std::string key, std::string json_body, uint32_t principal_id, int64_t ttl_ms) {
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
    const int64_t expires_at_us =
        (ttl_ms > 0) ? (now_epoch_us() + ttl_ms * 1000LL) : 0;
    new_blob->expires_at_us_ = expires_at_us;
    std::shared_ptr<const Blob> sys_ref;
    if (is_sys) {
      sys_ref = new_blob;
    }

    const size_t k_idx = get_routing_shard(key);

    {
      std::lock_guard<std::mutex> wlk(shards_[k_idx]->write_mu);
      std::unique_lock<std::shared_mutex> slk(shards_[k_idx]->read_mu);

      auto now = next_timestamp();
      new_blob->hlc_ = now;
      new_blob->has_hlc_ = true;
      new_blob->is_tombstone_ = false;

      append_wal_shard_locked(*shards_[k_idx], WalOp::PUT, key, json_body);
      if (is_sys) {
        upsert_blob_locked(*shards_[k_idx], std::string_view(key),
                           std::move(new_blob));
      } else {
        upsert_blob_locked(*shards_[k_idx], std::move(key),
                           std::move(new_blob));
      }
    }

    if (is_sys && sys_ref) {
      apply_system_hooks(key, json_body, *sys_ref);
    }
  }

Engine::CasResult Engine::cas_put(std::string key, std::string payload, uint64_t expected_hash, uint32_t principal_id, int64_t ttl_ms) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
        throw std::runtime_error("Unauthorized: Access Denied for key " + key);
      }
    }

    const bool is_meta = key.ends_with(":meta");
    const bool is_sys = key.starts_with("sys:");
    auto new_blob = std::make_shared<Blob>(payload, zstd_manager_.get(),
                                           nullptr, !is_meta);
    const int64_t now_us = now_epoch_us();
    const int64_t expires_at_us =
        (ttl_ms > 0) ? (now_us + ttl_ms * 1000LL) : 0;
    new_blob->expires_at_us_ = expires_at_us;
    const uint64_t new_hash = new_blob->content_hash();

    std::shared_ptr<const Blob> sys_ref;
    if (is_sys) {
      sys_ref = new_blob;
    }

    const size_t k_idx = get_routing_shard(key);

    {
      std::lock_guard<std::mutex> wlk(shards_[k_idx]->write_mu);
      std::unique_lock<std::shared_mutex> slk(shards_[k_idx]->read_mu);

      auto &ks = *shards_[k_idx];
      uint64_t actual = 0;
      if (auto it = ks.map.find(key);
          it != ks.map.end() && it->second && it->second->buf_.size() > 0 &&
          !it->second->is_expired(now_us)) {
        actual = it->second->content_hash();
      }
      if (actual != expected_hash) {
        return {false, 0, actual};
      }

      auto now = next_timestamp();
      new_blob->hlc_ = now;
      new_blob->has_hlc_ = true;
      new_blob->is_tombstone_ = false;

      append_wal_shard_locked(*shards_[k_idx], WalOp::PUT, key, payload);
      upsert_blob_locked(ks, key, std::move(new_blob));
    }

    if (is_sys && sys_ref) {
      apply_system_hooks(key, payload, *sys_ref);
    }
    return {true, new_hash, expected_hash};
  }

void Engine::patch_int(std::string key, std::string field, int64_t val, uint32_t principal_id) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
        throw std::runtime_error("Unauthorized: Access Denied for key " + key);
      }
    }

    std::string wal_patch = encode_wal_patch_i64(field, val);

    const size_t k_idx = get_routing_shard(key);
    std::lock_guard<std::mutex> wlk(shards_[k_idx]->write_mu);
    std::unique_lock<std::shared_mutex> slk(shards_[k_idx]->read_mu);

    auto now = next_timestamp();
    append_wal_shard_locked(*shards_[k_idx], WalOp::PATCH_I64, key, wal_patch);
    apply_patch_int_locked(*shards_[k_idx], key, field, val, now, true);
  }

void Engine::patch_str(std::string key, std::string field, std::string val, uint32_t principal_id) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
        throw std::runtime_error("Unauthorized: Access Denied for key " + key);
      }
    }

    std::string wal_patch = encode_wal_patch_str(field, val);

    const size_t k_idx = get_routing_shard(key);
    std::lock_guard<std::mutex> wlk(shards_[k_idx]->write_mu);
    std::unique_lock<std::shared_mutex> slk(shards_[k_idx]->read_mu);

    auto now = next_timestamp();
    append_wal_shard_locked(*shards_[k_idx], WalOp::PATCH_STR, key, wal_patch);
    apply_patch_str_locked(*shards_[k_idx], key, field, val, now, true);
  }

bool Engine::del(const std::string &key, uint32_t principal_id) {
    if (principal_id != INTERNAL_UID && principal_id != ADMIN_UID) {
      auto perm = credentials_->check_permission(principal_id, key);
      if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
        return false;
      }
    }

    const size_t k_idx = get_routing_shard(key);
    std::lock_guard<std::mutex> wlk(shards_[k_idx]->write_mu);
    std::unique_lock<std::shared_mutex> slk(shards_[k_idx]->read_mu);

    auto now = next_timestamp();
    append_wal_shard_locked(*shards_[k_idx], WalOp::DELETE_, key, "");
    return apply_del_locked(*shards_[k_idx], key, now, true);
  }

void Engine::batch_put(const lite3cpp::Buffer &batch, uint32_t principal_id) {
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
    std::array<bool, SHARDS> touched{};
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
      const size_t k_idx = get_routing_shard(key_sv);
      touched[k_idx] = true;

      prepared.push_back({key_sv, val_sv, std::string(), std::move(vblob),
                          nullptr, k_idx, k_idx});
    }

    if (prepared.empty()) {
      return;
    }

    struct PendingSysHook {
      std::string_view key;
      std::string_view val;
      std::shared_ptr<const Blob> blob;
    };
    std::vector<PendingSysHook> sys_hooks;

    {
      std::vector<std::unique_lock<std::mutex>> write_locks;
      write_locks.reserve(SHARDS);
      std::vector<std::unique_lock<std::shared_mutex>> shard_locks;
      shard_locks.reserve(SHARDS);
      for (size_t i = 0; i < SHARDS; ++i) {
        if (touched[i]) {
          write_locks.emplace_back(shards_[i]->write_mu);
        }
      }
      for (size_t i = 0; i < SHARDS; ++i) {
        if (touched[i]) {
          shard_locks.emplace_back(shards_[i]->read_mu);
        }
      }

      std::vector<BatchOp> wal_batch;
      wal_batch.reserve(prepared.size());
      for (auto &item : prepared) {
        auto now = next_timestamp();
        item.val_blob->hlc_ = now;
        item.val_blob->has_hlc_ = true;
        item.val_blob->is_tombstone_ = false;
        wal_batch.push_back({WalOp::PUT, item.key, item.val});
      }

      for (size_t i = 0; i < SHARDS; ++i) {
        if (touched[i]) {
          flush_shard_wal_locked(*shards_[i]);
        }
      }
      wal_->append_batch(wal_batch);

      for (auto &item : prepared) {
        if (item.key.starts_with("sys:")) {
          sys_hooks.push_back({item.key, item.val, item.val_blob});
        }
        upsert_blob_locked(*shards_[item.k_shard], item.key,
                           std::move(item.val_blob));
      }
    }

    for (const auto &sh : sys_hooks) {
      if (sh.blob) {
        apply_system_hooks(sh.key, sh.val, *sh.blob);
      }
    }
  }

lite3cpp::Buffer Engine::batch_get(const std::vector<std::string> &keys, uint32_t principal_id) {
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
        if (!zstd_manager_ || !zstd_manager_->get_dict()) {
          continue;
        }
        auto slot = res.set_bytes_uninitialized(0, hk, view->original_size_);
        if (!slot.empty()) {
          if (!zstd_manager_->decompress_into(
                  view->buf_.data(), view->buf_.size(),
                  reinterpret_cast<char *>(slot.data()), slot.size())) {
            std::memset(slot.data(), 0, slot.size());
          }
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

void Engine::apply_batch_mutations(const std::vector<BatchOp> &wal_ops, std::map<size_t, ShardMutation> shard_mutations) {
    if (wal_ops.empty() && shard_mutations.empty()) {
      return;
    }

    struct PreparedShardOps {
      size_t shard_idx;
      std::vector<std::string> dels;
      std::vector<std::pair<std::string, std::shared_ptr<Blob>>> puts;
    };
    std::vector<PreparedShardOps> prepared_shards;
    prepared_shards.reserve(shard_mutations.size());

    for (auto &[shard_idx, mut] : shard_mutations) {
      if (shard_idx >= SHARDS) {
        continue;
      }
      PreparedShardOps pso;
      pso.shard_idx = shard_idx;
      pso.dels = std::move(mut.dels);
      pso.puts.reserve(mut.puts.size());
      for (auto &p : mut.puts) {
        const bool is_meta = p.first.ends_with(":meta");
        pso.puts.emplace_back(
            std::move(p.first),
            std::make_shared<Blob>(p.second, zstd_manager_.get(), nullptr,
                                   !is_meta));
      }
      prepared_shards.push_back(std::move(pso));
    }

    std::vector<std::unique_lock<std::mutex>> write_locks;
    write_locks.reserve(prepared_shards.size());
    for (const auto &pso : prepared_shards) {
      write_locks.emplace_back(shards_[pso.shard_idx]->write_mu);
    }

    std::vector<std::unique_lock<std::shared_mutex>> shard_locks;
    shard_locks.reserve(prepared_shards.size());
    for (const auto &pso : prepared_shards) {
      shard_locks.emplace_back(shards_[pso.shard_idx]->read_mu);
    }

    if (!wal_ops.empty() && wal_) {
      for (const auto &pso : prepared_shards) {
        flush_shard_wal_locked(*shards_[pso.shard_idx]);
      }
      wal_->append_batch(wal_ops);
    }

    for (auto &pso : prepared_shards) {
      auto &s = *shards_[pso.shard_idx];
      for (const auto &d : pso.dels) {
        apply_del_locked(s, d);
      }
      for (auto &[pk, pblob] : pso.puts) {
        upsert_blob_locked(s, pk, std::move(pblob));
      }
    }
  }

void Engine::apply_mutation(const Mutation &m) {
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

    const size_t k_idx = get_routing_shard(m.key);

    std::shared_ptr<const Blob> sys_ref;
    const bool is_sys = (!m.is_delete && m.key.starts_with("sys:"));
    if (is_sys) {
      sys_ref = prebuilt_val;
    }

    {
      std::lock_guard<std::mutex> wlk(shards_[k_idx]->write_mu);
      std::unique_lock<std::shared_mutex> slk(shards_[k_idx]->read_mu);

      Timestamp local_ts{0, 0, 0};
      if (auto it = shards_[k_idx]->map.find(m.key);
          it != shards_[k_idx]->map.end() && it->second &&
          it->second->has_hlc_) {
        local_ts = it->second->hlc_;
      }
      if (m.timestamp <= local_ts) {
        std::cerr << "[Store] Rejecting mutation for " << m.key
                  << " (Stale). Inc: " << m.timestamp.wall_time
                  << " Local: " << local_ts.wall_time << "\n";
        return;
      }

      if (m.is_delete) {
        append_wal_shard_locked(*shards_[k_idx], WalOp::DELETE_, m.key, "");
        apply_del_locked(*shards_[k_idx], m.key, m.timestamp, true);
      } else {
        prebuilt_val->hlc_ = m.timestamp;
        prebuilt_val->has_hlc_ = true;
        prebuilt_val->is_tombstone_ = false;
        append_wal_shard_locked(*shards_[k_idx], WalOp::PUT, m.key, val_sv);
        upsert_blob_locked(*shards_[k_idx], m.key, std::move(prebuilt_val));
      }
    }

    if (is_sys && sys_ref) {
      apply_system_hooks(m.key, val_sv, *sys_ref);
    }
  }

void Engine::flush() {
    if (!wal_) return;
    for (size_t i = 0; i < SHARDS; ++i) {
      std::lock_guard<std::mutex> wlk(shards_[i]->write_mu);
      flush_shard_wal_locked(*shards_[i]);
    }
    wal_->flush();
  }

bool Engine::compact_wal_unlocked(uint64_t tombstone_ttl_ms, std::vector<std::shared_lock<std::shared_mutex>> *active_read_locks) {
    if (!wal_) {
      return false;
    }
    uint64_t old_bytes = wal_->file_size();
    size_t old_ops = wal_->last_recovered_ops();
    const int64_t now_wall_us = clock_.now().wall_time;
    size_t shard_idx = 0;
    auto it = shards_[0]->map.begin();
    size_t live_keys = 0;

    struct DeferredErasure {
      size_t shard;
      std::string key;
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
              if (!it->second) {
                deferred_erasures.push_back({shard_idx, it->first, 0});
                ++it;
                continue;
              }
              if (it->second->is_tombstone_) {
                if (it->second->is_tombstone_expired(now_wall_us,
                                                     tombstone_ttl_ms)) {
                  uint64_t h = hash_blob(it->second);
                  deferred_erasures.push_back({shard_idx, it->first, h});
                } else {
                  auto &mbuf = format_meta_tl(it->second->hlc_, true,
                                              it->second->expires_at_us_);
                  std::string mkey = it->first + ":meta";
                  std::string mval(
                      reinterpret_cast<const char *>(mbuf.data()), mbuf.size());
                  chunk_bytes += mkey.size() + mval.size() + 16;
                  out_chunk.emplace_back(std::move(mkey), std::move(mval));
                  ++live_keys;
                  ++it;
                  if (out_chunk.size() >= 5000 ||
                      chunk_bytes >= 16ULL * 1024 * 1024) {
                    return true;
                  }
                  continue;
                }
                ++it;
                continue;
              }
              if (it->second->buf_.size() == 0) {
                uint64_t h = hash_blob(it->second);
                deferred_erasures.push_back({shard_idx, it->first, h});
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
                  deferred_erasures.push_back({shard_idx, it->first, h});
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
      if (!deferred_erasures.empty()) {
        std::vector<std::unique_lock<std::shared_mutex>> write_locks;
        if (active_read_locks != nullptr) {
          active_read_locks->clear();
          write_locks.reserve(SHARDS);
          for (size_t i = 0; i < SHARDS; ++i) {
            write_locks.emplace_back(shards_[i]->read_mu);
          }
        }
        for (const auto &de : deferred_erasures) {
          if (de.hash != 0) {
            merkle_.apply_delta(de.key, de.hash);
          }
          shards_[de.shard]->map.erase(de.key);
        }
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

bool Engine::compact_wal(uint64_t tombstone_ttl_ms) {
    if (!wal_) {
      return false;
    }
    std::vector<std::unique_lock<std::mutex>> write_locks;
    write_locks.reserve(SHARDS);
    for (size_t i = 0; i < SHARDS; ++i) {
      write_locks.emplace_back(shards_[i]->write_mu);
      flush_shard_wal_locked(*shards_[i]);
    }
    wait_all_shards();
    std::vector<std::shared_lock<std::shared_mutex>> read_locks;
    read_locks.reserve(SHARDS);
    for (size_t i = 0; i < SHARDS; ++i) {
      read_locks.emplace_back(shards_[i]->read_mu);
    }
    return compact_wal_unlocked(tombstone_ttl_ms, &read_locks);
  }

void Engine::wait_all_shards() {
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

std::vector<std::pair<std::string, uint64_t>> Engine::get_bucket_keys(int bucket_idx) {
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

std::vector<std::pair<std::string, std::string>> Engine::get_prefix_chunk(const std::string &prefix, size_t shard_idx, const std::string &start_key, size_t limit) {
    std::vector<std::pair<std::string, std::string>> chunk;
    if (shard_idx >= SHARDS || limit == 0) {
      return chunk;
    }
    chunk.reserve(std::min<size_t>(limit, 256));

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

std::string Engine::get_max_key_with_prefix(const std::string &prefix) {
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

std::vector<std::pair<std::string, std::string>> Engine::get_prefix_entries_all_shards(const std::string &prefix, const std::string &start_key, size_t limit) {
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
      if (chunk.empty()) continue;
      if (results.empty()) {
        results = std::move(chunk);
      } else {
        results.reserve(results.size() + chunk.size());
        for (auto &entry : chunk) {
          results.push_back(std::move(entry));
        }
      }
    }
    std::sort(results.begin(), results.end(),
              [](const auto &a, const auto &b) { return a.first < b.first; });
    if (results.size() > limit) {
      results.resize(limit);
    }
    return results;
  }

std::vector<std::string> Engine::get_prefix_keys(const std::string &prefix, size_t shard_idx, const std::string &start_key, size_t limit) {
    std::vector<std::string> chunk;
    if (shard_idx >= SHARDS || limit == 0) {
      return chunk;
    }
    chunk.reserve(std::min<size_t>(limit, 256));

    auto &s = *shards_[shard_idx];
    const int64_t now_us = now_epoch_us();
    std::shared_lock<std::shared_mutex> lock(s.read_mu);
    auto it = (!start_key.empty() && start_key >= prefix)
                  ? s.map.upper_bound(start_key)
                  : s.map.lower_bound(prefix);
    while (it != s.map.end() && it->first.starts_with(prefix)) {
      if (!it->second || it->second->buf_.size() == 0 ||
          it->second->is_expired(now_us) || it->first.ends_with(":meta")) {
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

std::vector<std::string> Engine::get_prefix_keys_all_shards(const std::string &prefix, const std::string &start_key, size_t limit) {
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
      if (chunk.empty()) continue;
      if (results.empty()) {
        results = std::move(chunk);
      } else {
        results.reserve(results.size() + chunk.size());
        for (auto &k : chunk) {
          results.push_back(std::move(k));
        }
      }
    }
    std::sort(results.begin(), results.end());
    if (results.size() > limit) {
      results.resize(limit);
    }
    return results;
  }

std::vector<std::pair<std::string, lite3cpp::Buffer>> Engine::scan_prefix_kv(const std::string &prefix, const std::string &start_key, size_t limit, uint32_t principal_id) {
    if (limit == 0) {
      return {};
    }
    const int64_t now_us = now_epoch_us();
    const bool check_acl =
        (principal_id != INTERNAL_UID && principal_id != ADMIN_UID);

    auto scan_shard = [&](size_t shard_idx,
                          std::vector<std::pair<std::string, lite3cpp::Buffer>> &out) {
      auto &s = *shards_[shard_idx];
      std::shared_lock<std::shared_mutex> lock(s.read_mu);
      auto it = (!start_key.empty() && start_key >= prefix)
                    ? s.map.upper_bound(start_key)
                    : s.map.lower_bound(prefix);
      size_t added = 0;
      while (it != s.map.end() && it->first.starts_with(prefix)) {
        if (!it->second || it->second->buf_.size() == 0 ||
            it->second->is_expired(now_us) || it->first.ends_with(":meta")) {
          ++it;
          continue;
        }
        if (check_acl) {
          auto perm = credentials_->check_permission(principal_id, it->first);
          if (!(perm & Permission::READ) && !(perm & Permission::ADMIN)) {
            ++it;
            continue;
          }
        }
        if (it->second->compressed_) {
          std::vector<uint8_t> raw(it->second->original_size_);
          if (!zstd_manager_->decompress_into(it->second->buf_.data(),
                                              it->second->buf_.size(),
                                              raw.data(), raw.size())) {
            ++it;
            continue;
          }
          out.emplace_back(it->first, lite3cpp::Buffer(std::move(raw)));
        } else {
          out.emplace_back(it->first, it->second->buf_);
        }
        if (++added >= limit) {
          break;
        }
        ++it;
      }
    };

    std::vector<std::pair<std::string, lite3cpp::Buffer>> out;
    out.reserve(std::min<size_t>(limit, 256));
    if (has_complete_routing_tag(prefix)) {
      scan_shard(get_routing_shard(prefix), out);
      return out;
    }

    for (size_t i = 0; i < SHARDS; ++i) {
      scan_shard(i, out);
    }
    std::sort(out.begin(), out.end(),
              [](const auto &a, const auto &b) { return a.first < b.first; });
    if (out.size() > limit) {
      out.resize(limit);
    }
    return out;
  }

size_t Engine::sweep_expired_keys(size_t max_per_shard) {
    const int64_t now_us = now_epoch_us();
    std::vector<std::string> candidates;
    for (size_t i = 0; i < SHARDS; ++i) {
      auto &s = *shards_[i];
      std::shared_lock<std::shared_mutex> lock(s.read_mu);
      size_t count = 0;
      for (const auto &[k, blob] : s.map) {
        if (blob && blob->buf_.size() > 0 && !k.ends_with(":meta") &&
            blob->is_expired(now_us)) {
          candidates.push_back(k);
          if (++count >= max_per_shard) {
            break;
          }
        }
      }
    }

    size_t swept = 0;
    for (const auto &key : candidates) {
      const size_t k_idx = get_routing_shard(key);
      std::lock_guard<std::mutex> wlk(shards_[k_idx]->write_mu);
      std::unique_lock<std::shared_mutex> slk(shards_[k_idx]->read_mu);
      auto &ks = *shards_[k_idx];
      auto it = ks.map.find(key);
      if (it == ks.map.end() || !it->second || it->second->buf_.size() == 0 ||
          !it->second->is_expired(now_us)) {
        continue;
      }
      auto now = next_timestamp();
      append_wal_shard_locked(*shards_[k_idx], WalOp::DELETE_, key, "");
      apply_del_locked(ks, key, now, true);
      ++swept;
    }
    return swept;
  }

} // namespace l3kv
