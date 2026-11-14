#include "../engine/store.hpp"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace l3kv;

static void require_check(bool cond, const std::string &msg) {
  if (!cond) {
    std::cerr << "FAIL: " << msg << std::endl;
    std::exit(1);
  }
}

void test_conflict_resolution();
void test_tombstones();
void test_merkle_recovery();

void test_put_get() {
  std::string path = "test_store.wal";
  std::filesystem::remove(path);

  {
    Engine db(path, 1); // Node ID 1
    db.put("key1", R"({"foo":"bar"})");
    db.wait_all_shards();

    auto val = db.get("key1");
    std::string s((const char *)val.data(), val.size());
    if (s.find("foo") == std::string::npos) {
        std::cerr << "FAIL: Put/Get failed" << std::endl;
        exit(1);
    }
    std::cout << "[PASS] Store Put/Get" << std::endl;
  }
  std::filesystem::remove(path);
}

void test_sidecar_metadata() {
  std::string path = "test_sidecar.wal";
  std::filesystem::remove(path);

  {
    Engine db(path, 1);
    db.put("doc1", R"({"a": 1})");
    db.wait_all_shards();

    auto val = db.get("doc1");
    if (val.size() == 0) {
        std::cerr << "FAIL: Sidecar metadata test - doc not found" << std::endl;
        exit(1);
    }

    auto meta = db.get("doc1:meta");
    if (meta.size() == 0 || meta.get_i64(0, "ts") <= 0) {
        std::cerr << "FAIL: Sidecar metadata ts missing or <= 0" << std::endl;
        exit(1);
    }
    assert(meta.get_i64(0, "ts") > 0);

    db.del("doc1");
    db.wait_all_shards();
    auto del_meta = db.get("doc1:meta");
    assert(del_meta.size() > 0);
    assert(del_meta.get_bool(0, "tombstone") == true);
  }
  std::filesystem::remove(path);
  std::cout << "[PASS] Sidecar Metadata (Verified)" << std::endl;
}

void test_patch_sidecar() {
  std::string path = "test_patch.wal";
  std::filesystem::remove(path);

  {
    Engine db(path, 1);
    db.put("user1", R"({"age": 20, "score": 100})");
    db.wait_all_shards();

    db.patch_int("user1", "age", 21);
    db.wait_all_shards();

    auto val = db.get("user1");
    if (val.get_i64(0, "age") != 21) {
        std::cerr << "FAIL: Patch Int failed" << std::endl;
        exit(1);
    }

    auto meta = db.get("user1:meta");
    if (meta.size() == 0) {
        std::cerr << "FAIL: Sidecar meta not found" << std::endl;
        exit(1);
    }
    std::cout << "[PASS] Sidecar Patch logic" << std::endl;
  }
  std::filesystem::remove(path);
}

void test_manual_buffer() {
  lite3cpp::Buffer b(1024);
  b.init_object();
  b.set_i64(0, "age", 20);
  if (b.get_i64(0, "age") != 20) {
      std::cerr << "FAIL: Manual buffer logic" << std::endl;
      exit(1);
  }
  std::cout << "[PASS] Manual Buffer Logic" << std::endl;
}

void test_merkle_recovery() {
  std::cout << "TEST: Merkle Recovery from WAL..." << std::endl;
  std::string path = "test_recovery.wal";
  std::filesystem::remove(path);

  uint64_t hash_before = 0;
  {
    Engine db(path, 1);
    db.put("k1", R"({"a":1})");
    db.put("k2", R"({"b":2})");
    db.put("k1", R"({"a":2})");

    db.wait_all_shards();
    hash_before = db.get_merkle_root_hash();
    if (hash_before == 0) {
        std::cerr << "FAIL: Merkle hash is 0" << std::endl;
        exit(1);
    }
  }

  {
    Engine db(path, 1);
    db.wait_all_shards();
    uint64_t hash_after = db.get_merkle_root_hash();
    std::cout << "Hash Before: " << hash_before << " After: " << hash_after << std::endl;
    if (hash_after != hash_before) {
        std::cerr << "FAIL: Merkle Recovery Mismatch!" << std::endl;
        exit(1);
    }
    
    auto buf = db.get("k1");
    if (buf.get_i64(0, "a") != 2) {
        std::cerr << "FAIL: Data recovery mismatch" << std::endl;
        exit(1);
    }
  }
  std::filesystem::remove(path);
  std::cout << "[PASS] Merkle Recovery" << std::endl;
}

void test_conflict_resolution() {
  std::cout << "TEST: Conflict Resolution (LWW)..." << std::endl;
  std::string path = "test_conflict.wal";
  std::filesystem::remove(path);

  {
    Engine db(path, 1);
    Mutation m1;
    m1.key = "CR1";
    std::string v1 = R"({"v":"1"})";
    m1.value = std::vector<uint8_t>(v1.begin(), v1.end());
    m1.timestamp = {100, 0, 1};
    db.apply_mutation(m1);
    db.wait_all_shards();
 
    Mutation m_stale;
    m_stale.key = "CR1";
    std::string v_stale = R"({"v":"STALE"})";
    m_stale.value = std::vector<uint8_t>(v_stale.begin(), v_stale.end());
    m_stale.timestamp = {90, 0, 2};
    db.apply_mutation(m_stale);
    db.wait_all_shards();
 
    auto val = db.get("CR1");
    if (std::string(val.get_str(0, "v")) != "1") {
        std::cerr << "FAIL: Conflict resolution failed" << std::endl;
        exit(1);
    }
    std::cout << "[PASS] Conflict Resolution" << std::endl;
  }
  std::filesystem::remove(path);
}

void test_tombstones() {
  std::cout << "TEST: Tombstones..." << std::endl;
  std::string path = "test_tomb.wal";
  std::filesystem::remove(path);

  {
    Engine db(path, 1);
    Mutation m_put;
    m_put.key = "del_me";
    std::string v = R"({"alive":true})";
    m_put.value = std::vector<uint8_t>(v.begin(), v.end());
    m_put.timestamp = {100, 0, 1};
    db.apply_mutation(m_put);
    
    db.del("del_me");
    db.wait_all_shards();
 
    auto val = db.get("del_me");
    if (val.size() != 0) {
        std::cerr << "FAIL: Tombstone failed - data still exists" << std::endl;
        exit(1);
    }
    std::cout << "[PASS] Tombstones" << std::endl;
  }
  std::filesystem::remove(path);
}

void test_user_credentials() {
  std::cout << "TEST: User Credentials Registration..." << std::endl;
  std::string path = "test_creds.wal";
  std::filesystem::remove(path);

  {
    Engine db(path, 1);
    db.put("sys:u:101", R"({"name":"alice","public_key":"key1"})");
    db.wait_all_shards();

    auto user = db.credentials().get_user_by_key("key1");
    if (!user.has_value()) {
        std::cerr << "FAIL: User credential not found by key" << std::endl;
        exit(1);
    }
    assert(user.has_value());
    assert(user->uid == 101);
    assert(user->name == "alice");
    assert(user->public_key == "key1");

    lite3cpp::Buffer buf;
    buf.init_object();
    buf.set_str(0, "name", "bob");
    buf.set_str(0, "public_key", "key2");
    db.put("sys:u:102", buf.move_to_string());
    db.wait_all_shards();

    assert(db.credentials().get_user_by_key("key2") == 102);

    db.put("sys:u:103", "{}");
    db.wait_all_shards();

    assert(!db.credentials().get_user_by_key("").has_value());

    db.put("sys:u:104", "  \n\t{\"name\":\"charlie\",\"public_key\":\"key4\"}");
    db.wait_all_shards();
    assert(db.credentials().get_user_by_key("key4") == 104);

    std::cout << "[PASS] User Credentials Registration" << std::endl;
  }
  std::filesystem::remove(path);
}

void test_get_max_key_with_prefix() {
  std::cout << "TEST: get_max_key_with_prefix (0xFF, empty, deleted, {tag})..." << std::endl;
  std::string path = "test_max_prefix.wal";
  std::filesystem::remove(path);

  {
    Engine db(path, 1);

    // Empty store with empty prefix
    require_check(db.get_max_key_with_prefix("").empty(),
                  "Empty store get_max_key_with_prefix(\"\") should be empty");

    // Trailing 0xFF keys
    std::string pfx_bin_ff("bin\xFF", 4);
    std::string k_bin_fe("bin\xFE_lower", 10);
    std::string k_bin_ff_1("bin\xFF_1", 6);
    std::string k_bin_ff_2("bin\xFF_2", 6);
    std::string k_bio("bio_upper");

    db.put(k_bin_fe, R"({"v":0})");
    db.put(k_bin_ff_1, R"({"v":1})");
    db.put(k_bin_ff_2, R"({"v":2})");
    db.put(k_bio, R"({"v":3})");

    require_check(db.get_max_key_with_prefix(pfx_bin_ff) == k_bin_ff_2,
                  "Trailing 0xFF prefix \"bin\\xFF\" failed to find max key");

    // Multiple trailing 0xFF bytes
    std::string pfx_all_ffff("all\xFF\xFF", 5);
    std::string k_all_ffff_a("all\xFF\xFF_a", 7);
    std::string k_all_ffff_z("all\xFF\xFF_z", 7);
    std::string k_alm("alm_0");

    db.put(k_all_ffff_a, R"({"v":10})");
    db.put(k_all_ffff_z, R"({"v":20})");
    db.put(k_alm, R"({"v":30})");

    require_check(db.get_max_key_with_prefix(pfx_all_ffff) == k_all_ffff_z,
                  "Multiple trailing 0xFF prefix \"all\\xFF\\xFF\" failed");

    // Pure 0xFF prefix (upper becomes empty -> scans from end())
    std::string pfx_only_ffff("\xFF\xFF", 2);
    std::string k_ffff_1("\xFF\xFF_1", 4);
    std::string k_ffff_9("\xFF\xFF_9", 4);
    db.put(k_ffff_1, R"({"v":100})");
    db.put(k_ffff_9, R"({"v":900})");

    require_check(db.get_max_key_with_prefix(pfx_only_ffff) == k_ffff_9,
                  "Pure 0xFF prefix \"\\xFF\\xFF\" failed");

    // Empty prefix "" returns global maximum live non-:meta key
    require_check(db.get_max_key_with_prefix("") == k_ffff_9,
                  "Empty prefix \"\" did not return global max live key");

    // Deleted max key falls back to next live key
    db.del(k_ffff_9);
    require_check(db.get_max_key_with_prefix("") == k_ffff_1,
                  "Empty prefix \"\" did not fall back after deleting max key");
    db.del(k_ffff_1);
    require_check(db.get_max_key_with_prefix(pfx_only_ffff).empty(),
                  "All-deleted prefix should return empty string");

    db.del(k_bin_ff_2);
    require_check(db.get_max_key_with_prefix(pfx_bin_ff) == k_bin_ff_1,
                  "Deleted max key for \"bin\\xFF\" did not fall back to _1");

    // Single-shard {tag} routing prefix
    db.put("{irods}:seq:0001", R"({"seq":1})");
    db.put("{irods}:seq:0005", R"({"seq":5})");
    db.put("{irods}:seq:0003", R"({"seq":3})");
    db.put("{irods}:other:9999", R"({"seq":9999})");
    db.put("{other}:seq:9999", R"({"seq":9999})");

    require_check(db.get_max_key_with_prefix("{irods}:seq:") == "{irods}:seq:0005",
                  "{tag} prefix \"{irods}:seq:\" failed");
    db.del("{irods}:seq:0005");
    require_check(db.get_max_key_with_prefix("{irods}:seq:") == "{irods}:seq:0003",
                  "{tag} prefix fallback after del failed");

    std::string tag_ff_pfx("{irods}:bin\xFF", 12);
    std::string tag_ff_k1("{irods}:bin\xFF_1", 14);
    std::string tag_ff_k2("{irods}:bin\xFF_2", 14);
    db.put(tag_ff_k1, R"({"v":1})");
    db.put(tag_ff_k2, R"({"v":2})");
    require_check(db.get_max_key_with_prefix(tag_ff_pfx) == tag_ff_k2,
                  "{tag} prefix with trailing 0xFF failed");
  }

  std::filesystem::remove(path);
  std::cout << "[PASS] get_max_key_with_prefix" << std::endl;
}

void test_compact_wal_merkle_and_tombstones() {
  std::cout << "TEST: compact_wal + Restart + Merkle + Tombstone TTL GC..." << std::endl;
  std::string path = "test_compact_merkle.wal";
  std::filesystem::remove(path);
  std::filesystem::remove(path + ".compact.tmp");

  uint64_t root_after_default_compact = 0;
  std::string big_pad(2048, 'x');

  {
    Engine db(path, 1);

    // Feed >100KB of JSON payloads so ZstdManager dictionary training activates,
    // exercising both uncompressed and Zstd-compressed live & :meta tombstone blobs.
    for (int i = 0; i < 60; ++i) {
      db.put("live_" + std::to_string(i),
             std::string("{\"idx\":") + std::to_string(i) + ",\"pad\":\"" + big_pad + "\"}");
    }
    require_check(db.get_zstd_manager()->is_active(),
                  "Expected ZstdManager to be active after >100KB samples");

    // Overwrite some live keys and create + delete keys (producing compressed :meta tombstones)
    db.put("live_0", R"({"idx":1000,"updated":true})");
    for (int d = 0; d < 10; ++d) {
      std::string dk = "del_key_" + std::to_string(d);
      db.put(dk, std::string("{\"temp\":") + std::to_string(d) + ",\"pad\":\"" + big_pad + "\"}");
      require_check(db.del(dk), "Expected del() to succeed on " + dk);
    }

    // Also inject an old tombstone (48h old -> purged by default 24h TTL)
    // and a future-dated tombstone (1h in future -> must NOT underflow and must survive default TTL, then be purged by TTL=0)
    int64_t now_us = db.get_clock().now().wall_time;
    int64_t two_days_us = 48LL * 3600LL * 1000LL * 1000LL;
    int64_t one_hour_us = 3600LL * 1000LL * 1000LL;
    Mutation old_del;
    old_del.key = "ancient_del";
    old_del.is_delete = true;
    old_del.timestamp = {now_us - two_days_us, 0, 1};
    db.apply_mutation(old_del);

    Mutation future_del;
    future_del.key = "future_skew_del";
    future_del.is_delete = true;
    future_del.timestamp = {now_us + one_hour_us, 0, 1};
    db.apply_mutation(future_del);

    db.wait_all_shards();

    // Default compact_wal() (24h TTL):
    // - ancient_del:meta (48h old) should be purged
    // - future_skew_del:meta (ts > now_wall) MUST NOT underflow and MUST survive
    // - del_key_0..9:meta (just created) MUST survive
    require_check(db.compact_wal(), "Default compact_wal() returned false");

    require_check(db.get("ancient_del").size() == 0, "ancient_del should stay deleted");
    require_check(db.get("ancient_del:meta").size() == 0,
                  "Expired 48h tombstone ancient_del:meta should be purged by default compact_wal()");
    require_check(db.get("future_skew_del").size() == 0, "future_skew_del should stay deleted");
    auto fmeta = db.get("future_skew_del:meta");
    require_check(fmeta.size() > 0 && fmeta.get_bool(0, "tombstone") == true,
                  "Future-skewed tombstone future_skew_del:meta must survive default compact_wal()");

    for (int d = 0; d < 10; ++d) {
      std::string dk = "del_key_" + std::to_string(d);
      require_check(db.get(dk).size() == 0, dk + " should be deleted");
      auto tmeta = db.get(dk + ":meta");
      require_check(tmeta.size() > 0 && tmeta.get_bool(0, "tombstone") == true,
                    "Recent tombstone " + dk + ":meta must survive default compact_wal()");
    }

    root_after_default_compact = db.get_merkle_root_hash();
    require_check(root_after_default_compact != 0, "Merkle root should be non-zero");
  }

  uint64_t root_after_zero_ttl_compact = 0;
  {
    // Restart Engine #2 on compacted WAL
    Engine db2(path, 1);
    db2.wait_all_shards();

    uint64_t root_restarted = db2.get_merkle_root_hash();
    require_check(root_restarted == root_after_default_compact,
                  "Merkle root mismatch across restart after default compact_wal(): before=" +
                      std::to_string(root_after_default_compact) +
                      " after=" + std::to_string(root_restarted));

    // Verify live keys & overwritten live_0
    auto v0 = db2.get("live_0");
    require_check(v0.size() > 0 && v0.get_i64(0, "idx") == 1000 && v0.get_bool(0, "updated") == true,
                  "live_0 value mismatch after restart");
    for (int i = 1; i < 60; ++i) {
      auto vi = db2.get("live_" + std::to_string(i));
      require_check(vi.size() > 0 && vi.get_i64(0, "idx") == i,
                    "live_" + std::to_string(i) + " missing or corrupt after restart");
    }

    // Verify deleted keys stay deleted and recent tombstones survived restart
    for (int d = 0; d < 10; ++d) {
      std::string dk = "del_key_" + std::to_string(d);
      require_check(db2.get(dk).size() == 0, dk + " resurrected after restart!");
      auto tmeta = db2.get(dk + ":meta");
      require_check(tmeta.size() > 0 && tmeta.get_bool(0, "tombstone") == true,
                    "Recent tombstone " + dk + ":meta missing after restart");
    }

    // Now call compact_wal(0) to purge ALL :meta tombstones immediately
    require_check(db2.compact_wal(0), "compact_wal(0) returned false");

    require_check(db2.get("future_skew_del:meta").size() == 0,
                  "future_skew_del:meta should be purged by compact_wal(0)");
    for (int d = 0; d < 10; ++d) {
      std::string dk = "del_key_" + std::to_string(d);
      require_check(db2.get(dk).size() == 0, dk + " should remain absent after compact_wal(0)");
      require_check(db2.get(dk + ":meta").size() == 0,
                    dk + ":meta should be purged by compact_wal(0)");
    }

    // Live keys and their :meta sidecars must still exist
    require_check(db2.get("live_0:meta").size() > 0, "live_0:meta must not be purged");
    root_after_zero_ttl_compact = db2.get_merkle_root_hash();
    require_check(root_after_zero_ttl_compact != root_after_default_compact,
                  "Purging tombstones with compact_wal(0) should update Merkle root");
  }

  {
    // Restart Engine #3 on WAL compacted with TTL=0
    Engine db3(path, 1);
    db3.wait_all_shards();

    uint64_t root_restarted_2 = db3.get_merkle_root_hash();
    require_check(root_restarted_2 == root_after_zero_ttl_compact,
                  "Merkle root mismatch across restart after compact_wal(0): before=" +
                      std::to_string(root_after_zero_ttl_compact) +
                      " after=" + std::to_string(root_restarted_2));

    for (int d = 0; d < 10; ++d) {
      std::string dk = "del_key_" + std::to_string(d);
      require_check(db3.get(dk).size() == 0, dk + " resurrected after 2nd restart!");
      require_check(db3.get(dk + ":meta").size() == 0,
                    dk + ":meta resurrected after 2nd restart!");
    }
    for (int i = 0; i < 60; ++i) {
      require_check(db3.get("live_" + std::to_string(i)).size() > 0,
                    "live_" + std::to_string(i) + " missing after 2nd restart");
    }
  }

  std::filesystem::remove(path);
  std::filesystem::remove(path + ".compact.tmp");
  std::cout << "[PASS] compact_wal + Restart + Merkle + Tombstone TTL GC" << std::endl;
}

void test_concurrent_writers_and_compaction() {
  std::cout << "TEST: Concurrent Writers vs Online compact_wal() Stress..." << std::endl;
  std::string path = "test_concurrent_compact.wal";
  std::filesystem::remove(path);
  std::filesystem::remove(path + ".compact.tmp");

  constexpr int NUM_WRITERS = 4;
  constexpr int PUTS_PER_WRITER = 200;
  constexpr int DELS_PER_WRITER = 25;
  constexpr int BATCHES_PER_WRITER = 10;
  constexpr int KEYS_PER_BATCH = 5;

  uint64_t root_before_restart = 0;

  {
    Engine db(path, 1);
    std::atomic<bool> start{false};
    std::atomic<int> writers_done{0};

    std::vector<std::thread> writers;
    for (int t = 0; t < NUM_WRITERS; ++t) {
      writers.emplace_back([&, t]() {
        while (!start.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        // Pre-insert keys to be deleted mid-stream and a counter key for patch_int/patch_str
        std::string patch_key = "w" + std::to_string(t) + ":patched";
        db.put(patch_key, R"({"cnt":0,"tag":"init"})");
        for (int d = 0; d < DELS_PER_WRITER; ++d) {
          db.put("w" + std::to_string(t) + ":del:" + std::to_string(d),
                 R"({"to_delete":true})");
        }
        for (int i = 0; i < PUTS_PER_WRITER; ++i) {
          std::string k = "w" + std::to_string(t) + ":put:" + std::to_string(i);
          std::string v = "{\"writer\":" + std::to_string(t) +
                          ",\"seq\":" + std::to_string(i) + "}";
          db.put(k, v);

          if (i % 20 == 0) {
            db.patch_int(patch_key, "cnt", i + 1);
            db.patch_str(patch_key, "tag", "step_" + std::to_string(i));
          }

          if (i % (PUTS_PER_WRITER / DELS_PER_WRITER) == 0) {
            int d = i / (PUTS_PER_WRITER / DELS_PER_WRITER);
            if (d < DELS_PER_WRITER) {
              db.del("w" + std::to_string(t) + ":del:" + std::to_string(d));
            }
          }

          if (i % (PUTS_PER_WRITER / BATCHES_PER_WRITER) == 0) {
            int b = i / (PUTS_PER_WRITER / BATCHES_PER_WRITER);
            if (b < BATCHES_PER_WRITER) {
              lite3cpp::Buffer bbuf;
              bbuf.init_object();
              for (int m = 0; m < KEYS_PER_BATCH; ++m) {
                std::string bk = "w" + std::to_string(t) + ":batch:" +
                                 std::to_string(b) + ":" + std::to_string(m);
                std::string bv = "{\"b\":" + std::to_string(b) +
                                 ",\"m\":" + std::to_string(m) + "}";
                bbuf.set_str(0, bk, bv);
              }
              db.batch_put(bbuf);
            }
          }
        }
        writers_done.fetch_add(1, std::memory_order_release);
      });
    }

    std::thread compactor([&]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int c = 0; c < 5; ++c) {
        require_check(db.compact_wal(), "Concurrent compact_wal() failed");
        if (writers_done.load(std::memory_order_acquire) < NUM_WRITERS) {
          std::this_thread::sleep_for(std::chrono::microseconds(300));
        }
      }
    });

    start.store(true, std::memory_order_release);

    for (auto &w : writers) {
      w.join();
    }
    compactor.join();

    db.wait_all_shards();
    root_before_restart = db.get_merkle_root_hash();
    require_check(root_before_restart != 0, "Concurrent stress Merkle root is 0");
  }

  {
    // Restart fresh Engine on the WAL file and verify zero data loss & identical Merkle root
    Engine db_restarted(path, 1);
    db_restarted.wait_all_shards();

    for (int t = 0; t < NUM_WRITERS; ++t) {
      std::string patch_key = "w" + std::to_string(t) + ":patched";
      auto pval = db_restarted.get(patch_key);
      require_check(pval.size() > 0 && pval.get_i64(0, "cnt") == 181 &&
                        pval.get_str(0, "tag") == "step_180",
                    "Patched key mismatch after restart: " + patch_key);
      for (int i = 0; i < PUTS_PER_WRITER; ++i) {
        std::string k = "w" + std::to_string(t) + ":put:" + std::to_string(i);
        auto val = db_restarted.get(k);
        require_check(val.size() > 0, "Lost committed put key after restart: " + k);
        require_check(val.get_i64(0, "writer") == t && val.get_i64(0, "seq") == i,
                      "Corrupt value for key after restart: " + k);
      }
      for (int d = 0; d < DELS_PER_WRITER; ++d) {
        std::string dk = "w" + std::to_string(t) + ":del:" + std::to_string(d);
        require_check(db_restarted.get(dk).size() == 0,
                      "Deleted key resurrected after restart: " + dk);
      }
      for (int b = 0; b < BATCHES_PER_WRITER; ++b) {
        for (int m = 0; m < KEYS_PER_BATCH; ++m) {
          std::string bk = "w" + std::to_string(t) + ":batch:" +
                           std::to_string(b) + ":" + std::to_string(m);
          auto val = db_restarted.get(bk);
          require_check(val.size() > 0,
                        "Lost committed batch_put key after restart: " + bk);
          require_check(val.get_i64(0, "b") == b && val.get_i64(0, "m") == m,
                        "Corrupt batch_put value after restart: " + bk);
        }
      }
    }

    uint64_t root_after_restart = db_restarted.get_merkle_root_hash();
    require_check(root_after_restart == root_before_restart,
                  "Concurrent stress Merkle root mismatch across restart: before=" +
                      std::to_string(root_before_restart) +
                      " after=" + std::to_string(root_after_restart));
  }

  std::filesystem::remove(path);
  std::filesystem::remove(path + ".compact.tmp");
  std::cout << "[PASS] Concurrent Writers vs Online compact_wal() Stress" << std::endl;
}

int main() {
  try {
    test_manual_buffer();
    test_put_get();
    test_sidecar_metadata();
    test_patch_sidecar();
    test_conflict_resolution();
    test_tombstones();
    test_merkle_recovery();
    test_user_credentials();
    test_get_max_key_with_prefix();
    test_compact_wal_merkle_and_tombstones();
    test_concurrent_writers_and_compaction();
    std::cout << "All Store Tests Passed Deterministically!" << std::endl;
  } catch (const std::exception &e) {
    std::cerr << "Test Exception: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
