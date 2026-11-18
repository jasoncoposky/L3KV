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
    require_check(root_after_zero_ttl_compact == root_after_default_compact,
                  "Purging tombstones with compact_wal(0) should preserve live-key Merkle root");
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

void test_compact_wal_rollback_on_failure() {
  std::cout << "TEST: compact_streaming & compact_wal Rollback on Failure..." << std::endl;

  // Part 1: Direct WriteAheadLog::compact_streaming mid-stream failure (> UINT16_MAX key)
  std::string wal_path = "test_wal_rollback.wal";
  std::filesystem::remove(wal_path);
  std::filesystem::remove(wal_path + ".compact.tmp");
  {
    WriteAheadLog wal(wal_path);
    wal.recover([](WalOp, std::string_view, std::string_view) {});
    wal.append(WalOp::PUT, "orig_k1", "orig_v1");
    wal.append(WalOp::PUT, "orig_k2", "orig_v2");
    wal.flush();
    uint64_t orig_size = wal.file_size();
    require_check(orig_size > 0, "Expected non-zero initial WAL size");

    int step = 0;
    bool ok = wal.compact_streaming(
        [&](std::vector<std::pair<std::string, std::string>> &chunk) -> bool {
          chunk.clear();
          if (step == 0) {
            chunk.emplace_back("chunk1_ok", "val1");
            ++step;
            return true;
          }
          if (step == 1) {
            chunk.emplace_back(std::string(70000, 'x'), "oversize_key");
            ++step;
            return true;
          }
          return false;
        });
    require_check(!ok, "compact_streaming should return false on >UINT16_MAX key");
    require_check(!std::filesystem::exists(wal_path + ".compact.tmp"),
                  ".compact.tmp must be unlinked on compact_streaming failure");
    require_check(wal.file_size() == orig_size,
                  "Original WAL file size must be unchanged on compact_streaming failure");

    // Verify file_.fd remains valid for subsequent appends
    wal.append(WalOp::PUT, "orig_k3", "orig_v3");
    wal.flush();
  }
  {
    WriteAheadLog wal_rec(wal_path);
    std::vector<std::pair<std::string, std::string>> recovered;
    wal_rec.recover([&](WalOp op, std::string_view k, std::string_view v) {
      if (op == WalOp::PUT) {
        recovered.emplace_back(std::string(k), std::string(v));
      }
    });
    require_check(recovered.size() == 3 &&
                      recovered[0] == std::make_pair(std::string("orig_k1"), std::string("orig_v1")) &&
                      recovered[1] == std::make_pair(std::string("orig_k2"), std::string("orig_v2")) &&
                      recovered[2] == std::make_pair(std::string("orig_k3"), std::string("orig_v3")),
                  "Original WAL entries + post-failure append must survive failed compact_streaming");
  }
  std::filesystem::remove(wal_path);

  // Part 2: Engine::compact_wal(0) deferred s.map & merkle_ pruning on failure
  std::string eng_path = "test_engine_rollback.wal";
  std::filesystem::remove(eng_path);
  std::filesystem::remove(eng_path + ".compact.tmp");

  uint64_t root_disk = 0;
  {
    Engine db(eng_path, 1);
    db.put("{rb}:a_live", R"({"v":1})");
    db.put("{rb}:b_del", R"({"v":2})");
    require_check(db.del("{rb}:b_del"), "del({rb}:b_del) failed");
    db.wait_all_shards();

    root_disk = db.get_merkle_root_hash();
    uint64_t wal_size_before = std::filesystem::file_size(eng_path);

    // Inject an in-memory >UINT16_MAX key in the SAME {rb} shard sorted AFTER {rb}:b_del:meta
    // so compact_wal(0) scans {rb}:b_del and {rb}:b_del:meta BEFORE hitting the oversize failure.
    std::string oversize_key = "{rb}:z_" + std::string(70000, 'z');
    db.apply_put(oversize_key, R"({"v":3})");
    uint64_t root_with_oversize = db.get_merkle_root_hash();

    require_check(!db.compact_wal(0),
                  "compact_wal(0) must return false when a key exceeds UINT16_MAX");
    require_check(!std::filesystem::exists(eng_path + ".compact.tmp"),
                  ".compact.tmp must be unlinked after failed compact_wal(0)");
    require_check(std::filesystem::file_size(eng_path) == wal_size_before,
                  "On-disk WAL size must remain intact after failed compact_wal(0)");

    auto tmeta = db.get("{rb}:b_del:meta");
    require_check(tmeta.size() > 0 && tmeta.get_bool(0, "tombstone") == true,
                  "Tombstone {rb}:b_del:meta was prematurely erased on failed compact_wal(0)");
    require_check(db.get_merkle_root_hash() == root_with_oversize,
                  "In-memory Merkle root was mutated despite failed compact_wal(0)");

    // Remove the unpersisted oversize key and verify Merkle root matches on-disk WAL state
    db.apply_del(oversize_key);
    require_check(db.get_merkle_root_hash() == root_disk,
                  "Merkle root after removing unpersisted oversize key must match root_disk");
  }
  {
    Engine db_restarted(eng_path, 1);
    db_restarted.wait_all_shards();
    require_check(db_restarted.get_merkle_root_hash() == root_disk,
                  "Restarted Merkle root mismatch after rolled-back compact_wal(0)");
    auto live_v = db_restarted.get("{rb}:a_live");
    require_check(live_v.size() > 0 && live_v.get_i64(0, "v") == 1,
                  "{rb}:a_live missing after rolled-back compact_wal(0)");
    require_check(db_restarted.get("{rb}:b_del").size() == 0,
                  "{rb}:b_del resurrected after rolled-back compact_wal(0)");
    auto tmeta = db_restarted.get("{rb}:b_del:meta");
    require_check(tmeta.size() > 0 && tmeta.get_bool(0, "tombstone") == true,
                  "{rb}:b_del:meta missing after restart following rolled-back compact_wal(0)");
  }

  std::filesystem::remove(eng_path);
  std::filesystem::remove(eng_path + ".compact.tmp");
  std::cout << "[PASS] compact_streaming & compact_wal Rollback on Failure" << std::endl;
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

void test_wave1_optimizations_and_fixes() {
  std::cout << "TEST: Wave 1 P0/P1 Fixes (Co-routing, >1KB Keys, Zstd/Blob/batch_get, Pagination, Patch ACL)..." << std::endl;

  // 1. Co-routing of untagged keys and :meta sidecars across 100 distinct keys + {tag} routing
  {
    std::string path = "test_wave1_coroute.wal";
    std::filesystem::remove(path);
    {
      Engine db(path, 1);
      require_check(db.get_routing_shard("alpha_key") == db.get_routing_shard("alpha_key:meta"),
                    "alpha_key and alpha_key:meta must co-route to the same shard");
      for (int i = 0; i < 100; ++i) {
        std::string k = "untagged_doc_" + std::to_string(i) + "_payload";
        require_check(db.get_routing_shard(k) == db.get_routing_shard(k + ":meta"),
                      "Untagged key and :meta failed to co-route for " + k);
      }
      size_t tag_shard = db.get_routing_shard("{tenant42}:a");
      require_check(db.get_routing_shard("{tenant42}:b") == tag_shard,
                    "{tag} keys with same tag must share shard");
      require_check(db.get_routing_shard("{tenant42}:a:meta") == tag_shard,
                    "{tag} :meta key must route by {tag}");
    }
    std::filesystem::remove(path);
  }

  // 2. Keys > 1024 bytes (2048 bytes) round-trip cleanly without clobbering empty key ""
  {
    std::string path = "test_wave1_longkey.wal";
    std::filesystem::remove(path);
    std::string long_key(2048, 'L');
    {
      Engine db(path, 1);
      db.put("", R"({"empty_key_marker":777})");
      db.put(long_key, R"({"long_val":2048})");
      db.wait_all_shards();

      auto empty_val = db.get("");
      require_check(empty_val.size() > 0 &&
                        empty_val.try_get_i64(0, "empty_key_marker").value_or(0) == 777,
                    "2048-byte key put clobbered empty key \"\"!");

      auto lval = db.get(long_key);
      require_check(lval.size() > 0 && lval.get_i64(0, "long_val") == 2048,
                    "2048-byte key get mismatch");

      auto lmeta = db.get(long_key + ":meta");
      require_check(lmeta.size() > 0 && lmeta.get_i64(0, "ts") > 0,
                    "2048-byte key :meta missing or invalid");

      require_check(db.del(long_key), "del(2048-byte key) returned false");
      db.wait_all_shards();
      require_check(db.get(long_key).size() == 0, "2048-byte key still present after del");
      auto empty_after = db.get("");
      require_check(empty_after.size() > 0 &&
                        empty_after.try_get_i64(0, "empty_key_marker").value_or(0) == 777,
                    "del(2048-byte key) clobbered empty key \"\"!");
    }
    std::filesystem::remove(path);
  }

  // 3. Zstd >=512B threshold, Blob::content_hash(), batch_get Type::Bytes parity, and Engine::get_blob
  {
    std::string path = "test_wave1_zstd_blob.wal";
    std::filesystem::remove(path);
    {
      Engine db(path, 1);
      std::string pad(1600, 'z');
      for (int i = 0; i < 75; ++i) {
        db.put("sample_" + std::to_string(i),
               "{\"idx\":" + std::to_string(i) + ",\"pad\":\"" + pad + "\"}");
      }
      db.wait_all_shards();
      require_check(db.get_zstd_manager()->is_active(),
                    "ZstdManager should be active after 75 x 1.6KB samples");

      std::string small_key = "small_doc";
      std::string large_key = "large_doc";
      db.put(small_key, R"({"tiny":42,"label":"short"})");
      db.put(large_key, "{\"idx\":999,\"pad\":\"" + pad + "\"}");
      db.wait_all_shards();

      auto small_raw = db.get_view(small_key);
      auto small_meta_raw = db.get_view(small_key + ":meta");
      auto large_raw = db.get_view(large_key);
      auto large_meta_raw = db.get_view(large_key + ":meta");

      require_check(small_raw != nullptr && !small_raw->compressed_,
                    "Small blob (<512B) must stay uncompressed");
      require_check(small_meta_raw != nullptr && !small_meta_raw->compressed_,
                    "Small key :meta blob must stay uncompressed");
      require_check(large_meta_raw != nullptr && !large_meta_raw->compressed_,
                    "Large key :meta blob must stay uncompressed");
      require_check(large_raw != nullptr && large_raw->compressed_,
                    "Large compressible blob (>=1024B) must be compressed");

      auto small_buf = db.get(small_key);
      auto large_buf = db.get(large_key);
      require_check(small_raw->content_hash() ==
                        XXH3_64bits(small_buf.data(), small_buf.size()),
                    "Uncompressed Blob::content_hash() mismatch");
      require_check(large_raw->content_hash() ==
                        XXH3_64bits(large_buf.data(), large_buf.size()),
                    "Compressed Blob::content_hash() must equal XXH3 of uncompressed Lite3 bytes");

      // batch_get must return Type::Bytes for BOTH uncompressed and compressed keys with exact byte parity
      auto bres = db.batch_get({small_key, large_key});
      require_check(bres.get_type(0, small_key) == lite3cpp::Type::Bytes,
                    "batch_get must store uncompressed value as Type::Bytes");
      require_check(bres.get_type(0, large_key) == lite3cpp::Type::Bytes,
                    "batch_get must store compressed value as Type::Bytes (not Type::String!)");

      auto b_small = bres.get_bytes(0, small_key);
      auto b_large = bres.get_bytes(0, large_key);
      require_check(b_small.size() == small_buf.size() &&
                        std::memcmp(b_small.data(), small_buf.data(), small_buf.size()) == 0,
                    "batch_get uncompressed bytes mismatch vs get()");
      require_check(b_large.size() == large_buf.size() &&
                        std::memcmp(b_large.data(), large_buf.data(), large_buf.size()) == 0,
                    "batch_get compressed bytes mismatch vs get()");

      // Engine::get_blob zero-copy uncompressed Blob API + ACL check
      auto sblob = db.get_blob(small_key);
      require_check(sblob != nullptr && !sblob->compressed_,
                    "get_blob(small_key) must return uncompressed Blob");
      auto sview = lite3cpp::Buffer::from_view(sblob->buf_.data(), sblob->buf_.size());
      require_check(sview.get_i64(0, "tiny") == 42 && sview.get_str(0, "label") == "short",
                    "get_blob(small_key) payload mismatch");

      auto lblob = db.get_blob(large_key);
      require_check(lblob != nullptr && !lblob->compressed_,
                    "get_blob(large_key) must return decompressed Blob with compressed_==false");
      require_check(lblob->buf_.size() == large_buf.size() &&
                        std::memcmp(lblob->buf_.data(), large_buf.data(), large_buf.size()) == 0,
                    "get_blob(large_key) payload mismatch");

      require_check(db.get_blob("non_existent_key") == nullptr,
                    "get_blob on missing key must return nullptr");

      // ACL enforcement on get_blob for non-admin principal
      db.credentials().register_user(701, "reader", "rk701");
      db.credentials().set_acl(701, "small_", Permission::READ);
      require_check(db.get_blob(small_key, 701) != nullptr,
                    "Authorized principal should read small_key via get_blob");
      require_check(db.get_blob(large_key, 701) == nullptr,
                    "Unauthorized principal must get nullptr from get_blob");

      db.del(small_key);
      require_check(db.get_blob(small_key) == nullptr,
                    "get_blob on deleted key must return nullptr");
    }
    std::filesystem::remove(path);
  }

  // 4. Strict > start_key prefix pagination (zero boundary duplicates), lexicographical order, and {tag} fast-path
  {
    std::string path = "test_wave1_pagination.wal";
    std::filesystem::remove(path);
    {
      Engine db(path, 1);
      std::vector<std::string> expected_p;
      for (int i = 0; i < 25; ++i) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "p:%02d", i);
        expected_p.emplace_back(buf);
        db.put(buf, "{\"i\":" + std::to_string(i) + "}");
      }
      db.put("p:12_deleted", R"({"del":true})");
      db.del("p:12_deleted");
      db.put("o:99", R"({"other":1})");
      db.put("q:00", R"({"other":2})");

      // Paginate get_prefix_keys_all_shards
      std::vector<std::string> collected_keys;
      std::string cursor;
      int guard = 0;
      while (true) {
        auto page = db.get_prefix_keys_all_shards("p:", cursor, 6);
        if (page.empty()) break;
        require_check(page.size() <= 6, "get_prefix_keys_all_shards exceeded page limit");
        if (!cursor.empty()) {
          require_check(page.front() > cursor,
                        "Boundary duplicate or non-advancing cursor in get_prefix_keys_all_shards: front=" +
                            page.front() + " cursor=" + cursor);
        }
        for (size_t j = 1; j < page.size(); ++j) {
          require_check(page[j - 1] < page[j], "Page keys not strictly sorted");
        }
        collected_keys.insert(collected_keys.end(), page.begin(), page.end());
        cursor = page.back();
        require_check(++guard <= 10, "Pagination loop did not terminate");
      }
      require_check(collected_keys == expected_p,
                    "Paginated get_prefix_keys_all_shards mismatch vs expected 25 sorted keys");

      // Paginate get_prefix_entries_all_shards
      std::vector<std::string> collected_entry_keys;
      cursor.clear();
      guard = 0;
      while (true) {
        auto page = db.get_prefix_entries_all_shards("p:", cursor, 7);
        if (page.empty()) break;
        require_check(page.size() <= 7, "get_prefix_entries_all_shards exceeded page limit");
        if (!cursor.empty()) {
          require_check(page.front().first > cursor,
                        "Boundary duplicate in get_prefix_entries_all_shards");
        }
        for (const auto &[k, v] : page) {
          require_check(!v.empty(), "Empty entry value in get_prefix_entries_all_shards");
          collected_entry_keys.push_back(k);
        }
        cursor = page.back().first;
        require_check(++guard <= 10, "Entries pagination loop did not terminate");
      }
      require_check(collected_entry_keys == expected_p,
                    "Paginated get_prefix_entries_all_shards mismatch");

      // Paginate single-shard get_prefix_chunk with limit=1 (catches chunk.empty() && >= start_key bug!)
      std::vector<std::string> expected_tag;
      for (int i = 0; i < 9; ++i) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "n:{0100}:%02d", i);
        expected_tag.emplace_back(buf);
        db.put(buf, "{\"t\":" + std::to_string(i) + "}");
      }
      db.put("n:{0200}:00", R"({"t":999})");

      size_t tag_shard = db.get_routing_shard("n:{0100}:");
      std::vector<std::string> chunk_keys;
      cursor.clear();
      guard = 0;
      while (true) {
        auto cpage = db.get_prefix_chunk("n:{0100}:", tag_shard, cursor, 1);
        if (cpage.empty()) break;
        if (!cursor.empty()) {
          require_check(cpage.front().first > cursor,
                        "get_prefix_chunk failed to advance strictly past start_key with limit=1");
        }
        chunk_keys.push_back(cpage.front().first);
        cursor = cpage.front().first;
        require_check(++guard <= 15, "get_prefix_chunk infinite pagination loop");
      }
      require_check(chunk_keys == expected_tag,
                    "get_prefix_chunk single-shard pagination mismatch");

      // {tag}-prefixed get_prefix_keys_all_shards fast-path
      std::vector<std::string> fast_keys;
      cursor.clear();
      while (true) {
        auto page = db.get_prefix_keys_all_shards("n:{0100}:", cursor, 4);
        if (page.empty()) break;
        fast_keys.insert(fast_keys.end(), page.begin(), page.end());
        cursor = page.back();
      }
      require_check(fast_keys == expected_tag,
                    "{tag} fast-path get_prefix_keys_all_shards mismatch");
    }
    std::filesystem::remove(path);
  }

  // 5. patch_int & patch_str: WRITE ACL check, valid integer :meta HLC fields, and ':' in field names
  {
    std::string path = "test_wave1_patch.wal";
    std::filesystem::remove(path);
    uint64_t root_before = 0;
    {
      Engine db(path, 1);
      db.credentials().register_user(501, "patch_user", "pk501");
      db.credentials().set_acl(501, "ro:", Permission::READ);
      db.credentials().set_acl(501, "rw:", Permission::READ | Permission::WRITE);

      db.put("ro:item", R"({"cnt":1,"label":"orig"})");
      db.put("rw:item", R"({"stats:cnt":10,"ns:label":"init"})");

      bool blocked_int = false;
      try {
        db.patch_int("ro:item", "cnt", 99, 501);
      } catch (const std::exception &) {
        blocked_int = true;
      }
      require_check(blocked_int, "patch_int must reject principal without Permission::WRITE");

      bool blocked_str = false;
      try {
        db.patch_str("ro:item", "label", "hacked", 501);
      } catch (const std::exception &) {
        blocked_str = true;
      }
      require_check(blocked_str, "patch_str must reject principal without Permission::WRITE");

      auto ro_val = db.get("ro:item");
      require_check(ro_val.get_i64(0, "cnt") == 1 && ro_val.get_str(0, "label") == "orig",
                    "Rejected patch mutated ro:item!");

      db.patch_int("rw:item", "stats:cnt", 42, 501);
      db.patch_str("rw:item", "ns:label", "left:mid:right", 501);
      db.wait_all_shards();

      auto rw_val = db.get("rw:item");
      require_check(rw_val.get_i64(0, "stats:cnt") == 42 &&
                        rw_val.get_str(0, "ns:label") == "left:mid:right",
                    "Authorized patch_int/patch_str with ':' in field failed");

      auto rw_meta = db.get("rw:item:meta");
      require_check(rw_meta.size() > 0, "rw:item:meta missing after patch");
      require_check(rw_meta.try_get_i64(0, "ts").value_or(0) > 0,
                    "patch must update :meta with integer \"ts\" > 0");
      require_check(rw_meta.try_get_i64(0, "l").has_value(),
                    "patch must update :meta with integer \"l\"");
      require_check(rw_meta.try_get_i64(0, "n").value_or(0) == 1,
                    "patch must update :meta with integer \"n\" == node_id");
      require_check(!rw_meta.try_get_str(0, "stats:cnt").has_value() &&
                        !rw_meta.try_get_str(0, "ns:label").has_value(),
                    "patch must NOT pollute :meta with <field>:\"w:l:n\" strings");

      root_before = db.get_merkle_root_hash();
    }
    {
      Engine db_rec(path, 1);
      db_rec.wait_all_shards();
      auto rw_val = db_rec.get("rw:item");
      require_check(rw_val.size() > 0 &&
                        rw_val.get_i64(0, "stats:cnt") == 42 &&
                        rw_val.get_str(0, "ns:label") == "left:mid:right",
                    "WAL recovery failed for patch_int/patch_str with ':' in field name");
      require_check(db_rec.get_merkle_root_hash() == root_before,
                    "Merkle root mismatch after recovering colon-field patches");
    }
    std::filesystem::remove(path);
  }

  // 6. Backward-compatible WAL recovery of legacy "field:val" PATCH_I64 / PATCH_STR frames
  {
    std::string path = "test_wave1_legacy_patch.wal";
    std::filesystem::remove(path);
    {
      WriteAheadLog wal(path);
      wal.append(WalOp::PUT, "legacy_doc", R"({"age":10,"city":"LA"})");
      wal.append(WalOp::PATCH_I64, "legacy_doc", "age:33");
      wal.append(WalOp::PATCH_STR, "legacy_doc", "city:NYC");
      wal.flush();
    }
    {
      Engine db(path, 1);
      auto v = db.get("legacy_doc");
      require_check(v.size() > 0 && v.get_i64(0, "age") == 33 &&
                        v.get_str(0, "city") == "NYC",
                    "Legacy WAL PATCH_I64 / PATCH_STR backward-compatibility broken");
    }
    std::filesystem::remove(path);
  }

  // 7. +1h future apply_mutation skew followed by 4-thread db.put() completes < 1s with monotonic :meta
  {
    std::string path = "test_wave1_future_skew_puts.wal";
    std::filesystem::remove(path);
    {
      Engine db(path, 1);
      int64_t now_us = db.get_clock().now().wall_time;
      Timestamp skew_ts{now_us + 3600LL * 1000000LL, 7, 2};
      Mutation m_future;
      m_future.key = "skew_seed";
      std::string seed_json = R"({"seed":1})";
      m_future.value.assign(seed_json.begin(), seed_json.end());
      m_future.timestamp = skew_ts;
      db.apply_mutation(m_future);

      auto t0 = std::chrono::steady_clock::now();
      constexpr int THREADS = 4;
      constexpr int PUTS_PER_THREAD = 80;
      std::atomic<bool> monotonic_ok{true};

      std::vector<std::thread> workers;
      for (int t = 0; t < THREADS; ++t) {
        workers.emplace_back([&, t]() {
          std::string hot_key = "skew_worker_" + std::to_string(t);
          Timestamp prev = skew_ts;
          for (int i = 0; i < PUTS_PER_THREAD; ++i) {
            db.put(hot_key, "{\"seq\":" + std::to_string(i) + "}");
            auto meta = db.get(hot_key + ":meta");
            Timestamp cur{
                meta.try_get_i64(0, "ts").value_or(0),
                static_cast<uint32_t>(meta.try_get_i64(0, "l").value_or(0)),
                static_cast<uint32_t>(meta.try_get_i64(0, "n").value_or(0)),
            };
            if (!(cur > prev)) {
              monotonic_ok.store(false, std::memory_order_relaxed);
              break;
            }
            prev = cur;
          }
        });
      }
      for (auto &w : workers) {
        w.join();
      }
      auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
      require_check(elapsed_ms < 1000,
                    "Multi-threaded db.put() after +1h skew took too long (" +
                        std::to_string(elapsed_ms) + "ms)");
      require_check(monotonic_ok.load(),
                    "Non-monotonic :meta HLC timestamps after +1h apply_mutation skew");
      require_check(db.get_routing_shard("") == db.get_routing_shard(":meta"),
                    "Empty key \"\" and \":meta\" should co-route");
    }
    std::filesystem::remove(path);
  }

  // 8. Compressed blob patching + corrupt Zstd frame safety in set_int/set_str and batch_get
  {
    std::string path = "test_wave1_corrupt_zstd.wal";
    std::filesystem::remove(path);
    {
      Engine db(path, 1);

      // Before Zstd dictionary is active: corrupt compressed blob skipped by batch_get & rejected by set_int/set_str
      auto bad_blob = std::make_shared<Blob>();
      const uint8_t garbage[] = {0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02};
      bad_blob->buf_ = lite3cpp::Buffer(garbage, sizeof(garbage));
      bad_blob->compressed_ = true;
      bad_blob->original_size_ = 64;

      Blob copy_for_patch = *bad_blob;
      require_check(!copy_for_patch.set_int("f", 1, db.get_zstd_manager()),
                    "Blob::set_int must return false when decompression fails (no dict)");
      require_check(!copy_for_patch.set_str("f", "v", db.get_zstd_manager()),
                    "Blob::set_str must return false when decompression fails (no dict)");

      size_t bad_shard = db.get_routing_shard("corrupt_zkey");
      db.get_shard_by_index(bad_shard).map["corrupt_zkey"] = bad_blob;
      auto bres_no_dict = db.batch_get({"corrupt_zkey"});
      require_check(!bres_no_dict.try_get_bytes(0, "corrupt_zkey").has_value(),
                    "batch_get without active Zstd dict must skip compressed entry cleanly");

      // Train dictionary, verify patching a real compressed blob works, and verify corrupt frame zero-fills slot
      std::string pad(1600, 'p');
      for (int i = 0; i < 75; ++i) {
        db.put("dict_doc_" + std::to_string(i),
               "{\"cnt\":" + std::to_string(i) + ",\"pad\":\"" + pad + "\"}");
      }
      require_check(db.get_zstd_manager()->is_active(), "Expected active Zstd dict");

      db.put("comp_patch_target", "{\"cnt\":10,\"tag\":\"v0\",\"pad\":\"" + pad + "\"}");
      require_check(db.get_view("comp_patch_target")->compressed_,
                    "comp_patch_target should be compressed before patch");
      db.patch_int("comp_patch_target", "cnt", 99);
      db.patch_str("comp_patch_target", "tag", "v1");
      auto patched_comp = db.get("comp_patch_target");
      require_check(patched_comp.get_i64(0, "cnt") == 99 &&
                        patched_comp.get_str(0, "tag") == "v1",
                    "Patching compressed blob failed");

      Blob copy_with_dict = *bad_blob;
      require_check(!copy_with_dict.set_int("f", 1, db.get_zstd_manager()),
                    "Blob::set_int must return false on corrupt Zstd frame");
      require_check(!copy_with_dict.set_str("f", "v", db.get_zstd_manager()),
                    "Blob::set_str must return false on corrupt Zstd frame");

      auto bres_corrupt = db.batch_get({"corrupt_zkey"});
      auto cbytes = bres_corrupt.try_get_bytes(0, "corrupt_zkey");
      if (cbytes.has_value()) {
        for (std::byte b : *cbytes) {
          require_check(b == std::byte{0},
                        "batch_get exposed non-zero uninitialized byte on failed Zstd decompression");
        }
      }
    }
    std::filesystem::remove(path);
  }

  std::cout << "[PASS] Wave 1 P0/P1 Fixes" << std::endl;
}

int main() {
  try {
    test_wave1_optimizations_and_fixes();
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
    test_compact_wal_rollback_on_failure();
    test_concurrent_writers_and_compaction();
    std::cout << "All Store Tests Passed Deterministically!" << std::endl;
  } catch (const std::exception &e) {
    std::cerr << "Test Exception: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
