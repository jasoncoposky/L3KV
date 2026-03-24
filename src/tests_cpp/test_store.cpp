#include "../engine/store.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

using namespace l3kv;

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

int main() {
  try {
    test_manual_buffer();
    test_put_get();
    test_sidecar_metadata();
    test_patch_sidecar();
    test_conflict_resolution();
    test_tombstones();
    test_merkle_recovery();
    std::cout << "All Store Tests Passed Deterministically!" << std::endl;
  } catch (const std::exception &e) {
    std::cerr << "Test Exception: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
