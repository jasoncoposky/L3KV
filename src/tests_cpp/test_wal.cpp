#include "../engine/wal.hpp"
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <vector>

using namespace l3kv;

static void require_wal(bool cond, const std::string &msg) {
  if (!cond) {
    std::cerr << "FAIL: " << msg << std::endl;
    std::exit(1);
  }
}

static uint32_t reference_crc32_bitwise(uint8_t op, std::string_view key,
                                        std::string_view payload) {
  uint32_t crc = 0xFFFFFFFFu;
  auto feed = [&](const void *data, size_t len) {
    const auto *p = static_cast<const uint8_t *>(data);
    for (size_t i = 0; i < len; ++i) {
      crc ^= p[i];
      for (int j = 0; j < 8; ++j) {
        crc = (crc >> 1) ^ (0xEDB88320u & (0u - (crc & 1u)));
      }
    }
  };
  feed(&op, 1);
  feed(key.data(), key.size());
  feed(payload.data(), payload.size());
  return ~crc;
}

void test_simple_append_recover() {
  std::string path = "test_simple.wal";
  std::filesystem::remove(path);

  {
    WriteAheadLog wal(path);
    wal.recover([](WalOp, std::string_view, std::string_view) {});
    wal.append(WalOp::PUT, "key1", "val1");
    wal.append(WalOp::DELETE_, "key2", "");
    wal.flush();
  }

  {
    WriteAheadLog wal(path);
    std::vector<std::string> ops;
    wal.recover([&](WalOp op, std::string_view key, std::string_view val) {
      ops.push_back(std::string(key) + ":" + std::string(val));
    });

    assert(ops.size() == 2);
    assert(ops[0] == "key1:val1");
    assert(ops[1] == "key2:");
    std::cout << "[PASS] Simple Append/Recover" << std::endl;
  }
  std::filesystem::remove(path);
}

void test_batch_append_recover() {
  std::cout << "TEST: batch_append_recover start" << std::endl;
  std::string path = "test_batch.wal";
  std::filesystem::remove(path);

  {
    WriteAheadLog wal(path);
    wal.recover([](WalOp, std::string_view, std::string_view) {});

    std::vector<BatchOp> batch;
    batch.push_back({WalOp::PUT, "bkey1", "bval1"});
    batch.push_back({WalOp::PUT, "bkey2", "bval2"});

    wal.append_batch(batch);
    wal.flush();
  }

  {
    WriteAheadLog wal(path);
    std::vector<std::string> ops;
    wal.recover([&](WalOp op, std::string_view key, std::string_view val) {
      ops.push_back(std::string(key) + ":" + std::string(val));
    });

    assert(ops.size() == 2);
    assert(ops[0] == "bkey1:bval1");
    assert(ops[1] == "bkey2:bval2");
    std::cout << "[PASS] Batch Append/Recover" << std::endl;
  }
  std::filesystem::remove(path);
}

void test_oversize_key_validation() {
  std::string path = "test_oversize_key.wal";
  std::filesystem::remove(path);

  std::string big_key(70000, 'k');
  {
    WriteAheadLog wal(path);
    wal.recover([](WalOp, std::string_view, std::string_view) {});
    wal.append(WalOp::PUT, "valid_before", "v_before");
    wal.flush();
    uint64_t size_before = wal.file_size();
    require_wal(size_before > 0, "Expected non-zero WAL size after valid append");

    bool threw_append = false;
    try {
      wal.append(WalOp::PUT, big_key, "payload");
    } catch (const std::invalid_argument &) {
      threw_append = true;
    }
    require_wal(threw_append,
                "wal.append with >65535-byte key must throw std::invalid_argument");
    require_wal(wal.file_size() == size_before,
                "wal.append oversize key must not write partial/corrupt frame");

    bool threw_batch = false;
    try {
      std::vector<BatchOp> batch = {
          {WalOp::PUT, "ok_in_batch", "v1"},
          {WalOp::PUT, big_key, "v2"},
      };
      wal.append_batch(batch);
    } catch (const std::invalid_argument &) {
      threw_batch = true;
    }
    require_wal(threw_batch,
                "wal.append_batch with >65535-byte key must throw std::invalid_argument");
    require_wal(wal.file_size() == size_before,
                "wal.append_batch oversize key must not write partial/corrupt frame");

    // Max valid key (65535 bytes) must still succeed
    std::string max_valid_key(65535, 'm');
    wal.append(WalOp::PUT, max_valid_key, "ok_max");
    wal.flush();
  }

  {
    WriteAheadLog wal(path);
    std::vector<std::pair<std::string, std::string>> ops;
    wal.recover([&](WalOp, std::string_view k, std::string_view v) {
      ops.emplace_back(std::string(k), std::string(v));
    });
    require_wal(ops.size() == 2, "Expected 2 valid ops recovered after rejected oversize keys");
    require_wal(ops[0].first == "valid_before" && ops[0].second == "v_before",
                "First recovered op mismatch");
    require_wal(ops[1].first.size() == 65535 && ops[1].second == "ok_max",
                "65535-byte key recovery mismatch");
  }
  std::filesystem::remove(path);
  std::cout << "[PASS] Oversize Key Validation" << std::endl;
}

void test_crc32_vectors() {
  std::string path = "test_crc32_vectors.wal";
  std::filesystem::remove(path);

  // Vector 1: op='1' (0x31), key="2345", payload="6789" -> IEEE CRC32("123456789") == 0xCBF43926
  const uint32_t expected_123456789 = 0xCBF43926u;
  require_wal(reference_crc32_bitwise(static_cast<uint8_t>('1'), "2345", "6789") ==
                  expected_123456789,
              "Bitwise reference must match standard IEEE CRC32 vector 0xCBF43926");

  std::string all_bytes(256, '\0');
  for (size_t i = 0; i < 256; ++i) {
    all_bytes[i] = static_cast<char>(i);
  }
  std::string large_payload(4096, '\0');
  for (size_t i = 0; i < large_payload.size(); ++i) {
    large_payload[i] = static_cast<char>((i * 131u + 17u) & 0xFFu);
  }

  struct TestVec {
    WalOp op;
    std::string key;
    std::string payload;
  };
  std::vector<TestVec> vectors = {
      {static_cast<WalOp>('1'), "2345", "6789"},
      {WalOp::DELETE_, "", ""},
      {WalOp::PUT, "alpha", "beta_gamma_delta"},
      {WalOp::PUT, all_bytes, large_payload},
  };

  {
    WriteAheadLog wal(path);
    for (const auto &tv : vectors) {
      wal.append(tv.op, tv.key, tv.payload);
    }
    wal.flush();
  }

  {
    std::ifstream in(path, std::ios::binary);
    require_wal(in.good(), "Failed to open CRC32 test WAL for header inspection");
    for (size_t idx = 0; idx < vectors.size(); ++idx) {
      LogHeader hdr{};
      in.read(reinterpret_cast<char *>(&hdr), sizeof(hdr));
      require_wal(in.gcount() == static_cast<std::streamsize>(sizeof(hdr)),
                  "Short read of LogHeader");
      uint32_t expected = reference_crc32_bitwise(
          static_cast<uint8_t>(vectors[idx].op), vectors[idx].key, vectors[idx].payload);
      require_wal(hdr.crc == expected,
                  "CRC32 mismatch at vector " + std::to_string(idx));
      if (idx == 0) {
        require_wal(hdr.crc == expected_123456789,
                    "IEEE 802.3 123456789 CRC32 mismatch");
      }
      in.seekg(static_cast<std::streamoff>(hdr.key_len) +
                   static_cast<std::streamoff>(hdr.payload_len),
               std::ios::cur);
    }
  }

  std::filesystem::remove(path);
  std::cout << "[PASS] Table-Driven IEEE CRC32 Vectors" << std::endl;
}

int main() {
  std::cout << "DEBUG: Starting test_wal..." << std::endl;
  try {
    test_simple_append_recover();
    // batch test will fail to compile until we add the method
    test_batch_append_recover();
    test_oversize_key_validation();
    test_crc32_vectors();
    std::cout << "All WAL Tests Passed!" << std::endl;
  } catch (const std::exception &e) {
    std::cerr << "Test Failed: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
