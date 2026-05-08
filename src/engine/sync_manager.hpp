#pragma once

#include "json.hpp"
#include "mesh.hpp"
#include "store.hpp"
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <string_view>
#include <thread>
#include <vector>

#include "observability.hpp"

namespace l3kv {

class SyncManager {
  IMesh &mesh_;
  Engine &engine_;
  std::atomic<bool> running_{false};
  std::thread bg_thread_;
  uint32_t node_id_;

  enum MsgType : uint8_t {
    SYNC_INIT = 0x01,
    SYNC_REQ_NODE = 0x02,
    SYNC_REP_NODE = 0x03,
    SYNC_REQ_BUCKET = 0x04,
    SYNC_REP_BUCKET = 0x05,
    SYNC_GET_VAL = 0x06,
    SYNC_PUT_VAL = 0x07
  };

public:
  SyncManager(IMesh &mesh, Engine &engine, uint32_t node_id)
      : mesh_(mesh), engine_(engine), node_id_(node_id) {}

  void start() {
    if (running_)
      return;
    running_ = true;
    bg_thread_ = std::thread([this]() { run_loop(); });
  }

  void stop() {
    running_ = false;
    if (bg_thread_.joinable())
      bg_thread_.join();
  }

  void trigger_gossip() {
    std::random_device rd;
    std::mt19937 rng(rd());
    // Pick random peer
    auto peers = mesh_.get_active_peers();
    if (!peers.empty()) {
      // Send to one random peer
      std::uniform_int_distribution<size_t> dist(0, peers.size() - 1);
      NodeID target = peers[dist(rng)];
      send_sync_init(target);
    }
  }

  // Handle incoming Control messages
  void handle_message(NodeID /*ignored_from*/,
                      const lite3cpp::Buffer &payload_buf) {
    if (payload_buf.size() < 5)
      return;
    
    // Copy to vector for internal handlers that expect vector
    std::vector<uint8_t> payload(payload_buf.data(), payload_buf.data() + payload_buf.size());

    MsgType type = (MsgType)payload[0];
    NodeID sender_id;
    std::memcpy(&sender_id, &payload[1], 4);

    try {
      switch (type) {
      case SYNC_INIT:
        on_sync_init(sender_id, payload);
        break;
      case SYNC_REQ_NODE:
        on_req_node(sender_id, payload);
        break;
      case SYNC_REP_NODE:
        on_rep_node(sender_id, payload);
        break;
      case SYNC_REQ_BUCKET:
        on_req_bucket(sender_id, payload);
        break;
      case SYNC_REP_BUCKET:
        on_rep_bucket(sender_id, payload);
        break;
      case SYNC_GET_VAL:
        on_get_val(sender_id, payload);
        break;
      case SYNC_PUT_VAL:
        on_put_val(sender_id, payload);
        break;
      }
    } catch (const std::exception &e) {
      std::cerr << "Sync Error from " << sender_id << ": " << e.what() << "\n";
    }
  }

private:
  void run_loop() {
    std::cout << "[SyncManager] Started gossip loop.\n";

    while (running_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2000)); // Every 2s
      if (!running_)
        break;

      trigger_gossip();
    }
  }

  void send_sync_init(NodeID target) {
    uint64_t root = engine_.get_merkle_root_hash();
    std::vector<uint8_t> pay;
    pay.push_back(SYNC_INIT);
    pay.resize(1 + 4 + 8);
    std::memcpy(&pay[1], &node_id_, 4);
    std::memcpy(&pay[5], &root, 8);
    lite3cpp::Buffer buf_pay(std::move(pay));
    mesh_.send(target, Lane::Control, std::move(buf_pay));

#ifndef LITE3CPP_DISABLE_OBSERVABILITY
    if (auto *m = lite3cpp::g_metrics.load(std::memory_order_relaxed)) {
      m->increment_sync_ops("sync_init");
    }
#endif
  }

  // Old broadcast method replaced
  /*
  void send_sync_init_broadcast() {
    ...
  }
  */

  // Handlers
  void on_sync_init(NodeID from, const std::vector<uint8_t> &buf) {
    if (buf.size() < 13)
      return;
    uint64_t their_root;
    std::memcpy(&their_root, &buf[5], 8);

    uint64_t my_root = engine_.get_merkle_root_hash();
    if (my_root == their_root) {
      // In sync.
      return;
    }
    // std::cerr << "[Sync] Root Mismatch. My:" << my_root
    //           << " Theirs:" << their_root << "\n";

    // Mismatch. Drill down. Request Level 1 nodes.
    // We could request all 16 L1 nodes.
    // Or 1 by 1. Requesting all 16 is small (16 * 8 = 128 bytes).
    // Protocol: Request Level 1.
    send_req_node(from, 1, 0); // Level 1, Index Base 0
  }

  void send_req_node(NodeID to, uint8_t level, uint32_t idx_base) {
    // Request a block of nodes?
    // Let's simplified: "Level" implies requesting ALL nodes at that level?
    // No, exponential.
    // "Request Children of Node X".
    // Node X is at (Level-1, Index).
    // Let's say we request specific node hash verification?
    // The gossip protocol usually is:
    // A -> B: RootHash
    // B -> A: RootHash mismatch. Here are my L1 Hashes.
    // A -> B: L1 Hash mismatch at index `i`. Here are my L2 children of `i`.
    // ...

    // Let's invert: If I detect mismatch, *I* request information to find
    // delta. I received TheirRoot. It differs. I ask "Give me your Level 1
    // Hashes".
    std::vector<uint8_t> pay;
    uint32_t parent = idx_base; // Fix undeclared identifier
    pay.push_back(SYNC_REQ_NODE);
    pay.resize(1 + 4 + 1 + 4);
    std::memcpy(&pay[1], &node_id_, 4);
    pay[5] = level;
    std::memcpy(&pay[6], &parent, 4);

    lite3cpp::Buffer buf_pay(std::move(pay));
    mesh_.send(to, Lane::Control, std::move(buf_pay));
  }

  void on_req_node(NodeID from, const std::vector<uint8_t> &buf) {
    if (buf.size() < 10)
      return;
    uint8_t level = buf[5];
    uint32_t parent_idx = 0;
    std::memcpy(&parent_idx, &buf[6], 4);

    // Respond with all children of 'parent_idx'.
    // Branching factor = 16.
    // Children indices: parent_idx * 16 + (0..15)
    // Exception: Level 1 children are 0..15 (Parent 0).
    // If level > 4, error.

    std::vector<uint8_t> rep;
    rep.push_back(SYNC_REP_NODE);
    rep.resize(1 + 4 + 1 + 3 + 4); // Type+ID+Lvl+Pad+Parent
    std::memcpy(&rep[1], &node_id_, 4);
    rep[5] = level;
    // Padding 0
    std::memset(&rep[6], 0, 3);
    uint32_t p = parent_idx;
    std::memcpy(&rep[9], &p, 4);

    // Append 16 hashes
    for (int i = 0; i < 16; ++i) {
      size_t child_idx = parent_idx * 16 + i;
      uint64_t h = engine_.get_merkle_node(level, child_idx);
      size_t old_sz = rep.size();
      rep.resize(old_sz + 8);
      std::memcpy(&rep[old_sz], &h, 8);
    }
    lite3cpp::Buffer buf_rep(std::move(rep));
    mesh_.send(from, Lane::Control, std::move(buf_rep));
  }

  void on_rep_node(NodeID from, const std::vector<uint8_t> &buf) {
    if (buf.size() < 13) // Header(5) + Lvl(1) + Pad(3) + Parent(4) = 13? No.
      // Format: [T:1][ID:4][Lvl:1][Pad:3][Parent:4][Hashes...]
      // 1+4+1+3+4 = 13. Correct.
      return;
    uint8_t level = buf[5];
    uint32_t parent_idx;
    std::memcpy(&parent_idx, &buf[9], 4);

    const uint8_t *ptr = &buf[13];
    for (int i = 0; i < 16; ++i) {
      uint64_t their_h;
      std::memcpy(&their_h, ptr + i * 8, 8);

      size_t child_idx = parent_idx * 16 + i;
      uint64_t my_h = engine_.get_merkle_node(level, child_idx);

      if (my_h != their_h) {
        // Found mismatch at child `i`.
        // If level is 4 (Leaves), then this child IS a bucket.
        // child_idx is bucket_idx.
        if (level == 4) {
          // Divergent bucket!
          // std::cerr << "[Sync] Divergent Bucket " << child_idx << "\n";
#ifndef LITE3CPP_DISABLE_OBSERVABILITY
          if (auto *m = lite3cpp::g_metrics.load(std::memory_order_relaxed)) {
            m->increment_sync_ops("divergent_bucket");
          }
#endif
          send_req_bucket(from, (uint32_t)child_idx);
        } else {
          // Recurse. Ask for Level+1, Parent=child_idx.
          // std::cerr << "[Sync] Recursing L" << (int)level << " -> " <<
          // child_idx
          //           << "\n";
          send_req_node(from, level + 1, (uint32_t)child_idx);
        }
      }
    }
  }

  void send_req_bucket(NodeID to, uint32_t bucket_idx) {
    std::vector<uint8_t> pay;
    pay.push_back(SYNC_REQ_BUCKET);
    pay.resize(1 + 4 + 4);
    std::memcpy(&pay[1], &node_id_, 4);
    std::memcpy(&pay[5], &bucket_idx, 4);
    lite3cpp::Buffer buf_pay(std::move(pay));
    mesh_.send(to, Lane::Control, std::move(buf_pay));
  }

  void on_req_bucket(NodeID from, const std::vector<uint8_t> &buf) {
    if (buf.size() < 9)
      return;
    uint32_t bucket_idx;
    std::memcpy(&bucket_idx, &buf[5], 4);

    // Get keys
    auto keys = engine_.get_bucket_keys(bucket_idx);

    // Reply with key hashes/versions?
    // Or just [Key, ValHash].
    std::vector<uint8_t> pay;
    pay.push_back(SYNC_REP_BUCKET);
    pay.resize(1 + 4 + 4);
    std::memcpy(&pay[1], &node_id_, 4);
    std::memcpy(&pay[5], &bucket_idx, 4);

    // Count
    uint32_t count = (uint32_t)keys.size();
    size_t old = pay.size();
    pay.resize(old + 4);
    std::memcpy(&pay[old], &count, 4);

    for (auto &pair : keys) {
      if (pair.first.size() >= 5 &&
          pair.first.compare(pair.first.size() - 5, 5, ":meta") == 0) {
        continue; // Skip internal meta keys
      }
      // Format: [KeyLen:2][Key][Hash:8]
      uint16_t klen = (uint16_t)pair.first.size();
      size_t p = pay.size();
      pay.resize(p + 2 + klen + 8);
      std::memcpy(&pay[p], &klen, 2);
      std::memcpy(&pay[p + 2], pair.first.data(), klen);
      std::memcpy(&pay[p + 2 + klen], &pair.second, 8);
    }
    lite3cpp::Buffer buf_pay(std::move(pay));
    mesh_.send(from, Lane::Heavy, std::move(buf_pay)); // Use Heavy for data listing
  }

  void on_rep_bucket(NodeID from, const std::vector<uint8_t> &buf) {
    // Header: [T:1][ID:4][Bkt:4][Cnt:4][Keys...]
    // 1+4+4+4 = 13.
    size_t pos = 13;
    if (buf.size() < 13)
      return;

    uint32_t count;
    std::memcpy(&count, &buf[9], 4);
    std::cerr << "[Sync] Received Bucket Rep. Count: " << count << "\n";

    for (uint32_t i = 0; i < count; ++i) {
      if (pos + 10 > buf.size())
        break;
      uint16_t klen;
      std::memcpy(&klen, &buf[pos], 2);
      if (pos + 2 + klen + 8 > buf.size())
        break;

      std::string key((const char *)&buf[pos + 2], klen);
      uint64_t their_h;
      std::memcpy(&their_h, &buf[pos + 2 + klen], 8);

      pos += 2 + klen + 8;

      // Check local
      // We can't easily check hash without getting blob?
      // Engine::get_bucket_keys scanned everything.
      // Let's just use Engine::get() -> hash.
      // Or optimize?
      // For MVP: Simple.

      // Wait, Engine::get() returns Buffer.
      // We need to re-hash.
      auto local_buf = engine_.get(key);
      uint64_t my_h = 0;
      if (local_buf.size() > 0) {
        my_h = fnv1a_64(local_buf.data(),
                        local_buf.size()); // Duplicate hash logic?
        // Ideally expose hash_blob in public or allow getting hash.
        // Re-hashing is safe for now.
      }

      if (my_h != their_h) {
        std::cerr << "[Sync] Requesting Key: " << key << "\n";
        send_get_val(from, key);
      } else {
        std::cerr << "[Sync] Key Match: " << key << "\n";
      }
    }
  }

  void send_get_val(NodeID to, const std::string &key) {
    std::vector<uint8_t> pay;
    pay.push_back(SYNC_GET_VAL);
    pay.resize(5);
    std::memcpy(&pay[1], &node_id_, 4);
    pay.insert(pay.end(), key.begin(), key.end());
    lite3cpp::Buffer buf_pay(std::move(pay));
    mesh_.send(to, Lane::Heavy, std::move(buf_pay));
  }

  void on_get_val(NodeID from, const std::vector<uint8_t> &buf) {
    if (buf.size() < 5)
      return;
    std::string key((const char *)&buf[5], buf.size() - 5);
    std::cerr << "[Sync] OnGetVal: " << key << "\n";

    // To get Meta:
    auto meta = engine_.get(key + ":meta");
    if (meta.size() == 0) {
      std::cerr << "[Sync] Key (Meta) not found locally: " << key << "\n";
      return;
    }

    auto val = engine_.get(key);
    // Even if val is empty, if meta exists, we send it (as empty val + meta).

    std::vector<uint8_t> pay;
    pay.push_back(SYNC_PUT_VAL);
    pay.resize(5);
    std::memcpy(&pay[1], &node_id_, 4);
    uint16_t klen = (uint16_t)key.size();
    size_t p = pay.size();
    pay.resize(p + 2);
    std::memcpy(&pay[p], &klen, 2);
    pay.insert(pay.end(), key.begin(), key.end());

    // Meta
    std::string meta_s(meta.data(), meta.data() + meta.size());
    uint16_t mlen = (uint16_t)meta_s.size();
    size_t pos = pay.size();
    pay.resize(pos + 2);
    std::memcpy(&pay[pos], &mlen, 2);
    pay.insert(pay.end(), meta_s.begin(), meta_s.end());

    // Value
    if (val.size() > 0) {
      pay.insert(pay.end(), val.data(), val.data() + val.size());
    }

    std::cerr << "[Sync] Sending PutVal for " << key << " Size: " << pay.size()
              << "\n";
    lite3cpp::Buffer buf_pay(std::move(pay));
    mesh_.send(from, Lane::Heavy, std::move(buf_pay));
  }

  void on_put_val(NodeID from, const std::vector<uint8_t> &buf) {
    size_t pos = 5;
    if (pos + 2 > buf.size())
      return;
    uint16_t klen;
    std::memcpy(&klen, &buf[pos], 2);
    pos += 2;

    if (pos + klen > buf.size())
      return;
    std::string key((const char *)&buf[pos], klen);
    pos += klen;

    if (pos + 2 > buf.size())
      return;
    uint16_t mlen;
    std::memcpy(&mlen, &buf[pos], 2);
    pos += 2;

    if (pos + mlen > buf.size()) {
      std::cerr << "[Sync] PutVal short meta payload\n";
      return;
    }
    std::string meta((const char *)&buf[pos], mlen);
    pos += mlen;

    std::vector<uint8_t> val(buf.begin() + pos, buf.end());

    // Construct Mutation
    Mutation m;
    m.key = key;
    m.value = val;

    auto parsed = parse_meta(meta);
    std::cerr << "[Sync] Parsed TS for " << key << ": " << parsed.ts.wall_time
              << "." << parsed.ts.logical << "." << parsed.ts.node_id
              << " Tombstone:" << parsed.is_tombstone << "\n";
    m.timestamp = parsed.ts;
    m.is_delete = parsed.is_tombstone;

    std::cerr << "[Sync] Applying Mutation Key: " << key << "\n";
    engine_.apply_mutation(m);

#ifndef LITE3CPP_DISABLE_OBSERVABILITY
    if (auto *metrics = lite3cpp::g_metrics.load(std::memory_order_relaxed)) {
      metrics->increment_keys_repaired();
    }
#endif
  }

  struct ParsedMeta {
    Timestamp ts;
    bool is_tombstone;
  };

  ParsedMeta parse_meta(const std::string &meta_bytes) {
    if (meta_bytes.empty())
      return {{0, 0, 0}, false};

    try {
      std::vector<uint8_t> data(meta_bytes.begin(), meta_bytes.end());
      lite3cpp::Buffer buf(std::move(data));

      double w = 0.0;
      uint32_t l = 0;
      uint32_t n = 0;
      bool tombstone = false;

      // Type check and get
      if (buf.get_type(0, "ts") == lite3cpp::Type::Float64) {
        w = buf.get_f64(0, "ts");
      } else if (buf.get_type(0, "ts") == lite3cpp::Type::Int64) {
        w = (double)buf.get_i64(0, "ts");
      }
      if (buf.get_type(0, "l") == lite3cpp::Type::Int64 ||
          buf.get_type(0, "l") == lite3cpp::Type::Float64) {
        l = (uint32_t)buf.get_i64(0, "l");
      }
      if (buf.get_type(0, "n") == lite3cpp::Type::Int64 ||
          buf.get_type(0, "n") == lite3cpp::Type::Float64) {
        n = (uint32_t)buf.get_i64(0, "n");
      }
      if (buf.get_type(0, "tombstone") == lite3cpp::Type::Bool) {
        tombstone = buf.get_bool(0, "tombstone");
      }

      return {{(int64_t)w, l, n}, tombstone};
    } catch (...) {
      // Fallback or error
      return {{0, 0, 0}, false};
    }
  }
};

} // namespace l3kv
