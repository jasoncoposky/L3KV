#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <winsock2.h>

#include "engine/mesh.hpp"
#include "engine/store.hpp"
#include "engine/sync_manager.hpp"
#include "http/http_server.hpp"
#include "observability/simple_metrics.hpp"
#include <iostream>
#include <lite3/ring.hpp>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

// Global metrics instance to be accessible from signal handler
SimpleMetrics global_metrics;

struct PeerConfig {
  uint32_t id;
  std::string host;
  int mesh_port;
  int http_port;
};

struct Config {
  std::string address = "127.0.0.1";
  unsigned short port = 8080;
  int min_threads = 4;
  int max_threads = 16;
  std::string wal_path = "data.wal";
  uint32_t node_id = 1;
  int mesh_port = 9090;
  int mesh_threads = 2; // Default to 2 for thread pool
  std::vector<PeerConfig> peers;
  std::string cluster_mode = "replicated"; // "replicated" or "sharded"
  int num_shards = 1;
};

Config load_config(const std::string &path) {
  Config cfg;
  try {
    std::ifstream f(path);
    if (!f.is_open()) {
      std::cerr << "Config file not found: " << path << ". Using defaults."
                << std::endl;
      return cfg;
    }

    json j;
    f >> j;

    cfg.address = j.value("address", cfg.address);
    cfg.port = j.value("port", cfg.port);
    cfg.min_threads = j.value("min_threads", cfg.min_threads);
    cfg.max_threads = j.value("max_threads", cfg.max_threads);
    cfg.wal_path = j.value("wal_path", cfg.wal_path);
    cfg.node_id = j.value("node_id", cfg.node_id);
    cfg.mesh_port = j.value("mesh_port", cfg.mesh_port);
    cfg.mesh_threads = j.value("mesh_threads", cfg.mesh_threads);

    if (j.contains("cluster")) {
      auto &c = j["cluster"];
      cfg.cluster_mode = c.value("mode", cfg.cluster_mode);
      cfg.num_shards = c.value("shards", cfg.num_shards);

      if (c.contains("peers")) {
        auto &p_node = c["peers"];
        // Handle array format
        if (p_node.is_array()) {
          for (auto &p : p_node) {
            PeerConfig pc;
            pc.id = p.value("id", 0u);
            pc.host = p.value("host", "127.0.0.1");
            pc.mesh_port = p.value("mesh_port", 9090);
            pc.http_port = p.value("http_port", 8080);
            if (pc.id != 0) {
              cfg.peers.push_back(pc);
            }
          }
        }
      }
    }
    // Also check root "peers" if not found in cluster?
    if (cfg.peers.empty() && j.contains("peers") && j["peers"].is_array()) {
      for (auto &p : j["peers"]) {
        PeerConfig pc;
        pc.id = p.value("id", 0u);
        pc.host = p.value("host", "127.0.0.1");
        pc.mesh_port = p.value("mesh_port", 9090);
        pc.http_port = p.value("http_port", 8080);
        if (pc.id != 0) {
          cfg.peers.push_back(pc);
        }
      }
    }

    std::cout << "Loaded config from " << path << std::endl;
  } catch (const std::exception &e) {
    std::cerr << "Error loading config: " << e.what() << ". Using defaults."
              << std::endl;
  }
  return cfg;
}

int main(int argc, char *argv[]) {
  try {
    std::string config_path = "config.json";
    if (argc > 1) {
      config_path = argv[1];
    }

    Config cfg = load_config(config_path);

    std::cout << "Starting Lite3 Service..." << std::endl;
    std::cout << "  Address: " << cfg.address << ":" << cfg.port << std::endl;
    std::cout << "  Threads: " << cfg.min_threads << "-" << cfg.max_threads
              << "(Dynamic)" << std::endl;
    std::cout << "  WAL Path: " << cfg.wal_path << std::endl;
    std::cout << "  Node ID: " << cfg.node_id << std::endl;
    std::cout << "  Mesh Port: " << cfg.mesh_port << std::endl;
    std::cout << "  Mesh Threads: " << cfg.mesh_threads << std::endl;

    // Register metrics with lite3-cpp
    lite3cpp::set_metrics(&global_metrics);

    // Initialize Database Engine
    l3kv::Engine db(cfg.wal_path, cfg.node_id);

    // Initialize Mesh and SyncManager (Replication)
    boost::asio::io_context io_context;
    l3kv::Mesh mesh(io_context, cfg.node_id, cfg.mesh_port);
    l3kv::SyncManager sync(mesh, db, cfg.node_id);

    mesh.set_on_message([&](l3kv::NodeID from, l3kv::Lane lane,
                            const std::vector<uint8_t> &payload) {
      if (lane == l3kv::Lane::Control) {
        sync.handle_message(from, payload);
      }
    });

    mesh.listen();
    std::cout << "DEBUG: Mesh listening." << std::endl;

    // Start Mesh IO thread POOL
    std::vector<std::thread> mesh_pool;
    std::cout << "DEBUG: Starting Mesh pool with " << cfg.mesh_threads
              << " threads." << std::endl;
    for (int i = 0; i < cfg.mesh_threads; ++i) {
      mesh_pool.emplace_back([&io_context, i]() {
        // std::cout << "DEBUG: Mesh worker " << i << " started.\n";
        try {
          io_context.run();
        } catch (const std::exception &e) {
          std::cerr << "DEBUG: Mesh io_context error: " << e.what()
                    << std::endl;
        }
      });
    }

    // Map of peers for HTTP redirection: ID -> {Host, HTTP_Port}
    std::map<uint32_t, std::pair<std::string, int>> http_peers;

    // Connect to peers and populate map
    for (const auto &peer : cfg.peers) {
      std::cout << "Connecting to peer " << peer.id << " at " << peer.host
                << ":" << peer.mesh_port << " (HTTP: " << peer.http_port << ")"
                << std::endl;

      http_peers[peer.id] = {peer.host, peer.http_port};

      try {
        mesh.connect(peer.id, peer.host, peer.mesh_port);
      } catch (const std::exception &e) {
        std::cerr << "Failed to connect to peer " << peer.id << ": " << e.what()
                  << " (will wait for them to connect to us)" << std::endl;
      }
    }

    std::cout << "DEBUG: Starting SyncManager..." << std::endl;
    sync.start();
    std::cout << "DEBUG: SyncManager started." << std::endl;

    // Build Consistent Hash Ring
    std::shared_ptr<lite3::ConsistentHash> ring; // Updated namespace
    if (cfg.cluster_mode == "sharded") {
      ring = std::make_shared<lite3::ConsistentHash>();
      ring->add_node(cfg.node_id); // Add self

      // Add peers to ring
      for (const auto &peer : cfg.peers) {
        ring->add_node(peer.id);
      }
      std::cout << "Cluster Mode: SHARDED. Ring Size: " << ring->size()
                << " vnodes." << std::endl;
    } else {
      std::cout << "Cluster Mode: REPLICATED (Geo/Local)." << std::endl;
    }

    // Start HTTP Server
    std::cout << "DEBUG: Starting HTTP Server..." << std::endl;
    http_server::http_server server(db, cfg.address, cfg.port, cfg.min_threads,
                                    cfg.max_threads, ring, cfg.node_id,
                                    http_peers);
    std::cout << "Lite3 Service listening on :" << cfg.port << std::endl;
    server.run();

    // Cleanup
    sync.stop();
    io_context.stop();

    for (auto &t : mesh_pool) {
      if (t.joinable())
        t.join();
    }

    std::cout << "\nServer stopping gracefully..." << std::endl;
    db.flush();
    global_metrics.dump_metrics();
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}