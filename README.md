# L3KV: High-Performance Persistent Key-Value Store

![L3KV Logo](assets/l3kv_logo.jpg)

**L3KV** is a high-performance, persistent Key-Value service built on **Modern C++23** and the **Lite³** serialization library. It leverages zero-deserialization editing to modify large JSON documents in-place with microsecond latency.

## 🚀 Features
*   **Dynamic Scaling:** Predictive thread pool using a Kalman Filter to auto-scale resources.
*   **Zero-Copy WAL:** Write-Ahead Log integrated with `libconveyor`'s reserve/commit API, achieving **67k+ ops/sec** with 0 ms hot-path latency.
*   **Non-Allocating KeyBuilder:** High-performance meta-key generation with zero heap pressure in the hot write path.
*   **Graceful Durability:** Guaranteed persistence on shutdown (`SIGINT`, `SIGTERM`).
*   **Zero-Parse Mutations:** Update a single field in a 10MB document in **< 1 µs**.
*   **ZeroMQ Asynchronous Interface:** High-performance binary transport layer using `ZMQ_ROUTER` and `ZMQ_DEALER` patterns.
*   **Observability:** Built-in metrics endpoint and **HTML Dashboard**.


## 💾 Durability & Direct Append-Only WAL

L3KV utilizes a high-performance **Direct Append-Only Write-Ahead Log (WAL)** designed for zero-copy durability without intermediate thread-pool handoff:

*   **Fast Path:** Writes append directly to an append-only file stream (`data.wal` or configured path) protected by fine-grained thread-safe locking, eliminating secondary background thread pool overhead.
*   **Sequential Durability:** Sequential streaming writes leverage OS kernel dirty page writeback, with explicit `flush()` and sync hooks during critical transactional commits.
*   **Shutdown:** On receiving a shutdown signal (`SIGINT`, `SIGTERM`, `SIGBREAK`), the engine flushes and closes file descriptors gracefully.
*   **Crash Recovery:** On startup, the service sequentially scans and recovers the WAL using buffered chunk replay. Partial or truncated writes at EOF are detected via CRC validation and safely truncated to the last consistent state.

## 🏛️ Architectural Learnings & Engine Roadmap

Evaluations conducted with the **L3KVG Bifurcated Native Graph Substrate** and enterprise catalog benchmarks have yielded key architectural insights documented in [`docs/2026-10-10-l3kv-architectural-learnings-and-roadmap.md`](docs/2026-10-10-l3kv-architectural-learnings-and-roadmap.md):

1. **Substrate Separation:** L3KV acts as the durable cold storage engine for document payloads and secondary index mappings, while L3KVG handles topological adjacency (sub-20ns hop latency) via slotted 4KB cache-aligned pages.
2. **Metadata Inlining:** Demonstrates the value of small-record inlining directly inside primary key records to eliminate pointer dereferencing on hot lookups.
3. **Pure Binary Buffers:** Complete transition from JSON text strings to native `lite3cpp::Buffer` eliminates serialization overhead across inter-process and on-disk paths.
4. **Cache-Conscious B-Tree Layouts:** Roadmap incorporates slotted 4KB block allocations to maximize L1/L2 cache locality during range scans.

## 🌍 Geo-Distributed & Partition Tolerant
L3KV is designed for global scale, running across multiple regions with unreliable networks:
*   **Packet-Level Efficiency:** Anti-Entropy usage of Merkle Trees ensures ONLY changed data is transmitted, minimizing WAN bandwidth costs.
*   **Clock Skew Resistance:** **Hybrid Logical Clocks (HLC)** provide causality guarantees even when physical clocks drift across data centers.
*   **Partition Tolerance:** The system is **AP (Available, Partition-Tolerant)**. Writes are accepted locally and lazily propagated.
*   **Eventual Consistency:** Convergence is guaranteed via the Active Anti-Entropy (AAE) gossip protocol.

## 🔄 Multi-Master Replication
L3KV supports active-active replication with eventual consistency features:
*   **Active Anti-Entropy (AAE):** Uses Merkle Trees to efficiently detect and synchronize divergent data between nodes.
*   **Conflict Resolution:** Last-Writer-Wins (LWW) based on Hybrid Logical Clocks (HLC).
*   **Tombstones:** Propagates deletions across the cluster to ensure consistency.
*   **Mesh Networking:** High-performance binary protocol over TCP for inter-node communication.

## 📊 Observability

L3KV includes a comprehensive observability suite for real-time monitoring.

### Dashboard
A zero-dependency, real-time visual monitor is available at `/dashboard`.
*   **Live Charts:** Visualizes throughput (bytes in/out) and write latency.
*   **Replication Stats:** Live counters for Keys Repaired and Sync Events.
*   **Mesh Traffic:** Real-time In/Out throughput for peer communication.
*   **KPI Cards:** Tracks active connections, total errors (4xx/5xx), and current throughput.
*   **Dark Mode:** Sleek, modern UI.

[Access Dashboard](http://localhost:8080/dashboard)
 
![Lite3 Service Dashboard](assets/dashboard_screenshot.png)

### Metrics API
`GET /metrics` produces a JSON payload compatible with monitoring systems.
```json
{
  "system": { "active_connections": 5, "thread_count": 8 },
  "replication": {
     "keys_repaired": 12,
     "sync_ops": { "divergent_bucket": 5, "sync_init": 20 }
  },
  "throughput": { "bytes_received_total": 10240, "http_errors_4xx": 0 }
}
```

## 🛠️ Build & Run (Windows)

### Prerequisites
*   CMake (3.20+)
*   Visual Studio 2022 (C++20/23 support)
*   ZeroMQ (4.3.5) and cppzmq (integrated via FetchContent)
*   `lite3-cpp` library (linked via CMake)


### Build
```powershell
mkdir build
cd build
cmake -G "Visual Studio 17 2022" -A x64 -DCMAKE_BUILD_TYPE=Release ..
cmake --build . --config Release --target l3svc
```

### Run
### One-step Run
```powershell
.\Release\l3svc.exe [config_path]
# Defaults to config.json if not specified
```
Ensure `config.json` is present in the working directory or provide a path.

### Configuration (`config.json`)
```json
{
  "address": "127.0.0.1",
  "port": 8080,
  "node_id": 1,
  "peers": [
      { "id": 2, "host": "127.0.0.1", "mesh_port": 9091, "http_port": 8081 },
      { "id": 3, "host": "127.0.0.1", "mesh_port": 9092, "http_port": 8082 }
  ],
  "cluster": {
      "mode": "sharded",       // "sharded" or "standalone"
      "shards": 100            // Number of virtual nodes for Consistent Hashing
  },
  "min_threads": 4,
  "max_threads": 16,
  "mesh_threads": 2,
  "wal_path": "node1.wal"
}
```

## 🌐 Sharded Clustering

L3KV supports horizontal scaling via **Consistent Hashing**.

*   **Topology Awareness:** The cluster map is served at `/cluster/map`.
*   **Smart Clients:** Clients (like `liblite3client`) automatically download the topology and route requests directly to the owner node, achieving **O(1)** routing latency.
*   **Redirection:** If a legacy client hits the wrong node, it receives a `307 Temporary Redirect` to the correct owner.

### Running a Cluster (3-Node Example)

1.  **Start the Cluster:**
    ```powershell
    .\start_cluster.ps1
    ```
    This launches 3 instances of `l3svc` on ports 8080/9090, 8081/9091, and 8082/9092.

2.  **Run Smart Client Benchmark:**
    ```powershell
    .\build\Release\bench_ycsb.exe --threads 16 --ops 100000 --hosts 127.0.0.1:8080,127.0.0.1:8081,127.0.0.1:8082
    ```

## 🔌 API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/kv/{key}` | Retrieve document as JSON (or redirect if sharded). |
| `PUT` | `/kv/{key}` | Store JSON document. |
| `DELETE` | `/kv/{key}` | Delete document. |
| `POST` | `/kv/{key}?op=set_int&field={path}&val={v}` | fast-path integer update. |
| `GET` | `/cluster/map` | JSON map of cluster topology (nodes, shards). |
| `GET` | `/metrics` | Real-time JSON metrics. |
| `GET` | `/dashboard` | Visual Dashboard. |


## ⚡ Performance Metrics

The L3KV core engine is optimized for high concurrency, zero-copy `lite3-cpp` reads, and non-blocking WAL compaction.

### Linux (`Release -O3`, 7-Trial `bench_l2_l3kv` & Native Runners)

| Operation / Benchmark | Throughput / Latency | Notes |
| :--- | :---: | :--- |
| **`Engine::put` (256B doc, `/dev/shm` WAL)** | **5.55 µs** (**180,061 ops/s**) | **9.41x** faster than initial `52.20 µs` baseline |
| **`Engine::get` (populated shard map)** | **0.41 µs** (**2,492,601 ops/s**) | **3.00x** faster than `1.23 µs` baseline |
| **Tagged Prefix Scan (`{zone1}:d:`)** | **207.48 ns** (**4.82 Mops/s**) | Single-shard reverse scan |
| **Untagged Prefix Scan (`d:` across 8 shards)** | **1,232.23 ns** (**811,752 ops/s**) | Direct `shared_lock` scan |
| **WAL Compaction (`L2.3_compact_wal`)** | **245.1 MiB/s** (`850.6 ms` p50) | Non-blocking concurrent readers: **7.61 µs (`p50`) / 20.27 µs (`p99`)** |
| **`engine_bench` (10k async ops)** | **185,582 ops/sec** | `0.0539 s` total |
| **`bench_engine_threaded` (1.6M ops, 16 threads)** | **91,362 ops/sec** | `17.51 s` total |


## 📊 Benchmarks

### Engine Core (Linux Release)
```bash
./build_rel/engine_bench
# Result: 185,582 ops/sec (10k async ops)
./build_rel/bench_engine_threaded
# Result: 91,362 ops/sec (1.6M async ops, 16 threads)
```

### Historical Benchmark Summary (Windows 11, Ryzen 7 5700X, Loopback)

| Operation | Throughput / Latency | Description |
| :--- | :--- | :--- |
| **Raw Engine Put/Patch** | **~307,000 ops/sec** | Thread-Per-Core + Lock-Free EBR. |
| **Standalone KV (ZeroMQ)** | **~192,000 ops/sec** | Async binary transport (Batch 1000). |
| **Distributed Edge (L3KVG)** | **~97,000 ops/sec** | ZeroMQ-coordinated 3-node cluster. |
| **Zero-Parse Patch** | **< 1 µs** | `lite3-cpp` in-place binary edit. |

**Note on Performance:** The shift to ZeroMQ and direct `shared_lock` shard access has bypassed both HTTP/1.1 serialization bottlenecks and WAL-compaction reader stalls, keeping concurrent reader tail latency under **21 µs (`p99`)** even during 215 MiB $\rightarrow$ 112 MiB WAL compaction.

