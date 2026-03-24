# L3KV: High-Performance Persistent Key-Value Store

![L3KV Logo](assets/l3kv_logo.jpg)

**L3KV** is a high-performance, persistent Key-Value service built on **Modern C++23** and the **Lite³** serialization library. It leverages zero-deserialization editing to modify large JSON documents in-place with microsecond latency.

## 🚀 Features
*   **Dynamic Scaling:** Predictive thread pool using a Kalman Filter to auto-scale resources.
*   **Zero-Copy WAL:** Write-Ahead Log integrated with `libconveyor`'s reserve/commit API, achieving **67k+ ops/sec** with 0 ms hot-path latency.
*   **Non-Allocating KeyBuilder:** High-performance meta-key generation with zero heap pressure in the hot write path.
*   **Graceful Durability:** Guaranteed persistence on shutdown (`SIGINT`, `SIGTERM`).
*   **Zero-Parse Mutations:** Update a single field in a 10MB document in **< 1 µs**.
*   **HTTP/1.1 Interface:** Standard REST API (`GET`, `PUT`, `DELETE`, `PATCH`).
*   **Observability:** Built-in metrics endpoint and **HTML Dashboard**.

## 💾 Durability & Persistence

L3KV uses a high-performance **Buffered Write-Ahead Log (WAL)**.

*   **Fast Path:** Writes are appended to a 20MB in-memory buffer (`libconveyor`), achieving **0 ms latency** for the database engine.
*   **Flush Policy:**
    *   **Background:** The OS flushes dirty pages to disk asynchronously.
    *   **Shutdown:** On receiving a signal (`SIGINT`, `SIGTERM`, `SIGBREAK`), the server forcefully flushes all buffers to `data.wal`.
*   **Trade-off:**
    *   **Graceful Shutdown:** 100% Data Durability GUARANTEED.
    *   **Hard Crash / Power Loss:** Potential loss of buffered data (up to 20MB) that hasn't been flushed by the OS. The WAL integrity remains protected by CRC32, so no corruption occurs—only lost recent writes.

### Crash Recovery
*   **Startup:** The service scans `data.wal` using a fast-read buffer.
*   **Corrupt Entries:** Partial writes at the end of the log (from a hard crash) are detected via CRC32 mismatch and discarded, verifying the database to the last consistent state.

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
*   Boost Libraries (1.70+): `asio`, `beast`, `system`
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


## 📊 Benchmarks

### How to Run

1.  **Build** (Release Mode):
    ```powershell
    cd L3KV
    cmake --build build --config Release
    ```

2.  **Single Node:**
    *   Start Server: `./build/Release/l3svc.exe`
    *   Run Workload A: `./build/Release/bench_ycsb.exe --threads 1`

3.  **Cluster (3-Node):**
    *   Start Cluster: `./start_cluster.ps1`
    *   Run Distributed Workload: `./build/Release/bench_ycsb.exe --threads 8 --hosts 127.0.0.1:8080,127.0.0.1:8081,127.0.0.1:8082`

Benchmark results (Windows 11, Ryzen 7 5700X, Loopback):

| Operation | Latency (Server Internal) | Description |
| :--- | :--- | :--- |
| **Document Write** | **< 1 µs** | `lite3-cpp` zero-parse update. |
| **JSON Serialization** | **~3 µs** | Converting buffer to JSON string. |
| **Request Handler** | **~50 µs** | Total time in `http_server::handle_request`. |
| **Network RTT** | **~1-130 ms** | Dependent on Client/OS (Keep-Alive vs Close). |

**Note on Network Latency:**
The service enables `TCP_NODELAY` to minimize latency. However, observed latency on Windows localhost with some clients (e.g., Python `aiohttp`) can be high due to OS scheduling and driver overhead. The core engine processes requests in microseconds. For maximum throughput, use persistent connections (Keep-Alive).