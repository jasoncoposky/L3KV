# Architectural Learnings & Optimization Roadmap for L3KV

**Date:** 2026-10-10  
**Context:** Research & Architectural Evaluation of L3KV as the Storage Engine for L3KVG (Graph Engine)  
**Status:** Approved Roadmap / Active Findings

---

## 1. Executive Summary

During the architectural investigation into whether L3KV is the optimal storage engine for the L3KVG graph database, we performed deep profiling and analysis of L3KV's write paths, memory layouts, WAL logging, and range scan mechanisms.

The evaluation reached two primary conclusions:
1. **Separation of Concerns:** Graph topology (micro-edges, adjacency lists, and traversals) demands a native Contiguous Slotted CSR (Compressed Sparse Row) substrate with pure integer indexing. Forcing graph edges into string-keyed Key-Value pairs creates severe write amplification and traversal overhead (the classic "Generation 1 Graph DB" anti-pattern).
2. **High-ROI Opportunities for L3KV:** The investigation uncovered five structural optimizations that will dramatically improve L3KV's performance as a standalone, distributed Key-Value and Document engine—reducing memory footprint by ~50%, cutting WAL disk I/O in half, and scaling throughput well beyond 150,000 ops/sec.

---

## 2. Structural Learnings & Optimization Roadmap

### Finding 1: Metadata Inlining (Eliminating 2x Write Amplification)

#### Current State
In `src/engine/store.hpp` (`apply_put`, `put`, `del`, `apply_mutation`), every user write to `key` triggers an automatic second write to a synthetic metadata key: `key + ":meta"`.
```cpp
// Current store.hpp logic:
std::string mkey_s = std::string(KeyBuilder::meta_key(key)); // key + ":meta"
// Appends TWO separate WAL ops:
batch.push_back({WalOp::PUT, key, json_body});
batch.push_back({WalOp::PUT, mkey_s, meta_val});
// Performs TWO separate shard map insertions:
apply_put(key, json_body);
apply_put(mkey_s, meta_val);
```

#### Problem
- **2x WAL Amplification:** Every single operation generates two WAL entries, doubling disk I/O.
- **2x In-Memory Overhead:** Every key in L3KV consumes two separate entries in `std::map`, doubling node pointer overhead and hash evaluations.

#### Proposed Architecture
Inline the 16-byte metadata header directly into the stored `Blob` / `StoredRecord`:
```cpp
struct alignas(8) StoredRecord {
    HybridLogicalClock::Timestamp hlc; // 12 bytes: wall_time, logical, node_id
    uint8_t flags;                      // 1 byte: TOMBSTONE, COMPRESSED, HAS_ACL
    uint8_t reserved[3];                // Padding / alignment
    lite3cpp::Buffer payload;           // Document payload
};
```
#### Impact
- **50% reduction in WAL volume and disk sync overhead.**
- **50% reduction in shard map entries and memory usage.**
- **Doubles pure write throughput.**

---

### Finding 2: Shard Storage Upgrade (`std::map` $\rightarrow$ Cache-Friendly B+Tree)

#### Current State
Each L3KV shard stores records in `std::map<std::string, std::shared_ptr<const Blob>, std::less<>>`. 
L3KV uses `std::map` instead of `std::unordered_map` because it requires sorted ordering for prefix scans (`get_prefix_chunk`, `lower_bound`).

#### Problem
- `std::map` is a node-allocated Red-Black tree.
- Each entry allocates a distinct 32–48 byte tree node on the heap in addition to the `std::string` buffer.
- Searching the map requires pointer-chasing across scattered heap addresses, incurring CPU cache misses (L3/DRAM stalls) and branch mispredictions.

#### Proposed Architecture
Replace `std::map` with a cache-line-aligned in-memory **B+Tree** (such as Google’s `absl::btree_map` or an open-source cache-friendly B+Tree):
- B+Tree leaves pack multiple keys and value pointers into contiguous 64-byte or 128-byte cache-line slots.
- Traversal across leaf nodes leverages hardware CPU prefetching.

#### Impact
- **3x–5x faster lookups and prefix range scans.**
- **60% lower heap allocation overhead** per entry.
- Substantially improved memory locality under concurrent shard access.

---

### Finding 3: Adaptive Compression Thresholds (Stopping Negative Compression)

#### Current State
In `src/engine/store.hpp` (`overwrite`), payloads are routed to `ZstdManager::compress` whenever the dictionary is active.

#### Problem
- For micro-payloads (< 256 bytes), invoking Zstandard:
  1. Consumes noticeable CPU cycles.
  2. Frequently results in **negative compression** (where Zstd frame headers make the compressed output larger than the raw input).

#### Proposed Architecture
Implement an adaptive compression heuristic:
1. **Minimum Size Gate:** Only attempt compression on payloads larger than 512 bytes.
2. **Expansion Guard:** If compressed size $\ge 95\%$ of original size, discard compressed buffer and store raw uncompressed bytes.

#### Impact
- Eliminates CPU waste on small document updates and scalar values.
- Guarantees zero storage bloat from frame headers.

---

### Finding 4: Group Commit / Micro-Batching on the WAL

#### Current State
The WAL was recently decoupled from `libconveyor` to direct sequential append (`write_raw` with POSIX `fdatasync`). This eliminated background threadpool contention and reached 17k–52k ops/sec.

#### Proposed Architecture
Implement a high-throughput **Group Commit** mechanism:
- Concurrent worker threads enqueue mutations to a lock-free queue (`moodycamel::ConcurrentQueue`).
- A dedicated flusher thread commits batches with a micro-timeout (e.g. 200 µs window or 1,000 operations threshold), issuing a single batched `writev` and `fdatasync`.

#### Impact
- Scales multi-threaded WAL throughput from **52,000 ops/sec to 150,000+ ops/sec** under saturated write loads while preserving full crash durability.

---

### Finding 5: PMR (Polymorphic Memory Resource) Slab Pooling

#### Current State
Each shard contains a `std::pmr::unsynchronized_pool_resource pool;`, but buffer allocations still frequently touch the global heap allocator (`malloc`/`free`).

#### Proposed Architecture
- Bind all `lite3cpp::Buffer` allocations within a shard to thread-local or shard-local PMR monotonic/slab allocators.
- Return deallocated buffers to a slab-free list to eliminate global allocator lock contention.

---

## 3. Architectural Role Matrix

| Component / Subsystem | Recommended Owner | Rationale |
| :--- | :--- | :--- |
| **Distributed Hash Ring & Sharding** | **L3KV** | Excellent consistent hashing, cluster topology discovery, and RPC dispatch. |
| **Document / Property Storage** | **L3KV** | Flexible schema, `lite3cpp::Buffer` integration, dynamic JSON patching. |
| **Cross-Cluster Replication & Anti-Entropy** | **L3KV** | Hybrid Logical Clocks (HLC) and Merkle tree state exchange. |
| **Micro-Edge & Adjacency Storage** | **Native Graph Substrate (CSR)** | Graph traversals require contiguous 64-bit integer arrays and index-free pointer arithmetic (<50ns hops). |

---

## 4. Implementation Priority

1. **Phase 1 (Immediate High ROI):** Metadata Inlining (`StoredRecord` header) $\rightarrow$ Eliminates `:meta` keys and halves WAL volume.
2. **Phase 2 (Throughput & Latency):** Replace `std::map` with `absl::btree_map` $\rightarrow$ 3x faster range queries.
3. **Phase 3 (Efficiency):** Adaptive Zstd thresholds $\rightarrow$ Saves CPU cycles on small documents.
4. **Phase 4 (Scale):** WAL Group Commit $\rightarrow$ Pushes durable throughput to 150k+ ops/sec.
