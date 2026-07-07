# Tick Storage Engine

A C++ tick analytics engine that turns brute-force historical market-data scans into a warmed, adaptive, SIMD-accelerated query path.

The project stores 50 million synthetic ticks in a columnar binary file, maps that file with `mmap`, and answers average-price queries over `(symbol, time window)` filters. The important idea is not one isolated optimization. The speed comes from a layered pipeline where each stage removes a different cost: file I/O, poor locality, full-dataset scans, repeated symbol filtering, wide time scans, and scalar aggregation.

## Headline Result

The benchmark demonstrates a massive cold-to-hot latency shift. In our **Best-Case** workload (repeated hot symbols), we see the following results:

- Baseline full scan: `41,629.946 us/query`
- Ultimate warmed smart path: `1.055 us/query`
- Speedup vs measured baseline: `39,475.081x`
- Peak Throughput: `~948,237 QPS`
- Dataset: `50,000,000` ticks, `1,000,000,000` bytes

Even in a **Real-Life Skewed** workload (80% hot queries, 20% cold queries), the warmed path achieves an incredible **27,145x speedup** (1.582 us/query vs 42,948 us/query).

Furthermore, in a **Worst-Case** scenario (randomly jumping across 100 different partitions to defeat cache locality), the engine's binary-search and SIMD pipeline still manages a staggering **14,512x speedup** (2.741 us/query) and a peak throughput of **~365,000 QPS**.

That number is best understood as a regime change, not as one identical query becoming thousands of times faster. The first query for a symbol pays an adaptive partitioning and sorting cost. Repeated queries reuse the prepared structure, binary search the time boundary, and SIMD scan only tiny timestamp windows.

## Benchmark

The benchmark runs three distinct workload scenarios—**Best-Case Cached**, **Worst-Case Randomized Scatter**, and **Real-Life Skewed**—each processing 1000 queries against the 50-million-tick dataset.

### 1. Best-Case Cached Workload (Repeated Hot Symbols)
This tests the engine at its peak steady-state performance. Queries repeatedly hit the exact same 3 hot symbols with tight time windows, resulting in maximum temporal locality and cache utilization.
```text
+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
| #  | Stage Name                                    | Queries    | Avg Latency   | Total Time       | Throughput          | Speedup vs Baseline  |
|    |                                               |            | (us/query)    | (ms)             | (QPS)               | (x)                  |
+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
|  1 | Baseline O(N) Full Scan                       |       1000 |     41629.946 |        41629.946 |              24.021 |                1.000 |
|  2 | Adaptive Cracking (Cold Miss)                 |       1000 |      1377.873 |         1377.873 |             725.756 |               30.213 |
|  3 | Adaptive Cracking (Hot Hit)                   |       1000 |       291.774 |          291.774 |            3427.307 |              142.679 |
|  4 | Ultimate Hot Path (Smart SIMD)                |       1000 |         1.055 |            1.055 |          948237.606 |            39475.081 |
+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
```

### 2. Worst-Case Workload (Randomized Scatter)
This scenario randomly jumps across all 100 available symbols with wide time windows. It forces the engine to pay the initial cracking cost for every symbol in the dataset, and then heavily defeats CPU caching by ensuring no two consecutive queries hit the same memory region.
```text
+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
| #  | Stage Name                                    | Queries    | Avg Latency   | Total Time       | Throughput          | Speedup vs Baseline  |
|    |                                               |            | (us/query)    | (ms)             | (QPS)               | (x)                  |
+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
|  1 | Baseline O(N) Full Scan                       |       1000 |     39773.257 |        39773.257 |              25.143 |                1.000 |
|  2 | Adaptive Cracking (Cold Miss)                 |       1000 |      7590.720 |         7590.720 |             131.740 |                5.240 |
|  3 | Adaptive Cracking (Hot Hit)                   |       1000 |       311.279 |          311.279 |            3212.548 |              127.773 |
|  4 | Ultimate Hot Path (Smart SIMD)                |       1000 |         2.741 |            2.741 |          364881.072 |            14512.509 |
+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
```

### 3. Real-Life Skewed Workload (80/20 Distribution)
This scenario simulates an actual trading workload where 80% of queries hit a few hot symbols, and 20% hit cold symbols.
```text
+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
| #  | Stage Name                                    | Queries    | Avg Latency   | Total Time       | Throughput          | Speedup vs Baseline  |
|    |                                               |            | (us/query)    | (ms)             | (QPS)               | (x)                  |
+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
|  1 | Baseline O(N) Full Scan                       |       1000 |     42948.176 |        42948.176 |              23.284 |                1.000 |
|  2 | Adaptive Cracking (Cold Miss)                 |       1000 |      4587.372 |         4587.372 |             217.990 |                9.362 |
|  3 | Adaptive Cracking (Hot Hit)                   |       1000 |       306.282 |          306.282 |            3264.967 |              140.224 |
|  4 | Ultimate Hot Path (Smart SIMD)                |       1000 |         1.582 |            1.582 |          632058.913 |            27145.778 |
+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
```

Benchmark interpretation:

- **Stage 1** is the direct `O(N)` scan over all 50 million ticks.
- **Stage 2** exposes the cost profile of adaptive cracking. First-touch structural work can be expensive but always beats the baseline since the uncracked tail shrinks with every miss.
- **Stage 3** demonstrates the engine reusing partitioned symbol ranges (the cache hit). 
- **Stage 4** represents the ultimate warmed state: narrowed windows via binary-search over sorted ranges, and AVX2 SIMD aggregation. It effortlessly reaches between **360,000 and 940,000 queries per second** depending on memory locality and cache conditions.

## What The Engine Optimizes

The query is intentionally simple:

```cpp
average_price(symbol_id, start_time, end_time)
```

The implementation is interesting because this simple query stresses the same bottlenecks that appear in real historical tick research:

- Large datasets make full scans expensive.
- Repeated deserialization and copying waste time.
- Row layouts load fields the query does not need.
- A full up-front index may be wasteful for exploratory access patterns.
- Scalar branch-heavy loops leave CPU vector units underused.

This engine attacks those costs in layers.

## Optimization Pipeline

### 1. Baseline full scan

Implemented by `query_average_price` in [engine/tick_store.cpp](engine/tick_store.cpp).

Every query scans all ticks and checks:

- `symbol_ids[i] == target_symbol`
- `timestamps[i] >= start_time`
- `timestamps[i] <= end_time`

Complexity: `O(N)`.

For 50 million ticks, this is the expensive starting point.

### 2. Memory-mapped file access

Implemented in the `Engine` constructor in [engine/tick_store.cpp](engine/tick_store.cpp).

The engine opens `ticks.bin`, validates its size, and maps the whole file with `mmap`. Query code then works through typed pointers rather than explicit read, copy, and parse loops.

This removes per-query file I/O from the hot path and lets the operating system page cache manage residency.

Important detail: the map uses `MAP_PRIVATE`, so cracking and sorting mutate private copy-on-write pages in the process. The source `ticks.bin` file is not rewritten.

### 3. Columnar physical layout

Generated by [generate_data.cpp](generate_data.cpp) and sliced in [engine/tick_store.cpp](engine/tick_store.cpp).

The binary file stores full columns back-to-back:

```text
[ timestamps: int64_t[] ][ symbol_ids: int32_t[] ][ prices: float[] ][ sizes: int32_t[] ]
```

Queries mostly touch `symbol_ids`, `timestamps`, and `prices`, so the engine avoids dragging unrelated row fields through cache lines.

### 4. Adaptive cracking by symbol

Implemented by `crack_and_query` and used inside `smart_simd_query`.

On first touch for a symbol, the engine partitions matching rows into the front of the mapped arrays. It records the resulting partition size in an in-memory `cracking_index`.

For uniformly distributed symbols in `1..100`, one symbol partition is roughly `N / 100`, so repeated same-symbol queries no longer need to inspect the full dataset.

### 5. Cracking index cache

The `cracking_index` maps:

```text
symbol_id -> PartitionInfo{start, length, is_sorted}
```

On a cache hit, the engine skips partitioning and reuses the symbol-local range. This is the boundary between cold structural work and hot query execution.

### 6. One-time timestamp sort

After a symbol partition exists, `smart_simd_query` sorts that partition by timestamp once and flips the sorted flag.

That one-time cost enables fast pruning for every later time-window query over the same symbol.

### 7. Binary-search time pruning

Implemented by `binary_search_time`.

For a sorted symbol partition, the engine finds:

- first row with `timestamp >= start_time`
- first row with `timestamp > end_time`

The scan becomes:

```text
O(log P) + O(W)
```

Where:

- `P` is the symbol partition size
- `W` is the number of rows inside the requested time window

In observed runs, a partition of roughly `500,565` rows was narrowed to windows as small as `58`, `49`, and `20` rows.

### 8. AVX2 SIMD aggregation

Implemented by `simd_cracked_query` and the SIMD loop inside `smart_simd_query`.

The hot scan uses AVX2 intrinsics to process 8 prices per vector step, build timestamp masks, blend out non-matching lanes, and reduce the vector sum.

Scalar code handles the tail, preserving correctness for window sizes that are not multiples of 8.

## Architecture At A Glance

```text
ticks.bin
   |
   v
mmap once
   |
   v
column pointers
   |
   +--> baseline full scan
   |
   +--> smart_simd_query
          |
          v
      cracking_index lookup
          |
          +--> miss: partition by symbol
          |
          +--> hit: reuse partition
          |
          v
      sort partition by timestamp once
          |
          v
      binary search [start, end]
          |
          v
      AVX2 aggregate narrowed window
```

## Repository Layout

```text
.
|-- benchmark.cpp          # Multi-scenario ablation benchmark
|-- generate_data.cpp      # Synthetic 50M-tick data generator
|-- main.cpp               # Small cold/hot query demo
`-- engine
    |-- tick_store.hpp     # Engine public API
    `-- tick_store.cpp     # mmap, cracking, sorting, binary search, AVX2
```

## Build

Requirements:

- Linux or another POSIX-like system with `mmap`
- C++17 compiler
- AVX2-capable CPU
- About 1 GB free disk space for `ticks.bin`
- Enough RAM/page cache for meaningful benchmark results

Build commands:

```bash
g++ -O3 -std=c++17 generate_data.cpp -o generate_data
g++ -O3 -std=c++17 -mavx2 main.cpp engine/tick_store.cpp -o tick_engine
g++ -O3 -std=c++17 -mavx2 benchmark.cpp engine/tick_store.cpp -o benchmark_engine
```

## Run

Generate the dataset:

```bash
./generate_data
```

Run the query demo:

```bash
./tick_engine
```

Run the ablation benchmark (runs all three modes):

```bash
./benchmark_engine all
```

The generated `ticks.bin` file is intentionally ignored by Git because it is about 1 GB.

## API Sketch

```cpp
#include "engine/tick_store.hpp"

tick_store::Engine engine("ticks.bin");

double avg = engine.smart_simd_query(
    42,                  // symbol id
    1700000000000LL,     // start timestamp
    1700000005000LL      // end timestamp
);
```

Available query paths:

- `query_average_price`: baseline `O(N)` full scan.
- `crack_and_query`: adaptive symbol partition plus scalar scan.
- `simd_cracked_query`: AVX2 scan over an already cracked partition.
- `smart_simd_query`: cracking cache, one-time sort, binary search, and AVX2.

## Performance Notes

The fastest numbers are hot-path numbers. They assume:

- Symbol partitions have already been cracked.
- Timestamp sorting has already been paid for.
- The `cracking_index` metadata is warm.
- Relevant file pages are resident in the OS page cache.
- The benchmark is running many queries in a tight loop.

Cold queries and hot queries are different performance regimes. This README reports both because that distinction is the main lesson of the project.

## Why This Project Matters

This is a compact demonstration of systems techniques used in analytical storage engines:

- zero-copy style access through memory mapping
- cache-aware columnar layout
- adaptive indexing instead of full eager indexing
- binary-search pruning over sorted partitions
- vectorized aggregation with AVX2
- benchmark design that separates cold preparation from steady-state execution

The result is a small codebase that makes the cost model visible: every major latency drop comes from removing a specific class of work.
```
