# Tick Storage Engine

A C++ tick analytics engine that turns brute-force historical market-data scans into a warmed, adaptive, SIMD-accelerated query path.

The project stores 50 million synthetic ticks in a columnar binary file, maps that file with `mmap`, and answers average-price queries over `(symbol, time window)` filters. The important idea is not one isolated optimization. The speed comes from a layered pipeline where each stage removes a different cost: file I/O, poor locality, full-dataset scans, repeated symbol filtering, wide time scans, and scalar aggregation.

## Headline Result

In the captured 50-million-tick run, Smart SIMD averaged `0.746 us/query` on the best-case cached workload, `1.258 us/query` on the real-life skewed workload, and `1.278 us/query` on the worst-case first-touch workload. The corresponding speedups over each workload's full-scan baseline were `60,981.662x`, `32,655.970x`, and `30,649.308x`.

The large cold-to-hot difference reflects separate query regimes. A first-touch query can pay partitioning and sorting costs, while a warmed query reuses the prepared symbol range and narrows the time window before aggregation.

## Benchmark Results

The benchmark suites each issued 1,000 queries against a 50,000,000-tick dataset (`1,000,000,000` bytes). Latency and throughput are measured by the benchmark, and speedup is relative to that workload's full-scan baseline. Latency and percentile values are in microseconds per query; throughput is queries per second. The tables are ordered from best-case to real-life to worst-case. Small timing differences can vary between runs.

### Best-case cached workload (repeated hot symbols)

| Query path | Avg latency | p95 | p99 | Throughput | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline full scan | 45,491.283 | 65,575.862 | 73,359.143 | 21.982 | 1.000× |
| Adaptive cracking (cold miss) | 1,315.433 | 564.723 | 892.847 | 760.206 | 34.583× |
| Adaptive cracking (hot hit) | 324.797 | 453.414 | 906.828 | 3,078.846 | 140.061× |
| Smart SIMD hot path | 0.746 | 0.911 | 1.056 | 1,340,513.122 | 60,981.662× |

### Real-life skewed workload (80/20 distribution)

| Query path | Avg latency | p95 | p99 | Throughput | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline full scan | 41,079.251 | 50,067.446 | 56,859.349 | 24.343 | 1.000× |
| Adaptive cracking (cold miss) | 4,785.868 | 38,929.536 | 56,660.988 | 208.949 | 8.583× |
| Adaptive cracking (hot hit) | 374.019 | 545.943 | 824.348 | 2,673.661 | 109.832× |
| Smart SIMD hot path | 1.258 | 2.469 | 2.958 | 794,950.475 | 32,655.970× |

### Worst-case first-touch workload (unique symbols)

| Query path | Avg latency | p95 | p99 | Throughput | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline full scan | 39,184.864 | 45,997.635 | 56,236.555 | 25.520 | 1.000× |
| Adaptive cracking (cold miss) | 5,036.124 | 40,754.870 | 67,339.792 | 198.565 | 7.781× |
| Adaptive cracking (hot hit) | 326.161 | 389.007 | 447.326 | 3,065.971 | 120.140× |
| Smart SIMD hot path | 1.278 | 2.166 | 2.583 | 782,172.108 | 30,649.308× |

Smart SIMD follows the expected workload trend in this run: cached hot queries are fastest, the skewed mix is slightly slower, and first-touch unique-symbol queries are slowest. The cold-miss path has a wide latency spread because a small number of queries can incur substantial partitioning work. The hot-hit and Smart SIMD rows include warmed symbol partitions.

### Longer Smart SIMD profile

A separate profile repeatedly queried the three hot symbols, pinned to CPU 2 with `taskset -c 2`:

| Queries | Total time (ms) | Avg latency (µs/query) | Throughput (QPS) |
| ---: | ---: | ---: | ---: |
| 10,000,000 | 10,593.959 | 1.059 | 943,934.204 |
| 50,000,000 | 50,786.431 | 1.016 | 984,514.933 |

Benchmark timings depend on the CPU, compiler options, page-cache state, and system load. Treat these measurements as a record of the captured run, not as a performance guarantee.

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
