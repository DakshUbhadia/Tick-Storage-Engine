# Tick Storage Engine

A C++ tick analytics engine that turns brute-force historical market-data scans into a warmed, adaptive, SIMD-accelerated query path.

The project stores 50 million synthetic ticks in a columnar binary file, maps that file with `mmap`, and answers average-price queries over `(symbol, time window)` filters. The important idea is not one isolated optimization. The speed comes from a layered pipeline where each stage removes a different cost: file I/O, poor locality, full-dataset scans, repeated symbol filtering, wide time scans, and scalar aggregation.

## Headline Result

The benchmark demonstrates a cold-to-hot latency shift:

- Baseline full scan: `85,906.446 us/query`
- Ultimate warmed smart path: `1.206 us/query`
- Speedup vs measured baseline: `71,203.949x`
- Dataset: `50,000,000` ticks, `1,000,000,000` bytes

Earlier exploratory runs also showed the same architectural effect at a more extreme cold-vs-hot boundary:

```text
Cold structural path:     about 2.63 s
Hot steady-state query:   about 0.486 us
Cold-to-hot speedup:      about 5.4 million x
```

That number is best understood as a regime change, not as one identical query becoming millions of times faster. The first query for a symbol can pay adaptive partitioning and sorting cost. Repeated queries reuse the prepared structure and scan only tiny timestamp windows.

## Latest Ablation Benchmark

```text
============================================================
  Tick Storage Engine - Five-Stage Ablation Benchmark
============================================================
[tick_store::Engine] Successfully mapped 'ticks.bin' (1000000000 bytes, 50000000 ticks).
  Ticks loaded    : 50000000
  Probe map time  : 0.134 ms
============================================================

+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
| #  | Stage Name                                    | Queries    | Avg Latency   | Total Time       | Throughput          | Speedup vs Baseline  |
|    |                                               |            | (us/query)    | (ms)             | (QPS)               | (x)                  |
+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
|  1 | Stage 1: Baseline O(N) Full Scan              |        100 |     85906.446 |         8590.645 |              11.641 |                1.000 |
|  2 | Stage 2: Adaptive Cracking (Cold Miss)        |        100 |    131466.688 |        13146.669 |               7.606 |                0.653 |
|  3 | Stage 3: Adaptive Cracking (Hot Hit)          |       1000 |   2882177.408 |      2882177.408 |               0.347 |                0.030 |
|  4 | Stage 4: Sorted+Binary+SIMD (Cold Sort)       |       1000 |     10331.514 |        10331.514 |              96.791 |                8.315 |
|  5 | Stage 5: Ultimate Hot Path (Smart SIMD)       |      10000 |         1.206 |           12.065 |          828854.549 |            71203.949 |
+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+
```

Benchmark interpretation:

- Stage 1 is the direct `O(N)` scan over all 50 million ticks.
- Stages 2 and 3 expose the cost profile of adaptive cracking. First-touch structural work can be expensive.
- Stage 4 shows the impact of sorting by timestamp, binary-search pruning, and SIMD.
- Stage 5 is the real target workload: warmed partitions, sorted symbol ranges, hot pages, narrowed windows, and SIMD aggregation.

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
symbol_id -> (partition_size, sorted_flag)
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
|-- benchmark.cpp          # Five-stage ablation benchmark
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

Run the ablation benchmark:

```bash
./benchmark_engine
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
