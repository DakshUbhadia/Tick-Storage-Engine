#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <iomanip>
#include "engine/tick_store.hpp"

int main() {

    auto t_map_start = std::chrono::high_resolution_clock::now();

    tick_store::Engine engine("ticks.bin");

    auto t_map_end = std::chrono::high_resolution_clock::now();

    double map_ms = std::chrono::duration<double, std::milli>(
        t_map_end - t_map_start).count();

    std::cout << std::fixed << std::setprecision(3);
    std::cout << "============================================================\n";
    std::cout << "  Tick Storage Engine — Benchmark Suite\n";
    std::cout << "============================================================\n";
    std::cout << "  Ticks loaded    : " << engine.get_num_ticks() << "\n";
    std::cout << "  File map time   : " << map_ms << " ms\n";
    std::cout << "============================================================\n\n";

    std::mt19937 rng(42);

    std::uniform_int_distribution<std::int32_t> symbol_dist(1, 10);
    std::uniform_int_distribution<std::int64_t> time_dist(
        1'700'000'000'000LL, 1'700'000'500'000LL);
    std::uniform_int_distribution<std::int64_t> window_dist(1000LL, 5000LL);


    std::cout << "--- Phase 1: Cache Warm-Up (10 symbols) ---\n";

    auto t_warmup_start = std::chrono::high_resolution_clock::now();

    for (std::int32_t sym = 1; sym <= 10; ++sym) {

        engine.smart_simd_query(sym,
                                1'700'000'000'000LL,
                                1'700'000'005'000LL);
    }

    auto t_warmup_end = std::chrono::high_resolution_clock::now();

    double warmup_ms = std::chrono::duration<double, std::milli>(
        t_warmup_end - t_warmup_start).count();

    std::cout << "  Warm-up time    : " << warmup_ms << " ms\n";
    std::cout << "  All 10 symbol partitions cracked, sorted, and paged in.\n\n";


    constexpr int NUM_QUERIES = 10'000;

    double dummy_accumulator = 0.0;

    std::cout << "--- Phase 2: High-Frequency Benchmark (" << NUM_QUERIES
              << " queries) ---\n";

    auto t_bench_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_QUERIES; ++i) {
        std::int32_t sym   = symbol_dist(rng);
        std::int64_t t0    = time_dist(rng);
        std::int64_t t1    = t0 + window_dist(rng);

        dummy_accumulator += engine.smart_simd_query(sym, t0, t1);
    }

    auto t_bench_end = std::chrono::high_resolution_clock::now();


    double total_ms = std::chrono::duration<double, std::milli>(
        t_bench_end - t_bench_start).count();

    double avg_latency_us = (total_ms * 1000.0) / NUM_QUERIES;

    double qps = NUM_QUERIES / (total_ms / 1000.0);

    std::cout << "\n";
    std::cout << "============================================================\n";
    std::cout << "  Benchmark Results\n";
    std::cout << "============================================================\n";
    std::cout << "  Queries executed      : " << NUM_QUERIES << "\n";
    std::cout << "  Total time            : " << total_ms << " ms\n";
    std::cout << "  Avg latency per query : " << avg_latency_us << " µs\n";
    std::cout << "  Throughput            : " << std::setprecision(0)
              << qps << " QPS\n";
    std::cout << "============================================================\n";


    std::cout << "\n  [Anti-DCE] Accumulated value: " << std::setprecision(6)
              << dummy_accumulator << "\n\n";

    return 0;
}
