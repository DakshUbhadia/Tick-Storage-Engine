#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "engine/tick_store.hpp"

namespace {

constexpr std::int64_t kMinTimestamp = 1'700'000'000'000LL;
constexpr std::int64_t kMaxTimestamp = 1'700'000'500'000LL;
constexpr std::int64_t kMaxWindow = 10'000LL;

struct QueryTuple {
    std::int32_t symbol;
    std::int64_t start_time;
    std::int64_t end_time;
};

struct StageResult {
    std::string name;
    std::size_t query_count;
    double total_time_ms;
    double avg_latency_us;
    double throughput_qps;
    double speedup_vs_baseline;
};

class ScopedCoutSilencer {
public:
    explicit ScopedCoutSilencer(bool enabled)
        : enabled_(enabled), old_buf_(nullptr) {
        if (enabled_) {
            old_buf_ = std::cout.rdbuf(null_stream_.rdbuf());
        }
    }

    ~ScopedCoutSilencer() {
        if (enabled_ && old_buf_ != nullptr) {
            std::cout.rdbuf(old_buf_);
        }
    }

    ScopedCoutSilencer(const ScopedCoutSilencer&) = delete;
    ScopedCoutSilencer& operator=(const ScopedCoutSilencer&) = delete;

private:
    bool enabled_;
    std::streambuf* old_buf_;
    std::ostringstream null_stream_;
};

enum class Mode {
    All,
    Best,
    Worst,
    Real
};

Mode parse_mode(int argc, char** argv) {
    if (argc <= 1) {
        return Mode::All;
    }

    const std::string arg = argv[1];
    if (arg == "--mode=all" || arg == "all") return Mode::All;
    if (arg == "--mode=best" || arg == "best") return Mode::Best;
    if (arg == "--mode=worst" || arg == "worst") return Mode::Worst;
    if (arg == "--mode=real" || arg == "real") return Mode::Real;

    throw std::runtime_error(
        "Unknown mode. Use: best | worst | real | all (or --mode=best, --mode=worst, --mode=real, --mode=all)");
}

std::vector<std::int32_t> unique_symbols_in_order(const std::vector<QueryTuple>& queries) {
    std::unordered_set<std::int32_t> seen;
    seen.reserve(queries.size());

    std::vector<std::int32_t> unique;
    unique.reserve(queries.size());

    for (const auto& q : queries) {
        if (seen.insert(q.symbol).second) {
            unique.push_back(q.symbol);
        }
    }
    return unique;
}

std::vector<QueryTuple> make_best_case_workload() {
    std::vector<QueryTuple> queries;
    queries.reserve(1000);

    constexpr std::array<std::int32_t, 3> hot_symbols{{42, 7, 17}};

    for (std::size_t i = 0; i < 1000; ++i) {
        const std::int32_t symbol = hot_symbols[i % 3];
        const std::int64_t start = kMinTimestamp + (i % 100) * 1'000LL;
        const std::int64_t end = std::min<std::int64_t>(start + 500LL, kMaxTimestamp);

        queries.push_back(QueryTuple{symbol, start, end});
    }

    return queries;
}

std::vector<QueryTuple> make_worst_case_workload() {

    std::vector<std::int32_t> symbols(1000);
    std::iota(symbols.begin(), symbols.end(), 1);

    std::mt19937 rng(42);
    std::shuffle(symbols.begin(), symbols.end(), rng);

    std::vector<QueryTuple> queries;
    queries.reserve(symbols.size());

    for (std::size_t i = 0; i < symbols.size(); ++i) {
        const std::int64_t start = kMinTimestamp + static_cast<std::int64_t>(i) * 100LL;
        const std::int64_t end = std::min<std::int64_t>(start + 5'000LL, kMaxTimestamp);

        queries.push_back(QueryTuple{
            symbols[i],
            start,
            end
        });
    }

    return queries;
}

std::vector<QueryTuple> make_real_life_workload() {

    std::mt19937 rng(1337);
    std::uniform_int_distribution<std::int64_t> time_dist(
        kMinTimestamp,
        kMaxTimestamp - kMaxWindow);
    std::uniform_int_distribution<std::int64_t> window_dist(1'000LL, kMaxWindow);
    std::uniform_int_distribution<int> hot_pick(1, 100);
    std::uniform_int_distribution<std::size_t> hot_idx(0, 4);
    std::uniform_int_distribution<std::size_t> cold_idx(0, 94);

    constexpr std::array<std::int32_t, 5> hot_symbols{{42, 7, 17, 25, 66}};

    std::vector<std::int32_t> cold_symbols;
    cold_symbols.reserve(95);
    for (std::int32_t s = 1; s <= 100; ++s) {
        bool is_hot = false;
        for (auto h : hot_symbols) {
            if (s == h) {
                is_hot = true;
                break;
            }
        }
        if (!is_hot) {
            cold_symbols.push_back(s);
        }
    }

    std::vector<QueryTuple> queries;
    queries.reserve(1000);

    for (std::size_t i = 0; i < 1000; ++i) {
        std::int32_t symbol = 0;

        if (hot_pick(rng) <= 80) {
            symbol = hot_symbols[hot_idx(rng)];
        } else {
            symbol = cold_symbols[cold_idx(rng)];
        }

        const std::int64_t start = time_dist(rng);
        const std::int64_t end = std::min(start + window_dist(rng), kMaxTimestamp);

        queries.push_back(QueryTuple{symbol, start, end});
    }

    return queries;
}

template <typename WarmupFn, typename QueryFn>
StageResult run_stage(
    const std::string& stage_name,
    const std::vector<QueryTuple>& queries,
    WarmupFn&& warmup_fn,
    QueryFn&& query_fn,
    double& global_dummy_accumulator)
{
    if (queries.empty()) {
        throw std::runtime_error("Query list is empty.");
    }

    tick_store::Engine engine("ticks.bin");

    {
        ScopedCoutSilencer silence(true);
        warmup_fn(engine, queries);
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    {
        ScopedCoutSilencer silence(true);
        for (const auto& q : queries) {
            global_dummy_accumulator += query_fn(engine, q);
        }
    }
    auto t1 = std::chrono::high_resolution_clock::now();

    const double total_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    const double avg_latency_us = (total_ms * 1000.0) / static_cast<double>(queries.size());
    const double throughput_qps = (total_ms > 0.0)
        ? static_cast<double>(queries.size()) / (total_ms / 1000.0)
        : std::numeric_limits<double>::infinity();

    return StageResult{
        stage_name,
        queries.size(),
        total_ms,
        avg_latency_us,
        throughput_qps,
        0.0
    };
}

void print_table(const std::vector<StageResult>& results) {
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+\n";
    std::cout << "| #  | Stage Name                                    | Queries    | Avg Latency   | Total Time       | Throughput          | Speedup vs Baseline  |\n";
    std::cout << "|    |                                               |            | (us/query)    | (ms)             | (QPS)               | (x)                  |\n";
    std::cout << "+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+\n";

    for (std::size_t i = 0; i < results.size(); ++i) {
        const auto& r = results[i];

        std::cout << "| " << std::setw(2) << (i + 1)
                  << " | " << std::left << std::setw(45) << r.name << std::right
                  << " | " << std::setw(10) << r.query_count
                  << " | " << std::setw(13) << r.avg_latency_us
                  << " | " << std::setw(16) << r.total_time_ms
                  << " | " << std::setw(19) << r.throughput_qps
                  << " | " << std::setw(20) << r.speedup_vs_baseline
                  << " |\n";
    }

    std::cout << "+----+-----------------------------------------------+------------+---------------+------------------+---------------------+----------------------+\n";
}

void run_suite(const std::string& suite_name, const std::vector<QueryTuple>& workload) {
    std::cout << "\n============================================================\n";
    std::cout << "  " << suite_name << "\n";
    std::cout << "============================================================\n";

    double dummy_accumulator = 0.0;
    std::vector<StageResult> results;
    results.reserve(4);

    auto no_warmup = [](tick_store::Engine&, const std::vector<QueryTuple>&) {};

    auto warmup_crack_all_symbols = [](tick_store::Engine& e, const std::vector<QueryTuple>& queries) {
        const auto symbols = unique_symbols_in_order(queries);
        for (std::int32_t sym : symbols) {
            (void)e.crack_and_query(sym, kMinTimestamp, kMaxTimestamp);
        }
    };

    auto warmup_smart_all_symbols = [](tick_store::Engine& e, const std::vector<QueryTuple>& queries) {
        const auto symbols = unique_symbols_in_order(queries);
        for (std::int32_t sym : symbols) {
            (void)e.smart_simd_query(sym, kMinTimestamp, kMaxTimestamp);
        }
    };

    results.push_back(run_stage(
        "Baseline O(N) Full Scan",
        workload,
        no_warmup,
        [](tick_store::Engine& e, const QueryTuple& q) {
            return e.query_average_price(q.symbol, q.start_time, q.end_time);
        },
        dummy_accumulator));

    results.push_back(run_stage(
        "Adaptive Cracking (Cold Miss)",
        workload,
        no_warmup,
        [](tick_store::Engine& e, const QueryTuple& q) {
            return e.crack_and_query(q.symbol, q.start_time, q.end_time);
        },
        dummy_accumulator));

    results.push_back(run_stage(
        "Adaptive Cracking (Hot Hit)",
        workload,
        warmup_crack_all_symbols,
        [](tick_store::Engine& e, const QueryTuple& q) {
            return e.crack_and_query(q.symbol, q.start_time, q.end_time);
        },
        dummy_accumulator));

    results.push_back(run_stage(
        "Ultimate Hot Path (Smart SIMD)",
        workload,
        warmup_smart_all_symbols,
        [](tick_store::Engine& e, const QueryTuple& q) {
            return e.smart_simd_query(q.symbol, q.start_time, q.end_time);
        },
        dummy_accumulator));

    const double baseline_us = results.front().avg_latency_us;
    for (auto& r : results) {
        r.speedup_vs_baseline = (r.avg_latency_us > 0.0)
            ? (baseline_us / r.avg_latency_us)
            : std::numeric_limits<double>::infinity();
    }

    std::cout << "Workload size: " << workload.size() << "\n";
    print_table(results);

    std::cout << "\n[Anti-DCE] Global accumulated value: "
              << std::setprecision(9) << dummy_accumulator << "\n";
}

}

int main(int argc, char** argv) {
    try {
        const Mode mode = parse_mode(argc, argv);

        std::cout << "============================================================\n";
        std::cout << "  Tick Storage Engine — Benchmark (1000 queries per suite)\n";
        std::cout << "============================================================\n";

        auto map_start = std::chrono::high_resolution_clock::now();
        {
            tick_store::Engine probe("ticks.bin");
            std::cout << "  Ticks loaded    : " << probe.get_num_ticks() << "\n";
        }
        auto map_end = std::chrono::high_resolution_clock::now();
        const double map_ms = std::chrono::duration<double, std::milli>(map_end - map_start).count();

        std::cout << "  Probe map time  : " << std::fixed << std::setprecision(3)
                  << map_ms << " ms\n";
        std::cout << "============================================================\n";

        const auto best_case_workload = make_best_case_workload();
        const auto worst_case_workload = make_worst_case_workload();
        const auto real_life_workload = make_real_life_workload();

        switch (mode) {
            case Mode::Best:
                run_suite("Best-Case Cached Workload (Repeated Hot Symbols)", best_case_workload);
                break;

            case Mode::Worst:
                run_suite("Worst-Case First-Touch Workload (Unique Symbols)", worst_case_workload);
                break;

            case Mode::Real:
                run_suite("Real-Life Skewed Workload (80/20 Distribution)", real_life_workload);
                break;

            case Mode::All:
                run_suite("Best-Case Cached Workload (Repeated Hot Symbols)", best_case_workload);
                run_suite("Worst-Case First-Touch Workload (Unique Symbols)", worst_case_workload);
                run_suite("Real-Life Skewed Workload (80/20 Distribution)", real_life_workload);
                break;
        }

        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "[benchmark] Fatal error: " << ex.what() << "\n";
        return 1;
    }
}