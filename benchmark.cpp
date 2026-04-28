#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "engine/tick_store.hpp"

namespace {

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

constexpr std::size_t MASTER_QUERY_COUNT = 10'000;
constexpr std::size_t STAGE1_QUERY_COUNT = 100;
constexpr std::size_t STAGE2_QUERY_COUNT = 100;
constexpr std::size_t STAGE34_QUERY_COUNT = 1'000;
constexpr std::size_t STAGE5_QUERY_COUNT = 10'000;

std::vector<QueryTuple> make_master_queries() {
    std::mt19937 rng(42);
    std::uniform_int_distribution<std::int32_t> symbol_dist(1, 100);
    std::uniform_int_distribution<std::int64_t> time_dist(
        1'700'000'000'000LL,
        1'700'000'500'000LL);
    std::uniform_int_distribution<std::int64_t> window_dist(1000LL, 5000LL);

    std::vector<QueryTuple> queries;
    queries.reserve(MASTER_QUERY_COUNT);

    for (std::size_t i = 0; i < MASTER_QUERY_COUNT; ++i) {
        const std::int64_t t0 = time_dist(rng);
        const std::int64_t t1 = t0 + window_dist(rng);
        queries.push_back(QueryTuple{symbol_dist(rng), t0, t1});
    }

    return queries;
}

std::vector<QueryTuple> take_first_n(const std::vector<QueryTuple>& source,
                                     std::size_t n) {
    if (source.size() < n) {
        throw std::runtime_error("Query source smaller than requested subset.");
    }
    return std::vector<QueryTuple>(source.begin(), source.begin() + static_cast<std::ptrdiff_t>(n));
}

std::vector<QueryTuple> take_first_n_unique_symbols(
    const std::vector<QueryTuple>& source,
    std::size_t n,
    std::size_t expected_symbol_cardinality)
{
    std::vector<QueryTuple> subset;
    subset.reserve(n);
    std::unordered_set<std::int32_t> seen_symbols;
    seen_symbols.reserve(expected_symbol_cardinality);

    for (const auto& q : source) {
        if (seen_symbols.insert(q.symbol).second) {
            subset.push_back(q);
            if (subset.size() == n) {
                break;
            }
        }
    }

    if (subset.size() != n) {
        throw std::runtime_error(
            "Unable to build Stage 2 unique-symbol query set of requested size.");
    }

    return subset;
}

std::unordered_set<std::int32_t> unique_symbols_in(
    const std::vector<QueryTuple>& queries)
{
    std::unordered_set<std::int32_t> symbols;
    symbols.reserve(queries.size());
    for (const auto& q : queries) {
        symbols.insert(q.symbol);
    }
    return symbols;
}

StageResult run_stage(
    const std::string& stage_name,
    const std::vector<QueryTuple>& queries,
    double& global_dummy_accumulator,
    const std::function<double(tick_store::Engine&, const QueryTuple&)>& query_fn,
    const std::function<void(tick_store::Engine&, const std::vector<QueryTuple>&)>& warmup_fn)
{
    if (queries.empty()) {
        throw std::runtime_error("Stage query list is empty.");
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

} // namespace

int main() {
    try {
        std::cout << "============================================================\n";
        std::cout << "  Tick Storage Engine — Five-Stage Ablation Benchmark\n";
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
        std::cout << "============================================================\n\n";

        const auto master_queries = make_master_queries();
        const auto stage1_queries = take_first_n(master_queries, STAGE1_QUERY_COUNT);
        const auto stage2_queries = take_first_n_unique_symbols(master_queries,
                                                                 STAGE2_QUERY_COUNT,
                                                                 100);
        const auto stage34_queries = take_first_n(master_queries, STAGE34_QUERY_COUNT);
        const auto stage5_queries = take_first_n(master_queries, STAGE5_QUERY_COUNT);

        double dummy_accumulator = 0.0;
        std::vector<StageResult> results;
        results.reserve(5);

        results.push_back(run_stage(
            "Stage 1: Baseline O(N) Full Scan",
            stage1_queries,
            dummy_accumulator,
            [](tick_store::Engine& e, const QueryTuple& q) {
                return e.query_average_price(q.symbol, q.start_time, q.end_time);
            },
            [](tick_store::Engine&, const std::vector<QueryTuple>&) {}));

        results.push_back(run_stage(
            "Stage 2: Adaptive Cracking (Cold Miss)",
            stage2_queries,
            dummy_accumulator,
            [](tick_store::Engine& e, const QueryTuple& q) {
                return e.crack_and_query(q.symbol, q.start_time, q.end_time);
            },
            [](tick_store::Engine&, const std::vector<QueryTuple>&) {}));

        results.push_back(run_stage(
            "Stage 3: Adaptive Cracking (Hot Hit)",
            stage34_queries,
            dummy_accumulator,
            [](tick_store::Engine& e, const QueryTuple& q) {
                return e.crack_and_query(q.symbol, q.start_time, q.end_time);
            },
            [](tick_store::Engine& e, const std::vector<QueryTuple>& queries) {
                auto symbols = unique_symbols_in(queries);
                for (std::int32_t sym : symbols) {
                    (void)e.crack_and_query(sym,
                                            1'700'000'000'000LL,
                                            1'700'000'500'000LL);
                }
            }));

        results.push_back(run_stage(
            "Stage 4: Sorted+Binary+SIMD (Cold Sort)",
            stage34_queries,
            dummy_accumulator,
            [](tick_store::Engine& e, const QueryTuple& q) {
                return e.smart_simd_query(q.symbol, q.start_time, q.end_time);
            },
            [](tick_store::Engine& e, const std::vector<QueryTuple>& queries) {
                auto symbols = unique_symbols_in(queries);
                for (std::int32_t sym : symbols) {
                    // Prime partition layout without populating smart index metadata.
                    (void)e.crack_and_query(sym,
                                            1'700'000'000'000LL,
                                            1'700'000'500'000LL);
                }
            }));

        results.push_back(run_stage(
            "Stage 5: Ultimate Hot Path (Smart SIMD)",
            stage5_queries,
            dummy_accumulator,
            [](tick_store::Engine& e, const QueryTuple& q) {
                return e.smart_simd_query(q.symbol, q.start_time, q.end_time);
            },
            [](tick_store::Engine& e, const std::vector<QueryTuple>& queries) {
                auto symbols = unique_symbols_in(queries);
                for (std::int32_t sym : symbols) {
                    (void)e.smart_simd_query(sym,
                                             1'700'000'000'000LL,
                                             1'700'000'500'000LL);
                }
            }));

        const double baseline_us = results.front().avg_latency_us;
        for (auto& r : results) {
            r.speedup_vs_baseline = (r.avg_latency_us > 0.0)
                ? (baseline_us / r.avg_latency_us)
                : std::numeric_limits<double>::infinity();
        }

        print_table(results);

        std::cout << "\nRun profile:\n";
        std::cout << "  Stage 1 queries: " << STAGE1_QUERY_COUNT << "\n";
        std::cout << "  Stage 2 queries: " << STAGE2_QUERY_COUNT << " (unique symbols)\n";
        std::cout << "  Stage 3 queries: " << STAGE34_QUERY_COUNT << "\n";
        std::cout << "  Stage 4 queries: " << STAGE34_QUERY_COUNT << "\n";
        std::cout << "  Stage 5 queries: " << STAGE5_QUERY_COUNT << "\n";

        std::cout << "\n[Anti-DCE] Global accumulated value: "
                  << std::setprecision(9) << dummy_accumulator << "\n";

        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "[benchmark] Fatal error: " << ex.what() << "\n";
        return 1;
    }
}
