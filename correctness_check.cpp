#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "engine/tick_store.hpp"

class ScopedCoutSilencer {
public:
    explicit ScopedCoutSilencer(bool enabled)
        : enabled_(enabled), old_buf_(nullptr)
    {
        if (enabled_) {
            null_file_.open("/dev/null");
            old_buf_ = std::cout.rdbuf(null_file_.rdbuf());
        }
    }

    ~ScopedCoutSilencer()
    {
        if (enabled_ && old_buf_ != nullptr)
            std::cout.rdbuf(old_buf_);
    }

    ScopedCoutSilencer(const ScopedCoutSilencer&) = delete;
    ScopedCoutSilencer& operator=(const ScopedCoutSilencer&) = delete;

private:
    bool            enabled_;
    std::streambuf* old_buf_;
    std::ofstream   null_file_;
};

constexpr std::int64_t kMinTimestamp = 1'700'000'000'000LL;
constexpr std::int64_t kMaxTimestamp = 1'700'000'500'000LL;
constexpr std::int64_t kMaxWindow    =             10'000LL;

struct Query {
    std::int32_t symbol;
    std::int64_t start_time;
    std::int64_t end_time;
    std::string  label;
};

struct CheckResult {
    std::size_t  index;
    std::int32_t symbol;
    std::int64_t start_time;
    std::int64_t end_time;
    double       oracle_answer;
    double       fast_answer;
    double       abs_diff;
    bool         passed;
    std::string  label;
};

static constexpr double kTolerance = 1e-2;

std::vector<Query> build_queries()
{
    std::vector<Query> qs;
    qs.reserve(512);

    for (std::int32_t sym : {1, 7, 17, 42, 66, 100}) {
        qs.push_back({sym, kMinTimestamp, kMaxTimestamp,
                      "full-range sym=" + std::to_string(sym)});
    }

    for (std::int32_t sym : {7, 42}) {
        qs.push_back({sym, kMinTimestamp, kMinTimestamp + 1'000LL,
                      "narrow-start sym=" + std::to_string(sym)});
    }

    for (std::int32_t sym : {7, 42}) {
        qs.push_back({sym, kMaxTimestamp - 1'000LL, kMaxTimestamp,
                      "narrow-end sym=" + std::to_string(sym)});
    }

    qs.push_back({42, kMinTimestamp - 100'000LL, kMinTimestamp - 1LL,
                  "empty-before-data sym=42"});

    qs.push_back({42, kMaxTimestamp + 1LL, kMaxTimestamp + 100'000LL,
                  "empty-after-data sym=42"});

    qs.push_back({17, kMinTimestamp + 250'000LL, kMinTimestamp + 250'000LL,
                  "zero-width-window sym=17"});

    for (int repeat = 0; repeat < 3; ++repeat) {
        qs.push_back({42,
                      kMinTimestamp + 50'000LL * repeat,
                      kMinTimestamp + 50'000LL * repeat + 5'000LL,
                      "hot-hit-repeat=" + std::to_string(repeat) + " sym=42"});
    }

    {
        std::mt19937 rng(0xDEADBEEF);
        std::uniform_int_distribution<std::int32_t> sym_dist(1, 100);
        std::uniform_int_distribution<std::int64_t> time_dist(
            kMinTimestamp, kMaxTimestamp - kMaxWindow);
        std::uniform_int_distribution<std::int64_t> win_dist(500LL, kMaxWindow);

        for (int i = 0; i < 400; ++i) {
            std::int32_t sym   = sym_dist(rng);
            std::int64_t start = time_dist(rng);
            std::int64_t end   = std::min(start + win_dist(rng), kMaxTimestamp);
            qs.push_back({sym, start, end,
                          "rand#" + std::to_string(i) +
                          " sym=" + std::to_string(sym)});
        }
    }

    {
        constexpr std::array<std::int32_t, 5> hot{{7, 17, 25, 42, 66}};
        std::mt19937 rng(0xCAFEBABE);
        std::uniform_int_distribution<std::int64_t> time_dist(
            kMinTimestamp, kMaxTimestamp - kMaxWindow);
        std::uniform_int_distribution<std::int64_t> win_dist(1'000LL, kMaxWindow);

        for (int i = 0; i < 80; ++i) {
            std::int32_t sym   = hot[i % 5];
            std::int64_t start = time_dist(rng);
            std::int64_t end   = std::min(start + win_dist(rng), kMaxTimestamp);
            qs.push_back({sym, start, end,
                          "hot-stress#" + std::to_string(i) +
                          " sym=" + std::to_string(sym)});
        }
    }

    return qs;
}

int main()
{
    const std::string filepath = "ticks.bin";

    std::cout
        << "=================================================================\n"
        << "  Tick Storage Engine -- Correctness Checker\n"
        << "  Oracle : query_average_price()  (O(N) full scan, no mutations)\n"
        << "  Fast   : smart_simd_query()  (crack + sort + bsearch + AVX2)\n"
        << "  Tolerance : " << kTolerance << "\n"
        << "=================================================================\n\n";

    tick_store::Engine oracle_engine = [&]() -> tick_store::Engine {
        ScopedCoutSilencer s(true);
        return tick_store::Engine(filepath);
    }();

    tick_store::Engine fast_engine = [&]() -> tick_store::Engine {
        ScopedCoutSilencer s(true);
        return tick_store::Engine(filepath);
    }();

    const auto queries = build_queries();
    std::cout << "Engines loaded. Running " << queries.size() << " queries...\n\n";

    std::vector<CheckResult> results;
    results.reserve(queries.size());

    std::size_t pass_count = 0;
    std::size_t fail_count = 0;

    for (std::size_t i = 0; i < queries.size(); ++i) {
        const Query& q = queries[i];

        double oracle_ans, fast_ans;

        { ScopedCoutSilencer s(true);
          oracle_ans = oracle_engine.query_average_price(
              q.symbol, q.start_time, q.end_time); }

        { ScopedCoutSilencer s(true);
          fast_ans = fast_engine.smart_simd_query(
              q.symbol, q.start_time, q.end_time); }

        const double abs_diff = std::abs(oracle_ans - fast_ans);

        const bool passed = (oracle_ans == 0.0 && fast_ans == 0.0)
                         || (abs_diff <= kTolerance);

        if (passed) ++pass_count; else ++fail_count;

        results.push_back({i, q.symbol, q.start_time, q.end_time,
                           oracle_ans, fast_ans, abs_diff, passed, q.label});
    }

    if (fail_count > 0) {
        std::cout
            << "+---------------------------------------------------------+\n"
            << "|  FAILURES DETECTED                                      |\n"
            << "+---------------------------------------------------------+\n\n";

        for (const auto& r : results) {
            if (r.passed) continue;
            std::cout << std::fixed << std::setprecision(6)
                << "  Query #" << r.index << "  [" << r.label << "]\n"
                << "    symbol     : " << r.symbol       << "\n"
                << "    start_time : " << r.start_time   << "\n"
                << "    end_time   : " << r.end_time     << "\n"
                << "    oracle     : " << r.oracle_answer << "\n"
                << "    fast       : " << r.fast_answer   << "\n"
                << "    |diff|     : " << r.abs_diff
                << "  (tol=" << kTolerance << ")\n\n";
        }
    }

    std::cout << std::fixed << std::setprecision(6);
    std::cout
        << "+-------+--------+--------------------+--------------------+"
           "----------------+----------------+------------+--------+\n"
        << "| Query | Symbol |     Start Time     |      End Time      |"
           "     Oracle     |      Fast      |   |diff|   | Status |\n"
        << "+-------+--------+--------------------+--------------------+"
           "----------------+----------------+------------+--------+\n";

    for (const auto& r : results) {
        std::cout
            << "| " << std::setw(5)  << r.index
            << " | " << std::setw(6)  << r.symbol
            << " | " << std::setw(18) << r.start_time
            << " | " << std::setw(18) << r.end_time
            << " | " << std::setw(14) << r.oracle_answer
            << " | " << std::setw(14) << r.fast_answer
            << " | " << std::setw(10) << r.abs_diff
            << " | " << (r.passed ? " PASS " : "*FAIL*") << " |\n";
    }

    std::cout
        << "+-------+--------+--------------------+--------------------+"
           "----------------+----------------+------------+--------+\n\n";

    const double pass_rate =
        100.0 * static_cast<double>(pass_count)
              / static_cast<double>(results.size());

    std::cout
        << "=================================================================\n"
        << "  SUMMARY\n"
        << "  Total queries : " << results.size()  << "\n"
        << "  PASS          : " << pass_count
        << " (" << std::fixed << std::setprecision(2) << pass_rate << "%)\n"
        << "  FAIL          : " << fail_count << "\n"
        << "=================================================================\n";

    if (fail_count == 0) {
        std::cout << "\n  [OK]  All queries match. Fast pipeline is CORRECT.\n\n";
        return 0;
    } else {
        std::cout << "\n  [!!]  " << fail_count
                  << " query/queries produced wrong answers.\n\n";
        return 1;
    }
}
