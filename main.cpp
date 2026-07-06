#include <iostream>
#include <stdexcept>
#include <chrono>
#include "engine/tick_store.hpp"

static void run_query(tick_store::Engine& engine,
                      const char*        label,
                      std::int32_t       symbol,
                      std::int64_t       start_time,
                      std::int64_t       end_time)
{
    std::cout << "=== " << label << " ===\n"
              << "  Target symbol : " << symbol   << "\n"
              << "  Time window   : [" << start_time
              << ", " << end_time << "]\n\n";

    auto t0  = std::chrono::high_resolution_clock::now();
    double avg = engine.smart_simd_query(symbol, start_time, end_time);
    auto t1  = std::chrono::high_resolution_clock::now();

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    std::cout << "  Average price : " << avg << "\n"
              << "  Latency       : " << ms << " ms  (" << us << " µs)\n"
              << std::string(60, '=') << "\n\n";
}

int main() {

    try {
        tick_store::Engine engine("ticks.bin");

        engine.print_first_tick();

        std::cout << "\nTotal ticks loaded: " << engine.get_num_ticks() << "\n\n";

        run_query(engine,
                  "Query 1 — Symbol 42 | Cold Start (Cache Miss + Sort)",
                  42,
                  1'700'000'000'000LL,
                  1'700'000'005'000LL);

        run_query(engine,
                  "Query 2 — Symbol 42 | Hot Cache (Skip Partition + Sort)",
                  42,
                  1'700'000'010'000LL,
                  1'700'000'015'000LL);

        run_query(engine,
                  "Query 3 — Symbol 42 | Hot Cache (Narrow Window — O(log N) Payoff)",
                  42,
                  1'700'000'002'000LL,
                  1'700'000'003'000LL);

        run_query(engine,
                  "Query 4 — Symbol 7  | Cold Start (NEW symbol, cracks tail only)",
                  7,
                  1'700'000'000'000LL,
                  1'700'000'005'000LL);

        run_query(engine,
                  "Query 5 — Symbol 7  | Hot Cache",
                  7,
                  1'700'000'010'000LL,
                  1'700'000'015'000LL);

        run_query(engine,
                  "Query 6 — Symbol 42 | Hot Cache (after symbol 7 — partition must persist!)",
                  42,
                  1'700'000'020'000LL,
                  1'700'000'025'000LL);

    } catch (const std::runtime_error& e) {

        std::cerr << "\n[FATAL ERROR] " << e.what() << "\n";
        return 1;
    }

    return 0;
}