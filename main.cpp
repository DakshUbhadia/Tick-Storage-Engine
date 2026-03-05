#include <iostream>
#include <stdexcept>
#include <chrono>
#include "engine/tick_store.hpp"

int main() {

    try {
        tick_store::Engine engine("ticks.bin");

        engine.print_first_tick();

        std::cout << "\nTotal ticks loaded: " << engine.get_num_ticks() << "\n\n";

        constexpr std::int32_t target_symbol = 42;

        {
            constexpr std::int64_t start_time = 1'700'000'000'000;
            constexpr std::int64_t end_time   = 1'700'000'005'000;

            std::cout << "=== Query 1 — Cold Start (Cache Miss + Sort) ===\n"
                      << "  Target symbol : " << target_symbol << "\n"
                      << "  Time window   : [" << start_time << ", "
                      << end_time << "]\n\n";

            auto t0 = std::chrono::high_resolution_clock::now();

            double avg = engine.smart_simd_query(target_symbol,
                                                 start_time,
                                                 end_time);

            auto t1 = std::chrono::high_resolution_clock::now();

            auto ms = std::chrono::duration_cast<
                std::chrono::milliseconds>(t1 - t0).count();
            auto us = std::chrono::duration_cast<
                std::chrono::microseconds>(t1 - t0).count();

            std::cout << "  Average price : " << avg << "\n"
                      << "  Latency       : " << ms << " ms  ("
                      << us << " µs)\n"
                      << "==========================================\n\n";
        }

        {
            constexpr std::int64_t start_time = 1'700'000'010'000;
            constexpr std::int64_t end_time   = 1'700'000'015'000;

            std::cout << "=== Query 2 — Hot Cache (Skip Partition + Skip Sort) ===\n"
                      << "  Target symbol : " << target_symbol
                      << " (same symbol)\n"
                      << "  Time window   : [" << start_time << ", "
                      << end_time << "]\n\n";

            auto t0 = std::chrono::high_resolution_clock::now();

            double avg = engine.smart_simd_query(target_symbol,
                                                 start_time,
                                                 end_time);

            auto t1 = std::chrono::high_resolution_clock::now();

            auto ms = std::chrono::duration_cast<
                std::chrono::milliseconds>(t1 - t0).count();
            auto us = std::chrono::duration_cast<
                std::chrono::microseconds>(t1 - t0).count();

            std::cout << "  Average price : " << avg << "\n"
                      << "  Latency       : " << ms << " ms  ("
                      << us << " µs)\n"
                      << "==========================================\n\n";
        }

        {
            constexpr std::int64_t start_time = 1'700'000'002'000;
            constexpr std::int64_t end_time   = 1'700'000'003'000;

            std::cout << "=== Query 3 — O(log N) Payoff (Narrow Window) ===\n"
                      << "  Target symbol : " << target_symbol
                      << " (same symbol, tiny window)\n"
                      << "  Time window   : [" << start_time << ", "
                      << end_time << "]\n\n";

            auto t0 = std::chrono::high_resolution_clock::now();

            double avg = engine.smart_simd_query(target_symbol,
                                                 start_time,
                                                 end_time);

            auto t1 = std::chrono::high_resolution_clock::now();

            auto ms = std::chrono::duration_cast<
                std::chrono::milliseconds>(t1 - t0).count();
            auto us = std::chrono::duration_cast<
                std::chrono::microseconds>(t1 - t0).count();

            std::cout << "  Average price : " << avg << "\n"
                      << "  Latency       : " << ms << " ms  ("
                      << us << " µs)\n"
                      << "==========================================\n";
        }

    } catch (const std::runtime_error& e) {

        std::cerr << "\n[FATAL ERROR] " << e.what() << "\n";
        return 1;
    }

    return 0;
}