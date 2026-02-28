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
        constexpr std::int64_t start_time    = 1'700'000'000'000;
        constexpr std::int64_t end_time      = 1'700'000'005'000;

        std::cout << "--- Query Parameters ---\n"
                  << "  Target symbol : " << target_symbol << "\n"
                  << "  Time window   : [" << start_time << ", "
                  << end_time << "]\n\n";

        auto query_start = std::chrono::high_resolution_clock::now();

        double avg_price = engine.query_average_price(target_symbol,
                                                      start_time,
                                                      end_time);

        auto query_end = std::chrono::high_resolution_clock::now();

        auto elapsed_ms = std::chrono::duration_cast<
            std::chrono::milliseconds>(query_end - query_start).count();

        auto elapsed_us = std::chrono::duration_cast<
            std::chrono::microseconds>(query_end - query_start).count();

        std::cout << "  Average price : " << avg_price << "\n"
                  << "  Scan latency  : " << elapsed_ms << " ms  ("
                  << elapsed_us << " µs)\n"
                  << "==========================================\n";

    } catch (const std::runtime_error& e) {

        std::cerr << "\n[FATAL ERROR] " << e.what() << "\n";
        return 1;
    }

    return 0;
}