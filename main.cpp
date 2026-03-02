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

        auto crack_start = std::chrono::high_resolution_clock::now();

        double crack_avg = engine.crack_and_query(target_symbol,
                                                  start_time,
                                                  end_time);

        auto crack_end = std::chrono::high_resolution_clock::now();

        auto crack_ms = std::chrono::duration_cast<
            std::chrono::milliseconds>(crack_end - crack_start).count();

        auto crack_us = std::chrono::duration_cast<
            std::chrono::microseconds>(crack_end - crack_start).count();

        std::cout << "  Average price : " << crack_avg << "\n"
                  << "  Crack latency : " << crack_ms << " ms  ("
                  << crack_us << " µs)\n"
                  << "==========================================\n";

        constexpr std::int64_t start_time_2 = 1'700'000'010'000;
        constexpr std::int64_t end_time_2   = 1'700'000'015'000;

        std::cout << "--- New Query Parameters ---\n"
                  << "  Target symbol : " << target_symbol << " (Same as before)\n"
                  << "  Time window   : [" << start_time_2 << ", " << end_time_2 << "]\n\n";

        auto sub_start = std::chrono::high_resolution_clock::now();

        double sub_avg = engine.crack_and_query(target_symbol,
                                                start_time_2,
                                                end_time_2);

        auto sub_end = std::chrono::high_resolution_clock::now();

        auto sub_ms = std::chrono::duration_cast<std::chrono::milliseconds>(sub_end - sub_start).count();
        auto sub_us = std::chrono::duration_cast<std::chrono::microseconds>(sub_end - sub_start).count();

        std::cout << "  Average price : " << sub_avg << "\n"
                  << "  Subsequent latency : " << sub_ms << " ms  ("
                  << sub_us << " µs)\n"
                  << "==========================================\n";        
    } catch (const std::runtime_error& e) {

        std::cerr << "\n[FATAL ERROR] " << e.what() << "\n";
        return 1;
    }

    return 0;
}