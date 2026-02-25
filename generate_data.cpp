#include <iostream>
#include <fstream>
#include <vector>
#include <random>
#include <chrono>

const size_t NUM_TICKS = 50'000'000; 

int main() {
    std::cout << "Allocating memory for " << NUM_TICKS << " ticks..." << std::endl;

    std::vector<int64_t> timestamps(NUM_TICKS);
    std::vector<int32_t> symbol_ids(NUM_TICKS);
    std::vector<float> prices(NUM_TICKS);
    std::vector<int32_t> sizes(NUM_TICKS);

    std::cout << "Generating mock data..." << std::endl;
    std::mt19937 rng(42);
    std::uniform_int_distribution<int32_t> sym_dist(1, 100);
    std::uniform_real_distribution<float> price_dist(10.0f, 500.0f);
    std::uniform_int_distribution<int32_t> size_dist(1, 1000);

    int64_t current_ts = 1700000000000;
    for (size_t i = 0; i < NUM_TICKS; ++i) {
        timestamps[i] = current_ts++;
        symbol_ids[i] = sym_dist(rng);
        prices[i] = price_dist(rng);
        sizes[i] = size_dist(rng);
    }

    std::cout << "Writing columns to binary file (ticks.bin)..." << std::endl;
    
    std::ofstream out("ticks.bin", std::ios::binary);
    if (!out) {
        std::cerr << "Failed to open file for writing!" << std::endl;
        return 1;
    }
    
    out.write(reinterpret_cast<const char*>(timestamps.data()), NUM_TICKS * sizeof(int64_t));
    out.write(reinterpret_cast<const char*>(symbol_ids.data()), NUM_TICKS * sizeof(int32_t));
    out.write(reinterpret_cast<const char*>(prices.data()), NUM_TICKS * sizeof(float));
    out.write(reinterpret_cast<const char*>(sizes.data()), NUM_TICKS * sizeof(int32_t));

    out.close();
    std::cout << "Success! ticks.bin created." << std::endl;

    return 0;
}