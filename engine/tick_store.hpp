#ifndef TICK_STORE_HPP
#define TICK_STORE_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <immintrin.h>

namespace tick_store {

class Engine {
public:

    explicit Engine(const std::string& filepath);

    ~Engine();

    void print_first_tick() const;

    std::size_t get_num_ticks() const { return num_ticks; }

    double query_average_price(std::int32_t target_symbol,
                               std::int64_t start_time,
                               std::int64_t end_time) const;

    double crack_and_query(std::int32_t target_symbol,
                           std::int64_t start_time,
                           std::int64_t end_time);


    double simd_cracked_query(std::int64_t start_time,
                              std::int64_t end_time,
                              std::size_t partition_size) const;

    
    double smart_simd_query(std::int32_t target_symbol,
                            std::int64_t start_time,
                            std::int64_t end_time);

    std::size_t get_last_partition_size() const { return last_partition_size; }

private:

    int file_descriptor;

    void* mapped_memory;

    std::size_t file_size;
    std::int64_t* timestamps;
    std::int32_t* symbol_ids;
    float*        prices;
    std::int32_t* sizes;

    void swap_rows(std::size_t i, std::size_t j);


    void sort_partition_by_time(std::size_t left, std::size_t right);

    std::pair<std::size_t, std::size_t> binary_search_time(
        std::size_t   part_start,
        std::size_t   part_size,
        std::int64_t  start_time,
        std::int64_t  end_time) const;

    std::size_t num_ticks;

    std::size_t last_partition_size = 0;

    std::unordered_map<std::int32_t, std::pair<std::size_t, bool>> cracking_index;

    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;
};

}

#endif
