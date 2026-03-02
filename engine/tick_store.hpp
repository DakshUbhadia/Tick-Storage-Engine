#ifndef TICK_STORE_HPP
#define TICK_STORE_HPP

#include <cstddef>
#include <cstdint>
#include <string>

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

private:

    int file_descriptor;

    void* mapped_memory;

    std::size_t file_size;
    std::int64_t* timestamps;
    std::int32_t* symbol_ids;
    float*        prices;
    std::int32_t* sizes;

    void swap_rows(std::size_t i, std::size_t j);

    std::size_t num_ticks;

    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;
};

}

#endif
