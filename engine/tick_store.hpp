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

private:

    int file_descriptor;

    void* mapped_memory;

    std::size_t file_size;

    const std::int64_t* timestamps;
    const std::int32_t* symbol_ids;
    const float*        prices;
    const std::int32_t* sizes;

    std::size_t num_ticks;

    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;
};

}

#endif
