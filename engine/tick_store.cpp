#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <string>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include "tick_store.hpp"

namespace tick_store {

static constexpr std::size_t BYTES_PER_TICK = sizeof(std::int64_t)
                                            + sizeof(std::int32_t)
                                            + sizeof(float)
                                            + sizeof(std::int32_t);

Engine::Engine(const std::string& filepath)

    : file_descriptor{-1}
    , mapped_memory{nullptr}
    , file_size{0}
    , timestamps{nullptr}
    , symbol_ids{nullptr}
    , prices{nullptr}
    , sizes{nullptr}
    , num_ticks{0}
{

    file_descriptor = ::open(filepath.c_str(), O_RDONLY);

    if (file_descriptor == -1) {
        throw std::runtime_error(
            "tick_store::Engine — open() failed for '" + filepath + "'. "
            "Check that the file exists and you have read permission."
        );
    }

    struct stat file_info{};
    if (::fstat(file_descriptor, &file_info) == -1) {

        ::close(file_descriptor);
        throw std::runtime_error(
            "tick_store::Engine — fstat() failed. Cannot determine file size."
        );
    }

    file_size = static_cast<std::size_t>(file_info.st_size);

    if (file_size == 0 || (file_size % BYTES_PER_TICK) != 0) {
        ::close(file_descriptor);
        throw std::runtime_error(
            "tick_store::Engine — File size (" + std::to_string(file_size) +
            " bytes) is not a clean multiple of " +
            std::to_string(BYTES_PER_TICK) + " bytes per tick."
        );
    }

    num_ticks = file_size / BYTES_PER_TICK;


    mapped_memory = ::mmap(
        nullptr,
        file_size,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE,
        file_descriptor,
        0
    );

    if (mapped_memory == MAP_FAILED) {
        ::close(file_descriptor);
        throw std::runtime_error(
            "tick_store::Engine — mmap() failed. The kernel could not create "
            "the virtual mapping. Possible causes: insufficient virtual address "
            "space, or the file descriptor is invalid."
        );
    }


    char* base = static_cast<char*>(mapped_memory);

    timestamps = reinterpret_cast<std::int64_t*>(base);

    symbol_ids = reinterpret_cast<std::int32_t*>(
        base + (num_ticks * sizeof(std::int64_t))
    );

    prices = reinterpret_cast<float*>(
        base + (num_ticks * sizeof(std::int64_t))
              + (num_ticks * sizeof(std::int32_t))
    );

    sizes = reinterpret_cast<std::int32_t*>(
        base + (num_ticks * sizeof(std::int64_t))
              + (num_ticks * sizeof(std::int32_t))
              + (num_ticks * sizeof(float))
    );

    std::cout << "[tick_store::Engine] Successfully mapped '" << filepath
              << "' (" << file_size << " bytes, " << num_ticks << " ticks).\n";
}

Engine::~Engine() {
    
    if (mapped_memory != nullptr && mapped_memory != MAP_FAILED) {
        ::munmap(mapped_memory, file_size);
    }

    if (file_descriptor != -1) {
        ::close(file_descriptor);
    }

    std::cout << "[tick_store::Engine] Resources released (munmap + close).\n";
}

double Engine::query_average_price(std::int32_t target_symbol,
                                   std::int64_t start_time,
                                   std::int64_t end_time) const
{

    double sum = 0.0;
    std::size_t match_count = 0;

    for (std::size_t i = 0; i < num_ticks; ++i) {

        if (symbol_ids[i] == target_symbol
            && timestamps[i] >= start_time
            && timestamps[i] <= end_time)
        {
            sum += static_cast<double>(prices[i]);
            ++match_count;
        }
    }
    
    if (match_count == 0) {
        std::cout << "[query_average_price] No ticks matched the filter "
                     "(symbol=" << target_symbol
                  << ", time=[" << start_time << ", " << end_time << "]).\n";
        return 0.0;
    }

    double average = sum / static_cast<double>(match_count);

    std::cout << "[query_average_price] Scanned " << num_ticks
              << " ticks — " << match_count << " matched.  "
              << "Average price = " << average << "\n";

    return average;
}

void Engine::swap_rows(std::size_t i, std::size_t j) {
    std::swap(timestamps[i], timestamps[j]);
    std::swap(symbol_ids[i], symbol_ids[j]);
    std::swap(prices[i],     prices[j]);
    std::swap(sizes[i],      sizes[j]);
}

double Engine::crack_and_query(std::int32_t target_symbol,
                               std::int64_t start_time,
                               std::int64_t end_time)
{
    if (num_ticks == 0) {
        std::cout << "[crack_and_query] No ticks loaded.\n";
        return 0.0;
    }

    std::size_t    left  = 0;
    std::ptrdiff_t right = static_cast<std::ptrdiff_t>(num_ticks) - 1;

    while (static_cast<std::ptrdiff_t>(left) < right) {

        while (static_cast<std::ptrdiff_t>(left) < right
               && symbol_ids[left] == target_symbol) {
            ++left;
        }

        while (static_cast<std::ptrdiff_t>(left) < right
               && symbol_ids[right] != target_symbol) {
            --right;
        }

        if (static_cast<std::ptrdiff_t>(left) < right) {
            swap_rows(left, static_cast<std::size_t>(right));
            ++left;
            --right;
        }
    }

    std::size_t partition_end = left;
    if (partition_end < num_ticks && symbol_ids[partition_end] == target_symbol) {
        ++partition_end;
    }

    std::cout << "[crack_and_query] Partition complete.  "
              << partition_end << " / " << num_ticks
              << " ticks matched symbol " << target_symbol << ".\n";

    double sum = 0.0;
    std::size_t match_count = 0;

    for (std::size_t i = 0; i < partition_end; ++i) {
        if (timestamps[i] >= start_time && timestamps[i] <= end_time) {
            sum += static_cast<double>(prices[i]);
            ++match_count;
        }
    }

    if (match_count == 0) {
        std::cout << "[crack_and_query] No ticks in time window "
                  << "[" << start_time << ", " << end_time << "].\n";
        return 0.0;
    }

    double average = sum / static_cast<double>(match_count);

    std::cout << "[crack_and_query] Scanned " << partition_end
              << " ticks (cracked partition) — " << match_count
              << " in time window.  Average price = " << average << "\n";

    return average;
}

void Engine::print_first_tick() const {
    if (num_ticks == 0) {
        std::cout << "[tick_store::Engine] No ticks to display.\n";
        return;
    }

    std::cout << "--- First Tick (index 0) ---\n"
              << "  Timestamp : " << timestamps[0] << "\n"
              << "  Symbol ID : " << symbol_ids[0]  << "\n"
              << "  Price     : " << prices[0]      << "\n"
              << "  Size      : " << sizes[0]       << "\n"
              << "----------------------------\n";
}

}