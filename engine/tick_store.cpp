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

    last_partition_size = partition_end;

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


double Engine::simd_cracked_query(std::int64_t start_time,
                                  std::int64_t end_time,
                                  std::size_t partition_size) const
{
    if (partition_size == 0) {
        std::cout << "[simd_cracked_query] Empty partition — nothing to scan.\n";
        return 0.0;
    }

    __m256i v_start = _mm256_set1_epi64x(start_time);
    __m256i v_end   = _mm256_set1_epi64x(end_time);


    __m256 v_sum = _mm256_setzero_ps();

    std::size_t match_count = 0;


    std::size_t i = 0;
    std::size_t simd_end = (partition_size >= 8) ? (partition_size - 7) : 0;

    for (; i < simd_end; i += 8) {

        __m256i ts_lo = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(&timestamps[i])
        );
        __m256i ts_hi = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(&timestamps[i + 4])
        );

        __m256i too_early_lo = _mm256_cmpgt_epi64(v_start, ts_lo);
        __m256i too_late_lo  = _mm256_cmpgt_epi64(ts_lo, v_end);

        __m256i out_of_range_lo = _mm256_or_si256(too_early_lo, too_late_lo);

        __m256i all_ones = _mm256_set1_epi64x(-1);
        __m256i mask_lo = _mm256_andnot_si256(out_of_range_lo, all_ones);

        __m256i too_early_hi = _mm256_cmpgt_epi64(v_start, ts_hi);
        __m256i too_late_hi  = _mm256_cmpgt_epi64(ts_hi, v_end);
        __m256i out_of_range_hi = _mm256_or_si256(too_early_hi, too_late_hi);
        __m256i mask_hi = _mm256_andnot_si256(out_of_range_hi, all_ones);


        __m128i lo_lo = _mm256_castsi256_si128(mask_lo);
        __m128i lo_hi = _mm256_extracti128_si256(mask_lo, 1);
        __m128i hi_lo = _mm256_castsi256_si128(mask_hi);
        __m128i hi_hi = _mm256_extracti128_si256(mask_hi, 1);


        __m128i lo_compressed = _mm_unpacklo_epi64(
            _mm_shuffle_epi32(lo_lo, _MM_SHUFFLE(2, 0, 2, 0)),
            _mm_shuffle_epi32(lo_hi, _MM_SHUFFLE(2, 0, 2, 0))
        );
        __m128i hi_compressed = _mm_unpacklo_epi64(
            _mm_shuffle_epi32(hi_lo, _MM_SHUFFLE(2, 0, 2, 0)),
            _mm_shuffle_epi32(hi_hi, _MM_SHUFFLE(2, 0, 2, 0))
        );


        __m256i mask_8x32 = _mm256_set_m128i(hi_compressed, lo_compressed);


        __m256 v_prices = _mm256_loadu_ps(&prices[i]);


        __m256 float_mask = _mm256_castsi256_ps(mask_8x32);


        __m256 masked_prices = _mm256_blendv_ps(_mm256_setzero_ps(),
                                                v_prices,
                                                float_mask);


        v_sum = _mm256_add_ps(v_sum, masked_prices);


        int movemask = _mm256_movemask_ps(float_mask);
        match_count += __builtin_popcount(movemask);
    }

    

    __m256 hadd1 = _mm256_hadd_ps(v_sum, v_sum);

    __m256 hadd2 = _mm256_hadd_ps(hadd1, hadd1);

    __m128 upper = _mm256_extractf128_ps(hadd2, 1);

    __m128 lower = _mm256_castps256_ps128(hadd2);

    __m128 total_vec = _mm_add_ss(lower, upper);

    double sum = static_cast<double>(_mm_cvtss_f32(total_vec));



    for (; i < partition_size; ++i) {
        if (timestamps[i] >= start_time && timestamps[i] <= end_time) {
            sum += static_cast<double>(prices[i]);
            ++match_count;
        }
    }



    if (match_count == 0) {
        std::cout << "[simd_cracked_query] No ticks in time window ["
                  << start_time << ", " << end_time << "].\n";
        return 0.0;
    }

    double average = sum / static_cast<double>(match_count);

    std::cout << "[simd_cracked_query] Scanned " << partition_size
              << " ticks via AVX2 — " << match_count
              << " in time window.  Average price = " << average << "\n";

    return average;
}

void Engine::sort_partition_by_time(std::size_t left, std::size_t right)
{

    if (left >= right) return;

    std::size_t  mid   = left + (right - left) / 2;
    std::int64_t pivot = timestamps[mid];

    std::size_t    i = left;
    std::ptrdiff_t j = static_cast<std::ptrdiff_t>(right);

    while (static_cast<std::ptrdiff_t>(i) <= j) {
        while (timestamps[i] < pivot) ++i;
        while (timestamps[static_cast<std::size_t>(j)] > pivot) --j;

        if (static_cast<std::ptrdiff_t>(i) <= j) {
            swap_rows(i, static_cast<std::size_t>(j));
            ++i;
            --j;
        }
    }

    if (static_cast<std::ptrdiff_t>(left) < j)
        sort_partition_by_time(left, static_cast<std::size_t>(j));
    if (i < right)
        sort_partition_by_time(i, right);
}

std::pair<std::size_t, std::size_t> Engine::binary_search_time(
    std::size_t   part_start,
    std::size_t   part_size,
    std::int64_t  start_time,
    std::int64_t  end_time) const
{
    std::size_t part_end = part_start + part_size;

    std::size_t lo = part_start;
    std::size_t hi = part_end;

    while(lo < hi){
        std::size_t mid = lo + (hi - lo)/2;
        if(timestamps[mid] < start_time){
            lo = mid + 1;
        }
        else{
            hi = mid;
        }
    }
    std::size_t lower_bound_idx = lo;

    lo = lower_bound_idx;
    hi = part_end;

    while(lo < hi){
        std::size_t mid = lo + (hi - lo) / 2;
        if(timestamps[mid] <= end_time){
            lo = mid + 1;
        } 
        else{
            hi = mid;
        }
    }

    std::size_t upper_bound_idx = lo;

    return { lower_bound_idx, upper_bound_idx };
}

double Engine::smart_simd_query(std::int32_t target_symbol,
                                std::int64_t start_time,
                                std::int64_t end_time)
{
    if (num_ticks == 0) {
        std::cout << "[smart_simd_query] No ticks loaded.\n";
        return 0.0;
    }

    std::size_t partition_size = 0;
    bool        is_sorted      = false;

    auto it = cracking_index.find(target_symbol);

    if(it != cracking_index.end()){

        partition_size = it->second.first;
        is_sorted      = it->second.second;
        std::cout << "[Index] Cache hit for symbol " << target_symbol
                  << ". Skipping partition. (partition_size = "
                  << partition_size << ")\n";
    } 
    else{

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
        if (partition_end < num_ticks
            && symbol_ids[partition_end] == target_symbol) {
            ++partition_end;
        }

        partition_size = partition_end;
        last_partition_size = partition_end;

        cracking_index[target_symbol] = { partition_size, false };

        std::cout << "[Index] Cache miss. Partitioned array and updated index. "
                  << "(symbol = " << target_symbol
                  << ", partition_size = " << partition_size << ")\n";
    }

    if (partition_size == 0) {
        std::cout << "[smart_simd_query] Partition is empty for symbol "
                  << target_symbol << ".\n";
        return 0.0;
    }

    if (!is_sorted) {
        sort_partition_by_time(0, partition_size - 1);
        cracking_index[target_symbol].second = true;
        std::cout << "[Sort] Chronologically sorted the cracked partition.\n";
    }

    auto [range_start, range_end] = binary_search_time(
        0, partition_size, start_time, end_time);

    std::size_t scan_size = (range_end > range_start)
                          ? (range_end - range_start)
                          : 0;

    std::cout << "[Search] Binary search narrowed scope to "
              << scan_size << " rows.\n";

    if (scan_size == 0) {
        std::cout << "[smart_simd_query] No ticks in time window ["
                  << start_time << ", " << end_time << "].\n";
        return 0.0;
    }


    __m256i v_start = _mm256_set1_epi64x(start_time);
    __m256i v_end   = _mm256_set1_epi64x(end_time);
    __m256  v_sum   = _mm256_setzero_ps();
    std::size_t match_count = 0;

    std::size_t i        = range_start;
    std::size_t simd_end = (range_end >= range_start + 8)
                         ? (range_end - 7)
                         : range_start;

    for (; i < simd_end; i += 8) {
        __m256i ts_lo = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(&timestamps[i]));
        __m256i ts_hi = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(&timestamps[i + 4]));

        __m256i too_early_lo   = _mm256_cmpgt_epi64(v_start, ts_lo);
        __m256i too_late_lo    = _mm256_cmpgt_epi64(ts_lo, v_end);
        __m256i out_of_range_lo = _mm256_or_si256(too_early_lo, too_late_lo);
        __m256i all_ones        = _mm256_set1_epi64x(-1);
        __m256i mask_lo         = _mm256_andnot_si256(out_of_range_lo, all_ones);

        __m256i too_early_hi    = _mm256_cmpgt_epi64(v_start, ts_hi);
        __m256i too_late_hi     = _mm256_cmpgt_epi64(ts_hi, v_end);
        __m256i out_of_range_hi = _mm256_or_si256(too_early_hi, too_late_hi);
        __m256i mask_hi         = _mm256_andnot_si256(out_of_range_hi, all_ones);

        __m128i lo_lo = _mm256_castsi256_si128(mask_lo);
        __m128i lo_hi = _mm256_extracti128_si256(mask_lo, 1);
        __m128i hi_lo = _mm256_castsi256_si128(mask_hi);
        __m128i hi_hi = _mm256_extracti128_si256(mask_hi, 1);

        __m128i lo_compressed = _mm_unpacklo_epi64(
            _mm_shuffle_epi32(lo_lo, _MM_SHUFFLE(2, 0, 2, 0)),
            _mm_shuffle_epi32(lo_hi, _MM_SHUFFLE(2, 0, 2, 0)));
        __m128i hi_compressed = _mm_unpacklo_epi64(
            _mm_shuffle_epi32(hi_lo, _MM_SHUFFLE(2, 0, 2, 0)),
            _mm_shuffle_epi32(hi_hi, _MM_SHUFFLE(2, 0, 2, 0)));

        __m256i mask_8x32 = _mm256_set_m128i(hi_compressed, lo_compressed);
        __m256  float_mask = _mm256_castsi256_ps(mask_8x32);

        __m256 v_prices = _mm256_loadu_ps(&prices[i]);
        __m256 masked   = _mm256_blendv_ps(_mm256_setzero_ps(),
                                           v_prices, float_mask);
        v_sum = _mm256_add_ps(v_sum, masked);

        int movemask = _mm256_movemask_ps(float_mask);
        match_count += __builtin_popcount(movemask);
    }

    __m256 hadd1     = _mm256_hadd_ps(v_sum, v_sum);
    __m256 hadd2     = _mm256_hadd_ps(hadd1, hadd1);
    __m128 upper     = _mm256_extractf128_ps(hadd2, 1);
    __m128 lower     = _mm256_castps256_ps128(hadd2);
    __m128 total_vec = _mm_add_ss(lower, upper);
    double sum       = static_cast<double>(_mm_cvtss_f32(total_vec));

    
    for (; i < range_end; ++i) {
        if (timestamps[i] >= start_time && timestamps[i] <= end_time) {
            sum += static_cast<double>(prices[i]);
            ++match_count;
        }
    }

    if (match_count == 0) {
        std::cout << "[smart_simd_query] No ticks in time window ["
                  << start_time << ", " << end_time << "].\n";
        return 0.0;
    }

    double average = sum / static_cast<double>(match_count);

    std::cout << "[smart_simd_query] Scanned " << scan_size
              << " ticks via AVX2 (of " << partition_size
              << " in partition) — " << match_count
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