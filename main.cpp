#include <iostream>
#include <stdexcept>
#include "engine/tick_store.hpp"

int main() {

    try {
        std::cout << "=== Tick Storage Engine — Phase 1 ===\n\n";

        tick_store::Engine engine("ticks.bin");

        engine.print_first_tick();

        std::cout << "\nTotal ticks loaded: " << engine.get_num_ticks() << "\n";

    } catch (const std::runtime_error& e) {

        std::cerr << "\n[FATAL ERROR] " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}