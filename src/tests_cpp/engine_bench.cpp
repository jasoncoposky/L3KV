#include "engine/store.hpp"
#include <chrono>
#include <iostream>
#include <vector>
#include <filesystem>
#include <string>

using namespace l3kv;

int main() {
    std::string path = "bench_engine.wal";
    std::filesystem::remove(path);
    
    try {
        {
            Engine db(path, 1);
            
            const int iterations = 10000;
            std::string payload = "{\"age\": 25, \"name\": \"John Doe\", \"active\": true, \"score\": 98.5}";
            
            std::cout << "Benchmarking Engine (Async Path) with " << iterations << " ops..." << std::endl;
            auto start = std::chrono::high_resolution_clock::now();
            
            for (int i = 0; i < iterations; ++i) {
                db.put("u" + std::to_string(i), payload);
                if ((i + 1) % 10000 == 0) {
                    std::cout << "Submitted " << (i + 1) << " ops..." << std::endl;
                }
            }
            
            // Wait for all shard threads to finish processing the mutations
            std::cout << "Waiting for shards to sync..." << std::endl;
            db.wait_all_shards();
            
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> diff = end - start;
            
            double ops_per_sec = iterations / diff.count();
            std::cout << "Engine Throughput: " << (int)ops_per_sec << " ops/sec" << std::endl;
            std::cout << "Total Time: " << diff.count() << "s" << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Bench Error: " << e.what() << std::endl;
        return 1;
    }
    
    std::filesystem::remove(path);
    return 0;
}
