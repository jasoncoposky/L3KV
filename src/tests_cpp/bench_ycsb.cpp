#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <future>
#include <cstdio>
#include <random>

#include <lite3-cpp/smart_client.hpp>

struct ThreadResult {
  int reads = 0;
  int updates = 0;
  int errors = 0;
};

void run_worker(int thread_id, int ops, int batch_size, bool is_read, ThreadResult &result, const std::string &host, int port) {
  lite3::Client client(host, port);
  // Direct Client connects automatically in constructor for ZeroMQ


  std::mt19937 generator(12345 + thread_id);
  std::uniform_int_distribution<int> key_dist(0, 999);

  for (int i = 0; i < ops; i += batch_size) {
    int current_batch = std::min(batch_size, ops - i);
    
    if (is_read) {
        lite3cpp::Buffer keys_buf;
        keys_buf.init_object();
        for (int j = 0; j < current_batch; ++j) {
            keys_buf.set_str(0, "user" + std::to_string(key_dist(generator)), "");
        }
        auto res = client.batch_get(keys_buf);
        if (res) result.reads += current_batch; else result.errors += current_batch;
    } else {

        lite3cpp::Buffer batch_buf;
        batch_buf.init_object();
        for (int j = 0; j < current_batch; ++j) {
            int key_id = key_dist(generator);
            std::string key = "user" + std::to_string(key_id);
            batch_buf.set_str(0, key, "perf_test_payload_1kb_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        }
        auto res = client.batch_put(batch_buf);
        if (res) result.updates += current_batch; else result.errors += current_batch;
    }
  }
}

int main(int argc, char **argv) {
  int threads = 1;
  int ops = 1000;
  int batch_size = 1;
  bool is_read = false;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--threads" && i + 1 < argc)
      threads = std::stoi(argv[++i]);
    else if (arg == "--ops" && i + 1 < argc)
      ops = std::stoi(argv[++i]);
    else if (arg == "--batch" && i + 1 < argc)
      batch_size = std::stoi(argv[++i]);
    else if (arg == "--read")
      is_read = true;
  }

  std::string host = "127.0.0.1";
  int port = 8081;


  printf("Starting YCSB Baseline (%d threads, %d ops, batch %d, workload %s)...\n",
         threads, ops, batch_size, is_read ? "READ" : "WRITE");

  std::vector<std::thread> workers;
  std::vector<ThreadResult> results(threads);

  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < threads; ++i) {
    workers.emplace_back(run_worker, i, ops / threads, batch_size, is_read,
                         std::ref(results[i]), host, port);
  }

  for (auto &t : workers)
    t.join();

  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> diff = end - start;

  int total_reads = 0;
  int total_updates = 0;
  int total_errors = 0;
  for (const auto &r : results) {
    total_reads += r.reads;
    total_updates += r.updates;
    total_errors += r.errors;
  }

  double throughput = (total_reads + total_updates) / diff.count();
  printf("Results:\n");
  printf("  Total Reads:   %d\n", total_reads);
  printf("  Total Updates: %d\n", total_updates);
  printf("  Total Errors:  %d\n", total_errors);
  printf("  Time:          %.2f s\n", diff.count());
  printf("  Throughput:    %.2f ops/sec\n", throughput);

  return 0;
}
