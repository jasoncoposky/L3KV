#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <document.hpp>
#include <lite3/smart_client.hpp>

// --- Configuration ---
// Workload A: 50% Read, 50% Update
const int READ_PERCENTAGE = 50;
const int UPDATE_PERCENTAGE = 50;
int RECORD_COUNT = 10000;
int OPERATION_COUNT = 1000;
const int FIELD_LENGTH = 100; // 10 fields of 100 bytes = 1KB record
const int FIELD_COUNT = 10;

// --- Util ---
std::string random_string(size_t length) {
  static const char charset[] = "0123456789"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                "abcdefghijklmnopqrstuvwxyz";
  std::string str;
  str.reserve(length);
  // Simple PRNG for speed
  static thread_local std::mt19937 generator(std::random_device{}());
  std::uniform_int_distribution<int> distribution(0, sizeof(charset) - 2);

  for (size_t i = 0; i < length; ++i) {
    str += charset[distribution(generator)];
  }
  return str;
}

std::string build_key(int id) { return "user" + std::to_string(id); }

lite3cpp::Buffer build_record(int id) {
  lite3cpp::Document doc;
  auto root = doc.root_obj();
  root["id"] = (int64_t)id;
  for (int i = 0; i < FIELD_COUNT; ++i) {
    root["field" + std::to_string(i)] = random_string(FIELD_LENGTH);
  }
  return std::move(doc.buffer());
}

// --- Benchmark ---

struct Host {
  std::string address;
  int port;
};

std::vector<Host> parse_hosts(std::string arg) {
  std::vector<Host> hosts;
  size_t pos = 0;
  while ((pos = arg.find_first_not_of(",")) != std::string::npos) {
    arg.erase(0, pos);
    size_t end = arg.find(",");
    std::string token = arg.substr(0, end);
    if (end != std::string::npos)
      arg.erase(0, end + 1);
    else
      arg.clear();

    size_t colon = token.find(":");
    if (colon != std::string::npos) {
      hosts.push_back(
          {token.substr(0, colon), std::stoi(token.substr(colon + 1))});
    }
  }
  return hosts;
}

void load_phase(const std::vector<Host> &hosts) {
  std::cout << "Loading " << RECORD_COUNT
            << " records using SmartClient (Seed: " << hosts[0].address << ":"
            << hosts[0].port << ")...\n";
  auto start = std::chrono::high_resolution_clock::now();
  int errors = 0;

  // Use single SmartClient for loading
  lite3::SmartClient client(hosts[0].address, hosts[0].port);
  auto connect_res = client.connect();
  if (!connect_res) {
    std::cerr << "Failed to connect to cluster: " << connect_res.error().message
              << "\n";
    return;
  }

  for (int i = 0; i < RECORD_COUNT; ++i) {
    std::string key = build_key(i);
    lite3cpp::Buffer rec = build_record(i);
    // Send raw bytes. Client PUT expects string.
    std::string val(reinterpret_cast<const char *>(rec.data()), rec.size());

    auto res = client.put(key, val);
    if (!res) {
      errors++;
      if (errors < 10)
        std::cerr << "Load error: " << res.error().message << "\n";
    }
  }
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> diff = end - start;
  std::cout << "Load Complete: " << diff.count() << "s ("
            << RECORD_COUNT / diff.count() << " ops/sec). Errors: " << errors
            << "\n";
}

struct ThreadResult {
  int reads = 0;
  int updates = 0;
  int errors = 0;
};

void run_worker(int thread_id, int ops_per_thread, ThreadResult &result,
                const std::vector<Host> &hosts) {
  try {
    const auto &host = hosts[thread_id % hosts.size()];
    lite3::SmartClient client(host.address, host.port);
    if (auto res = client.connect(); !res) {
      std::cerr << "Worker " << thread_id
                << " failed to connect: " << res.error().message << "\n";
      result.errors += ops_per_thread; // Mark all as failed
      return;
    }

    std::mt19937 generator(12345 + thread_id); // Unique seed per thread
    std::uniform_int_distribution<int> key_dist(0, RECORD_COUNT - 1);
    std::uniform_int_distribution<int> op_dist(0, 99);

    for (int i = 0; i < ops_per_thread; ++i) {
      int key_id = key_dist(generator);
      std::string key = build_key(key_id);
      int op = op_dist(generator);

      if (op < READ_PERCENTAGE) {
        // READ
        auto res = client.get(key);
        if (res) {
          try {
            // Modern API: Wrap buffer in Document
            lite3cpp::Document doc(lite3cpp::Buffer(std::move(res.value())));
            auto root = doc.root_obj();

            // Verify we can read a field using proxy
            std::string_view s = static_cast<std::string_view>(root["field0"]);
            if (s.empty()) {
              if (result.errors < 5)
                std::cerr << "Read Validation Error (Empty)\n";
              result.errors++;
            }
          } catch (...) {
            if (result.errors < 5)
              std::cerr << "Read Exception\n";
            result.errors++;
          }
          result.reads++;
        } else {
          if (result.errors < 5)
            std::cerr << "Read Error: " << res.error().message << "\n";
          result.errors++;
        }
      } else {
        // UPDATE (Zero-Parse String Patch)
        auto res_patch =
            client.patch_str(key, "field0", random_string(FIELD_LENGTH));
        if (!res_patch) {
          if (result.errors < 5)
            std::cerr << "Patch Error: " << res_patch.error().message << "\n";
          result.errors++;
        }

        result.updates++;
      }
    }
  } catch (const std::exception &e) {
    std::cerr << "Thread " << thread_id << " error: " << e.what() << "\n";
    result.errors++;
  }
}

void run_phase_concurrent(int threads, const std::vector<Host> &hosts) {
  std::cout << "Running Workload A (" << OPERATION_COUNT
            << " ops, 50/50 R/W) with " << threads << " threads against "
            << hosts.size() << " hosts...\n";

  int ops_per_thread = OPERATION_COUNT / threads;
  std::vector<std::thread> workers;
  std::vector<ThreadResult> results(threads);

  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < threads; ++i) {
    workers.emplace_back(run_worker, i, ops_per_thread, std::ref(results[i]),
                         std::cref(hosts));
  }

  for (auto &t : workers) {
    t.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> diff = end - start;

  // Aggregate results
  int total_reads = 0;
  int total_updates = 0;
  int total_errors = 0;
  for (const auto &r : results) {
    total_reads += r.reads;
    total_updates += r.updates;
    total_errors += r.errors;
  }
  int total_ops = total_reads + total_updates;

  std::cout << "Run Complete: " << diff.count() << "s ("
            << total_ops / diff.count() << " ops/sec)\n";
  std::cout << "  Reads: " << total_reads << "\n";
  std::cout << "  Updates: " << total_updates << "\n";
  std::cout << "  Errors: " << total_errors << "\n";
}

int main(int argc, char **argv) {
  int threads = 1;
  bool skip_load = false;
  std::vector<Host> hosts;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--threads" && i + 1 < argc) {
      threads = std::stoi(argv[++i]);
    } else if (arg == "--ops" && i + 1 < argc) {
      OPERATION_COUNT = std::stoi(argv[++i]);
    } else if (arg == "--records" && i + 1 < argc) {
      RECORD_COUNT = std::stoi(argv[++i]);
    } else if (arg == "--skip-load") {
      skip_load = true;
    } else if (arg == "--hosts" && i + 1 < argc) {
      hosts = parse_hosts(argv[++i]);
    }
  }

  if (hosts.empty()) {
    hosts.push_back({"127.0.0.1", 8080});
  }

  try {
    if (!skip_load) {
      load_phase(hosts);
      std::cout << "Waiting 1s for consistency...\n";
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    run_phase_concurrent(threads, hosts);

  } catch (const std::exception &e) {
    std::cerr << "Fatal Error: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
