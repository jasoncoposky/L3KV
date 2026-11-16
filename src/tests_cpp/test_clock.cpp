#include "../engine/clock.hpp"
#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>


using namespace l3kv;

void test_monotonicity() {
  HybridLogicalClock clock(1);
  Timestamp t1 = clock.now();
  Timestamp t2 = clock.now();

  assert(t2 > t1 && "Clock must be monotonic");
  std::cout << "[PASS] Monotonicity" << std::endl;
}

void test_causality_receive() {
  HybridLogicalClock clock(1);
  Timestamp local = clock.now();

  // Simulate incoming message from future
  Timestamp remote = local;
  remote.wall_time += 1000;
  remote.logical = 0;

  clock.update(remote);
  Timestamp next = clock.now();

  assert(next.wall_time >= remote.wall_time &&
         "Clock must catch up to remote physical time");
  assert(next > remote && "Next event must be after remote event");
  std::cout << "[PASS] Causality Receive" << std::endl;
}

void test_logical_increment() {
  HybridLogicalClock clock(1);

  // Freeze physical time for testing
  // Note: In real impl, we might need dependency injection for time.
  // Here we assume fast loop increments logical

  Timestamp t1 = clock.now();
  Timestamp t2 = clock.now();

  if (t1.wall_time == t2.wall_time) {
    assert(t2.logical > t1.logical &&
           "Logical counter must increment if physical time is same");
  }
  std::cout << "[PASS] Logical Increment" << std::endl;
}

void test_thread_local_future_skew_no_spin() {
  HybridLogicalClock clock(1);
  Timestamp base = clock.now();
  Timestamp future_remote{base.wall_time + 3600LL * 1000000LL, 5, 2};
  clock.update(future_remote);

  constexpr int THREADS = 4;
  constexpr int OPS_PER_THREAD = 500;
  std::vector<std::vector<Timestamp>> per_thread(THREADS);
  auto t0 = std::chrono::steady_clock::now();

  std::vector<std::thread> workers;
  for (int t = 0; t < THREADS; ++t) {
    workers.emplace_back([&, t]() {
      ThreadLocalClock local(&clock);
      per_thread[t].reserve(OPS_PER_THREAD);
      for (int i = 0; i < OPS_PER_THREAD; ++i) {
        per_thread[t].push_back(local.now());
      }
    });
  }
  for (auto &w : workers) {
    w.join();
  }

  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - t0)
                        .count();
  assert(elapsed_ms < 1000 &&
         "ThreadLocalClock::now() spun/stalled under +1h future clock skew");

  for (int t = 0; t < THREADS; ++t) {
    Timestamp prev = future_remote;
    for (const auto &ts : per_thread[t]) {
      assert(ts > prev && "ThreadLocalClock must be strictly monotonic above future skew");
      prev = ts;
    }
  }
  std::cout << "[PASS] ThreadLocalClock +1h Future Skew No-Spin (" << elapsed_ms
            << "ms)" << std::endl;
}

int main() {
  try {
    test_monotonicity();
    test_causality_receive();
    test_logical_increment();
    test_thread_local_future_skew_no_spin();
    std::cout << "All Clock Tests Passed!" << std::endl;
  } catch (const std::exception &e) {
    std::cerr << "Test Failed: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
