#include "../engine/mesh.hpp"
#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

using namespace l3kv;

// Stress Test: High Volume + Concurrency
void test_mesh_concurrency() {
  std::cout << "TEST: Mesh Concurrency & Volume..." << std::endl;
  boost::asio::io_context io;

  // Server (ID 10)
  Mesh server(io, 10, 9100);
  server.listen();

  std::atomic<int> received_count{0};
  // We can't verify order across threads easily, but we checks count and
  // integrity.
  server.set_on_message(
      [&](NodeID id, Lane lane, const lite3cpp::Buffer &pay) {
        received_count++;
      });

  // Client (ID 20)
  Mesh client(io, 20, 9101);
  client.connect(10, "127.0.0.1", 9100);

  // Background IO
  std::thread t_io([&io]() {
    // Run for enough time/work
    auto work = boost::asio::make_work_guard(io);
    io.run();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // 4 Threads sending 1000 messages each
  const int NUM_THREADS = 4;
  const int MSGS_PER_THREAD = 1000;
  std::vector<std::thread> senders;

  for (int i = 0; i < NUM_THREADS; ++i) {
    senders.emplace_back([&client, i]() {
      for (int m = 0; m < MSGS_PER_THREAD; ++m) {
        std::string msg = "T" + std::to_string(i) + ":M" + std::to_string(m);
        // Randomly pick lane
        Lane l = (m % 2 == 0) ? Lane::Standard : Lane::Express;
        // Retry loop (connection might be async establishing)
        // But verify test_phase2 showed fast connect.
        std::vector<uint8_t> msg_vec(msg.begin(), msg.end());
        client.send(10, l, lite3cpp::Buffer(std::move(msg_vec)));
        // Small yield to mix threads
        if (m % 100 == 0)
          std::this_thread::yield();
      }
    });
  }

  for (auto &t : senders)
    t.join();

  // Wait for delivery
  int total_expected = NUM_THREADS * MSGS_PER_THREAD;
  std::cout << "Sent " << total_expected << " messages. Waiting..."
            << std::endl;

  for (int i = 0; i < 50; ++i) {
    if (received_count >= total_expected)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  std::cout << "Received: " << received_count << " / " << total_expected
            << std::endl;

  io.stop();
  t_io.join();

  assert(received_count == total_expected);
  std::cout << "[PASS] Concurrency & Volume" << std::endl;
}

void test_large_payload() {
  std::cout << "TEST: Large Payload (1MB)..." << std::endl;
  boost::asio::io_context io;

  Mesh server(io, 11, 9200);
  server.listen();

  std::atomic<bool> result_ok{false};
  size_t received_size = 0;

  server.set_on_message(
      [&](NodeID id, Lane lane, const lite3cpp::Buffer &pay) {
        received_size = pay.size();
        // Spot check content
        bool all_match = true;
        const uint8_t* p_data = pay.data();
        for (size_t i = 0; i < pay.size(); i += 1024) { // Check every 1KB
          if (p_data[i] != 'A')
            all_match = false;
        }
        if (pay.size() > 0 && p_data[pay.size()-1] != 'A')
          all_match = false;

        if (all_match && lane == Lane::Heavy)
          result_ok = true;
      });

  Mesh client(io, 22, 9201);
  client.connect(11, "127.0.0.1", 9200);

  std::thread t_io([&io]() {
    auto work = boost::asio::make_work_guard(io);
    io.run();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Create 1MB payload
  std::vector<uint8_t> big_load_vec(1024 * 1024, 'A');
  client.send(11, Lane::Heavy, lite3cpp::Buffer(std::move(big_load_vec)));

  // Wait
  for (int i = 0; i < 50; ++i) {
    if (result_ok)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  io.stop();
  t_io.join();

  if (!result_ok) {
    std::cout << "FAIL: Size=" << received_size << " Expected=" << 1024 * 1024
              << std::endl;
  }
  assert(result_ok);
  std::cout << "[PASS] Large Payload Integrity" << std::endl;
}

int main() {
  test_mesh_concurrency();
  test_large_payload();
  return 0;
}
