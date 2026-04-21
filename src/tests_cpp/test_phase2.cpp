#include "../engine/mesh.hpp"
#include "../engine/replication_log.hpp"
#include <boost/asio.hpp>
#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

using namespace l3kv;

void test_repl_log() {
  std::cout << "TEST: Replication Log..." << std::endl;
  ReplicationLog log(10);

  Mutation m1;
  m1.key = "k1";
  log.append(std::move(m1));
  Mutation m2;
  m2.key = "k2";
  log.append(std::move(m2));

  assert(log.size() == 2);

  auto batch = log.pop_batch(1);
  assert(batch.size() == 1);
  assert(batch[0].key == "k1");
  assert(log.size() == 1);

  auto batch2 = log.pop_batch(5);
  assert(batch2.size() == 1);
  assert(batch2[0].key == "k2");
  assert(log.empty());

  std::cout << "PASS" << std::endl;
}

void test_mesh_loopback() {
  std::cout << "TEST: Mesh Loopback (Networking)..." << std::endl;
  boost::asio::io_context io;

  // Server Node (ID 100, Port 9000)
  Mesh server(io, 100, 9000);
  server.listen();

  std::atomic<int> received_count{0};
  server.set_on_message(
      [&](NodeID id, Lane lane, const std::vector<uint8_t> &pay) {
        std::string s(pay.begin(), pay.end());
        std::cout << "Server received on lane " << (int)lane << ": " << s
                  << std::endl;
        received_count++;
      });

  // Client Node (ID 200, Port 9001)
  Mesh client(io, 200, 9001);
  // Connect Client -> Server
  client.connect(100, "127.0.0.1", 9000);

  // Run IO in background thread
  std::thread t([&io]() { io.run(); });

  // Give time to connect
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Send message
  std::string msg = "Hello Lane 0";
  client.send(100, Lane::Express, lite3cpp::Buffer(std::vector<uint8_t>(msg.begin(), msg.end())));


  // Wait for receipt
  for (int i = 0; i < 10; ++i) {
    if (received_count > 0)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  io.stop();
  t.join();

  assert(received_count == 1);
  std::cout << "PASS" << std::endl;
}

int main() {
  test_repl_log();
  test_mesh_loopback();
  std::cout << "All Phase 2 Tests Passed!" << std::endl;
  return 0;
}
