#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <cstdint>

namespace l3kv {
class Engine;

class ZmqServer {
public:
    ZmqServer(Engine* engine, const std::string& address, uint16_t port);
    ~ZmqServer();

    void run();
    void stop();
    void set_secret(const std::string& secret) { server_secret_ = secret; }

private:
    Engine* engine_;
    std::string address_;
    uint16_t port_;
    bool running_;
    std::string server_secret_;
    std::unordered_map<std::string, uint32_t> session_identities_; // Identity -> ClientUID
};

} // namespace l3kv
