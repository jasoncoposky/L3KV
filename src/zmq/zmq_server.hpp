#pragma once

#include <string>
#include <memory>

namespace l3kv {
class Engine;

class ZmqServer {
public:
    ZmqServer(Engine* engine, const std::string& address, uint16_t port);
    ~ZmqServer();

    void run();
    void stop();

private:
    Engine* engine_;
    std::string address_;
    uint16_t port_;
    bool running_;
};

} // namespace l3kv
