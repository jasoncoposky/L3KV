#include "zmq_server.hpp"
#include "engine/store.hpp"
#include <zmq.hpp>
#include <zmq_addon.hpp>
#include <iostream>
#include <thread>

namespace l3kv {

ZmqServer::ZmqServer(Engine* engine, const std::string& address, uint16_t port)
    : engine_(engine), address_(address), port_(port), running_(false) {}

ZmqServer::~ZmqServer() {
    stop();
}

void ZmqServer::stop() {
    running_ = false;
}

void ZmqServer::run() {
    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_ROUTER);

    // Set high-water marks to prevent memory ballooning
    socket.set(zmq::sockopt::sndhwm, 5000);
    socket.set(zmq::sockopt::rcvhwm, 5000);

    std::string endpoint = "tcp://" + address_ + ":" + std::to_string(port_);
    socket.bind(endpoint);

    std::cout << "[ZmqServer] Listening on " << endpoint << std::endl;

    running_ = true;
    while (running_) {
        std::vector<zmq::message_t> recv_msgs;
        try {
            auto result = zmq::recv_multipart(socket, std::back_inserter(recv_msgs), zmq::recv_flags::dontwait);
            if (!result) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                continue;
            }

            if (recv_msgs.size() < 3) {
                std::cerr << "[ZmqServer] Received malformed message: size=" << recv_msgs.size() << std::endl;
                continue;
            }

            auto& identity = recv_msgs[0];
            auto& opcode_msg = recv_msgs[2];
            if (opcode_msg.size() == 0) continue;
            char opcode = *static_cast<char*>(opcode_msg.data());

            if (opcode == 'G' && recv_msgs.size() >= 4) {
                std::string key = recv_msgs[3].to_string();
                lite3cpp::Buffer val = engine_->get(key);
                
                socket.send(identity, zmq::send_flags::sndmore);
                socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                socket.send(zmq::message_t(val.data(), val.size()), zmq::send_flags::none);
            }
            else if (opcode == 'P' && recv_msgs.size() >= 5) {
                std::string key = recv_msgs[3].to_string();
                std::string val = recv_msgs[4].to_string();
                
                engine_->put(key, val);
                socket.send(identity, zmq::send_flags::sndmore);
                socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                socket.send(zmq::message_t("OK", 2), zmq::send_flags::none);
            }
            else if (opcode == 'B' && recv_msgs.size() >= 4) {
                auto& batch_msg = recv_msgs[3];
                // Use a managed copy to avoid lifetime issues
                std::vector<uint8_t> vec((uint8_t*)batch_msg.data(), (uint8_t*)batch_msg.data() + batch_msg.size());
                lite3cpp::Buffer batch_buf(std::move(vec));
                
                engine_->batch_put(batch_buf);
                socket.send(identity, zmq::send_flags::sndmore);
                socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                socket.send(zmq::message_t("OK", 2), zmq::send_flags::none);
            } else {
                std::cerr << "[ZmqServer] Unknown OpCode or missing frames: " << opcode << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "[ZmqServer] Runtime Error: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "[ZmqServer] Unknown Runtime Error" << std::endl;
        }
    }
}


} // namespace l3kv

