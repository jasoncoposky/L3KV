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
            auto identity_str = identity.to_string();
            auto& opcode_msg = recv_msgs[2];
            if (opcode_msg.size() == 0) continue;
            char opcode = *static_cast<char*>(opcode_msg.data());

            if (opcode == 'A' && recv_msgs.size() >= 5) {
                // AUTH [ClientUID] [Secret]
                uint32_t uid = std::stoul(recv_msgs[3].to_string());
                std::string secret = recv_msgs[4].to_string();
                
                if (server_secret_.empty() || secret == server_secret_) {
                    session_identities_[identity_str] = uid;
                    std::cout << "[ZmqServer] Auth SUCCESS for UID " << uid << " (identity=" << identity_str << ")" << std::endl;
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("OK", 2), zmq::send_flags::none);
                } else {
                    std::cerr << "[ZmqServer] Auth FAILED for UID " << uid << std::endl;
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("ERR_AUTH", 8), zmq::send_flags::none);
                }
                continue;
            }

            // Check if identity is authenticated
            if (!session_identities_.contains(identity_str)) {
                // For backward compatibility during migration, if server_secret is empty, allow anonymous
                if (!server_secret_.empty()) {
                    std::cerr << "[ZmqServer] Rejected unauthenticated message from " << identity_str << std::endl;
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("ERR_UNAUTH", 8), zmq::send_flags::none);
                    continue;
                }
                // If no secret, treat as anonymous user 0
                session_identities_[identity_str] = 0; 
            }

            uint32_t current_uid = session_identities_[identity_str];

            if (opcode == 'G' && recv_msgs.size() >= 4) {
                std::string key = recv_msgs[3].to_string();
                
                // Permission Check
                auto perm = engine_->credentials().check_permission(current_uid, key);
                if (!(perm & Permission::READ) && !(perm & Permission::ADMIN)) {
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("ERR_FORBIDDEN", 13), zmq::send_flags::none);
                    continue;
                }

                lite3cpp::Buffer val = engine_->get(key);
                
                socket.send(identity, zmq::send_flags::sndmore);
                socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                socket.send(zmq::message_t(val.data(), val.size()), zmq::send_flags::none);
            }
            else if (opcode == 'P' && recv_msgs.size() >= 5) {
                std::string key = recv_msgs[3].to_string();
                std::string val = recv_msgs[4].to_string();
                
                // Permission Check
                auto perm = engine_->credentials().check_permission(current_uid, key);
                if (!(perm & Permission::WRITE) && !(perm & Permission::ADMIN)) {
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("ERR_FORBIDDEN", 13), zmq::send_flags::none);
                    continue;
                }

                engine_->put(key, val);
                socket.send(identity, zmq::send_flags::sndmore);
                socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                socket.send(zmq::message_t("OK", 2), zmq::send_flags::none);
            }
            else if (opcode == 'B' && recv_msgs.size() >= 4) {
                // BATCH operations require ADMIN for now
                auto perm = engine_->credentials().check_permission(current_uid, "*");
                if (!(perm & Permission::ADMIN)) {
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("ERR_FORBIDDEN", 13), zmq::send_flags::none);
                    continue;
                }

                auto& batch_msg = recv_msgs[3];
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

