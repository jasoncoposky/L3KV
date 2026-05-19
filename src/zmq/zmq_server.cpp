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
            
            // Check if identity is authenticated
            if (!session_identities_.contains(identity_str)) {
                // Peek at opcode for AUTH
                if (recv_msgs.size() >= 5 && *static_cast<char*>(recv_msgs[2].data()) == 'A') {
                    // Handshake allowed without existing session
                } else if (!server_secret_.empty()) {
                    std::cerr << "[ZmqServer] Rejected unauthenticated message from " << identity_str << std::endl;
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("ERR_UNAUTH", 8), zmq::send_flags::none);
                    continue;
                } else {
                    session_identities_[identity_str] = 0; // Anonymous
                }
            }

            // New Protocol Structure: [Identity, Delimiter, EffectiveUID (4b), OpCode (1b), ...]
            if (recv_msgs.size() < 4) continue;
            
            uint32_t effective_uid = 0;
            if (recv_msgs[2].size() == 4) {
                effective_uid = *static_cast<uint32_t*>(recv_msgs[2].data());
            }

            auto& opcode_msg = recv_msgs[3];
            if (opcode_msg.size() == 0) continue;
            char opcode = *static_cast<char*>(opcode_msg.data());

            if (opcode == 'A' && recv_msgs.size() >= 5) {
                // AUTH [ClientUID] [Secret]
                // Note: Auth message uses different index if we stick to old structure for auth
                // Let's unify it: [Identity, Delimiter, 0, 'A', UID, Secret]
                uint32_t auth_uid = std::stoul(recv_msgs[4].to_string());
                std::string secret = recv_msgs[5].to_string();
                
                if (server_secret_.empty() || secret == server_secret_) {
                    session_identities_[identity_str] = auth_uid;
                    std::cout << "[ZmqServer] Auth SUCCESS for UID " << auth_uid << " (identity=" << identity_str << ")" << std::endl;
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("OK", 2), zmq::send_flags::none);
                } else {
                    std::cerr << "[ZmqServer] Auth FAILED for UID " << auth_uid << std::endl;
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("ERR_AUTH", 8), zmq::send_flags::none);
                }
                continue;
            }

            uint32_t session_uid = session_identities_[identity_str];
            
            // Principal Propagation:
            // If session is a trusted peer (ADMIN or INTERNAL), use the provided effective_uid.
            // Otherwise, enforce the session's own UID.
            uint32_t current_uid = session_uid;
            if (session_uid == ADMIN_UID || session_uid == INTERNAL_UID) {
                current_uid = effective_uid;
            }

            if (opcode == 'G' && recv_msgs.size() >= 5) {
                std::string key = recv_msgs[4].to_string();
                lite3cpp::Buffer val = engine_->get(key, current_uid);
                
                socket.send(identity, zmq::send_flags::sndmore);
                socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                socket.send(zmq::message_t(val.data(), val.size()), zmq::send_flags::none);
            }
            else if (opcode == 'P' && recv_msgs.size() >= 6) {
                std::string key = recv_msgs[4].to_string();
                std::string val = recv_msgs[5].to_string();
                
                try {
                    engine_->put(key, val, current_uid);
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("OK", 2), zmq::send_flags::none);
                } catch (const std::exception& e) {
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("ERR_FORBIDDEN", 13), zmq::send_flags::none);
                }
            }
            else if (opcode == 'D' && recv_msgs.size() >= 5) {
                std::string key = recv_msgs[4].to_string();

                bool ok = engine_->del(key, current_uid);
                socket.send(identity, zmq::send_flags::sndmore);
                socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                socket.send(zmq::message_t(ok ? "OK" : "ERR_FORBIDDEN", ok ? 2 : 13), zmq::send_flags::none);
            }
            else if (opcode == 'M' && recv_msgs.size() >= 5) {
                // MULTI-GET [Key1] [Key2] ...
                std::vector<std::string> keys;
                for (size_t i = 4; i < recv_msgs.size(); ++i) {
                    keys.push_back(recv_msgs[i].to_string());
                }

                lite3cpp::Buffer res_buf = engine_->batch_get(keys, current_uid);

                socket.send(identity, zmq::send_flags::sndmore);
                socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                socket.send(zmq::message_t(res_buf.data(), res_buf.size()), zmq::send_flags::none);
            }
            else if (opcode == 'B' && recv_msgs.size() >= 5) {
                auto& batch_msg = recv_msgs[4];
                std::vector<uint8_t> vec((uint8_t*)batch_msg.data(), (uint8_t*)batch_msg.data() + batch_msg.size());
                lite3cpp::Buffer batch_buf(std::move(vec));

                try {
                    engine_->batch_put(batch_buf, current_uid);
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("OK", 2), zmq::send_flags::none);
                } catch (const std::exception& e) {
                    socket.send(identity, zmq::send_flags::sndmore);
                    socket.send(zmq::message_t(), zmq::send_flags::sndmore);
                    socket.send(zmq::message_t("ERR_FORBIDDEN", 13), zmq::send_flags::none);
                }
            }
 else {
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

