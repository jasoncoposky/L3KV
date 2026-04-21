#include "mesh.hpp"
#include "observability.hpp"
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/strand.hpp>
#include <deque>
#include <iostream>
#include <thread>

namespace l3kv {

static std::string lane_to_string(Lane l) {
  switch (l) {
  case Lane::Control:
    return "control";
  case Lane::Express:
    return "express";
  case Lane::Standard:
    return "standard";
  case Lane::Heavy:
    return "heavy";
  }
  return "unknown";
}

// Internal connection handler
class Mesh::Connection : public std::enable_shared_from_this<Connection> {
public:
  Connection(boost::asio::ip::tcp::socket socket, Mesh *mesh)
      : socket_(std::move(socket)), mesh_(mesh),
        strand_(boost::asio::make_strand(
            boost::asio::any_io_executor(mesh->io_context_.get_executor()))) {}

  void start(bool is_outbound, NodeID local_id) {
    auto self(shared_from_this());
    boost::asio::post(strand_, [this, self, is_outbound, local_id]() {
      if (is_outbound) {
        do_send_id(local_id);
      } else {
        do_read_id();
      }
    });
  }

  void on_identified(std::function<void(NodeID)> cb) { on_id_ = cb; }

  void do_close() {
    auto self(shared_from_this());
    boost::asio::post(strand_, [this, self]() {
      boost::system::error_code ec;
      socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);
      socket_.close(ec);
    });
  }

  void send(lite3cpp::Buffer payload) {
    boost::asio::post(
        strand_, [self = shared_from_this(), p = std::move(payload)]() mutable {
          bool write_in_progress = !self->outbox_.empty();
          self->outbox_.push_back(std::move(p));
          if (!write_in_progress) {
            self->do_write();
          }
        });
  }

  boost::asio::ip::tcp::socket &socket() { return socket_; }

private:
  boost::asio::ip::tcp::socket socket_;
  Mesh *mesh_;
  boost::asio::strand<boost::asio::any_io_executor> strand_;
  NodeID peer_id_ = 0;
  std::function<void(NodeID)> on_id_;
  std::vector<uint8_t> read_buffer_; // For body
  uint32_t header_buffer_[2];        // [0]=Lane, [1]=Size
  uint32_t handshake_id_ = 0;
  std::deque<lite3cpp::Buffer> outbox_;

  void do_send_id(NodeID my_id) {
    auto self(shared_from_this());
    handshake_id_ = my_id;
    boost::asio::async_write(
        socket_, boost::asio::buffer(&handshake_id_, 4),
        boost::asio::bind_executor(
            strand_, [this, self](boost::system::error_code ec, std::size_t) {
              if (!ec) {
                do_read_header();
              }
            }));
  }

  void do_read_id() {
    auto self(shared_from_this());
    boost::asio::async_read(
        socket_, boost::asio::buffer(&handshake_id_, 4),
        boost::asio::bind_executor(
            strand_, [this, self](boost::system::error_code ec, std::size_t) {
              if (!ec) {
                peer_id_ = handshake_id_;
                if (on_id_)
                  on_id_(peer_id_);
                do_read_header();
              }
            }));
  }

  void do_read_header() {
    auto self(shared_from_this());
    boost::asio::async_read(
        socket_, boost::asio::buffer(header_buffer_, sizeof(header_buffer_)),
        boost::asio::bind_executor(
            strand_,
            [this, self](boost::system::error_code ec, std::size_t /*length*/) {
              if (!ec) {
                uint32_t lane = header_buffer_[0];
                uint32_t size = header_buffer_[1];
                do_read_body(lane, size);
              } else {
                // Handle close/error
              }
            }));
  }

  void do_read_body(uint32_t lane, uint32_t size) {
    auto self(shared_from_this());
    lite3cpp::Buffer read_buf;
    read_buf.init_object(); // Allocate or ensure capacity if needed, but we'll use a vector for reading
    auto read_vec = std::make_shared<std::vector<uint8_t>>(size);
    boost::asio::async_read(
        socket_, boost::asio::buffer(*read_vec),
        boost::asio::bind_executor(strand_, [this, self, lane, size,
                                             read_vec](boost::system::error_code ec,
                                                       std::size_t /*length*/) {
          if (!ec) {
            lite3cpp::Buffer final_buf(std::move(*read_vec));
            if (mesh_->on_message_) {
              mesh_->on_message_(self->peer_id_, static_cast<Lane>(lane), final_buf);
#ifndef LITE3CPP_DISABLE_OBSERVABILITY
              if (auto *m =
                      lite3cpp::g_metrics.load(std::memory_order_relaxed)) {
                m->increment_mesh_bytes(lane_to_string(static_cast<Lane>(lane)),
                                        size, false);
              }
#endif
            }
            do_read_header(); // Loop
          }
        }));
  }

  void do_write() {
    auto self(shared_from_this());
    auto& front = outbox_.front();
    boost::asio::async_write(
        socket_, boost::asio::buffer(front.data(), front.size()),
        boost::asio::bind_executor(
            strand_,
            [this, self](boost::system::error_code ec, std::size_t /*length*/) {
              if (!ec) {
                outbox_.pop_front();
                if (!outbox_.empty()) {
                  do_write();
                }
              }
            }));
  }
};

struct Mesh::Peer {
  NodeID id;
  std::string host;
  int port;
  // In full impl: 3 pointers for 3 lanes. For basic: 1 connection per lane or 1
  // shared? Spec says "Multi-Lane". Implementing 1 connection multiplexed for
  // simplicity first, or 3 separate sockets. Let's do 1 socket multiplexed
  // logic for now to get it running (header has Lane ID).
  std::shared_ptr<Connection> conn;
};

Mesh::Mesh(boost::asio::io_context &io_context, NodeID my_id, int port)
    : io_context_(io_context), my_id_(my_id), port_(port),
      acceptor_(io_context, boost::asio::ip::tcp::endpoint(
                                boost::asio::ip::tcp::v4(), port)) {}

Mesh::~Mesh() {}

void Mesh::listen() { do_accept(); }

void Mesh::do_accept() {
  acceptor_.async_accept([this](boost::system::error_code ec,
                                boost::asio::ip::tcp::socket socket) {
    if (!ec) {
      // ... (existing logic)
    } else {
      // ERROR: Add delay to avoid CPU spin
      auto timer = std::make_shared<boost::asio::steady_timer>(io_context_);
      timer->expires_after(std::chrono::milliseconds(100));
      timer->async_wait(
          [this, timer](const boost::system::error_code &) { do_accept(); });
      return;
    }
    do_accept();
  });
}

void Mesh::connect(NodeID peer_id, const std::string &host, int port) {
  boost::asio::ip::tcp::resolver resolver(io_context_);
  auto endpoints = resolver.resolve(host, std::to_string(port));

  boost::asio::ip::tcp::socket socket(io_context_);
  boost::asio::connect(socket, endpoints);

  auto conn = std::make_shared<Connection>(std::move(socket), this);
  conn->start(true, my_id_);

  std::lock_guard<std::mutex> lock(peers_mx_);
  auto peer = std::make_shared<Peer>();
  peer->id = peer_id;
  peer->host = host;
  peer->port = port;
  peer->conn = conn;
  peers_[peer_id] = peer;
}

bool Mesh::send(NodeID peer_id, Lane lane, lite3cpp::Buffer payload) {
  std::shared_ptr<Peer> peer;
  {
    std::lock_guard<std::mutex> lock(peers_mx_);
    auto it = peers_.find(peer_id);
    if (it == peers_.end())
      return false;
    peer = it->second;
  }

  if (!peer->conn)
    return false;

  // Frame: [Lane:4][Size:4][Body...]
  uint32_t header[2];
  header[0] = static_cast<uint32_t>(lane);
  header[1] = static_cast<uint32_t>(payload.size());

  lite3cpp::Buffer frame;
  frame.init_object();
  frame.set_bytes(0, "h", std::span(reinterpret_cast<const std::byte*>(header), sizeof(header)));
  frame.set_bytes(0, "b", std::span(reinterpret_cast<const std::byte*>(payload.data()), payload.size()));

  int lat = latency_ms_.load();
  if (lat > 0) {
    auto timer = std::make_shared<boost::asio::steady_timer>(io_context_);
    timer->expires_after(std::chrono::milliseconds(lat));
    timer->async_wait([timer, peer, f = std::move(frame)](
                          const boost::system::error_code &ec) mutable {
      if (!ec) {
        if (peer->conn)
          peer->conn->send(std::move(f));
      }
    });
  } else {
    peer->conn->send(std::move(frame));
  }

#ifndef LITE3CPP_DISABLE_OBSERVABILITY
  if (auto *m = lite3cpp::g_metrics.load(std::memory_order_relaxed)) {
    m->increment_mesh_bytes(lane_to_string(lane), payload.size(), true);
  }
#endif

  return true;
}

void Mesh::set_on_message(MessageCallback cb) { on_message_ = cb; }

std::vector<NodeID> Mesh::get_active_peers() {
  std::lock_guard<std::mutex> lock(peers_mx_);
  std::vector<NodeID> ids;
  for (auto &pair : peers_) {
    ids.push_back(pair.first);
  }
  return ids;
}

} // namespace l3kv
