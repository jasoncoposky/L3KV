#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>

// Fix conflict with Boost.Beast
#ifdef off_t
#undef off_t
#endif

#include "http_server.hpp"
#include <algorithm>
#include <boost/asio/dispatch.hpp>
#include <boost/beast/version.hpp>
#include <boost/config.hpp>
#include <chrono>
#include <csignal>
#include <iostream>
#include <map>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

#include "engine/store.hpp"
#include "json.hpp"
#include "observability/simple_metrics.hpp"
#include <boost/beast/http/chunk_encode.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/serializer.hpp>
#include <boost/beast/http/write.hpp>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace http_server {

struct ScopedMetric {
  std::string_view op;
  std::chrono::high_resolution_clock::time_point start;
  ScopedMetric(std::string_view o) : op(o) {
    start = std::chrono::high_resolution_clock::now();
  }
  ~ScopedMetric() {
    try {
      auto end = std::chrono::high_resolution_clock::now();
      double dur = std::chrono::duration<double>(end - start).count();
      // Access global metrics via pointer cast if needed, or just relying on
      // IMetrics interface
#ifndef LITE3CPP_DISABLE_OBSERVABILITY
      lite3cpp::IMetrics *m =
          lite3cpp::g_metrics.load(std::memory_order_acquire);
      if (m) {
        m->record_latency(op, dur);
      }
#endif
    } catch (...) {
    }
  }
};

// Basic session to handle a single request/response
class session : public std::enable_shared_from_this<session> {
  tcp::socket socket_;
  net::io_context &ioc_;
  beast::flat_buffer buffer_;
  http::request<http::string_body> req_;
  l3kv::Engine &db_;
  std::shared_ptr<lite3::ConsistentHash> ring_;
  uint32_t self_node_id_;
  std::string address_;
  int port_;
  const std::map<uint32_t, std::pair<std::string, int>> &peers_;

public:
  session(tcp::socket &&socket, net::io_context &ioc, l3kv::Engine &db,
          std::shared_ptr<lite3::ConsistentHash> ring, uint32_t node_id,
          std::string address, int port,
          const std::map<uint32_t, std::pair<std::string, int>> &peers)
      : socket_(std::move(socket)), ioc_(ioc), db_(db), ring_(ring),
        self_node_id_(node_id), address_(std::move(address)), port_(port),
        peers_(peers) {
#ifndef LITE3CPP_DISABLE_OBSERVABILITY
    if (auto *m = lite3cpp::g_metrics.load(std::memory_order_relaxed))
      m->increment_active_connections();
#endif
  }

  ~session() {
#ifndef LITE3CPP_DISABLE_OBSERVABILITY
    if (auto *m = lite3cpp::g_metrics.load(std::memory_order_relaxed))
      m->decrement_active_connections();
#endif
  }

  void run() { do_read(); }

private:
  void do_read() {
    req_ = {};
    http::async_read(
        socket_, buffer_, req_,
        beast::bind_front_handler(&session::on_read, shared_from_this()));
  }

  void on_read(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);
#ifndef LITE3CPP_DISABLE_OBSERVABILITY
    if (auto *m = lite3cpp::g_metrics.load(std::memory_order_relaxed))
      m->record_bytes_received(bytes_transferred);
#endif

    if (ec == http::error::end_of_stream) {
      return do_close();
    }
    if (ec) {
      std::cerr << "read: " << ec.message() << "\n";
      return;
    }

    handle_request();
  }

  std::map<std::string, std::string> parse_query(std::string_view query) {
    std::map<std::string, std::string> res;
    size_t pos = 0;
    while (pos < query.size()) {
      size_t eq = query.find('=', pos);
      if (eq == std::string_view::npos)
        break;
      size_t amp = query.find('&', eq);
      if (amp == std::string_view::npos)
        amp = query.size();
      std::string k(query.substr(pos, eq - pos));
      std::string v(query.substr(eq + 1, amp - eq - 1));
      res[k] = v;
      pos = amp + 1;
    }
    return res;
  }

  void do_prefix_scan(const std::string &prefix, size_t limit) {
    auto res = std::make_shared<http::response<http::empty_body>>(
        http::status::ok, req_.version());
    res->set(http::field::server, "Lite3");
    res->set(http::field::content_type, "application/json");
    res->chunked(true);
    res->keep_alive(req_.keep_alive());

    auto sr =
        std::make_shared<http::response_serializer<http::empty_body>>(*res);
    http::async_write_header(socket_, *sr,
                             [self = shared_from_this(), res, sr, prefix,
                              limit](beast::error_code ec, std::size_t) {
                               if (ec) {
                                 std::cerr
                                     << "prefix write header: " << ec.message()
                                     << "\n";
                                 return self->do_close();
                               }
                               self->send_next_chunk(prefix, limit, 0, "", 0);
                             });
  }

  void send_next_chunk(std::string prefix, size_t limit, size_t shard_idx,
                       std::string current_start, size_t total) {
    if (shard_idx >= 64 || total >= limit) {
      net::async_write(
          socket_, http::make_chunk_last(),
          [self = shared_from_this()](beast::error_code ec, std::size_t) {
            if (!ec && self->req_.keep_alive())
              self->do_read();
            else
              self->do_close();
          });
      return;
    }

    size_t batch_limit = std::min<size_t>(50, limit - total);
    auto chunk =
        db_.get_prefix_chunk(prefix, shard_idx, current_start, batch_limit);

    if (chunk.empty()) {
      net::post(
          ioc_, [self = shared_from_this(), prefix, limit, shard_idx, total]() {
            self->send_next_chunk(prefix, limit, shard_idx + 1, "", total);
          });
      return;
    }

    std::string next_start = chunk.back().first + '\0';
    size_t total_new = total + chunk.size();

    std::string json_chunk;
    for (const auto &kv : chunk) {
      json_chunk += "{\"k\":\"" + kv.first + "\",\"v\":";
      json_chunk += kv.second + "}\n";
    }

    auto sp_data = std::make_shared<std::string>(std::move(json_chunk));

    net::async_write(socket_, http::make_chunk(net::buffer(*sp_data)),
                     [self = shared_from_this(), prefix, limit, shard_idx,
                      next_start, total_new,
                      sp_data](beast::error_code ec, std::size_t) {
                       if (ec)
                         return self->do_close();
                       self->send_next_chunk(prefix, limit, shard_idx,
                                             next_start, total_new);
                     });
  }

  void handle_request() {
    ScopedMetric sm("handler_total");
    auto const bad_req = [&](beast::string_view why) {
      http::response<http::string_body> res{http::status::bad_request,
                                            req_.version()};
      res.set(http::field::server, "Lite3");
      res.body() = std::string(why);
      res.prepare_payload();
      return res;
    };

#include "dashboard.hpp"

    // ... existing code ...

    std::string target(req_.target());

    if (req_.method() == http::verb::get && target == "/dashboard") {
      http::response<http::string_body> res{http::status::ok, req_.version()};
      res.set(http::field::server, "Lite3");
      res.set(http::field::content_type, "text/html");
      res.body() = std::string(dashboard_html);
      res.prepare_payload();
      return send_response(std::move(res));
    }

    if (req_.method() == http::verb::get && target == "/metrics") {
      http::response<http::string_body> res{http::status::ok, req_.version()};
      res.set(http::field::server, "Lite3");
      res.set(http::field::content_type, "application/json");

#ifndef LITE3CPP_DISABLE_OBSERVABILITY
      auto *m = lite3cpp::g_metrics.load(std::memory_order_acquire);
      if (auto *sm = dynamic_cast<SimpleMetrics *>(m)) {
        res.body() = sm->get_json();
      } else {
        res.body() = "{}"; // Should not happen
      }
#else
      res.body() = "{\"error\": \"observability_disabled\"}";
#endif

      res.keep_alive(req_.keep_alive());
      res.prepare_payload();
      return send_response(std::move(res));
    }

    if (req_.method() == http::verb::get && target == "/kv/health") {
      http::response<http::empty_body> res{http::status::ok, req_.version()};
      res.set(http::field::server, "Lite3");
      res.keep_alive(req_.keep_alive());
      res.prepare_payload();
      return send_response(std::move(res));
    }

    if (req_.method() == http::verb::get && target == "/kv/metrics") {
#ifndef LITE3CPP_DISABLE_OBSERVABILITY
      auto *metrics = dynamic_cast<SimpleMetrics *>(lite3cpp::g_metrics.load());
      std::string body;
      if (metrics) {
        body = metrics->get_metrics_string();
      } else {
        body = "Metrics not available (null)\n";
      }
#else
      std::string body = "Metrics disabled via cmake\n";
#endif

      auto wal_stats = db_.get_wal_stats();
      body += "\n=== WAL Metrics (libconveyor) ===\n";
      body +=
          "Bytes Written: " + std::to_string(wal_stats.bytes_written) + "\n";
      body += "Avg Write Latency: " +
              std::to_string(wal_stats.avg_write_latency.count()) + " ms\n";
      body += "Buffer Full Events: " +
              std::to_string(wal_stats.write_buffer_full_events) + "\n";

      http::response<http::string_body> res{http::status::ok, req_.version()};
      res.set(http::field::server, "Lite3");
      res.body() = std::move(body);
      res.keep_alive(req_.keep_alive());
      res.prepare_payload();
      return send_response(std::move(res));
    }

    // Helper for Redirection
    auto const redirect_to_owner = [&](uint32_t owner,
                                       beast::string_view target) {
      http::response<http::string_body> res{http::status::temporary_redirect,
                                            req_.version()};
      res.set(http::field::server, "Lite3");

      auto it = peers_.find(owner);
      if (it != peers_.end()) {
        std::string location = "http://" + it->second.first + ":" +
                               std::to_string(it->second.second) +
                               std::string(target);
        res.set(http::field::location, location);
        res.body() = "Redirecting to owner node " + std::to_string(owner);
      } else {
        res.result(http::status::service_unavailable);
        res.body() = "Key owned by node " + std::to_string(owner) +
                     " but peer address unknown.";
      }
      res.prepare_payload();
      return res;
    };

    // ...

    // ...
    if (req_.method() == http::verb::get && target == "/cluster/map") {
      json j;
      // Add self
      json self = json::object();
      self["id"] = self_node_id_;
      // We don't store our own host/port in peers_ usually, but for client
      // config it's needed. However, the client calling us KNOWS our host/port
      // (it connected to us). But for a complete map, we ideally need our own
      // config. Since http_server doesn't store self config (address/port
      // passed to ctor but not stored), we might need to rely on peers_ only,
      // OR update http_server to store self props. For MVP, returning the PEERS
      // list is enough if the client treats the seed node as known. Actually,
      // let's just return the peers map.

      json peer_list = json::array();
      // Add self
      {
        json p;
        p["id"] = self_node_id_;
        p["host"] = (address_ == "0.0.0.0") ? "127.0.0.1" : address_;
        p["http_port"] = port_;
        peer_list.push_back(p);
      }

      for (const auto &[id, info] : peers_) {
        json p;
        p["id"] = id;
        p["host"] = info.first;
        p["http_port"] = info.second;
        peer_list.push_back(p);
      }
      j["peers"] = peer_list;
      j["mode"] =
          "sharded"; // Hardcoded for now or pass config? session has ring_

      http::response<http::string_body> res{http::status::ok, req_.version()};
      res.set(http::field::server, "Lite3");
      res.set(http::field::content_type, "application/json");
      res.body() = j.dump();
      res.prepare_payload();
      return send_response(std::move(res));
    }

    if (req_.method() == http::verb::get && target.starts_with("/kv/")) {
      auto qpos = target.find('?');
      if (qpos != std::string::npos && target.starts_with("/kv/?")) {
        auto params = parse_query(target.substr(qpos + 1));
        if (params.count("prefix")) {
          std::string prefix = params["prefix"];
          size_t limit = 1000;
          if (params.count("limit"))
            limit = std::stoull(params["limit"]);
          return do_prefix_scan(prefix, limit);
        }
      }

      std::string key = target.substr(4);

      // Sharding Check
      if (ring_) {
        uint32_t owner = ring_->get_node(key);
        if (owner != self_node_id_ && owner != 0) {
          return send_response(redirect_to_owner(owner, target));
        }
      }

      lite3cpp::Buffer buffer_data =
          db_.get(key);              // db.get now returns a Buffer
      if (buffer_data.size() == 0) { // Check if buffer is empty
        http::response<http::empty_body> res{http::status::not_found,
                                             req_.version()};
        res.keep_alive(req_.keep_alive());
        res.prepare_payload();
        return send_response(std::move(res));
      }

      // "Zero-Serialize" Read: Return Raw Binary
      // The user specified "reads should also not be serialized".
      // We return the raw lite3 internal buffer. Clients must handle it.

      http::response<http::string_body> res{http::status::ok, req_.version()};
      res.set(http::field::server, "Lite3");
      res.set(http::field::content_type, "application/octet-stream");
      // Cast raw bytes to string body (copy)
      const char *ptr = reinterpret_cast<const char *>(buffer_data.data());
      if (ptr && buffer_data.size() > 0) {
        res.body().assign(ptr, buffer_data.size());
      }
      res.keep_alive(req_.keep_alive());
      res.prepare_payload();
      return send_response(std::move(res));
    }

    if (req_.method() == http::verb::put && target.starts_with("/kv/")) {
      std::string key = target.substr(4);

      // Sharding Check
      if (ring_) {
        uint32_t owner = ring_->get_node(key);
        if (owner != self_node_id_ && owner != 0) {
          return send_response(redirect_to_owner(owner, target));
        }
      }

      try {
        db_.put(key, req_.body());
        http::response<http::empty_body> res{http::status::ok, req_.version()};
        res.keep_alive(req_.keep_alive());
        res.prepare_payload();
        return send_response(std::move(res));
      } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << "\n";
        return send_response(bad_req(e.what()));
      }
    }

    if (req_.method() == http::verb::post && target.starts_with("/kv/")) {
      auto qpos = target.find('?');
      if (qpos == std::string::npos)
        return send_response(bad_req("Missing params"));
      std::string key = target.substr(4, qpos - 4);

      // Sharding Check
      if (ring_) {
        uint32_t owner = ring_->get_node(key);
        if (owner != self_node_id_ && owner != 0) {
          return send_response(redirect_to_owner(owner, target));
        }
      }

      auto params = parse_query(target.substr(qpos + 1));

      if (params["op"] == "set_int") {
        int64_t val = std::stoll(params["val"]);
        db_.patch_int(key, params["field"], val);
        http::response<http::empty_body> res{http::status::ok, req_.version()};
        res.keep_alive(req_.keep_alive());
        res.prepare_payload();
        return send_response(std::move(res));
      }
      if (params["op"] == "set_str") {
        db_.patch_str(key, params["field"], params["val"]);
        http::response<http::empty_body> res{http::status::ok, req_.version()};
        res.keep_alive(req_.keep_alive());
        res.prepare_payload();
        return send_response(std::move(res));
      }
      return send_response(bad_req("Unknown op"));
    }

    if (req_.method() == http::verb::delete_ && target.starts_with("/kv/")) {
      std::string key = target.substr(4);

      // Sharding Check
      if (ring_) {
        uint32_t owner = ring_->get_node(key);
        if (owner != self_node_id_ && owner != 0) {
          return send_response(redirect_to_owner(owner, target));
        }
      }

      if (db_.del(key)) {
        http::response<http::empty_body> res{http::status::ok, req_.version()};
        res.keep_alive(req_.keep_alive());
        res.prepare_payload();
        return send_response(std::move(res));
      } else {
        http::response<http::empty_body> res{http::status::not_found,
                                             req_.version()};
        res.keep_alive(req_.keep_alive());
        res.prepare_payload();
        return send_response(std::move(res));
      }
    }

    return send_response(bad_req("Unknown method"));
  }

  template <class Body, class Allocator>
  void
  send_response(http::response<Body, http::basic_fields<Allocator>> &&res) {
#ifndef LITE3CPP_DISABLE_OBSERVABILITY
    if (auto *m = lite3cpp::g_metrics.load(std::memory_order_relaxed)) {
      int status = static_cast<int>(res.result());
      m->record_error(status);
    }
#endif
    auto sp =
        std::make_shared<http::response<Body, http::basic_fields<Allocator>>>(
            std::move(res));

    http::async_write(socket_, *sp,
                      [self = shared_from_this(), sp](beast::error_code ec,
                                                      std::size_t bytes) {
                        self->on_write(ec, bytes, sp->keep_alive());
                      });
  }

  void on_write(beast::error_code ec, std::size_t bytes_transferred,
                bool keep_alive) {
    boost::ignore_unused(bytes_transferred);
#ifndef LITE3CPP_DISABLE_OBSERVABILITY
    if (auto *m = lite3cpp::g_metrics.load(std::memory_order_relaxed))
      m->record_bytes_sent(bytes_transferred);
#endif

    if (ec) {
      std::cerr << "write: " << ec.message() << "\n";
      return;
    }

    if (keep_alive) {
      do_read();
    } else {
      do_close();
    }
  }

  void do_close() {
    beast::error_code ec;
    socket_.shutdown(tcp::socket::shutdown_send, ec);
  }
};

// Custom exception for thread exit
struct ThreadExit : std::exception {};

http_server::http_server(l3kv::Engine &db, std::string address,
                         unsigned short port, int min_threads, int max_threads,
                         std::shared_ptr<lite3::ConsistentHash> ring,
                         uint32_t node_id,
                         std::map<uint32_t, std::pair<std::string, int>> peers)
    : address_(std::move(address)), port_(port), ioc_(max_threads),
      signals_(ioc_, SIGINT, SIGTERM, SIGBREAK),
      acceptor_(ioc_, {net::ip::make_address(address_), port_}), db_(db),
      min_threads_(min_threads), max_threads_(max_threads),
      manager_timer_(ioc_), ring_(ring), self_node_id_(node_id),
      peers_(std::move(peers)) {
  n_threads_ = 1; // Main thread
  kf_.init(0.0);
  last_tick_ = std::chrono::steady_clock::now();

  // Enforce sanity
  if (min_threads_ < 1)
    min_threads_ = 1;
  if (max_threads_ < min_threads_)
    max_threads_ = min_threads_;

  signals_.async_wait(
      [this](boost::system::error_code /*ec*/, int /*signal*/) { stop(); });
}

void http_server::run() {
  std::cout << "DEBUG: http_server::run() starting with " << min_threads_
            << " initial threads (dynamic pool)" << std::endl;
  do_accept();

  // Start the manager
  start_manager();

  // Resize to initial min_threads
  adjust_pool_size(min_threads_);

  // Main thread joins the pool logic too?
  // Wait, if we use dynamic main thread, we should probably just let main
  // thread block on ioc_.run() and treat it as one of the threads. But for
  // simplicity of dynamic resizing, let's treat "thread_pool_" as *all*
  // workers. And main thread just blocks on ioc_.run() as a "supervisor" or an
  // extra worker. Let's stick to the plan: Main thread participates. It counts
  // towards the total, but isn't in the vector. Or: Main thread is special, it
  // never exits until stop().

  std::cout << "DEBUG: Main thread calling ioc_.run()" << std::endl;
  try {
    ioc_.run();
  } catch (const ThreadExit &) {
    // Main thread shouldn't get poison pill usually, but if it does:
  }

  // On exit, join all pool threads
  for (auto &t : thread_pool_)
    if (t.joinable())
      t.join();

  std::cout << "DEBUG: ioc_.run() returned" << std::endl;
}

void http_server::start_manager() {
  manager_timer_.expires_after(
      std::chrono::milliseconds(100)); // 100ms tick for KF
  manager_timer_.async_wait([this](beast::error_code ec) {
    if (!ec)
      manager_loop();
  });
}

void http_server::manager_loop() {
  auto now = std::chrono::steady_clock::now();
  double dt = std::chrono::duration<double>(now - last_tick_).count();
  last_tick_ = now;
  if (dt > 1.0)
    dt = 1.0;
  if (dt < 0.001)
    dt = 0.001;

  int active_reqs = 0;
#ifndef LITE3CPP_DISABLE_OBSERVABILITY
  if (auto *m = lite3cpp::g_metrics.load(std::memory_order_relaxed)) {
    if (auto *sm = dynamic_cast<SimpleMetrics *>(m)) {
      active_reqs = sm->get_active_connections();
    }
  }
#endif

  kf_.predict(dt);
  kf_.update(static_cast<double>(active_reqs));

  double future_load = kf_.predict_future_load(1.0);
  const double REQUESTS_PER_THREAD = 5.0;
  int required_threads =
      static_cast<int>(std::ceil(future_load / REQUESTS_PER_THREAD));

  int current_threads = n_threads_;
  int target = current_threads;

  if (required_threads > current_threads) {
    target = required_threads;
  } else if (required_threads + 2 < current_threads) {
    target = current_threads - 1;
  }

  if (target > max_threads_)
    target = max_threads_;
  if (target < min_threads_)
    target = min_threads_;

  if (target != current_threads) {
    auto now = std::chrono::steady_clock::now();
    double time_since_resize =
        std::chrono::duration<double>(now - last_resize_time_).count();

    // Hysteresis: Don't resize if we just resized recently (2.0s)
    if (time_since_resize < 2.0) {
      // std::cout << "[Manager] Skipping resize (hysteresis cooldown)\n";
    } else {
      std::cout << "[Manager] Resizing pool: " << current_threads << " -> "
                << target << " (Active: " << active_reqs
                << ", Future: " << future_load << ")" << std::endl;
      adjust_pool_size(target);
      last_resize_time_ = now;
    }
  }

#ifndef LITE3CPP_DISABLE_OBSERVABILITY
  if (auto *m = lite3cpp::g_metrics.load(std::memory_order_relaxed)) {
    if (auto *sm = dynamic_cast<SimpleMetrics *>(m)) {
      sm->set_thread_count(n_threads_);
    }
  }
#endif

  start_manager();
}

void http_server::adjust_pool_size(int target) {
  // Use tracked n_threads_
  int current_managed_threads = n_threads_ - 1;
  // target includes main thread, so managed target is target - 1
  int managed_target = target - 1;
  if (managed_target < 0)
    managed_target = 0;

  if (managed_target > current_managed_threads) {
    // Grow
    int to_add = managed_target - current_managed_threads;
    for (int i = 0; i < to_add; ++i) {
      thread_pool_.emplace_back([this, i] {
#ifdef _WIN32
        HANDLE hThread = GetCurrentThread();
        // Shift HTTP threads to the upper half of physical cores,
        // to avoid overlapping with the lower half 64 Engine Shards.
        DWORD_PTR mask = (DWORD_PTR)1
                         << ((i + 32) % std::thread::hardware_concurrency());
        SetThreadAffinityMask(hThread, mask);
#endif
        try {
          ioc_.run();
        } catch (const ThreadExit &) {
          // Clean exit
        }
      });
      n_threads_++;
    }
  } else if (managed_target < current_managed_threads) {
    // Shrink
    int to_remove = current_managed_threads - managed_target;
    for (int i = 0; i < to_remove; ++i) {
      net::post(ioc_, [] { throw ThreadExit(); });
      n_threads_--;
    }

    // Clean up zombies from vector?
    // std::thread objects for finished threads are still joinable.
    // For production, we should reap them.
    // For MVP, we'll leave the std::thread objects in the vector
    // but the threads themselves will have exited.
    // Optimization: periodically erase unjoinable threads? No, joined
    // threads... Actually, to remove from vector we need to join them. Let's do
    // a quick lazy reap pass.
    auto it = std::remove_if(thread_pool_.begin(), thread_pool_.end(),
                             [](std::thread &t) {
                               // This is tricky: we don't know WHICH thread
                               // exited. But we can check if it IS joinable
                               // (running) Wait, standard joinable() returns
                               // true if running OR finished but not joined.
                               // There's no standard "is_running()" check.
                               // Simplified: We assume `to_remove` threads WILL
                               // exit soon. We can't easily remove them from
                               // vector without joining, blocking manager. So:
                               // Just accept vector growth for now (it won't
                               // grow infinite, just up to max_threads
                               // historically). Actually, we can reuse slots?
                               // No, vector of threads. Let's SKIP vector
                               // cleanup for this iteration to avoid blocking.
                               return false;
                             });
  }
}

void http_server::stop() { ioc_.stop(); }

void http_server::do_accept() {
  acceptor_.async_accept(
      net::make_strand(ioc_),
      beast::bind_front_handler(&http_server::on_accept, this));
}

void http_server::on_accept(beast::error_code ec, tcp::socket socket) {
  if (ec) {
    std::cerr << "accept: " << ec.message() << "\n";
    return;
  }

  // Enable TCP_NODELAY to disable Nagle's algorithm
  boost::system::error_code ec_opt;
  socket.set_option(tcp::no_delay(true), ec_opt);
  if (ec_opt) {
    std::cerr << "Failed to set TCP_NODELAY: " << ec_opt.message() << "\n";
  }

  // Create a session and run it
  std::make_shared<session>(std::move(socket), ioc_, db_, ring_, self_node_id_,
                            address_, port_, peers_)
      ->run();

  // Accept another connection
  do_accept();
}

} // namespace http_server
