#pragma once
#include "libconveyor/conveyor_modern.hpp"
#include "wal_storage.hpp"
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace l3kv {

enum class WalOp : uint8_t {
  PUT = 1,
  PATCH_I64 = 2,
  DELETE_ = 3,
  BATCH = 4,
  PATCH_STR = 5
};

struct BatchOp {
  WalOp op;
  std::string key;
  std::string value;
};

#pragma pack(push, 1)
struct LogHeader {
  uint32_t crc;
  uint8_t op;
  uint16_t key_len;
  uint32_t payload_len;
};
#pragma pack(pop)

class WriteAheadLog {
  struct FileHandle {
    HANDLE h;
    FileHandle(const std::string &p) {
      h = CreateFileA(p.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ,
                      NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
      if (h == INVALID_HANDLE_VALUE) {
        DWORD err = GetLastError();
        throw std::runtime_error("Failed to open WAL file: " + p +
                                 " Error: " + std::to_string(err));
      }
    }
    ~FileHandle() {
      if (h != INVALID_HANDLE_VALUE)
        CloseHandle(h);
    }
  };

  struct PendingCommit {
    WalOp op;
    std::string key;
    std::string payload;
    std::vector<BatchOp> batch;
    bool is_batch;
    std::promise<bool> promise;
  };

  std::string path_;
  FileHandle file_; // Destroyed LAST (after wal_)
  std::unique_ptr<libconveyor::v2::Conveyor>
      wal_; // Destroyed FIRST (flushes to file_)

  std::mutex mx_;
  std::vector<uint8_t> scratch_;

  std::mutex queue_mx_;
  std::condition_variable queue_cv_;
  std::queue<std::shared_ptr<PendingCommit>> commit_queue_;
  bool stop_flusher_ = false;
  std::thread flusher_thread_;

  static uint32_t compute_crc(uint8_t op, std::string_view key,
                              std::string_view payload) {
    uint32_t crc = 0xFFFFFFFF;
    auto process = [&](const void *data, size_t len) {
      const uint8_t *p = (const uint8_t *)data;
      for (size_t i = 0; i < len; i++) {
        crc ^= p[i];
        for (int j = 0; j < 8; j++)
          crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
      }
    };
    process(&op, sizeof(op));
    process(key.data(), key.size());
    process(payload.data(), payload.size());
    return ~crc;
  }

  void append_internal(WalOp op, std::string_view key,
                       std::string_view payload) {
    uint32_t crc = compute_crc((uint8_t)op, key, payload);

    LogHeader h{crc, (uint8_t)op, (uint16_t)key.size(),
                (uint32_t)payload.size()};
    size_t total_len = sizeof(h) + key.size() + payload.size();

    if (scratch_.capacity() < total_len)
      scratch_.reserve(total_len * 2);
    scratch_.clear();

    const uint8_t *p = (const uint8_t *)&h;
    scratch_.insert(scratch_.end(), p, p + sizeof(h));
    scratch_.insert(scratch_.end(), key.begin(), key.end());
    scratch_.insert(scratch_.end(), payload.begin(), payload.end());

    auto res = wal_->write(scratch_);
    if (!res)
      std::cerr << "WAL Write Error: " << res.error().message() << "\n";
  }

  void format_and_append_batch_internal(const std::vector<BatchOp> &ops) {
    size_t estimated_size = 4;
    for (const auto &op : ops) {
      estimated_size += 1 + 2 + op.key.size() + 4 + op.value.size();
    }

    std::vector<uint8_t> buf;
    buf.reserve(estimated_size);

    uint32_t count = (uint32_t)ops.size();
    auto append4 = [&](uint32_t v) {
      uint8_t *p = (uint8_t *)&v;
      buf.insert(buf.end(), p, p + 4);
    };
    auto append2 = [&](uint16_t v) {
      uint8_t *p = (uint8_t *)&v;
      buf.insert(buf.end(), p, p + 2);
    };

    append4(count);

    for (const auto &op : ops) {
      buf.push_back((uint8_t)op.op);
      append2((uint16_t)op.key.size());
      buf.insert(buf.end(), op.key.begin(), op.key.end());
      append4((uint32_t)op.value.size());
      buf.insert(buf.end(), op.value.begin(), op.value.end());
    }

    append_internal(WalOp::BATCH, "",
                    std::string_view((char *)buf.data(), buf.size()));
  }

  void flusher_loop() {
    std::vector<std::shared_ptr<PendingCommit>> current_batch;
    current_batch.reserve(256);

    while (true) {
      {
        std::unique_lock lock(queue_mx_);
        queue_cv_.wait(
            lock, [this]() { return !commit_queue_.empty() || stop_flusher_; });

        if (stop_flusher_ && commit_queue_.empty())
          break;

        while (!commit_queue_.empty()) {
          current_batch.push_back(commit_queue_.front());
          commit_queue_.pop();
        }
      }

      if (current_batch.empty())
        continue;

      if (wal_) {
        std::lock_guard lock(mx_);
        for (auto &req : current_batch) {
          if (req->is_batch) {
            format_and_append_batch_internal(req->batch);
          } else {
            append_internal(req->op, req->key, req->payload);
          }
        }
        wal_->flush();
      }

      for (auto &req : current_batch) {
        req->promise.set_value(true);
      }
      current_batch.clear();
    }
  }

public:
  explicit WriteAheadLog(std::string path)
      : path_(std::move(path)), file_(path_) {}

  ~WriteAheadLog() {
    {
      std::lock_guard lock(queue_mx_);
      stop_flusher_ = true;
    }
    queue_cv_.notify_all();
    if (flusher_thread_.joinable())
      flusher_thread_.join();
  }

  void append(WalOp op, std::string_view key, std::string_view payload) {
    auto req = std::make_shared<PendingCommit>();
    req->is_batch = false;
    req->op = op;
    req->key = key;
    req->payload = payload;
    auto fut = req->promise.get_future();
    {
      std::lock_guard lock(queue_mx_);
      commit_queue_.push(req);
    }
    queue_cv_.notify_one();
    fut.wait();
  }

  void append_batch(const std::vector<BatchOp> &ops) {
    if (ops.empty())
      return;
    auto req = std::make_shared<PendingCommit>();
    req->is_batch = true;
    req->batch = ops;
    auto fut = req->promise.get_future();
    {
      std::lock_guard lock(queue_mx_);
      commit_queue_.push(req);
    }
    queue_cv_.notify_one();
    fut.wait();
  }

  using RecoverCallback =
      std::function<void(WalOp, std::string_view, std::string_view)>;
  void recover(RecoverCallback callback) {
    std::cout << "DEBUG: WAL::recover start" << std::endl;

    LARGE_INTEGER fileSize;
    if (!GetFileSizeEx(file_.h, &fileSize)) {
      std::cerr << "WAL: Failed to get file size. Error: " << GetLastError()
                << "\n";
      throw std::runtime_error("WAL: Failed to get file size");
    }

    off_t offset = 0;

    if (fileSize.QuadPart > 0) {
      // Initialize temporary Reader Conveyor for buffered recovery
      libconveyor::v2::Config read_cfg;
      read_cfg.handle = (storage_handle_t)file_.h;
      read_cfg.ops = wal::WindowsStorage::get_ops();
      read_cfg.write_capacity = 64 * 1024;
      read_cfg.read_capacity = 10 * 1024 * 1024;

      std::cout << "DEBUG: Creating reader conveyor..." << std::endl;
      auto read_create_res = libconveyor::v2::Conveyor::create(read_cfg);

      if (!read_create_res) {
        std::cerr << "WAL Recovery: Failed to create reader conveyor: "
                  << read_create_res.error().message()
                  << ". Skipping recovery.\n";
      } else {
        std::cout << "DEBUG: Reader created." << std::endl;
        auto &reader = read_create_res.value();

        struct Buffered {
          libconveyor::v2::Conveyor &r;
          off_t &off;
          std::vector<uint8_t> &buf;

          bool read(void *dest, size_t len, const char *ctx) {
            buf.resize(len);
            auto res = r.read(buf);
            if (!res) {
              std::cerr << "WAL Recovery [" << ctx
                        << "]: Read error: " << res.error().message() << "\n";
              return false;
            }
            if (res.value() != len) {
              if (res.value() == 0 && std::strcmp(ctx, "HEADER") == 0)
                return false;
              if (res.value() > 0) {
                std::cerr << "WAL Recovery [" << ctx << "]: Partial read ("
                          << res.value() << "/" << len << ")\n";
              }
              return false;
            }
            std::memcpy(dest, buf.data(), len);
            off += (off_t)len;
            return true;
          }
        };

        std::vector<uint8_t> read_buf;
        Buffered b{reader, offset, read_buf};

        while (true) {
          LogHeader h;
          if (!b.read(&h, sizeof(h), "HEADER"))
            break;

          std::string key(h.key_len, '\0');
          std::string payload(h.payload_len, '\0');

          if (h.key_len > 0) {
            if (!b.read(key.data(), h.key_len, "KEY")) {
              std::cerr << "WAL Recovery: Truncated key at offset " << offset
                        << "\n";
              break;
            }
          }

          if (h.payload_len > 0) {
            if (!b.read(payload.data(), h.payload_len, "PAYLOAD")) {
              std::cerr << "WAL Recovery: Truncated payload at offset "
                        << offset << "\n";
              break;
            }
          }

          uint32_t computed = compute_crc(h.op, key, payload);
          if (computed != h.crc) {
            if (h.crc == 0 && computed != 0) {
              std::cerr << "WAL WARNING: Zero CRC allowed for legacy.\n";
            } else {
              std::cerr << "WAL ERROR: CRC Mismatch at offset " << offset
                        << ". Corrupt.\n";
              break;
            }
          }

          if ((WalOp)h.op == WalOp::BATCH) {
            const uint8_t *ptr = (const uint8_t *)payload.data();
            const uint8_t *end = ptr + payload.size();

            if (payload.size() < 4) {
              std::cerr << "WAL: Corrupt batch (too small)\n";
              continue;
            }
            uint32_t count = *(uint32_t *)ptr;
            ptr += 4;

            for (uint32_t i = 0; i < count; ++i) {
              if (ptr + 1 > end)
                break;
              uint8_t op_byte = *ptr++;

              if (ptr + 2 > end)
                break;
              uint16_t klen = *(uint16_t *)ptr;
              ptr += 2;

              if (ptr + klen > end)
                break;
              std::string_view k((const char *)ptr, klen);
              ptr += klen;

              if (ptr + 4 > end)
                break;
              uint32_t vlen = *(uint32_t *)ptr;
              ptr += 4;

              if (ptr + vlen > end)
                break;
              std::string_view v((const char *)ptr, vlen);
              ptr += vlen;

              callback((WalOp)op_byte, k, v);
            }
          } else {
            callback((WalOp)h.op, key, payload);
          }
        }
        std::cout << "DEBUG: Recovery loop done. Offset: " << offset
                  << std::endl;
      }
    } else {
      std::cout << "DEBUG: WAL file is empty (Cold Start). Skipping recovery."
                << std::endl;
    }

    std::cerr << "WAL Recovery: Completed at offset " << offset << "\n";

    // Initialize Writer Conveyor
    libconveyor::v2::Config cfg;
    cfg.handle = (storage_handle_t)file_.h;
    cfg.ops = wal::WindowsStorage::get_ops();
    cfg.write_capacity = 20 * 1024 * 1024;
    cfg.read_capacity = 5 * 1024 * 1024;

    std::cout << "DEBUG: Creating Writer Conveyor..." << std::endl;
    auto create_res = libconveyor::v2::Conveyor::create(cfg);
    if (!create_res)
      throw std::system_error(create_res.error());
    wal_ = std::make_unique<libconveyor::v2::Conveyor>(
        std::move(create_res.value()));
    std::cout << "DEBUG: Writer Conveyor created." << std::endl;

    auto seek_res = wal_->seek(0, SEEK_END);
    if (!seek_res) {
      std::cerr << "WAL: Seeding failure: " << seek_res.error().message()
                << "\n";
    }

    flusher_thread_ = std::thread([this]() { flusher_loop(); });
  }

  void flush() {
    std::lock_guard lock(mx_);
    if (wal_) {
      auto res = wal_->flush();
      if (!res) {
        std::cerr << "WAL Flush Error: " << res.error().message() << "\n";
      }
    }
  }

  auto stats() {
    if (!wal_)
      return libconveyor::v2::Conveyor::Stats{};
    return wal_->stats();
  }
};

} // namespace l3kv