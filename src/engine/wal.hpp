#pragma once
#include "libconveyor/conveyor_modern.hpp"
#include "wal_storage.hpp"
#include <array>
#include <atomic>
#include <cstring>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <errno.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif
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
#ifdef _WIN32
    HANDLE h;
#else
    int fd;
#endif

    FileHandle(const std::string &p) {
#ifdef _WIN32
      h = CreateFileA(p.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ,
                      NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
      if (h == INVALID_HANDLE_VALUE) {
        DWORD err = GetLastError();
        throw std::runtime_error("Failed to open WAL file: " + p +
                                 " Error: " + std::to_string(err));
      }
#else
      fd = open(p.c_str(), O_RDWR | O_CREAT, 0644);
      if (fd < 0) {
        throw std::runtime_error("Failed to open WAL file: " + p +
                                 " Error: " + std::to_string(errno));
      }
#endif
    }
    ~FileHandle() {
#ifdef _WIN32
      if (h != INVALID_HANDLE_VALUE)
        CloseHandle(h);
#else
      if (fd >= 0)
        close(fd);
#endif
    }

    storage_handle_t get() const {
#ifdef _WIN32
      return (storage_handle_t)h;
#else
      return (storage_handle_t)(intptr_t)fd;
#endif
    }
  };

  std::string path_;
  FileHandle file_;
  std::unique_ptr<libconveyor::v2::Conveyor> wal_;

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

public:
  explicit WriteAheadLog(std::string path)
      : path_(std::move(path)), file_(path_) {}

  ~WriteAheadLog() {
      if (wal_) wal_->flush();
  }

  void append(WalOp op, std::string_view key, std::string_view payload) {
    if (!wal_) return;
    
    uint32_t crc = compute_crc((uint8_t)op, key, payload);
    size_t total_len = sizeof(LogHeader) + key.size() + payload.size();

    void* dst = conveyor_reserve(wal_->impl_for_c_api(), total_len);
    if (!dst) return;

    uint8_t* ptr = static_cast<uint8_t*>(dst);
    
    LogHeader h{crc, (uint8_t)op, (uint16_t)key.size(), (uint32_t)payload.size()};
    std::memcpy(ptr, &h, sizeof(h));
    ptr += sizeof(h);
    
    std::memcpy(ptr, key.data(), key.size());
    ptr += key.size();
    
    std::memcpy(ptr, payload.data(), payload.size());
    
    conveyor_commit(wal_->impl_for_c_api(), total_len);
  }

  void append_batch(const std::vector<BatchOp> &ops) {
    if (ops.empty() || !wal_) return;

    size_t payload_size = 4; // count
    for (const auto &op : ops) {
      payload_size += 1 + 2 + op.key.size() + 4 + op.value.size();
    }

    size_t total_len = sizeof(LogHeader) + payload_size;
    void* dst = conveyor_reserve(wal_->impl_for_c_api(), total_len);
    if (!dst) return;

    uint8_t* ptr = static_cast<uint8_t*>(dst);
    uint8_t* payload_start = ptr + sizeof(LogHeader);

    // Format payload directly
    uint32_t count = (uint32_t)ops.size();
    std::memcpy(payload_start, &count, 4);
    uint8_t* p = payload_start + 4;

    for (const auto &op : ops) {
      *p++ = (uint8_t)op.op;
      uint16_t klen = (uint16_t)op.key.size();
      std::memcpy(p, &klen, 2); p += 2;
      std::memcpy(p, op.key.data(), klen); p += klen;
      uint32_t vlen = (uint32_t)op.value.size();
      std::memcpy(p, &vlen, 4); p += 4;
      std::memcpy(p, op.value.data(), vlen); p += vlen;
    }

    std::string_view payload_view((const char*)payload_start, payload_size);
    uint32_t crc = compute_crc((uint8_t)WalOp::BATCH, "", payload_view);

    LogHeader h{crc, (uint8_t)WalOp::BATCH, 0, (uint32_t)payload_size};
    std::memcpy(ptr, &h, sizeof(h));

    conveyor_commit(wal_->impl_for_c_api(), total_len);
  }

  using RecoverCallback =
      std::function<void(WalOp, std::string_view, std::string_view)>;

  void recover(RecoverCallback callback) {
    uint64_t fileSize64 = 0;
#ifdef _WIN32
    LARGE_INTEGER fileSize;
    if (!GetFileSizeEx(file_.h, &fileSize)) return;
    fileSize64 = fileSize.QuadPart;
#else
    struct stat st;
    if (fstat(file_.fd, &st) < 0) return;
    fileSize64 = st.st_size;
#endif

    if (fileSize64 > 0) {
        uint64_t current_offset = 0;
#ifdef _WIN32
        SetFilePointer(file_.h, 0, NULL, FILE_BEGIN);
#else
        lseek(file_.fd, 0, SEEK_SET);
#endif

        while (current_offset < fileSize64) {
            LogHeader h;
            size_t header_read = 0;
#ifdef _WIN32
            DWORD win_read = 0;
            if (!ReadFile(file_.h, &h, sizeof(h), &win_read, NULL) || win_read != sizeof(h)) break;
            header_read = win_read;
#else
            ssize_t posix_read = read(file_.fd, &h, sizeof(h));
            if (posix_read != sizeof(h)) break;
            header_read = posix_read;
#endif

            std::string key(h.key_len, '\0');
            std::string payload(h.payload_len, '\0');
            
            if (h.key_len > 0) {
#ifdef _WIN32
                ReadFile(file_.h, key.data(), h.key_len, &win_read, NULL);
#else
                read(file_.fd, key.data(), h.key_len);
#endif
            }
            if (h.payload_len > 0) {
#ifdef _WIN32
                ReadFile(file_.h, payload.data(), h.payload_len, &win_read, NULL);
#else
                read(file_.fd, payload.data(), h.payload_len);
#endif
            }

            if (compute_crc(h.op, key, payload) != h.crc) break;

            current_offset += sizeof(h) + h.key_len + h.payload_len;

            if ((WalOp)h.op == WalOp::BATCH) {
                const uint8_t *p = (const uint8_t *)payload.data();
                const uint8_t *end = p + payload.size();
                if (payload.size() >= 4) {
                    uint32_t count = *(uint32_t *)p; p += 4;
                    for (uint32_t i = 0; i < count; ++i) {
                        if (p + 7 > end) break;
                        uint8_t b_op = *p++;
                        uint16_t b_klen = *(uint16_t *)p; p += 2;
                        if (p + b_klen + 4 > end) break;
                        std::string_view b_key((const char *)p, b_klen); p += b_klen;
                        uint32_t b_vlen = *(uint32_t *)p; p += 4;
                        if (p + b_vlen > end) break;
                        std::string_view b_val((const char *)p, b_vlen); p += b_vlen;
                        callback((WalOp)b_op, b_key, b_val);
                    }
                }
            } else {
                callback((WalOp)h.op, key, payload);
            }
        }
#ifdef _WIN32
        LARGE_INTEGER li; li.QuadPart = 0;
        SetFilePointerEx(file_.h, li, NULL, FILE_END);
#else
        lseek(file_.fd, 0, SEEK_END);
#endif
    }

    libconveyor::v2::Config cfg;
    cfg.handle = file_.get();
#ifdef _WIN32
    cfg.ops = wal::WindowsStorage::get_ops();
#else
    cfg.ops = wal::POSIXStorage::get_ops();
#endif
    cfg.write_capacity = 20 * 1024 * 1024;
    cfg.read_capacity = 5 * 1024 * 1024;

    auto create_res = libconveyor::v2::Conveyor::create(cfg);
    if (create_res) {
        wal_ = std::make_unique<libconveyor::v2::Conveyor>(std::move(create_res.value()));
        wal_->seek(0, SEEK_END);
    }
  }

  void flush() { if (wal_) wal_->flush(); }
  auto stats() { return wal_ ? wal_->stats() : libconveyor::v2::Conveyor::Stats{}; }
};

} // namespace l3kv