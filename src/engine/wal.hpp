#pragma once
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
  std::string_view key;
  std::string_view value;
};

struct WalStats {
  size_t bytes_written{0};
  size_t bytes_read{0};
  int last_error_code{0};
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
    HANDLE h{INVALID_HANDLE_VALUE};
#else
    int fd{-1};
#endif

    explicit FileHandle(const std::string &p) {
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
  };

  std::string path_;
  FileHandle file_;
  mutable std::mutex write_mutex_;
  std::atomic<size_t> bytes_written_{0};
  std::atomic<size_t> bytes_read_{0};
  std::atomic<int> last_error_code_{0};

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

  bool write_raw(const void* data, size_t len) {
    if (!data || len == 0) return true;
    std::lock_guard<std::mutex> lock(write_mutex_);
#ifdef _WIN32
    DWORD written = 0;
    if (!WriteFile(file_.h, data, static_cast<DWORD>(len), &written, NULL) || written != len) {
      last_error_code_.store(static_cast<int>(GetLastError()), std::memory_order_relaxed);
      return false;
    }
#else
    const char* ptr = static_cast<const char*>(data);
    size_t remaining = len;
    while (remaining > 0) {
      ssize_t n = write(file_.fd, ptr, remaining);
      if (n < 0) {
        if (errno == EINTR) continue;
        last_error_code_.store(errno, std::memory_order_relaxed);
        return false;
      }
      ptr += n;
      remaining -= static_cast<size_t>(n);
    }
#endif
    bytes_written_.fetch_add(len, std::memory_order_relaxed);
    return true;
  }

public:
  explicit WriteAheadLog(std::string path)
      : path_(std::move(path)), file_(path_) {
#ifdef _WIN32
    SetFilePointer(file_.h, 0, NULL, FILE_END);
#else
    lseek(file_.fd, 0, SEEK_END);
#endif
  }

  ~WriteAheadLog() {
    flush();
  }

  void append(WalOp op, std::string_view key, std::string_view payload) {
    uint32_t crc = compute_crc((uint8_t)op, key, payload);
    size_t total_len = sizeof(LogHeader) + key.size() + payload.size();

    std::vector<uint8_t> buffer(total_len);
    uint8_t* ptr = buffer.data();
    
    LogHeader h{crc, (uint8_t)op, (uint16_t)key.size(), (uint32_t)payload.size()};
    std::memcpy(ptr, &h, sizeof(h));
    ptr += sizeof(h);
    
    std::memcpy(ptr, key.data(), key.size());
    ptr += key.size();
    
    std::memcpy(ptr, payload.data(), payload.size());
    
    write_raw(buffer.data(), buffer.size());
  }

  void append_batch(const std::vector<BatchOp> &ops) {
    if (ops.empty()) return;

    size_t payload_size = 4; // count
    for (const auto &op : ops) {
      payload_size += 1 + 2 + op.key.size() + 4 + op.value.size();
    }

    size_t total_len = sizeof(LogHeader) + payload_size;
    std::vector<uint8_t> buffer(total_len);
    uint8_t* ptr = buffer.data();
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

    write_raw(buffer.data(), buffer.size());
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

    std::fprintf(stderr, "[WAL RECOVER] Starting recovery. fileSize64 = %llu\n", (unsigned long long)fileSize64);
    size_t recovered_ops = 0;

    if (fileSize64 > 0) {
        uint64_t current_offset = 0;
#ifdef _WIN32
        SetFilePointer(file_.h, 0, NULL, FILE_BEGIN);
#else
        lseek(file_.fd, 0, SEEK_SET);
#endif

        while (current_offset < fileSize64) {
#ifdef _WIN32
            LogHeader h;
            DWORD win_read = 0;
            if (!ReadFile(file_.h, &h, sizeof(h), &win_read, NULL) || win_read != sizeof(h)) break;
            if (current_offset + sizeof(h) + h.key_len + h.payload_len > fileSize64) break;
            std::string key(h.key_len, '\0');
            std::string payload(h.payload_len, '\0');
            if (h.key_len > 0) ReadFile(file_.h, key.data(), h.key_len, &win_read, NULL);
            if (h.payload_len > 0) ReadFile(file_.h, payload.data(), h.payload_len, &win_read, NULL);
            if (compute_crc(h.op, key, payload) != h.crc) break;
            current_offset += sizeof(h) + h.key_len + h.payload_len;
#else
            LogHeader h;
            if (current_offset + sizeof(h) > fileSize64) {
                std::fprintf(stderr, "[WAL RECOVER] EOF at offset %llu (fileSize64=%llu)\n", (unsigned long long)current_offset, (unsigned long long)fileSize64);
                break;
            }
            if (pread(file_.fd, &h, sizeof(h), current_offset) != sizeof(h)) {
                std::fprintf(stderr, "[WAL RECOVER] Failed to pread header at offset %llu\n", (unsigned long long)current_offset);
                break;
            }

            if (h.op == 0 && h.crc == 0 && h.key_len == 0 && h.payload_len == 0) {
                std::fprintf(stderr, "[WAL RECOVER] Zero entry at offset %llu\n", (unsigned long long)current_offset);
                break;
            }

            if (current_offset + sizeof(h) + h.key_len + h.payload_len > fileSize64) {
                std::fprintf(stderr, "[WAL RECOVER] Oversize payload at offset %llu: klen=%u plen=%u fileSize=%llu\n", (unsigned long long)current_offset, h.key_len, h.payload_len, (unsigned long long)fileSize64);
                break;
            }

            std::string key(h.key_len, '\0');
            std::string payload(h.payload_len, '\0');
            if (h.key_len > 0) {
                if (pread(file_.fd, key.data(), h.key_len, current_offset + sizeof(h)) != (ssize_t)h.key_len) {
                    std::fprintf(stderr, "[WAL RECOVER] Failed to pread key at offset %llu\n", (unsigned long long)current_offset);
                    break;
                }
            }
            if (h.payload_len > 0) {
                if (pread(file_.fd, payload.data(), h.payload_len, current_offset + sizeof(h) + h.key_len) != (ssize_t)h.payload_len) {
                    std::fprintf(stderr, "[WAL RECOVER] Failed to pread payload at offset %llu\n", (unsigned long long)current_offset);
                    break;
                }
            }

            if (compute_crc(h.op, key, payload) != h.crc) {
                std::fprintf(stderr, "[WAL RECOVER] CRC mismatch at offset %llu: expected=%08x\n", (unsigned long long)current_offset, h.crc);
                break;
            }

            current_offset += sizeof(h) + h.key_len + h.payload_len;
#endif

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
                        recovered_ops++;
                    }
                }
            } else {
                callback((WalOp)h.op, key, payload);
                recovered_ops++;
            }
        }
        std::fprintf(stderr, "[WAL RECOVER] Completed! Recovered %zu ops. Final offset: %llu / %llu\n", recovered_ops, (unsigned long long)current_offset, (unsigned long long)fileSize64);
#ifdef _WIN32
        LARGE_INTEGER li; li.QuadPart = current_offset;
        SetFilePointerEx(file_.h, li, NULL, FILE_BEGIN);
        SetEndOfFile(file_.h);
#else
        if (current_offset < fileSize64) {
            (void)ftruncate(file_.fd, current_offset);
        }
        lseek(file_.fd, current_offset, SEEK_SET);
#endif
        bytes_read_.store(current_offset, std::memory_order_relaxed);
    }
  }

  void flush() {
    std::lock_guard<std::mutex> lock(write_mutex_);
#ifdef _WIN32
    FlushFileBuffers(file_.h);
#else
    fdatasync(file_.fd);
#endif
  }

  WalStats stats() const {
    return WalStats{bytes_written_.load(std::memory_order_relaxed),
                    bytes_read_.load(std::memory_order_relaxed),
                    last_error_code_.load(std::memory_order_relaxed)};
  }
};

} // namespace l3kv