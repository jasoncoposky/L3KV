#pragma once

#include "libconveyor/conveyor_modern.hpp"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>

namespace wal {

// Windows implementation of storage operations using OVERLAPPED for
// pwrite/pread semantics
struct WindowsStorage {
  static ssize_t pwrite_impl(storage_handle_t handle, const void *buf,
                             size_t count, off_t offset) {
    HANDLE hFile = static_cast<HANDLE>(handle);
    OVERLAPPED ov = {0};
    ov.Offset = static_cast<DWORD>(offset);
    ov.OffsetHigh = static_cast<DWORD>(offset >> 32);

    DWORD bytesWritten = 0;
    if (!WriteFile(hFile, buf, static_cast<DWORD>(count), &bytesWritten, &ov)) {
      // Check for IO_PENDING if we were doing async, but here we are
      // synchronous block-based WriteFile with OVERLAPPED is still "atomic" wrt
      // the file pointer but might return FALSE on error
      return -1;
    }
    return static_cast<ssize_t>(bytesWritten);
  }

  static ssize_t pread_impl(storage_handle_t handle, void *buf, size_t count,
                            off_t offset) {
    HANDLE hFile = static_cast<HANDLE>(handle);
    OVERLAPPED ov = {0};
    ov.Offset = static_cast<DWORD>(offset);
    ov.OffsetHigh = static_cast<DWORD>(offset >> 32);

    DWORD bytesRead = 0;
    if (!ReadFile(hFile, buf, static_cast<DWORD>(count), &bytesRead, &ov)) {
      // Handle EOF? ReadFile returns FALSE on EOF if using synchronous?
      // Actually ReadFile returns TRUE with bytesRead < count on EOF usually,
      // unless error.
      if (GetLastError() == ERROR_HANDLE_EOF) {
        return 0;
      }
      return -1;
    }
    return static_cast<ssize_t>(bytesRead);
  }

  static off_t lseek_impl(storage_handle_t handle, off_t offset, int whence) {
    HANDLE hFile = static_cast<HANDLE>(handle);
    LARGE_INTEGER li;
    li.QuadPart = offset;
    LARGE_INTEGER newPos;
    DWORD method;

    switch (whence) {
    case SEEK_SET:
      method = FILE_BEGIN;
      break;
    case SEEK_CUR:
      method = FILE_CURRENT;
      break;
    case SEEK_END:
      method = FILE_END;
      break;
    default:
      return -1;
    }

    if (!SetFilePointerEx(hFile, li, &newPos, method)) {
      return -1;
    }
    return static_cast<off_t>(newPos.QuadPart);
  }

  static storage_operations_t get_ops() {
    return {pwrite_impl, pread_impl, lseek_impl};
  }
};

} // namespace wal

#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>

namespace wal {

struct POSIXStorage {
  static ssize_t pwrite_impl(storage_handle_t handle, const void *buf,
                             size_t count, off_t offset) {
    int fd = (int)(intptr_t)handle;
    return pwrite(fd, buf, count, offset);
  }

  static ssize_t pread_impl(storage_handle_t handle, void *buf, size_t count,
                            off_t offset) {
    int fd = (int)(intptr_t)handle;
    return pread(fd, buf, count, offset);
  }

  static off_t lseek_impl(storage_handle_t handle, off_t offset, int whence) {
    int fd = (int)(intptr_t)handle;
    return lseek(fd, offset, whence);
  }

  static storage_operations_t get_ops() {
    return {pwrite_impl, (ssize_t(*)(storage_handle_t, void*, size_t, off_t))pread_impl, lseek_impl};
  }
};

} // namespace wal
#endif
