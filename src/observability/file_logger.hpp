#pragma once

#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <chrono>
#include <iomanip>

namespace l3kv {

class FileLogger {
public:
    static FileLogger& instance() {
        static FileLogger logger;
        return logger;
    }

    void open(const std::string& path) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (file_.is_open()) file_.close();
        file_.open(path, std::ios::out | std::ios::app);
        if (!file_.is_open()) {
            std::cerr << "Failed to open log file: " << path << std::endl;
        }
    }

    void log(const std::string& level, const std::string& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!file_.is_open()) return;

        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

        file_ << "[" << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S") 
              << "." << std::setfill('0') << std::setw(3) << ms.count() << "] "
              << "[" << level << "] " << msg << std::endl;
        file_.flush();
    }

private:
    FileLogger() = default;
    ~FileLogger() {
        if (file_.is_open()) file_.close();
    }

    std::ofstream file_;
    std::mutex mutex_;
};

#define L3_LOG(level, msg) l3kv::FileLogger::instance().log(level, msg)
#define L3_INFO(msg) L3_LOG("INFO", msg)
#define L3_ERROR(msg) L3_LOG("ERROR", msg)
#define L3_DEBUG(msg) L3_LOG("DEBUG", msg)

} // namespace l3kv
