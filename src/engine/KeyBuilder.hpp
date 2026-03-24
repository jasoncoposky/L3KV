#pragma once

#include <string_view>
#include <string>
#include <cstring>
#include <cstdio>
#include <array>

namespace l3kv {

class KeyBuilder {
public:
    static constexpr size_t MAX_KEY_SIZE = 1024;

    // Build meta key: {key}:meta
    static std::string_view meta_key(std::string_view key) {
        char* buf = get_buffer();
        int len = std::snprintf(buf, MAX_KEY_SIZE, "%.*s:meta", 
                                static_cast<int>(key.size()), key.data());
        if (len < 0 || len >= static_cast<int>(MAX_KEY_SIZE)) return {};
        return std::string_view(buf, static_cast<size_t>(len));
    }

private:
    static char* get_buffer() {
        // Dual-buffer circular recycler to prevent aliasing with two simultaneous keys
        static thread_local std::array<std::array<char, MAX_KEY_SIZE>, 2> buffers;
        static thread_local size_t index = 0;
        char* buf = buffers[index].data();
        index = (index + 1) % 2;
        return buf;
    }
};

} // namespace l3kv
