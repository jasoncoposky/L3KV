#pragma once

#include <string>
#include <string_view>

namespace l3kv {

class KeyBuilder {
public:
    // Build meta key: {key}:meta
    static std::string_view meta_key(std::string_view key) {
        thread_local std::string buf;
        buf.clear();
        buf.reserve(key.size() + 5);
        buf.append(key);
        buf.append(":meta", 5);
        return buf;
    }
};

} // namespace l3kv
