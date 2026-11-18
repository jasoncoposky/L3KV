#include "engine/store.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace l3kv;

namespace {

void test_cas_basic() {
    std::string wal = "test_cas_basic.wal";
    std::filesystem::remove(wal);
    {
        Engine engine(wal, 1);

        // Create-if-absent succeeds when key missing
        auto r1 = engine.cas_put("ref:main", R"({"head":"c1"})", 0);
        assert(r1.ok);
        assert(r1.new_hash != 0);

        // Create-if-absent fails when key already exists
        auto r2 = engine.cas_put("ref:main", R"({"head":"c2"})", 0);
        assert(!r2.ok);
        assert(r2.actual_hash == r1.new_hash);

        // Update-if-match with wrong hash fails
        auto r3 = engine.cas_put("ref:main", R"({"head":"c2"})", r1.new_hash + 1);
        assert(!r3.ok);
        assert(r3.actual_hash == r1.new_hash);

        // Update-if-match with exact hash succeeds
        auto r4 = engine.cas_put("ref:main", R"({"head":"c2"})", r1.new_hash);
        assert(r4.ok);
        assert(r4.new_hash != 0);
        assert(r4.new_hash != r1.new_hash);

        auto buf = engine.get("ref:main");
        assert(buf.size() > 0);
        assert(buf.get_str(0, lite3cpp::HashedKey("head")) == "c2");
    }
    std::filesystem::remove(wal);
}

void test_cas_contention() {
    std::string wal = "test_cas_contention.wal";
    std::filesystem::remove(wal);
    {
        Engine engine(wal, 1);
        auto init = engine.cas_put("counter", R"({"v":0})", 0);
        assert(init.ok);

        constexpr int kThreads = 8;
        constexpr int kIters = 150;
        std::vector<std::thread> threads;
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&]() {
                for (int i = 0; i < kIters; ++i) {
                    while (true) {
                        auto blob = engine.get_blob("counter");
                        assert(blob != nullptr);
                        uint64_t h = blob->content_hash();
                        int64_t v = blob->buf_.get_i64(0, lite3cpp::HashedKey("v"));
                        std::string next = R"({"v":)" + std::to_string(v + 1) + "}";
                        auto res = engine.cas_put("counter", std::move(next), h);
                        if (res.ok) {
                            break;
                        }
                    }
                }
            });
        }
        for (auto& th : threads) {
            th.join();
        }
        auto final_buf = engine.get("counter");
        assert(final_buf.get_i64(0, lite3cpp::HashedKey("v")) == kThreads * kIters);
    }
    std::filesystem::remove(wal);
}

void test_ttl_and_wal_recovery() {
    std::string wal = "test_ttl_recovery.wal";
    std::filesystem::remove(wal);
    {
        Engine engine(wal, 1);
        engine.put("lock:{ov1}:1", R"({"holder":1})", ADMIN_UID, 60);
        engine.put("lock:{ov1}:2", R"({"holder":2})", ADMIN_UID, 5000);

        auto rows = engine.scan_prefix_kv("lock:{ov1}:", "", 10);
        assert(rows.size() == 2);

        std::this_thread::sleep_for(std::chrono::milliseconds(80));

        // Key 1 expired, Key 2 still alive
        assert(engine.get("lock:{ov1}:1").size() == 0);
        assert(engine.get("lock:{ov1}:2").size() > 0);

        auto rows_after = engine.scan_prefix_kv("lock:{ov1}:", "", 10);
        assert(rows_after.size() == 1);
        assert(rows_after[0].first == "lock:{ov1}:2");

        // CAS create-if-absent succeeds on expired key
        auto cas_expired = engine.cas_put("lock:{ov1}:1", R"({"holder":9})", 0, ADMIN_UID, 50);
        assert(cas_expired.ok);
        assert(engine.get("lock:{ov1}:1").size() > 0);

        std::this_thread::sleep_for(std::chrono::milliseconds(70));
        size_t swept = engine.sweep_expired_keys();
        assert(swept >= 1);
    }

    // Reopen WAL and verify TTLs survived recovery
    {
        Engine engine(wal, 1);
        assert(engine.get("lock:{ov1}:1").size() == 0);
        assert(engine.get("lock:{ov1}:2").size() > 0);
    }
    std::filesystem::remove(wal);
}

} // namespace

int main() {
    test_cas_basic();
    test_cas_contention();
    test_ttl_and_wal_recovery();
    std::cout << "test_cas_ttl PASSED\n";
    return 0;
}
