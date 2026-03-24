# Benchmark YCSB: Workload A
add_executable(bench_ycsb src/tests_cpp/bench_ycsb.cpp)
target_include_directories(bench_ycsb PRIVATE
    src
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/liblite3client/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/liblite3client/external"
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/lite3-cpp/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/lib/concurrentqueue"
)
target_link_libraries(bench_ycsb PRIVATE
    lite3client
    lite3-cpp
)

add_executable(engine_bench src/tests_cpp/engine_bench.cpp src/engine/clock.cpp)
target_include_directories(engine_bench PRIVATE src)
target_link_libraries(engine_bench PRIVATE Threads::Threads l3kv_engine)
if(MSVC)
    add_definitions(-D_WIN32_WINNT=0x0A00 -DNOMINMAX)
endif()
