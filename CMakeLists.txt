cmake_minimum_required(VERSION 3.20)
project(L3KV VERSION 1.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)

# Dependencies
find_package(Threads REQUIRED)

set(LITE3CPP_DISABLE_OBSERVABILITY OFF CACHE BOOL "" FORCE)
if(LITE3CPP_DISABLE_OBSERVABILITY)
    add_compile_definitions(LITE3CPP_DISABLE_OBSERVABILITY)
endif()
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../lib/lite3-cpp ${CMAKE_CURRENT_SOURCE_DIR}/../lib/lite3-cpp/build)

set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../libconveyor ${CMAKE_CURRENT_SOURCE_DIR}/../libconveyor/build)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../lib/liblite3client ${CMAKE_CURRENT_SOURCE_DIR}/../lib/liblite3client/build)

include(FetchContent)
FetchContent_Declare(
  zstd
  GIT_REPOSITORY https://github.com/facebook/zstd.git
  GIT_TAG        v1.5.5
  SOURCE_SUBDIR  build/cmake
)
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(zstd)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/_deps/zstd-src/lib)

FetchContent_Declare(
  xxhash
  GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
  GIT_TAG        v0.8.2
  SOURCE_SUBDIR  cmake_unofficial
)
set(XXHASH_BUILD_ALIASES ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(xxhash)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/_deps/xxhash-src)

# ZeroMQ & cppzmq
include(FetchContent)

FetchContent_Declare(
  libzmq
  GIT_REPOSITORY https://github.com/zeromq/libzmq.git
  GIT_TAG        v4.3.5
)
set(ZMQ_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(libzmq)

FetchContent_Declare(
  cppzmq
  GIT_REPOSITORY https://github.com/zeromq/cppzmq.git
  GIT_TAG        v4.10.0
)
set(CPPZMQ_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(cppzmq)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

add_library(l3kv_engine STATIC
    src/engine/clock.cpp
    src/engine/mesh.cpp
    src/observability/simple_metrics.cpp
)

target_include_directories(l3kv_engine PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/../lib/lite3-cpp/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../libconveyor/include
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/boost_1_89_0"
    ${CMAKE_CURRENT_SOURCE_DIR}/_deps/zstd-src/lib
)
target_link_libraries(l3kv_engine PUBLIC
    lite3-cpp
    conveyor
    libzstd_static
    xxhash
)

add_executable(l3svc
    src/main.cpp
    src/zmq/zmq_server.cpp
    src/observability/simple_metrics.cpp
    src/engine/clock.cpp
    src/engine/mesh.cpp
)

target_include_directories(l3svc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/../lib/lite3-cpp/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../libconveyor/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../lib/liblite3client/external
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/boost_1_89_0"
)

target_link_libraries(l3svc PRIVATE
    Threads::Threads
    $<$<BOOL:${WIN32}>:ws2_32>
    $<$<BOOL:${WIN32}>:bcrypt>
    $<$<BOOL:${WIN32}>:iphlpapi>
    lite3-cpp
    conveyor
    libzstd_static
    xxhash
    l3kv_engine
    libzmq
    cppzmq
)




if(MSVC)
    add_definitions(-D_WIN32_WINNT=0x0A00 -DNOMINMAX -DBOOST_ASIO_HAS_IOCP)
    target_compile_options(l3svc PRIVATE /bigobj)
endif()

if(NOT MSVC)
    target_compile_options(l3svc PRIVATE -O3 -march=native)
    # add_definitions(-DBOOST_ASIO_HAS_IO_URING -DBOOST_ASIO_DISABLE_EPOLL)
endif()

add_executable(test_clock src/tests_cpp/test_clock.cpp src/engine/clock.cpp)
target_include_directories(test_clock PRIVATE src)
target_link_libraries(test_clock PRIVATE Threads::Threads)

add_executable(test_wal src/tests_cpp/test_wal.cpp)
target_include_directories(test_wal PRIVATE src)
target_link_libraries(test_wal PRIVATE Threads::Threads l3kv_engine)

add_executable(test_store src/tests_cpp/test_store.cpp src/engine/clock.cpp)
target_include_directories(test_store PRIVATE src)
target_link_libraries(test_store PRIVATE Threads::Threads l3kv_engine)
if(MSVC)
    add_definitions(-D_WIN32_WINNT=0x0A00 -DNOMINMAX)
endif()

add_executable(test_clock_simple src/tests_cpp/test_clock_simple.cpp src/engine/clock.cpp)
target_include_directories(test_clock_simple PRIVATE src)
target_link_libraries(test_clock_simple PRIVATE Threads::Threads)

add_executable(test_phase2 src/tests_cpp/test_phase2.cpp)
target_include_directories(test_phase2 PRIVATE
    src
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/boost_1_89_0"
)
target_link_libraries(test_phase2 PRIVATE
    Threads::Threads
    $<$<BOOL:${WIN32}>:ws2_32>
    lite3-cpp
    l3kv_engine
)

add_executable(test_merkle src/tests_cpp/test_merkle.cpp)
target_include_directories(test_merkle PRIVATE src ${CMAKE_CURRENT_SOURCE_DIR}/_deps/xxhash-src)
target_link_libraries(test_merkle PRIVATE Threads::Threads xxhash)

add_executable(test_mesh_stress src/tests_cpp/test_mesh_stress.cpp)
target_include_directories(test_mesh_stress PRIVATE
    src
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/boost_1_89_0"
)
target_link_libraries(test_mesh_stress PRIVATE
    Threads::Threads
    $<$<BOOL:${WIN32}>:ws2_32>
    lite3-cpp
    l3kv_engine
)

add_executable(test_sync_simulation src/tests_cpp/test_sync_simulation.cpp)
target_include_directories(test_sync_simulation PRIVATE
    src
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/boost_1_89_0"
)
target_link_libraries(test_sync_simulation PRIVATE
    Threads::Threads
    $<$<BOOL:${WIN32}>:ws2_32>
    conveyor
    lite3-cpp
    l3kv_engine
)

add_executable(test_sync_latency src/tests_cpp/test_sync_latency.cpp)
target_include_directories(test_sync_latency PRIVATE
    src
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/boost_1_89_0"
)
target_link_libraries(test_sync_latency PRIVATE
    Threads::Threads
    $<$<BOOL:${WIN32}>:ws2_32>
    conveyor
    lite3-cpp
    l3kv_engine
)

add_executable(bench_twitter src/tests_cpp/bench_twitter.cpp)
target_include_directories(bench_twitter PRIVATE
    src
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/liblite3client/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/liblite3client/external"
)
target_link_libraries(bench_twitter PRIVATE
    lite3client
)

add_executable(bench_ycsb src/tests_cpp/bench_ycsb.cpp)
target_include_directories(bench_ycsb PRIVATE
    src
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/liblite3client/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/liblite3client/external"
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/lite3-cpp/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/../lib/boost_1_89_0"
)
target_link_libraries(bench_ycsb PRIVATE
    lite3client
    lite3-cpp
)

add_executable(engine_bench src/tests_cpp/engine_bench.cpp src/engine/clock.cpp)
target_include_directories(engine_bench PRIVATE src)
target_link_libraries(engine_bench PRIVATE Threads::Threads l3kv_engine)

add_executable(bench_engine_threaded src/tests_cpp/bench_engine_threaded.cpp src/engine/clock.cpp)
target_include_directories(bench_engine_threaded PRIVATE src)
target_link_libraries(bench_engine_threaded PRIVATE Threads::Threads l3kv_engine)

if(MSVC)
    add_definitions(-D_WIN32_WINNT=0x0A00 -DNOMINMAX)
endif()

add_executable(hello src/tests_cpp/hello.cpp)