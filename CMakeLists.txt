cmake_minimum_required(VERSION 3.12.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade_cbdc CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(nlohmann_json 3.2.0 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(spdlog 1.3.1 REQUIRED)
find_package(derecho CONFIG REQUIRED)
find_package(cascade CONFIG REQUIRED)

if(NOT DEFINED NUM_LOCAL_SHARDS)
    set(NUM_LOCAL_SHARDS 1)
endif(NOT DEFINED NUM_LOCAL_SHARDS)

if(NOT DEFINED NUM_LOCAL_REPLICAS)
    set(NUM_LOCAL_REPLICAS 1)
endif(NOT DEFINED NUM_LOCAL_REPLICAS)

# CBDC UDL
add_library(cbdc_udl SHARED cbdc_udl.hpp cbdc_udl.cpp)
target_include_directories(cbdc_udl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)

add_executable(generate_workload generate_workload.cpp benchmark_workload.cpp)
target_link_libraries(generate_workload gzstream z)

add_executable(run_benchmark run_benchmark.cpp cbdc_client.cpp benchmark_workload.cpp)
target_link_libraries(run_benchmark derecho::cascade gzstream z)

# config
add_custom_command(TARGET cbdc_udl POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg
    COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/setup_config.sh ${CMAKE_CURRENT_BINARY_DIR}/
    COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/metrics.py ${CMAKE_CURRENT_BINARY_DIR}/
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/setup_config.sh ${NUM_LOCAL_SHARDS} ${NUM_LOCAL_REPLICAS}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/setup_config.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/metrics.py
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/layout.json.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/derecho.cfg
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/dfgs.json.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/udl_dlls.cfg.tmp
)

