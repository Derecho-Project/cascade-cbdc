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

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

add_subdirectory(src)

