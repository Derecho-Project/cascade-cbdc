project(cascade_cbdc_core)

add_library(cbdc_udl SHARED cbdc_udl.hpp cbdc_udl.cpp)
target_include_directories(cbdc_udl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)

# config
add_custom_command(TARGET cbdc_udl POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/cfg
        ${CMAKE_BINARY_DIR}/cfg
    COMMAND cp ${CMAKE_SOURCE_DIR}/scripts/setup_config.sh ${CMAKE_BINARY_DIR}/
    COMMAND cp ${CMAKE_SOURCE_DIR}/scripts/metrics.py ${CMAKE_BINARY_DIR}/
    COMMAND ${CMAKE_BINARY_DIR}/setup_config.sh ${NUM_LOCAL_SHARDS} ${NUM_LOCAL_REPLICAS}
    DEPENDS ${CMAKE_SOURCE_DIR}/scripts/setup_config.sh
            ${CMAKE_SOURCE_DIR}/scripts/metrics.py
            ${CMAKE_SOURCE_DIR}/cfg/layout.json.tmp
            ${CMAKE_SOURCE_DIR}/cfg/derecho.cfg.tmp
            ${CMAKE_SOURCE_DIR}/cfg/dfgs.json
            ${CMAKE_SOURCE_DIR}/cfg/udl_dlls.cfg
)

