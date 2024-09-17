project(cascade_cbdc_benchmark)

add_executable(generate_workload generate_workload.cpp benchmark_workload.cpp)
target_link_libraries(generate_workload gzstream z)

add_executable(run_benchmark run_benchmark.cpp cbdc_client.cpp benchmark_workload.cpp)
target_link_libraries(run_benchmark derecho::cascade gzstream z)

