cmake_minimum_required(VERSION 3.18)
project(os_ex3)

set(CMAKE_CXX_STANDARD 11)

add_executable(
  os_ex3
  main.cpp
  MapReduceClient.h
        pdebug.cpp
        pdebug.hpp
  MapReduceFramework.h
  MapReduceFramework.cpp
  job.hpp
  job.cpp
  worker.hpp
  worker.cpp)

target_compile_options(os_ex3 PUBLIC -Wall -Wextra -Werror -pedantic)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(os_ex3 PRIVATE Threads::Threads)
