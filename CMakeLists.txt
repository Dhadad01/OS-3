cmake_minimum_required(VERSION 3.18)
project(os_ex3 VERSION 1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 11)

add_library(
  MapReduceFramework
  MapReduceClient.h
  pdebug.cpp
  pdebug.hpp
  MapReduceFramework.h
  MapReduceFramework.cpp
  job.hpp
  job.cpp
  worker.hpp
  worker.cpp)

set_property(TARGET MapReduceFramework PROPERTY CXX_STANDARD 11)
#target_compile_options(MapReduceFramework PUBLIC -Wall -Wextra -Werror -pedantic)
target_compile_options(MapReduceFramework PUBLIC -Wall -Wextra -pedantic)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_include_directories(MapReduceFramework PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(MapReduceFramework PUBLIC Threads::Threads)

add_executable(
	os_ex3
	test1.cpp
)
set_property(TARGET os_ex3 PROPERTY CXX_STANDARD 11)
# target_compile_options(os_ex3 PUBLIC -Wall -Wextra -Werror -pedantic)
target_compile_options(os_ex3 PUBLIC -Wall -Wextra -pedantic)
target_include_directories(os_ex3 PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(os_ex3 PUBLIC MapReduceFramework Threads::Threads)


# NOTE: You can't have both ThreadSanitizer and AddressSanitizer enabled at the same time.

# Uncomment the following to enable ThreadSanitizer.
#set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")
#set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")

# Uncomment the following to enable AddressSanitizer.
#set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
#set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")

# Add tests
add_subdirectory(basic_test)
