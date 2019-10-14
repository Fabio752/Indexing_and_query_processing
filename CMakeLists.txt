Project(CO572CW1)
cmake_minimum_required(VERSION 3.7)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

if (MSVC)
    add_compile_options(/W4 /WX)
  else()
    if(${CMAKE_SYSTEM_PROCESSOR} STREQUAL armv7l)
      # setting architecture explicitly on Pi because raspbian doesn't actually use correct ISA
      add_compile_options(-Wall -Wextra -pedantic -Werror -Wno-psabi -march=armv8-a+crc)
    else()
      add_compile_options(-Wall -Wextra -pedantic -Werror -march=native)
    endif()
endif()

# --------------------------------------------------

include(ExternalProject)

ExternalProject_Add(googlebenchmark
	URL "https://github.com/google/benchmark/archive/v1.5.0.tar.gz"
	CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${CO572CW1_BINARY_DIR}/deps -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON -DBENCHMARK_ENABLE_GTEST_TESTS=OFF -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  )

ExternalProject_Add(catch2
	URL "https://github.com/catchorg/Catch2/archive/v2.9.1.tar.gz"
	CMAKE_ARGS -DCATCH_BUILD_TESTING=OFF -DCMAKE_INSTALL_PREFIX=${CO572CW1_BINARY_DIR}/deps -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} 
  )

# --------------------------------------------------

add_executable(microbenchmarks microbenchmarks.cpp solution.c)
add_dependencies(microbenchmarks googlebenchmark)

set_property(TARGET microbenchmarks PROPERTY CXX_STANDARD 14)

target_link_libraries(microbenchmarks Threads::Threads)
target_link_libraries(microbenchmarks ${CO572CW1_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark.a)
target_include_directories(microbenchmarks SYSTEM PUBLIC ${CO572CW1_BINARY_DIR}/deps/include)


# --------------------------------------------------

add_executable(macrobenchmark macrobenchmark.cpp solution.c)
add_dependencies(macrobenchmark googlebenchmark)

set_property(TARGET macrobenchmark PROPERTY CXX_STANDARD 14)

target_link_libraries(macrobenchmark Threads::Threads)
target_link_libraries(macrobenchmark ${CO572CW1_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark.a)
target_include_directories(macrobenchmark SYSTEM PUBLIC ${CO572CW1_BINARY_DIR}/deps/include)

# --------------------------------------------------
add_executable(tests tests.cpp solution.c)
add_dependencies(tests catch2)
set_property(TARGET tests PROPERTY CXX_STANDARD 14)
target_include_directories(tests SYSTEM PUBLIC ${CO572CW1_BINARY_DIR}/deps/include)
