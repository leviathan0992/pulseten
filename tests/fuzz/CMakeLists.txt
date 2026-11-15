cmake_minimum_required(VERSION 3.24)
project(PulseTenWALFuzz LANGUAGES CXX)

# Standalone build: no DuckDB dependency, and every WAL translation unit is
# instrumented. Use Clang with a libFuzzer runtime, e.g.:
# cmake -S tests/fuzz -B /tmp/wal-fuzz -G Ninja -DCMAKE_CXX_COMPILER=clang++
# If Apple ld rejects a relocation, add
# -DCMAKE_EXE_LINKER_FLAGS=--ld-path=/path/to/ld64.lld when configuring.
# cmake --build /tmp/wal-fuzz
# /tmp/wal-fuzz/wal-fuzz-replay --self-test
# /tmp/wal-fuzz/wal-fuzz-replay --write-seeds /tmp/corpus
# /tmp/wal-fuzz/wal-fuzz /tmp/corpus -max_total_time=300 -max_len=65536
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(FATAL_ERROR "The WAL fuzzer requires Clang and libFuzzer.")
endif()
get_filename_component(PULSETEN_DEFAULT_ROOT "../.." ABSOLUTE)
set(PULSETEN_FUZZ_SOURCE_ROOT "${PULSETEN_DEFAULT_ROOT}" CACHE PATH
    "PulseTen source snapshot to test")
set(root "${PULSETEN_FUZZ_SOURCE_ROOT}")
find_package(Threads REQUIRED)
add_library(wal-fuzz-engine STATIC
  "${root}/src/wal/wal.cc" "${root}/src/wal/wal_recovery.cc"
  "${root}/src/os/os_file.cc"
  "${root}/src/ut/ut.cc" "${root}/src/ut/ut_debug.cc")
target_include_directories(wal-fuzz-engine PUBLIC
  "${root}/include" "${root}/src/wal" "${root}/src/os"
  "${root}/src/ut" "${root}/tests/support")
target_compile_definitions(wal-fuzz-engine PUBLIC PT_TESTING)
target_compile_options(wal-fuzz-engine PRIVATE
  -fsanitize=fuzzer-no-link,address,undefined
  "-fprofile-instr-generate=${CMAKE_CURRENT_BINARY_DIR}/wal-%p.profraw" -fcoverage-mapping)
target_link_libraries(wal-fuzz-engine PUBLIC Threads::Threads)

foreach(target wal-fuzz wal-fuzz-replay)
  add_executable(${target} wal-fuzz.cc)
  target_link_libraries(${target} PRIVATE wal-fuzz-engine)
  target_compile_options(${target} PRIVATE
    -Wall -Wextra -Werror -pedantic -fno-exceptions -g -O1
    -fno-omit-frame-pointer -fsanitize=address,undefined
    "-fprofile-instr-generate=${CMAKE_CURRENT_BINARY_DIR}/wal-%p.profraw" -fcoverage-mapping)
  target_link_options(${target} PRIVATE
    -fsanitize=address,undefined "-fprofile-instr-generate=${CMAKE_CURRENT_BINARY_DIR}/wal-%p.profraw")
endforeach()
target_compile_options(wal-fuzz-engine PRIVATE
  -Wall -Wextra -Werror -pedantic -fno-exceptions -g -O1
  -fno-omit-frame-pointer)
target_compile_options(wal-fuzz PRIVATE -fsanitize=fuzzer)
target_link_options(wal-fuzz PRIVATE -fsanitize=fuzzer)
# The replay executable uses identical WAL objects without the fuzzer runtime.
target_sources(wal-fuzz-replay PRIVATE wal-fuzz-replay.cc)
target_link_options(wal-fuzz-replay PRIVATE -fsanitize=fuzzer-no-link)

# No PulseTen code: a control for libFuzzer's process-exit leak reports.
add_executable(wal-fuzz-empty wal-fuzz-empty.cc)
target_compile_options(wal-fuzz-empty PRIVATE
  -g -O1 -fsanitize=fuzzer,address,undefined
  "-fprofile-instr-generate=${CMAKE_CURRENT_BINARY_DIR}/wal-%p.profraw" -fcoverage-mapping)
target_link_options(wal-fuzz-empty PRIVATE
  -fsanitize=fuzzer,address,undefined "-fprofile-instr-generate=${CMAKE_CURRENT_BINARY_DIR}/wal-%p.profraw")

add_executable(wal-replay-empty wal-fuzz-empty.cc)
target_compile_definitions(wal-replay-empty PRIVATE WAL_FUZZ_WITHOUT_RUNTIME)
target_compile_options(wal-replay-empty PRIVATE
  -g -O1 -fsanitize=address,undefined "-fprofile-instr-generate=${CMAKE_CURRENT_BINARY_DIR}/wal-%p.profraw" -fcoverage-mapping)
target_link_options(wal-replay-empty PRIVATE
  -fsanitize=address,undefined "-fprofile-instr-generate=${CMAKE_CURRENT_BINARY_DIR}/wal-%p.profraw")
