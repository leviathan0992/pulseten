cmake_minimum_required(VERSION 3.24)

# The version is the one in the public header.
file(STRINGS include/pulseten.h PULSETEN_VERSION_LINE
     REGEX "^#define PULSETEN_VERSION_STRING ")
string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" PULSETEN_VERSION
       "${PULSETEN_VERSION_LINE}")
project(PulseTen VERSION ${PULSETEN_VERSION} LANGUAGES C CXX)

# Build outside the source tree:
# cmake -S . -B /path/to/build -G Ninja -DCMAKE_BUILD_TYPE=Debug
# cmake --build /path/to/build && ctest --test-dir /path/to/build
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
  message(FATAL_ERROR "Use an out-of-source build directory with cmake -B.")
endif()

set(CMAKE_CXX_STANDARD 20 CACHE STRING "C++ standard for PulseTen" FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

# DuckDB is pinned to an exact release commit.
set(PULSETEN_DUCKDB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/duckdb")
set(PULSETEN_DUCKDB_COMMIT "d54cbbe3df17e8e5ce68312f483b4155836112aa")
if(NOT EXISTS "${PULSETEN_DUCKDB_DIR}/CMakeLists.txt")
  message(FATAL_ERROR "Initialize DuckDB with git submodule update --init third_party/duckdb.")
endif()
find_package(Git REQUIRED)
execute_process(
  COMMAND "${GIT_EXECUTABLE}" rev-parse HEAD
  WORKING_DIRECTORY "${PULSETEN_DUCKDB_DIR}"
  OUTPUT_VARIABLE PULSETEN_DUCKDB_ACTUAL_COMMIT
  OUTPUT_STRIP_TRAILING_WHITESPACE
  RESULT_VARIABLE PULSETEN_DUCKDB_GIT_RESULT)
if(NOT PULSETEN_DUCKDB_GIT_RESULT EQUAL 0 OR
   NOT PULSETEN_DUCKDB_ACTUAL_COMMIT STREQUAL PULSETEN_DUCKDB_COMMIT)
  message(FATAL_ERROR "DuckDB must be checked out at ${PULSETEN_DUCKDB_COMMIT}.")
endif()
# Only the core library and its built-in functions are built: no shell,
# tests, loadable extensions or HTTP client.
set(DUCKDB_EXPLICIT_VERSION "v2.0.0" CACHE STRING "Pinned DuckDB release" FORCE)
set(GIT_COMMIT_HASH "${PULSETEN_DUCKDB_COMMIT}")
set(BUILD_SHELL OFF CACHE BOOL "" FORCE)
set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
set(BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(ENABLE_EXTENSION_LOAD OFF CACHE BOOL "" FORCE)
set(ENABLE_BUILTIN_HTTPLIB OFF CACHE BOOL "" FORCE)
set(STATICALLY_LINK_EXTENSIONS "core_functions" CACHE STRING "" FORCE)
set(DUCKDB_CAPABILITIES "none" CACHE STRING "" FORCE)
set(PULSETEN_SANITIZE "" CACHE STRING "Sanitizers for PulseTen code only")

# DuckDB enables ASan/UBSan by default in Debug, but its static libraries do
# not export those link flags to our executables or installed consumers.
# Instrument PulseTen through PULSETEN_SANITIZE instead.
set(ENABLE_SANITIZER OFF CACHE BOOL "" FORCE)
set(ENABLE_UBSAN OFF CACHE BOOL "" FORCE)
set(ENABLE_THREAD_SANITIZER OFF CACHE BOOL "" FORCE)
set(RELEASE_SANITIZER OFF CACHE BOOL "" FORCE)

# DuckDB uses its own jemalloc on Linux. Its fork handler takes hundreds of
# arena locks, more than ThreadSanitizer's deadlock detector can track, and
# TSan then hangs in the tests that fork. TSan builds use the system malloc.
if(PULSETEN_SANITIZE MATCHES "thread")
  set(ENABLE_JEMALLOC OFF CACHE BOOL "" FORCE)
else()
  set(ENABLE_JEMALLOC ON CACHE BOOL "" FORCE)
endif()
add_subdirectory(third_party/duckdb EXCLUDE_FROM_ALL)

set(PULSETEN_WARNINGS -Wall -Wextra -Werror -pedantic)
# PulseTen reports errors with dberr_t only: no try, catch or throw. A
# failed allocation inside a standard container ends the process.
set(PULSETEN_CXX_FLAGS ${PULSETEN_WARNINGS} -fno-exceptions)
find_package(Threads REQUIRED)

add_library(pulseten STATIC)
target_compile_options(pulseten PRIVATE ${PULSETEN_CXX_FLAGS})
# PT_DEBUG turns on ut_ad() checks and test hooks. It is on in Debug builds;
# PULSETEN_DEBUG turns it on in other builds without rebuilding DuckDB.
option(PULSETEN_DEBUG "Enable PulseTen debug checks in any build type" OFF)
target_compile_definitions(pulseten PUBLIC
  $<$<OR:$<CONFIG:Debug>,$<BOOL:${PULSETEN_DEBUG}>>:PT_DEBUG>)

# PULSETEN_SANITIZE instruments PulseTen and its tests, not DuckDB, e.g.
# -DPULSETEN_SANITIZE=thread or -DPULSETEN_SANITIZE=address,undefined.
# With address, run with ASAN_OPTIONS=detect_container_overflow=0: inline
# standard-library code compiled both inside and outside DuckDB reports
# false container overflows in a partly instrumented program.
if(PULSETEN_SANITIZE)
  target_compile_options(pulseten PUBLIC -fsanitize=${PULSETEN_SANITIZE} -g)
  set(PULSETEN_SANITIZE_FLAGS -fsanitize=${PULSETEN_SANITIZE} -g)
  target_link_options(pulseten PUBLIC -fsanitize=${PULSETEN_SANITIZE})
endif()
target_link_libraries(pulseten PUBLIC Threads::Threads)

# Register a module: its directory under src/ and its source files.
function(pulseten_module name)
  list(TRANSFORM ARGN PREPEND "src/${name}/" OUTPUT_VARIABLE sources)
  if(sources)
    target_sources(pulseten PRIVATE ${sources})
  endif()
  target_include_directories(pulseten PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/src/${name}")
endfunction()

pulseten_module(ut ut.cc ut_err.cc ut_crc32.cc ut_byte.cc ut_time.cc)
pulseten_module(os os_file.cc)
pulseten_module(wal wal_format.cc wal_recv.cc wal.cc)
pulseten_module(schema schema.cc)
pulseten_module(batch batch.cc)
pulseten_module(duckdb duckdb_database.cc)
pulseten_module(memtable memtable.cc)
pulseten_module(ingest ingest.cc ingest_codec.cc)
pulseten_module(engine engine.cc engine_table.cc)
pulseten_module(storage storage.cc storage_flush.cc)
# The function extension goes before the core archive it depends on.
target_link_libraries(pulseten PUBLIC core_functions_extension duckdb_static)
pulseten_module(query query.cc query_memtable.cc)
pulseten_module(api api.cc)
target_include_directories(pulseten PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")

# The C++ adapter to DuckDB compiles against DuckDB's C++ headers, which do
# not build cleanly under PulseTen's warning flags and need exceptions. It
# still has no try or catch: it only parses SQL the C API accepted.
add_library(pulseten_duckdb_adapter OBJECT src/query/query_duckdb.cc)
target_include_directories(pulseten_duckdb_adapter PRIVATE
  $<TARGET_PROPERTY:pulseten,INTERFACE_INCLUDE_DIRECTORIES>)
target_link_libraries(pulseten_duckdb_adapter PRIVATE duckdb_static)
target_compile_options(pulseten_duckdb_adapter PRIVATE ${PULSETEN_SANITIZE_FLAGS})
target_sources(pulseten PRIVATE $<TARGET_OBJECTS:pulseten_duckdb_adapter>)
target_include_directories(pulseten PUBLIC "${PULSETEN_DUCKDB_DIR}/src/include")

# Register a test: tests/<module>/<name>.cc becomes CTest test <name>.
enable_testing()
function(pulseten_test module name)
  add_executable(${name} "tests/${module}/${name}.cc")
  target_link_libraries(${name} PRIVATE pulseten)
  target_include_directories(${name} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_compile_options(${name} PRIVATE ${PULSETEN_CXX_FLAGS})
  add_test(NAME ${name} COMMAND ${name})
endfunction()

pulseten_test(ut ut-t)
pulseten_test(ut ut_new-t)
pulseten_test(os os_file-t)
pulseten_test(wal wal-t)
pulseten_test(wal wal_recv-t)
pulseten_test(wal wal_seg-t)
pulseten_test(ingest ingest-t)
pulseten_test(ingest ingest_codec-t)
pulseten_test(memtable memtable-t)
pulseten_test(engine engine-t)
pulseten_test(engine engine_table-t)
pulseten_test(engine engine_flush-t)
pulseten_test(engine engine_flusher-t)
pulseten_test(engine engine_ttl-t)
pulseten_test(engine engine_drop-t)
pulseten_test(storage storage-t)
pulseten_test(query query-t)
pulseten_test(query query_bench-t)
pulseten_test(query query_layout_bench-t)

# The C API test is C, so that pulseten.h is compiled as a C header.
add_executable(api-t tests/api/api-t.c)
set_target_properties(api-t PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON
                                           C_EXTENSIONS OFF LINKER_LANGUAGE CXX)
target_link_libraries(api-t PRIVATE pulseten)
target_compile_options(api-t PRIVATE ${PULSETEN_WARNINGS})
add_test(NAME api-t COMMAND api-t)

# Register a benchmark: built with the project, run by hand, not by CTest.
function(pulseten_bench module name)
  add_executable(${name} "tests/${module}/${name}.cc")
  target_link_libraries(${name} PRIVATE pulseten)
  target_include_directories(${name} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_compile_options(${name} PRIVATE ${PULSETEN_CXX_FLAGS})
endfunction()

pulseten_bench(wal wal-bench)
pulseten_bench(engine engine-bench)
pulseten_bench(query query-bench)
pulseten_bench(query query_layout-bench)

# Examples of the C API, built with the project.
add_executable(quickstart examples/quickstart.cc)
target_link_libraries(quickstart PRIVATE pulseten)
target_compile_options(quickstart PRIVATE ${PULSETEN_CXX_FLAGS})

# ---- Installation ------------------------------------------------------
#
# cmake --install installs pulseten.h and one static library holding
# PulseTen and the DuckDB it needs, with a CMake package (PulseTen::pulseten)
# and a pkg-config file. A program links it with the C++ linker, or adds the
# C++ runtime itself, and the thread library.

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(PULSETEN_BUNDLE "${CMAKE_CURRENT_BINARY_DIR}/bundle/${CMAKE_STATIC_LIBRARY_PREFIX}pulseten${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(PULSETEN_BUNDLE_INPUTS
  $<TARGET_FILE:pulseten>
  $<TARGET_FILE:core_functions_extension>
  $<TARGET_FILE:duckdb_static>)

if(APPLE)
  add_custom_command(OUTPUT "${PULSETEN_BUNDLE}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_CURRENT_BINARY_DIR}/bundle"
    COMMAND libtool -static -no_warning_for_no_symbols -o "${PULSETEN_BUNDLE}"
            ${PULSETEN_BUNDLE_INPUTS}
    DEPENDS pulseten core_functions_extension duckdb_static
    COMMENT "Bundling PulseTen and DuckDB into one static library"
    VERBATIM)
else()
  # GNU ar merges archives with an MRI script.
  set(PULSETEN_BUNDLE_MRI "${CMAKE_CURRENT_BINARY_DIR}/bundle/pulseten.mri")
  file(GENERATE OUTPUT "${PULSETEN_BUNDLE_MRI}" CONTENT
"CREATE ${PULSETEN_BUNDLE}
ADDLIB $<TARGET_FILE:pulseten>
ADDLIB $<TARGET_FILE:core_functions_extension>
ADDLIB $<TARGET_FILE:duckdb_static>
SAVE
END
")
  add_custom_command(OUTPUT "${PULSETEN_BUNDLE}"
    COMMAND "${CMAKE_COMMAND}" -E rm -f "${PULSETEN_BUNDLE}"
    COMMAND "${CMAKE_AR}" -M < "${PULSETEN_BUNDLE_MRI}"
    COMMAND "${CMAKE_RANLIB}" "${PULSETEN_BUNDLE}"
    DEPENDS pulseten core_functions_extension duckdb_static "${PULSETEN_BUNDLE_MRI}"
    COMMENT "Bundling PulseTen and DuckDB into one static library"
    VERBATIM)
endif()
add_custom_target(pulseten_bundle ALL DEPENDS "${PULSETEN_BUNDLE}")

# What a program links besides the library.
if(APPLE)
  set(PULSETEN_PC_LIBS "-lc++")
else()
  set(PULSETEN_PC_LIBS "-lstdc++ -lm")
  foreach(lib IN LISTS CMAKE_DL_LIBS)
    string(APPEND PULSETEN_PC_LIBS " -l${lib}")
  endforeach()
endif()
string(APPEND PULSETEN_PC_LIBS " ${CMAKE_THREAD_LIBS_INIT}")
include(cmake/PulseTenPkgConfig.cmake)
pulseten_configure_pkg_config("${CMAKE_CURRENT_BINARY_DIR}/pulseten.pc")

# Check pkg-config paths without rebuilding DuckDB for each install layout.
find_program(PULSETEN_PKG_CONFIG_EXECUTABLE NAMES pkg-config)
if(PULSETEN_PKG_CONFIG_EXECUTABLE)
  add_test(NAME install0pkgconfig-t
    COMMAND "${CMAKE_COMMAND}"
      "-DPULSETEN_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
      "-DPULSETEN_TEST_DIR=${CMAKE_CURRENT_BINARY_DIR}/pkgconfig-test"
      "-DPULSETEN_PKG_CONFIG_EXECUTABLE=${PULSETEN_PKG_CONFIG_EXECUTABLE}"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/install/pkgconfig.cmake")
endif()

# DuckDB is not instrumented; see tests/tsan.supp.
if(PULSETEN_SANITIZE MATCHES "thread")
  get_property(PULSETEN_TESTS DIRECTORY PROPERTY TESTS)
  set_tests_properties(${PULSETEN_TESTS} PROPERTIES ENVIRONMENT
    "TSAN_OPTIONS=halt_on_error=1:suppressions=${CMAKE_CURRENT_SOURCE_DIR}/tests/tsan.supp")
endif()

configure_package_config_file(cmake/PulseTenConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/PulseTenConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/PulseTen"
  PATH_VARS CMAKE_INSTALL_LIBDIR CMAKE_INSTALL_INCLUDEDIR)
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/PulseTenConfigVersion.cmake"
  COMPATIBILITY SameMinorVersion)

install(FILES include/pulseten.h DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(FILES "${PULSETEN_BUNDLE}" DESTINATION "${CMAKE_INSTALL_LIBDIR}")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/pulseten.pc"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/PulseTenConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/PulseTenConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/PulseTen")
