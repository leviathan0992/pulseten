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

# Assertions/lock diagnostics and white-box testing are independent modes.
option(PULSETEN_DEBUG "Enable PulseTen debug checks in any build type" OFF)
include(CTest)

# The two PulseTen variants share the same pinned DuckDB dependency.
set(PULSETEN_SOURCES)
set(PULSETEN_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/include"
                          "${PULSETEN_DUCKDB_DIR}/src/include")
function(pulseten_module name)
  list(TRANSFORM ARGN PREPEND "src/${name}/" OUTPUT_VARIABLE sources)
  set(PULSETEN_SOURCES ${PULSETEN_SOURCES} ${sources} PARENT_SCOPE)
  set(PULSETEN_INCLUDE_DIRS ${PULSETEN_INCLUDE_DIRS}
      "${CMAKE_CURRENT_SOURCE_DIR}/src/${name}" PARENT_SCOPE)
endfunction()

pulseten_module(ut ut.cc ut_err.cc ut_crc32.cc ut_byte.cc ut_time.cc)
pulseten_module(os os_file.cc os_memory.cc)
pulseten_module(wal wal_format.cc wal_recv.cc wal.cc)
pulseten_module(schema schema.cc)
pulseten_module(batch batch.cc)
pulseten_module(duckdb duckdb_database.cc)
pulseten_module(memtable memtable.cc)
pulseten_module(ingest ingest.cc ingest_codec.cc)
pulseten_module(db db.cc db_write.cc db_flush.cc db_query.cc db_table.cc)
pulseten_module(storage storage.cc storage_flush.cc)
pulseten_module(query query.cc query_memtable.cc)
pulseten_module(api api.cc)

if(PULSETEN_SANITIZE)
  set(PULSETEN_SANITIZE_FLAGS -fsanitize=${PULSETEN_SANITIZE} -g)
endif()

# Compile every project translation unit with its variant's definitions.
# The DuckDB C++ adapter needs exceptions for its upstream headers.
function(pulseten_library target)
  add_library(${target} STATIC ${PULSETEN_SOURCES})
  target_include_directories(${target} PUBLIC ${PULSETEN_INCLUDE_DIRS})
  target_compile_options(${target} PRIVATE ${PULSETEN_CXX_FLAGS})
  target_compile_definitions(${target} PUBLIC
    $<$<OR:$<CONFIG:Debug>,$<BOOL:${PULSETEN_DEBUG}>>:PT_DEBUG>)
  if(PULSETEN_SANITIZE)
    target_compile_options(${target} PUBLIC ${PULSETEN_SANITIZE_FLAGS})
    target_link_options(${target} PUBLIC ${PULSETEN_SANITIZE_FLAGS})
  endif()
  # The function extension goes before the core archive it depends on.
  target_link_libraries(${target} PUBLIC Threads::Threads
                        core_functions_extension duckdb_static)

  add_library(${target}_duckdb_adapter OBJECT src/query/query_duckdb.cc)
  target_include_directories(${target}_duckdb_adapter PRIVATE
    ${PULSETEN_INCLUDE_DIRS})
  target_include_directories(${target}_duckdb_adapter SYSTEM PRIVATE
    "${PULSETEN_DUCKDB_DIR}/src/include")
  target_compile_definitions(${target}_duckdb_adapter PRIVATE
    $<TARGET_PROPERTY:${target},INTERFACE_COMPILE_DEFINITIONS>)
  target_compile_options(${target}_duckdb_adapter PRIVATE
    ${PULSETEN_WARNINGS} ${PULSETEN_SANITIZE_FLAGS})
  target_link_libraries(${target}_duckdb_adapter PRIVATE duckdb_static)
  target_sources(${target} PRIVATE
    $<TARGET_OBJECTS:${target}_duckdb_adapter>)
endfunction()

# This is the only PulseTen library bundled and installed.
pulseten_library(pulseten)

if(BUILD_TESTING)
  pulseten_library(pulseten_testlib)
  target_compile_definitions(pulseten_testlib PUBLIC PT_TESTING)

  add_library(pulseten_test_support STATIC
    tests/test_db_access.cc tests/test_storage_access.cc tests/test_wal_access.cc)
  target_include_directories(pulseten_test_support PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_compile_options(pulseten_test_support PRIVATE ${PULSETEN_CXX_FLAGS})
  target_link_libraries(pulseten_test_support PUBLIC pulseten_testlib)

  # Functional tests use the normal library unless white-box access is named.
  function(pulseten_test module name)
    add_executable(${name} "tests/${module}/${name}.cc")
    if(ARGC GREATER 2)
      target_link_libraries(${name} PRIVATE ${ARGV2})
    else()
      target_link_libraries(${name} PRIVATE pulseten)
    endif()
    target_include_directories(${name} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    target_compile_options(${name} PRIVATE ${PULSETEN_CXX_FLAGS})
    add_test(NAME ${name} COMMAND ${name})
  endfunction()

  pulseten_test(ut ut-t)
  pulseten_test(ut ut_new-t)
  pulseten_test(os os_file-t)
  pulseten_test(os os_memory-t)
  pulseten_test(wal wal-t pulseten_test_support)
  pulseten_test(wal wal_recv-t)
  pulseten_test(wal wal_seg-t)
  pulseten_test(ingest ingest-t)
  pulseten_test(ingest ingest_codec-t)
  pulseten_test(memtable memtable-t)
  pulseten_test(db db-t pulseten_test_support)
  pulseten_test(db db_table-t pulseten_test_support)
  pulseten_test(db db_flush-t pulseten_test_support)
  pulseten_test(db db_flusher-t pulseten_test_support)
  pulseten_test(db db_ttl-t pulseten_test_support)
  pulseten_test(db db_query-t)
  pulseten_test(storage storage-t pulseten_test_support)
  pulseten_test(query query-t)
  # White-box adapter access links the same test library.
  add_library(pulseten_query_test_support STATIC tests/test_query_access.cc)
  target_include_directories(pulseten_query_test_support PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_include_directories(pulseten_query_test_support SYSTEM PUBLIC
    "${PULSETEN_DUCKDB_DIR}/src/include")
  target_compile_options(pulseten_query_test_support PRIVATE ${PULSETEN_WARNINGS})
  target_link_libraries(pulseten_query_test_support PUBLIC pulseten_testlib)
  pulseten_test(query query_reuse-t pulseten_query_test_support)
  target_compile_options(query_reuse-t PRIVATE -fexceptions)
  pulseten_test(query query_bench-t)
  pulseten_test(query query_layout_bench-t)

  # C API tests validate the normal library through a C-only header.
  add_executable(api-t tests/api/api-t.c)
  set_target_properties(api-t PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON
                                             C_EXTENSIONS OFF LINKER_LANGUAGE CXX)
  target_link_libraries(api-t PRIVATE pulseten)
  target_compile_options(api-t PRIVATE ${PULSETEN_WARNINGS})
  add_test(NAME api-t COMMAND api-t)

  # Benchmarks measure the normal library, without white-box instrumentation.
  function(pulseten_bench module name)
    add_executable(${name} "tests/${module}/${name}.cc")
    target_link_libraries(${name} PRIVATE pulseten)
    target_include_directories(${name} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    target_compile_options(${name} PRIVATE ${PULSETEN_CXX_FLAGS})
  endfunction()

  pulseten_bench(wal wal-bench)
  pulseten_bench(db db-bench)
  pulseten_bench(query query-bench)
  pulseten_bench(query query_layout-bench)

  add_test(NAME testing-isolation-t
    COMMAND "${CMAKE_COMMAND}"
      "-DPULSETEN_NORMAL_LIBRARY=$<TARGET_FILE:pulseten>"
      "-DPULSETEN_TEST_SUPPORT=$<TARGET_FILE:pulseten_test_support>"
      "-DPULSETEN_NM=${CMAKE_NM}"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/install/testing_isolation.cmake")
endif()

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
if(BUILD_TESTING AND PULSETEN_PKG_CONFIG_EXECUTABLE)
  add_test(NAME install0pkgconfig-t
    COMMAND "${CMAKE_COMMAND}"
      "-DPULSETEN_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
      "-DPULSETEN_TEST_DIR=${CMAKE_CURRENT_BINARY_DIR}/pkgconfig-test"
      "-DPULSETEN_PKG_CONFIG_EXECUTABLE=${PULSETEN_PKG_CONFIG_EXECUTABLE}"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/install/pkgconfig.cmake")
endif()

# DuckDB is not instrumented; see tests/tsan.supp.
if(BUILD_TESTING AND PULSETEN_SANITIZE MATCHES "thread")
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
  COMPATIBILITY ExactVersion)

install(FILES include/pulseten.h DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(FILES "${PULSETEN_BUNDLE}" DESTINATION "${CMAKE_INSTALL_LIBDIR}")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/pulseten.pc"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/PulseTenConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/PulseTenConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/PulseTen")
