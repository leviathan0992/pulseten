# Programs outside the source tree using an installed PulseTen, as a user
# would. CI builds them against the install prefix and runs them:
#
# cmake -S tests/install -B /path/to/consumer -DCMAKE_PREFIX_PATH=<prefix>
# cmake --build /path/to/consumer && /path/to/consumer/consumer <dir>
#
# By default only C is enabled, which checks that a C project can link the
# package. With -DPULSETEN_CONSUMER_CXX=ON the C++ example is built too.
cmake_minimum_required(VERSION 3.24)
project(PulseTenConsumer LANGUAGES C)

option(PULSETEN_CONSUMER_CXX "Also build the C++ example" OFF)
if(PULSETEN_CONSUMER_CXX)
  enable_language(CXX)
endif()

find_package(PulseTen 0.1 REQUIRED)

add_executable(consumer consumer.c)
set_target_properties(consumer PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON)
target_link_libraries(consumer PRIVATE PulseTen::pulseten)

if(PULSETEN_CONSUMER_CXX)
  add_executable(quickstart ../../examples/quickstart.cc)
  set_target_properties(quickstart PROPERTIES CXX_STANDARD 17
                                              CXX_STANDARD_REQUIRED ON)
  target_link_libraries(quickstart PRIVATE PulseTen::pulseten)
endif()
