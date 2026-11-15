/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal-fuzz-replay.cc
 *    Replays fuzz inputs, self-tests and writes seeds without libFuzzer.
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <vector>

#include <sys/stat.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *, size_t);
extern "C" void wal_fuzz_boundary_tests();

int main(int argc, char **argv) {
  if (argc == 3 && std::strcmp(argv[1], "--write-seeds") == 0) {
    if (mkdir(argv[2], 0700) != 0 && errno != EEXIST)

      return 2;

    for (unsigned mode = 1; mode <= 3; ++mode) {
      const unsigned count = mode == 1 ? 16 : mode == 2 ? 8 : 2;
      for (unsigned scenario = 0; scenario < count; ++scenario) {
        const uint8_t seed[] = {static_cast<uint8_t>(mode),
                                static_cast<uint8_t>(scenario),
                                4,
                                7,
                                1,
                                100,
                                1};

        std::ofstream out(std::string(argv[2]) + "/seed-" +
                              std::to_string(mode) + "-" +
                              std::to_string(scenario),
                          std::ios::binary);
        out.write(reinterpret_cast<const char *>(seed), sizeof(seed));
        if (!out)

          return 2;
      }
    }

    std::vector<unsigned char> raw(1 + 40 + 28 + 4);
    wal_encode_file_header(raw.data() + 1, 123, 1);
    wal_encode_record(raw.data() + 41, 1, 0,
                      reinterpret_cast<const unsigned char *>("seed"), 4);

    std::ofstream out(std::string(argv[2]) + "/raw-valid", std::ios::binary);
    out.write(reinterpret_cast<const char *>(raw.data()), raw.size());

    return out ? 0 : 2;
  }

  if (argc == 2 && std::strcmp(argv[1], "--self-test") == 0) {
    for (uint8_t scenario = 0; scenario < 16; ++scenario) {
      const uint8_t seed[] = {1, scenario, 4, 7, 1, 100, 1};
      LLVMFuzzerTestOneInput(seed, sizeof(seed));
    }

    for (uint8_t scenario = 0; scenario < 8; ++scenario) {
      const uint8_t seed[] = {2, scenario, 7, 32, 99};
      LLVMFuzzerTestOneInput(seed, sizeof(seed));
    }

    for (uint8_t partial = 0; partial < 2; ++partial) {
      const uint8_t seed[] = {3, partial, 64};
      LLVMFuzzerTestOneInput(seed, sizeof(seed));
    }

    wal_fuzz_boundary_tests();
    std::puts(
        "WAL oracle: 26 structured scenarios, every-byte cuts and size/I/O "
        "boundaries passed");

    return 0;
  }

  for (int i = 1; i < argc; ++i) {
    std::ifstream in(argv[i], std::ios::binary);
    if (!in)

      return 2;
    const std::vector<uint8_t> data((std::istreambuf_iterator<char>(in)), {});
    LLVMFuzzerTestOneInput(data.data(), data.size());
  }

  return 0;
}
