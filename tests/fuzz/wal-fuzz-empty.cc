/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal-fuzz-empty.cc
 *    Empty fuzz target: a baseline for the fuzzer and replay runtimes.
 *
 *-------------------------------------------------------------------------
 */

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *, size_t) {
  return 0;
}

#ifdef WAL_FUZZ_WITHOUT_RUNTIME
int main() {
  return 0;
}
#endif
