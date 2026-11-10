/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_byte.cc
 *    Little-endian integer encoding.
 *
 *-------------------------------------------------------------------------
 */

#include "ut_byte.h"

#include <cstdint>

/** Store a 16-bit value little-endian. */
void ut_write_u16(unsigned char *b, uint16_t v) {
  b[0] = static_cast<unsigned char>(v);
  b[1] = static_cast<unsigned char>(v >> 8);
}

/** Store a 32-bit value little-endian. */
void ut_write_u32(unsigned char *b, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    b[i] = static_cast<unsigned char>(v >> (8 * i));
  }
}

/** Store a 64-bit value little-endian. */
void ut_write_u64(unsigned char *b, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    b[i] = static_cast<unsigned char>(v >> (8 * i));
  }
}

/** Load a little-endian 16-bit value. */
uint16_t ut_read_u16(const unsigned char *b) {
  return (static_cast<uint16_t>(b[0] | (b[1] << 8)));
}

/** Load a little-endian 32-bit value. */
uint32_t ut_read_u32(const unsigned char *b) {
  uint32_t v = 0;

  for (int i = 0; i < 4; ++i) {
    v |= static_cast<uint32_t>(b[i]) << (8 * i);
  }

  return (v);
}

/** Load a little-endian 64-bit value. */
uint64_t ut_read_u64(const unsigned char *b) {
  uint64_t v = 0;

  for (int i = 0; i < 8; ++i) {
    v |= static_cast<uint64_t>(b[i]) << (8 * i);
  }

  return (v);
}
