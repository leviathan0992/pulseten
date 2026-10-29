/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut0byte.cc
 *    Little-endian integer encoding.
 *
 *-------------------------------------------------------------------------
 */

#include "ut0byte.h"

/** Store a 16-bit value little-endian.
@param[out]  b  2 bytes
@param[in]   v  value */
void ut_write_u16(unsigned char *b, uint16_t v) {
  b[0] = (unsigned char)v;
  b[1] = (unsigned char)(v >> 8);
}

/** Store a 32-bit value little-endian.
@param[out]  b  4 bytes
@param[in]   v  value */
void ut_write_u32(unsigned char *b, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    b[i] = (unsigned char)(v >> (8 * i));
  }
}

/** Store a 64-bit value little-endian.
@param[out]  b  8 bytes
@param[in]   v  value */
void ut_write_u64(unsigned char *b, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    b[i] = (unsigned char)(v >> (8 * i));
  }
}

/** Load a little-endian 16-bit value.
@param[in]  b  2 bytes
@return the value */
uint16_t ut_read_u16(const unsigned char *b) {
  return ((uint16_t)(b[0] | (b[1] << 8)));
}

/** Load a little-endian 32-bit value.
@param[in]  b  4 bytes
@return the value */
uint32_t ut_read_u32(const unsigned char *b) {
  uint32_t v = 0;

  for (int i = 0; i < 4; ++i) {
    v |= (uint32_t)b[i] << (8 * i);
  }

  return (v);
}

/** Load a little-endian 64-bit value.
@param[in]  b  8 bytes
@return the value */
uint64_t ut_read_u64(const unsigned char *b) {
  uint64_t v = 0;

  for (int i = 0; i < 8; ++i) {
    v |= (uint64_t)b[i] << (8 * i);
  }

  return (v);
}
