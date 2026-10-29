/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut0crc32.cc
 *    Table-driven CRC-32C.
 *
 * Hardware acceleration can replace this later without changing the
 * interface.
 *
 *-------------------------------------------------------------------------
 */

#include "ut0crc32.h"

/** Reflected CRC-32C polynomial. */
static const uint32_t CRC32C_POLY = 0x82F63B78;

/** Lookup table, initialized when CRC-32C is first used. */
struct crc32c_table_t {
  /** CRC-32C of each byte value. */
  uint32_t value[256]{};

  /** Build the lookup table for the reflected CRC-32C polynomial. */
  crc32c_table_t() {
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int k = 0; k < 8; ++k) {
        if (c & 1) {
          c = (c >> 1) ^ CRC32C_POLY;
        } else {
          c = c >> 1;
        }
      }
      value[i] = c;
    }
  }
};

/** Compute CRC-32C.
@param[in]  buf  data
@param[in]  len  length of data in bytes
@param[in]  crc  result of a previous call, or 0 to start
@return CRC-32C */
uint32_t ut_crc32c(const void *buf, size_t len, uint32_t crc) {
  /* Safe even when a different translation unit calls us before main().
  The local static is initialized once, including concurrent first use. */
  static const crc32c_table_t crc32c_table;

  const unsigned char *p = static_cast<const unsigned char *>(buf);
  uint32_t c = ~crc;
  for (size_t i = 0; i < len; ++i) {
    c = crc32c_table.value[(c ^ p[i]) & 0xFF] ^ (c >> 8);
  }

  return (~c);
}
