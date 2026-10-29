/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut0crc32.h
 *    CRC-32C (Castagnoli) checksum.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut0crc32_h
#define ut0crc32_h

#include <cstddef>
#include <cstdint>

/** Compute CRC-32C. Calls can be chained: ut_crc32c(b, nb, ut_crc32c(a, na,
0)) equals the CRC of a followed by b.
@param[in]  buf  data
@param[in]  len  length of data in bytes
@param[in]  crc  result of a previous call, or 0 to start
@return CRC-32C */
uint32_t ut_crc32c(const void *buf, size_t len, uint32_t crc);

#endif
