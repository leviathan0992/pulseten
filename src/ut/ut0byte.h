/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut0byte.h
 *    Little-endian integer encoding used by every on-disk format.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut0byte_h
#define ut0byte_h

#include <cstdint>

/** Store a 16-bit value little-endian.
@param[out]  b  2 bytes
@param[in]   v  value */
void ut_write_u16(unsigned char *b, uint16_t v);

/** Store a 32-bit value little-endian.
@param[out]  b  4 bytes
@param[in]   v  value */
void ut_write_u32(unsigned char *b, uint32_t v);

/** Store a 64-bit value little-endian.
@param[out]  b  8 bytes
@param[in]   v  value */
void ut_write_u64(unsigned char *b, uint64_t v);

/** Load a little-endian 16-bit value.
@param[in]  b  2 bytes
@return the value */
uint16_t ut_read_u16(const unsigned char *b);

/** Load a little-endian 32-bit value.
@param[in]  b  4 bytes
@return the value */
uint32_t ut_read_u32(const unsigned char *b);

/** Load a little-endian 64-bit value.
@param[in]  b  8 bytes
@return the value */
uint64_t ut_read_u64(const unsigned char *b);

#endif
