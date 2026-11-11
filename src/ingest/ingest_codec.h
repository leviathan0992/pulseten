/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ingest_codec.h
 *    WAL payload of a write: the schema change and the column batch.
 *
 * All integers are little-endian.
 *
 *   u8   payload version (1)
 *   u16  table name length, then the canonical table name
 *   u8   1 if the write creates the table, else 0
 *   u64  base schema version
 *   u64  schema version of the batch
 *   u16  number of new columns, then for each:
 *          u32 id, u8 kind, u8 type, u16 name length, name
 *   u32  number of rows
 *   u16  number of columns, then for each column, in id order:
 *          u8  type
 *          ceil(rows / 8) bytes: bit r is 1 if row r is not NULL
 *          the non-NULL values only: BOOLEAN 1 byte, BIGINT and TIMESTAMP
 *          8 bytes, DOUBLE 8 bytes (IEEE 754 bits), VARCHAR u32 length and
 *          the bytes
 *
 * Decoding checks the structure only. Whether the schema change fits the
 * current schema is checked when the plan is applied.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ingest_codec_h
#define ingest_codec_h

#include <cstddef>
#include <string>

#include "ingest.h"
#include "ut_err.h"

/** Version written first in every encoded payload. */
#define INGEST_CODEC_VERSION 1

/** Encode a write plan as a WAL payload. */
void ingest_encode(const ingest_plan_t &plan, std::string &out);

/** Decode a WAL payload. The error field of the plan stays empty.
@return DB_SUCCESS, or DB_CORRUPTION if the payload is malformed */
db_status_t ingest_decode(const unsigned char *buf, size_t len,
                          ingest_plan_t &plan);

#endif /* ingest_codec_h */
