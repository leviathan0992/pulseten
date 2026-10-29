/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * batch0batch.h
 *    Column batch: the rows of one accepted write, stored by column.
 *
 * A batch belongs to one table and one schema version and holds a column
 * for every column of that version. A batch is never changed once it is
 * published; readers may share it without locks.
 *
 *-------------------------------------------------------------------------
 */

#ifndef batch0batch_h
#define batch0batch_h

#include <cstdint>
#include <string>
#include <vector>

#include "schema0schema.h"

/** Values of one column for every row of a batch. Only the vector that
matches the type is used. A NULL row still has a slot (0, false or an
empty string) so that row r is always at index r. */
struct batch_col_t {
  /** Column id in the schema version of the batch. */
  uint32_t col_id{0};

  /** Value type; selects the vector below that holds the values. */
  col_type_t type{COL_VARCHAR};

  /** One byte per row: 1 if the value is NULL. */
  std::vector<uint8_t> nulls;

  /** COL_BOOLEAN: 0 or 1 per row. */
  std::vector<uint8_t> bools;

  /** COL_BIGINT and COL_TIMESTAMP. */
  std::vector<int64_t> ints;

  /** COL_DOUBLE. */
  std::vector<double> doubles;

  /** COL_VARCHAR: bytes of all rows; row r is [offsets[r], offsets[r + 1]). */
  std::string chars;

  /** COL_VARCHAR: n_rows + 1 entries, starting with 0. */
  std::vector<uint32_t> offsets;
};

/** Rows of one write, by column. */
struct batch_t {
  /** Canonical table name. */
  std::string table;

  /** Schema version the columns follow. */
  uint64_t schema_version{0};

  /** Rows in every column. */
  uint32_t n_rows{0};

  /** cols[i] holds column id i. */
  std::vector<batch_col_t> cols;
};

/** Measure the column data held by a batch.
@param[in]  batch  immutable column batch
@return bytes of column data a batch holds, for memory accounting */
uint64_t batch_bytes(const batch_t &batch);

/** Size a column for n_rows rows, all NULL.
@param[out]  col     column to reset
@param[in]   col_id  column id
@param[in]   type    value type
@param[in]   n_rows  number of rows */
void batch_col_init(batch_col_t &col, uint32_t col_id, col_type_t type,
                    uint32_t n_rows);

#endif
