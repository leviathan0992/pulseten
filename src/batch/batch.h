/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * batch.h
 *    Column batch: the rows of one accepted write, stored by column.
 *
 * A batch belongs to one table and holds a prefix of its logical columns.
 * Missing trailing columns are read as NULL. A batch is never changed once it
 * is published; readers may share it without locks.
 *
 *-------------------------------------------------------------------------
 */

#ifndef batch_h
#define batch_h

#include <cstdint>
#include <string>
#include <vector>

#include "schema.h"

/** Values of one column for every row of a batch. Only the vector that
matches the type is used. A NULL row still has a slot (0, false or an
empty string) so that row r is always at index r. */
struct batch_col_t {
  /** Reset this column to n_rows NULL values of the given type. */
  void reset(uint32_t col_id, col_type_t type, uint32_t n_rows);

  /** Stable logical column id. */
  uint32_t m_col_id{0};

  /** Value type; selects the vector below that holds the values. */
  col_type_t m_type{col_type_t::VARCHAR};

  /** One byte per row: 1 if the value is NULL. */
  std::vector<uint8_t> m_nulls;

  /** col_type_t::BOOLEAN: 0 or 1 per row. */
  std::vector<uint8_t> m_bools;

  /** col_type_t::BIGINT and col_type_t::TIMESTAMP. */
  std::vector<int64_t> m_ints;

  /** col_type_t::DOUBLE. */
  std::vector<double> m_doubles;

  /** col_type_t::VARCHAR: bytes of all rows; row r is [offsets[r], offsets[r +
   * 1]). */
  std::string m_chars;

  /** col_type_t::VARCHAR: n_rows + 1 entries, starting with 0. */
  std::vector<uint32_t> m_offsets;
};

/** Rows of one write, by column. */
struct batch_t {
  /** Count column data bytes for memory accounting. */
  uint64_t data_bytes() const;

  /** Canonical table name. */
  std::string m_table_name;

  /** Rows in every column. */
  uint32_t m_n_rows{0};

  /** cols[i] holds column id i. */
  std::vector<batch_col_t> m_cols;
};

#endif /* batch_h */
