/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_batch.h
 *    Column batch: the rows of one accepted write, stored by column.
 *
 * A batch belongs to one table and holds a prefix of its logical columns.
 * Missing trailing columns are read as NULL. A batch is never changed once it
 * is published; readers may share it without locks.
 *
 *-------------------------------------------------------------------------
 */

#ifndef db_batch_h
#define db_batch_h

#include <cstdint>
#include <string>
#include <vector>

#include "db_schema.h"

/** Column values and NULL flags for one batch. */
struct batch_column_t {
  /** Reset this column to n_rows NULL values of the given type. */
  void reset(data_type_t type, uint32_t n_rows);

  /** Check row-array lengths and the bounds of packed strings. */
  bool validate(uint32_t n_rows) const;

  /** Value type; selects fixed-width values or packed string storage. */
  data_type_t m_data_type{data_type_t::VARCHAR};

  /** One byte per row: 1 if the value is NULL. */
  std::vector<uint8_t> m_nulls;

  /** data_type_t::BOOLEAN: 0 or 1 per row. */
  std::vector<uint8_t> m_booleans;

  /** BIGINT or TIMESTAMP value per row, including NULL rows. */
  std::vector<int64_t> m_integers;

  /** DOUBLE value per row, including NULL rows. */
  std::vector<double> m_doubles;

  /** Packed VARCHAR bytes; row r spans
  [m_string_offsets[r], m_string_offsets[r + 1]). */
  std::string m_string_data;

  /** data_type_t::VARCHAR: n_rows + 1 entries, starting with 0. */
  std::vector<uint32_t> m_string_offsets;
};

/** Rows of one write, by column. */
struct batch_t {
  /** Count column data bytes for memory accounting. */
  uint64_t calculate_data_bytes() const;

  /** Canonical table name. */
  std::string m_table_name;

  /** Table schema version assigned at reservation; 0 before resolution. */
  schema_version_t m_schema_version{0};

  /** Rows in every column. */
  uint32_t m_row_count{0};

  /** Column data bytes, computed at reservation before the batch freezes. */
  uint64_t m_data_bytes{0};

  /** m_columns[i] holds column id i. */
  std::vector<batch_column_t> m_columns;
};

#endif /* db_batch_h */
