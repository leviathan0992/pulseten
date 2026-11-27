/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_batch.cc
 *    Column batch helpers.
 *
 *-------------------------------------------------------------------------
 */

#include "db_batch.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "db_schema.h"

/** Reset a column to n_rows NULL values of the given type. */
void batch_column_t::reset(data_type_t type, uint32_t n_rows) {
  *this = batch_column_t();

  m_data_type = type;
  m_nulls.assign(n_rows, 1);

  switch (type) {
    case data_type_t::BOOLEAN:
      m_booleans.assign(n_rows, 0);

      break;
    case data_type_t::BIGINT:
    case data_type_t::TIMESTAMP:
      m_integers.assign(n_rows, 0);

      break;
    case data_type_t::DOUBLE:
      m_doubles.assign(n_rows, 0);

      break;
    case data_type_t::VARCHAR:
      m_string_offsets.assign(n_rows + 1, 0);
      break;
    default:
      ut_error;
  }

  return;
}

/** Validate the layout assumed by WAL encoding and storage append. */
bool batch_column_t::validate(uint32_t n_rows) const {
  if (m_nulls.size() != n_rows) {
    return (false);
  }

  switch (m_data_type) {
    case data_type_t::BOOLEAN:
      return (m_booleans.size() == n_rows);
    case data_type_t::BIGINT:
    case data_type_t::TIMESTAMP:
      return (m_integers.size() == n_rows);
    case data_type_t::DOUBLE:
      return (m_doubles.size() == n_rows);
    case data_type_t::VARCHAR:
      return (m_string_offsets.size() == static_cast<uint64_t>(n_rows) + 1 &&
              m_string_offsets.front() == 0 &&
              m_string_offsets.back() == m_string_data.size() &&
              std::is_sorted(m_string_offsets.begin(), m_string_offsets.end()));
  }

  return (false);
}

/** Calculate the total bytes of column data in a batch. */
uint64_t batch_t::calculate_data_bytes() const {
  uint64_t bytes = 0;

  for (size_t i = 0; i < m_columns.size(); ++i) {
    const batch_column_t &c = m_columns[i];

    bytes += c.m_nulls.size() + c.m_booleans.size();
    bytes += 8 * (c.m_integers.size() + c.m_doubles.size());
    bytes += c.m_string_data.size() + 4 * c.m_string_offsets.size();
  }

  return (bytes);
}
