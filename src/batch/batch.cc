/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * batch.cc
 *    Column batch helpers.
 *
 *-------------------------------------------------------------------------
 */

#include "batch.h"

#include <cstddef>
#include <cstdint>

#include "schema.h"

/** Reset a column to n_rows NULL values of the given type. */
void batch_reset_column(batch_col_t &col, uint32_t col_id, col_type_t type,
                        uint32_t n_rows) {
  col = batch_col_t();

  col.m_col_id = col_id;
  col.m_type = type;
  col.m_nulls.assign(n_rows, 1);

  switch (type) {
    case col_type_t::BOOLEAN:
      col.m_bools.assign(n_rows, 0);
      break;
    case col_type_t::BIGINT:
    case col_type_t::TIMESTAMP:
      col.m_ints.assign(n_rows, 0);
      break;
    case col_type_t::DOUBLE:
      col.m_doubles.assign(n_rows, 0);
      break;
    case col_type_t::VARCHAR:
      col.m_offsets.assign(n_rows + 1, 0);
      break;
  }
}

/** Calculate the total bytes of column data in a batch. */
uint64_t batch_calc_data_bytes(const batch_t &batch) {
  uint64_t bytes = 0;

  for (size_t i = 0; i < batch.m_cols.size(); ++i) {
    const batch_col_t &c = batch.m_cols[i];

    bytes += c.m_nulls.size() + c.m_bools.size();
    bytes += 8 * (c.m_ints.size() + c.m_doubles.size());
    bytes += c.m_chars.size() + 4 * c.m_offsets.size();
  }

  return (bytes);
}
