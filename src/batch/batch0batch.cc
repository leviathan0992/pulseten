/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * batch0batch.cc
 *    Column batch helpers.
 *
 *-------------------------------------------------------------------------
 */

#include "batch0batch.h"

/** Size a column for n_rows rows, all NULL.
@param[out]  col     column to reset
@param[in]   col_id  column id
@param[in]   type    value type
@param[in]   n_rows  number of rows */
void batch_col_init(batch_col_t &col, uint32_t col_id, col_type_t type,
                    uint32_t n_rows) {
  col = batch_col_t();

  col.col_id = col_id;
  col.type = type;
  col.nulls.assign(n_rows, 1);

  switch (type) {
    case COL_BOOLEAN:
      col.bools.assign(n_rows, 0);
      break;
    case COL_BIGINT:
    case COL_TIMESTAMP:
      col.ints.assign(n_rows, 0);
      break;
    case COL_DOUBLE:
      col.doubles.assign(n_rows, 0);
      break;
    case COL_VARCHAR:
      col.offsets.assign(n_rows + 1, 0);
      break;
  }
}

/** Measure the column data held by a batch.
@param[in]  batch  immutable column batch
@return bytes of column data a batch holds, for memory accounting */
uint64_t batch_bytes(const batch_t &batch) {
  uint64_t bytes = 0;

  for (size_t i = 0; i < batch.cols.size(); ++i) {
    const batch_col_t &c = batch.cols[i];

    bytes += c.nulls.size() + c.bools.size();
    bytes += 8 * (c.ints.size() + c.doubles.size());
    bytes += c.chars.size() + 4 * c.offsets.size();
  }

  return (bytes);
}
