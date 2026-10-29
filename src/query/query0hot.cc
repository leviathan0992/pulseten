/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query0hot.cc
 *    _pt_hot_scan(token, table): a DuckDB table function over the memtable
 *    batches of a running query.
 *
 * It returns every column of the table's snapshot schema; DuckDB asks only
 * for the columns it needs (projection pushdown). A batch written before a
 * column was added has no values for it: NULL. Filters are applied by
 * DuckDB above the scan.
 *
 *-------------------------------------------------------------------------
 */

#include "query0priv.h"

#include <atomic>
#include <new>

#include "duck0duck.h"
#include "ut0new.h"

/** Bind data: the table being scanned. */
struct query_hot_bind_t {
  /** Set while binding, read by DuckDB's worker threads while scanning.
  DuckDB hands the bind data to its workers through its own task queue,
  which orders this correctly; the release and acquire make that order
  visible to ThreadSanitizer, which does not see into DuckDB. */
  std::atomic<const query_table_t *> table{nullptr};
};

/** Scan state: projected columns and the read position. */
struct query_hot_state_t {
  /** Column ids, in output order. */
  std::vector<idx_t> cols;

  /** Batch the next row is read from. */
  size_t batch{0};

  /** Next row in that batch. */
  uint32_t row{0};
};

/** Free bind data; DuckDB calls it when the bind data goes.
@param[in]  p  bind data allocated for the hot scan; released by this callback
*/
static void query_hot_delete_bind(void *p) {
  ut_delete(static_cast<query_hot_bind_t *>(p));
}

/** Free scan state; DuckDB calls it when the scan ends.
@param[in]  p  hot-scan state to destroy */
static void query_hot_delete_state(void *p) {
  ut_delete(static_cast<query_hot_state_t *>(p));
}

/** Own binding resources until DuckDB accepts the completed bind data. */
struct query_hot_bind_guard_t {
  /** Parameter 0: the query's registry token. */
  duckdb_value token_value{nullptr};

  /** Parameter 1: the canonical table name. */
  duckdb_value name_value{nullptr};

  /** The table name as a C string. */
  char *name{nullptr};

  /** Bind data not yet handed to DuckDB. */
  query_hot_bind_t *bind{nullptr};

  /** Type of the result column being added. */
  duckdb_logical_type type{nullptr};

  /** Release temporary binding resources and any untransferred bind data. */
  ~query_hot_bind_guard_t() {
    duckdb_destroy_logical_type(&type);
    ut_delete(bind);
    duckdb_free(name);
    duckdb_destroy_value(&name_value);
    duckdb_destroy_value(&token_value);
  }
};

/** Bind callback: find the query and table named by the parameters and
declare the columns.
@param[in,out]  info  DuckDB binding context receiving columns, cardinality
                      and owned bind data */
static void query_hot_bind(duckdb_bind_info info) {
  query_hot_bind_guard_t owner;
  owner.token_value = duckdb_bind_get_parameter(info, 0);
  owner.name_value = duckdb_bind_get_parameter(info, 1);

  const uint64_t token = duckdb_get_uint64(owner.token_value);
  owner.name = duckdb_get_varchar(owner.name_value);

  const query_snapshot_t *snap = query_find_by_token(token);
  const query_table_t *table = nullptr;

  if (snap != nullptr && owner.name != nullptr) {
    std::map<std::string, query_table_t>::const_iterator it =
        snap->tables.find(owner.name);

    if (it != snap->tables.end()) {
      table = &it->second;
    }
  }

  /* Only the generated SQL of a running query names this function. */
  if (table == nullptr) {
    duckdb_bind_set_error(info, QUERY_HOT_SCAN " is internal");

    return;
  }

  owner.bind = ut_new<query_hot_bind_t>();
  if (owner.bind == nullptr) {
    duckdb_bind_set_error(info, "out of memory");

    return;
  }

  owner.bind->table.store(table, std::memory_order_release);

  uint64_t rows = 0;
  for (size_t i = 0; i < table->batches.size(); ++i) {
    rows += table->batches[i]->n_rows;
  }

  for (size_t c = 0; c < table->schema.cols.size(); ++c) {
    const col_def_t &col = table->schema.cols[c];

    owner.type = duckdb_create_logical_type(duck_type(col.type));
    if (owner.type == nullptr) {
      duckdb_bind_set_error(info, "out of memory");

      return;
    }
    duckdb_bind_add_result_column(info, col.name.c_str(), owner.type);
    duckdb_destroy_logical_type(&owner.type);
  }

  duckdb_bind_set_cardinality(info, rows, true);
  duckdb_bind_set_bind_data(info, owner.bind, query_hot_delete_bind);
  owner.bind = nullptr;
}

/** Record the projected columns; the scan runs on one thread.
@param[in,out]  info  DuckDB initialization context receiving the scan
                      position and projection */
static void query_hot_init(duckdb_init_info info) {
  query_hot_state_t *state = ut_new<query_hot_state_t>();
  if (state == nullptr) {
    duckdb_init_set_error(info, "out of memory");

    return;
  }

  const idx_t n = duckdb_init_get_column_count(info);

  for (idx_t i = 0; i < n; ++i) {
    state->cols.push_back(duckdb_init_get_column_index(info, i));
  }

  duckdb_init_set_max_threads(info, 1);
  duckdb_init_set_init_data(info, state, query_hot_delete_state);
}

/** Copy rows [row, row + n) of a batch column to positions [at, at + n) of an
output vector. A missing column, or a column id beyond the schema such as the
row id, gives NULL.
@param[in]   b       immutable source batch
@param[in]   col_id  column id in the schema
@param[in]   row     first source row
@param[in]   n       number of rows to copy
@param[out]  out     destination vector; NULL rows are marked in its validity
                     mask
@param[in]   at      first destination row */
static void query_hot_copy(const batch_t &b, idx_t col_id, uint32_t row,
                           idx_t n, duckdb_vector out, idx_t at) {
  duckdb_vector_ensure_validity_writable(out);
  uint64_t *validity = duckdb_vector_get_validity(out);
  void *data = duckdb_vector_get_data(out);

  if (col_id >= b.cols.size()) {
    for (idx_t i = 0; i < n; ++i) {
      duckdb_validity_set_row_invalid(validity, at + i);
    }

    return;
  }

  const batch_col_t &col = b.cols[col_id];

  for (idx_t i = 0; i < n; ++i) {
    const uint32_t r = row + (uint32_t)i;
    const idx_t o = at + i;

    if (col.nulls[r]) {
      duckdb_validity_set_row_invalid(validity, o);
      continue;
    }

    switch (col.type) {
      case COL_BOOLEAN:
        static_cast<bool *>(data)[o] = col.bools[r] != 0;
        break;

      case COL_BIGINT:
        static_cast<int64_t *>(data)[o] = col.ints[r];
        break;

      case COL_DOUBLE:
        static_cast<double *>(data)[o] = col.doubles[r];
        break;

      case COL_TIMESTAMP:
        static_cast<duckdb_timestamp *>(data)[o].micros = col.ints[r];
        break;

      case COL_VARCHAR: {
        const uint32_t start = col.offsets[r];
        const uint32_t len = col.offsets[r + 1] - start;

        duckdb_vector_assign_string_element_len(out, o,
                                                col.chars.data() + start, len);
        break;
      }
    }
  }
}

/** Fill one output chunk from the batches.
@param[in]   info    DuckDB callback context containing bind data and scan
                     state
@param[out]  output  next output chunk; size 0 marks the end */
static void query_hot_scan(duckdb_function_info info,
                           duckdb_data_chunk output) {
  const query_hot_bind_t *bind = static_cast<const query_hot_bind_t *>(
      duckdb_function_get_bind_data(info));
  query_hot_state_t *state =
      static_cast<query_hot_state_t *>(duckdb_function_get_init_data(info));

  const std::vector<const batch_t *> &batches =
      bind->table.load(std::memory_order_acquire)->batches;
  const idx_t capacity = duckdb_vector_size();
  idx_t filled = 0;

  while (filled < capacity && state->batch < batches.size()) {
    const batch_t *b = batches[state->batch];
    const idx_t left = b->n_rows - state->row;
    const idx_t n = left < capacity - filled ? left : capacity - filled;

    for (size_t i = 0; i < state->cols.size(); ++i) {
      duckdb_vector out = duckdb_data_chunk_get_vector(output, i);
      query_hot_copy(*b, state->cols[i], state->row, n, out, filled);
    }

    filled += n;
    state->row += (uint32_t)n;

    if (state->row == b->n_rows) {
      state->batch++;
      state->row = 0;
    }
  }

  duckdb_data_chunk_set_size(output, filled);
}

/** Own registration handles from creation through every failure path. */
struct query_hot_install_guard_t {
  /** The table function being registered. */
  duckdb_table_function fn{nullptr};

  /** Type of its token parameter, UBIGINT. */
  duckdb_logical_type token_type{nullptr};

  /** Type of its table name parameter, VARCHAR. */
  duckdb_logical_type name_type{nullptr};

  /** Destroy temporary registration handles and logical types. */
  ~query_hot_install_guard_t() {
    duckdb_destroy_logical_type(&name_type);
    duckdb_destroy_logical_type(&token_type);
    duckdb_destroy_table_function(&fn);
  }
};

/** Register the memtable scan table function on a connection.
@param[in,out]  conn  connection on which the internal hot-scan function is
                      registered
@return DB_SUCCESS or DB_ERROR if DuckDB refuses registration */
dberr_t query_install_hot_scan(duckdb_connection conn) {
  query_hot_install_guard_t owner;
  owner.fn = duckdb_create_table_function();

  duckdb_table_function_set_name(owner.fn, QUERY_HOT_SCAN);

  owner.token_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  owner.name_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

  duckdb_table_function_add_parameter(owner.fn, owner.token_type);
  duckdb_table_function_add_parameter(owner.fn, owner.name_type);

  duckdb_destroy_logical_type(&owner.token_type);
  duckdb_destroy_logical_type(&owner.name_type);

  duckdb_table_function_set_bind(owner.fn, query_hot_bind);
  duckdb_table_function_set_init(owner.fn, query_hot_init);
  duckdb_table_function_set_function(owner.fn, query_hot_scan);
  duckdb_table_function_supports_projection_pushdown(owner.fn, true);

  const bool ok =
      duckdb_register_table_function(conn, owner.fn) == DuckDBSuccess;

  return (ok ? DB_SUCCESS : DB_ERROR);
}
