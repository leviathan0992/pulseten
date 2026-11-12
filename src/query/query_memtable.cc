/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query_memtable.cc
 *    _pt_memtable_scan(token, table): a DuckDB table function over the memtable
 *    batches of a running query.
 *
 * It returns every column of the table's snapshot schema; DuckDB asks only
 * for the columns it needs (projection pushdown). A batch written before a
 * column was added has no values for it: NULL. Filters are applied by
 * DuckDB above the scan.
 *
 *-------------------------------------------------------------------------
 */

#include "query.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include <duckdb.h>

#include "batch.h"
#include "duckdb_database.h"
#include "query_priv.h"
#include "schema.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_new.h"
#include "ut_noncopyable.h"

/** Bind data: the table being scanned. */
struct query_memtable_bind_t {
  /** Set while binding, read by DuckDB's worker threads while scanning.
  DuckDB hands the bind data to its workers through its own task queue,
  which orders this correctly; the release and acquire make that order
  visible to ThreadSanitizer, which does not see into DuckDB. */
  std::atomic<const query_table_t *> m_table{nullptr};

  /** Query receiving callback failures; published before workers initialize. */
  std::atomic<uint64_t> m_token{0};
};

/** Scan state: projected columns and the read position. */
class query_memtable_scan_state final : private ut_noncopyable {
 public:
  /** Retain the projected columns selected by DuckDB. */
  void init(duckdb_init_info info);

  /** Produce the next chunk and advance this scan's position. */
  void scan(const query_table_t &table, duckdb_data_chunk output);

 private:
  /** Column ids, in output order. */
  std::vector<idx_t> m_cols;

  /** Batch the next row is read from. */
  size_t m_batch{0};

  /** Next row in that batch. */
  uint32_t m_row{0};
};

/** Free bind data; DuckDB calls it when the bind data goes. */
static void query_memtable_delete_bind(void *p) {
  ut_delete(static_cast<query_memtable_bind_t *>(p));

  return;
}

/** Free scan state; DuckDB calls it when the scan ends. */
static void query_memtable_delete_state(void *p) {
  ut_delete(static_cast<query_memtable_scan_state *>(p));

  return;
}

/** Own binding resources until DuckDB accepts the completed bind data. */
class query_memtable_bind_guard final : private ut_noncopyable {
 public:
  /** Parameter 0: the query's registry token. */
  duckdb_value m_token_value{nullptr};

  /** Parameter 1: the normalized table name. */
  duckdb_value m_name_value{nullptr};

  /** The table name as a C string. */
  char *m_name{nullptr};

  /** Bind data not yet handed to DuckDB. */
  query_memtable_bind_t *m_bind{nullptr};

  /** Type of the result column being added. */
  duckdb_logical_type m_type{nullptr};

  /** Release temporary binding resources and any untransferred bind data. */
  ~query_memtable_bind_guard() {
    duckdb_destroy_logical_type(&m_type);
    ut_delete(m_bind);
    duckdb_free(m_name);
    duckdb_destroy_value(&m_name_value);
    duckdb_destroy_value(&m_token_value);
  }
};

/** Bind callback: find the query and table named by the parameters and
declare the columns.
@param[in,out]  info  DuckDB binding context receiving columns, cardinality
                      and owned bind data */
static void query_memtable_bind(duckdb_bind_info info) {
  query_memtable_bind_guard owner;
  owner.m_token_value = duckdb_bind_get_parameter(info, 0);
  owner.m_name_value = duckdb_bind_get_parameter(info, 1);

  const uint64_t token = duckdb_get_uint64(owner.m_token_value);
  owner.m_name = duckdb_get_varchar(owner.m_name_value);

  const query_snapshot_t *snapshot =
      query_registry::instance().find_by_token(token);
  const query_table_t *table = nullptr;

  if (snapshot != nullptr && owner.m_name != nullptr) {
    std::map<std::string, query_table_t>::const_iterator it =
        snapshot->m_tables.find(owner.m_name);

    if (it != snapshot->m_tables.end()) {
      table = &it->second;
    }
  }

  /* Only the generated SQL of a running query names this function. */
  if (table == nullptr) {
    duckdb_bind_set_error(info, QUERY_MEMTABLE_SCAN " is internal");

    return;
  }

  owner.m_bind = ut_new<query_memtable_bind_t>();
  if (owner.m_bind == nullptr) {
    query_registry::instance().record_error(token, DB_OUT_OF_MEMORY);
    duckdb_bind_set_error(info, "out of memory");

    return;
  }

  owner.m_bind->m_token.store(token, std::memory_order_release);
  owner.m_bind->m_table.store(table, std::memory_order_release);

  uint64_t rows = 0;
  for (size_t i = 0; i < table->m_batches.size(); ++i) {
    rows += table->m_batches[i]->m_n_rows;
  }

  for (size_t c = 0; c < table->m_schema.m_cols.size(); ++c) {
    const col_def_t &col = table->m_schema.m_cols[c];

    owner.m_type = duckdb_create_logical_type(duckdb_type_from_col(col.m_type));
    if (owner.m_type == nullptr) {
      query_registry::instance().record_error(token, DB_OUT_OF_MEMORY);
      duckdb_bind_set_error(info, "out of memory");

      return;
    }

    duckdb_bind_add_result_column(info, col.m_name.c_str(), owner.m_type);
    duckdb_destroy_logical_type(&owner.m_type);
  }

  duckdb_bind_set_cardinality(info, rows, true);
  duckdb_bind_set_bind_data(info, owner.m_bind, query_memtable_delete_bind);
  owner.m_bind = nullptr;

  return;
}

/** Record the projected columns; the scan runs on one thread. */
void query_memtable_scan_state::init(duckdb_init_info info) {
  const idx_t n = duckdb_init_get_column_count(info);

  for (idx_t i = 0; i < n; ++i) {
    m_cols.push_back(duckdb_init_get_column_index(info, i));
  }

  return;
}

/** Create scan state, preserving allocation failure across DuckDB callbacks. */
static void query_memtable_init(duckdb_init_info info) {
  query_memtable_scan_state *state = ut_new<query_memtable_scan_state>();
  if (state == nullptr) {
    const query_memtable_bind_t *bind =
        static_cast<const query_memtable_bind_t *>(
            duckdb_init_get_bind_data(info));
    query_registry::instance().record_error(
        bind->m_token.load(std::memory_order_acquire), DB_OUT_OF_MEMORY);
    duckdb_init_set_error(info, "out of memory");

    return;
  }

  state->init(info);

  duckdb_init_set_max_threads(info, 1);
  duckdb_init_set_init_data(info, state, query_memtable_delete_state);

  return;
}

/** Copy rows [row, row + n) of a batch column to positions [at, at + n) of an
output vector. A missing column, or a column id beyond the schema such as the
row id, gives NULL.
@param[out]  out  destination vector; NULL rows are marked in its validity
                  mask */
static void query_memtable_copy(const batch_t &b, idx_t col_id, uint32_t row,
                                idx_t n, duckdb_vector out, idx_t at) {
  ut_ad(row + n <= b.m_n_rows);

  duckdb_vector_ensure_validity_writable(out);
  uint64_t *validity = duckdb_vector_get_validity(out);
  void *data = duckdb_vector_get_data(out);

  if (col_id >= b.m_cols.size()) {
    for (idx_t i = 0; i < n; ++i) {
      duckdb_validity_set_row_invalid(validity, at + i);
    }

    return;
  }

  const batch_col_t &col = b.m_cols[col_id];

  for (idx_t i = 0; i < n; ++i) {
    const uint32_t r = row + static_cast<uint32_t>(i);
    const idx_t out_row = at + i;

    if (col.m_nulls[r]) {
      duckdb_validity_set_row_invalid(validity, out_row);
      continue;
    }

    switch (col.m_type) {
      case col_type_t::BOOLEAN:
        static_cast<bool *>(data)[out_row] = col.m_bools[r] != 0;
        break;

      case col_type_t::BIGINT:
        static_cast<int64_t *>(data)[out_row] = col.m_ints[r];
        break;

      case col_type_t::DOUBLE:
        static_cast<double *>(data)[out_row] = col.m_doubles[r];
        break;

      case col_type_t::TIMESTAMP:
        static_cast<duckdb_timestamp *>(data)[out_row].micros = col.m_ints[r];
        break;

      case col_type_t::VARCHAR: {
        const uint32_t start = col.m_offsets[r];
        const uint32_t len = col.m_offsets[r + 1] - start;

        duckdb_vector_assign_string_element_len(
            out, out_row, col.m_chars.data() + start, len);
        break;
      }
    }
  }

  return;
}

/** Fill one output chunk from the batches.
@param[out]  output  next output chunk; size 0 marks the end */
void query_memtable_scan_state::scan(const query_table_t &table,
                                     duckdb_data_chunk output) {
  const std::vector<const batch_t *> &batches = table.m_batches;
  const idx_t capacity = duckdb_vector_size();
  idx_t filled = 0;

  while (filled < capacity && m_batch < batches.size()) {
    const batch_t *b = batches[m_batch];
    ut_a(m_row < b->m_n_rows);

    const idx_t left = b->m_n_rows - m_row;
    const idx_t n = left < capacity - filled ? left : capacity - filled;

    for (size_t i = 0; i < m_cols.size(); ++i) {
      duckdb_vector out = duckdb_data_chunk_get_vector(output, i);
      query_memtable_copy(*b, m_cols[i], m_row, n, out, filled);
    }

    filled += n;
    m_row += static_cast<uint32_t>(n);

    if (m_row == b->m_n_rows) {
      m_batch++;
      m_row = 0;
    }
  }

  duckdb_data_chunk_set_size(output, filled);

  return;
}

/** Native callback adapter; DuckDB owns bind and scan state. */
static void query_memtable_scan(duckdb_function_info info,
                                duckdb_data_chunk output) {
  const query_memtable_bind_t *bind =
      static_cast<const query_memtable_bind_t *>(
          duckdb_function_get_bind_data(info));
  query_memtable_scan_state *state = static_cast<query_memtable_scan_state *>(
      duckdb_function_get_init_data(info));

  state->scan(*bind->m_table.load(std::memory_order_acquire), output);

  return;
}

/** Own registration handles from creation through every failure path. */
class query_memtable_install_guard final : private ut_noncopyable {
 public:
  /** The table function being registered. */
  duckdb_table_function m_fn{nullptr};

  /** Type of its token parameter, UBIGINT. */
  duckdb_logical_type m_token_type{nullptr};

  /** Type of its table name parameter, VARCHAR. */
  duckdb_logical_type m_name_type{nullptr};

  /** Destroy temporary registration handles and logical types. */
  ~query_memtable_install_guard() {
    duckdb_destroy_logical_type(&m_name_type);
    duckdb_destroy_logical_type(&m_token_type);
    duckdb_destroy_table_function(&m_fn);
  }
};

/** Install the memtable scan table function on a connection. */
db_status_t query_install_memtable_scan(duckdb_connection conn) {
  query_memtable_install_guard owner;

  owner.m_fn = duckdb_create_table_function();

  duckdb_table_function_set_name(owner.m_fn, QUERY_MEMTABLE_SCAN);

  owner.m_token_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  owner.m_name_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

  duckdb_table_function_add_parameter(owner.m_fn, owner.m_token_type);
  duckdb_table_function_add_parameter(owner.m_fn, owner.m_name_type);

  duckdb_destroy_logical_type(&owner.m_token_type);
  duckdb_destroy_logical_type(&owner.m_name_type);

  duckdb_table_function_set_bind(owner.m_fn, query_memtable_bind);
  duckdb_table_function_set_init(owner.m_fn, query_memtable_init);
  duckdb_table_function_set_function(owner.m_fn, query_memtable_scan);
  duckdb_table_function_supports_projection_pushdown(owner.m_fn, true);

  query_enable_memtable_reuse(owner.m_fn);

  const bool ok =
      duckdb_register_table_function(conn, owner.m_fn) == DuckDBSuccess;

  return (ok ? DB_SUCCESS : DB_ERROR);
}
