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

#include "db_batch.h"
#include "db_schema.h"
#include "memtable.h"
#include "pt_duckdb.h"
#include "ut.h"

/** Bind data: the table being scanned. */
struct query_memtable_bind_t {
  /* ================== Atomically published binding ================== */

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
  void init(duckdb_init_info info, const query_table_t &table);

  /** Produce the next chunk and advance this scan's position. */
  void scan(const query_table_t &table, duckdb_data_chunk output);

 private:
  /** Column ids, in output order. */
  std::vector<idx_t> m_column_ids;

  /** Last sequence inspected; the registered interval supplies the bounds. */
  batch_seq_t m_seq{0};

  /** Maximum batch pointers fetched per memtable scan. */
  static const size_t m_batch_cache_size = 256;

  /** Immutable batches kept alive by the snapshot. */
  const batch_t *m_batches[m_batch_cache_size]{};

  /** Number of populated slots in m_batches. */
  size_t m_batch_count{0};

  /** Batch the next row is read from, within the bounded pointer cache. */
  size_t m_batch_index{0};

  /** Next row in that batch. */
  uint32_t m_row_index{0};
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

/** Bind callback: find the query and table named by the parameters and
declare the columns.
@param[in,out]  info  DuckDB binding context receiving columns, cardinality
                      and owned bind data */
static void query_memtable_bind(duckdb_bind_info info) {
  duckdb_value token_value = duckdb_bind_get_parameter(info, 0);
  const uint64_t token = duckdb_get_uint64(token_value);
  duckdb_destroy_value(&token_value);

  duckdb_value name_value = duckdb_bind_get_parameter(info, 1);
  char *name = duckdb_get_varchar(name_value);
  duckdb_destroy_value(&name_value);

  const query_snapshot_t *snapshot =
      query_registry::instance().find_by_token(token);
  const query_table_t *table = nullptr;

  if (snapshot != nullptr && name != nullptr) {
    std::map<std::string, query_table_t>::const_iterator it =
        snapshot->m_tables.find(name);

    if (it != snapshot->m_tables.end()) {
      table = &it->second;
    }
  }

  duckdb_free(name);

  /* Only the generated SQL of a running query names this function. */
  if (table == nullptr) {
    duckdb_bind_set_error(info, QUERY_MEMTABLE_SCAN_NAME " is internal");

    return;
  }

  query_memtable_bind_t *bind = ut_new<query_memtable_bind_t>();
  if (bind == nullptr) {
    query_registry::instance().record_error(token, DB_OUT_OF_MEMORY);
    duckdb_bind_set_error(info, "out of memory");

    return;
  }

  bind->m_token.store(token, std::memory_order_release);
  bind->m_table.store(table, std::memory_order_release);

  for (size_t c = 0; c < table->m_schema.m_columns.size(); ++c) {
    const schema_column_t &col = table->m_schema.m_columns[c];

    duckdb_logical_type data_type =
        duckdb_create_logical_type(pt_duckdb_convert_type(col.m_data_type));
    if (data_type == nullptr) {
      ut_delete(bind);
      query_registry::instance().record_error(token, DB_OUT_OF_MEMORY);
      duckdb_bind_set_error(info, "out of memory");

      return;
    }

    duckdb_bind_add_result_column(info, col.m_name.c_str(), data_type);
    duckdb_destroy_logical_type(&data_type);
  }

  duckdb_bind_set_bind_data(info, bind, query_memtable_delete_bind);

  return;
}

/** Record the projected columns; the scan runs on one thread. */
void query_memtable_scan_state::init(duckdb_init_info info,
                                     const query_table_t &table) {
  ut_a(table.m_memtable_snapshot != nullptr);

  m_seq = table.m_memtable_snapshot->m_base_seq;

  const idx_t n = duckdb_init_get_column_count(info);

  for (idx_t i = 0; i < n; ++i) {
    m_column_ids.push_back(duckdb_init_get_column_index(info, i));
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

  const query_memtable_bind_t *bind =
      static_cast<const query_memtable_bind_t *>(
          duckdb_init_get_bind_data(info));
  state->init(info, *bind->m_table.load(std::memory_order_acquire));

  duckdb_init_set_max_threads(info, 1);
  duckdb_init_set_init_data(info, state, query_memtable_delete_state);

  return;
}

/** Copy rows [row, row + n) of a batch column to positions [at, at + n) of an
output vector. A missing column, or a column id beyond the schema such as the
row id, gives NULL.
@param[out]  out  destination vector; NULL rows are marked in its validity
                  mask */
static void query_memtable_copy_column(const batch_t &b, idx_t col_id,
                                       uint32_t row, idx_t n, duckdb_vector out,
                                       idx_t at) {
  ut_ad(row + n <= b.m_row_count);

  duckdb_vector_ensure_validity_writable(out);
  uint64_t *validity = duckdb_vector_get_validity(out);
  void *data = duckdb_vector_get_data(out);

  if (col_id >= b.m_columns.size()) {
    for (idx_t i = 0; i < n; ++i) {
      duckdb_validity_set_row_invalid(validity, at + i);
    }

    return;
  }

  const batch_column_t &col = b.m_columns[col_id];

  for (idx_t i = 0; i < n; ++i) {
    const uint32_t r = row + static_cast<uint32_t>(i);
    const idx_t out_row = at + i;

    if (col.m_nulls[r]) {
      duckdb_validity_set_row_invalid(validity, out_row);
      continue;
    }

    switch (col.m_data_type) {
      case data_type_t::BOOLEAN:
        static_cast<bool *>(data)[out_row] = col.m_booleans[r] != 0;
        break;

      case data_type_t::BIGINT:
        static_cast<int64_t *>(data)[out_row] = col.m_integers[r];
        break;

      case data_type_t::DOUBLE:
        static_cast<double *>(data)[out_row] = col.m_doubles[r];
        break;

      case data_type_t::TIMESTAMP:
        static_cast<duckdb_timestamp *>(data)[out_row].micros =
            col.m_integers[r];
        break;

      case data_type_t::VARCHAR: {
        const uint32_t start = col.m_string_offsets[r];
        const uint32_t len = col.m_string_offsets[r + 1] - start;

        duckdb_vector_assign_string_element_len(
            out, out_row, col.m_string_data.data() + start, len);
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
  const memtable::snapshot_t &snapshot = *table.m_memtable_snapshot;
  const idx_t capacity = duckdb_vector_size();
  idx_t filled = 0;

  while (filled < capacity) {
    if (m_batch_index == m_batch_count) {
      if (m_seq == snapshot.m_visible_seq) {
        break;
      }

      m_batch_count = snapshot.m_owner->scan_batches(
          snapshot, table.m_schema.m_name, m_seq, m_batches,
          sizeof(m_batches) / sizeof(m_batches[0]));
      m_batch_index = 0;
      if (m_batch_count == 0) {
        continue;
      }
    }

    const batch_t *b = m_batches[m_batch_index];
    ut_a(m_row_index < b->m_row_count);

    const idx_t left = b->m_row_count - m_row_index;
    const idx_t n = left < capacity - filled ? left : capacity - filled;

    for (size_t i = 0; i < m_column_ids.size(); ++i) {
      duckdb_vector out = duckdb_data_chunk_get_vector(output, i);
      query_memtable_copy_column(*b, m_column_ids[i], m_row_index, n, out,
                                 filled);
    }

    filled += n;
    m_row_index += static_cast<uint32_t>(n);

    if (m_row_index == b->m_row_count) {
      m_batch_index++;
      m_row_index = 0;
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

/** Install the memtable scan table function on a connection. */
db_status_t query_install_memtable_scan(duckdb_connection conn) {
  duckdb_table_function function = duckdb_create_table_function();

  duckdb_table_function_set_name(function, QUERY_MEMTABLE_SCAN_NAME);

  duckdb_logical_type token_type =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type name_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

  duckdb_table_function_add_parameter(function, token_type);
  duckdb_table_function_add_parameter(function, name_type);

  duckdb_destroy_logical_type(&token_type);
  duckdb_destroy_logical_type(&name_type);

  duckdb_table_function_set_bind(function, query_memtable_bind);
  duckdb_table_function_set_init(function, query_memtable_init);
  duckdb_table_function_set_function(function, query_memtable_scan);
  duckdb_table_function_supports_projection_pushdown(function, true);

  query_wrap_memtable_bind(function);

  const bool ok =
      duckdb_register_table_function(conn, function) == DuckDBSuccess;

  duckdb_destroy_table_function(&function);

  return (ok ? DB_SUCCESS : DB_ERROR);
}
