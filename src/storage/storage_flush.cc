/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * storage_flush.cc
 *    Storing a run of writes in one DuckDB transaction.
 *
 * A flush works on a copy of the catalog, in two phases inside one
 * transaction:
 *
 *   1. Catalog: for every write, in order, add its table and extend its column
 *      directory if needed, and create or widen the partitions its rows fall
 *      in. Each partition ends up as wide as the widest write it receives
 *      in this flush.
 *   2. Rows: for every touched partition, one appender receives the rows
 *      of all its writes in sequence order, column by column in data
 *      chunks of up to one vector (2048 rows).
 *
 * Then the touched partitions and F are updated and the transaction
 * commits. Only after the commit succeeds does the copy replace the catalog.
 *
 *-------------------------------------------------------------------------
 */

#include "storage.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "batch.h"
#include "duckdb_database.h"
#include "schema.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_noncopyable.h"
#include "ut_time.h"

/** The rows of one write that fall in one partition, in batch order. */
struct storage_slice_t {
  /** Borrowed write whose batch contains the selected rows. */
  const storage_write_t *m_write{nullptr};

  /** Indexes of its rows that fall in the partition. */
  std::vector<uint32_t> m_rows;
};

/** What a flush appends, by partition; slices in sequence order. */
typedef std::map<storage_key_t, std::vector<storage_slice_t>>
    storage_append_plan_t;

/** One flush's metadata work and partition append plan. */
class storage::flush_context final : private ut_noncopyable {
 public:
  /** Borrow the active transaction and the catalog to publish after commit. */
  flush_context(duckdb_session &session, storage_catalog_t &catalog)
      : m_session(session), m_catalog(catalog) {}

  /** Resolve metadata and group one write into partition slices. */
  db_status_t resolve_write(const storage_write_t &w);

  /** Persist touched partition counts and the new stored boundary. */
  db_status_t write_catalog();

  /** Append all resolved partition slices in plan order. */
  db_status_t append_rows();

 private:
  /** Find or create the stored logical table. */
  db_status_t ensure_table(const std::string &name, storage_table_t *&out);

  /** Extend the stored logical column directory. */
  db_status_t ensure_schema(storage_table_t &table, const schema_t &schema);

  /** Find or create a physical time partition. */
  db_status_t ensure_partition(uint64_t table_id, int64_t first_day,
                               const schema_t &schema,
                               storage_partition_t *&out);

  /** Add missing physical columns to the partition. */
  db_status_t ensure_columns(storage_partition_t &p, const schema_t &schema);

  /** Append the planned slices of a partition. */
  db_status_t append_partition(storage_partition_t &p, const schema_t &full,
                               const std::vector<storage_slice_t> &slices);

  /** Borrowed connection running this flush transaction. */
  duckdb_session &m_session;

  /** Pending state; the committed storage catalog is never modified here. */
  storage_catalog_t &m_catalog;

  /** Row slices grouped by physical partition. */
  storage_append_plan_t m_plan;

  /** Schemas resolved in phase 1; catalog map entries remain stable. */
  std::map<uint64_t, const schema_t *> m_schemas;
};

/** Find the table of a write, adding it to the catalog and _pt_tables if it is
new.
@param[in]  name  normalized logical table name
@return DB_SUCCESS, DB_LIMIT_EXCEEDED if table ids are exhausted, or a
  catalog insertion error */
db_status_t storage::flush_context::ensure_table(const std::string &name,
                                                 storage_table_t *&out) {
  std::map<std::string, storage_table_t>::iterator it =
      m_catalog.m_tables.find(name);

  if (it != m_catalog.m_tables.end()) {
    out = &it->second;

    return (DB_SUCCESS);
  }

  if (m_catalog.m_next_table_id == std::numeric_limits<uint64_t>::max()) {
    return (db_status_t(DB_LIMIT_EXCEEDED, "table ids are exhausted"));
  }

  storage_table_t table;
  table.m_table_id = m_catalog.m_next_table_id++;

  const std::string sql = "INSERT INTO _pt_tables VALUES (" +
                          std::to_string(table.m_table_id) + ", " +
                          duckdb_literal(name) + ")";

  if (const db_status_t status = duckdb_exec(m_session.connection(), sql);
      !status.ok()) {
    return (status);
  }

  out = &m_catalog.m_tables.insert(std::make_pair(name, table)).first->second;

  return (DB_SUCCESS);
}

/** Extend the stored column directory when a write adds columns.
Older batches may use any valid prefix of the current directory.
@return DB_SUCCESS, DB_INVALID_ARGUMENT for an invalid directory, or
  a classified DuckDB catalog failure */
db_status_t storage::flush_context::ensure_schema(storage_table_t &table,
                                                  const schema_t &schema) {
  const schema_t &stored = table.m_schema;
  const size_t n_stored = stored.m_cols.size();
  const size_t n_common = std::min(n_stored, schema.m_cols.size());
  for (size_t i = 0; i < n_common; ++i) {
    const col_def_t &a = stored.m_cols[i];
    const col_def_t &b = schema.m_cols[i];
    if (a.m_id != b.m_id || a.m_name != b.m_name || a.m_kind != b.m_kind ||
        a.m_type != b.m_type) {
      return (db_status_t(DB_INVALID_ARGUMENT,
                          "schema changes an existing column"));
    }
  }

  if (schema.m_cols.size() <= n_stored) {
    return (DB_SUCCESS);
  }

  for (size_t i = n_stored; i < schema.m_cols.size(); ++i) {
    const col_def_t &c = schema.m_cols[i];
    const std::string sql =
        "INSERT INTO _pt_columns VALUES (" + std::to_string(table.m_table_id) +
        ", " + std::to_string(c.m_id) + ", " + duckdb_literal(c.m_name) + ", " +
        std::to_string(static_cast<int>(c.m_kind)) + ", " +
        std::to_string(static_cast<int>(c.m_type)) + ")";
    if (const db_status_t err = duckdb_exec(m_session.connection(), sql);
        !err.ok()) {
      return (err);
    }
  }

  table.m_schema = schema;
  return (DB_SUCCESS);
}

/** Find a partition, creating its physical table if it is new.
@param[in]  first_day  first UTC day of the partition, relative to
                       1970-01-01
@return DB_SUCCESS or a classified DuckDB creation failure */
db_status_t storage::flush_context::ensure_partition(
    uint64_t table_id, int64_t first_day, const schema_t &schema,
    storage_partition_t *&out) {
  const storage_key_t key(table_id, first_day);

  std::map<storage_key_t, storage_partition_t>::iterator it =
      m_catalog.m_partitions.find(key);

  if (it != m_catalog.m_partitions.end()) {
    out = &it->second;

    return (DB_SUCCESS);
  }

  storage_partition_t p;
  p.m_table_id = table_id;
  p.m_first_day = first_day;
  p.m_phys_name = storage_partition_name(table_id, first_day);
  p.m_n_cols = static_cast<uint32_t>(schema.m_cols.size());

  /* Row identity, then the columns by id. The time column must lie in
  the partition's days. */
  std::string sql = "CREATE TABLE " + p.m_phys_name +
                    " (_pt_seq UBIGINT NOT NULL, _pt_row UINTEGER NOT NULL";

  for (uint32_t c = 0; c < p.m_n_cols; ++c) {
    sql += ", c" + std::to_string(c) + " " +
           schema_type_name(schema.m_cols[c].m_type);
  }

  const int64_t end_day = first_day + m_catalog.m_partition_duration_days;

  sql += ", CHECK (c0 >= TIMESTAMP '" + ut_day_to_date(first_day) +
         "' AND c0 < TIMESTAMP '" + ut_day_to_date(end_day) + "'))";

  if (const db_status_t status = duckdb_exec(m_session.connection(), sql);
      !status.ok()) {
    return (status);
  }

  sql = "INSERT INTO _pt_partitions VALUES (" + std::to_string(table_id) +
        ", " + std::to_string(first_day) + ", " +
        duckdb_literal(p.m_phys_name) + ", " + std::to_string(p.m_n_cols) +
        ", 0)";

  if (const db_status_t status = duckdb_exec(m_session.connection(), sql);
      !status.ok()) {
    return (status);
  }

  out = &m_catalog.m_partitions.insert(std::make_pair(key, p)).first->second;

  return (DB_SUCCESS);
}

/** Add to a partition the columns a write has and it lacks.
@param[in]  schema  write schema whose missing columns must be added
@return DB_SUCCESS or a classified DuckDB failure when adding a column */
db_status_t storage::flush_context::ensure_columns(storage_partition_t &p,
                                                   const schema_t &schema) {
  while (p.m_n_cols < schema.m_cols.size()) {
    const col_def_t &c = schema.m_cols[p.m_n_cols];

    const std::string sql = "ALTER TABLE " + p.m_phys_name + " ADD COLUMN c" +
                            std::to_string(c.m_id) + " " +
                            schema_type_name(c.m_type);

    if (const db_status_t status = duckdb_exec(m_session.connection(), sql);
        !status.ok()) {
      return (status);
    }

    p.m_n_cols++;
  }

  return (DB_SUCCESS);
}

/** Phase 1 for one write: its table, column directory and partitions. Adds the
write's rows, grouped by partition, to the append plan.
@return DB_SUCCESS or the table/schema/partition catalog error */
db_status_t storage::flush_context::resolve_write(const storage_write_t &w) {
  storage_table_t *table = nullptr;

  if (const db_status_t err = ensure_table(w.m_batch->m_table_name, table);
      !err.ok()) {
    return (err);
  }

  if (const db_status_t err = ensure_schema(*table, *w.m_schema); !err.ok()) {
    return (err);
  }

  m_schemas[table->m_table_id] = &table->m_schema;

  /* Group the rows by partition, keeping their order. */
  std::map<int64_t, std::vector<uint32_t>> parts;
  const batch_col_t &time_col = w.m_batch->m_cols[0];

  for (uint32_t r = 0; r < w.m_batch->m_n_rows; ++r) {
    const int64_t day = ut_utc_day(time_col.m_ints[r]);

    parts[storage_partition_first_day(day, m_catalog.m_partition_duration_days)]
        .push_back(r);
  }

  std::map<int64_t, std::vector<uint32_t>>::iterator it;

  for (it = parts.begin(); it != parts.end(); ++it) {
    storage_partition_t *p = nullptr;

    if (const db_status_t err =
            ensure_partition(table->m_table_id, it->first, *w.m_schema, p);
        !err.ok()) {
      return (err);
    }

    if (const db_status_t err = ensure_columns(*p, *w.m_schema); !err.ok()) {
      return (err);
    }

    storage_slice_t slice;
    slice.m_write = &w;
    slice.m_rows.swap(it->second);

    m_plan[storage_key_t(table->m_table_id, it->first)].push_back(
        std::move(slice));
  }

  return (DB_SUCCESS);
}

/** A data chunk being filled for one partition: _pt_seq, _pt_row, then
c0 .. c<n_cols - 1>. */
class storage_chunk final : private ut_noncopyable {
 public:
  /** Release the native chunk. */
  ~storage_chunk() { duckdb_destroy_data_chunk(&m_chunk); }

  /** Allocate buffers for a physical partition's column prefix. */
  db_status_t init(const schema_t &full, uint32_t n_cols);

  /** Start filling the chunk again, refreshing its native buffers. */
  void reset();

  /** Append one batch row; absent trailing columns become NULL. */
  void add_row(batch_seq_t seq, const batch_t &batch, uint32_t row);

  /** Pass filled rows to an appender and reset the chunk. */
  bool append(duckdb_appender app);

  /** Check whether the next row requires appending this chunk first. */
  bool full() const { return (m_size == m_capacity); }

 private:
  /** The chunk, reused for every batch of rows. */
  duckdb_data_chunk m_chunk{nullptr};

  /** Column types of the partition, c0 first. */
  std::vector<col_type_t> m_types;

  /** Rows a chunk can hold. */
  idx_t m_capacity{0};

  /** Rows in it now. */
  idx_t m_size{0};

  /** Vector of each chunk column. */
  std::vector<duckdb_vector> m_vectors;

  /** Data of each chunk column. */
  std::vector<void *> m_data;

  /** Validity mask of each chunk column. */
  std::vector<uint64_t *> m_validity;
};

/** Empty the chunk and look up its column buffers again.
 */
void storage_chunk::reset() {
  duckdb_data_chunk_reset(m_chunk);

  for (size_t i = 0; i < m_vectors.size(); ++i) {
    duckdb_vector v = duckdb_data_chunk_get_vector(m_chunk, i);

    /* Every value starts valid; NULLs are marked one by one. */
    duckdb_vector_ensure_validity_writable(v);

    m_vectors[i] = v;
    m_data[i] = duckdb_vector_get_data(v);
    m_validity[i] = duckdb_vector_get_validity(v);
  }

  m_size = 0;

  return;
}

/** Destroy the temporary logical types on every return. */
class storage_logical_types final : private ut_noncopyable {
 public:
  /** Types to destroy. */
  std::vector<duckdb_logical_type> m_types;

  /** Destroy every temporary logical type. */
  ~storage_logical_types() {
    for (size_t i = 0; i < m_types.size(); ++i) {
      duckdb_destroy_logical_type(&m_types[i]);
    }
  }
};

/** Create the chunk for a partition's columns.
@return DB_SUCCESS or DB_OUT_OF_MEMORY if DuckDB cannot create the chunk */
db_status_t storage_chunk::init(const schema_t &full, uint32_t n_cols) {
  const size_t n = 2 + static_cast<size_t>(n_cols);

  m_types.resize(n_cols);
  m_vectors.resize(n);
  m_data.resize(n);
  m_validity.resize(n);

  {
    storage_logical_types logical;
    logical.m_types.resize(n);
    logical.m_types[0] = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    logical.m_types[1] = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER);

    for (uint32_t k = 0; k < n_cols; ++k) {
      m_types[k] = full.m_cols[k].m_type;
      logical.m_types[2 + k] =
          duckdb_create_logical_type(duckdb_type_from_col(m_types[k]));
    }

    m_chunk = duckdb_create_data_chunk(logical.m_types.data(), n);
  }

  if (m_chunk == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  m_capacity = duckdb_vector_size();
  reset();

  return (DB_SUCCESS);
}

/** Copy row r of a batch into the next row of the chunk. Columns the batch
does not have, because they were added later, are NULL. */
void storage_chunk::add_row(batch_seq_t seq, const batch_t &batch, uint32_t r) {
  ut_ad(m_size < m_capacity);

  const idx_t i = m_size;

  static_cast<uint64_t *>(m_data[0])[i] = seq;
  static_cast<uint32_t *>(m_data[1])[i] = r;

  for (size_t k = 0; k < m_types.size(); ++k) {
    const size_t dst = 2 + k;

    /* An older batch lacks the columns added after it. */
    if (k >= batch.m_cols.size() || batch.m_cols[k].m_nulls[r]) {
      duckdb_validity_set_row_invalid(m_validity[dst], i);
      continue;
    }

    const batch_col_t &col = batch.m_cols[k];

    switch (m_types[k]) {
      case col_type_t::BOOLEAN:
        static_cast<bool *>(m_data[dst])[i] = col.m_bools[r] != 0;
        break;

      case col_type_t::BIGINT:
        static_cast<int64_t *>(m_data[dst])[i] = col.m_ints[r];
        break;

      case col_type_t::DOUBLE:
        static_cast<double *>(m_data[dst])[i] = col.m_doubles[r];
        break;

      case col_type_t::TIMESTAMP:
        static_cast<duckdb_timestamp *>(m_data[dst])[i].micros = col.m_ints[r];
        break;

      case col_type_t::VARCHAR: {
        const uint32_t start = col.m_offsets[r];
        const uint32_t len = col.m_offsets[r + 1] - start;

        duckdb_vector_assign_string_element_len(
            m_vectors[dst], i, col.m_chars.data() + start, len);
        break;
      }
    }
  }

  m_size++;

  return;
}

/** Hand the filled rows of the chunk to the appender and start over.
@return true if the chunk is empty or DuckDB accepts it; false on append
  failure */
bool storage_chunk::append(duckdb_appender app) {
  if (m_size == 0) {
    return (true);
  }

  duckdb_data_chunk_set_size(m_chunk, m_size);

  const bool ok = duckdb_append_data_chunk(app, m_chunk) == DuckDBSuccess;

  reset();

  return (ok);
}

/** Own an appender while filling, flushing and constructing diagnostics. */
class storage_appender_guard final : private ut_noncopyable {
 public:
  /** Appender to destroy. */
  duckdb_appender m_app{nullptr};

  /** Destroy the owned partition appender. */
  ~storage_appender_guard() {
    static_cast<void>(duckdb_appender_destroy(&m_app));
  }
};

/** Copy an appender failure before its guard releases the handle.
@return the classified DuckDB failure */
static db_status_t storage_appender_error(duckdb_appender app) {
  const char *message = app != nullptr ? duckdb_appender_error(app) : nullptr;
  return (duckdb_error(message));
}

/** Phase 2 for one partition: append the rows of all its slices.
@return DB_SUCCESS, DB_OUT_OF_MEMORY if chunk creation fails, or a classified
  DuckDB appender failure */
db_status_t storage::flush_context::append_partition(
    storage_partition_t &p, const schema_t &full,
    const std::vector<storage_slice_t> &slices) {
  ut_a(p.m_n_cols <= full.m_cols.size());

  storage_chunk chunk;

  if (const db_status_t err = chunk.init(full, p.m_n_cols); !err.ok()) {
    return (db_status_t(err.code(), "cannot create a data chunk"));
  }

  storage_appender_guard owner;
  duckdb_appender &app = owner.m_app;
  if (duckdb_appender_create(m_session.connection(), nullptr,
                             p.m_phys_name.c_str(), &app) != DuckDBSuccess) {
    return (storage_appender_error(app));
  }

  uint64_t rows = 0;

  for (size_t s = 0; s < slices.size(); ++s) {
    const storage_slice_t &slice = slices[s];

    for (size_t i = 0; i < slice.m_rows.size(); ++i) {
      chunk.add_row(slice.m_write->m_seq, *slice.m_write->m_batch,
                    slice.m_rows[i]);

      if (chunk.full()) {
        if (!chunk.append(app)) {
          return (storage_appender_error(app));
        }
      }
    }

    rows += slice.m_rows.size();
  }

  if (!chunk.append(app)) {
    return (storage_appender_error(app));
  }

  /* Appended data is buffered; flushing it checks the rows and hands them
  to the transaction. Its result must be checked like any other. */
  if (duckdb_appender_flush(app) != DuckDBSuccess) {
    return (storage_appender_error(app));
  }

  if (duckdb_appender_close(app) != DuckDBSuccess) {
    return (storage_appender_error(app));
  }

  p.m_rows += rows;

  return (DB_SUCCESS);
}

/** Write the final width and row count of the touched partitions, and F.
@return DB_SUCCESS or a classified DuckDB catalog-update failure */
db_status_t storage::flush_context::write_catalog() {
  storage_append_plan_t::const_iterator it;

  for (it = m_plan.begin(); it != m_plan.end(); ++it) {
    const storage_partition_t &p =
        m_catalog.m_partitions.find(it->first)->second;

    const std::string sql =
        "UPDATE _pt_partitions SET n_cols = " + std::to_string(p.m_n_cols) +
        ", row_count = " + std::to_string(p.m_rows) +
        " WHERE table_id = " + std::to_string(p.m_table_id) +
        " AND first_day = " + std::to_string(p.m_first_day);

    if (const db_status_t status = duckdb_exec(m_session.connection(), sql);
        !status.ok()) {
      return (status);
    }
  }

  const std::string sql =
      "UPDATE _pt_state SET flushed_seq = " +
      std::to_string(m_catalog.m_flushed_seq) +
      ", next_table_id = " + std::to_string(m_catalog.m_next_table_id);

  return (duckdb_exec(m_session.connection(), sql));
}

/** Check the arguments of a flush against the catalog.
@param[in]  writes   consecutive writes starting immediately after the stored
                     sequence number
@param[in]  n        number of writes; must be nonzero
@return true if write sequences, batches and schemas fit the stored boundary */
bool storage::check_flush(const storage_write_t *writes, size_t n) const {
  if (writes == nullptr || n == 0) {
    return (false);
  }

  for (size_t i = 0; i < n; ++i) {
    const storage_write_t &w = writes[i];

    if (w.m_seq != m_catalog.m_flushed_seq + 1 + i) {
      return (false);
    }

    if (w.m_batch == nullptr || w.m_schema == nullptr ||
        w.m_batch->m_n_rows == 0) {
      return (false);
    }

    if (w.m_schema->m_name != w.m_batch->m_table_name ||
        w.m_batch->m_cols.empty() ||
        w.m_schema->m_cols.size() < w.m_batch->m_cols.size()) {
      return (false);
    }

    for (size_t c = 0; c < w.m_batch->m_cols.size(); ++c) {
      if (w.m_batch->m_cols[c].m_col_id != c ||
          w.m_batch->m_cols[c].m_type != w.m_schema->m_cols[c].m_type) {
        return (false);
      }
    }
  }

  return (true);
}

/** Append all partitions after metadata resolution has completed. */
db_status_t storage::flush_context::append_rows() {
  storage_append_plan_t::iterator it;

  for (it = m_plan.begin(); it != m_plan.end(); ++it) {
    /* Phase 1 created both. */
    std::map<storage_key_t, storage_partition_t>::iterator part =
        m_catalog.m_partitions.find(it->first);
    const auto schema = m_schemas.find(it->first.first);
    ut_a(part != m_catalog.m_partitions.end() && schema != m_schemas.end());
    const schema_t *full = schema->second;

    storage_partition_t &p = part->second;

    if (const db_status_t err = append_partition(p, *full, it->second);
        !err.ok()) {
      return (err);
    }
  }

  return (DB_SUCCESS);
}

/** Store writes inside the transaction, on a copy of the catalog.
@param[in]  writes  consecutive writes starting immediately after the
                    stored sequence number
@return DB_SUCCESS, DB_CORRUPTION for a changed stored boundary, or a
  catalog/appender error; the caller handles rollback */
db_status_t storage::flush_low(const storage_write_t *writes, size_t n,
                               storage_catalog_t &next) {
  /* The file must still be where this handle left it. */
  duckdb_rows_t rows;
  if (const db_status_t status = duckdb_exec(
          m_duckdb->connection(), "SELECT flushed_seq FROM _pt_state", rows);
      !status.ok()) {
    return (status);
  }

  if (rows.size() != 1 || rows[0].size() != 1 || rows[0][0].m_is_null) {
    return (DB_CORRUPTION);
  }

  if (static_cast<batch_seq_t>(rows[0][0].m_i) != m_catalog.m_flushed_seq) {
    return (db_status_t(DB_CORRUPTION,
                        "flushed_seq in the file differs from the open state"));
  }

  /* Phase 1: tables, column directories and partitions. */
  flush_context context(m_duckdb->session(), next);

  for (size_t i = 0; i < n; ++i) {
    if (const db_status_t err = context.resolve_write(writes[i]); !err.ok()) {
      return (err);
    }
  }

  /* Phase 2: the rows, one appender per partition. */
  if (const db_status_t appended = context.append_rows(); !appended.ok()) {
    return (appended);
  }

#ifdef PT_TESTING
  if (m_test_fail_next_flush || m_test_fail_flush) {
    m_test_fail_next_flush = false;
    return (db_status_t(DB_ERROR, "injected flush failure"));
  }
#endif /* PT_TESTING */

  /* The arguments were checked: consecutive writes right after F. */
  next.m_flushed_seq = writes[n - 1].m_seq;
  ut_a(next.m_flushed_seq == m_catalog.m_flushed_seq + n);

  return (context.write_catalog());
}

/** Store consecutive writes and publish their catalog after commit. */
db_status_t storage::flush(const storage_write_t *writes, size_t n) {
  if (!check_flush(writes, n)) {
    return (
        db_status_t(DB_INVALID_ARGUMENT,
                    "writes do not continue from the flushed sequence number"));
  }

  if (const db_status_t err = m_duckdb->session().begin(); !err.ok()) {
    return (err);
  }

  storage_catalog_t next = m_catalog;
  if (const db_status_t err = flush_low(writes, n, next); !err.ok()) {
    return (m_duckdb->session().rollback(err));
  }

  /* Publish only after COMMIT confirms the transaction. Failed operations
  leave the committed catalog unchanged, including uncertain outcomes. */
  if (const db_status_t err = m_duckdb->session().commit(); !err.ok()) {
    return (err);
  }

  std::swap(m_catalog, next);

  return (DB_SUCCESS);
}
