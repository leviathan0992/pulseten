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
 *   1. Catalog: for every write, in order, add its table and schema
 *      version if new, and create or widen the partitions its rows fall
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

/** Find the table of a write, adding it to the catalog and _pt_tables if it is
new.
@param[in]  name  canonical logical table name
@return DB_SUCCESS, DB_LIMIT_EXCEEDED if table ids are exhausted, or a
  catalog insertion error */
static db_status_t storage_flush_ensure_table(duckdb_database_t &database,
                                              storage_catalog_t &catalog,
                                              const std::string &name,
                                              storage_table_t *&out) {
  db_status_t status;

  std::map<std::string, storage_table_t>::iterator it =
      catalog.m_tables.find(name);

  if (it != catalog.m_tables.end()) {
    out = &it->second;

    return (DB_SUCCESS);
  }

  if (catalog.m_next_table_id == std::numeric_limits<uint64_t>::max()) {
    return (db_status_t(DB_LIMIT_EXCEEDED, "table ids are exhausted"));
  }

  storage_table_t table;
  table.m_table_id = catalog.m_next_table_id++;

  const std::string sql = "INSERT INTO _pt_tables VALUES (" +
                          std::to_string(table.m_table_id) + ", " +
                          duckdb_literal(name) + ")";

  status = duckdb_database_exec(database, sql);
  if (!status.ok()) {
    return (status);
  }

  out = &catalog.m_tables.insert(std::make_pair(name, table)).first->second;

  return (DB_SUCCESS);
}

/** Store the schema version of a write if it is newer than the stored ones. A
newer version is always the next one: the write that added it comes before
every write that uses it.
@param[in]  schema  write schema to store if newer
@return DB_SUCCESS, DB_INVALID_ARGUMENT for a skipped schema version, or
  DB_ERROR on a catalog statement failure */
static db_status_t storage_flush_ensure_schema(duckdb_database_t &database,
                                               storage_table_t &table,
                                               const schema_t &schema) {
  db_status_t status;

  const uint64_t stored = table.m_versions.size();

  /* Most writes follow a version that is already stored. */
  if (schema.m_version <= stored) {
    return (DB_SUCCESS);
  }

  if (schema.m_version != stored + 1) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "schema version " + std::to_string(schema.m_version) +
                            " follows stored version " +
                            std::to_string(stored)));
  }

  const size_t first_new =
      stored == 0 ? 0 : table.m_versions.back().m_cols.size();

  for (size_t i = first_new; i < schema.m_cols.size(); ++i) {
    const col_def_t &c = schema.m_cols[i];

    const std::string sql =
        "INSERT INTO _pt_columns VALUES (" + std::to_string(table.m_table_id) +
        ", " + std::to_string(c.m_id) + ", " + duckdb_literal(c.m_name) + ", " +
        std::to_string(static_cast<int>(c.m_kind)) + ", " +
        std::to_string(static_cast<int>(c.m_type)) + ", " +
        std::to_string(schema.m_version) + ")";

    status = duckdb_database_exec(database, sql);
    if (!status.ok()) {
      return (status);
    }
  }

  table.m_versions.push_back(schema);

  return (DB_SUCCESS);
}

/** Find a partition, creating its physical table if it is new.
@param[in]  first_day  first UTC day of the partition, relative to
                       1970-01-01
@return DB_SUCCESS or DB_ERROR if physical-table or catalog creation fails */
static db_status_t storage_flush_ensure_partition(
    duckdb_database_t &database, storage_catalog_t &catalog, uint64_t table_id,
    int64_t first_day, const schema_t &schema, storage_partition_t *&out) {
  db_status_t status;

  const storage_key_t key(table_id, first_day);

  std::map<storage_key_t, storage_partition_t>::iterator it =
      catalog.m_partitions.find(key);

  if (it != catalog.m_partitions.end()) {
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

  const int64_t end_day = first_day + catalog.m_partition_duration_days;

  sql += ", CHECK (c0 >= TIMESTAMP '" + ut_day_to_date(first_day) +
         "' AND c0 < TIMESTAMP '" + ut_day_to_date(end_day) + "'))";

  status = duckdb_database_exec(database, sql);
  if (!status.ok()) {
    return (status);
  }

  sql = "INSERT INTO _pt_partitions VALUES (" + std::to_string(table_id) +
        ", " + std::to_string(first_day) + ", " +
        duckdb_literal(p.m_phys_name) + ", " + std::to_string(p.m_n_cols) +
        ", 0)";

  status = duckdb_database_exec(database, sql);
  if (!status.ok()) {
    return (status);
  }

  out = &catalog.m_partitions.insert(std::make_pair(key, p)).first->second;

  return (DB_SUCCESS);
}

/** Add to a partition the columns a write has and it lacks.
@param[in]  schema  write schema whose missing columns must be added
@return DB_SUCCESS or DB_ERROR if adding a column fails */
static db_status_t storage_flush_ensure_columns(duckdb_database_t &database,
                                                storage_partition_t &p,
                                                const schema_t &schema) {
  db_status_t status;

  while (p.m_n_cols < schema.m_cols.size()) {
    const col_def_t &c = schema.m_cols[p.m_n_cols];

    const std::string sql = "ALTER TABLE " + p.m_phys_name + " ADD COLUMN c" +
                            std::to_string(c.m_id) + " " +
                            schema_type_name(c.m_type);

    status = duckdb_database_exec(database, sql);
    if (!status.ok()) {
      return (status);
    }

    p.m_n_cols++;
  }

  return (DB_SUCCESS);
}

/** Phase 1 for one write: its table, schema version and partitions. Adds the
write's rows, grouped by partition, to the append plan.
@return DB_SUCCESS or the table/schema/partition catalog error */
static db_status_t storage_flush_resolve_write(duckdb_database_t &database,
                                               storage_catalog_t &catalog,
                                               const storage_write_t &w,
                                               storage_append_plan_t &plan) {
  storage_table_t *table = nullptr;

  db_status_t err =
      storage_flush_ensure_table(database, catalog, w.m_batch->m_table_name, table);
  if (!err.ok()) {
    return (err);
  }

  err = storage_flush_ensure_schema(database, *table, *w.m_schema);
  if (!err.ok()) {
    return (err);
  }

  /* Group the rows by partition, keeping their order. */
  std::map<int64_t, std::vector<uint32_t>> parts;
  const batch_col_t &time_col = w.m_batch->m_cols[0];

  for (uint32_t r = 0; r < w.m_batch->m_n_rows; ++r) {
    const int64_t day = ut_utc_day(time_col.m_ints[r]);

    parts[storage_partition_first_day(day, catalog.m_partition_duration_days)].push_back(
        r);
  }

  std::map<int64_t, std::vector<uint32_t>>::iterator it;

  for (it = parts.begin(); it != parts.end(); ++it) {
    storage_partition_t *p = nullptr;

    err = storage_flush_ensure_partition(database, catalog, table->m_table_id,
                                         it->first, *w.m_schema, p);
    if (!err.ok()) {
      return (err);
    }

    err = storage_flush_ensure_columns(database, *p, *w.m_schema);
    if (!err.ok()) {
      return (err);
    }

    storage_slice_t slice;
    slice.m_write = &w;
    slice.m_rows.swap(it->second);

    plan[storage_key_t(table->m_table_id, it->first)].push_back(slice);
  }

  return (DB_SUCCESS);
}

/** A data chunk being filled for one partition: _pt_seq, _pt_row, then
c0 .. c<n_cols - 1>. */
struct storage_chunk_t {
  /** The chunk, reused for every batch of rows. */
  duckdb_data_chunk m_chunk{nullptr};

  /** Destroy the data chunk and its vector storage. */
  ~storage_chunk_t() { duckdb_destroy_data_chunk(&m_chunk); }

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
@param[in,out]  chunk  chunk to reset and refresh vector buffers */
static void storage_chunk_reset(storage_chunk_t &chunk) {
  duckdb_data_chunk_reset(chunk.m_chunk);

  for (size_t i = 0; i < chunk.m_vectors.size(); ++i) {
    duckdb_vector v = duckdb_data_chunk_get_vector(chunk.m_chunk, i);

    /* Every value starts valid; NULLs are marked one by one. */
    duckdb_vector_ensure_validity_writable(v);

    chunk.m_vectors[i] = v;
    chunk.m_data[i] = duckdb_vector_get_data(v);
    chunk.m_validity[i] = duckdb_vector_get_validity(v);
  }

  chunk.m_size = 0;
}

/** Destroy the temporary logical types on every return. */
struct storage_logical_types_t {
  /** Types to destroy. */
  std::vector<duckdb_logical_type> m_types;

  /** Destroy every temporary logical type. */
  ~storage_logical_types_t() {
    for (size_t i = 0; i < m_types.size(); ++i) {
      duckdb_destroy_logical_type(&m_types[i]);
    }
  }
};

/** Create the chunk for a partition's columns.
@return DB_SUCCESS or DB_OUT_OF_MEMORY if DuckDB cannot create the chunk */
static db_status_t storage_chunk_create(storage_chunk_t &chunk,
                                        const schema_t &full, uint32_t n_cols) {
  const size_t n = 2 + static_cast<size_t>(n_cols);

  chunk.m_types.resize(n_cols);
  chunk.m_vectors.resize(n);
  chunk.m_data.resize(n);
  chunk.m_validity.resize(n);

  {
    storage_logical_types_t logical;
    logical.m_types.resize(n);
    logical.m_types[0] = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    logical.m_types[1] = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER);

    for (uint32_t k = 0; k < n_cols; ++k) {
      chunk.m_types[k] = full.m_cols[k].m_type;
      logical.m_types[2 + k] =
          duckdb_create_logical_type(duckdb_type_from_col(chunk.m_types[k]));
    }

    chunk.m_chunk = duckdb_create_data_chunk(logical.m_types.data(), n);
  }

  if (chunk.m_chunk == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  chunk.m_capacity = duckdb_vector_size();
  storage_chunk_reset(chunk);

  return (DB_SUCCESS);
}

/** Copy row r of a batch into the next row of the chunk. Columns the batch
does not have, because they were added later, are NULL. */
static void storage_chunk_add_row(storage_chunk_t &chunk, batch_seq_t seq,
                                  const batch_t &batch, uint32_t r) {
  ut_ad(chunk.m_size < chunk.m_capacity);

  const idx_t i = chunk.m_size;

  static_cast<uint64_t *>(chunk.m_data[0])[i] = seq;
  static_cast<uint32_t *>(chunk.m_data[1])[i] = r;

  for (size_t k = 0; k < chunk.m_types.size(); ++k) {
    const size_t dst = 2 + k;

    /* An older batch lacks the columns added after it. */
    if (k >= batch.m_cols.size() || batch.m_cols[k].m_nulls[r]) {
      duckdb_validity_set_row_invalid(chunk.m_validity[dst], i);
      continue;
    }

    const batch_col_t &col = batch.m_cols[k];

    switch (chunk.m_types[k]) {
      case col_type_t::BOOLEAN:
        static_cast<bool *>(chunk.m_data[dst])[i] = col.m_bools[r] != 0;
        break;

      case col_type_t::BIGINT:
        static_cast<int64_t *>(chunk.m_data[dst])[i] = col.m_ints[r];
        break;

      case col_type_t::DOUBLE:
        static_cast<double *>(chunk.m_data[dst])[i] = col.m_doubles[r];
        break;

      case col_type_t::TIMESTAMP:
        static_cast<duckdb_timestamp *>(chunk.m_data[dst])[i].micros =
            col.m_ints[r];
        break;

      case col_type_t::VARCHAR: {
        const uint32_t start = col.m_offsets[r];
        const uint32_t len = col.m_offsets[r + 1] - start;

        duckdb_vector_assign_string_element_len(
            chunk.m_vectors[dst], i, col.m_chars.data() + start, len);
        break;
      }
    }
  }

  chunk.m_size++;
}

/** Hand the filled rows of the chunk to the appender and start over.
@param[in,out]  chunk  filled chunk to append and reset
@return true if the chunk is empty or DuckDB accepts it; false on append
  failure */
static bool storage_chunk_append(storage_chunk_t &chunk, duckdb_appender app) {
  if (chunk.m_size == 0) {
    return (true);
  }

  duckdb_data_chunk_set_size(chunk.m_chunk, chunk.m_size);

  const bool ok = duckdb_append_data_chunk(app, chunk.m_chunk) == DuckDBSuccess;

  storage_chunk_reset(chunk);

  return (ok);
}

/** Own an appender while filling, flushing and constructing diagnostics. */
struct storage_appender_guard_t {
  /** Appender to destroy. */
  duckdb_appender m_app{nullptr};

  /** Destroy the owned partition appender. */
  ~storage_appender_guard_t() {
    static_cast<void>(duckdb_appender_destroy(&m_app));
  }
};

/** Copy an appender failure before its guard releases the handle.
@return DB_ERROR */
static db_status_t storage_appender_error(duckdb_appender app) {
  const char *message = app != nullptr ? duckdb_appender_error(app) : nullptr;
  return (
      db_status_t(DB_ERROR, message != nullptr ? message : "appender failed"));
}

/** Phase 2 for one partition: append the rows of all its slices.
@return DB_SUCCESS, DB_OUT_OF_MEMORY if chunk creation fails, or DB_ERROR if
  the appender fails */
static db_status_t storage_flush_append(
    duckdb_database_t &database, storage_partition_t &p, const schema_t &full,
    const std::vector<storage_slice_t> &slices) {
  ut_a(p.m_n_cols <= full.m_cols.size());

  storage_chunk_t chunk;

  db_status_t err = storage_chunk_create(chunk, full, p.m_n_cols);
  if (!err.ok()) {
    return (db_status_t(err.code(), "cannot create a data chunk"));
  }

  storage_appender_guard_t owner;
  duckdb_appender &app = owner.m_app;
  if (duckdb_appender_create(database.m_conn, nullptr, p.m_phys_name.c_str(),
                             &app) != DuckDBSuccess) {
    return (storage_appender_error(app));
  }

  uint64_t rows = 0;

  for (size_t s = 0; s < slices.size(); ++s) {
    const storage_slice_t &slice = slices[s];

    for (size_t i = 0; i < slice.m_rows.size(); ++i) {
      storage_chunk_add_row(chunk, slice.m_write->m_seq,
                            *slice.m_write->m_batch, slice.m_rows[i]);

      if (chunk.m_size == chunk.m_capacity) {
        if (!storage_chunk_append(chunk, app)) {
          return (storage_appender_error(app));
        }
      }
    }

    rows += slice.m_rows.size();
  }

  if (!storage_chunk_append(chunk, app)) {
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
@return DB_SUCCESS or DB_ERROR if a catalog update fails */
static db_status_t storage_flush_write_catalog(
    duckdb_database_t &database, const storage_catalog_t &catalog,
    const storage_append_plan_t &plan) {
  db_status_t status;

  storage_append_plan_t::const_iterator it;

  for (it = plan.begin(); it != plan.end(); ++it) {
    const storage_partition_t &p = catalog.m_partitions.find(it->first)->second;

    const std::string sql =
        "UPDATE _pt_partitions SET n_cols = " + std::to_string(p.m_n_cols) +
        ", row_count = " + std::to_string(p.m_rows) +
        " WHERE table_id = " + std::to_string(p.m_table_id) +
        " AND first_day = " + std::to_string(p.m_first_day);

    status = duckdb_database_exec(database, sql);
    if (!status.ok()) {
      return (status);
    }
  }

  const std::string sql =
      "UPDATE _pt_state SET flushed_seq = " +
      std::to_string(catalog.m_flushed_seq) +
      ", next_table_id = " + std::to_string(catalog.m_next_table_id);

  status = duckdb_database_exec(database, sql);
  if (!status.ok()) {
    return (status);
  }

  return (DB_SUCCESS);
}

/** Check the arguments of a flush against the catalog.
@param[in]  catalog  stored sequence boundary the writes must continue
@param[in]  writes   consecutive writes starting immediately after the stored
                     sequence number
@param[in]  n        number of writes; must be nonzero
@return true if write sequences, batches and schemas fit the stored boundary */
static bool storage_flush_validate(const storage_catalog_t &catalog,
                                   const storage_write_t *writes, size_t n) {
  if (writes == nullptr || n == 0) {
    return (false);
  }

  for (size_t i = 0; i < n; ++i) {
    const storage_write_t &w = writes[i];

    if (w.m_seq != catalog.m_flushed_seq + 1 + i) {
      return (false);
    }

    if (w.m_batch == nullptr || w.m_schema == nullptr ||
        w.m_batch->m_n_rows == 0) {
      return (false);
    }

    if (w.m_schema->m_name != w.m_batch->m_table_name ||
        w.m_schema->m_version != w.m_batch->m_schema_version ||
        w.m_schema->m_cols.size() != w.m_batch->m_cols.size()) {
      return (false);
    }
  }

  return (true);
}

/** Find a stored table's newest schema by id.
@return the newest stored schema of a table, by id */
static const schema_t *storage_newest_schema(const storage_catalog_t &catalog,
                                             uint64_t table_id) {
  std::map<std::string, storage_table_t>::const_iterator it;

  for (it = catalog.m_tables.begin(); it != catalog.m_tables.end(); ++it) {
    if (it->second.m_table_id == table_id) {
      return (&it->second.m_versions.back());
    }
  }

  return (nullptr);
}

/** Store writes inside the transaction, on a copy of the catalog.
@param[in]  writes  consecutive writes starting immediately after the
                    stored sequence number
@return DB_SUCCESS, DB_CORRUPTION for a changed stored boundary, or a
  catalog/appender error; the caller handles rollback */
static db_status_t storage_flush_low(storage_t &st,
                                     const storage_write_t *writes, size_t n,
                                     storage_catalog_t &next) {
  db_status_t status;

  /* The file must still be where this handle left it. */
  int64_t stored_seq = 0;

  status =
      storage_query_int(st, "SELECT flushed_seq FROM _pt_state", stored_seq);
  if (!status.ok()) {
    return (status);
  }

  if (static_cast<batch_seq_t>(stored_seq) != st.m_catalog.m_flushed_seq) {
    return (db_status_t(DB_CORRUPTION,
                        "flushed_seq in the file differs from the open state"));
  }

  /* Phase 1: tables, schema versions and partitions. */
  storage_append_plan_t plan;

  for (size_t i = 0; i < n; ++i) {
    const db_status_t err =
        storage_flush_resolve_write(*st.m_duckdb, next, writes[i], plan);
    if (!err.ok()) {
      return (err);
    }
  }

  /* Phase 2: the rows, one appender per partition. */
  storage_append_plan_t::iterator it;

  for (it = plan.begin(); it != plan.end(); ++it) {
    /* Phase 1 created both. */
    std::map<storage_key_t, storage_partition_t>::iterator part =
        next.m_partitions.find(it->first);
    const schema_t *full = storage_newest_schema(next, it->first.first);
    ut_a(part != next.m_partitions.end() && full != nullptr);

    storage_partition_t &p = part->second;

    const db_status_t err =
        storage_flush_append(*st.m_duckdb, p, *full, it->second);
    if (!err.ok()) {
      return (err);
    }
  }

  if (st.m_dbg_fail_next_flush || st.m_dbg_fail_flush) {
    st.m_dbg_fail_next_flush = false;
    return (db_status_t(DB_ERROR, "injected flush failure"));
  }

  /* The arguments were checked: consecutive writes right after F. */
  next.m_flushed_seq = writes[n - 1].m_seq;
  ut_a(next.m_flushed_seq == st.m_catalog.m_flushed_seq + n);

  return (storage_flush_write_catalog(*st.m_duckdb, next, plan));
}

/** Store writes F + 1 .. F + n, leaving the transaction open. */
db_status_t storage_flush_prepare(storage_t &st, const storage_write_t *writes,
                                  size_t n) {
  ut_a(!st.m_prepared);

  if (!storage_flush_validate(st.m_catalog, writes, n)) {
    return (
        db_status_t(DB_INVALID_ARGUMENT,
                    "writes do not continue from the flushed sequence number"));
  }

  db_status_t err = duckdb_database_exec(*st.m_duckdb, "BEGIN TRANSACTION");
  if (!err.ok()) {
    return (err);
  }

  storage_catalog_t next = st.m_catalog;
  err = storage_flush_low(st, writes, n, next);
  if (!err.ok()) {
    return (duckdb_database_rollback(*st.m_duckdb, err));
  }

  /* Hold the new catalog until the transaction commits. */
  std::swap(st.m_pending, next);
  st.m_prepared = true;

  return (DB_SUCCESS);
}

/** Commit the prepared flush and publish its catalog. */
db_status_t storage_flush_commit(storage_t &st) {
  ut_a(st.m_prepared);
  ut_a(st.m_pending.m_flushed_seq > st.m_catalog.m_flushed_seq);
  st.m_prepared = false;

  const db_status_t err = duckdb_database_commit(*st.m_duckdb);
  if (!err.ok()) {
    return (err);
  }

  std::swap(st.m_catalog, st.m_pending);
  st.m_pending = storage_catalog_t();

  return (DB_SUCCESS);
}
