/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * storage0flush.cc
 *    Storing a run of writes in one DuckDB transaction.
 *
 * A flush works on a copy of the state, in two phases inside one
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
 * commits. Only after the commit succeeds does the copy replace the state.
 *
 *-------------------------------------------------------------------------
 */

#include "storage0priv.h"

#include <map>
#include <new>
#include <set>

#include "ut0time.h"

/** The rows of one write that fall in one partition, in batch order. */
struct storage_slice_t {
  /** The write. */
  const storage_write_t *write{nullptr};

  /** Indexes of its rows that fall in the partition. */
  std::vector<uint32_t> rows;
};

/** What a flush appends, by partition; slices in sequence order. */
typedef std::map<storage_key_t, std::vector<storage_slice_t>>
    storage_append_plan_t;

/** Record a DuckDB failure.
@param[in,out]  duck   DuckDB database and connection
@param[out]     error  copy of duck.error
@return DB_ERROR */
static dberr_t storage_duck_error(duck_t &duck, std::string &error) {
  error = duck.error;

  return (DB_ERROR);
}

/** Find the table of a write, adding it to the state and _pt_tables if it is
new.
@param[in,out]  duck   DuckDB database and connection
@param[in,out]  state  prospective stored catalog and next table id
@param[in]      name   canonical logical table name
@param[out]     out    table entry in state
@param[out]     error  reason for failure
@return DB_SUCCESS or DB_ERROR if catalog insertion fails */
static dberr_t storage_flush_table(duck_t &duck, storage_state_t &state,
                                   const std::string &name,
                                   storage_table_t **out, std::string &error) {
  std::map<std::string, storage_table_t>::iterator it = state.tables.find(name);

  if (it != state.tables.end()) {
    *out = &it->second;

    return (DB_SUCCESS);
  }

  storage_table_t table;
  table.table_id = state.next_table_id++;

  const std::string sql = "INSERT INTO _pt_tables VALUES (" +
                          std::to_string(table.table_id) + ", " +
                          duck_literal(name) + ")";

  if (duck_exec(duck, sql) != DB_SUCCESS) {
    return (storage_duck_error(duck, error));
  }

  *out = &state.tables.insert(std::make_pair(name, table)).first->second;

  return (DB_SUCCESS);
}

/** Store the schema version of a write if it is newer than the stored ones. A
newer version is always the next one: the write that added it comes before
every write that uses it.
@param[in,out]  duck    DuckDB database and connection
@param[in,out]  table   stored schema history to extend
@param[in]      schema  write schema to store if newer
@param[out]     error   reason for failure
@return DB_SUCCESS, DB_INVALID_ARGUMENT for a skipped schema version, or
  DB_ERROR on a catalog statement failure */
static dberr_t storage_flush_schema(duck_t &duck, storage_table_t &table,
                                    const schema_t &schema,
                                    std::string &error) {
  const uint64_t stored = table.versions.size();

  if (schema.version <= stored) {
    return (DB_SUCCESS);
  }

  if (schema.version != stored + 1) {
    error = "schema version " + std::to_string(schema.version) +
            " follows stored version " + std::to_string(stored);

    return (DB_INVALID_ARGUMENT);
  }

  const size_t first_new = stored == 0 ? 0 : table.versions.back().cols.size();

  for (size_t i = first_new; i < schema.cols.size(); ++i) {
    const col_def_t &c = schema.cols[i];

    const std::string sql =
        "INSERT INTO _pt_columns VALUES (" + std::to_string(table.table_id) +
        ", " + std::to_string(c.id) + ", " + duck_literal(c.name) + ", " +
        std::to_string((int)c.role) + ", " + std::to_string((int)c.type) +
        ", " + std::to_string(schema.version) + ")";

    if (duck_exec(duck, sql) != DB_SUCCESS) {
      return (storage_duck_error(duck, error));
    }
  }

  table.versions.push_back(schema);

  return (DB_SUCCESS);
}

/** Find a partition, creating its physical table if it is new.
@param[in,out]  duck       DuckDB database and connection
@param[in,out]  state      prospective partition catalog
@param[in]      table_id   stored logical table id
@param[in]      first_day  first UTC day of the partition, relative to
                           1970-01-01
@param[in]      schema     schema defining the initial physical columns
@param[out]     out        existing or newly created partition entry
@param[out]     error      reason for failure
@return DB_SUCCESS or DB_ERROR if physical-table or catalog creation fails */
static dberr_t storage_flush_partition(duck_t &duck, storage_state_t &state,
                                       uint64_t table_id, int64_t first_day,
                                       const schema_t &schema,
                                       storage_partition_t **out,
                                       std::string &error) {
  const storage_key_t key(table_id, first_day);

  std::map<storage_key_t, storage_partition_t>::iterator it =
      state.partitions.find(key);

  if (it != state.partitions.end()) {
    *out = &it->second;

    return (DB_SUCCESS);
  }

  storage_partition_t p;
  p.table_id = table_id;
  p.first_day = first_day;
  p.phys_name = storage_partition_name(table_id, first_day);
  p.n_cols = (uint32_t)schema.cols.size();

  /* Row identity, then the columns by id. The time column must lie in
  the partition's days. */
  std::string sql = "CREATE TABLE " + p.phys_name +
                    " (_pt_seq UBIGINT NOT NULL, _pt_row UINTEGER NOT NULL";

  for (uint32_t c = 0; c < p.n_cols; ++c) {
    sql +=
        ", c" + std::to_string(c) + " " + schema_type_name(schema.cols[c].type);
  }

  const int64_t end_day = first_day + state.partition_days;

  sql += ", CHECK (c0 >= TIMESTAMP '" + ut_day_to_date(first_day) +
         "' AND c0 < TIMESTAMP '" + ut_day_to_date(end_day) + "'))";

  if (duck_exec(duck, sql) != DB_SUCCESS) {
    return (storage_duck_error(duck, error));
  }

  sql = "INSERT INTO _pt_partitions VALUES (" + std::to_string(table_id) +
        ", " + std::to_string(first_day) + ", " + duck_literal(p.phys_name) +
        ", " + std::to_string(p.n_cols) + ", 0)";

  if (duck_exec(duck, sql) != DB_SUCCESS) {
    return (storage_duck_error(duck, error));
  }

  *out = &state.partitions.insert(std::make_pair(key, p)).first->second;

  return (DB_SUCCESS);
}

/** Add to a partition the columns a write has and it lacks.
@param[in,out]  duck    DuckDB database and connection
@param[in,out]  p       physical partition and column count to extend
@param[in]      schema  write schema whose missing columns must be added
@param[out]     error   reason for failure
@return DB_SUCCESS or DB_ERROR if adding a column fails */
static dberr_t storage_flush_widen(duck_t &duck, storage_partition_t &p,
                                   const schema_t &schema, std::string &error) {
  while (p.n_cols < schema.cols.size()) {
    const col_def_t &c = schema.cols[p.n_cols];

    const std::string sql = "ALTER TABLE " + p.phys_name + " ADD COLUMN c" +
                            std::to_string(c.id) + " " +
                            schema_type_name(c.type);

    if (duck_exec(duck, sql) != DB_SUCCESS) {
      return (storage_duck_error(duck, error));
    }

    p.n_cols++;
  }

  return (DB_SUCCESS);
}

/** Phase 1 for one write: its table, schema version and partitions. Adds the
write's rows, grouped by partition, to the append plan.
@param[in,out]  duck   DuckDB database and connection
@param[in,out]  state  prospective catalog and partition row counts
@param[in]      w      write whose schema and time partitions are resolved
@param[in,out]  plan   per-partition batch slices to append
@param[out]     error  reason for failure
@return DB_SUCCESS or the table/schema/partition catalog error */
static dberr_t storage_flush_catalog(duck_t &duck, storage_state_t &state,
                                     const storage_write_t &w,
                                     storage_append_plan_t &plan,
                                     std::string &error) {
  storage_table_t *table = nullptr;

  dberr_t err = storage_flush_table(duck, state, w.batch->table, &table, error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  err = storage_flush_schema(duck, *table, *w.schema, error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  /* Group the rows by partition, keeping their order. */
  std::map<int64_t, std::vector<uint32_t>> parts;
  const batch_col_t &time_col = w.batch->cols[0];

  for (uint32_t r = 0; r < w.batch->n_rows; ++r) {
    const int64_t day = ut_utc_day(time_col.ints[r]);

    parts[storage_partition_first_day(day, state.partition_days)].push_back(r);
  }

  std::map<int64_t, std::vector<uint32_t>>::iterator it;

  for (it = parts.begin(); it != parts.end(); ++it) {
    storage_partition_t *p = nullptr;

    err = storage_flush_partition(duck, state, table->table_id, it->first,
                                  *w.schema, &p, error);
    if (err != DB_SUCCESS) {
      return (err);
    }

    err = storage_flush_widen(duck, *p, *w.schema, error);
    if (err != DB_SUCCESS) {
      return (err);
    }

    storage_slice_t slice;
    slice.write = &w;
    slice.rows.swap(it->second);

    plan[storage_key_t(table->table_id, it->first)].push_back(slice);
  }

  return (DB_SUCCESS);
}

/** A data chunk being filled for one partition: _pt_seq, _pt_row, then
c0 .. c<n_cols - 1>. */
struct storage_chunk_t {
  /** The chunk, reused for every batch of rows. */
  duckdb_data_chunk chunk{nullptr};

  /** Destroy the data chunk and its vector storage. */
  ~storage_chunk_t() { duckdb_destroy_data_chunk(&chunk); }

  /** Column types of the partition, c0 first. */
  std::vector<col_type_t> types;

  /** Rows a chunk can hold. */
  idx_t capacity{0};

  /** Rows in it now. */
  idx_t size{0};

  /** Vector of each chunk column. */
  std::vector<duckdb_vector> vectors;

  /** Data of each chunk column. */
  std::vector<void *> data;

  /** Validity mask of each chunk column. */
  std::vector<uint64_t *> validity;
};

/** Empty the chunk and look up its column buffers again.
@param[in,out]  c  chunk to reset and refresh vector buffers */
static void storage_chunk_start(storage_chunk_t &c) {
  duckdb_data_chunk_reset(c.chunk);

  for (size_t i = 0; i < c.vectors.size(); ++i) {
    duckdb_vector v = duckdb_data_chunk_get_vector(c.chunk, i);

    /* Every value starts valid; NULLs are marked one by one. */
    duckdb_vector_ensure_validity_writable(v);

    c.vectors[i] = v;
    c.data[i] = duckdb_vector_get_data(v);
    c.validity[i] = duckdb_vector_get_validity(v);
  }

  c.size = 0;
}

/** Destroy the temporary logical types on every return. */
struct storage_logical_types_t {
  /** Types to destroy. */
  std::vector<duckdb_logical_type> types;

  /** Destroy every temporary logical type. */
  ~storage_logical_types_t() {
    for (size_t i = 0; i < types.size(); ++i) {
      duckdb_destroy_logical_type(&types[i]);
    }
  }
};

/** Create the chunk for a partition's columns.
@param[out]  c       chunk, types and column buffers to initialize
@param[in]   full    newest schema containing every physical column of the
                     partition
@param[in]   n_cols  number of physical columns, including time
@return DB_SUCCESS or DB_OUT_OF_MEMORY if DuckDB cannot create the chunk */
static dberr_t storage_chunk_create(storage_chunk_t &c, const schema_t &full,
                                    uint32_t n_cols) {
  const size_t n = 2 + (size_t)n_cols;

  c.types.resize(n_cols);
  c.vectors.resize(n);
  c.data.resize(n);
  c.validity.resize(n);

  {
    storage_logical_types_t logical;
    logical.types.resize(n);
    logical.types[0] = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    logical.types[1] = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER);

    for (uint32_t k = 0; k < n_cols; ++k) {
      c.types[k] = full.cols[k].type;
      logical.types[2 + k] = duckdb_create_logical_type(duck_type(c.types[k]));
    }

    c.chunk = duckdb_create_data_chunk(logical.types.data(), n);
  }

  if (c.chunk == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  c.capacity = duckdb_vector_size();
  storage_chunk_start(c);

  return (DB_SUCCESS);
}

/** Copy row r of a batch into the next row of the chunk. Columns the batch
does not have, because they were added later, are NULL.
@param[in,out]  c      chunk receiving its next row
@param[in]      seq    sequence number of the source write
@param[in]      batch  immutable source batch
@param[in]      r      row index in the source batch */
static void storage_chunk_add_row(storage_chunk_t &c, batch_seq_t seq,
                                  const batch_t &batch, uint32_t r) {
  const idx_t i = c.size;

  static_cast<uint64_t *>(c.data[0])[i] = seq;
  static_cast<uint32_t *>(c.data[1])[i] = r;

  for (size_t k = 0; k < c.types.size(); ++k) {
    const size_t dst = 2 + k;

    if (k >= batch.cols.size() || batch.cols[k].nulls[r]) {
      duckdb_validity_set_row_invalid(c.validity[dst], i);
      continue;
    }

    const batch_col_t &col = batch.cols[k];

    switch (c.types[k]) {
      case COL_BOOLEAN:
        static_cast<bool *>(c.data[dst])[i] = col.bools[r] != 0;
        break;

      case COL_BIGINT:
        static_cast<int64_t *>(c.data[dst])[i] = col.ints[r];
        break;

      case COL_DOUBLE:
        static_cast<double *>(c.data[dst])[i] = col.doubles[r];
        break;

      case COL_TIMESTAMP:
        static_cast<duckdb_timestamp *>(c.data[dst])[i].micros = col.ints[r];
        break;

      case COL_VARCHAR: {
        const uint32_t start = col.offsets[r];
        const uint32_t len = col.offsets[r + 1] - start;

        duckdb_vector_assign_string_element_len(c.vectors[dst], i,
                                                col.chars.data() + start, len);
        break;
      }
    }
  }

  c.size++;
}

/** Hand the filled rows of the chunk to the appender and start over.
@param[in,out]  c    filled chunk to append and reset
@param[in,out]  app  partition appender receiving the chunk
@return true if the chunk is empty or DuckDB accepts it; false on append
  failure */
static bool storage_chunk_flush(storage_chunk_t &c, duckdb_appender app) {
  if (c.size == 0) {
    return (true);
  }

  duckdb_data_chunk_set_size(c.chunk, c.size);

  const bool ok = duckdb_append_data_chunk(app, c.chunk) == DuckDBSuccess;

  storage_chunk_start(c);

  return (ok);
}

/** Own an appender while filling, flushing and constructing diagnostics. */
struct storage_appender_guard_t {
  /** Appender to destroy. */
  duckdb_appender app{nullptr};

  /** Destroy the owned partition appender. */
  ~storage_appender_guard_t() { (void)duckdb_appender_destroy(&app); }
};

/** Phase 2 for one partition: append the rows of all its slices.
@param[in,out]  duck    DuckDB database and connection
@param[in,out]  p       target partition whose row count advances on success
@param[in]      full    newest schema containing every physical column of the
                        partition
@param[in]      slices  batch rows to append to this partition, in write order
@param[out]     error   reason for failure
@return DB_SUCCESS, DB_OUT_OF_MEMORY if chunk creation fails, or DB_ERROR if
  the appender fails */
static dberr_t storage_flush_append(duck_t &duck, storage_partition_t &p,
                                    const schema_t &full,
                                    const std::vector<storage_slice_t> &slices,
                                    std::string &error) {
  storage_chunk_t c;

  dberr_t err = storage_chunk_create(c, full, p.n_cols);
  if (err != DB_SUCCESS) {
    error = "cannot create a data chunk";

    return (err);
  }

  storage_appender_guard_t owner;
  duckdb_appender &app = owner.app;
  bool ok = duckdb_appender_create(duck.conn, nullptr, p.phys_name.c_str(),
                                   &app) == DuckDBSuccess;
  uint64_t rows = 0;

  for (size_t s = 0; s < slices.size() && ok; ++s) {
    const storage_slice_t &slice = slices[s];

    for (size_t i = 0; i < slice.rows.size() && ok; ++i) {
      storage_chunk_add_row(c, slice.write->seq, *slice.write->batch,
                            slice.rows[i]);

      if (c.size == c.capacity) {
        ok = storage_chunk_flush(c, app);
      }
    }

    rows += slice.rows.size();
  }

  if (ok) {
    ok = storage_chunk_flush(c, app);
  }

  /* Appended data is buffered; flushing it checks the rows and hands them
  to the transaction. Its result must be checked like any other. */
  if (ok) {
    ok = duckdb_appender_flush(app) == DuckDBSuccess;
  }

  if (ok) {
    ok = duckdb_appender_close(app) == DuckDBSuccess;
  }

  if (!ok) {
    const char *message = app != nullptr ? duckdb_appender_error(app) : nullptr;
    error = message != nullptr ? message : "appender failed";
  }

  if (!ok) {
    return (DB_ERROR);
  }

  p.rows += rows;

  return (DB_SUCCESS);
}

/** Write the final width and row count of the touched partitions, and F.
@param[in,out]  duck   DuckDB database and connection
@param[in]      state  prospective committed boundaries, column widths and row
                       counts
@param[in]      plan   touched partition keys to update
@param[out]     error  reason for failure
@return DB_SUCCESS or DB_ERROR if a catalog update fails */
static dberr_t storage_flush_finish(duck_t &duck, const storage_state_t &state,
                                    const storage_append_plan_t &plan,
                                    std::string &error) {
  storage_append_plan_t::const_iterator it;

  for (it = plan.begin(); it != plan.end(); ++it) {
    const storage_partition_t &p = state.partitions.find(it->first)->second;

    const std::string sql =
        "UPDATE _pt_partitions SET n_cols = " + std::to_string(p.n_cols) +
        ", row_count = " + std::to_string(p.rows) +
        " WHERE table_id = " + std::to_string(p.table_id) +
        " AND first_day = " + std::to_string(p.first_day);

    if (duck_exec(duck, sql) != DB_SUCCESS) {
      return (storage_duck_error(duck, error));
    }
  }

  const std::string sql =
      "UPDATE _pt_state SET flushed_seq = " +
      std::to_string(state.flushed_seq) +
      ", next_table_id = " + std::to_string(state.next_table_id);

  if (duck_exec(duck, sql) != DB_SUCCESS) {
    return (storage_duck_error(duck, error));
  }

  return (DB_SUCCESS);
}

/** Check the arguments of a flush against the state.
@param[in]  state   stored sequence boundary the writes must continue
@param[in]  writes  consecutive writes starting immediately after the stored
                    sequence number
@param[in]  n       number of writes; must be nonzero
@return true if write sequences, batches and schemas fit the stored boundary */
static bool storage_flush_args_ok(const storage_state_t &state,
                                  const storage_write_t *writes, size_t n) {
  if (writes == nullptr || n == 0) {
    return (false);
  }

  for (size_t i = 0; i < n; ++i) {
    const storage_write_t &w = writes[i];

    if (w.seq != state.flushed_seq + 1 + i) {
      return (false);
    }

    if (w.batch == nullptr || w.schema == nullptr || w.batch->n_rows == 0) {
      return (false);
    }

    if (w.schema->name != w.batch->table ||
        w.schema->version != w.batch->schema_version ||
        w.schema->cols.size() != w.batch->cols.size()) {
      return (false);
    }
  }

  return (true);
}

/** Find a stored table's newest schema by id.
@param[in]  state     catalog containing the table
@param[in]  table_id  stored logical table id
@return the newest stored schema of a table, by id */
static const schema_t *storage_newest_schema(const storage_state_t &state,
                                             uint64_t table_id) {
  std::map<std::string, storage_table_t>::const_iterator it;

  for (it = state.tables.begin(); it != state.tables.end(); ++it) {
    if (it->second.table_id == table_id) {
      return (&it->second.versions.back());
    }
  }

  return (nullptr);
}

/** storage_flush() inside the transaction, on a copy of the state.
@param[in,out]  st      analytical storage
@param[in]      writes  consecutive writes starting immediately after the
                        stored sequence number
@param[in]      n       number of writes
@param[in,out]  next    copy of stored state receiving the transaction changes
@param[out]     error   reason for failure
@return DB_SUCCESS, DB_CORRUPTION for a changed stored boundary, or a
  catalog/appender error; the caller handles rollback */
static dberr_t storage_flush_low(storage_t &st, const storage_write_t *writes,
                                 size_t n, storage_state_t &next,
                                 std::string &error) {
  /* The file must still be where this handle left it. */
  int64_t stored_seq = 0;

  if (storage_query_int(st, "SELECT flushed_seq FROM _pt_state", stored_seq) !=
      DB_SUCCESS) {
    return (storage_duck_error(*st.duck, error));
  }

  if ((batch_seq_t)stored_seq != st.state.flushed_seq) {
    error = "flushed_seq in the file differs from the open state";

    return (DB_CORRUPTION);
  }

  /* Phase 1: tables, schema versions and partitions. */
  storage_append_plan_t plan;

  for (size_t i = 0; i < n; ++i) {
    const dberr_t err =
        storage_flush_catalog(*st.duck, next, writes[i], plan, error);
    if (err != DB_SUCCESS) {
      return (err);
    }
  }

  /* Phase 2: the rows, one appender per partition. */
  storage_append_plan_t::iterator it;

  for (it = plan.begin(); it != plan.end(); ++it) {
    storage_partition_t &p = next.partitions.find(it->first)->second;
    const schema_t *full = storage_newest_schema(next, it->first.first);

    const dberr_t err =
        storage_flush_append(*st.duck, p, *full, it->second, error);
    if (err != DB_SUCCESS) {
      return (err);
    }
  }

  if (st.dbg_fail_next_flush) {
    st.dbg_fail_next_flush = false;
    error = "injected flush failure";

    return (DB_ERROR);
  }

  next.flushed_seq = writes[n - 1].seq;

  return (storage_flush_finish(*st.duck, next, plan, error));
}

/** First half of storage_flush(): do all the work of storing writes F + 1 ..
@param[in,out]  st      analytical storage
@param[in]      writes  consecutive writes starting immediately after the
                        stored sequence number
@param[in]      n       number of writes; at least one
@param[out]     error   reason for failure
@return as storage_flush(), except never DB_OUTCOME_UNKNOWN; on failure
  nothing is prepared */
dberr_t storage_flush_prepare(storage_t &st, const storage_write_t *writes,
                              size_t n, std::string &error) {
  error.clear();

  ut_a(!st.prepared);

  if (!storage_flush_args_ok(st.state, writes, n)) {
    error = "writes do not continue from the flushed sequence number";

    return (DB_INVALID_ARGUMENT);
  }

  dberr_t err = duck_exec(*st.duck, "BEGIN TRANSACTION");
  if (err != DB_SUCCESS) {
    return (storage_duck_error(*st.duck, error));
  }

  storage_state_t next = st.state;
  err = storage_flush_low(st, writes, n, next, error);

  if (err != DB_SUCCESS) {
    (void)duck_exec(*st.duck, "ROLLBACK");

    return (err);
  }

  /* Hold the new state until the transaction commits. */
  std::swap(st.pending, next);
  st.prepared = true;

  return (DB_SUCCESS);
}

/** Second half of storage_flush(): commit the prepared flush.
@param[in,out]  st     analytical storage
@param[out]     error  reason for failure
@retval DB_SUCCESS committed; the state now includes the writes
@retval DB_OUTCOME_UNKNOWN the commit failed; reopen to learn F */
dberr_t storage_flush_commit(storage_t &st, std::string &error) {
  error.clear();

  ut_a(st.prepared);
  st.prepared = false;

  const dberr_t err = duck_commit(*st.duck, error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  std::swap(st.state, st.pending);
  st.pending = storage_state_t();

  return (DB_SUCCESS);
}

/** Store writes F + 1 ..
@param[in]   st      storage
@param[in]   writes  the writes, in sequence order starting at F + 1
@param[in]   n       number of writes, at least 1
@param[out]  error   message on failure
@retval DB_SUCCESS committed
@retval DB_INVALID_ARGUMENT the writes do not start at F + 1 or are not
  consecutive; nothing changed
@retval DB_ERROR DuckDB refused a statement; rolled back
@retval DB_OUTCOME_UNKNOWN the commit itself failed; reopen to learn F */
dberr_t storage_flush(storage_t &st, const storage_write_t *writes, size_t n,
                      std::string &error) {
  const dberr_t err = storage_flush_prepare(st, writes, n, error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  return (storage_flush_commit(st, error));
}
