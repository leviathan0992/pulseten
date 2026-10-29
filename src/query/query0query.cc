/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query0query.cc
 *    Running queries: the registry of running queries, the SQL that stands
 *    for a table, and reading results.
 *
 *-------------------------------------------------------------------------
 */

#include "query0priv.h"

#include <atomic>
#include <cstring>
#include <map>
#include <mutex>
#include <new>

#include "duck0duck.h"
#include "ut0new.h"
#include "ut0time.h"

/** A registered query. */
struct query_entry_t {
  /** Connection the query runs on. */
  query_connection_key_t connection{0, 0};

  /** What it sees; owned by its engine cursor. */
  const query_snapshot_t *snap{nullptr};

  /** Times each table may be read at; owned by its query cursor. */
  const query_ranges_t *ranges{nullptr};
};

/** Running queries shared by the replacement and hot scans. */
struct query_registry_t {
  /** Protects every field below; a leaf lock, held without other locks. */
  std::mutex mutex;

  /** Queries indexed by their process-wide token. */
  std::map<uint64_t, query_entry_t> by_token;

  /** Token of the query running on each connection. */
  std::map<query_connection_key_t, uint64_t> by_connection;

  /** Next token to assign. */
  uint64_t next_token{1};
};

/** Get the process-lifetime registry, also during application destructors.
Placement construction in static storage deliberately registers no destructor:
application globals may close cursors after other translation units' globals
have gone. The local static initialization is thread-safe; unregistering each
query still frees its map entries.
@return registry that remains alive until process exit */
static query_registry_t &query_registry() {
  alignas(
      query_registry_t) static unsigned char bytes[sizeof(query_registry_t)];
  static query_registry_t *const registry = new (bytes) query_registry_t;

  return (*registry);
}

/** Register a running query. A connection runs one query at a time.
@param[in]  connection  database instance and DuckDB connection id
@param[in]  snap        what the query sees
@param[in]  ranges      times the query may read from each table
@return the token, never 0 */
static uint64_t query_register(const query_connection_key_t &connection,
                               const query_snapshot_t &snap,
                               const query_ranges_t &ranges) {
  query_registry_t &registry = query_registry();
  std::lock_guard<std::mutex> lock(registry.mutex);

  const uint64_t token = registry.next_token++;

  query_entry_t entry;
  entry.connection = connection;
  entry.snap = &snap;
  entry.ranges = &ranges;

  registry.by_token[token] = entry;

  const bool inserted =
      registry.by_connection.emplace(connection, token).second;
  ut_a(inserted);

  return (token);
}

/** Remove a running query.
@param[in]  token  registration token to remove; an unknown token is ignored */
static void query_unregister(uint64_t token) {
  query_registry_t &registry = query_registry();
  std::lock_guard<std::mutex> lock(registry.mutex);

  std::map<uint64_t, query_entry_t>::iterator it =
      registry.by_token.find(token);
  if (it == registry.by_token.end()) {
    return;
  }

  registry.by_connection.erase(it->second.connection);
  registry.by_token.erase(it);
}

/** Find a running query by its connection.
@param[in]   connection  database instance and DuckDB connection id
@param[out]  token       its token
@param[out]  ranges      its time ranges
@return its snapshot, or NULL */
const query_snapshot_t *query_find_by_connection(
    const query_connection_key_t &connection, uint64_t &token,
    const query_ranges_t **ranges) {
  query_registry_t &registry = query_registry();
  std::lock_guard<std::mutex> lock(registry.mutex);

  std::map<query_connection_key_t, uint64_t>::const_iterator it =
      registry.by_connection.find(connection);
  if (it == registry.by_connection.end()) {
    return (nullptr);
  }

  std::map<uint64_t, query_entry_t>::const_iterator entry =
      registry.by_token.find(it->second);
  if (entry == registry.by_token.end()) {
    return (nullptr);
  }
  token = it->second;
  *ranges = entry->second.ranges;

  return (entry->second.snap);
}

/** Find the snapshot of a registered query.
@param[in]  token  registration token of the query
@return the snapshot of a running query, or NULL */
const query_snapshot_t *query_find_by_token(uint64_t token) {
  query_registry_t &registry = query_registry();
  std::lock_guard<std::mutex> lock(registry.mutex);

  std::map<uint64_t, query_entry_t>::iterator it =
      registry.by_token.find(token);

  return (it != registry.by_token.end() ? it->second.snap : nullptr);
}

/** Quote a canonical table or column name as an SQL identifier.
@param[in]  name  canonical SQL identifier; needs no internal escaping
@return "name" quoted as an SQL identifier; names are canonical identifiers,
  so no character needs escaping */
static std::string query_quote(const std::string &name) {
  return ("\"" + name + "\"");
}

/** Check whether a time partition overlaps an inclusive time range.
@param[in]  part            stored time partition
@param[in]  partition_days  partition span in UTC days
@param[in]  range           inclusive timestamp range to test
@return true if a partition may hold times in a range */
static bool query_partition_overlaps(const storage_partition_t &part,
                                     uint32_t partition_days,
                                     const query_range_t &range) {
  const int64_t first = part.first_day * UT_DAY_US;
  const int64_t last = (part.first_day + partition_days) * UT_DAY_US - 1;

  return (first <= range.hi && last >= range.lo);
}

/** Build the SELECT that stands for a table: its partitions that overlap a
time range, and its memtable batches.
@param[in]  token           token of the query
@param[in]  name            canonical table name
@param[in]  table           the table in the query's snapshot
@param[in]  partition_days  days covered by one partition
@param[in]  range           times the query may read from the table
@param[in]  flushed_seq     logical cold upper bound F; replacement scans
                            always pass their snapshot F explicitly
@return the SQL text */
std::string query_table_sql(uint64_t token, const std::string &name,
                            const query_table_t &table, uint32_t partition_days,
                            const query_range_t &range,
                            batch_seq_t flushed_seq) {
  const std::vector<col_def_t> &cols = table.schema.cols;
  std::string sql;

  /* One branch per partition the range overlaps. Columns the partition
  does not have yet were added after its rows were written: NULL. */
  for (size_t p = 0; p < table.partitions.size(); ++p) {
    const storage_partition_t &part = table.partitions[p];

    if (!query_partition_overlaps(part, partition_days, range)) {
      continue;
    }

    if (!sql.empty()) {
      sql += " UNION ALL ";
    }

    sql += "SELECT ";

    for (size_t c = 0; c < cols.size(); ++c) {
      if (c > 0) {
        sql += ", ";
      }

      if (c < part.n_cols) {
        sql += "c" + std::to_string(c);
      } else {
        sql +=
            "CAST(NULL AS " + std::string(schema_type_name(cols[c].type)) + ")";
      }

      sql += " AS " + query_quote(cols[c].name);
    }

    /* The logical cold boundary is explicit. Never let a physical scan
    include batches also supplied by the retained hot version. */
    sql += " FROM " + part.phys_name +
           " WHERE _pt_seq <= " + std::to_string(flushed_seq);
  }

  /* The memtable batches after F, if any. */
  if (!table.batches.empty()) {
    if (!sql.empty()) {
      sql += " UNION ALL ";
    }

    sql += "SELECT ";

    for (size_t c = 0; c < cols.size(); ++c) {
      sql += (c > 0 ? ", " : "") + query_quote(cols[c].name);
    }

    sql += " FROM " QUERY_HOT_SCAN "(" + std::to_string(token) + ", " +
           duck_literal(name) + ")";
  }

  /* No partition overlaps and nothing is in the memtable: an empty
result with the table's columns. */
  if (sql.empty()) {
    sql = "SELECT ";

    for (size_t c = 0; c < cols.size(); ++c) {
      sql += (c > 0 ? ", " : "") + std::string("CAST(NULL AS ") +
             schema_type_name(cols[c].type) + ") AS " +
             query_quote(cols[c].name);
    }

    sql += " LIMIT 0";
  }

  return (sql);
}

/** Register the replacement scan and the memtable scan with a database.
@param[in]  db  database
@return DB_SUCCESS or DB_ERROR */
dberr_t query_install(duckdb_database db) {
  duckdb_connection conn = nullptr;

  if (duckdb_connect(db, &conn) != DuckDBSuccess) {
    return (DB_ERROR);
  }

  const dberr_t err = query_install_hot_scan(conn);
  duckdb_disconnect(&conn);

  if (err != DB_SUCCESS) {
    return (err);
  }

  query_install_replacement(db);

  return (DB_SUCCESS);
}

/** Check that a statement is a single SELECT reading only allowed names.
@param[in]   conn   a connection, used to parse the statement
@param[in]   sql    statement
@param[out]  error  reason for rejection
@return DB_SUCCESS, or DB_INVALID_ARGUMENT */
dberr_t query_check(duckdb_connection conn, const std::string &sql,
                    std::string &error) {
  error.clear();

  /* DuckDB's C++ parser reports a syntax error by throwing. Parse with
  the C API first, which returns it; a statement it accepts also parses in
  query_check_statement(), with the same grammar and options. */
  duckdb_extracted_statements statements = nullptr;
  const idx_t n = duckdb_extract_statements(conn, sql.c_str(), &statements);
  const char *message = duckdb_extract_statements_error(statements);

  if (n == 0 && message != nullptr) {
    error = message;
  } else if (n != 1) {
    error = "exactly one statement is required";
  }
  duckdb_destroy_extracted(&statements);

  if (!error.empty()) {
    return (DB_INVALID_ARGUMENT);
  }

  return (query_check_statement(sql, error));
}

/** Start the snapshot transaction of a query connection and read F.
@param[in]   conn         a new connection
@param[out]  flushed_seq  F as seen by the transaction
@param[out]  error        DuckDB's message on failure
@return DB_SUCCESS or DB_ERROR */
dberr_t query_begin(duckdb_connection conn, batch_seq_t &flushed_seq,
                    std::string &error) {
  /* Borrow the connection; the cursor still owns it. */
  duck_t duck;
  duck.conn = conn;

  dberr_t err = duck_exec(duck, "BEGIN TRANSACTION");
  if (err != DB_SUCCESS) {
    error = duck.error;

    return (err);
  }

  /* Reading F also fixes the transaction's snapshot here and now. */
  duck_rows_t rows;
  err = duck_query(duck, "SELECT flushed_seq FROM _pt_state", rows);
  if (err != DB_SUCCESS) {
    error = duck.error;

    return (err);
  }

  if (rows.size() != 1 || rows[0][0].is_null) {
    error = "bad _pt_state";

    return (DB_CORRUPTION);
  }

  flushed_seq = (batch_seq_t)rows[0][0].i;

  return (DB_SUCCESS);
}

/** How to read one result column. */
struct query_reader_t {
  /** DuckDB type of the column. */
  duckdb_type type{DUCKDB_TYPE_INVALID};

  /** DECIMAL: the integer type it is stored as. */
  duckdb_type decimal_type{DUCKDB_TYPE_INVALID};

  /** DECIMAL: 10 to the power of its scale. */
  double decimal_divisor{1};
};

/** Map a DuckDB result type to a result column type.
@param[in]   prepared  prepared DuckDB statement
@param[in]   col       zero-based result column index
@param[out]  reader    DuckDB representation and decimal conversion metadata
@param[out]  out       publicly representable column type
@return false if the type cannot be returned */
static bool query_map_type(duckdb_prepared_statement prepared, idx_t col,
                           query_reader_t &reader, col_type_t &out) {
  reader.type = duckdb_prepared_statement_column_type(prepared, col);

  switch (reader.type) {
    case DUCKDB_TYPE_BOOLEAN:
      out = COL_BOOLEAN;

      return (true);

    case DUCKDB_TYPE_TINYINT:
    case DUCKDB_TYPE_SMALLINT:
    case DUCKDB_TYPE_INTEGER:
    case DUCKDB_TYPE_BIGINT:
    case DUCKDB_TYPE_UTINYINT:
    case DUCKDB_TYPE_USMALLINT:
    case DUCKDB_TYPE_UINTEGER:
    case DUCKDB_TYPE_UBIGINT:
    case DUCKDB_TYPE_HUGEINT:
      out = COL_BIGINT;

      return (true);

    case DUCKDB_TYPE_FLOAT:
    case DUCKDB_TYPE_DOUBLE:
      out = COL_DOUBLE;

      return (true);

    case DUCKDB_TYPE_DECIMAL: {
      duckdb_logical_type lt =
          duckdb_prepared_statement_column_logical_type(prepared, col);
      reader.decimal_type = duckdb_decimal_internal_type(lt);

      for (uint8_t s = duckdb_decimal_scale(lt); s > 0; --s) {
        reader.decimal_divisor *= 10;
      }

      duckdb_destroy_logical_type(&lt);
      out = COL_DOUBLE;

      return (true);
    }

    case DUCKDB_TYPE_TIMESTAMP:
    case DUCKDB_TYPE_DATE:
      out = COL_TIMESTAMP;

      return (true);

    case DUCKDB_TYPE_VARCHAR:
      out = COL_VARCHAR;

      return (true);

    default:
      return (false);
  }
}

/** Convert a signed HUGEINT to BIGINT if it fits.
@param[in]   h    signed 128-bit integer to convert
@param[out]  out  signed 64-bit value, if it fits
@return a HUGEINT as BIGINT, or false if it does not fit */
static bool query_hugeint(duckdb_hugeint h, int64_t &out) {
  const bool positive = h.upper == 0 && h.lower <= (uint64_t)INT64_MAX;
  const bool negative = h.upper == -1 && h.lower > (uint64_t)INT64_MAX;

  if (!positive && !negative) {
    return (false);
  }

  out = (int64_t)h.lower;

  return (true);
}

/** Read one value of a result chunk.
@param[in]   reader  column representation and conversion metadata
@param[in]   v       source column vector
@param[in]   row     zero-based row index
@param[out]  out     decoded value and NULL flag
@return false if the value cannot be represented */
static bool query_read_value(const query_reader_t &reader, duckdb_vector v,
                             idx_t row, query_value_t &out) {
  if (!duckdb_validity_row_is_valid(duckdb_vector_get_validity(v), row)) {
    out.is_null = true;

    return (true);
  }

  void *data = duckdb_vector_get_data(v);
  out.is_null = false;

  switch (reader.type) {
    case DUCKDB_TYPE_BOOLEAN:
      out.b = static_cast<bool *>(data)[row];

      return (true);
    case DUCKDB_TYPE_TINYINT:
      out.i = static_cast<int8_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_SMALLINT:
      out.i = static_cast<int16_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_INTEGER:
      out.i = static_cast<int32_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_BIGINT:
      out.i = static_cast<int64_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_UTINYINT:
      out.i = static_cast<uint8_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_USMALLINT:
      out.i = static_cast<uint16_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_UINTEGER:
      out.i = static_cast<uint32_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_UBIGINT: {
      const uint64_t u = static_cast<uint64_t *>(data)[row];
      out.i = (int64_t)u;

      return (u <= (uint64_t)INT64_MAX);
    }

    case DUCKDB_TYPE_HUGEINT:
      return (query_hugeint(static_cast<duckdb_hugeint *>(data)[row], out.i));

    case DUCKDB_TYPE_FLOAT:
      out.d = static_cast<float *>(data)[row];

      return (true);
    case DUCKDB_TYPE_DOUBLE:
      out.d = static_cast<double *>(data)[row];

      return (true);

    case DUCKDB_TYPE_DECIMAL: {
      double unscaled = 0;

      switch (reader.decimal_type) {
        case DUCKDB_TYPE_SMALLINT:
          unscaled = static_cast<int16_t *>(data)[row];
          break;
        case DUCKDB_TYPE_INTEGER:
          unscaled = static_cast<int32_t *>(data)[row];
          break;
        case DUCKDB_TYPE_BIGINT:
          unscaled = (double)static_cast<int64_t *>(data)[row];
          break;
        case DUCKDB_TYPE_HUGEINT: {
          const duckdb_hugeint h = static_cast<duckdb_hugeint *>(data)[row];
          unscaled = duckdb_hugeint_to_double(h);
          break;
        }
        default:
          return (false);
      }

      out.d = unscaled / reader.decimal_divisor;

      return (true);
    }

    case DUCKDB_TYPE_TIMESTAMP:
      out.i = static_cast<duckdb_timestamp *>(data)[row].micros;

      return (true);

    case DUCKDB_TYPE_DATE: {
      const duckdb_date date = static_cast<duckdb_date *>(data)[row];
      if (!duckdb_is_finite_date(date)) {
        out.i = date.days > 0 ? INT64_MAX : -INT64_MAX;

        return (true);
      }
      const int64_t days = date.days;
      if (days < INT64_MIN / UT_DAY_US || days > INT64_MAX / UT_DAY_US) {
        return (false);
      }
      out.i = days * UT_DAY_US;

      return (true);
    }

    case DUCKDB_TYPE_VARCHAR: {
      duckdb_string_t *s = &static_cast<duckdb_string_t *>(data)[row];
      out.s.assign(duckdb_string_t_data(s), duckdb_string_t_length(*s));

      return (true);
    }

    default:
      return (false);
  }
}

/** A running query whose rows are read chunk by chunk. */
struct query_cursor_t {
  /** Registry token; 0 when not registered. */
  uint64_t token{0};

  /** Times each table may be read at; the registry points here. */
  query_ranges_t ranges;

  /** Connection the statement runs on. */
  duckdb_connection conn{nullptr};

  /** The bound and planned statement. */
  duckdb_prepared_statement prepared{nullptr};

  /** Its streaming execution, run by the first fetch. */
  duckdb_pending_result pending{nullptr};

  /** Set by the first fetch, which runs the statement. */
  duckdb_result result;

  /** result is set and must be destroyed. */
  bool has_result{false};

  /** All rows were read, or the query failed. */
  bool done{false};

  /** Set by query_interrupt(), from any thread. */
  std::atomic<bool> interrupted{false};

  /** The query ended by an interrupt. */
  bool ended_interrupted{false};

  /** Result columns, from the prepared statement. */
  std::vector<query_column_t> columns;

  /** How to read each result column. */
  std::vector<query_reader_t> readers;
};

/** Stop a query and free its cursor.
@param[in]  cursor  cursor; may be NULL */
void query_close(query_cursor_t *cursor) {
  if (cursor == nullptr) {
    return;
  }

  /* DuckDB stops the statement first, then the snapshot may go. */
  if (cursor->has_result) {
    duckdb_destroy_result(&cursor->result);
  }

  if (cursor->pending != nullptr) {
    duckdb_destroy_pending(&cursor->pending);
  }

  if (cursor->prepared != nullptr) {
    duckdb_destroy_prepare(&cursor->prepared);
  }

  if (cursor->token != 0) {
    query_unregister(cursor->token);
  }

  ut_delete(cursor);
}

/** Map the result columns of the prepared statement to the types a cursor
returns.
@param[in,out]  cursor  prepared cursor receiving column names and conversion
                        metadata
@param[out]     error   reason for failure
@return DB_SUCCESS, DB_OUT_OF_MEMORY for a missing column name, or
  DB_INVALID_ARGUMENT for an unsupported result type */
static dberr_t query_open_columns(query_cursor_t &cursor, std::string &error) {
  const idx_t n_cols = duckdb_prepared_statement_column_count(cursor.prepared);
  cursor.readers.resize(n_cols);

  for (idx_t c = 0; c < n_cols; ++c) {
    query_column_t col;

    char *name =
        (char *)duckdb_prepared_statement_column_name(cursor.prepared, c);
    if (name == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }
    col.name = name;
    duckdb_free(name);

    if (!query_map_type(cursor.prepared, c, cursor.readers[c], col.type)) {
      error = "result column \"" + col.name +
              "\" has a type that cannot be returned; cast it";

      return (DB_INVALID_ARGUMENT);
    }

    cursor.columns.push_back(col);
  }

  return (DB_SUCCESS);
}

/** Fill a new cursor; query_open() frees it on failure.
@param[in]   conn     DuckDB connection
@param[in]   snap     immutable snapshot kept alive until the cursor closes
@param[in]   sql      SQL statement to check or execute
@param[in]   explain  true to return EXPLAIN output instead of executing the
                      SELECT
@param[out]  cursor   allocated cursor receiving registration and DuckDB
                      handles
@param[out]  error    reason for failure
@return DB_SUCCESS or a prepare/result-type/pending-query error */
static dberr_t query_open_low(duckdb_connection conn,
                              const query_snapshot_t &snap,
                              const std::string &sql, bool explain,
                              query_cursor_t &cursor, std::string &error) {
  /* The times each table may be read at, for choosing partitions. */
  query_find_ranges(sql, cursor.ranges);

  cursor.token =
      query_register(query_connection_key(conn), snap, cursor.ranges);

  /* Binding runs the replacement scan. Executing waits for the first
  fetch, so that a query can be interrupted while it runs, and as a stream
  the rows are produced while they are fetched. */
  const std::string text = explain ? "EXPLAIN " + sql : sql;
  cursor.conn = conn;

  if (duckdb_prepare(conn, text.c_str(), &cursor.prepared) != DuckDBSuccess) {
    const char *message = duckdb_prepare_error(cursor.prepared);
    error = message != nullptr ? message : "DuckDB error";

    return (DB_ERROR);
  }

  const dberr_t err = query_open_columns(cursor, error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  if (duckdb_pending_prepared_streaming(cursor.prepared, &cursor.pending) !=
      DuckDBSuccess) {
    const char *message = duckdb_pending_error(cursor.pending);
    error = message != nullptr ? message : "DuckDB error";

    return (DB_ERROR);
  }

  return (DB_SUCCESS);
}

/** Start a checked SELECT, or EXPLAIN of it, in the snapshot transaction of
the connection, for reading its rows chunk by chunk.
@param[in]   conn     connection after query_begin(); runs nothing else until
                      query_close()
@param[in]   snap     what the query sees
@param[in]   sql      statement accepted by query_check()
@param[in]   explain  run EXPLAIN on it instead
@param[out]  out      open cursor, or NULL on failure
@param[out]  error    reason on failure
@return DB_SUCCESS, DB_ERROR for a SQL error, DB_INVALID_ARGUMENT for a result
  type that cannot be returned, or DB_OUT_OF_MEMORY */
dberr_t query_open(duckdb_connection conn, const query_snapshot_t &snap,
                   const std::string &sql, bool explain, query_cursor_t **out,
                   std::string &error) {
  *out = nullptr;
  error.clear();

  query_cursor_t *cursor = ut_new<query_cursor_t>();
  if (cursor == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  const dberr_t err = query_open_low(conn, snap, sql, explain, *cursor, error);

  if (err != DB_SUCCESS) {
    query_close(cursor);

    return (err);
  }

  *out = cursor;

  return (DB_SUCCESS);
}

/** Read the column metadata fixed when the cursor was opened.
@param[in]  cursor  query cursor
@return the result columns of a cursor */
const std::vector<query_column_t> &query_cursor_columns(
    const query_cursor_t &cursor) {
  return (cursor.columns);
}

/** Convert one DuckDB chunk into rows.
@param[in]   cursor  column metadata used for conversion
@param[in]   chunk   DuckDB result chunk
@param[out]  rows    decoded rows from this chunk
@param[out]  error   reason for failure
@return DB_SUCCESS or DB_INVALID_ARGUMENT if a value does not fit its returned
  type */
static dberr_t query_read_chunk(query_cursor_t &cursor, duckdb_data_chunk chunk,
                                query_rows_t &rows, std::string &error) {
  const idx_t n = duckdb_data_chunk_get_size(chunk);
  const size_t n_cols = cursor.columns.size();

  rows.resize(n);

  for (idx_t r = 0; r < n; ++r) {
    rows[r].resize(n_cols);
  }

  for (size_t c = 0; c < n_cols; ++c) {
    duckdb_vector v = duckdb_data_chunk_get_vector(chunk, c);

    for (idx_t r = 0; r < n; ++r) {
      if (!query_read_value(cursor.readers[c], v, r, rows[r][c])) {
        error = "a value of result column \"" + cursor.columns[c].name +
                "\" does not fit its returned type";

        return (DB_INVALID_ARGUMENT);
      }
    }
  }

  return (DB_SUCCESS);
}

/** Request interruption of an active cursor. Thread-safe with respect to
query_fetch(), not to query_close(). The running or next fetch observes the
request; after it returns DB_INTERRUPTED, every later fetch does too. Normal
end is reached when query_fetch() returns DB_SUCCESS with empty rows;
interrupting after that leaves later fetches successful and empty.
@param[in]  cursor  cursor */
void query_interrupt(query_cursor_t &cursor) {
  cursor.interrupted.store(true);
  duckdb_interrupt(cursor.conn);
}

/** End a cursor after a DuckDB failure, which an interrupt may have caused.
@param[in,out]  cursor   cursor to mark finished or interrupted
@param[in]      message  DuckDB failure text; may be NULL
@param[out]     error    reason for failure
@return DB_INTERRUPTED if an interrupt was requested; otherwise DB_ERROR */
static dberr_t query_failed(query_cursor_t &cursor, const char *message,
                            std::string &error) {
  cursor.done = true;

  if (cursor.interrupted.load()) {
    cursor.ended_interrupted = true;
    error = "the query was interrupted";

    return (DB_INTERRUPTED);
  }

  error = message != nullptr ? message : "DuckDB error";

  return (DB_ERROR);
}

/** Read the next rows, at most QUERY_CHUNK_ROWS.
@param[in]   cursor  cursor
@param[out]  rows    the rows; empty once every row was read
@param[out]  error   reason on failure
@return DB_SUCCESS, DB_ERROR for an error while executing, DB_INVALID_ARGUMENT
  for a value that does not fit its returned type, or DB_INTERRUPTED after
  query_interrupt(); after a failure the cursor returns no more rows. Once a
  fetch returns DB_INTERRUPTED, every later fetch returns DB_INTERRUPTED */
dberr_t query_fetch(query_cursor_t &cursor, query_rows_t &rows,
                    std::string &error) {
  rows.clear();
  error.clear();

  if (!cursor.done && cursor.interrupted.load()) {
    cursor.done = true;
    cursor.ended_interrupted = true;
  }

  if (cursor.done) {
    if (cursor.ended_interrupted) {
      error = "the query was interrupted";

      return (DB_INTERRUPTED);
    }

    return (DB_SUCCESS);
  }

  /* The first fetch runs the statement until rows are ready. */
  if (!cursor.has_result) {
    const bool ok =
        duckdb_execute_pending(cursor.pending, &cursor.result) == DuckDBSuccess;
    cursor.has_result = true;

    if (!ok) {
      return (query_failed(cursor, duckdb_result_error(&cursor.result), error));
    }
  }

  /* Empty chunks may come before the end; NULL is the end or an error. */
  for (;;) {
    duckdb_data_chunk chunk = duckdb_fetch_chunk(cursor.result);

    if (chunk == nullptr) {
      const char *message = duckdb_result_error(&cursor.result);
      if (message != nullptr) {
        return (query_failed(cursor, message, error));
      }

      cursor.done = true;

      return (DB_SUCCESS);
    }

    const dberr_t err = query_read_chunk(cursor, chunk, rows, error);

    duckdb_destroy_data_chunk(&chunk);

    if (err != DB_SUCCESS) {
      cursor.done = true;
      rows.clear();

      return (err);
    }

    if (!rows.empty()) {
      return (DB_SUCCESS);
    }
  }
}
