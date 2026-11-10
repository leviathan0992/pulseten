/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query.cc
 *    Running queries: the registry of running queries, the SQL that stands
 *    for a table, and reading results.
 *
 *-------------------------------------------------------------------------
 */

#include "query.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <map>
#include <mutex>
#include <new>
#include <string>
#include <vector>

#include <duckdb.h>

#include "duckdb_database.h"
#include "query_priv.h"
#include "schema.h"
#include "storage.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_new.h"
#include "ut_time.h"

/** A registered query. */
struct query_entry_t {
  /** Connection the query runs on. */
  query_connection_key_t m_connection_key{0, 0};

  /** What it sees; owned by its engine cursor. */
  const query_snapshot_t *m_snap{nullptr};

  /** Times each table may be read at; owned by its query cursor. */
  const query_ranges_t *m_ranges{nullptr};
};

/** Running queries shared by the replacement and memtable scans. */
struct query_registry_t {
  /** Protects every field below; a leaf lock, held without other locks. */
  std::mutex m_mutex;

  /** Queries indexed by their process-wide token. */
  std::map<uint64_t, query_entry_t> m_by_token;

  /** Token of the query running on each connection. */
  std::map<query_connection_key_t, uint64_t> m_by_connection;

  /** Next token to assign. */
  uint64_t m_next_token{1};
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
@param[in]  ranges  times the query may read from each table
@return the token, never 0 */
static uint64_t query_register(const query_connection_key_t &connection,
                               const query_snapshot_t &snap,
                               const query_ranges_t &ranges) {
  query_registry_t &registry = query_registry();
  std::lock_guard<std::mutex> lock(registry.m_mutex);

  const uint64_t token = registry.m_next_token++;
  ut_a(token != 0);

  query_entry_t entry;
  entry.m_connection_key = connection;
  entry.m_snap = &snap;
  entry.m_ranges = &ranges;

  registry.m_by_token[token] = entry;

  const bool inserted =
      registry.m_by_connection.emplace(connection, token).second;
  ut_a(inserted);

  return (token);
}

/** Remove a running query.
@param[in]  token  registration token to remove; an unknown token is ignored */
static void query_unregister(uint64_t token) {
  query_registry_t &registry = query_registry();
  std::lock_guard<std::mutex> lock(registry.m_mutex);

  std::map<uint64_t, query_entry_t>::iterator it =
      registry.m_by_token.find(token);
  if (it == registry.m_by_token.end()) {
    return;
  }

  const size_t erased =
      registry.m_by_connection.erase(it->second.m_connection_key);
  ut_a(erased == 1);

  registry.m_by_token.erase(it);
}

/** Find a running query by its connection.
@return its snapshot, or NULL */
const query_snapshot_t *query_find_by_connection(
    const query_connection_key_t &connection, uint64_t &token,
    const query_ranges_t *&ranges) {
  query_registry_t &registry = query_registry();
  std::lock_guard<std::mutex> lock(registry.m_mutex);

  std::map<query_connection_key_t, uint64_t>::const_iterator it =
      registry.m_by_connection.find(connection);
  if (it == registry.m_by_connection.end()) {
    return (nullptr);
  }

  /* Both maps change together, under the same lock. */
  std::map<uint64_t, query_entry_t>::const_iterator entry =
      registry.m_by_token.find(it->second);
  ut_a(entry != registry.m_by_token.end());

  token = it->second;
  ranges = entry->second.m_ranges;

  return (entry->second.m_snap);
}

/** Find the snapshot of a registered query.
@return the snapshot of a running query, or NULL */
const query_snapshot_t *query_find_by_token(uint64_t token) {
  query_registry_t &registry = query_registry();
  std::lock_guard<std::mutex> lock(registry.m_mutex);

  std::map<uint64_t, query_entry_t>::iterator it =
      registry.m_by_token.find(token);

  return (it != registry.m_by_token.end() ? it->second.m_snap : nullptr);
}

/** Quote a canonical table or column name as an SQL identifier.
@param[in]  name  canonical SQL identifier; needs no internal escaping
@return "name" quoted as an SQL identifier; names are canonical identifiers,
  so no character needs escaping */
static std::string query_quote(const std::string &name) {
  return ("\"" + name + "\"");
}

/** Check whether a time partition overlaps an inclusive time range.
@param[in]  partition_days  partition span in UTC days
@param[in]  range           inclusive timestamp range to test
@return true if a partition may hold times in a range */
static bool query_partition_overlaps(const storage_partition_t &part,
                                     uint32_t partition_days,
                                     const query_range_t &range) {
  const int64_t first = part.m_first_day * UT_DAY_US;
  const int64_t last = (part.m_first_day + partition_days) * UT_DAY_US - 1;

  return (first <= range.m_hi && last >= range.m_lo);
}

/** Build the SELECT that stands for a table: its partitions that overlap a
time range, and its memtable batches.
@param[in]  name   canonical table name
@param[in]  range  times the query may read from the table
@return the SQL text */
std::string query_build_table_sql(uint64_t token, const std::string &name,
                                  const query_table_t &table,
                                  uint32_t partition_days,
                                  const query_range_t &range,
                                  batch_seq_t flushed_seq) {
  const std::vector<col_def_t> &cols = table.m_schema.m_cols;
  ut_a(!cols.empty());

  std::string sql;

  /* One branch per partition the range overlaps. Columns the partition
  does not have yet were added after its rows were written: NULL. */
  for (size_t p = 0; p < table.m_partitions.size(); ++p) {
    const storage_partition_t &part = table.m_partitions[p];

    if (!query_partition_overlaps(part, partition_days, range)) {
      continue;
    }

    /* A partition holds a prefix of the newest schema. */
    ut_a(part.m_n_cols <= cols.size());

    if (!sql.empty()) {
      sql += " UNION ALL ";
    }

    sql += "SELECT ";

    for (size_t c = 0; c < cols.size(); ++c) {
      if (c > 0) {
        sql += ", ";
      }

      if (c < part.m_n_cols) {
        sql += "c" + std::to_string(c);
      } else {
        sql += "CAST(NULL AS " + std::string(schema_type_name(cols[c].m_type)) +
               ")";
      }

      sql += " AS " + query_quote(cols[c].m_name);
    }

    /* The stored boundary F is explicit. Never let a physical scan include
    batches that the retained memtable snapshot also supplies. */
    sql += " FROM " + part.m_phys_name +
           " WHERE _pt_seq <= " + std::to_string(flushed_seq);
  }

  /* The memtable batches after F, if any. */
  if (!table.m_batches.empty()) {
    if (!sql.empty()) {
      sql += " UNION ALL ";
    }

    sql += "SELECT ";

    for (size_t c = 0; c < cols.size(); ++c) {
      sql += (c > 0 ? ", " : "") + query_quote(cols[c].m_name);
    }

    sql += " FROM " QUERY_MEMTABLE_SCAN "(" + std::to_string(token) + ", " +
           duckdb_literal(name) + ")";
  }

  /* No partition overlaps and nothing is in the memtable: an empty
result with the table's columns. */
  if (sql.empty()) {
    sql = "SELECT ";

    for (size_t c = 0; c < cols.size(); ++c) {
      sql += (c > 0 ? ", " : "") + std::string("CAST(NULL AS ") +
             schema_type_name(cols[c].m_type) + ") AS " +
             query_quote(cols[c].m_name);
    }

    sql += " LIMIT 0";
  }

  return (sql);
}

/** Install the memtable scan, then the replacement scan whose generated
SQL calls it. Call once during engine opening, before user queries or
background work can use the database. The connection is borrowed and is not
disconnected.
@param[in,out]  conn  connection to the database; NULL is rejected
@return DB_SUCCESS or DB_ERROR */
db_status_t query_install_scans(duckdb_connection conn) {
  const db_status_t err = query_install_memtable_scan(conn);
  if (!err.ok()) {
    return (db_status_t(err.code(), "cannot register the memtable scan"));
  }

  query_register_table_replacement(conn);

  return (DB_SUCCESS);
}

/** Check syntax and require exactly one SQL statement before using the C++
parser, which reports syntax errors by throwing.
@return DB_SUCCESS, or DB_INVALID_ARGUMENT */
static db_status_t query_check_syntax(duckdb_connection conn,
                                      const std::string &sql) {
  /* The C API contains parser exceptions and owns its diagnostic until destroy.
   */
  duckdb_extracted_statements statements = nullptr;
  const idx_t n = duckdb_extract_statements(conn, sql.c_str(), &statements);
  const char *message = duckdb_extract_statements_error(statements);
  db_status_t status;
  if (n == 0 && message != nullptr) {
    status = db_status_t(DB_INVALID_ARGUMENT, message);
  } else if (n != 1) {
    status =
        db_status_t(DB_INVALID_ARGUMENT, "exactly one statement is required");
  }
  duckdb_destroy_extracted(&statements);

  return (status);
}

/** Validate a single SELECT against PulseTen's query restrictions. */
db_status_t query_validate_select(duckdb_connection conn,
                                  const std::string &sql) {
  const db_status_t err = query_check_syntax(conn, sql);
  if (!err.ok()) {
    return (err);
  }

  return (query_validate_select_statement(sql));
}

/** Validate one ordinary-table DDL statement. */
db_status_t query_check_ddl(duckdb_connection conn, const std::string &sql,
                            query_ddl_t &ddl) {
  ddl = query_ddl_t();
  const db_status_t err = query_check_syntax(conn, sql);
  if (!err.ok()) {
    return (err);
  }

  return (query_check_ddl_statement(sql, ddl));
}

/** Execute checked DDL with replacement scans over its pinned snapshot. */
db_status_t query_execute_ddl(duckdb_connection conn,
                              const query_snapshot_t &snap,
                              const std::string &sql) {
  query_ranges_t ranges;
  const uint64_t token =
      query_register(query_connection_key(conn), snap, ranges);
  duckdb_database_t database;
  database.m_conn = conn;
  const db_status_t err = duckdb_database_exec(database, sql);
  query_unregister(token);
  return (err);
}

/** Commit the transaction of a query connection. */
db_status_t query_commit(duckdb_connection conn) {
  /* Borrow the connection; the cursor still owns it. */
  duckdb_database_t database;
  database.m_conn = conn;

  return (duckdb_database_commit(database));
}

/** Start the snapshot transaction of a query connection and read F.
@return DB_SUCCESS or DB_ERROR */
db_status_t query_begin_snapshot(duckdb_connection conn,
                                 batch_seq_t &flushed_seq) {
  /* Borrow the connection; the cursor still owns it. */
  duckdb_database_t database;
  database.m_conn = conn;

  db_status_t err = duckdb_database_exec(database, "BEGIN TRANSACTION");
  if (!err.ok()) {
    return (err);
  }

  /* Reading F also fixes the transaction's snapshot here and now. */
  duckdb_rows_t rows;
  err = duckdb_database_query(database, "SELECT flushed_seq FROM _pt_state",
                              rows);
  if (!err.ok()) {
    return (err);
  }

  if (rows.size() != 1 || rows[0][0].m_is_null) {
    return (db_status_t(DB_CORRUPTION, "bad _pt_state"));
  }

  flushed_seq = static_cast<batch_seq_t>(rows[0][0].m_i);

  return (DB_SUCCESS);
}

/** How to read one result column. */
struct query_reader_t {
  /** DuckDB type of the column. */
  duckdb_type m_type{DUCKDB_TYPE_INVALID};

  /** DECIMAL: the integer type it is stored as. */
  duckdb_type m_decimal_type{DUCKDB_TYPE_INVALID};

  /** DECIMAL: 10 to the power of its scale. */
  double m_decimal_divisor{1};
};

/** Map a DuckDB result type to a result column type.
@param[in]  col  zero-based result column index
@return false if the type cannot be returned */
static bool query_map_type(duckdb_prepared_statement prepared, idx_t col,
                           query_reader_t &reader, col_type_t &out) {
  reader.m_type = duckdb_prepared_statement_column_type(prepared, col);

  switch (reader.m_type) {
    case DUCKDB_TYPE_BOOLEAN:
      out = col_type_t::BOOLEAN;

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
      out = col_type_t::BIGINT;

      return (true);

    case DUCKDB_TYPE_FLOAT:
    case DUCKDB_TYPE_DOUBLE:
      out = col_type_t::DOUBLE;

      return (true);

    case DUCKDB_TYPE_DECIMAL: {
      duckdb_logical_type lt =
          duckdb_prepared_statement_column_logical_type(prepared, col);
      reader.m_decimal_type = duckdb_decimal_internal_type(lt);

      for (uint8_t s = duckdb_decimal_scale(lt); s > 0; --s) {
        reader.m_decimal_divisor *= 10;
      }

      duckdb_destroy_logical_type(&lt);
      out = col_type_t::DOUBLE;

      return (true);
    }

    case DUCKDB_TYPE_TIMESTAMP:
    case DUCKDB_TYPE_DATE:
      out = col_type_t::TIMESTAMP;

      return (true);

    case DUCKDB_TYPE_VARCHAR:
      out = col_type_t::VARCHAR;

      return (true);

    default:
      return (false);
  }
}

/** Convert a signed HUGEINT to BIGINT if it fits.
@param[in]   h    signed 128-bit integer to convert
@param[out]  out  signed 64-bit value, if it fits
@return a HUGEINT as BIGINT, or false if it does not fit */
static bool query_hugeint_to_bigint(duckdb_hugeint h, int64_t &out) {
  const bool positive =
      h.upper == 0 && h.lower <= static_cast<uint64_t>(INT64_MAX);
  const bool negative =
      h.upper == -1 && h.lower > static_cast<uint64_t>(INT64_MAX);

  if (!positive && !negative) {
    return (false);
  }

  out = static_cast<int64_t>(h.lower);

  return (true);
}

/** Read one value of a result chunk.
@param[in]   row  zero-based row index
@param[out]  out  decoded value and NULL flag
@return false if the value cannot be represented */
static bool query_read_value(const query_reader_t &reader, duckdb_vector v,
                             idx_t row, query_value_t &out) {
  if (!duckdb_validity_row_is_valid(duckdb_vector_get_validity(v), row)) {
    out.m_is_null = true;

    return (true);
  }

  void *data = duckdb_vector_get_data(v);
  out.m_is_null = false;

  switch (reader.m_type) {
    case DUCKDB_TYPE_BOOLEAN:
      out.m_b = static_cast<bool *>(data)[row];

      return (true);
    case DUCKDB_TYPE_TINYINT:
      out.m_i = static_cast<int8_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_SMALLINT:
      out.m_i = static_cast<int16_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_INTEGER:
      out.m_i = static_cast<int32_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_BIGINT:
      out.m_i = static_cast<int64_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_UTINYINT:
      out.m_i = static_cast<uint8_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_USMALLINT:
      out.m_i = static_cast<uint16_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_UINTEGER:
      out.m_i = static_cast<uint32_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_UBIGINT: {
      const uint64_t u = static_cast<uint64_t *>(data)[row];
      out.m_i = static_cast<int64_t>(u);

      return (u <= static_cast<uint64_t>(INT64_MAX));
    }

    case DUCKDB_TYPE_HUGEINT:
      return (query_hugeint_to_bigint(static_cast<duckdb_hugeint *>(data)[row],
                                      out.m_i));

    case DUCKDB_TYPE_FLOAT:
      out.m_d = static_cast<float *>(data)[row];

      return (true);
    case DUCKDB_TYPE_DOUBLE:
      out.m_d = static_cast<double *>(data)[row];

      return (true);

    case DUCKDB_TYPE_DECIMAL: {
      double unscaled = 0;

      switch (reader.m_decimal_type) {
        case DUCKDB_TYPE_SMALLINT:
          unscaled = static_cast<int16_t *>(data)[row];
          break;
        case DUCKDB_TYPE_INTEGER:
          unscaled = static_cast<int32_t *>(data)[row];
          break;
        case DUCKDB_TYPE_BIGINT:
          unscaled = static_cast<double>(static_cast<int64_t *>(data)[row]);
          break;
        case DUCKDB_TYPE_HUGEINT: {
          const duckdb_hugeint h = static_cast<duckdb_hugeint *>(data)[row];
          unscaled = duckdb_hugeint_to_double(h);
          break;
        }
        default:
          return (false);
      }

      out.m_d = unscaled / reader.m_decimal_divisor;

      return (true);
    }

    case DUCKDB_TYPE_TIMESTAMP:
      out.m_i = static_cast<duckdb_timestamp *>(data)[row].micros;

      return (true);

    case DUCKDB_TYPE_DATE: {
      const duckdb_date date = static_cast<duckdb_date *>(data)[row];
      if (!duckdb_is_finite_date(date)) {
        out.m_i = date.days > 0 ? INT64_MAX : -INT64_MAX;

        return (true);
      }
      const int64_t days = date.days;
      if (days < INT64_MIN / UT_DAY_US || days > INT64_MAX / UT_DAY_US) {
        return (false);
      }
      out.m_i = days * UT_DAY_US;

      return (true);
    }

    case DUCKDB_TYPE_VARCHAR: {
      duckdb_string_t *s = &static_cast<duckdb_string_t *>(data)[row];
      out.m_s.assign(duckdb_string_t_data(s), duckdb_string_t_length(*s));

      return (true);
    }

    default:
      return (false);
  }
}

/** A running query whose rows are read chunk by chunk. */
struct query_cursor_t {
  /** Registry token; 0 when not registered. */
  uint64_t m_token{0};

  /** Times each table may be read at; the registry points here. */
  query_ranges_t m_ranges;

  /** Connection the statement runs on. */
  duckdb_connection m_conn{nullptr};

  /** The bound and planned statement. */
  duckdb_prepared_statement m_prepared{nullptr};

  /** Its streaming execution, run by the first fetch. */
  duckdb_pending_result m_pending{nullptr};

  /** Set by the first fetch, which runs the statement. */
  duckdb_result m_result;

  /** result is set and must be destroyed. */
  bool m_has_result{false};

  /** All rows were read, or the query failed. */
  bool m_done{false};

  /** Set by query_cursor_interrupt(), from any thread. */
  std::atomic<bool> m_interrupt_requested{false};

  /** The query ended by an interrupt. */
  bool m_ended_by_interrupt{false};

  /** Result columns, from the prepared statement. */
  std::vector<query_column_t> m_columns;

  /** How to read each result column. */
  std::vector<query_reader_t> m_readers;
};

/** Stop a query and free its cursor.
@param[in]  cursor  cursor; may be NULL */
void query_cursor_close(query_cursor_t *cursor) {
  if (cursor == nullptr) {
    return;
  }

  /* DuckDB stops the statement first, then the snapshot may go. */
  if (cursor->m_has_result) {
    duckdb_destroy_result(&cursor->m_result);
  }

  if (cursor->m_pending != nullptr) {
    duckdb_destroy_pending(&cursor->m_pending);
  }

  if (cursor->m_prepared != nullptr) {
    duckdb_destroy_prepare(&cursor->m_prepared);
  }

  if (cursor->m_token != 0) {
    query_unregister(cursor->m_token);
  }

  ut_delete(cursor);
}

/** Map the result columns of the prepared statement to the types a cursor
returns.
@return DB_SUCCESS, DB_OUT_OF_MEMORY for a missing column name, or
  DB_INVALID_ARGUMENT for an unsupported result type */
static db_status_t query_open_columns(query_cursor_t &cursor) {
  const idx_t n_cols =
      duckdb_prepared_statement_column_count(cursor.m_prepared);
  cursor.m_readers.resize(n_cols);

  for (idx_t c = 0; c < n_cols; ++c) {
    query_column_t col;

    const char *name =
        duckdb_prepared_statement_column_name(cursor.m_prepared, c);
    if (name == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }

    col.m_name = name;

    /* DuckDB returns this owned allocation as const char *. */
    duckdb_free(const_cast<char *>(name));

    if (!query_map_type(cursor.m_prepared, c, cursor.m_readers[c],
                        col.m_type)) {
      return (
          db_status_t(DB_INVALID_ARGUMENT,
                      "result column \"" + col.m_name +
                          "\" has a type that cannot be returned; cast it"));
    }

    cursor.m_columns.push_back(col);
  }

  return (DB_SUCCESS);
}

/** Fill a new cursor; query_cursor_open() frees it on failure.
@param[in]  snap  immutable snapshot kept alive until the cursor closes
@return DB_SUCCESS or a prepare/result-type/pending-query error */
static db_status_t query_open_low(duckdb_connection conn,
                                  const query_snapshot_t &snap,
                                  const std::string &sql, bool explain,
                                  query_cursor_t &cursor) {
  /* The times each table may be read at, for choosing partitions. */
  query_find_ranges(sql, cursor.m_ranges);

  cursor.m_token =
      query_register(query_connection_key(conn), snap, cursor.m_ranges);

  const std::string text = explain ? "EXPLAIN " + sql : sql;
  cursor.m_conn = conn;

  /* Bind the query to its registered snapshot and resolve output columns. */
  if (duckdb_prepare(conn, text.c_str(), &cursor.m_prepared) != DuckDBSuccess) {
    const char *message = duckdb_prepare_error(cursor.m_prepared);
    return (
        db_status_t(DB_ERROR, message != nullptr ? message : "DuckDB error"));
  }

  const db_status_t err = query_open_columns(cursor);
  if (!err.ok()) {
    return (err);
  }

  /* Create a streaming query; execution starts on the first fetch. */
  if (duckdb_pending_prepared_streaming(cursor.m_prepared, &cursor.m_pending) !=
      DuckDBSuccess) {
    const char *message = duckdb_pending_error(cursor.m_pending);
    return (
        db_status_t(DB_ERROR, message != nullptr ? message : "DuckDB error"));
  }

  return (DB_SUCCESS);
}

/** Start a checked SELECT, or EXPLAIN of it, in the snapshot transaction of
the connection, for reading its rows chunk by chunk.
@param[in]   conn  connection after query_begin_snapshot(); runs nothing else
until query_cursor_close()
@param[out]  out   open cursor, or NULL on failure
@return DB_SUCCESS, DB_ERROR for a SQL error, DB_INVALID_ARGUMENT for a result
  type that cannot be returned, or DB_OUT_OF_MEMORY */
db_status_t query_cursor_open(duckdb_connection conn,
                              const query_snapshot_t &snap,
                              const std::string &sql, bool explain,
                              query_cursor_t **out) {
  *out = nullptr;

  query_cursor_t *cursor = ut_new<query_cursor_t>();
  if (cursor == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  const db_status_t err = query_open_low(conn, snap, sql, explain, *cursor);

  if (!err.ok()) {
    query_cursor_close(cursor);

    return (err);
  }

  *out = cursor;

  return (DB_SUCCESS);
}

/** Read the column metadata fixed when the cursor was opened.
@return the result columns of a cursor */
const std::vector<query_column_t> &query_cursor_columns(
    const query_cursor_t &cursor) {
  return (cursor.m_columns);
}

/** Convert one DuckDB chunk into rows.
@return DB_SUCCESS or DB_INVALID_ARGUMENT if a value does not fit its returned
  type */
static db_status_t query_read_chunk(query_cursor_t &cursor,
                                    duckdb_data_chunk chunk,
                                    query_rows_t &rows) {
  const idx_t n = duckdb_data_chunk_get_size(chunk);
  const size_t n_cols = cursor.m_columns.size();

  ut_a(n <= QUERY_CHUNK_ROWS);
  ut_a(duckdb_data_chunk_get_column_count(chunk) == n_cols);

  rows.resize(n);

  for (idx_t r = 0; r < n; ++r) {
    rows[r].resize(n_cols);
  }

  for (size_t c = 0; c < n_cols; ++c) {
    duckdb_vector v = duckdb_data_chunk_get_vector(chunk, c);

    for (idx_t r = 0; r < n; ++r) {
      if (!query_read_value(cursor.m_readers[c], v, r, rows[r][c])) {
        return (db_status_t(DB_INVALID_ARGUMENT,
                            "a value of result column \"" +
                                cursor.m_columns[c].m_name +
                                "\" does not fit its returned type"));
      }
    }
  }

  return (DB_SUCCESS);
}

/** Request interruption of an active cursor. Thread-safe with respect to
query_cursor_fetch(), not to query_cursor_close(). The running or next fetch
observes the request; after it returns DB_INTERRUPTED, every later fetch does
too. Normal end is reached when query_cursor_fetch() returns DB_SUCCESS with
empty rows; interrupting after that leaves later fetches successful and empty.
*/
void query_cursor_interrupt(query_cursor_t &cursor) {
  cursor.m_interrupt_requested.store(true);
  duckdb_interrupt(cursor.m_conn);
}

/** End a cursor after a DuckDB failure, which an interrupt may have caused.
@param[in]  message  DuckDB failure text; may be NULL
@return DB_INTERRUPTED if an interrupt was requested; otherwise DB_ERROR */
static db_status_t query_end_failed(query_cursor_t &cursor,
                                    const char *message) {
  cursor.m_done = true;

  if (cursor.m_interrupt_requested.load()) {
    cursor.m_ended_by_interrupt = true;
    return (db_status_t(DB_INTERRUPTED, "the query was interrupted"));
  }

  return (db_status_t(DB_ERROR, message != nullptr ? message : "DuckDB error"));
}

/** Read the next rows, at most QUERY_CHUNK_ROWS.
@param[out]  rows  the rows; empty once every row was read
@return DB_SUCCESS, DB_ERROR for an error while executing, DB_INVALID_ARGUMENT
  for a value that does not fit its returned type, or DB_INTERRUPTED after
  query_cursor_interrupt(); after a failure the cursor returns no more rows.
Once a fetch returns DB_INTERRUPTED, every later fetch returns DB_INTERRUPTED */
db_status_t query_cursor_fetch(query_cursor_t &cursor, query_rows_t &rows) {
  rows.clear();

  if (!cursor.m_done && cursor.m_interrupt_requested.load()) {
    cursor.m_done = true;
    cursor.m_ended_by_interrupt = true;
  }

  if (cursor.m_done) {
    if (cursor.m_ended_by_interrupt) {
      return (db_status_t(DB_INTERRUPTED, "the query was interrupted"));
    }

    return (DB_SUCCESS);
  }

  /* The first fetch runs the statement until rows are ready. */
  if (!cursor.m_has_result) {
    const bool ok = duckdb_execute_pending(cursor.m_pending,
                                           &cursor.m_result) == DuckDBSuccess;
    cursor.m_has_result = true;

    if (!ok) {
      return (query_end_failed(cursor, duckdb_result_error(&cursor.m_result)));
    }
  }

  /* Empty chunks may come before the end; NULL is the end or an error. */
  for (;;) {
    duckdb_data_chunk chunk = duckdb_fetch_chunk(cursor.m_result);

    if (chunk == nullptr) {
      const char *message = duckdb_result_error(&cursor.m_result);
      if (message != nullptr) {
        return (query_end_failed(cursor, message));
      }

      cursor.m_done = true;

      return (DB_SUCCESS);
    }

    const db_status_t err = query_read_chunk(cursor, chunk, rows);

    duckdb_destroy_data_chunk(&chunk);

    if (!err.ok()) {
      cursor.m_done = true;
      rows.clear();

      return (err);
    }

    if (!rows.empty()) {
      return (DB_SUCCESS);
    }
  }
}
