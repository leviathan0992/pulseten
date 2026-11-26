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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <map>
#include <mutex>
#include <new>
#include <string>
#include <vector>

#include <duckdb.h>

#include "db_schema.h"
#include "pt_duckdb.h"
#include "store.h"
#include "ut.h"

/** Interrupt while retaining the connection's lifetime under the mutex. */
void query_cancel::request() {
  m_requested.store(true);

  std::lock_guard<std::mutex> lock(m_mutex);
  if (m_connection != nullptr) {
    duckdb_interrupt(m_connection);
  }

  return;
}

/** Attach the next connection, retaining a cancellation made before open. */
void query_cancel::attach(duckdb_connection conn) {
  std::lock_guard<std::mutex> lock(m_mutex);
  ut_a(m_connection == nullptr);

  m_connection = conn;
  if (requested()) {
    duckdb_interrupt(m_connection);
  }

  return;
}

/** Prevent future requests from using a connection that is about to close. */
void query_cancel::detach() {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_connection = nullptr;

  return;
}

/** Register the memtable scan before replacement SQL can reference it. */
db_status_t query_init(duckdb_connection conn) {
  if (const db_status_t err = query_install_memtable_scan(conn); !err.ok()) {
    return (err.with_context("cannot register the memtable scan"));
  }

  query_register_callbacks(conn);

  return (DB_SUCCESS);
}

/** Get the process-lifetime registry, also during application destructors.
Placement construction in static storage deliberately registers no destructor:
application globals may close cursors after other translation units' globals
have gone. The local static initialization is thread-safe; unregistering each
query still frees its map entries.
@return registry that remains alive until process exit */
query_registry &query_registry::instance() {
  alignas(query_registry) static unsigned char bytes[sizeof(query_registry)];

  static query_registry *const registry = new (bytes) query_registry;

  return (*registry);
}

/** Register a running query. A connection runs one query at a time.
@param[in]  ranges  times the query may read from each table
@return the token, never 0 */
uint64_t query_registry::register_query(
    const query_connection_key_t &connection, const query_snapshot_t &snapshot,
    const query_ranges_t &ranges) {
  std::lock_guard<std::mutex> lock(m_mutex);

  const uint64_t token = m_next_token++;
  ut_a(token != 0);

  query_entry_t entry;
  entry.m_connection_key = connection;
  entry.m_snapshot = &snapshot;
  entry.m_ranges = &ranges;

  m_by_token[token] = entry;

  const bool inserted = m_by_connection.emplace(connection, token).second;
  ut_a(inserted);

  return (token);
}

/** Remove a running query.
@param[in]  token  registration token to remove; an unknown token is ignored */
void query_registry::unregister_query(uint64_t token) {
  std::lock_guard<std::mutex> lock(m_mutex);

  std::map<uint64_t, query_entry_t>::iterator it = m_by_token.find(token);
  if (it == m_by_token.end()) {
    return;
  }

  const size_t erased = m_by_connection.erase(it->second.m_connection_key);
  ut_a(erased == 1);

  m_by_token.erase(it);

  return;
}

/** Find a running query by its connection.
@return its snapshot, or NULL */
const query_snapshot_t *query_registry::find_by_connection(
    const query_connection_key_t &connection, uint64_t &token,
    const query_ranges_t *&ranges) {
  std::lock_guard<std::mutex> lock(m_mutex);

  std::map<query_connection_key_t, uint64_t>::const_iterator it =
      m_by_connection.find(connection);
  if (it == m_by_connection.end()) {
    return (nullptr);
  }

  /* Both maps change together, under the same lock. */
  std::map<uint64_t, query_entry_t>::const_iterator entry =
      m_by_token.find(it->second);
  ut_a(entry != m_by_token.end());

  token = it->second;
  ranges = entry->second.m_ranges;

  return (entry->second.m_snapshot);
}

/** Retain the first callback failure until the query unregisters. */
void query_registry::record_error(uint64_t token, const db_status_t &error) {
  ut_a(!error.ok());

  std::lock_guard<std::mutex> lock(m_mutex);

  std::map<uint64_t, query_entry_t>::iterator entry = m_by_token.find(token);
  ut_a(entry != m_by_token.end());

  if (entry->second.m_callback_status.ok()) {
    entry->second.m_callback_status = error;
  }

  return;
}

/** DuckDB reports callback failures as its own errors; return the original. */
db_status_t query_registry::restore_error(uint64_t token,
                                          const db_status_t &status) {
  ut_a(!status.ok());

  std::lock_guard<std::mutex> lock(m_mutex);

  std::map<uint64_t, query_entry_t>::const_iterator entry =
      m_by_token.find(token);
  ut_a(entry != m_by_token.end());

  if (!entry->second.m_callback_status.ok()) {
    return (entry->second.m_callback_status);
  }

  return (status);
}

/** Find the snapshot of a registered query.
@return the snapshot of a running query, or NULL */
const query_snapshot_t *query_registry::find_by_token(uint64_t token) {
  std::lock_guard<std::mutex> lock(m_mutex);

  std::map<uint64_t, query_entry_t>::iterator it = m_by_token.find(token);

  return (it != m_by_token.end() ? it->second.m_snapshot : nullptr);
}

/** Quote a normalized table or column name as an SQL identifier.
@param[in]  name  normalized SQL identifier; needs no internal escaping
@return name in double quotes */
static std::string query_quote_identifier(const std::string &name) {
  return ("\"" + name + "\"");
}

/** Check whether a time partition overlaps an inclusive time range.
@param[in]  partition_duration_hours  partition duration in UTC hours
@param[in]  range           inclusive timestamp range to test
@return true if a partition may hold times in a range */
static bool query_partition_overlaps(const store_partition_t &part,
                                     uint32_t partition_duration_hours,
                                     const query_range_t &range) {
  const int64_t first = part.m_start_hour * UT_HOUR_US;
  const int64_t last =
      (part.m_start_hour + partition_duration_hours) * UT_HOUR_US - 1;

  return (range.m_start_time_us <= range.m_end_time_us &&
          first <= range.m_end_time_us && last >= range.m_start_time_us);
}

/** Build a table source from stored partitions and visible memtable batches. */
std::string query_build_scan_sql(uint64_t token, const std::string &name,
                                 const query_table_t &table,
                                 const query_range_t &range,
                                 batch_seq_t flushed_seq) {
  const std::vector<schema_column_t> &cols = table.m_schema.m_columns;
  ut_a(!cols.empty());

  std::string sql;
  query_range_t visible = range;
  visible.m_start_time_us =
      std::max(visible.m_start_time_us, table.m_cutoff_us);

  /* One branch per partition the range overlaps. Columns added after the
  partition's rows were written read as NULL. */
  for (size_t p = 0; p < table.m_partitions.size(); ++p) {
    const store_partition_t &part = table.m_partitions[p];

    if (!query_partition_overlaps(part, table.m_partition_duration_hours,
                                  visible)) {
      continue;
    }

    /* A partition holds a prefix of the newest schema. */
    ut_a(part.m_column_count <= cols.size());

    if (!sql.empty()) {
      sql += " UNION ALL ";
    }

    sql += "SELECT ";

    for (size_t c = 0; c < cols.size(); ++c) {
      if (c > 0) {
        sql += ", ";
      }

      if (c < part.m_column_count) {
        sql += "c" + std::to_string(c);
      } else {
        sql += "CAST(NULL AS " +
               std::string(schema_data_type_name(cols[c].m_data_type)) + ")";
      }

      sql += " AS " + query_quote_identifier(cols[c].m_name);
    }

    /* The stored boundary F is explicit. Never let a physical scan include
    batches that the retained memtable snapshot also supplies. */
    sql += " FROM " + part.m_table_name +
           " WHERE _pt_seq <= " + std::to_string(flushed_seq);
    /* Later partitions already enforce this lower bound in their CHECK. */
    if (table.m_cutoff_us != INT64_MIN &&
        part.m_start_hour * UT_HOUR_US < table.m_cutoff_us) {
      sql += " AND c0 >= " + ut_timestamp_literal(table.m_cutoff_us);
    }
  }

  /* The memtable batches after F, if any. */
  if (table.m_memtable_snapshot != nullptr &&
      visible.m_start_time_us <= visible.m_end_time_us) {
    if (!sql.empty()) {
      sql += " UNION ALL ";
    }

    sql += "SELECT ";

    for (size_t c = 0; c < cols.size(); ++c) {
      sql += (c > 0 ? ", " : "") + query_quote_identifier(cols[c].m_name);
    }

    sql += " FROM " QUERY_MEMTABLE_SCAN_NAME "(" + std::to_string(token) +
           ", " + pt_duckdb_literal(name) + ")";
    if (table.m_cutoff_us != INT64_MIN) {
      sql += " WHERE " + query_quote_identifier(cols[0].m_name) +
             " >= " + ut_timestamp_literal(table.m_cutoff_us);
    }
  }

  /* No partition overlaps and nothing is in the memtable: an empty
result with the table's columns. */
  if (sql.empty()) {
    sql = "SELECT ";

    for (size_t c = 0; c < cols.size(); ++c) {
      sql += (c > 0 ? ", " : "") + std::string("CAST(NULL AS ") +
             schema_data_type_name(cols[c].m_data_type) + ") AS " +
             query_quote_identifier(cols[c].m_name);
    }

    sql += " LIMIT 0";
  }

  return (sql);
}

/** Map a DuckDB result type to a result column type.
@return false if the type cannot be returned */
bool query_column_reader::init(duckdb_logical_type type, data_type_t &out) {
  m_data_type = duckdb_get_type_id(type);
  m_decimal_type = DUCKDB_TYPE_INVALID;
  m_decimal_divisor = 1;

  switch (m_data_type) {
    case DUCKDB_TYPE_BOOLEAN:
      out = data_type_t::BOOLEAN;

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
      out = data_type_t::BIGINT;

      return (true);

    case DUCKDB_TYPE_FLOAT:
    case DUCKDB_TYPE_DOUBLE:
      out = data_type_t::DOUBLE;

      return (true);

    case DUCKDB_TYPE_DECIMAL: {
      m_decimal_type = duckdb_decimal_internal_type(type);

      for (uint8_t s = duckdb_decimal_scale(type); s > 0; --s) {
        m_decimal_divisor *= 10;
      }

      out = data_type_t::DOUBLE;

      return (true);
    }

    case DUCKDB_TYPE_TIMESTAMP:
    case DUCKDB_TYPE_DATE:
      out = data_type_t::TIMESTAMP;

      return (true);

    case DUCKDB_TYPE_VARCHAR:
      out = data_type_t::VARCHAR;

      return (true);

    default:
      return (false);
  }
}

/** Convert a signed HUGEINT to BIGINT if it fits.
@param[in]   h    signed 128-bit integer to convert
@param[out]  out  signed 64-bit value, if it fits
@return true if h fits in a BIGINT */
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
bool query_column_reader::read(duckdb_vector v, idx_t row,
                               query_value_t &out) const {
  if (!duckdb_validity_row_is_valid(duckdb_vector_get_validity(v), row)) {
    out.m_is_null = true;

    return (true);
  }

  void *data = duckdb_vector_get_data(v);
  out.m_is_null = false;

  switch (m_data_type) {
    case DUCKDB_TYPE_BOOLEAN:
      out.m_boolean = static_cast<bool *>(data)[row];

      return (true);

    case DUCKDB_TYPE_TINYINT:
      out.m_integer = static_cast<int8_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_SMALLINT:
      out.m_integer = static_cast<int16_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_INTEGER:
      out.m_integer = static_cast<int32_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_BIGINT:
      out.m_integer = static_cast<int64_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_UTINYINT:
      out.m_integer = static_cast<uint8_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_USMALLINT:
      out.m_integer = static_cast<uint16_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_UINTEGER:
      out.m_integer = static_cast<uint32_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_UBIGINT: {
      const uint64_t u = static_cast<uint64_t *>(data)[row];
      out.m_integer = static_cast<int64_t>(u);

      return (u <= static_cast<uint64_t>(INT64_MAX));
    }

    case DUCKDB_TYPE_HUGEINT:
      return (query_hugeint_to_bigint(static_cast<duckdb_hugeint *>(data)[row],
                                      out.m_integer));

    case DUCKDB_TYPE_FLOAT:
      out.m_double = static_cast<float *>(data)[row];

      return (true);

    case DUCKDB_TYPE_DOUBLE:
      out.m_double = static_cast<double *>(data)[row];

      return (true);

    case DUCKDB_TYPE_DECIMAL: {
      double unscaled = 0;

      switch (m_decimal_type) {
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

      out.m_double = unscaled / m_decimal_divisor;

      return (true);
    }

    case DUCKDB_TYPE_TIMESTAMP:
      out.m_integer = static_cast<duckdb_timestamp *>(data)[row].micros;

      return (true);

    case DUCKDB_TYPE_DATE: {
      const duckdb_date date = static_cast<duckdb_date *>(data)[row];
      if (!duckdb_is_finite_date(date)) {
        out.m_integer = date.days > 0 ? INT64_MAX : -INT64_MAX;

        return (true);
      }

      const int64_t days = date.days;
      if (days < INT64_MIN / UT_DAY_US || days > INT64_MAX / UT_DAY_US) {
        return (false);
      }

      out.m_integer = days * UT_DAY_US;

      return (true);
    }

    case DUCKDB_TYPE_VARCHAR: {
      duckdb_string_t *s = &static_cast<duckdb_string_t *>(data)[row];
      out.m_string.assign(duckdb_string_t_data(s), duckdb_string_t_length(*s));

      return (true);
    }

    default:
      return (false);
  }
}

query_cursor::query_cursor() = default;

/** Stop the query before releasing its registry entry and native handles. */
query_cursor::~query_cursor() {
  close();
}

/** Delete a query cursor; NULL is accepted. */
void query_cursor::destroy(query_cursor *cursor) {
  ut_delete(cursor);

  return;
}

/** Release execution resources before unregistering the query snapshot. */
void query_cursor::close() {
  /* DuckDB stops the statement first, then the snapshot may go. */
  duckdb_destroy_result(&m_result);

  if (m_pending != nullptr) {
    duckdb_destroy_pending(&m_pending);
  }

  if (m_token != 0) {
    query_registry::instance().unregister_query(m_token);
  }

  m_token = 0;
  m_connection = nullptr;

  return;
}

/** Map the result columns of the submitted statement to the types a cursor
returns.
@return DB_SUCCESS, or DB_INVALID_ARGUMENT for an unsupported result type */
db_status_t query_cursor::init_columns() {
  const idx_t n_cols = query_pending_column_count(m_pending);
  m_column_readers.resize(n_cols);

  for (idx_t c = 0; c < n_cols; ++c) {
    query_column_t col;
    col.m_name = query_pending_column_name(m_pending, c);

    if (!m_column_readers[c].init(query_pending_column_type(m_pending, c),
                                  col.m_data_type)) {
      return (
          db_status_t(DB_INVALID_ARGUMENT,
                      "result column \"" + col.m_name +
                          "\" has a type that cannot be returned; cast it"));
    }

    m_columns.push_back(col);
  }

  return (DB_SUCCESS);
}

/** Register the snapshot and submit the statement; query_cursor::open() frees
the cursor on failure.
@param[in]  snapshot  immutable snapshot kept alive until the cursor closes
@return DB_SUCCESS or a bind, plan or result-type error */
db_status_t query_cursor::open_low(duckdb_connection conn,
                                   const query_snapshot_t &snapshot,
                                   query_statement &statement, bool explain,
                                   const query_ranges_t &ranges) {
  m_ranges = ranges;

  m_token = query_registry::instance().register_query(
      query_connection_key(conn), snapshot, m_ranges);

  m_connection = conn;

  /* Binding resolves time-series names through the registered snapshot. Rows
  are produced from the first fetch on. */
  if (const db_status_t err = statement.submit(conn, explain, m_pending);
      !err.ok()) {
    return (query_registry::instance().restore_error(m_token, err));
  }

  return (init_columns());
}

/** Start a checked SELECT, or EXPLAIN of it, in the snapshot transaction of
the connection, for reading its rows chunk by chunk.
@param[in]   conn  connection with an active snapshot transaction; runs nothing
else until query_cursor::destroy()
@param[out]  out   open cursor, or NULL on failure
@return DB_SUCCESS, DB_ERROR for a SQL error, DB_INVALID_ARGUMENT for a result
  type that cannot be returned, or DB_OUT_OF_MEMORY */
db_status_t query_cursor::open(duckdb_connection conn,
                               const query_snapshot_t &snapshot,
                               query_statement &statement, bool explain,
                               const query_ranges_t &ranges,
                               query_cursor **out) {
  *out = nullptr;

  query_cursor *cursor = ut_new<query_cursor>();
  if (cursor == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  if (const db_status_t err =
          cursor->open_low(conn, snapshot, statement, explain, ranges);
      !err.ok()) {
    query_cursor::destroy(cursor);

    return (err);
  }

  *out = cursor;

  return (DB_SUCCESS);
}

/** Read the column metadata fixed when the cursor was opened.
@return the result columns of a cursor */
const std::vector<query_column_t> &query_cursor::columns() const {
  return (m_columns);
}

/** Convert one DuckDB chunk into rows.
@return DB_SUCCESS or DB_INVALID_ARGUMENT if a value does not fit its returned
  type */
db_status_t query_cursor::read_chunk(duckdb_data_chunk chunk,
                                     query_rows_t &rows) {
  const idx_t n = duckdb_data_chunk_get_size(chunk);
  const size_t n_cols = m_columns.size();

  ut_a(n <= QUERY_MAX_CHUNK_ROWS);
  ut_a(duckdb_data_chunk_get_column_count(chunk) == n_cols);

  rows.resize(n);

  for (idx_t r = 0; r < n; ++r) {
    rows[r].resize(n_cols);
  }

  for (size_t c = 0; c < n_cols; ++c) {
    duckdb_vector v = duckdb_data_chunk_get_vector(chunk, c);

    for (idx_t r = 0; r < n; ++r) {
      if (!m_column_readers[c].read(v, r, rows[r][c])) {
        return (db_status_t(DB_INVALID_ARGUMENT,
                            "a value of result column \"" +
                                m_columns[c].m_name +
                                "\" does not fit its returned type"));
      }
    }
  }

  return (DB_SUCCESS);
}

/** Request interruption of an active cursor. Thread-safe with respect to
fetch(), not to query_cursor::destroy(). The running or next fetch
observes the request; after it returns DB_INTERRUPTED, every later fetch does
too. Normal end is reached when fetch() returns DB_SUCCESS with
empty rows; interrupting after that leaves later fetches successful and empty.
*/
void query_cursor::interrupt() {
  m_interrupt_requested.store(true);

  duckdb_interrupt(m_connection);

  return;
}

/** Mark the cursor done after a DuckDB result error.
@return DB_INTERRUPTED if requested, otherwise the classified DuckDB failure */
db_status_t query_cursor::fail_from_result() {
  if (m_interrupt_requested.load()) {
    m_status.emplace(DB_INTERRUPTED, "the query was interrupted");
  } else {
    const db_status_t status = pt_duckdb_error(
        duckdb_result_error_type(&m_result), duckdb_result_error(&m_result));
    m_status = query_registry::instance().restore_error(m_token, status);
  }

  return (*m_status);
}

/** Read the next rows, at most QUERY_MAX_CHUNK_ROWS.
@param[out]  rows  the rows; empty once every row was read
@return DB_SUCCESS, a classified DuckDB execution error, DB_INVALID_ARGUMENT
  for a value that does not fit its returned type, or DB_INTERRUPTED after
  interrupt(). After any failure, every later fetch returns the same code and
  diagnostic with no rows. */
db_status_t query_cursor::fetch(query_rows_t &rows) {
  rows.clear();

  if (!m_status && m_interrupt_requested.load()) {
    m_status.emplace(DB_INTERRUPTED, "the query was interrupted");
  }

  if (m_status) {
    return (*m_status);
  }

  /* The first fetch runs the statement until rows are ready. */
  if (m_pending != nullptr) {
    const bool ok =
        duckdb_execute_pending(m_pending, &m_result) == DuckDBSuccess;
    duckdb_destroy_pending(&m_pending);

    if (!ok) {
      return (fail_from_result());
    }
  }

  /* Empty chunks may come before the end; NULL is the end or an error. */
  for (;;) {
    duckdb_data_chunk chunk = duckdb_fetch_chunk(m_result);

    if (chunk == nullptr) {
      const char *message = duckdb_result_error(&m_result);
      if (message != nullptr) {
        return (fail_from_result());
      }

      m_status = DB_SUCCESS;

      return (DB_SUCCESS);
    }

    const db_status_t err = read_chunk(chunk, rows);

    duckdb_destroy_data_chunk(&chunk);

    if (!err.ok()) {
      m_status = err;
      rows.clear();

      return (err);
    }

    if (!rows.empty()) {
      return (DB_SUCCESS);
    }
  }
}
