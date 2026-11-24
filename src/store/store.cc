/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * store.cc
 *    Durable storage, catalog validation and transactional maintenance.
 *
 *-------------------------------------------------------------------------
 */

#include "store.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>
#include <duckdb_static_extension.h>

#include "db_batch.h"
#include "db_schema.h"
#include "os_file.h"
#include "pt_duckdb.h"
#include "ut.h"

/* ================= File format and partition naming ================= */

/** First UTC day a row may have: 1900-01-01. */
#define STORE_MIN_DAY (-25567)

/** Last UTC day a row may have: 2199-12-31. */
#define STORE_MAX_DAY 84005

/** Statements that create the internal tables of a new file. */
static const char *store_create_sql[] = {
    "CREATE TABLE _pt_state ("
    "  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),"
    "  format UINTEGER NOT NULL,"
    "  flushed_seq UBIGINT NOT NULL,"
    "  next_table_id UBIGINT NOT NULL,"
    "  partition_duration_hours UINTEGER NOT NULL,"
    "  wal_id UBIGINT NOT NULL CHECK (wal_id > 0))",

    "CREATE TABLE _pt_tables ("
    "  table_id UBIGINT PRIMARY KEY,"
    "  name VARCHAR NOT NULL UNIQUE,"
    "  initialized BOOLEAN NOT NULL,"
    "  partition_duration_hours UINTEGER NOT NULL,"
    "  retention_hours UINTEGER)",

    "CREATE TABLE _pt_columns ("
    "  table_id UBIGINT NOT NULL,"
    "  col_id UINTEGER NOT NULL,"
    "  name VARCHAR NOT NULL,"
    "  kind UTINYINT NOT NULL,"
    "  type UTINYINT NOT NULL,"
    "  PRIMARY KEY (table_id, col_id))",

    "CREATE TABLE _pt_partitions ("
    "  table_id UBIGINT NOT NULL,"
    "  start_hour BIGINT NOT NULL,"
    "  n_cols UINTEGER NOT NULL,"
    "  PRIMARY KEY (table_id, start_hour))",
};

/** Build a physical partition name from its table id and first UTC hour. */
std::string store_partition_name(uint64_t table_id, int64_t start_hour) {
  std::string name = "_pt_t" + std::to_string(table_id) + "_";

  if (start_hour < 0) {
    return (name + "n" +
            std::to_string(uint64_t{0} - static_cast<uint64_t>(start_hour)));
  }

  return (name + "p" + std::to_string(start_hour));
}

/** Align a UTC hour to the start of its partition. */
int64_t store_partition_start_hour(int64_t hour,
                                   uint32_t partition_duration_hours) {
  ut_ad(partition_duration_hours > 0);

  /* Round toward the earlier partition, also before 1970. */
  int64_t n = hour / partition_duration_hours;
  if (hour % partition_duration_hours < 0) {
    n--;
  }

  return (n * partition_duration_hours);
}

/** Create the internal tables in a new, empty file.
@param[in]  partition_duration_hours  number of UTC hours covered by one
partition
@return DB_SUCCESS, a random-identity or SQL error, or DB_OUTCOME_UNKNOWN
  after an uncertain commit */
db_status_t store::create_catalog(uint32_t partition_duration_hours) {
  uint64_t wal_id = 0;
  if (const db_status_t err = os_generate_uid(wal_id); !err.ok()) {
    return (err);
  }

  /* Create all internal tables and the initial state in one transaction. */
  if (const db_status_t err = m_session.begin(); !err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < sizeof(store_create_sql) / sizeof(char *); ++i) {
    if (const db_status_t err =
            pt_duckdb_exec(m_session.connection(), store_create_sql[i]);
        !err.ok()) {
      return (m_session.rollback(err));
    }
  }

  /* Initialize the storage format, partition duration and WAL identity.
  No writes are flushed yet, and table ids start at 1. */
  if (const db_status_t err =
          pt_duckdb_exec(m_session.connection(),
                         "INSERT INTO _pt_state VALUES (1, " +
                             std::to_string(STORE_FORMAT_VERSION) + ", 0, 1, " +
                             std::to_string(partition_duration_hours) + ", " +
                             std::to_string(wal_id) + ")");
      !err.ok()) {
    return (m_session.rollback(err));
  }

  return (m_session.commit());
}

/* ================== Catalog reading and validation ================== */

/** Describe inconsistent persistent metadata.
@return DB_CORRUPTION */
static db_status_t store_corruption(const std::string &why) {
  return (db_status_t(DB_CORRUPTION, "storage file is inconsistent: " + why));
}

/** Read and validate the single row in _pt_state.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION */
db_status_t store_catalog_reader::read_state() {
  pt_duckdb_rows_t rows;

  if (const db_status_t err =
          pt_duckdb_exec(m_connection,
                         "SELECT format, flushed_seq, next_table_id, "
                         "partition_duration_hours, wal_id FROM _pt_state",
                         rows);
      !err.ok()) {
    return (err);
  }

  if (rows.size() != 1) {
    return (store_corruption("bad _pt_state"));
  }

  const int64_t format = rows[0][0].m_integer;
  const int64_t flushed_seq = rows[0][1].m_integer;
  const int64_t next_table_id = rows[0][2].m_integer;
  const int64_t partition_duration_hours = rows[0][3].m_integer;
  const int64_t wal_id = rows[0][4].m_integer;

  if (format != STORE_FORMAT_VERSION || partition_duration_hours < 1 ||
      partition_duration_hours > STORE_MAX_PARTITION_HOURS || wal_id == 0 ||
      next_table_id == 0) {
    return (store_corruption("bad _pt_state"));
  }

  m_catalog.m_wal_id = static_cast<uint64_t>(wal_id);
  m_catalog.m_flushed_seq = static_cast<batch_seq_t>(flushed_seq);
  m_catalog.m_next_table_id = static_cast<uint64_t>(next_table_id);
  m_catalog.m_partition_duration_hours =
      static_cast<uint32_t>(partition_duration_hours);

  return (DB_SUCCESS);
}

/** Load the table names of _pt_tables, by table id.
REQUIRES: m_catalog.m_next_table_id is loaded.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION */
db_status_t store_catalog_reader::read_table_names(
    std::map<uint64_t, std::string> &names) {
  pt_duckdb_rows_t rows;

  if (const db_status_t err = pt_duckdb_exec(
          m_connection,
          "SELECT table_id, name, initialized, "
          "partition_duration_hours, retention_hours FROM _pt_tables",
          rows);
      !err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    const uint64_t id = static_cast<uint64_t>(rows[i][0].m_integer);

    if (id == 0 || id >= m_catalog.m_next_table_id) {
      return (store_corruption("table id out of range"));
    }

    const std::string &stored = rows[i][1].m_string;

    std::string name;
    if (!schema_normalize_table_name(stored.data(), stored.size(), name).ok() ||
        name != stored) {
      return (store_corruption("bad table name"));
    }

    const int64_t duration = rows[i][3].m_integer;
    if (duration < 1 || duration > STORE_MAX_PARTITION_HOURS) {
      return (store_corruption("bad partition duration of table " + name));
    }

    store_table_t table;
    table.m_table_id = id;
    table.m_partition_duration_hours = static_cast<uint32_t>(duration);
    if (!rows[i][4].m_is_null) {
      const int64_t retention = rows[i][4].m_integer;
      if (retention < 0 || retention > STORE_MAX_RETENTION_HOURS) {
        return (store_corruption("bad retention of table " + name));
      }

      table.m_retention_hours = static_cast<uint32_t>(retention);
    }

    table.m_initialized = rows[i][2].m_integer != 0;
    table.m_schema.m_name = name;
    if (!names.emplace(id, name).second ||
        !m_catalog.m_tables.emplace(name, std::move(table)).second) {
      return (store_corruption("duplicate table " + name));
    }
  }

  return (DB_SUCCESS);
}

/** Load the rows of _pt_columns, grouped by table id.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION */
db_status_t store_catalog_reader::read_columns(
    std::map<uint64_t, std::vector<schema_column_t>> &cols) {
  pt_duckdb_rows_t rows;

  if (const db_status_t err =
          pt_duckdb_exec(m_connection,
                         "SELECT table_id, col_id, name, kind, type "
                         "FROM _pt_columns ORDER BY table_id, col_id",
                         rows);
      !err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    const uint64_t table_id = static_cast<uint64_t>(rows[i][0].m_integer);
    const int64_t column_type = rows[i][3].m_integer;
    const int64_t type = rows[i][4].m_integer;

    schema_column_t c;
    c.m_id = static_cast<uint32_t>(rows[i][1].m_integer);
    c.m_name = rows[i][2].m_string;
    c.m_column_type = static_cast<column_type_t>(column_type);
    c.m_data_type = static_cast<data_type_t>(type);

    if (column_type > static_cast<int64_t>(column_type_t::FIELD) ||
        type < static_cast<int64_t>(data_type_t::BOOLEAN) ||
        type > static_cast<int64_t>(data_type_t::VARCHAR)) {
      return (store_corruption("bad column kind or type"));
    }

    cols[table_id].push_back(c);
  }

  return (DB_SUCCESS);
}

/** Load storage state and logical table metadata.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION if stored metadata
  is inconsistent */
db_status_t store_catalog_reader::read_metadata() {
  if (const db_status_t err = read_state(); !err.ok()) {
    return (err);
  }

  std::map<uint64_t, std::string> names;

  if (const db_status_t err = read_table_names(names); !err.ok()) {
    return (err);
  }

  std::map<uint64_t, std::vector<schema_column_t>> cols;

  if (const db_status_t err = read_columns(cols); !err.ok()) {
    return (err);
  }

  for (const auto &[id, columns] : cols) {
    if (names.count(id) == 0) {
      return (store_corruption("columns refer to an unknown table"));
    }
  }

  for (const auto &[id, name] : names) {
    store_table_t &table = m_catalog.m_tables.at(name);
    const auto columns = cols.find(id);
    if (!table.m_initialized) {
      if (columns != cols.end()) {
        return (store_corruption("uninitialized table has columns: " + name));
      }

      continue;
    }

    if (columns == cols.end() || !schema_validate_columns(columns->second)) {
      return (store_corruption("bad columns of table " + name));
    }

    table.m_schema.m_columns = std::move(columns->second);
  }

  return (DB_SUCCESS);
}

/** One physical column, in the order reported by DuckDB. */
struct store_physical_column_t {
  /** Physical column name. */
  std::string m_name;

  /** Canonical SQL type reported by information_schema.columns. */
  std::string m_data_type;

  /** One-based position in the physical table. */
  int64_t m_position{0};
};

/** Check the physical columns against the logical schema prefix.
@param[in]  schema  newest logical schema; columns only ever append
@return true if names, positions and types match the partition format */
static bool store_partition_matches_schema(
    const std::vector<store_physical_column_t> &cols, const schema_t &schema,
    uint32_t n_cols) {
  if (cols.size() != static_cast<size_t>(n_cols) + 2) {
    return (false);
  }

  for (size_t c = 0; c < cols.size(); ++c) {
    std::string name;
    const char *type;

    if (c == 0) {
      name = "_pt_seq";
      type = "UBIGINT";
    } else if (c == 1) {
      name = "_pt_row";
      type = "UINTEGER";
    } else {
      name = "c" + std::to_string(c - 2);
      type = schema_data_type_name(schema.m_columns[c - 2].m_data_type);
    }

    if (cols[c].m_position != static_cast<int64_t>(c) + 1 ||
        cols[c].m_name != name || cols[c].m_data_type != type) {
      return (false);
    }
  }

  return (true);
}

/** Check SQL tables while rejecting unknown internal objects and
name collisions with time-series tables.
@return DB_SUCCESS, a query error, or DB_CORRUPTION */
db_status_t store::validate_catalog_tables() {
  std::set<std::string> internal = {"_pt_state", "_pt_tables", "_pt_columns",
                                    "_pt_partitions"};

  for (const auto &[name, table] : m_catalog.m_tables) {
    for (const auto &[hour, partition] : table.m_partitions) {
      internal.insert(partition.m_table_name);
    }
  }

  pt_duckdb_rows_t rows;
  if (const db_status_t err = pt_duckdb_exec(
          m_session.connection(),
          "SELECT lower(table_name), table_type FROM information_schema.tables "
          "WHERE table_catalog = current_database() AND table_schema = 'main'",
          rows);
      !err.ok()) {
    return (err);
  }

  for (const std::vector<pt_duckdb_cell_t> &row : rows) {
    const std::string &name = row[0].m_string;
    const bool known = internal.erase(name) != 0;

    if (known) {
      if (row[1].m_string != "BASE TABLE") {
        return (store_corruption("unexpected object " + name));
      }

      continue;
    }

    if (name.compare(0, strlen(SCHEMA_RESERVED_PREFIX),
                     SCHEMA_RESERVED_PREFIX) == 0) {
      return (store_corruption("unexpected table " + name));
    }

    if (m_catalog.m_tables.count(name) != 0) {
      return (store_corruption("unexpected table " + name));
    }
  }

  if (!internal.empty()) {
    return (store_corruption("missing internal table " + *internal.begin()));
  }

  return (DB_SUCCESS);
}

/** Read SQL table names once for recovery conflict checks. */
db_status_t store::read_sql_tables(std::set<std::string> &names) {
  names.clear();
  pt_duckdb_rows_t rows;
  if (const db_status_t err = pt_duckdb_exec(
          m_session.connection(),
          "SELECT lower(table_name) FROM information_schema.tables "
          "WHERE table_catalog = current_database() AND table_schema = 'main' "
          "AND left(lower(table_name), 4) <> '_pt_'",
          rows);
      !err.ok()) {
    return (err);
  }

  for (const std::vector<pt_duckdb_cell_t> &row : rows) {
    names.insert(row[0].m_string);
  }

  return (DB_SUCCESS);
}

/** Resolve a SQL table name against the current DuckDB catalog. */
db_status_t store::sql_table_exists(const std::string &name, bool &exists) {
  exists = false;

  duckdb_connection connection = nullptr;
  if (duckdb_connect(m_db, &connection) != DuckDBSuccess) {
    return (db_status_t(DB_ERROR, "cannot connect to DuckDB"));
  }

  pt_duckdb_rows_t rows;
  const db_status_t err = pt_duckdb_exec(
      connection,
      "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
      "WHERE table_catalog = current_database() AND table_schema = 'main' "
      "AND lower(table_name) = " +
          pt_duckdb_literal(name) + ")",
      rows);

  duckdb_disconnect(&connection);
  if (!err.ok()) {
    return (err);
  }

  exists = rows[0][0].m_integer != 0;

  return (DB_SUCCESS);
}

/** Read partition metadata and check logical names, ranges and schemas.
@return DB_SUCCESS, a query error, or DB_CORRUPTION */
db_status_t store_catalog_reader::read_partitions() {
  pt_duckdb_rows_t rows;
  if (const db_status_t err = pt_duckdb_exec(
          m_connection,
          "SELECT table_id, start_hour, n_cols FROM _pt_partitions", rows);
      !err.ok()) {
    return (err);
  }

  std::map<uint64_t, store_table_t *> tables;
  for (auto &[name, table] : m_catalog.m_tables) {
    tables.emplace(table.m_table_id, &table);
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    store_partition_t p;
    p.m_table_id = static_cast<uint64_t>(rows[i][0].m_integer);
    p.m_start_hour = rows[i][1].m_integer;
    p.m_table_name = store_partition_name(p.m_table_id, p.m_start_hour);
    const int64_t n_cols = rows[i][2].m_integer;
    const auto owner = tables.find(p.m_table_id);
    if (owner == tables.end()) {
      return (store_corruption("partition refers to an unknown table"));
    }

    store_table_t &table = *owner->second;
    const int64_t duration = table.m_partition_duration_hours;
    p.m_column_count = static_cast<uint32_t>(n_cols);

    /* Check overlap without adding to an untrusted hour. Modulo checks
    alignment without rounding outside the int64_t range. */
    const bool hour_ok = p.m_start_hour > STORE_MIN_DAY * 24 - duration &&
                         p.m_start_hour < (STORE_MAX_DAY + 1) * 24 &&
                         p.m_start_hour % duration == 0;

    if (!table.m_initialized || !hour_ok || n_cols <= 0 ||
        n_cols > SCHEMA_MAX_COLUMNS ||
        p.m_column_count > table.m_schema.m_columns.size()) {
      return (store_corruption("bad partition " + p.m_table_name));
    }

    table.m_partitions.insert_or_assign(p.m_start_hour, std::move(p));
  }

  return (DB_SUCCESS);
}

/** Read the partitions and check them against the physical tables.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION if stored metadata
  is inconsistent */
db_status_t store::load_partitions() {
  /* Physical column definitions of every table in this file. */
  pt_duckdb_rows_t rows;

  if (const db_status_t err =
          pt_duckdb_exec(m_session.connection(),
                         "SELECT table_name, column_name, data_type, "
                         "ordinal_position FROM information_schema.columns "
                         "WHERE table_catalog = current_database() "
                         "AND table_schema = 'main' "
                         "ORDER BY table_name, ordinal_position",
                         rows);
      !err.ok()) {
    return (err);
  }

  std::map<std::string, std::vector<store_physical_column_t>> phys_cols;

  for (size_t i = 0; i < rows.size(); ++i) {
    store_physical_column_t col;
    col.m_name = rows[i][1].m_string;
    col.m_data_type = rows[i][2].m_string;
    col.m_position = rows[i][3].m_integer;
    phys_cols[rows[i][0].m_string].push_back(col);
  }

  if (const db_status_t err =
          store_catalog_reader(m_session.connection(), m_catalog)
              .read_partitions();
      !err.ok()) {
    return (err);
  }

  for (const auto &[name, table] : m_catalog.m_tables) {
    for (const auto &[hour, p] : table.m_partitions) {
      /* An old partition keeps a prefix of its table's newest schema. */
      const auto columns = phys_cols.find(p.m_table_name);
      if (columns == phys_cols.end() ||
          !store_partition_matches_schema(columns->second, table.m_schema,
                                          p.m_column_count)) {
        return (store_corruption("partition table " + p.m_table_name +
                                 " is missing or has the wrong columns"));
      }
    }
  }

  return (validate_catalog_tables());
}

/** Read only the internal metadata using an already active query
transaction. */
db_status_t store_catalog_reader::read() {
  m_catalog = store_catalog_t();
  if (const db_status_t err = read_metadata(); !err.ok()) {
    return (err);
  }

  /* SQL tables use DuckDB's MVCC catalog directly. Its complete
  physical-object validation belongs to store::open(), not every query.

*/
  return (read_partitions());
}

/** Load the catalog from the internal tables and check it.
@return DB_SUCCESS, DB_CORRUPTION or DB_ERROR */
db_status_t store::load_catalog() {
  m_catalog = store_catalog_t();

  if (const db_status_t err =
          store_catalog_reader(m_session.connection(), m_catalog)
              .read_metadata();
      !err.ok()) {
    return (err);
  }

  return (load_partitions());
}

/* ===================== Opening and maintenance ====================== */

/** Fill the descriptor of DuckDB's built-in function extension, linked
statically from its archive.
@return 0 if the extension descriptor was filled successfully; nonzero on
  failure */
extern "C" int32_t duckdb_extension_core_functions_describe(
    duckdb_extension_descriptor *descriptor);

/** Register the statically linked extensions before the first database is
opened. The static local runs once, even with concurrent callers; calling the
registration again would be a no-op anyway.
@return true if registration succeeded */
static bool pt_duckdb_register_extensions() {
  static const int32_t result = duckdb_register_static_extension(
      duckdb_extension_core_functions_describe);

  return (result == 0);
}

/** Settings for every database: no network, no extension loading. */
static const char *pt_duckdb_settings[][2] = {
    {"autoinstall_known_extensions", "false"},
    {"autoload_known_extensions", "false"},
};

/** Open the database and its storage connection. The caller releases config
on every return and closes any partially opened database on failure. */
db_status_t store::open_database(const char *path, duckdb_config &config) {
  /* Build startup settings with automatic extension installation and loading
  disabled. */
  if (duckdb_create_config(&config) != DuckDBSuccess) {
    return (DB_OUT_OF_MEMORY);
  }

  for (size_t i = 0;
       i < sizeof(pt_duckdb_settings) / sizeof(pt_duckdb_settings[0]); ++i) {
    if (duckdb_set_config(config, pt_duckdb_settings[i][0],
                          pt_duckdb_settings[i][1]) != DuckDBSuccess) {
      return (db_status_t(DB_ERROR, "cannot configure DuckDB"));
    }
  }

  if (m_memory_limit_bytes != 0) {
    const std::string memory_limit =
        std::to_string(m_memory_limit_bytes) + " B";
    if (duckdb_set_config(config, "memory_limit", memory_limit.c_str()) !=
        DuckDBSuccess) {
      return (db_status_t(DB_INVALID_ARGUMENT,
                          "cannot configure DuckDB memory limit"));
    }
  }

  if (m_threads != 0) {
    const std::string threads = std::to_string(m_threads);
    if (duckdb_set_config(config, "threads", threads.c_str()) !=
        DuckDBSuccess) {
      return (
          db_status_t(DB_INVALID_ARGUMENT, "cannot configure DuckDB threads"));
    }
  }

  /* Open the existing database file or create a new one with these settings. */
  char *error = nullptr;

  const duckdb_state result = duckdb_open_ext(path, &m_db, config, &error);

  /* Preserve the diagnostic before releasing DuckDB's error text. */
  const db_status_t err = result == DuckDBSuccess ? db_status_t(DB_SUCCESS)
                                                  : pt_duckdb_error(error);
  duckdb_free(error);
  if (!err.ok()) {
    return (err);
  }

  /* Create the connection used by storage statements. */
  if (duckdb_connect(m_db, &m_session.m_connection) != DuckDBSuccess) {
    return (db_status_t(DB_ERROR, "cannot connect to DuckDB"));
  }

  /* Keep qualified SQL names stable and switch this connection to the new
  alias. DuckDB derives its initial alias from the storage filename. */
  if (const db_status_t err =
          pt_duckdb_exec(m_session.connection(),
                         "ALTER DATABASE _pt_ SET ALIAS TO data; USE data");
      !err.ok()) {
    return (err.with_context("cannot set DuckDB database alias"));
  }

  m_memory_limit_bytes = pt_duckdb_memory_limit_bytes(m_session.connection());

  pt_duckdb_rows_t rows;
  if (const db_status_t err = pt_duckdb_exec(
          m_session.connection(), "SELECT current_setting('threads')", rows);
      !err.ok()) {
    return (err.with_context("cannot read DuckDB threads"));
  }

  if (rows.size() != 1 || rows[0].size() != 1 || rows[0][0].m_is_null ||
      rows[0][0].m_integer < 1 ||
      static_cast<uint64_t>(rows[0][0].m_integer) > UINT32_MAX) {
    return (db_status_t(DB_ERROR, "invalid DuckDB thread setting"));
  }

  m_threads = static_cast<uint32_t>(rows[0][0].m_integer);

  return (DB_SUCCESS);
}

/** Open DuckDB handles and initialize or restore the storage catalog. */
db_status_t store::open_low(const char *dir, uint32_t partition_duration_hours,
                            duckdb_config &config, bool initialize_catalog) {
  const std::string path = std::string(dir) + "/" + STORE_FILE_NAME;

  if (const db_status_t err = open_database(path.c_str(), config); !err.ok()) {
    return (err);
  }

  pt_duckdb_rows_t rows;
  if (const db_status_t err =
          pt_duckdb_exec(m_session.connection(),
                         "SELECT NOT EXISTS ("
                         "SELECT 1 FROM information_schema.tables "
                         "WHERE table_schema = 'main')",
                         rows);
      !err.ok()) {
    return (err);
  }

  const bool is_empty = rows[0][0].m_integer != 0;

  if (is_empty) {
    if (!initialize_catalog) {
      return (store_corruption("initialized storage catalog is missing"));
    }

    /* Create the internal tables before loading the catalog. */
    if (const db_status_t err = create_catalog(partition_duration_hours);
        !err.ok()) {
      return (err);
    }
  }

  return (load_catalog());
}

/** Open native handles and load the catalog; all failures share one cleanup. */
db_status_t store::open(const char *dir, uint32_t partition_duration_hours,
                        store **out, uint64_t memory_limit_bytes,
                        uint32_t threads) {
  *out = nullptr;

  store *st = ut_new<store>();
  if (st == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  if (const db_status_t err = st->initialize(dir, partition_duration_hours,
                                             memory_limit_bytes, threads, true);
      !err.ok()) {
    ut_delete(st);

    return (err);
  }

  *out = st;

  return (DB_SUCCESS);
}

/** Configure and initialize this store before its owner publishes it. */
db_status_t store::initialize(const char *dir,
                              uint32_t partition_duration_hours,
                              uint64_t memory_limit_bytes, uint32_t threads,
                              bool initialize_catalog) {
  if (partition_duration_hours < 1 ||
      partition_duration_hours > STORE_MAX_PARTITION_HOURS) {
    return (
        db_status_t(DB_INVALID_ARGUMENT, "partition duration is out of range"));
  }

  if (threads > PT_DUCKDB_MAX_THREADS) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "DuckDB threads must be at most " +
                            std::to_string(PT_DUCKDB_MAX_THREADS)));
  }

  if (memory_limit_bytes > PT_DUCKDB_MAX_MEMORY_LIMIT_BYTES) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "DuckDB memory limit must be at most 2^53 bytes"));
  }

  if (!pt_duckdb_register_extensions()) {
    return (
        db_status_t(DB_ERROR, "cannot register DuckDB's built-in functions"));
  }

  m_memory_limit_bytes = memory_limit_bytes;
  m_threads = threads;

  duckdb_config config = nullptr;
  const db_status_t err =
      open_low(dir, partition_duration_hours, config, initialize_catalog);
  duckdb_destroy_config(&config);

  return (err);
}

/** Reject duplicates before starting a declaration transaction. */
db_status_t store::create_table(const std::string &name,
                                uint32_t partition_hours,
                                std::optional<uint32_t> retention_hours) {
  if (m_catalog.m_tables.count(name) != 0) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "time-series table already exists: " + name));
  }

  return (write_table_options(name, partition_hours, retention_hours));
}

/** Remove a whole table, preserving the flushed boundary and the next id. */
db_status_t store::drop_table(const std::string &name) {
  const auto table = m_catalog.m_tables.find(name);
  if (table == m_catalog.m_tables.end()) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "time-series table does not exist: " + name));
  }

  const uint64_t id = table->second.m_table_id;

  if (const db_status_t err = m_session.begin(); !err.ok()) {
    return (err);
  }

  for (const auto &[hour, partition] : table->second.m_partitions) {
    if (const db_status_t err = pt_duckdb_exec(
            m_session.connection(), "DROP TABLE " + partition.m_table_name);
        !err.ok()) {
      return (m_session.rollback(err));
    }
  }

  for (const char *directory :
       {"_pt_partitions", "_pt_columns", "_pt_tables"}) {
    if (const db_status_t err =
            pt_duckdb_exec(m_session.connection(),
                           "DELETE FROM " + std::string(directory) +
                               " WHERE table_id = " + std::to_string(id));
        !err.ok()) {
      return (m_session.rollback(err));
    }
  }

  if (const db_status_t err = UT_DEBUG_FAULT("store.drop_table"); !err.ok()) {
    return (m_session.rollback(err));
  }

  if (const db_status_t err = m_session.commit(); !err.ok()) {
    return (err);
  }

  m_catalog.m_tables.erase(table);

  return (DB_SUCCESS);
}

/** Persist settings, keeping an existing schema and partition duration. */
db_status_t store::set_table_options(const std::string &name,
                                     uint32_t partition_hours,
                                     std::optional<uint32_t> retention_hours) {
  return (write_table_options(name, partition_hours, retention_hours));
}

/** Commit metadata only, preserving an existing column directory. */
db_status_t store::write_table_options(
    const std::string &name, uint32_t partition_hours,
    std::optional<uint32_t> retention_hours) {
  if (partition_hours == 0 || partition_hours > STORE_MAX_PARTITION_HOURS ||
      retention_hours.value_or(0) > STORE_MAX_RETENTION_HOURS) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "table partition or retention hours out of range"));
  }

  const auto existing = m_catalog.m_tables.find(name);
  const bool creating = existing == m_catalog.m_tables.end();
  if (creating && m_catalog.m_next_table_id == UINT64_MAX) {
    return (DB_LIMIT_EXCEEDED);
  }

  if (!creating &&
      existing->second.m_partition_duration_hours != partition_hours) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "table partition span is fixed: " + name));
  }

  const uint64_t id =
      creating ? m_catalog.m_next_table_id : existing->second.m_table_id;
  std::map<std::string, store_table_t> pending;
  if (creating) {
    /* Allocate the new map node before committing the transaction. */
    store_table_t &table = pending.try_emplace(name).first->second;
    table.m_table_id = id;
    table.m_schema.m_name = name;
    table.m_partition_duration_hours = partition_hours;
    table.m_retention_hours = retention_hours;
  }

  const std::string value =
      retention_hours ? std::to_string(*retention_hours) : "NULL";
  const std::string sql =
      creating ? "INSERT INTO _pt_tables VALUES (" + std::to_string(id) + ", " +
                     pt_duckdb_literal(name) + ", false, " +
                     std::to_string(partition_hours) + ", " + value + ")"
               : "UPDATE _pt_tables SET retention_hours = " + value +
                     " WHERE table_id = " + std::to_string(id);

  if (const db_status_t err = m_session.begin(); !err.ok()) {
    return (err);
  }

  if (const db_status_t err = pt_duckdb_exec(m_session.connection(), sql);
      !err.ok()) {
    return (m_session.rollback(err));
  }

  if (creating) {
    if (const db_status_t err = pt_duckdb_exec(
            m_session.connection(),
            "UPDATE _pt_state SET next_table_id = " + std::to_string(id + 1));
        !err.ok()) {
      return (m_session.rollback(err));
    }
  }

  if (const db_status_t err = UT_DEBUG_FAULT("store.table_options");
      !err.ok()) {
    return (m_session.rollback(err));
  }

  if (const db_status_t err = m_session.commit(); !err.ok()) {
    return (err);
  }

  if (creating) {
    m_catalog.m_next_table_id = id + 1;
    m_catalog.m_tables.merge(pending);
  } else {
    existing->second.m_retention_hours = retention_hours;
  }

  return (DB_SUCCESS);
}

/** Select partitions without changing the catalog. */
void store::collect_expired_partitions(
    int64_t now_us, uint32_t default_retention_hours,
    std::vector<store_partition_key_t> &expired_partitions) const {
  expired_partitions.clear();

  /* Collect expired partitions using each table's effective retention. */
  for (const auto &[name, table] : m_catalog.m_tables) {
    const uint32_t retention =
        table.m_retention_hours.value_or(default_retention_hours);

    if (retention == 0) {
      continue;
    }

    const int64_t end_hour =
        ut_utc_hour(now_us - static_cast<int64_t>(retention) * UT_HOUR_US);

    for (const auto &[start_hour, partition] : table.m_partitions) {
      if (start_hour + table.m_partition_duration_hours > end_hour) {
        break;
      }

      expired_partitions.emplace_back(table.m_table_id, start_hour);
    }
  }

  return;
}

/** Commit partition deletion before removing its catalog entries. */
db_status_t store::expire_partitions(
    const std::vector<store_partition_key_t> &expired_partitions,
    uint64_t &removed) {
  removed = 0;

  if (expired_partitions.empty()) {
    return (DB_SUCCESS);
  }

  std::map<uint64_t, store_table_t *> tables;
  for (auto &[name, table] : m_catalog.m_tables) {
    tables.emplace(table.m_table_id, &table);
  }

  /* The physical tables and their catalog rows go together. */
  if (const db_status_t err = m_session.begin(); !err.ok()) {
    return (err);
  }

  for (const auto &[id, start_hour] : expired_partitions) {
    const store_partition_t &p = tables.at(id)->m_partitions.at(start_hour);

    if (const db_status_t err = pt_duckdb_exec(m_session.connection(),
                                               "DROP TABLE " + p.m_table_name);
        !err.ok()) {
      return (m_session.rollback(err));
    }

    if (const db_status_t err = pt_duckdb_exec(
            m_session.connection(),
            "DELETE FROM _pt_partitions WHERE table_id = " +
                std::to_string(p.m_table_id) +
                " AND start_hour = " + std::to_string(p.m_start_hour));
        !err.ok()) {
      return (m_session.rollback(err));
    }
  }

  if (const db_status_t err = m_session.commit(); !err.ok()) {
    return (err);
  }

  for (const auto &[id, start_hour] : expired_partitions) {
    tables.at(id)->m_partitions.erase(start_hour);
  }

  removed = expired_partitions.size();

  return (DB_SUCCESS);
}

/** Disconnect the storage session before closing the database handle. */
void store::close() {
  duckdb_disconnect(&m_session.m_connection);
  duckdb_close(&m_db);

  return;
}

/** Release handles left by successful or partial initialization. */
store::~store() {
  close();
}

/** Delete a storage owner; NULL is accepted. */
void store::destroy(store *st) {
  ut_delete(st);

  return;
}

/** Return the native database handle for opening query connections. */
duckdb_database store::database() {
  return (m_db);
}

/** Return the resolved memory budget for this open. */
uint64_t store::memory_limit_bytes() const {
  return (m_memory_limit_bytes);
}

/** Return the effective execution thread setting selected at open. */
uint32_t store::threads() const {
  return (m_threads);
}

/** Return the storage connection for database initialization. */
duckdb_connection store::connection() {
  return (m_session.connection());
}

/** Return the current committed catalog without copying it. */
const store_catalog_t &store::catalog() {
  return (m_catalog);
}

/* ======================= Transactional append ======================= */

/* Resolve tables, columns and partitions in a private catalog first, then
append each partition's rows in sequence order. Publish the catalog only after
its metadata and rows have committed in the same transaction. */

/** The rows of one write that fall in one partition, in batch order. */
struct store_slice_t {
  /** Write containing the selected rows; valid throughout the append. */
  const store_write_t *m_write{nullptr};

  /** Indexes of its rows that fall in the partition. */
  std::vector<uint32_t> m_row_indices;
};

/** Write slices grouped by partition, in sequence order. */
typedef std::map<store_partition_key_t, std::vector<store_slice_t>>
    store_append_plan_t;

/** One append's metadata work and partition append plan. */
class store::append_context final : private ut_noncopyable {
 public:
  /** Stage catalog changes in the active transaction. */
  append_context(pt_duckdb_session &session, const store_catalog_t &base,
                 store_catalog_t &changes)
      : m_session(session), m_base(base), m_changes(changes) {}

  /** Resolve metadata and group one write into partition slices. */
  db_status_t resolve_write(const store_write_t &w);

  /** Persist touched partition counts and the new stored boundary. */
  db_status_t write_catalog();

  /** Append all resolved partition slices in plan order. */
  db_status_t append_rows();

 private:
  /** Find or create the stored logical table. */
  db_status_t ensure_table(const std::string &name, store_table_t *&out);

  /** Extend the stored logical column directory. */
  db_status_t ensure_schema(store_table_t &table, const schema_t &schema);

  /** Find or create a physical time partition. */
  db_status_t ensure_partition(store_table_t &table,
                               const store_table_t *base_table,
                               int64_t start_hour, const schema_t &schema,
                               store_partition_t *&out);

  /** Add missing physical columns to the partition. */
  db_status_t ensure_columns(store_partition_t &p, const schema_t &schema);

  /** Append the planned slices of a partition. */
  db_status_t append_partition(store_partition_t &p, const schema_t &full,
                               const std::vector<store_slice_t> &slices);

  /** Session running the append transaction. */
  pt_duckdb_session &m_session;

  /** Committed metadata, unchanged until the append commits. */
  const store_catalog_t &m_base;

  /** Uncommitted metadata; only touched entries are copied here. */
  store_catalog_t &m_changes;

  /** Row slices grouped by physical partition. */
  store_append_plan_t m_plan;

  /** Staged tables resolved in phase 1; their map entries remain stable. */
  std::map<uint64_t, store_table_t *> m_tables_by_id;
};

/** Find the table of a write, adding it to the catalog and _pt_tables if it is
new.
@param[in]  name  normalized logical table name
@return DB_SUCCESS, DB_LIMIT_EXCEEDED if table ids are exhausted, or a
  catalog insertion error */
db_status_t store::append_context::ensure_table(const std::string &name,
                                                store_table_t *&out) {
  std::map<std::string, store_table_t>::iterator it =
      m_changes.m_tables.find(name);

  if (it != m_changes.m_tables.end()) {
    out = &it->second;

    return (DB_SUCCESS);
  }

  const auto existing = m_base.m_tables.find(name);
  if (existing != m_base.m_tables.end()) {
    const store_table_t &base = existing->second;
    store_table_t &table = m_changes.m_tables.try_emplace(name).first->second;

    /* Stage table metadata now and copy partitions as writes reach them. */
    table.m_table_id = base.m_table_id;
    table.m_partition_duration_hours = base.m_partition_duration_hours;
    table.m_retention_hours = base.m_retention_hours;
    table.m_initialized = base.m_initialized;
    table.m_schema = base.m_schema;
    out = &table;

    return (DB_SUCCESS);
  }

  if (m_changes.m_next_table_id == std::numeric_limits<uint64_t>::max()) {
    return (db_status_t(DB_LIMIT_EXCEEDED, "table ids are exhausted"));
  }

  store_table_t table;
  table.m_table_id = m_changes.m_next_table_id++;
  table.m_partition_duration_hours = m_base.m_partition_duration_hours;
  table.m_schema.m_name = name;

  const std::string sql =
      "INSERT INTO _pt_tables VALUES (" + std::to_string(table.m_table_id) +
      ", " + pt_duckdb_literal(name) + ", false, " +
      std::to_string(table.m_partition_duration_hours) + ", NULL)";

  if (const db_status_t status = pt_duckdb_exec(m_session.connection(), sql);
      !status.ok()) {
    return (status);
  }

  out = &m_changes.m_tables.emplace(name, std::move(table)).first->second;

  return (DB_SUCCESS);
}

/** Extend the stored column directory when a write adds columns.
Older batches may use any valid prefix of the current directory.
@return DB_SUCCESS, DB_INVALID_ARGUMENT for an invalid directory, or
  a classified DuckDB catalog failure */
db_status_t store::append_context::ensure_schema(store_table_t &table,
                                                 const schema_t &schema) {
  const schema_t &stored = table.m_schema;
  const size_t n_stored = stored.m_columns.size();
  const size_t n_common = std::min(n_stored, schema.m_columns.size());

  for (size_t i = 0; i < n_common; ++i) {
    const schema_column_t &a = stored.m_columns[i];
    const schema_column_t &b = schema.m_columns[i];

    if (a.m_id != b.m_id || a.m_name != b.m_name ||
        a.m_column_type != b.m_column_type || a.m_data_type != b.m_data_type) {
      return (db_status_t(DB_INVALID_ARGUMENT,
                          "schema changes an existing column"));
    }
  }

  if (schema.m_columns.size() <= n_stored) {
    return (DB_SUCCESS);
  }

  for (size_t i = n_stored; i < schema.m_columns.size(); ++i) {
    const schema_column_t &c = schema.m_columns[i];
    const std::string sql =
        "INSERT INTO _pt_columns VALUES (" + std::to_string(table.m_table_id) +
        ", " + std::to_string(c.m_id) + ", " + pt_duckdb_literal(c.m_name) +
        ", " + std::to_string(static_cast<int>(c.m_column_type)) + ", " +
        std::to_string(static_cast<int>(c.m_data_type)) + ")";

    if (const db_status_t err = pt_duckdb_exec(m_session.connection(), sql);
        !err.ok()) {
      return (err);
    }
  }

  if (!table.m_initialized) {
    if (const db_status_t err =
            pt_duckdb_exec(m_session.connection(),
                           "UPDATE _pt_tables SET initialized = true "
                           "WHERE table_id = " +
                               std::to_string(table.m_table_id));
        !err.ok()) {
      return (err);
    }

    table.m_initialized = true;
  }

  table.m_schema = schema;

  return (DB_SUCCESS);
}

/** Find a partition, creating its physical table if it is new.
@param[in]  start_hour  first UTC hour of the partition, relative to
                       1970-01-01
@return DB_SUCCESS or a classified DuckDB creation failure */
db_status_t store::append_context::ensure_partition(
    store_table_t &table, const store_table_t *base_table, int64_t start_hour,
    const schema_t &schema, store_partition_t *&out) {
  const auto it = table.m_partitions.find(start_hour);
  if (it != table.m_partitions.end()) {
    out = &it->second;

    return (DB_SUCCESS);
  }

  if (base_table != nullptr) {
    const auto existing = base_table->m_partitions.find(start_hour);
    if (existing != base_table->m_partitions.end()) {
      out = &table.m_partitions.emplace(start_hour, existing->second)
                 .first->second;

      return (DB_SUCCESS);
    }
  }

  store_partition_t p;
  p.m_table_id = table.m_table_id;
  p.m_start_hour = start_hour;
  p.m_table_name = store_partition_name(table.m_table_id, start_hour);
  p.m_column_count = static_cast<uint32_t>(schema.m_columns.size());

  /* Row identity, then the columns by id. The time column must lie in
  the partition's hours. */
  std::string sql = "CREATE TABLE " + p.m_table_name +
                    " (_pt_seq UBIGINT NOT NULL, _pt_row UINTEGER NOT NULL";

  for (uint32_t c = 0; c < p.m_column_count; ++c) {
    sql += ", c" + std::to_string(c) + " " +
           schema_data_type_name(schema.m_columns[c].m_data_type);
  }

  const int64_t end_hour = start_hour + table.m_partition_duration_hours;

  sql += ", CHECK (c0 >= " + ut_timestamp_literal(start_hour * UT_HOUR_US) +
         " AND c0 < " + ut_timestamp_literal(end_hour * UT_HOUR_US) + "))";

  if (const db_status_t status = pt_duckdb_exec(m_session.connection(), sql);
      !status.ok()) {
    return (status);
  }

  sql = "INSERT INTO _pt_partitions VALUES (" +
        std::to_string(table.m_table_id) + ", " + std::to_string(start_hour) +
        ", " + std::to_string(p.m_column_count) + ")";

  if (const db_status_t status = pt_duckdb_exec(m_session.connection(), sql);
      !status.ok()) {
    return (status);
  }

  out = &table.m_partitions.emplace(start_hour, std::move(p)).first->second;

  return (DB_SUCCESS);
}

/** Add to a partition the columns a write has and it lacks.
@param[in]  schema  write schema whose missing columns must be added
@return DB_SUCCESS or a classified DuckDB failure when adding a column */
db_status_t store::append_context::ensure_columns(store_partition_t &p,
                                                  const schema_t &schema) {
  const uint32_t before = p.m_column_count;
  while (p.m_column_count < schema.m_columns.size()) {
    const schema_column_t &c = schema.m_columns[p.m_column_count];

    const std::string sql = "ALTER TABLE " + p.m_table_name + " ADD COLUMN c" +
                            std::to_string(c.m_id) + " " +
                            schema_data_type_name(c.m_data_type);

    if (const db_status_t status = pt_duckdb_exec(m_session.connection(), sql);
        !status.ok()) {
      return (status);
    }

    p.m_column_count++;
  }

  if (p.m_column_count != before) {
    return (pt_duckdb_exec(
        m_session.connection(),
        "UPDATE _pt_partitions SET n_cols = " +
            std::to_string(p.m_column_count) +
            " WHERE table_id = " + std::to_string(p.m_table_id) +
            " AND start_hour = " + std::to_string(p.m_start_hour)));
  }

  return (DB_SUCCESS);
}

/** Phase 1 for one write: its table, column directory and partitions. Adds the
write's rows, grouped by partition, to the append plan.
@return DB_SUCCESS or the table/schema/partition catalog error */
db_status_t store::append_context::resolve_write(const store_write_t &w) {
  store_table_t *table = nullptr;

  if (const db_status_t err = ensure_table(w.m_batch->m_table_name, table);
      !err.ok()) {
    return (err);
  }

  if (const db_status_t err = ensure_schema(*table, *w.m_schema); !err.ok()) {
    return (err);
  }

  m_tables_by_id[table->m_table_id] = table;
  const auto base = m_base.m_tables.find(w.m_batch->m_table_name);
  const store_table_t *base_table =
      base == m_base.m_tables.end() ? nullptr : &base->second;

  /* Group the rows by partition, keeping their order. */
  std::map<int64_t, std::vector<uint32_t>> parts;
  const batch_column_t &time_col = w.m_batch->m_columns[0];

  for (uint32_t r = 0; r < w.m_batch->m_row_count; ++r) {
    const int64_t hour = ut_utc_hour(time_col.m_integers[r]);

    parts[store_partition_start_hour(hour, table->m_partition_duration_hours)]
        .push_back(r);
  }

  std::map<int64_t, std::vector<uint32_t>>::iterator it;

  for (it = parts.begin(); it != parts.end(); ++it) {
    store_partition_t *p = nullptr;

    if (const db_status_t err =
            ensure_partition(*table, base_table, it->first, *w.m_schema, p);
        !err.ok()) {
      return (err);
    }

    if (const db_status_t err = ensure_columns(*p, *w.m_schema); !err.ok()) {
      return (err);
    }

    store_slice_t slice;
    slice.m_write = &w;
    slice.m_row_indices.swap(it->second);

    m_plan[store_partition_key_t(table->m_table_id, it->first)].push_back(
        std::move(slice));
  }

  return (DB_SUCCESS);
}

/** A data chunk being filled for one partition: _pt_seq, _pt_row, then
c0 .. c<n_cols - 1>. */
class store_chunk final : private ut_noncopyable {
 public:
  /** Release the native chunk. */
  ~store_chunk() { duckdb_destroy_data_chunk(&m_chunk); }

  /** Allocate buffers for a physical partition's column prefix. */
  db_status_t init(const schema_t &full, uint32_t n_cols);

  /** Start filling the chunk again, refreshing its native buffers. */
  void reset();

  /** Append one batch row; absent trailing columns become NULL. */
  void add_row(batch_seq_t seq, const batch_t &batch, uint32_t row);

  /** Pass filled rows to an appender and reset the chunk. */
  bool append_to(duckdb_appender app);

  /** Check whether the next row requires appending this chunk first. */
  bool full() const { return (m_row_count == m_row_capacity); }

 private:
  /** The chunk, reused for every batch of rows. */
  duckdb_data_chunk m_chunk{nullptr};

  /** Column types of the partition, c0 first. */
  std::vector<data_type_t> m_data_types;

  /** Rows a chunk can hold. */
  idx_t m_row_capacity{0};

  /** Rows in it now. */
  idx_t m_row_count{0};

  /** Vector of each chunk column. */
  std::vector<duckdb_vector> m_vectors;

  /** Data of each chunk column. */
  std::vector<void *> m_data;

  /** Validity mask of each chunk column. */
  std::vector<uint64_t *> m_validity;
};

/** Empty the chunk and look up its column buffers again. */
void store_chunk::reset() {
  duckdb_data_chunk_reset(m_chunk);

  for (size_t i = 0; i < m_vectors.size(); ++i) {
    duckdb_vector v = duckdb_data_chunk_get_vector(m_chunk, i);

    /* Every value starts valid; NULLs are marked one by one. */
    duckdb_vector_ensure_validity_writable(v);

    m_vectors[i] = v;
    m_data[i] = duckdb_vector_get_data(v);
    m_validity[i] = duckdb_vector_get_validity(v);
  }

  m_row_count = 0;

  return;
}

/** Create the chunk for a partition's columns.
@return DB_SUCCESS or DB_OUT_OF_MEMORY if DuckDB cannot create the chunk */
db_status_t store_chunk::init(const schema_t &full, uint32_t n_cols) {
  const size_t n = 2 + static_cast<size_t>(n_cols);

  m_data_types.resize(n_cols);
  m_vectors.resize(n);
  m_data.resize(n);
  m_validity.resize(n);

  {
    std::vector<duckdb_logical_type> logical(n);
    logical[0] = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    logical[1] = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER);

    for (uint32_t k = 0; k < n_cols; ++k) {
      m_data_types[k] = full.m_columns[k].m_data_type;
      logical[2 + k] =
          duckdb_create_logical_type(pt_duckdb_convert_type(m_data_types[k]));
    }

    m_chunk = duckdb_create_data_chunk(logical.data(), n);
    for (duckdb_logical_type &type : logical) {
      duckdb_destroy_logical_type(&type);
    }
  }

  if (m_chunk == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  m_row_capacity = duckdb_vector_size();
  reset();

  return (DB_SUCCESS);
}

/** Copy row r of a batch into the next row of the chunk. Columns the batch
does not have, because they were added later, are NULL. */
void store_chunk::add_row(batch_seq_t seq, const batch_t &batch, uint32_t r) {
  ut_ad(m_row_count < m_row_capacity);
  ut_ad(r < batch.m_row_count);

  const idx_t i = m_row_count;

  static_cast<uint64_t *>(m_data[0])[i] = seq;
  static_cast<uint32_t *>(m_data[1])[i] = r;

  for (size_t k = 0; k < m_data_types.size(); ++k) {
    const size_t dst = 2 + k;

    /* An older batch lacks the columns added after it. */
    if (k >= batch.m_columns.size() || batch.m_columns[k].m_nulls[r]) {
      duckdb_validity_set_row_invalid(m_validity[dst], i);
      continue;
    }

    const batch_column_t &col = batch.m_columns[k];

    switch (m_data_types[k]) {
      case data_type_t::BOOLEAN:
        static_cast<bool *>(m_data[dst])[i] = col.m_booleans[r] != 0;
        break;

      case data_type_t::BIGINT:
        static_cast<int64_t *>(m_data[dst])[i] = col.m_integers[r];
        break;

      case data_type_t::DOUBLE:
        static_cast<double *>(m_data[dst])[i] = col.m_doubles[r];
        break;

      case data_type_t::TIMESTAMP:
        static_cast<duckdb_timestamp *>(m_data[dst])[i].micros =
            col.m_integers[r];
        break;

      case data_type_t::VARCHAR: {
        const uint32_t start = col.m_string_offsets[r];
        const uint32_t len = col.m_string_offsets[r + 1] - start;

        duckdb_vector_assign_string_element_len(
            m_vectors[dst], i, col.m_string_data.data() + start, len);
        break;
      }
    }
  }

  m_row_count++;

  return;
}

/** Hand the filled rows of the chunk to the appender and start over.
@return true if the chunk is empty or DuckDB accepts it; false on append
  failure */
bool store_chunk::append_to(duckdb_appender app) {
  if (m_row_count == 0) {
    return (true);
  }

  duckdb_data_chunk_set_size(m_chunk, m_row_count);

  const bool ok = duckdb_append_data_chunk(app, m_chunk) == DuckDBSuccess;

  reset();

  return (ok);
}

/** Copy an appender failure before releasing the handle.
@return the classified DuckDB failure */
static db_status_t store_appender_error(duckdb_appender app) {
  const char *message = app != nullptr ? duckdb_appender_error(app) : nullptr;

  return (pt_duckdb_error(message));
}

/** Append all slices, then check flush and close; the caller destroys app. */
static db_status_t store_append_slices(
    store_chunk &chunk, duckdb_appender app,
    const std::vector<store_slice_t> &slices) {
  for (size_t s = 0; s < slices.size(); ++s) {
    const store_slice_t &slice = slices[s];

    for (size_t i = 0; i < slice.m_row_indices.size(); ++i) {
      chunk.add_row(slice.m_write->m_seq, *slice.m_write->m_batch,
                    slice.m_row_indices[i]);

      if (chunk.full()) {
        if (!chunk.append_to(app)) {
          return (store_appender_error(app));
        }
      }
    }
  }

  if (!chunk.append_to(app)) {
    return (store_appender_error(app));
  }

  /* Appended data is buffered; flushing it checks the rows and hands them
  to the transaction. Its result must be checked like any other. */
  if (duckdb_appender_flush(app) != DuckDBSuccess) {
    return (store_appender_error(app));
  }

  if (duckdb_appender_close(app) != DuckDBSuccess) {
    return (store_appender_error(app));
  }

  return (DB_SUCCESS);
}

/** Phase 2 for one partition: append the rows of all its slices.
@return DB_SUCCESS, DB_OUT_OF_MEMORY if chunk creation fails, or a classified
  DuckDB appender failure */
db_status_t store::append_context::append_partition(
    store_partition_t &p, const schema_t &full,
    const std::vector<store_slice_t> &slices) {
  ut_a(p.m_column_count <= full.m_columns.size());

  store_chunk chunk;

  if (const db_status_t err = chunk.init(full, p.m_column_count); !err.ok()) {
    return (db_status_t(err.code(), "cannot create a data chunk"));
  }

  duckdb_appender app = nullptr;
  if (duckdb_appender_create(m_session.connection(), nullptr,
                             p.m_table_name.c_str(), &app) != DuckDBSuccess) {
    const db_status_t err = store_appender_error(app);
    static_cast<void>(duckdb_appender_destroy(&app));

    return (err);
  }

  const db_status_t err = store_append_slices(chunk, app, slices);
  static_cast<void>(duckdb_appender_destroy(&app));

  return (err);
}

/** Persist the stored boundary and next table identity.
@return DB_SUCCESS or a classified DuckDB catalog-update failure */
db_status_t store::append_context::write_catalog() {
  const std::string sql =
      "UPDATE _pt_state SET flushed_seq = " +
      std::to_string(m_changes.m_flushed_seq) +
      ", next_table_id = " + std::to_string(m_changes.m_next_table_id);

  return (pt_duckdb_exec(m_session.connection(), sql));
}

/** Assert write sequences, batches and schemas before the transaction. */
void store::validate_append(const store_write_t *writes, size_t n) const {
  ut_a(writes != nullptr);
  ut_a(n > 0);

  for (size_t i = 0; i < n; ++i) {
    const store_write_t &w = writes[i];

    ut_a(w.m_seq == m_catalog.m_flushed_seq + 1 + i);
    ut_a(w.m_batch != nullptr);
    ut_a(w.m_schema != nullptr);
    ut_a(w.m_batch->m_row_count > 0);
    ut_a(w.m_schema->m_name == w.m_batch->m_table_name);
    ut_a(!w.m_batch->m_columns.empty());
    ut_a(w.m_schema->m_columns.size() >= w.m_batch->m_columns.size());

    for (size_t c = 0; c < w.m_batch->m_columns.size(); ++c) {
      ut_a(w.m_batch->m_columns[c].m_column_id == c);
      ut_a(w.m_batch->m_columns[c].m_data_type ==
           w.m_schema->m_columns[c].m_data_type);
      ut_ad(w.m_batch->m_columns[c].validate(w.m_batch->m_row_count));
    }
  }

  return;
}

/** Append all partitions after metadata resolution has completed. */
db_status_t store::append_context::append_rows() {
  for (const auto &[key, slices] : m_plan) {
    store_table_t &table = *m_tables_by_id.at(key.first);
    store_partition_t &p = table.m_partitions.at(key.second);

    if (const db_status_t err = append_partition(p, table.m_schema, slices);
        !err.ok()) {
      return (err);
    }
  }

  return (DB_SUCCESS);
}

/** Store writes inside the transaction, with pending catalog changes.
@param[in]  writes  consecutive writes starting immediately after the
                    stored sequence number
@return DB_SUCCESS, DB_CORRUPTION for a changed stored boundary, or a
  catalog/appender error; the caller handles rollback */
db_status_t store::append_low(const store_write_t *writes, size_t n,
                              store_catalog_t &next) {
  /* The file must still be where this handle left it. */
  pt_duckdb_rows_t rows;
  if (const db_status_t status = pt_duckdb_exec(
          m_session.connection(), "SELECT flushed_seq FROM _pt_state", rows);
      !status.ok()) {
    return (status);
  }

  if (rows.size() != 1 || rows[0].size() != 1 || rows[0][0].m_is_null) {
    return (db_status_t(
        DB_CORRUPTION,
        "_pt_state must return exactly one non-NULL flushed_seq value"));
  }

  if (static_cast<batch_seq_t>(rows[0][0].m_integer) !=
      m_catalog.m_flushed_seq) {
    return (db_status_t(DB_CORRUPTION,
                        "flushed_seq in the file differs from the open state"));
  }

  /* Phase 1: tables, column directories and partitions. */
  append_context context(m_session, m_catalog, next);

  for (size_t i = 0; i < n; ++i) {
    if (const db_status_t err = context.resolve_write(writes[i]); !err.ok()) {
      return (err);
    }
  }

  /* Phase 2: the rows, one appender per partition. */
  if (const db_status_t appended = context.append_rows(); !appended.ok()) {
    return (appended);
  }

  if (const db_status_t err = UT_DEBUG_FAULT("store.append"); !err.ok()) {
    return (err);
  }

  /* The arguments were checked: consecutive writes right after F. */
  next.m_flushed_seq = writes[n - 1].m_seq;
  ut_a(next.m_flushed_seq == m_catalog.m_flushed_seq + n);

  return (context.write_catalog());
}

/** Store consecutive writes and publish their catalog after commit. */
db_status_t store::append(const store_write_t *writes, size_t n) {
  validate_append(writes, n);

  if (const db_status_t err = m_session.begin(); !err.ok()) {
    return (err);
  }

  store_catalog_t next;
  next.m_wal_id = m_catalog.m_wal_id;
  next.m_flushed_seq = m_catalog.m_flushed_seq;
  next.m_next_table_id = m_catalog.m_next_table_id;
  next.m_partition_duration_hours = m_catalog.m_partition_duration_hours;

  if (const db_status_t err = append_low(writes, n, next); !err.ok()) {
    return (m_session.rollback(err));
  }

  /* Publish only after COMMIT confirms the transaction. Failed operations
  leave the committed catalog unchanged, including uncertain outcomes. */
  if (const db_status_t err = m_session.commit(); !err.ok()) {
    return (err);
  }

  for (auto &[name, table] : next.m_tables) {
    const auto existing = m_catalog.m_tables.find(name);
    if (existing == m_catalog.m_tables.end()) {
      continue;
    }

    store_table_t &stored = existing->second;
    std::swap(stored.m_schema, table.m_schema);
    stored.m_initialized = table.m_initialized;

    for (auto &[hour, partition] : table.m_partitions) {
      const auto part = stored.m_partitions.find(hour);
      if (part != stored.m_partitions.end()) {
        std::swap(part->second, partition);
      }
    }
    stored.m_partitions.merge(table.m_partitions);
  }

  m_catalog.m_tables.merge(next.m_tables);
  m_catalog.m_flushed_seq = next.m_flushed_seq;
  m_catalog.m_next_table_id = next.m_next_table_id;

  return (DB_SUCCESS);
}
