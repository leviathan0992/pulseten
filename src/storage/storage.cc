/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * storage.cc
 *    Opening the storage file, its internal tables and loading its catalog.
 *
 *-------------------------------------------------------------------------
 */

#include "storage.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "duckdb_database.h"
#include "os_file.h"
#include "schema.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_new.h"

/** First UTC day a row may have: 1900-01-01. */
#define STORAGE_MIN_DAY (-25567)

/** Last UTC day a row may have: 2199-12-31. */
#define STORAGE_MAX_DAY 84005

/** Statements that create the internal tables of a new file. */
static const char *storage_create_sql[] = {
    "CREATE TABLE _pt_state ("
    "  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),"
    "  format UINTEGER NOT NULL,"
    "  flushed_seq UBIGINT NOT NULL,"
    "  next_table_id UBIGINT NOT NULL,"
    "  partition_days UINTEGER NOT NULL,"
    "  wal_id UBIGINT NOT NULL CHECK (wal_id > 0))",

    "CREATE TABLE _pt_tables ("
    "  table_id UBIGINT PRIMARY KEY,"
    "  name VARCHAR NOT NULL UNIQUE)",

    "CREATE TABLE _pt_columns ("
    "  table_id UBIGINT NOT NULL,"
    "  col_id UINTEGER NOT NULL,"
    "  name VARCHAR NOT NULL,"
    "  kind UTINYINT NOT NULL,"
    "  type UTINYINT NOT NULL,"
    "  PRIMARY KEY (table_id, col_id))",

    "CREATE TABLE _pt_partitions ("
    "  table_id UBIGINT NOT NULL,"
    "  first_day BIGINT NOT NULL,"
    "  phys_name VARCHAR NOT NULL UNIQUE,"
    "  n_cols UINTEGER NOT NULL,"
    "  row_count UBIGINT NOT NULL,"
    "  PRIMARY KEY (table_id, first_day))",
};

/** Build a physical table name from its table id and first day. */
std::string storage_partition_name(uint64_t table_id, int64_t first_day) {
  std::string name = "_pt_t" + std::to_string(table_id) + "_";

  if (first_day < 0) {
    return (name + "n" + std::to_string(-first_day));
  }

  return (name + "p" + std::to_string(first_day));
}

/** Find the aligned partition containing a UTC day. */
int64_t storage_partition_first_day(int64_t day, uint32_t partition_days) {
  ut_ad(partition_days > 0);

  /* Round toward the earlier partition, also before 1970. */
  int64_t n = day / partition_days;
  if (day % partition_days < 0) {
    n--;
  }

  return (n * partition_days);
}

/** Create the internal tables in a new, empty file.
@param[in]  partition_days  number of UTC days covered by one partition
@return DB_SUCCESS, a random-identity or SQL error, or DB_OUTCOME_UNKNOWN
  after an uncertain commit */
db_status_t storage::create_tables(uint32_t partition_days) {
  uint64_t wal_id = 0;
  if (const db_status_t err = os_generate_uid(wal_id); !err.ok()) {
    return (err);
  }

  /* Create all internal tables and the initial state in one transaction. */
  if (const db_status_t err = m_duckdb->session().begin(); !err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < sizeof(storage_create_sql) / sizeof(char *); ++i) {
    if (const db_status_t err =
            duckdb_exec(m_duckdb->connection(), storage_create_sql[i]);
        !err.ok()) {
      return (m_duckdb->session().rollback(err));
    }
  }

  /* Initialize the storage format, partition span and WAL identity.
  No writes are flushed yet, and table IDs start at 1. */
  if (const db_status_t err =
          duckdb_exec(m_duckdb->connection(),
                      "INSERT INTO _pt_state VALUES (1, " +
                          std::to_string(STORAGE_FORMAT_VERSION) + ", 0, 1, " +
                          std::to_string(partition_days) + ", " +
                          std::to_string(wal_id) + ")");
      !err.ok()) {
    return (m_duckdb->session().rollback(err));
  }

  return (m_duckdb->session().commit());
}

/** Describe inconsistent persistent metadata.
@return DB_CORRUPTION */
static db_status_t storage_corruption(const std::string &why) {
  return (db_status_t(DB_CORRUPTION, "storage file is inconsistent: " + why));
}

/** Check the columns of one table and build its latest directory.
@param[in]  name  normalized table name
@return true if the columns are consistent */
static bool storage_build_schema(const std::string &name,
                                 const std::vector<col_def_t> &cols,
                                 storage_table_t &table) {
  if (cols.empty()) {
    return (false);
  }

  if (cols.size() > SCHEMA_MAX_COLUMNS) {
    return (false);
  }

  /* Column 0 is the time column. */
  const col_def_t &time_col = cols[0];
  if (time_col.m_kind != col_kind_t::TIME ||
      time_col.m_type != col_type_t::TIMESTAMP ||
      time_col.m_name != SCHEMA_TIME_COLUMN) {
    return (false);
  }

  std::set<std::string> names;

  for (size_t i = 0; i < cols.size(); ++i) {
    const col_def_t &c = cols[i];

    if (c.m_id != i || !names.insert(c.m_name).second) {
      return (false);
    }

    std::string normalized;
    if (!schema_normalize_name(c.m_name.data(), c.m_name.size(), normalized)
             .ok() ||
        normalized != c.m_name) {
      return (false);
    }

    /* Column 0 was checked above. */
    if (i > 0) {
      const bool tag_ok =
          c.m_kind == col_kind_t::TAG && c.m_type == col_type_t::VARCHAR;
      const bool field_ok =
          c.m_kind == col_kind_t::FIELD && schema_field_type_is_valid(c.m_type);

      if (!tag_ok && !field_ok) {
        return (false);
      }
    }
  }

  /* Restore the current directory. Batch column counts select its prefixes. */
  table.m_schema.m_name = name;
  for (const col_def_t &col : cols) {
    table.m_schema.m_cols.push_back(col);
  }

  return (true);
}

/** Load the single row of _pt_state.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION */
db_status_t storage_catalog_reader::read_state_row() {
  duckdb_rows_t rows;

  if (const db_status_t err =
          duckdb_exec(m_conn,
                      "SELECT format, flushed_seq, next_table_id, "
                      "partition_days, wal_id FROM _pt_state",
                      rows);
      !err.ok()) {
    return (err);
  }

  if (rows.size() != 1) {
    return (storage_corruption("bad _pt_state"));
  }

  const int64_t format = rows[0][0].m_i;
  const int64_t flushed_seq = rows[0][1].m_i;
  const int64_t next_table_id = rows[0][2].m_i;
  const int64_t partition_days = rows[0][3].m_i;
  const int64_t wal_id = rows[0][4].m_i;

  if (format != STORAGE_FORMAT_VERSION || partition_days < 1 ||
      partition_days > STORAGE_MAX_PARTITION_DAYS || wal_id == 0 ||
      next_table_id == 0) {
    return (storage_corruption("bad _pt_state"));
  }

  m_catalog.m_wal_id = static_cast<uint64_t>(wal_id);
  m_catalog.m_flushed_seq = static_cast<batch_seq_t>(flushed_seq);
  m_catalog.m_next_table_id = static_cast<uint64_t>(next_table_id);
  m_catalog.m_partition_duration_days = static_cast<uint32_t>(partition_days);

  return (DB_SUCCESS);
}

/** Load the table names of _pt_tables, by table id.
REQUIRES: m_catalog.m_next_table_id is loaded.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION */
db_status_t storage_catalog_reader::read_table_names(
    std::map<uint64_t, std::string> &names) {
  duckdb_rows_t rows;

  if (const db_status_t err =
          duckdb_exec(m_conn, "SELECT table_id, name FROM _pt_tables", rows);
      !err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    const uint64_t id = static_cast<uint64_t>(rows[i][0].m_i);

    if (id == 0 || id >= m_catalog.m_next_table_id) {
      return (storage_corruption("table id out of range"));
    }

    const std::string &stored = rows[i][1].m_s;

    std::string name;
    if (!schema_normalize_table_name(stored.data(), stored.size(), name).ok() ||
        name != stored) {
      return (storage_corruption("bad table name"));
    }

    names[id] = name;
  }

  return (DB_SUCCESS);
}

/** Load the rows of _pt_columns, grouped by table id.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION */
db_status_t storage_catalog_reader::read_columns(
    std::map<uint64_t, std::vector<col_def_t>> &cols) {
  duckdb_rows_t rows;

  if (const db_status_t err =
          duckdb_exec(m_conn,
                      "SELECT table_id, col_id, name, kind, type "
                      "FROM _pt_columns ORDER BY table_id, col_id",
                      rows);
      !err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    const uint64_t table_id = static_cast<uint64_t>(rows[i][0].m_i);
    const int64_t kind = rows[i][3].m_i;
    const int64_t type = rows[i][4].m_i;

    col_def_t c;
    c.m_id = static_cast<uint32_t>(rows[i][1].m_i);
    c.m_name = rows[i][2].m_s;
    c.m_kind = static_cast<col_kind_t>(kind);
    c.m_type = static_cast<col_type_t>(type);

    if (kind > static_cast<int64_t>(col_kind_t::FIELD) ||
        type < static_cast<int64_t>(col_type_t::BOOLEAN) ||
        type > static_cast<int64_t>(col_type_t::VARCHAR)) {
      return (storage_corruption("bad column kind or type"));
    }

    cols[table_id].push_back(c);
  }

  return (DB_SUCCESS);
}

/** Load storage state and logical table metadata.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION if stored metadata
  is inconsistent */
db_status_t storage_catalog_reader::read_metadata() {
  if (const db_status_t err = read_state_row(); !err.ok()) {
    return (err);
  }

  std::map<uint64_t, std::string> names;

  if (const db_status_t err = read_table_names(names); !err.ok()) {
    return (err);
  }

  std::map<uint64_t, std::vector<col_def_t>> cols;

  if (const db_status_t err = read_columns(cols); !err.ok()) {
    return (err);
  }

  if (cols.size() != names.size()) {
    return (storage_corruption("tables and columns do not match"));
  }

  std::map<uint64_t, std::string>::iterator it;

  /* Match table names and columns by id to build the logical table m_catalog.
   */
  for (it = names.begin(); it != names.end(); ++it) {
    storage_table_t table;

    table.m_table_id = it->first;

    if (cols.count(it->first) == 0 ||
        !storage_build_schema(it->second, cols[it->first], table)) {
      return (storage_corruption("bad columns of table " + it->second));
    }

    if (!m_catalog.m_tables.insert(std::make_pair(it->second, table)).second) {
      return (storage_corruption("duplicate table " + it->second));
    }
  }

  return (DB_SUCCESS);
}

/** One physical column, in the order reported by DuckDB. */
struct storage_phys_col_t {
  /** Physical column name. */
  std::string m_name;

  /** Canonical SQL type reported by information_schema.columns. */
  std::string m_type;

  /** One-based position in the physical table. */
  int64_t m_position{0};
};

/** Check the physical columns against the logical schema prefix.
@param[in]  schema  newest logical schema; columns only ever append
@return true if names, positions and types match the partition format */
static bool storage_partition_matches_schema(
    const std::vector<storage_phys_col_t> &cols, const schema_t &schema,
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
      type = schema_type_name(schema.m_cols[c - 2].m_type);
    }

    if (cols[c].m_position != static_cast<int64_t>(c) + 1 ||
        cols[c].m_name != name || cols[c].m_type != type) {
      return (false);
    }
  }

  return (true);
}

/** Check user tables while rejecting unknown internal objects and
name collisions with time-series tables.
@return DB_SUCCESS, a query error, or DB_CORRUPTION */
db_status_t storage::check_user_tables() {
  std::set<std::string> internal = {"_pt_state", "_pt_tables", "_pt_columns",
                                    "_pt_partitions"};
  for (const std::pair<const storage_key_t, storage_partition_t> &entry :
       m_catalog.m_partitions) {
    internal.insert(entry.second.m_phys_name);
  }

  duckdb_rows_t rows;
  if (const db_status_t err = duckdb_exec(
          m_duckdb->connection(),
          "SELECT table_name, table_type FROM information_schema.tables "
          "WHERE table_catalog = current_database() AND table_schema = 'main'",
          rows);
      !err.ok()) {
    return (err);
  }

  std::set<std::string> user_names;
  for (const std::vector<duckdb_cell_t> &row : rows) {
    const std::string &name = row[0].m_s;
    const bool known = internal.erase(name) != 0;
    /* PulseTen and its DDL only create tables. */
    if (row[1].m_s != "BASE TABLE") {
      return (storage_corruption("unexpected object " + name));
    }

    if (known) {
      continue;
    }

    std::string normalized;
    if (!schema_normalize_table_name(name.data(), name.size(), normalized)
             .ok() ||
        m_catalog.m_tables.count(normalized) != 0 ||
        !user_names.insert(normalized).second) {
      return (storage_corruption("unexpected table " + name));
    }
  }

  if (!internal.empty()) {
    return (storage_corruption("missing internal table " + *internal.begin()));
  }

  return (DB_SUCCESS);
}

/** Read partition metadata and check logical names, ranges and schemas.
@return DB_SUCCESS, a query error, or DB_CORRUPTION */
db_status_t storage_catalog_reader::read_partitions() {
  duckdb_rows_t rows;
  if (const db_status_t err =
          duckdb_exec(m_conn,
                      "SELECT table_id, first_day, phys_name, n_cols, "
                      "row_count FROM _pt_partitions",
                      rows);
      !err.ok()) {
    return (err);
  }

  std::map<uint64_t, const schema_t *> table_schemas;
  for (const std::pair<const std::string, storage_table_t> &table :
       m_catalog.m_tables) {
    table_schemas[table.second.m_table_id] = &table.second.m_schema;
  }

  const int64_t span = m_catalog.m_partition_duration_days;

  for (size_t i = 0; i < rows.size(); ++i) {
    storage_partition_t p;
    p.m_table_id = static_cast<uint64_t>(rows[i][0].m_i);
    p.m_first_day = rows[i][1].m_i;
    p.m_phys_name = rows[i][2].m_s;
    p.m_n_cols = static_cast<uint32_t>(rows[i][3].m_i);
    p.m_rows = static_cast<uint64_t>(rows[i][4].m_i);

    /* Check overlap without adding to an untrusted day. Modulo checks
    alignment without rounding outside the int64_t range. */
    const bool day_ok = p.m_first_day > STORAGE_MIN_DAY - span &&
                        p.m_first_day <= STORAGE_MAX_DAY &&
                        p.m_first_day % span == 0;

    if (table_schemas.count(p.m_table_id) == 0 || !day_ok ||
        p.m_phys_name != storage_partition_name(p.m_table_id, p.m_first_day) ||
        p.m_n_cols == 0 ||
        p.m_n_cols > table_schemas[p.m_table_id]->m_cols.size()) {
      return (storage_corruption("bad partition " + p.m_phys_name));
    }

    m_catalog.m_partitions[std::make_pair(p.m_table_id, p.m_first_day)] = p;
  }

  return (DB_SUCCESS);
}

/** Read the partitions and check them against the physical tables.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION if stored metadata
  is inconsistent */
db_status_t storage::check_partitions() {
  /* Physical column definitions of every table in this file. */
  duckdb_rows_t rows;

  if (const db_status_t err =
          duckdb_exec(m_duckdb->connection(),
                      "SELECT table_name, column_name, data_type, "
                      "ordinal_position FROM information_schema.columns "
                      "WHERE table_catalog = current_database() "
                      "AND table_schema = 'main' "
                      "ORDER BY table_name, ordinal_position",
                      rows);
      !err.ok()) {
    return (err);
  }

  std::map<std::string, std::vector<storage_phys_col_t>> phys_cols;
  for (size_t i = 0; i < rows.size(); ++i) {
    storage_phys_col_t col;
    col.m_name = rows[i][1].m_s;
    col.m_type = rows[i][2].m_s;
    col.m_position = rows[i][3].m_i;
    phys_cols[rows[i][0].m_s].push_back(col);
  }

  /* An old partition keeps the prefix of its table's newest schema. */
  std::map<uint64_t, const schema_t *> table_schemas;
  std::map<std::string, storage_table_t>::iterator t;

  for (t = m_catalog.m_tables.begin(); t != m_catalog.m_tables.end(); ++t) {
    table_schemas[t->second.m_table_id] = &t->second.m_schema;
  }

  if (const db_status_t err =
          storage_catalog_reader(m_duckdb->connection(), m_catalog)
              .read_partitions();
      !err.ok()) {
    return (err);
  }

  for (const std::pair<const storage_key_t, storage_partition_t> &entry :
       m_catalog.m_partitions) {
    const storage_partition_t &p = entry.second;
    /* _pt_seq and _pt_row, then c0 .. c<n_cols - 1>. */
    if (phys_cols.count(p.m_phys_name) == 0 ||
        !storage_partition_matches_schema(phys_cols[p.m_phys_name],
                                          *table_schemas[p.m_table_id],
                                          p.m_n_cols)) {
      return (storage_corruption("partition table " + p.m_phys_name +
                                 " is missing or has the wrong columns"));
    }
  }

  return (check_user_tables());
}

/** Read only the internal metadata using an already active query
transaction. */
db_status_t storage_catalog_reader::read() {
  m_catalog = storage_catalog_t();
  if (const db_status_t err = read_metadata(); !err.ok()) {
    return (err);
  }

  /* User tables use DuckDB's MVCC catalog directly. Its complete
  physical-object validation belongs to storage::open(), not every query. */
  return (read_partitions());
}

/** Load the catalog from the internal tables and check it.
@return DB_SUCCESS, DB_CORRUPTION or DB_ERROR */
db_status_t storage::load_catalog() {
  m_catalog = storage_catalog_t();

  if (const db_status_t err =
          storage_catalog_reader(m_duckdb->connection(), m_catalog)
              .read_metadata();
      !err.ok()) {
    return (err);
  }

  return (check_partitions());
}

/** Fill a new storage; storage::open() frees it on failure.
@param[in]  partition_days  number of UTC days covered by one partition
@return DB_SUCCESS or the database-open, initialization or catalog-validation
  error */
db_status_t storage::open_low(const char *dir, uint32_t partition_days,
                              uint64_t memory_limit_bytes) {
  const std::string path = std::string(dir) + "/" + STORAGE_FILE_NAME;

  if (const db_status_t err =
          duckdb_backend::open(path.c_str(), &m_duckdb, memory_limit_bytes);
      !err.ok()) {
    return (err);
  }

  duckdb_rows_t rows;
  if (const db_status_t err =
          duckdb_exec(m_duckdb->connection(),
                      "SELECT NOT EXISTS ("
                      "SELECT 1 FROM information_schema.tables "
                      "WHERE table_schema = 'main')",
                      rows);
      !err.ok()) {
    return (err);
  }

  const bool is_empty = rows[0][0].m_i != 0;

  if (is_empty) {
    /* Create the internal tables before loading the catalog. */
    if (const db_status_t err = create_tables(partition_days); !err.ok()) {
      return (err);
    }
  }

  return (load_catalog());
}

/** Open the storage file of a directory, creating it if there is none, and load
its catalog. */
db_status_t storage::open(const char *dir, uint32_t partition_days,
                          storage **out, uint64_t memory_limit_bytes) {
  *out = nullptr;

  ut_ad(partition_days >= 1 && partition_days <= STORAGE_MAX_PARTITION_DAYS);

  storage *st = ut_new<storage>();
  if (st == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  if (const db_status_t err =
          st->open_low(dir, partition_days, memory_limit_bytes);
      !err.ok()) {
    storage::destroy(st);

    return (err);
  }

  *out = st;

  return (DB_SUCCESS);
}

/** Drop every partition that ends on or before a UTC day, in one
transaction. */
db_status_t storage::expire(int64_t end_day, uint64_t &removed) {
  removed = 0;

  /* The partitions that end on or before end_day. */
  const int64_t span = m_catalog.m_partition_duration_days;
  std::vector<storage_key_t> expired;

  std::map<storage_key_t, storage_partition_t>::const_iterator it;
  for (it = m_catalog.m_partitions.begin(); it != m_catalog.m_partitions.end();
       ++it) {
    if (it->second.m_first_day + span <= end_day) {
      expired.push_back(it->first);
    }
  }

  if (expired.empty()) {
    return (DB_SUCCESS);
  }

  /* The physical tables and their catalog rows go together. */
  if (const db_status_t err = m_duckdb->session().begin(); !err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < expired.size(); ++i) {
    const storage_partition_t &p = m_catalog.m_partitions[expired[i]];

    if (const db_status_t err =
            duckdb_exec(m_duckdb->connection(), "DROP TABLE " + p.m_phys_name);
        !err.ok()) {
      return (m_duckdb->session().rollback(err));
    }

    if (const db_status_t err = duckdb_exec(
            m_duckdb->connection(),
            "DELETE FROM _pt_partitions WHERE table_id = " +
                std::to_string(p.m_table_id) +
                " AND first_day = " + std::to_string(p.m_first_day));
        !err.ok()) {
      return (m_duckdb->session().rollback(err));
    }
  }

  if (const db_status_t err = m_duckdb->session().commit(); !err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < expired.size(); ++i) {
    m_catalog.m_partitions.erase(expired[i]);
  }

  removed = expired.size();

  return (DB_SUCCESS);
}

/** Close the storage file. */
void storage::close() {
  duckdb_backend::destroy(m_duckdb);
  m_duckdb = nullptr;

  return;
}

storage::~storage() {
  close();
}

void storage::destroy(storage *st) {
  ut_delete(st);

  return;
}

/** Borrow the DuckDB database behind an open storage handle. */
duckdb_database storage::database() {
  return (m_duckdb->database());
}

/** Borrow the storage connection for db-opening initialization only. */
duckdb_connection storage::connection() {
  return (m_duckdb->connection());
}

/** Return the current committed catalog without copying it. */
const storage_catalog_t &storage::catalog() {
  return (m_catalog);
}
