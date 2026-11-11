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
static const char *storage_init_sql[] = {
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
    "  added_version UBIGINT NOT NULL,"
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

/** Create the internal tables and initial state inside the caller's
transaction.
@return DB_SUCCESS or the first SQL error */
static db_status_t storage_create_tables_low(duckdb_database_t &database,
                                             uint32_t partition_days,
                                             uint64_t wal_id) {
  for (size_t i = 0; i < sizeof(storage_init_sql) / sizeof(char *); ++i) {
    const db_status_t err = duckdb_database_exec(database, storage_init_sql[i]);
    if (!err.ok()) {
      return (err);
    }
  }

  return (duckdb_database_exec(
      database, "INSERT INTO _pt_state VALUES (1, " +
                    std::to_string(STORAGE_FORMAT_VERSION) + ", 0, 1, " +
                    std::to_string(partition_days) + ", " +
                    std::to_string(wal_id) + ")"));
}

/** Create the internal tables in a new, empty file.
@param[in]  partition_days  number of UTC days covered by one partition
@return DB_SUCCESS, a random-identity or SQL error, or DB_OUTCOME_UNKNOWN
  after an uncertain commit */
static db_status_t storage_create_tables(duckdb_database_t &database,
                                         uint32_t partition_days) {
  uint64_t wal_id = 0;
  db_status_t err = os_random_id(wal_id);
  if (!err.ok()) {
    return (err);
  }

  err = duckdb_database_exec(database, "BEGIN TRANSACTION");
  if (!err.ok()) {
    return (err);
  }

  err = storage_create_tables_low(database, partition_days, wal_id);
  if (!err.ok()) {
    return (duckdb_database_rollback(database, err));
  }

  return (duckdb_database_commit(database));
}

/** Describe inconsistent persistent metadata.
@return DB_CORRUPTION */
static db_status_t storage_corruption(const std::string &why) {
  return (db_status_t(DB_CORRUPTION, "storage file is inconsistent: " + why));
}

/** A column row of _pt_columns. */
struct storage_col_row_t {
  /** The column. */
  col_def_t m_def;

  /** Schema version that added it. */
  uint64_t m_added_version{0};
};

/** Check the columns of one table and build its schema versions.
@param[in]  name  canonical table name
@return true if the columns are consistent */
static bool storage_build_versions(const std::string &name,
                                   const std::vector<storage_col_row_t> &cols,
                                   storage_table_t &table) {
  if (cols.empty()) {
    return (false);
  }

  if (cols.size() > SCHEMA_MAX_COLUMNS) {
    return (false);
  }

  /* Column 0 is the time column, added in version 1. */
  const col_def_t &time_col = cols[0].m_def;
  if (time_col.m_kind != col_kind_t::TIME ||
      time_col.m_type != col_type_t::TIMESTAMP ||
      time_col.m_name != SCHEMA_TIME_COLUMN || cols[0].m_added_version != 1) {
    return (false);
  }

  std::set<std::string> names;

  for (size_t i = 0; i < cols.size(); ++i) {
    const storage_col_row_t &c = cols[i];

    if (c.m_def.m_id != i || !names.insert(c.m_def.m_name).second) {
      return (false);
    }

    std::string canonical;
    if (!schema_canonical_name(c.m_def.m_name.data(), c.m_def.m_name.size(),
                               canonical)
             .ok() ||
        canonical != c.m_def.m_name) {
      return (false);
    }

    /* Column 0 was checked above. */
    if (i > 0) {
      const bool tag_ok = c.m_def.m_kind == col_kind_t::TAG &&
                          c.m_def.m_type == col_type_t::VARCHAR;
      const bool field_ok = c.m_def.m_kind == col_kind_t::FIELD &&
                            schema_field_type_is_valid(c.m_def.m_type);

      if (!tag_ok && !field_ok) {
        return (false);
      }

      /* Each version adds at least one column, so versions never skip. */
      const uint64_t prev = cols[i - 1].m_added_version;
      if (c.m_added_version != prev && c.m_added_version != prev + 1) {
        return (false);
      }
    }
  }

  /* Version v holds every column added in versions 1 .. v. */
  const uint64_t newest = cols.back().m_added_version;

  for (uint64_t v = 1; v <= newest; ++v) {
    schema_t schema;
    schema.m_name = name;
    schema.m_version = v;

    for (size_t i = 0; i < cols.size() && cols[i].m_added_version <= v; ++i) {
      schema.m_cols.push_back(cols[i].m_def);
    }

    table.m_versions.push_back(schema);
  }

  /* The last version holds every column. */
  ut_a(table.m_versions.back().m_cols.size() == cols.size());

  return (true);
}

/** Load the single row of _pt_state.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION */
static db_status_t storage_load_state_row(duckdb_database_t &database,
                                          storage_catalog_t &catalog) {
  duckdb_rows_t rows;

  const db_status_t err =
      duckdb_database_query(database,
                            "SELECT format, flushed_seq, next_table_id, "
                            "partition_days, wal_id FROM _pt_state",
                            rows);
  if (!err.ok()) {
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

  catalog.m_wal_id = static_cast<uint64_t>(wal_id);
  catalog.m_flushed_seq = static_cast<batch_seq_t>(flushed_seq);
  catalog.m_next_table_id = static_cast<uint64_t>(next_table_id);
  catalog.m_partition_duration_days = static_cast<uint32_t>(partition_days);

  return (DB_SUCCESS);
}

/** Load the table names of _pt_tables, by table id.
REQUIRES: catalog.m_next_table_id is loaded.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION */
static db_status_t storage_load_table_names(
    duckdb_database_t &database, const storage_catalog_t &catalog,
    std::map<uint64_t, std::string> &names) {
  duckdb_rows_t rows;

  const db_status_t err = duckdb_database_query(
      database, "SELECT table_id, name FROM _pt_tables", rows);
  if (!err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    const uint64_t id = static_cast<uint64_t>(rows[i][0].m_i);

    if (id == 0 || id >= catalog.m_next_table_id) {
      return (storage_corruption("table id out of range"));
    }

    const std::string &stored = rows[i][1].m_s;

    std::string name;
    if (!schema_canonical_table_name(stored.data(), stored.size(), name).ok() ||
        name != stored) {
      return (storage_corruption("bad table name"));
    }
    names[id] = name;
  }

  return (DB_SUCCESS);
}

/** Load the rows of _pt_columns, grouped by table id.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION */
static db_status_t storage_load_columns(
    duckdb_database_t &database,
    std::map<uint64_t, std::vector<storage_col_row_t>> &cols) {
  duckdb_rows_t rows;

  const db_status_t err = duckdb_database_query(
      database,
      "SELECT table_id, col_id, name, kind, type, added_version "
      "FROM _pt_columns ORDER BY table_id, col_id",
      rows);
  if (!err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    const uint64_t table_id = static_cast<uint64_t>(rows[i][0].m_i);
    const int64_t kind = rows[i][3].m_i;
    const int64_t type = rows[i][4].m_i;

    storage_col_row_t c;
    c.m_def.m_id = static_cast<uint32_t>(rows[i][1].m_i);
    c.m_def.m_name = rows[i][2].m_s;
    c.m_def.m_kind = static_cast<col_kind_t>(kind);
    c.m_def.m_type = static_cast<col_type_t>(type);
    c.m_added_version = static_cast<uint64_t>(rows[i][5].m_i);

    if (kind > static_cast<int64_t>(col_kind_t::FIELD) ||
        type < static_cast<int64_t>(col_type_t::BOOLEAN) ||
        type > static_cast<int64_t>(col_type_t::VARCHAR)) {
      return (storage_corruption("bad column kind or type"));
    }

    cols[table_id].push_back(c);
  }

  return (DB_SUCCESS);
}

/** Load the header row and the tables with their columns.
@param[in,out]  catalog  stored catalog being loaded and validated
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION if stored metadata
  is inconsistent */
static db_status_t storage_load_state_and_tables(duckdb_database_t &database,
                                                 storage_catalog_t &catalog) {
  db_status_t err = storage_load_state_row(database, catalog);
  if (!err.ok()) {
    return (err);
  }

  std::map<uint64_t, std::string> names;

  err = storage_load_table_names(database, catalog, names);
  if (!err.ok()) {
    return (err);
  }

  std::map<uint64_t, std::vector<storage_col_row_t>> cols;

  err = storage_load_columns(database, cols);
  if (!err.ok()) {
    return (err);
  }

  if (cols.size() != names.size()) {
    return (storage_corruption("tables and columns do not match"));
  }

  std::map<uint64_t, std::string>::iterator it;

  for (it = names.begin(); it != names.end(); ++it) {
    storage_table_t table;
    table.m_table_id = it->first;

    if (cols.count(it->first) == 0 ||
        !storage_build_versions(it->second, cols[it->first], table)) {
      return (storage_corruption("bad columns of table " + it->second));
    }

    if (!catalog.m_tables.insert(std::make_pair(it->second, table)).second) {
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

/** Classify ordinary tables while rejecting unknown internal objects and
name collisions with time-series tables.
@param[in,out]  database  database or borrowed snapshot connection
loaded
@return DB_SUCCESS, a query error, or DB_CORRUPTION */
static db_status_t storage_load_user_tables(duckdb_database_t &database,
                                            storage_catalog_t &catalog) {
  std::set<std::string> internal = {"_pt_state", "_pt_tables", "_pt_columns",
                                    "_pt_partitions"};
  for (const std::pair<const storage_key_t, storage_partition_t> &entry :
       catalog.m_partitions) {
    internal.insert(entry.second.m_phys_name);
  }

  duckdb_rows_t rows;
  const db_status_t err = duckdb_database_query(
      database,
      "SELECT table_name, table_type FROM information_schema.tables "
      "WHERE table_catalog = current_database() AND table_schema = 'main'",
      rows);
  if (!err.ok()) {
    return (err);
  }

  catalog.m_user_tables.clear();
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

    std::string canonical;
    if (!schema_canonical_table_name(name.data(), name.size(), canonical)
             .ok() ||
        catalog.m_tables.count(canonical) != 0 ||
        !catalog.m_user_tables.insert(canonical).second) {
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
static db_status_t storage_read_partitions(duckdb_database_t &database,
                                           storage_catalog_t &catalog) {
  duckdb_rows_t rows;
  db_status_t err =
      duckdb_database_query(database,
                            "SELECT table_id, first_day, phys_name, n_cols, "
                            "row_count FROM _pt_partitions",
                            rows);
  if (!err.ok()) {
    return (err);
  }

  std::map<uint64_t, const schema_t *> table_schemas;
  for (const std::pair<const std::string, storage_table_t> &table :
       catalog.m_tables) {
    table_schemas[table.second.m_table_id] = &table.second.m_versions.back();
  }

  const int64_t span = catalog.m_partition_duration_days;

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

    catalog.m_partitions[std::make_pair(p.m_table_id, p.m_first_day)] = p;
  }

  return (DB_SUCCESS);
}

/** Read the partitions and check them against the physical tables.
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION if stored metadata
  is inconsistent */
static db_status_t storage_check_physical_partitions(
    duckdb_database_t &database, storage_catalog_t &catalog) {
  /* Physical column definitions of every table in this file. */
  duckdb_rows_t rows;

  db_status_t err =
      duckdb_database_query(database,
                            "SELECT table_name, column_name, data_type, "
                            "ordinal_position FROM information_schema.columns "
                            "WHERE table_catalog = current_database() "
                            "AND table_schema = 'main' "
                            "ORDER BY table_name, ordinal_position",
                            rows);
  if (!err.ok()) {
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

  for (t = catalog.m_tables.begin(); t != catalog.m_tables.end(); ++t) {
    table_schemas[t->second.m_table_id] = &t->second.m_versions.back();
  }

  err = storage_read_partitions(database, catalog);
  if (!err.ok()) {
    return (err);
  }

  for (const std::pair<const storage_key_t, storage_partition_t> &entry :
       catalog.m_partitions) {
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

  return (storage_load_user_tables(database, catalog));
}

/** Read only the internal metadata using an already active query
transaction. */
db_status_t storage_read_catalog(duckdb_connection conn,
                                 storage_catalog_t &catalog) {
  catalog = storage_catalog_t();
  duckdb_database_t reader;
  reader.m_conn = conn;  // Borrowed; no close/disconnect ownership.
  db_status_t err = storage_load_state_and_tables(reader, catalog);
  if (!err.ok()) {
    return (err);
  }
  /* Ordinary tables use DuckDB's MVCC catalog directly. Its complete
  physical-object validation belongs to storage_open(), not every query. */
  return (storage_read_partitions(reader, catalog));
}

/** Update the ordinary-table catalog after a successful DDL commit. */
void storage_user_table_committed(storage_t &st, const std::string &table_name,
                                  bool exists) {
  ut_a(!st.m_prepared);
  if (exists) {
    st.m_catalog.m_user_tables.insert(table_name);
  } else {
    st.m_catalog.m_user_tables.erase(table_name);
  }
}

/** Load the catalog from the internal tables and check it.
@return DB_SUCCESS, DB_CORRUPTION or DB_ERROR */
static db_status_t storage_load_and_check_catalog(duckdb_database_t &database,
                                                  storage_catalog_t &catalog) {
  catalog = storage_catalog_t();

  db_status_t err = storage_load_state_and_tables(database, catalog);
  if (!err.ok()) {
    return (err);
  }

  err = storage_check_physical_partitions(database, catalog);
  /* The returned status already owns a query or validation diagnostic. */

  return (err);
}

/** Fill a new storage; storage_open() frees it on failure.
@param[in]  partition_days  number of UTC days covered by one partition
@return DB_SUCCESS or the database-open, initialization or catalog-validation
  error */
static db_status_t storage_open_low(const char *dir, uint32_t partition_days,
                                    storage_t &st) {
  const std::string path = std::string(dir) + "/" + STORAGE_FILE_NAME;

  db_status_t err = duckdb_database_open(path.c_str(), &st.m_duckdb);
  if (!err.ok()) {
    return (err);
  }

  /* A new file has no tables; create the internal ones. */
  duckdb_rows_t rows;
  err = duckdb_database_query(
      *st.m_duckdb,
      "SELECT count(*), "
      "count(*) FILTER (WHERE table_name = '_pt_state') "
      "FROM information_schema.tables WHERE table_schema = 'main'",
      rows);
  if (!err.ok()) {
    return (err);
  }

  const int64_t n_tables = rows[0][0].m_i;
  const int64_t n_state_tables = rows[0][1].m_i;

  if (n_tables == 0) {
    err = storage_create_tables(*st.m_duckdb, partition_days);
    if (!err.ok()) {
      return (err);
    }
  } else if (n_state_tables != 1) {
    return (storage_corruption("not a PulseTen storage file"));
  }

  return (storage_load_and_check_catalog(*st.m_duckdb, st.m_catalog));
}

/** Open the storage file of a directory, creating it if there is none, and load
its catalog. */
db_status_t storage_open(const char *dir, uint32_t partition_days,
                         storage_t **out) {
  *out = nullptr;

  if (partition_days < 1 || partition_days > STORAGE_MAX_PARTITION_DAYS) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "partition span must be 1 to " +
                            std::to_string(STORAGE_MAX_PARTITION_DAYS) +
                            " days"));
  }

  storage_t *st = ut_new<storage_t>();
  if (st == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  const db_status_t err = storage_open_low(dir, partition_days, *st);
  if (!err.ok()) {
    storage_close(st);

    return (err);
  }

  *out = st;

  return (DB_SUCCESS);
}

/** Drop every partition that ends on or before a UTC day, in one
transaction. */
db_status_t storage_expire(storage_t &st, int64_t end_day, uint64_t &removed) {
  ut_a(!st.m_prepared);
  removed = 0;

  /* The partitions that end on or before end_day. */
  const int64_t span = st.m_catalog.m_partition_duration_days;
  std::vector<storage_key_t> expired;

  std::map<storage_key_t, storage_partition_t>::const_iterator it;
  for (it = st.m_catalog.m_partitions.begin();
       it != st.m_catalog.m_partitions.end(); ++it) {
    if (it->second.m_first_day + span <= end_day) {
      expired.push_back(it->first);
    }
  }

  if (expired.empty()) {
    return (DB_SUCCESS);
  }

  /* The physical tables and their catalog rows go together. */
  db_status_t err = duckdb_database_exec(*st.m_duckdb, "BEGIN TRANSACTION");
  if (!err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < expired.size(); ++i) {
    const storage_partition_t &p = st.m_catalog.m_partitions[expired[i]];

    err = duckdb_database_exec(*st.m_duckdb, "DROP TABLE " + p.m_phys_name);
    if (!err.ok()) {
      break;
    }

    err = duckdb_database_exec(
        *st.m_duckdb, "DELETE FROM _pt_partitions WHERE table_id = " +
                          std::to_string(p.m_table_id) +
                          " AND first_day = " + std::to_string(p.m_first_day));
    if (!err.ok()) {
      break;
    }
  }

  if (!err.ok()) {
    return (duckdb_database_rollback(*st.m_duckdb, err));
  }

  err = duckdb_database_commit(*st.m_duckdb);
  if (!err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < expired.size(); ++i) {
    st.m_catalog.m_partitions.erase(expired[i]);
  }

  removed = expired.size();

  return (DB_SUCCESS);
}

/** Close the storage file. */
void storage_close(storage_t *st) {
  if (st == nullptr) {
    return;
  }

  duckdb_database_close(st->m_duckdb);
  ut_delete(st);
}

/** Borrow the DuckDB database behind an open storage handle. */
duckdb_database storage_database(storage_t &st) {
  return (st.m_duckdb->m_db);
}

/** Borrow the storage connection for engine-opening initialization only. */
duckdb_connection storage_connection(storage_t &st) {
  return (st.m_duckdb->m_conn);
}

/** Borrow the storage metadata as of the last committed change. */
const storage_catalog_t &storage_catalog(storage_t &st) {
  return (st.m_catalog);
}

/** Run a read-only SQL query returning one integer. */
db_status_t storage_query_int(storage_t &st, const std::string &sql,
                              int64_t &value) {
  duckdb_rows_t rows;

  const db_status_t err = duckdb_database_query(*st.m_duckdb, sql, rows);
  if (!err.ok()) {
    return (err);
  }

  if (rows.size() != 1 || rows[0].size() != 1 || rows[0][0].m_is_null) {
    return (DB_ERROR);
  }

  value = rows[0][0].m_i;

  return (DB_SUCCESS);
}

#ifdef PT_DEBUG
/** Make the next flush preparation fail after writing rows. */
void storage_dbg_fail_next_flush(storage_t &st) {
  st.m_dbg_fail_next_flush = true;
}

/** Enable or disable persistent pre-commit flush failures. */
void storage_dbg_set_flush_failure(storage_t &st, bool fail) {
  st.m_dbg_fail_flush = fail;
}
#endif /* PT_DEBUG */
