/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * storage0storage.cc
 *    Opening the storage file, its internal tables and loading its state.
 *
 *-------------------------------------------------------------------------
 */

#include "storage0priv.h"

#include <new>
#include <set>

#include "os0file.h"
#include "ut0new.h"

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
    "  role UTINYINT NOT NULL,"
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

/** Build a physical table name from its table id and first day.
@param[in]  table_id   table id
@param[in]  first_day  first UTC day of the partition
@return the physical table name of a partition */
std::string storage_partition_name(uint64_t table_id, int64_t first_day) {
  std::string name = "_pt_t" + std::to_string(table_id) + "_";

  if (first_day < 0) {
    return (name + "n" + std::to_string(-first_day));
  }

  return (name + "p" + std::to_string(first_day));
}

/** Find the aligned partition containing a UTC day.
@param[in]  day             UTC day
@param[in]  partition_days  partition span
@return the first day of the partition holding a UTC day */
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
@param[in,out]  duck            DuckDB database and connection
@param[in]      partition_days  number of UTC days covered by one partition
@param[out]     error           DuckDB's message on failure
@return DB_SUCCESS, a random-identity or SQL error, or DB_OUTCOME_UNKNOWN
  after an uncertain commit */
static dberr_t storage_init(duck_t &duck, uint32_t partition_days,
                            std::string &error) {
  uint64_t wal_id = 0;
  dberr_t err = os_random_id(wal_id);
  if (err != DB_SUCCESS) {
    return (err);
  }

  err = duck_exec(duck, "BEGIN TRANSACTION");
  if (err != DB_SUCCESS) {
    return (err);
  }

  for (size_t i = 0; i < sizeof(storage_init_sql) / sizeof(char *); ++i) {
    err = duck_exec(duck, storage_init_sql[i]);
    if (err != DB_SUCCESS) {
      break;
    }
  }

  if (err == DB_SUCCESS) {
    err = duck_exec(duck, "INSERT INTO _pt_state VALUES (1, " +
                              std::to_string(STORAGE_FORMAT_VERSION) +
                              ", 0, 1, " + std::to_string(partition_days) +
                              ", " + std::to_string(wal_id) + ")");
  }

  if (err != DB_SUCCESS) {
    (void)duck_exec(duck, "ROLLBACK");

    return (err);
  }

  return (duck_commit(duck, error));
}

/** Set error and return DB_CORRUPTION.
@param[out]  error  diagnostic explaining the inconsistent stored state
@param[in]   why    description of the inconsistent stored state
@return DB_CORRUPTION */
static dberr_t storage_corrupt(std::string &error, const std::string &why) {
  error = "storage file is inconsistent: " + why;

  return (DB_CORRUPTION);
}

/** A column row of _pt_columns. */
struct storage_col_row_t {
  /** The column. */
  col_def_t def;

  /** Schema version that added it. */
  uint64_t added_version{0};
};

/** Check the columns of one table and build its schema versions.
@param[in]   name   canonical table name
@param[in]   cols   its columns in id order
@param[out]  table  versions filled in
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
  const col_def_t &time_col = cols[0].def;
  if (time_col.role != COL_ROLE_TIME || time_col.type != COL_TIMESTAMP ||
      time_col.name != SCHEMA_TIME_COLUMN || cols[0].added_version != 1) {
    return (false);
  }

  std::set<std::string> names;

  for (size_t i = 0; i < cols.size(); ++i) {
    const storage_col_row_t &c = cols[i];

    if (c.def.id != i || !names.insert(c.def.name).second) {
      return (false);
    }

    std::string canonical;
    if (schema_canonical_name(c.def.name.data(), c.def.name.size(),
                              canonical) != DB_SUCCESS ||
        canonical != c.def.name) {
      return (false);
    }

    if (i > 0) {
      const bool tag_ok =
          c.def.role == COL_ROLE_TAG && c.def.type == COL_VARCHAR;
      const bool field_ok = c.def.role == COL_ROLE_FIELD &&
                            schema_field_type_is_valid(c.def.type);

      if (!tag_ok && !field_ok) {
        return (false);
      }

      /* Each version adds at least one column, so versions never skip. */
      const uint64_t prev = cols[i - 1].added_version;
      if (c.added_version != prev && c.added_version != prev + 1) {
        return (false);
      }
    }
  }

  /* Version v holds every column added in versions 1 .. v. */
  const uint64_t newest = cols.back().added_version;

  for (uint64_t v = 1; v <= newest; ++v) {
    schema_t schema;
    schema.name = name;
    schema.version = v;

    for (size_t i = 0; i < cols.size() && cols[i].added_version <= v; ++i) {
      schema.cols.push_back(cols[i].def);
    }

    table.versions.push_back(schema);
  }

  return (true);
}

/** Load the header row and the tables with their columns.
@param[in,out]  duck   DuckDB database and connection
@param[in,out]  state  stored catalog being loaded and validated
@param[out]     error  reason for failure
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION if stored metadata
  is inconsistent */
static dberr_t storage_load_tables(duck_t &duck, storage_state_t &state,
                                   std::string &error) {
  duck_rows_t rows;

  dberr_t err = duck_query(duck,
                           "SELECT format, flushed_seq, next_table_id, "
                           "partition_days, wal_id FROM _pt_state",
                           rows);
  if (err != DB_SUCCESS) {
    return (err);
  }

  if (rows.size() != 1 || rows[0][0].i != STORAGE_FORMAT_VERSION ||
      rows[0][3].i < 1 || rows[0][3].i > STORAGE_MAX_PARTITION_DAYS ||
      rows[0][4].i == 0) {
    return (storage_corrupt(error, "bad _pt_state"));
  }

  state.wal_id = (uint64_t)rows[0][4].i;
  state.flushed_seq = (batch_seq_t)rows[0][1].i;
  state.next_table_id = (uint64_t)rows[0][2].i;
  state.partition_days = (uint32_t)rows[0][3].i;

  /* Tables. */
  std::map<uint64_t, std::string> names;

  err = duck_query(duck, "SELECT table_id, name FROM _pt_tables", rows);
  if (err != DB_SUCCESS) {
    return (err);
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    const uint64_t id = (uint64_t)rows[i][0].i;

    if (id == 0 || id >= state.next_table_id) {
      return (storage_corrupt(error, "table id out of range"));
    }

    std::string name;
    if (schema_table_name(rows[i][1].s.data(), rows[i][1].s.size(), name) !=
            DB_SUCCESS ||
        name != rows[i][1].s) {
      return (storage_corrupt(error, "bad table name"));
    }
    names[id] = name;
  }

  /* Columns, grouped by table. */
  std::map<uint64_t, std::vector<storage_col_row_t>> cols;

  err = duck_query(duck,
                   "SELECT table_id, col_id, name, role, type, added_version "
                   "FROM _pt_columns ORDER BY table_id, col_id",
                   rows);
  if (err != DB_SUCCESS) {
    return (err);
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    storage_col_row_t c;
    c.def.id = (uint32_t)rows[i][1].i;
    c.def.name = rows[i][2].s;
    c.def.role = (col_role_t)rows[i][3].i;
    c.def.type = (col_type_t)rows[i][4].i;
    c.added_version = (uint64_t)rows[i][5].i;

    if (rows[i][3].i > COL_ROLE_FIELD || rows[i][4].i < COL_BOOLEAN ||
        rows[i][4].i > COL_VARCHAR) {
      return (storage_corrupt(error, "bad column role or type"));
    }

    cols[(uint64_t)rows[i][0].i].push_back(c);
  }

  if (cols.size() != names.size()) {
    return (storage_corrupt(error, "tables and columns do not match"));
  }

  std::map<uint64_t, std::string>::iterator it;

  for (it = names.begin(); it != names.end(); ++it) {
    storage_table_t table;
    table.table_id = it->first;

    if (cols.count(it->first) == 0 ||
        !storage_build_versions(it->second, cols[it->first], table)) {
      return (storage_corrupt(error, "bad columns of table " + it->second));
    }

    if (!state.tables.insert(std::make_pair(it->second, table)).second) {
      return (storage_corrupt(error, "duplicate table " + it->second));
    }
  }

  return (DB_SUCCESS);
}

/** One physical column, in the order reported by DuckDB. */
struct storage_phys_col_t {
  /** Physical column name. */
  std::string name;

  /** Canonical SQL type reported by information_schema.columns. */
  std::string type;

  /** One-based position in the physical table. */
  int64_t position{0};
};

/** Check the physical columns against the logical schema prefix.
@param[in]  cols    physical columns in ordinal order
@param[in]  schema  newest logical schema; columns only ever append
@param[in]  n_cols  number of logical columns stored in this partition
@return true if names, positions and types match the partition format */
static bool storage_partition_columns_ok(
    const std::vector<storage_phys_col_t> &cols, const schema_t &schema,
    uint32_t n_cols) {
  if (cols.size() != (size_t)n_cols + 2) {
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
      type = schema_type_name(schema.cols[c - 2].type);
    }

    if (cols[c].position != (int64_t)c + 1 || cols[c].name != name ||
        cols[c].type != type) {
      return (false);
    }
  }

  return (true);
}

/** Load the partitions and check them against the physical tables.
@param[in,out]  duck   DuckDB database and connection
@param[in,out]  state  stored catalog being loaded and validated
@param[out]     error  reason for failure
@return DB_SUCCESS, a DuckDB query error, or DB_CORRUPTION if stored metadata
  is inconsistent */
static dberr_t storage_load_partitions(duck_t &duck, storage_state_t &state,
                                       std::string &error) {
  /* Physical column definitions of every table in this file. */
  duck_rows_t rows;

  dberr_t err = duck_query(duck,
                           "SELECT table_name, column_name, data_type, "
                           "ordinal_position FROM information_schema.columns "
                           "WHERE table_catalog = current_database() "
                           "AND table_schema = 'main' "
                           "ORDER BY table_name, ordinal_position",
                           rows);
  if (err != DB_SUCCESS) {
    return (err);
  }

  std::map<std::string, std::vector<storage_phys_col_t>> phys_cols;
  for (size_t i = 0; i < rows.size(); ++i) {
    storage_phys_col_t col;
    col.name = rows[i][1].s;
    col.type = rows[i][2].s;
    col.position = rows[i][3].i;
    phys_cols[rows[i][0].s].push_back(col);
  }

  /* An old partition keeps the prefix of its table's newest schema. */
  std::map<uint64_t, const schema_t *> table_schemas;
  std::map<std::string, storage_table_t>::iterator t;

  for (t = state.tables.begin(); t != state.tables.end(); ++t) {
    table_schemas[t->second.table_id] = &t->second.versions.back();
  }

  err = duck_query(duck,
                   "SELECT table_id, first_day, phys_name, n_cols, "
                   "row_count FROM _pt_partitions",
                   rows);
  if (err != DB_SUCCESS) {
    return (err);
  }

  const int64_t span = state.partition_days;

  for (size_t i = 0; i < rows.size(); ++i) {
    storage_partition_t p;
    p.table_id = (uint64_t)rows[i][0].i;
    p.first_day = rows[i][1].i;
    p.phys_name = rows[i][2].s;
    p.n_cols = (uint32_t)rows[i][3].i;
    p.rows = (uint64_t)rows[i][4].i;

    /* Check overlap without adding to an untrusted day. Modulo checks
    alignment without rounding outside the int64_t range. */
    const bool day_ok = p.first_day > STORAGE_MIN_DAY - span &&
                        p.first_day <= STORAGE_MAX_DAY &&
                        p.first_day % span == 0;

    if (table_schemas.count(p.table_id) == 0 || !day_ok ||
        p.phys_name != storage_partition_name(p.table_id, p.first_day) ||
        p.n_cols == 0 || p.n_cols > table_schemas[p.table_id]->cols.size()) {
      return (storage_corrupt(error, "bad partition " + p.phys_name));
    }

    /* _pt_seq and _pt_row, then c0 .. c<n_cols - 1>. */
    if (phys_cols.count(p.phys_name) == 0 ||
        !storage_partition_columns_ok(phys_cols[p.phys_name],
                                      *table_schemas[p.table_id], p.n_cols)) {
      return (
          storage_corrupt(error, "partition table " + p.phys_name +
                                     " is missing or has the wrong columns"));
    }

    state.partitions[std::make_pair(p.table_id, p.first_day)] = p;
  }

  /* Every other table of the file is an internal one. */
  const size_t expected = state.partitions.size() + 4;
  if (phys_cols.size() != expected) {
    return (storage_corrupt(error, "unexpected tables in the file"));
  }

  return (DB_SUCCESS);
}

/** Read only the internal metadata using an already active query transaction.
@param[in]   conn   connection with an active read transaction
@param[out]  state  metadata read from that transaction's snapshot
@param[out]  error  reason for failure
@return DB_SUCCESS or the catalog query/validation error */
dberr_t storage_snapshot(duckdb_connection conn, storage_state_t &state,
                         std::string &error) {
  state = storage_state_t();
  duck_t reader;
  reader.conn = conn;  // Borrowed; no close/disconnect ownership.
  dberr_t err = storage_load_tables(reader, state, error);
  if (err != DB_SUCCESS) {
    if (err == DB_ERROR) {
      error = reader.error;
    }

    return (err);
  }
  duck_rows_t rows;
  err = duck_query(reader,
                   "SELECT table_id, first_day, phys_name, n_cols, row_count "
                   "FROM _pt_partitions",
                   rows);
  if (err != DB_SUCCESS) {
    error = reader.error;

    return (err);
  }
  for (const std::vector<duck_cell_t> &row : rows) {
    storage_partition_t part;
    part.table_id = static_cast<uint64_t>(row[0].i);
    part.first_day = row[1].i;
    part.phys_name = row[2].s;
    part.n_cols = static_cast<uint32_t>(row[3].i);
    part.rows = static_cast<uint64_t>(row[4].i);
    state.partitions[std::make_pair(part.table_id, part.first_day)] = part;
  }

  return (DB_SUCCESS);
}

/** Load the state from the internal tables and check it.
@param[in]   duck   database
@param[out]  state  loaded state
@param[out]  error  reason on failure
@return DB_SUCCESS, DB_CORRUPTION or DB_ERROR */
static dberr_t storage_load_state(duck_t &duck, storage_state_t &state,
                                  std::string &error) {
  state = storage_state_t();

  dberr_t err = storage_load_tables(duck, state, error);

  if (err == DB_SUCCESS) {
    err = storage_load_partitions(duck, state, error);
  }

  if (err == DB_ERROR) {
    error = duck.error;
  }

  return (err);
}

/** Fill a new storage; storage_open() frees it on failure.
@param[in]   dir             database or WAL directory
@param[in]   partition_days  number of UTC days covered by one partition
@param[out]  st              allocated storage handle receiving the open
                             database and catalog
@param[out]  error           reason for failure
@return DB_SUCCESS or the database-open, initialization or catalog-validation
  error */
static dberr_t storage_open_low(const char *dir, uint32_t partition_days,
                                storage_t &st, std::string &error) {
  const std::string path = std::string(dir) + "/" + STORAGE_FILE_NAME;

  dberr_t err = duck_open(path.c_str(), &st.duck, error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  /* A new file has no tables; create the internal ones. */
  duck_rows_t rows;
  err = duck_query(*st.duck,
                   "SELECT count(*), "
                   "count(*) FILTER (WHERE table_name = '_pt_state') "
                   "FROM information_schema.tables WHERE table_schema = 'main'",
                   rows);
  if (err != DB_SUCCESS) {
    error = st.duck->error;

    return (err);
  }

  if (rows[0][0].i == 0) {
    err = storage_init(*st.duck, partition_days, error);
    if (err != DB_SUCCESS) {
      error = st.duck->error;

      return (err);
    }
  } else if (rows[0][1].i != 1) {
    return (storage_corrupt(error, "not a PulseTen storage file"));
  }

  return (storage_load_state(*st.duck, st.state, error));
}

/** Open the storage file of a directory, creating it if there is none, and
load its state.
@param[in]   dir             existing directory
@param[in]   partition_days  partition span of a new file, 1 to
                             STORAGE_MAX_PARTITION_DAYS; an existing file
                             keeps its own
@param[out]  out             open storage
@param[out]  error           message on failure
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_CORRUPTION, DB_IO_ERROR, DB_ERROR
  or DB_OUT_OF_MEMORY */
dberr_t storage_open(const char *dir, uint32_t partition_days, storage_t **out,
                     std::string &error) {
  *out = nullptr;

  error.clear();

  if (partition_days < 1 || partition_days > STORAGE_MAX_PARTITION_DAYS) {
    error = "partition span must be 1 to " +
            std::to_string(STORAGE_MAX_PARTITION_DAYS) + " days";

    return (DB_INVALID_ARGUMENT);
  }

  storage_t *st = ut_new<storage_t>();
  if (st == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  const dberr_t err = storage_open_low(dir, partition_days, *st, error);

  if (err != DB_SUCCESS) {
    storage_close(st);

    return (err);
  }

  *out = st;

  return (DB_SUCCESS);
}

/** Drop every partition that ends on or before a UTC day, in one transaction.
@param[in]   st       storage
@param[in]   end_day  partitions whose last day is before this day go
@param[out]  removed  partitions dropped
@param[out]  error    message on failure
@retval DB_SUCCESS committed, or nothing to drop
@retval DB_ERROR DuckDB refused a statement; rolled back
@retval DB_OUTCOME_UNKNOWN the commit failed; reopen to learn the state */
dberr_t storage_expire(storage_t &st, int64_t end_day, uint64_t &removed,
                       std::string &error) {
  ut_a(!st.prepared);
  removed = 0;

  error.clear();

  /* The partitions that end on or before end_day. */
  const int64_t span = st.state.partition_days;
  std::vector<storage_key_t> expired;

  std::map<storage_key_t, storage_partition_t>::const_iterator it;
  for (it = st.state.partitions.begin(); it != st.state.partitions.end();
       ++it) {
    if (it->second.first_day + span <= end_day) {
      expired.push_back(it->first);
    }
  }

  if (expired.empty()) {
    return (DB_SUCCESS);
  }

  /* The physical tables and their catalog rows go together. */
  dberr_t err = duck_exec(*st.duck, "BEGIN TRANSACTION");

  for (size_t i = 0; i < expired.size() && err == DB_SUCCESS; ++i) {
    const storage_partition_t &p = st.state.partitions[expired[i]];

    err = duck_exec(*st.duck, "DROP TABLE " + p.phys_name);

    if (err == DB_SUCCESS) {
      err = duck_exec(*st.duck,
                      "DELETE FROM _pt_partitions WHERE table_id = " +
                          std::to_string(p.table_id) +
                          " AND first_day = " + std::to_string(p.first_day));
    }
  }

  if (err != DB_SUCCESS) {
    error = st.duck->error;
    (void)duck_exec(*st.duck, "ROLLBACK");

    return (DB_ERROR);
  }

  err = duck_commit(*st.duck, error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  for (size_t i = 0; i < expired.size(); ++i) {
    st.state.partitions.erase(expired[i]);
  }

  removed = expired.size();

  return (DB_SUCCESS);
}

/** Drop a table: its partitions and its catalog rows, in one transaction.
@param[in]   st     storage
@param[in]   table  canonical table name
@param[out]  error  message on failure
@retval DB_SUCCESS committed, or the table is not stored
@retval DB_ERROR DuckDB refused a statement; rolled back
@retval DB_OUTCOME_UNKNOWN the commit failed; reopen to learn the state */
dberr_t storage_drop_table(storage_t &st, const std::string &table,
                           std::string &error) {
  ut_a(!st.prepared);

  error.clear();

  /* A table whose writes were never flushed is not stored. */
  std::map<std::string, storage_table_t>::const_iterator t =
      st.state.tables.find(table);
  if (t == st.state.tables.end()) {
    return (DB_SUCCESS);
  }

  const uint64_t table_id = t->second.table_id;
  const std::string id = std::to_string(table_id);

  std::vector<storage_key_t> dropped;

  std::map<storage_key_t, storage_partition_t>::const_iterator it;
  for (it = st.state.partitions.begin(); it != st.state.partitions.end();
       ++it) {
    if (it->first.first == table_id) {
      dropped.push_back(it->first);
    }
  }

  /* The physical tables and every catalog row go together. */
  dberr_t err = duck_exec(*st.duck, "BEGIN TRANSACTION");

  for (size_t i = 0; i < dropped.size() && err == DB_SUCCESS; ++i) {
    err = duck_exec(*st.duck,
                    "DROP TABLE " + st.state.partitions[dropped[i]].phys_name);
  }

  if (err == DB_SUCCESS) {
    err = duck_exec(*st.duck,
                    "DELETE FROM _pt_partitions WHERE table_id = " + id);
  }

  if (err == DB_SUCCESS) {
    err = duck_exec(*st.duck, "DELETE FROM _pt_columns WHERE table_id = " + id);
  }

  if (err == DB_SUCCESS) {
    err = duck_exec(*st.duck, "DELETE FROM _pt_tables WHERE table_id = " + id);
  }

  if (err != DB_SUCCESS) {
    error = st.duck->error;
    (void)duck_exec(*st.duck, "ROLLBACK");

    return (DB_ERROR);
  }

  err = duck_commit(*st.duck, error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  for (size_t i = 0; i < dropped.size(); ++i) {
    st.state.partitions.erase(dropped[i]);
  }

  st.state.tables.erase(table);

  return (DB_SUCCESS);
}

/** Close the storage file.
@param[in]  st  storage; may be NULL */
void storage_close(storage_t *st) {
  if (st == nullptr) {
    return;
  }

  duck_close(st->duck);
  ut_delete(st);
}

/** Borrow the DuckDB database behind an open storage handle.
@param[in,out]  st  analytical storage
@return the DuckDB database, for opening query connections */
duckdb_database storage_get_database(storage_t &st) {
  return (st.duck->db);
}

/** Borrow the stored catalog as of the last committed change.
@param[in,out]  st  analytical storage
@return the state as of the last commit, valid until the next storage_flush()
  or storage_close() */
const storage_state_t *storage_get_state(storage_t &st) {
  return (&st.state);
}

/** Run a read-only SQL query returning one integer.
@param[in,out]  st     analytical storage
@param[in]      sql    SQL statement to check or execute
@param[out]     value  single non-NULL integer result
@return DB_SUCCESS or DB_ERROR */
dberr_t storage_query_int(storage_t &st, const std::string &sql,
                          int64_t &value) {
  duck_rows_t rows;

  const dberr_t err = duck_query(*st.duck, sql, rows);
  if (err != DB_SUCCESS) {
    return (err);
  }

  if (rows.size() != 1 || rows[0].size() != 1 || rows[0][0].is_null) {
    return (DB_ERROR);
  }

  value = rows[0][0].i;

  return (DB_SUCCESS);
}

#ifdef PT_DEBUG
/** Make the next storage_flush() fail after it has written rows, before it
commits.
@param[in,out]  st  analytical storage */
void storage_dbg_fail_next_flush(storage_t &st) {
  st.dbg_fail_next_flush = true;
}
#endif
