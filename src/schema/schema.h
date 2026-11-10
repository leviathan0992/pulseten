/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * schema.h
 *    Table schemas of the schema-on-write data model.
 *
 * A table is created by its first write. Column 0 is the
 * non-NULL TIMESTAMP column "time". Every other column is a tag (VARCHAR)
 * or a field (BOOLEAN, BIGINT, DOUBLE or VARCHAR) and may be NULL. Columns
 * are only ever added, at the end, with the next column id; a column never
 * changes its kind or type. Each change creates a new schema version.
 *
 * Table and column names are ASCII identifiers matched case-insensitively
 * and stored in lower case. Names starting with "_pt_" are reserved.
 *
 *-------------------------------------------------------------------------
 */

#ifndef schema_h
#define schema_h

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "ut_err.h"

/** Value types. The numbers are part of the WAL format. */
enum class col_type_t {
  /** True or false. */
  BOOLEAN = 1,

  /** A signed 64-bit integer. */
  BIGINT = 2,

  /** A finite IEEE 754 double. */
  DOUBLE = 3,

  /** Microseconds since 1970-01-01 00:00:00 UTC. */
  TIMESTAMP = 4,

  /** A UTF-8 string. */
  VARCHAR = 5
};

/** Column kinds. The numbers are part of the WAL format. */
enum class col_kind_t {
  /** Column 0, the TIMESTAMP "time". */
  TIME = 0,

  /** A VARCHAR tag. */
  TAG = 1,

  /** A field of any field type. */
  FIELD = 2
};

/** Most columns in a table, the time column included. */
#define SCHEMA_MAX_COLUMNS 64

/** Longest table or column name in bytes. */
#define SCHEMA_MAX_NAME_LEN 128

/** Name of column 0. */
#define SCHEMA_TIME_COLUMN "time"

/** Reserved name prefix. */
#define SCHEMA_RESERVED_PREFIX "_pt_"

/** One column of a table. */
struct col_def_t {
  /** Position in the table, 0 for the time column. */
  uint32_t m_id{0};

  /** Canonical (lower-case) name. */
  std::string m_name;

  /** Time, tag or field; never changes. */
  col_kind_t m_kind{col_kind_t::FIELD};

  /** Value type; never changes. */
  col_type_t m_type{col_type_t::VARCHAR};
};

/** One version of a table's schema. Never modified once published; a
change produces a new schema_t with a higher version. */
struct schema_t {
  /** Canonical table name. */
  std::string m_name;

  /** 1 for the first version. */
  uint64_t m_version{0};

  /** m_cols[i].m_id == i. */
  std::vector<col_def_t> m_cols;
};

/** Check a table or column name and convert it to canonical form.
@return DB_SUCCESS, or DB_INVALID_ARGUMENT if the name is empty, too long, not
  an identifier or reserved */
db_status_t schema_canonical_name(const char *name, size_t len,
                                  std::string &out);

/** Check a table name and convert it to canonical form. Beyond the rules for
column names, DuckDB's catalog prefixes are reserved, so that every table can
be queried.
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
db_status_t schema_canonical_table_name(const char *name, size_t len,
                                        std::string &out);

/** Find a column by canonical name.
@param[in]  name  canonical name
@return the column, or NULL */
const col_def_t *schema_find_col(const std::vector<col_def_t> &cols,
                                 const std::string &name);

/** Check whether a column type is valid for a field.
@return true if type is allowed for a field */
bool schema_field_type_is_valid(col_type_t type);

/** Name a column type in SQL; the names are also DuckDB's.
@return the SQL type name */
const char *schema_type_name(col_type_t type);

#endif
