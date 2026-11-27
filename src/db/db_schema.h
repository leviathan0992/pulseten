/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_schema.h
 *    Table schemas of the schema-on-write data model.
 *
 * A table is created by its first write. Column 0 is the
 * non-NULL TIMESTAMP column "time". Every other column is a tag (VARCHAR)
 * or a field (BOOLEAN, BIGINT, DOUBLE or VARCHAR) and may be NULL. Columns
 * are only ever added, at the end, with the next column id; a column never
 * changes its column type or data type. Each change publishes a new immutable
 * column directory.
 *
 * Table and column names are ASCII identifiers matched case-insensitively
 * and stored in lower case. Names starting with "_pt_" are reserved.
 *
 *-------------------------------------------------------------------------
 */

#ifndef db_schema_h
#define db_schema_h

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "ut.h"

/** Data types. Numeric values are part of the WAL format. */
enum class data_type_t {
  /** True or false. */
  BOOLEAN = 1,

  /** A signed 64-bit integer. */
  BIGINT = 2,

  /** An IEEE 754 double; write input must be finite. */
  DOUBLE = 3,

  /** Microseconds since 1970-01-01 00:00:00 UTC. */
  TIMESTAMP = 4,

  /** A UTF-8 string. */
  VARCHAR = 5
};

/** Column types. Numeric values are part of the WAL format. */
enum class column_type_t {
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
#define SCHEMA_MAX_NAME_BYTES 128

/** Name of column 0. */
#define SCHEMA_TIME_COLUMN_NAME "time"

/** Reserved name prefix. */
#define SCHEMA_RESERVED_PREFIX "_pt_"

/** One column of a table. */
struct schema_column_t {
  /** Position in the table, 0 for the time column. */
  uint32_t m_id{0};

  /** Canonical (lower-case) name. */
  std::string m_name;

  /** Time, tag or field; never changes. */
  column_type_t m_column_type{column_type_t::FIELD};

  /** Value type; never changes. */
  data_type_t m_data_type{data_type_t::VARCHAR};
};

/** Column count identifies an append-only schema version within one table.
Zero is invalid; versions are bounded by SCHEMA_MAX_COLUMNS. */
typedef uint32_t schema_version_t;

/** A logical column directory. Never modified once published; adding
columns publishes a new directory. */
struct schema_t {
  /** Canonical table name. */
  std::string m_name;

  /** m_columns[i].m_id == i. */
  std::vector<schema_column_t> m_columns;
};

/** Shared ownership of an immutable schema. */
using schema_ref_t = std::shared_ptr<const schema_t>;

/** Validate a table or column name and normalize it to lowercase.
@return DB_SUCCESS, or DB_INVALID_ARGUMENT if the name is empty, too long, not
  an identifier or reserved */
db_status_t schema_normalize_name(const char *name, size_t len,
                                  std::string &out);

/** Validate a table name and normalize it to lowercase. Beyond the rules for
column names, DuckDB's catalog prefixes are reserved, so that every table can
be queried.
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
db_status_t schema_normalize_table_name(const char *name, size_t len,
                                        std::string &out);

/** Validate a complete column list: time column, consecutive ids, unique
normalized names and valid types. Table names are checked separately. */
bool schema_validate_columns(const std::vector<schema_column_t> &cols);

/** Find a column by normalized name.
@param[in]  name  normalized name
@return the column, or NULL */
const schema_column_t *schema_find_column(std::span<const schema_column_t> cols,
                                          const std::string &name);

/** Check whether a data type is valid for a field.
@return true if type is allowed for a field */
bool schema_validate_field_type(data_type_t type);

/** Return the SQL type name used by DuckDB. */
const char *schema_data_type_name(data_type_t type);

#endif /* db_schema_h */
