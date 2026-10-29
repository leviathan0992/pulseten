/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * schema0schema.cc
 *    Name rules and column lookup for table schemas.
 *
 *-------------------------------------------------------------------------
 */

#include "schema0schema.h"

#include <cstring>

/** Check an ASCII identifier's first character.
@param[in]  c  ASCII character to check
@return true if c may start an identifier: an ASCII letter or '_' */
static bool schema_is_name_start(char c) {
  return ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_');
}

/** Check whether a character is an ASCII digit.
@param[in]  c  character to check
@return true if c is an ASCII digit */
static bool schema_is_digit(char c) {
  return (c >= '0' && c <= '9');
}

/** Check a table or column name and convert it to canonical form.
@param[in]   name  name as given by the user
@param[in]   len   length of name in bytes
@param[out]  out   lower-case name
@return DB_SUCCESS, or DB_INVALID_ARGUMENT if the name is empty, too long, not
  an identifier or reserved */
dberr_t schema_canonical_name(const char *name, size_t len, std::string &out) {
  out.clear();

  if (name == nullptr || len == 0 || len > SCHEMA_MAX_NAME_LEN) {
    return (DB_INVALID_ARGUMENT);
  }

  /* An identifier: a letter or '_', then letters, digits or '_'. */
  if (!schema_is_name_start(name[0])) {
    return (DB_INVALID_ARGUMENT);
  }

  for (size_t i = 1; i < len; ++i) {
    if (!schema_is_name_start(name[i]) && !schema_is_digit(name[i])) {
      return (DB_INVALID_ARGUMENT);
    }
  }

  out.assign(name, len);

  for (size_t i = 0; i < len; ++i) {
    if (out[i] >= 'A' && out[i] <= 'Z') {
      out[i] = (char)(out[i] - 'A' + 'a');
    }
  }

  const size_t prefix_len = strlen(SCHEMA_RESERVED_PREFIX);
  if (out.compare(0, prefix_len, SCHEMA_RESERVED_PREFIX) == 0) {
    out.clear();

    return (DB_INVALID_ARGUMENT);
  }

  return (DB_SUCCESS);
}

/** Check a table name and convert it to canonical form.
@param[in]   name  name as given by the user
@param[in]   len   length of name in bytes
@param[out]  out   lower-case name
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
dberr_t schema_table_name(const char *name, size_t len, std::string &out) {
  const dberr_t err = schema_canonical_name(name, len, out);
  if (err != DB_SUCCESS) {
    return (err);
  }

  static const char *prefixes[] = {"duckdb_", "sqlite_", "pragma_", "pg_"};

  for (size_t i = 0; i < sizeof(prefixes) / sizeof(prefixes[0]); ++i) {
    if (out.compare(0, strlen(prefixes[i]), prefixes[i]) == 0) {
      out.clear();

      return (DB_INVALID_ARGUMENT);
    }
  }

  return (DB_SUCCESS);
}

/** Find a column by canonical name.
@param[in]  cols  columns to search
@param[in]  name  canonical name
@return the column, or NULL */
const col_def_t *schema_find_col(const std::vector<col_def_t> &cols,
                                 const std::string &name) {
  for (size_t i = 0; i < cols.size(); ++i) {
    if (cols[i].name == name) {
      return (&cols[i]);
    }
  }

  return (nullptr);
}

/** Check whether a column type is valid for a field.
@param[in]  type  column value type
@return true if type is allowed for a field */
bool schema_field_type_is_valid(col_type_t type) {
  switch (type) {
    case COL_BOOLEAN:
    case COL_BIGINT:
    case COL_DOUBLE:
    case COL_VARCHAR:
      return (true);
    case COL_TIMESTAMP:
      return (false);
  }

  return (false);
}

/** Name a column type in SQL; the names are also DuckDB's.
@param[in]  type  column value type
@return the SQL type name */
const char *schema_type_name(col_type_t type) {
  switch (type) {
    case COL_BOOLEAN:
      return ("BOOLEAN");
    case COL_BIGINT:
      return ("BIGINT");
    case COL_DOUBLE:
      return ("DOUBLE");
    case COL_TIMESTAMP:
      return ("TIMESTAMP");
    case COL_VARCHAR:
      return ("VARCHAR");
  }

  return ("VARCHAR");
}
