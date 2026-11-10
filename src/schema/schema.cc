/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * schema.cc
 *    Name rules and column lookup for table schemas.
 *
 *-------------------------------------------------------------------------
 */

#include "schema.h"

#include <cstring>
#include <string>
#include <vector>

#include "ut_err.h"

/** Check an ASCII identifier's first character.
@return true if c may start an identifier: an ASCII letter or '_' */
static bool schema_is_name_start(char c) {
  return ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_');
}

/** Check whether a character is an ASCII digit.
@return true if c is an ASCII digit */
static bool schema_is_digit(char c) {
  return (c >= '0' && c <= '9');
}

/** Check a table or column name and convert it to canonical form. */
db_status_t schema_canonical_name(const char *name, size_t len,
                                  std::string &out) {
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
      out[i] = static_cast<char>(out[i] - 'A' + 'a');
    }
  }

  const size_t prefix_len = strlen(SCHEMA_RESERVED_PREFIX);
  if (out.compare(0, prefix_len, SCHEMA_RESERVED_PREFIX) == 0) {
    out.clear();

    return (DB_INVALID_ARGUMENT);
  }

  return (DB_SUCCESS);
}

/** Check a table name and convert it to canonical form. */
db_status_t schema_canonical_table_name(const char *name, size_t len,
                                        std::string &out) {
  const db_status_t err = schema_canonical_name(name, len, out);
  if (!err.ok()) {
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

/** Find a column by canonical name. */
const col_def_t *schema_find_col(const std::vector<col_def_t> &cols,
                                 const std::string &name) {
  for (size_t i = 0; i < cols.size(); ++i) {
    if (cols[i].m_name == name) {
      return (&cols[i]);
    }
  }

  return (nullptr);
}

/** Check whether a column type is valid for a field. */
bool schema_field_type_is_valid(col_type_t type) {
  switch (type) {
    case col_type_t::BOOLEAN:
    case col_type_t::BIGINT:
    case col_type_t::DOUBLE:
    case col_type_t::VARCHAR:
      return (true);
    case col_type_t::TIMESTAMP:
      return (false);
  }

  return (false);
}

/** Name a column type in SQL; the names are also DuckDB's. */
const char *schema_type_name(col_type_t type) {
  switch (type) {
    case col_type_t::BOOLEAN:
      return ("BOOLEAN");
    case col_type_t::BIGINT:
      return ("BIGINT");
    case col_type_t::DOUBLE:
      return ("DOUBLE");
    case col_type_t::TIMESTAMP:
      return ("TIMESTAMP");
    case col_type_t::VARCHAR:
      return ("VARCHAR");
  }

  return ("VARCHAR");
}
