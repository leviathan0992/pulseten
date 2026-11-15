/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_schema.cc
 *    Schema validation, name rules and column lookup.
 *
 *-------------------------------------------------------------------------
 */

#include "db_schema.h"

#include <cstring>
#include <set>
#include <string>
#include <vector>

#include "ut.h"

/** Table-name prefixes reserved for DuckDB system objects. */
static constexpr const char *schema_reserved_table_prefixes[] = {
    "duckdb_", "sqlite_", "pragma_", "pg_"};

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

/** Validate an identifier and normalize ASCII letters to lowercase. */
db_status_t schema_normalize_name(const char *name, size_t len,
                                  std::string &out) {
  out.clear();

  if (name == nullptr) {
    return (db_status_t(DB_INVALID_ARGUMENT, "name must not be NULL"));
  }

  if (len == 0 || len > SCHEMA_MAX_NAME_BYTES) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "name length must be 1 to " +
                            std::to_string(SCHEMA_MAX_NAME_BYTES) + " bytes"));
  }

  /* An identifier: a letter or '_', then letters, digits or '_'. */
  if (!schema_is_name_start(name[0])) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "name must start with an ASCII letter or underscore"));
  }

  for (size_t i = 1; i < len; ++i) {
    if (!schema_is_name_start(name[i]) && !schema_is_digit(name[i])) {
      return (
          db_status_t(DB_INVALID_ARGUMENT,
                      "invalid name character at byte " + std::to_string(i)));
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

    return (db_status_t(DB_INVALID_ARGUMENT,
                        "reserved name prefix: " SCHEMA_RESERVED_PREFIX));
  }

  return (DB_SUCCESS);
}

/** Validate a table name and normalize it to lowercase. */
db_status_t schema_normalize_table_name(const char *name, size_t len,
                                        std::string &out) {
  if (const db_status_t err = schema_normalize_name(name, len, out);
      !err.ok()) {
    return (err);
  }

  for (const char *prefix : schema_reserved_table_prefixes) {
    if (out.compare(0, strlen(prefix), prefix) == 0) {
      out.clear();

      return (db_status_t(DB_INVALID_ARGUMENT, "reserved table name prefix: " +
                                                   std::string(prefix)));
    }
  }

  return (DB_SUCCESS);
}

/** Validate column order, names and types for a complete table schema. */
bool schema_validate_columns(const std::vector<schema_column_t> &cols) {
  if (cols.empty()) {
    return (false);
  }

  if (cols.size() > SCHEMA_MAX_COLUMNS) {
    return (false);
  }

  /* Column 0 is the time column. */
  const schema_column_t &time_col = cols[0];
  if (time_col.m_column_type != column_type_t::TIME ||
      time_col.m_data_type != data_type_t::TIMESTAMP ||
      time_col.m_name != SCHEMA_TIME_COLUMN_NAME) {
    return (false);
  }

  std::set<std::string> names;

  for (size_t i = 0; i < cols.size(); ++i) {
    const schema_column_t &c = cols[i];

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
      const bool tag_ok = c.m_column_type == column_type_t::TAG &&
                          c.m_data_type == data_type_t::VARCHAR;
      const bool field_ok = c.m_column_type == column_type_t::FIELD &&
                            schema_validate_field_type(c.m_data_type);

      if (!tag_ok && !field_ok) {
        return (false);
      }
    }
  }

  return (true);
}

/** Find a column by normalized name; return NULL if absent. */
const schema_column_t *schema_find_column(
    const std::vector<schema_column_t> &cols, const std::string &name) {
  for (size_t i = 0; i < cols.size(); ++i) {
    if (cols[i].m_name == name) {
      return (&cols[i]);
    }
  }

  return (nullptr);
}

/** Check field data types; TIMESTAMP is reserved for the time column. */
bool schema_validate_field_type(data_type_t type) {
  switch (type) {
    case data_type_t::BOOLEAN:
    case data_type_t::BIGINT:
    case data_type_t::DOUBLE:
    case data_type_t::VARCHAR:
      return (true);

    case data_type_t::TIMESTAMP:
      return (false);
  }

  return (false);
}

/** Return the SQL name of a data type. */
const char *schema_data_type_name(data_type_t type) {
  switch (type) {
    case data_type_t::BOOLEAN:
      return ("BOOLEAN");

    case data_type_t::BIGINT:
      return ("BIGINT");

    case data_type_t::DOUBLE:
      return ("DOUBLE");

    case data_type_t::TIMESTAMP:
      return ("TIMESTAMP");

    case data_type_t::VARCHAR:
      return ("VARCHAR");
  }

  return ("VARCHAR");
}
