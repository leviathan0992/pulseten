/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_storage.h
 *    Storage assertions using the normal borrowed connection interface.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_storage_h
#define test_storage_h

#include <cstdint>
#include <string>

#include "duckdb_database.h"
#include "storage.h"
#include "ut_err.h"

/** Read a scalar integer; the test caller excludes storage mutations. */
static inline db_status_t test_storage_query_int(storage &store,
                                                 const std::string &sql,
                                                 int64_t &value) {
  duckdb_rows_t rows;
  const db_status_t err = duckdb_exec(store.connection(), sql, rows);
  if (!err.ok()) {
    return (err);
  }

  if (rows.size() != 1 || rows[0].size() != 1 || rows[0][0].m_is_null) {
    return (DB_ERROR);
  }

  value = rows[0][0].m_i;

  return (DB_SUCCESS);
}

#endif /* test_storage_h */
