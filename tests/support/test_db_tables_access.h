/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_db_tables_access.h
 *    White-box table-cache inspection.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_db_tables_access_h
#define test_db_tables_access_h

#ifndef PT_TESTING
#error "test_db_tables_access.h requires the PulseTen test library"
#endif

#include <cstddef>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "db_table.h"
#include "ut.h"

/** Inspect cache ownership without adding production inspection methods. */
class db_tables_test_access final {
 public:
  db_tables_test_access() = delete;

  /** Inspect table lock state in concurrency tests. */
  static ut_rwlock &lock(db_table &table) { return (table.m_lock); }

  /** Count stable name slots while holding the cache structure lock. */
  static size_t size(const db_tables &tables) {
    std::shared_lock<ut_shared_mutex_t> lock(tables.m_mutex);

    return (tables.m_entries.size());
  }

  /** Retain each initialized table's immutable schema in name order. */
  static void collect_schemas(const db_tables &tables,
                              std::vector<schema_ref_t> &schemas) {
    schemas.clear();
    std::shared_lock<ut_shared_mutex_t> lock(tables.m_mutex);

    for (const auto &[name, table] : tables.m_entries) {
      schema_ref_t schema = table->schema();
      if (schema != nullptr) {
        schemas.push_back(std::move(schema));
      }
    }
  }
};

#endif /* test_db_tables_access_h */
