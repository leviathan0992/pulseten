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

#include "db_table.h"
#include "ut_shared_mutex.h"

/** Inspect cache ownership without adding production inspection methods. */
class db_tables_test_access final {
 public:
  /** This access class contains only static operations. */
  db_tables_test_access() = delete;

  /** Count stable name slots while holding the cache structure lock. */
  static size_t size(const db_tables &tables) {
    std::shared_lock<ut_shared_mutex_t> lock(tables.m_mutex);

    return (tables.m_entries.size());
  }
};

#endif /* test_db_tables_access_h */
