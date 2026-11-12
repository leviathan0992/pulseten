/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_table.cc
 *    Stable table-cache lookup and insertion.
 *
 *-------------------------------------------------------------------------
 */

#include "db_table.h"

#include <mutex>
#include <shared_mutex>
#include <utility>

#include "db.h"
#include "query.h"
#include "ut_new.h"
#include "ut_shared_mutex.h"

/** Find under a shared cache lock, or insert after an exclusive recheck. */
db_status_t db_tables::find_or_create(const std::string &name,
                                      db_table_t *&out) {
  out = find(name);
  if (out != nullptr) {
    return (DB_SUCCESS);
  }

  std::unique_lock<ut_shared_mutex_t> lock(m_mutex);
  auto it = m_entries.find(name);
  if (it == m_entries.end()) {
    std::unique_ptr<db_table_t> table(ut_new<db_table_t>());
    if (table == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }

    it = m_entries.emplace(name, std::move(table)).first;
  }

  out = it->second.get();

  return (DB_SUCCESS);
}

/** Read an entry while protecting the cache structure, not its contents. */
db_table_t *db_tables::find(const std::string &name) const {
  std::shared_lock<ut_shared_mutex_t> lock(m_mutex);
  const auto it = m_entries.find(name);

  return (it == m_entries.end() ? nullptr : it->second.get());
}

/** Acquire and retain one mode, checking database failure outside lock state.
 */
db_status_t db_table_lock::acquire(db &database, db_table_t &table,
                                   db_table_lock_mode_t mode,
                                   const query_cancel *cancel) {
  ut_a(m_table == nullptr);

  db_status_t err;
  /* The lock API takes a predicate; capture this operation's diagnostic. */
  auto allowed = [&] {
    if (cancel != nullptr && cancel->requested()) {
      err = DB_INTERRUPTED;
    } else {
      err = database.check_status();
    }
    return (err.ok());
  };
  const bool acquired = mode == db_table_lock_mode_t::EXCLUSIVE
                            ? table.m_mutex.lock_while(allowed)
                            : table.m_mutex.lock_shared_while(allowed);
  ut_a(acquired == err.ok());
  if (acquired) {
    m_table = &table;
    m_mode = mode;
  }

  return (err);
}

/** Release exactly the mode this guard acquired. */
void db_table_lock::release() {
  if (m_table == nullptr) {
    return;
  }

  if (m_mode == db_table_lock_mode_t::EXCLUSIVE) {
    m_table->m_mutex.unlock();
  } else {
    m_table->m_mutex.unlock_shared();
  }

  m_table = nullptr;
  m_mode = db_table_lock_mode_t::SHARED;

  return;
}
