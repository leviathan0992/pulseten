/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_table.h
 *    Database-owned table objects and their S/X locks.
 *
 *-------------------------------------------------------------------------
 */

#ifndef db_table_h
#define db_table_h

#include <atomic>
#include <cstddef>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "schema.h"
#include "ut_err.h"
#include "ut_noncopyable.h"
#include "ut_rwlock.h"
#include "ut_shared_mutex.h"

class db;
class query_cancel;

/** Access required by a table operation. */
enum class db_table_lock_mode_t {
  /** Concurrent access to existing columns. */
  SHARED,

  /** Exclusive access while adding columns. */
  EXCLUSIVE
};

/** A stable table object retained until database close.
An empty schema is a name slot, not evidence that a DuckDB user table exists. */
struct db_table_t {
  /** Provides the table-name S/X synchronization boundary. */
  ut_rwlock_t m_mutex;

  /** Retain the current immutable time-series schema.
  @return the schema, or NULL before a time-series table is created */
  schema_ref_t schema() const { return (std::atomic_load(&m_schema)); }

  /** Publish a time-series schema while writes are serialized. */
  void set_schema(schema_ref_t schema) {
    std::atomic_store(&m_schema, std::move(schema));

    return;
  }

 private:
  /** Current immutable time-series schema; accessed through atomic operations.
   */
  schema_ref_t m_schema;
};

/** Own table-name slots until close. Returned pointers remain valid across
insertions and failed operations; no entry is erased while the database is open.
*/
class db_tables final : private ut_noncopyable {
 public:
  /** Find or create a name slot. Release the cache lock before taking its S/X
  lock. The cache must outlive every operation using the returned pointer.
  @param[out] out table object, or NULL on failure
  @return DB_SUCCESS or DB_OUT_OF_MEMORY */
  db_status_t find_or_create(const std::string &name, db_table_t *&out);

  /** Find an existing slot without creating one.
  @return its stable pointer, or NULL */
  db_table_t *find(const std::string &name) const;

 private:
#ifdef PT_TESTING
  /** Cache inspection belongs to the test support code. */
  friend class db_tables_test_access;
#endif /* PT_TESTING */

  /** Protects cache lookup and insertion, never table operations or I/O. */
  mutable ut_shared_mutex_t m_mutex;

  /** Owned objects; map keys provide their normalized names. */
  std::map<std::string, std::unique_ptr<db_table_t>> m_entries;
};

/** Own one table lock, shared or exclusive, until release or scope exit.
The table must outlive this guard. Acquisition starts from an empty guard;
changing modes requires releasing it first, then revalidating protected state.
*/
class db_table_lock final : private ut_noncopyable {
 public:
  /** Release the currently held mode. */
  ~db_table_lock() { release(); }

  /** Acquire a table lock, observing database failure and query cancellation.
  No database or table state mutex remains held during the wait.
  @param[in] cancel optional request state, alive until acquisition returns
  @return DB_SUCCESS with the lock held; DB_FENCED or DB_INTERRUPTED leaves
    this guard empty */
  db_status_t acquire(db &database, db_table_t &table,
                      db_table_lock_mode_t mode,
                      const query_cancel *cancel = nullptr);

  /** Release the held mode; an empty guard needs no action. */
  void release();

  /** Borrow the locked table; NULL while the guard is empty. */
  db_table_t *table() const { return (m_table); }

  /** Whether this guard holds the given table. */
  bool holds(const db_table_t *table) const {
    return (m_table != nullptr && m_table == table);
  }

  /** Whether this guard holds an exclusive lock. */
  bool exclusive() const {
    return (m_table != nullptr && m_mode == db_table_lock_mode_t::EXCLUSIVE);
  }

 private:
  /** Held table; NULL while empty or waiting. */
  db_table_t *m_table{nullptr};

  /** Mode of the held lock; meaningful only when m_table is non-NULL. */
  db_table_lock_mode_t m_mode{db_table_lock_mode_t::SHARED};
};

/** Release table locks after an operation's transaction and snapshots end.
Shared acquisitions can be released by the thread that closes a cursor. */
class db_table_locks final : private ut_noncopyable {
 public:
  /** Release every acquired mode in reverse order. */
  ~db_table_locks() { release(); }

  /** Release every held mode and leave the guard empty. */
  void release() {
    while (!m_locks.empty()) {
      m_locks.pop_back();
    }

    return;
  }

  /** Whether this operation already holds a mode on the object. */
  bool holds(const db_table_t *table) const {
    for (const db_table_lock &held : m_locks) {
      if (held.holds(table)) {
        return (true);
      }
    }

    return (false);
  }

 private:
  /** The database acquires sorted requests through its single-table flow. */
  friend class db;

  /** Stable, nonmovable guards in acquisition order. */
  std::deque<db_table_lock> m_locks;
};

#endif /* db_table_h */
