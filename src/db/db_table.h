/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_table.h
 *    Database-owned table objects and their S/X locks.
 *
 * db_tables maps each normalized name to one db_table, created on first use
 * and kept until close, so a pointer to it never dangles. An entry without a
 * schema only reserves the name for a first write.
 *
 * A table's S/X lock orders operations: writes to existing columns and
 * queries take S; adding columns takes X. Each schema version is immutable
 * and is retained for its table incarnation, so batches under an older version
 * remain readable after the table grows.
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
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db_schema.h"
#include "ut.h"

/** Forward declarations. */
class db;
class memtable;
struct ingest_plan_t;
class query_cancel;

/** Access required by a table operation. */
enum class db_table_lock_mode_t {
  /** Concurrent access to existing columns. */
  SHARED,

  /** Exclusive access while adding columns. */
  EXCLUSIVE
};

/** Metadata for one table in a catalog snapshot. */
struct db_table_info_t {
  /** Normalized time-series table name. */
  std::string m_name;

  /** Retained immutable schema. NULL until the columns are initialized. */
  schema_ref_t m_schema;

  /** Fixed duration of each partition in hours. */
  uint32_t m_partition_duration_hours{0};

  /** Retention override in hours. An empty value inherits the database
  default. Zero keeps all data. */
  std::optional<uint32_t> m_retention_hours;
};

/** A stable table object retained until database close.
A slot can be unused, declared without columns, or initialized.
An empty schema does not imply that a SQL table exists. */
class db_table final : private ut_noncopyable {
 public:
  /** Retain the current immutable time-series schema.
  @return the schema, or NULL before its columns are initialized */
  schema_ref_t schema() const { return (std::atomic_load(&m_schema)); }

  /** Check whether this slot names a declared or initialized table. */
  bool exists() const { return (m_declared.load() || schema() != nullptr); }

  /** In-memory incarnation of this name; advances after a committed drop. */
  uint64_t generation() const { return (m_generation.load()); }

  /** Publish a time-series schema while writes are serialized. */
  void publish_schema(schema_ref_t schema);

  /** Resolve a previously accepted version; NULL if it is unknown. */
  schema_ref_t schema(schema_version_t version) const;

  /** Prepare a schema, reserve its batch, then publish the version.
  Caller serializes reservation and schema changes for this table. */
  db_status_t reserve_write(memtable &mt, batch_seq_t seq, ingest_plan_t &plan);

 private:
  friend class db_tables;
  friend class db;
  friend class db_cursor;

  /** Guards own the table lock through each operation. */
  friend class db_table_lock;

#ifdef PT_TESTING
  friend class db_tables_test_access;
#endif /* PT_TESTING */

  /** Check the batch and schema change against the retained schema. */
  static bool validate_plan(const ingest_plan_t &plan, const schema_t *current);

  /** Validate a write and prepare its immutable schema without publishing it.
  Caller serializes writes to this table through acceptance.
  @return DB_SUCCESS, DB_CORRUPTION or DB_OUT_OF_MEMORY */
  db_status_t prepare_schema(const ingest_plan_t &plan,
                             schema_ref_t &out) const;

  /* ================= Table operation serialization ================== */

  /** Serializes operations and schema changes; acquired before write and
  schema mutexes. */
  ut_rwlock m_lock;

  /* ======================== Schema versions ========================= */

  /** Protects this group. Never held while entering the memtable. */
  mutable std::mutex m_schema_mutex;

  /** At most SCHEMA_MAX_COLUMNS versions per incarnation; owned references
  remain valid after the table is dropped. */
  std::map<schema_version_t, schema_ref_t> m_versions;

  /* ================== Atomically published schema =================== */

  /** Current immutable schema; readers use atomic_load without a table lock.
  Publication uses atomic_store while m_schema_mutex is held. */
  schema_ref_t m_schema;

  /** Publish a new incarnation only after clearing the previous table state. */
  std::atomic<uint64_t> m_generation{0};

  /** A persisted declaration reserves the name before schema initialization. */
  std::atomic<bool> m_declared{false};

  /** Fixed after recovery or first reservation; read under the table lock. */
  uint32_t m_partition_duration_hours{0};

  /** Retention override, published under table X and snapshot X. */
  std::optional<uint32_t> m_retention_hours;
};

/** Own table-name slots until close. Returned pointers remain valid across
insertions and failed operations; no entry is erased while the database is
open. */
class db_tables final : private ut_noncopyable {
 public:
  /** Find or create a name slot. Release the cache lock before taking its S/X
  lock. The cache must outlive every operation using the returned pointer.
  @param[out] out table object, or NULL on failure
  @return DB_SUCCESS or DB_OUT_OF_MEMORY */
  db_status_t find_or_create(const std::string &name, db_table *&out);

  /** Find an existing slot without creating one.
  @return its stable pointer, or NULL */
  db_table *find(const std::string &name) const;

  /** Copy stable slots in name order; callers inspect state after releasing
  the directory mutex. Slots remain alive until close. */
  void collect_entries(
      std::vector<std::pair<std::string, db_table *>> &entries) const;

  /** Restore a persistent name with no column directory before WAL replay. */
  db_status_t restore_declared(const std::string &name);

  /** Collect all declared and initialized names, replacing the output. */
  void collect_names(std::set<std::string> &names) const;

  /** Restore a stored table schema before WAL replay.
  @return DB_SUCCESS, DB_CORRUPTION for duplicates, or DB_OUT_OF_MEMORY */
  db_status_t restore_schema(const schema_t &schema);

 private:
#ifdef PT_TESTING
  /** Cache inspection belongs to the test support code. */
  friend class db_tables_test_access;
#endif /* PT_TESTING */

  /* ======================== Table directory ========================= */

  /** Protects this group; released before table operations or I/O. */
  mutable ut_shared_mutex_t m_mutex;

  /** Owned objects; map keys provide their normalized names. */
  std::map<std::string, std::unique_ptr<db_table>> m_entries;
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
  @param[in] timeout_ms wait limit; 0 waits without a deadline
  @return DB_SUCCESS with the lock held; DB_FENCED, DB_INTERRUPTED or
  DB_LOCK_TIMEOUT leaves this guard empty */
  db_status_t acquire(db &database, db_table &table, db_table_lock_mode_t mode,
                      const query_cancel *cancel = nullptr,
                      uint32_t timeout_ms = 0);

  /** Release the held mode; an empty guard needs no action. */
  void release();

  /** Return the locked table, or NULL if no lock is held. */
  db_table *table() const { return (m_table); }

  /** Check whether this guard holds the given table. */
  bool holds(const db_table *table) const {
    return (m_table != nullptr && m_table == table);
  }

  /** Check whether this guard holds an exclusive lock. */
  bool exclusive() const {
    return (m_table != nullptr && m_mode == db_table_lock_mode_t::EXCLUSIVE);
  }

 private:
  /** Held table; NULL while empty or waiting. */
  db_table *m_table{nullptr};

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

  /** Check whether this operation already holds a mode on the object. */
  bool holds(const db_table *table) const {
    for (const db_table_lock &lock : m_locks) {
      if (lock.holds(table)) {
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
