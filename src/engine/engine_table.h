/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine_table.h
 *    Stable table entries shared by writes and ordinary DDL.
 *
 *-------------------------------------------------------------------------
 */

#ifndef engine_table_h
#define engine_table_h

#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>

#include "schema.h"
#include "ut_err.h"

/** A table or a name currently used by writes or ordinary DDL. */
struct engine_table_t {
  /** Canonical name, fixed before insertion in the registry. */
  std::string m_name;

  /** Latest immutable schema, owned by the memtable; NULL before creation. */
  std::atomic<const schema_t *> m_schema{nullptr};

  /** Committed ordinary-table state, protected by the registry lock. */
  bool m_is_ordinary{false};

  /** Transactions reserving this name; protected by the registry lock. */
  size_t m_active_ddl{0};

  /** References acquired before the entry became a permanent time-series table.
  Only the registry updates this count, while holding its shared/exclusive lock
  or after the schema has made the entry permanent. */
  std::atomic<size_t> m_references{0};
};

struct engine_tables_t;

/** Keep a name entry alive for one operation. Existing time-series entries
need no reference count; their immutable schemas keep them registered until
engine close. The registry must outlive every reference. */
struct engine_table_ref_t {
  engine_table_ref_t() = default;
  ~engine_table_ref_t();
  engine_table_ref_t(const engine_table_ref_t &) = delete;
  engine_table_ref_t &operator=(const engine_table_ref_t &) = delete;
  engine_table_ref_t(engine_table_ref_t &&other) noexcept;
  engine_table_ref_t &operator=(engine_table_ref_t &&other) noexcept;

  engine_table_t *get() const { return (m_table); }
  engine_table_t *operator->() const { return (m_table); }
  engine_table_t &operator*() const { return (*m_table); }
  void reset();

 private:
  friend struct engine_tables_t;
  engine_tables_t *m_registry{nullptr};
  engine_table_t *m_table{nullptr};
  bool m_pinned{false};
};

/** Name registry. Time-series tables live until close. Absent names are
removed once no operation references or reserves them; ordinary names remain
registered until the table is dropped and its last user releases it. */
struct engine_tables_t {
  /** Protects entry insertion/removal, ordinary state and DDL reservations.
  Taken after write_mutex when both locks are needed. Never held during I/O. */
  mutable std::shared_mutex m_mutex;

  /** Find under a shared lock, or insert after an exclusive recheck.
  Replaces out; its reference must outlive every use of the entry.
  @return DB_SUCCESS or DB_OUT_OF_MEMORY */
  db_status_t find_or_create(const std::string &name, engine_table_ref_t &out);

  /** Number of registered names, including entries in use by operations. */
  size_t size() const;

 private:
  friend struct engine_table_ref_t;
  void release(engine_table_t *table, bool pinned);
  std::map<std::string, std::unique_ptr<engine_table_t>> m_entries;
};

#endif /* engine_table_h */
