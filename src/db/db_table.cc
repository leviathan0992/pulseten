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

#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "db.h"
#include "ingest.h"
#include "memtable.h"
#include "query.h"
#include "ut.h"

/** Find under a shared cache lock, or insert after an exclusive recheck. */
db_status_t db_tables::find_or_create(const std::string &name, db_table *&out) {
  out = find(name);
  if (out != nullptr) {
    return (DB_SUCCESS);
  }

  std::unique_lock<ut_shared_mutex_t> lock(m_mutex);
  auto it = m_entries.find(name);
  if (it == m_entries.end()) {
    std::unique_ptr<db_table> table(ut_new<db_table>());
    if (table == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }

    it = m_entries.emplace(name, std::move(table)).first;
  }

  out = it->second.get();

  return (DB_SUCCESS);
}

/** Read an entry while protecting the cache structure, not its contents. */
db_table *db_tables::find(const std::string &name) const {
  std::shared_lock<ut_shared_mutex_t> lock(m_mutex);
  const auto it = m_entries.find(name);

  return (it == m_entries.end() ? nullptr : it->second.get());
}

/** Retain slot identities without holding the directory across other locks. */
void db_tables::collect_entries(
    std::vector<std::pair<std::string, db_table *>> &entries) const {
  entries.clear();
  std::shared_lock<ut_shared_mutex_t> lock(m_mutex);
  entries.reserve(m_entries.size());
  for (const auto &[name, table] : m_entries) {
    entries.emplace_back(name, table.get());
  }
}

/** Acquire and retain one mode; check database failure outside lock state. */
db_status_t db_table_lock::acquire(db &database, db_table &table,
                                   db_table_lock_mode_t mode,
                                   const query_cancel *cancel,
                                   uint32_t timeout_ms) {
  ut_a(m_table == nullptr);

  db_status_t err;
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

  /* The lock API takes a predicate; capture this operation's diagnostic. */
  auto allowed = [&] {
    if (cancel != nullptr && cancel->requested()) {
      err = DB_INTERRUPTED;
    } else {
      err = database.status();
    }

    if (err.ok() && timeout_ms != 0 &&
        std::chrono::steady_clock::now() >= deadline) {
      err =
          db_status_t(DB_LOCK_TIMEOUT, "table lock wait exceeded " +
                                           std::to_string(timeout_ms) + " ms");
    }

    return (err.ok());
  };

  const bool acquired = mode == db_table_lock_mode_t::EXCLUSIVE
                            ? table.m_lock.lock_while(allowed)
                            : table.m_lock.lock_shared_while(allowed);
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
    m_table->m_lock.unlock();
  } else {
    m_table->m_lock.unlock_shared();
  }

  m_table = nullptr;
  m_mode = db_table_lock_mode_t::SHARED;

  return;
}

/** Check whether a plan is consistent with the table and schema state.
@return true if the consistency checks pass; allocation may still fail when
  applying the plan */
bool db_table::validate_plan(const ingest_plan_t &plan) const {
  const schema_ref_t latest = schema();
  const schema_t *current = latest.get();

  /* The table exists exactly when the plan does not create it. */
  if ((current == nullptr) != plan.m_creates_table) {
    return (false);
  }

  const size_t n_cols = current != nullptr ? current->m_columns.size() : 0;

  /* A schema change must be based on the current directory, and its columns
  must continue the column ids with new names. */
  if (!plan.m_new_columns.empty()) {
    if (plan.m_base_column_count != n_cols) {
      return (false);
    }

    if (n_cols + plan.m_new_columns.size() > SCHEMA_MAX_COLUMNS) {
      return (false);
    }

    for (size_t i = 0; i < plan.m_new_columns.size(); ++i) {
      const schema_column_t &def = plan.m_new_columns[i];

      if (def.m_id != n_cols + i) {
        return (false);
      }

      if (current != nullptr &&
          schema_find_column(current->m_columns, def.m_name)) {
        return (false);
      }

      for (size_t k = 0; k < i; ++k) {
        if (plan.m_new_columns[k].m_name == def.m_name) {
          return (false);
        }
      }
    }
  }

  /* A batch identifies its columns by their prefix length. Missing trailing
  columns of the current directory are NULL. */
  const size_t total_cols = n_cols + plan.m_new_columns.size();
  const size_t batch_columns = plan.m_batch.m_columns.size();
  if (batch_columns == 0 || batch_columns > total_cols) {
    return (false);
  }

  if (batch_columns != plan.m_base_column_count + plan.m_new_columns.size()) {
    return (false);
  }

  if (!plan.m_new_columns.empty() && batch_columns != total_cols) {
    return (false);
  }

  for (size_t i = 0; i < plan.m_batch.m_columns.size(); ++i) {
    const schema_column_t &column =
        i < n_cols ? current->m_columns[i] : plan.m_new_columns[i - n_cols];
    if (plan.m_batch.m_columns[i].m_column_id != i) {
      return (false);
    }

    if (plan.m_batch.m_columns[i].m_data_type != column.m_data_type) {
      return (false);
    }
  }

  return (true);
}

/** Allocate a new version only when this plan adds columns. */
db_status_t db_table::prepare_schema(const ingest_plan_t &plan,
                                     schema_ref_t &out) const {
  if (!validate_plan(plan)) {
    return (DB_CORRUPTION);
  }

  out = schema();
  if (plan.m_new_columns.empty()) {
    return (DB_SUCCESS);
  }

  schema_t *next = ut_new<schema_t>();
  if (next == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  if (out != nullptr) {
    *next = *out;
  }

  next->m_name = plan.m_table_name;
  next->m_columns.insert(next->m_columns.end(), plan.m_new_columns.begin(),
                         plan.m_new_columns.end());
  out.reset(next);

  return (DB_SUCCESS);
}

/** Register an immutable version before making it current. */
void db_table::publish_schema(schema_ref_t schema) {
  ut_a(schema != nullptr);

  if (this->schema() == schema) {
    return;
  }

  const schema_version_t version =
      static_cast<schema_version_t>(schema->m_columns.size());
  ut_a(version > 0 && version <= SCHEMA_MAX_COLUMNS);

  std::lock_guard<std::mutex> lock(m_schema_mutex);
  const auto [it, inserted] = m_versions.emplace(version, schema);
  ut_a(inserted || it->second == schema);

  std::atomic_store(&m_schema, std::move(schema));
}

/** Retain a schema version, or return NULL if it is unknown. */
schema_ref_t db_table::schema(schema_version_t version) const {
  std::lock_guard<std::mutex> lock(m_schema_mutex);
  const auto found = m_versions.find(version);

  return (found == m_versions.end() ? nullptr : found->second);
}

/** Reserve a persisted name without inventing a schema. */
db_status_t db_tables::restore_declared(const std::string &name) {
  db_table *table = nullptr;
  if (const db_status_t err = find_or_create(name, table); !err.ok()) {
    return (err);
  }

  if (table->exists()) {
    return (DB_CORRUPTION);
  }

  table->m_declared.store(true);

  return (DB_SUCCESS);
}

/** Include declarations while excluding unused slots from failed writes. */
void db_tables::collect_names(std::set<std::string> &names) const {
  names.clear();
  std::shared_lock<ut_shared_mutex_t> lock(m_mutex);
  for (const auto &[name, table] : m_entries) {
    if (table->exists()) {
      names.insert(name);
    }
  }
}

/** Install a recovered schema in an empty table entry. */
db_status_t db_tables::restore_schema(const schema_t &schema) {
  db_table *table = nullptr;
  if (const db_status_t err = find_or_create(schema.m_name, table); !err.ok()) {
    return (err);
  }

  if (table->schema() != nullptr) {
    return (DB_CORRUPTION);
  }

  schema_t *copy = ut_new<schema_t>(schema);
  if (copy == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  table->publish_schema(schema_ref_t(copy));

  return (DB_SUCCESS);
}

/** Reserve a batch before publishing its prepared schema version. */
db_status_t db_tables::reserve_write(memtable &mt, batch_seq_t seq,
                                     ingest_plan_t &plan) {
  db_table *table = nullptr;
  if (const db_status_t err = find_or_create(plan.m_table_name, table);
      !err.ok()) {
    return (err);
  }

  schema_ref_t schema;
  if (const db_status_t err = table->prepare_schema(plan, schema); !err.ok()) {
    return (err);
  }

  plan.m_batch.m_schema_version =
      static_cast<schema_version_t>(schema->m_columns.size());
  if (const db_status_t err = mt.reserve(seq, plan.m_batch); !err.ok()) {
    return (err);
  }

  table->publish_schema(std::move(schema));

  return (DB_SUCCESS);
}

/** Retain the latest schema of each initialized table. */
void db_tables::collect_schemas(std::vector<schema_ref_t> &schemas) const {
  schemas.clear();

  std::shared_lock<ut_shared_mutex_t> lock(m_mutex);

  for (const auto &[name, table] : m_entries) {
    schema_ref_t schema = table->schema();
    if (schema != nullptr) {
      schemas.push_back(std::move(schema));
    }
  }
}
