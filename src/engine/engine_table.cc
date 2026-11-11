/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine_table.cc
 *    Shared name lookup and exclusive, checked insertion.
 *
 *-------------------------------------------------------------------------
 */

#include "engine_table.h"

#include <mutex>
#include <utility>

#include "ut.h"
#include "ut_new.h"

/** Release this operation's reference. */
engine_table_ref_t::~engine_table_ref_t() { reset(); }

/** Transfer a reference without changing its count. */
engine_table_ref_t::engine_table_ref_t(engine_table_ref_t &&other) noexcept
    : m_registry(std::exchange(other.m_registry, nullptr)),
      m_table(std::exchange(other.m_table, nullptr)),
      m_pinned(std::exchange(other.m_pinned, false)) {}

/** Release the old reference before taking ownership of another. */
engine_table_ref_t &engine_table_ref_t::operator=(
    engine_table_ref_t &&other) noexcept {
  if (this != &other) {
    reset();
    m_registry = std::exchange(other.m_registry, nullptr);
    m_table = std::exchange(other.m_table, nullptr);
    m_pinned = std::exchange(other.m_pinned, false);
  }
  return (*this);
}

/** Drop the reference and reclaim an unused absent name if this was its last
user. No registry lock is needed once the entry has a time-series schema. */
void engine_table_ref_t::reset() {
  if (m_table != nullptr) {
    m_registry->release(m_table, m_pinned);
    m_registry = nullptr;
    m_table = nullptr;
    m_pinned = false;
  }
}

/** Find a shared entry, or insert after releasing the reader and rechecking.
Only entries without a time-series schema need a counted reference. */
db_status_t engine_tables_t::find_or_create(const std::string &name,
                                           engine_table_ref_t &out) {
  /* Keep the old reference alive if name aliases its entry. */
  engine_table_ref_t previous(std::move(out));
  {
    std::shared_lock<std::shared_mutex> lock(m_mutex);
    const auto it = m_entries.find(name);
    if (it != m_entries.end()) {
      out.m_registry = this;
      out.m_table = it->second.get();
      out.m_pinned = out->m_schema.load() == nullptr;
      if (out.m_pinned) {
        out->m_references.fetch_add(1, std::memory_order_relaxed);
      }
      return (DB_SUCCESS);
    }
  }

  std::unique_lock<std::shared_mutex> lock(m_mutex);
  auto it = m_entries.find(name);
  if (it == m_entries.end()) {
    std::unique_ptr<engine_table_t> table(ut_new<engine_table_t>());
    if (table == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }
    table->m_name = name;
    it = m_entries.emplace(name, std::move(table)).first;
  }
  out.m_registry = this;
  out.m_table = it->second.get();
  out.m_pinned = out->m_schema.load() == nullptr;
  if (out.m_pinned) {
    out->m_references.fetch_add(1, std::memory_order_relaxed);
  }
  return (DB_SUCCESS);
}

/** Retire temporary entries only after all users and DDL reservations leave. */
void engine_tables_t::release(engine_table_t *table, bool pinned) {
  if (!pinned) {
    return;
  }
  if (table->m_schema.load() != nullptr) {
    const size_t before =
        table->m_references.fetch_sub(1, std::memory_order_relaxed);
    ut_a(before > 0);
    return;
  }

  std::unique_lock<std::shared_mutex> lock(m_mutex);
  const size_t before =
      table->m_references.fetch_sub(1, std::memory_order_relaxed);
  ut_a(before > 0);
  if (before == 1 && table->m_schema.load() == nullptr &&
      !table->m_is_ordinary && table->m_active_ddl == 0) {
    const auto it = m_entries.find(table->m_name);
    ut_a(it != m_entries.end() && it->second.get() == table);
    m_entries.erase(it);
  }
}

/** Read the number of names without excluding other readers. */
size_t engine_tables_t::size() const {
  std::shared_lock<std::shared_mutex> lock(m_mutex);
  return (m_entries.size());
}
