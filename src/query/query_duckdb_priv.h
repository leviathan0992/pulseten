/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query_duckdb_priv.h
 *    Private DuckDB adapter types, shared only with its white-box tests.
 *
 *-------------------------------------------------------------------------
 */
#ifndef query_duckdb_priv_h
#define query_duckdb_priv_h

#include <atomic>
#include <memory>
#include <utility>

#include "duckdb/main/capi/capi_internal_table.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_context.hpp"

#include "query_priv.h"
#include "ut_noncopyable.h"

/** The id counter belongs to the database, not the process.
@return the database-instance address and database-local connection id */
inline query_connection_key_t query_context_key(
    duckdb::ClientContext &context) {
  return (std::make_pair(reinterpret_cast<uintptr_t>(context.db.get()),
                         static_cast<uint64_t>(context.GetConnectionId())));
}

/** Authorize plan retention only while preparing this registered snapshot.
The connection remains in the same explicit transaction until its cursor closes.
Nested preparations restore their caller's scope on every return. */
class query_prepare_scope final : private ut_noncopyable {
 public:
  /** Enter a preparation without extending the lifetime of its snapshot. */
  query_prepare_scope(duckdb_connection connection, uint64_t token,
                      const query_snapshot_t &snapshot)
      : m_connection(query_connection_key(connection)),
        m_token(token),
        m_snapshot(&snapshot),
        m_previous(m_current) {
    duckdb::ClientContext &context =
        *reinterpret_cast<duckdb::Connection *>(connection)->context;
    if (context.transaction.HasActiveTransaction() &&
        !context.transaction.IsAutoCommit()) {
      m_transaction = &context.transaction.ActiveTransaction();
    }

    m_current = this;
  }

  /** Restore an enclosing preparation, if any. */
  ~query_prepare_scope() { m_current = m_previous; }

  /** Check connection, transaction and registered snapshot identity. */
  static bool matches(duckdb::ClientContext &context) {
    const query_prepare_scope *scope = m_current;

    /* Only the connection preparing this registered query may retain a plan. */
    if (scope == nullptr || scope->m_connection != query_context_key(context)) {
      return (false);
    }

    /* The cursor must still own the same valid explicit transaction. */
    if (scope->m_transaction == nullptr ||
        !context.transaction.HasActiveTransaction() ||
        context.transaction.IsAutoCommit()) {
      return (false);
    }

    duckdb::MetaTransaction &transaction =
        context.transaction.ActiveTransaction();
    if (scope->m_transaction != &transaction ||
        duckdb::ValidChecker::IsInvalidated(transaction)) {
      return (false);
    }

    /* Registration must still identify the exact snapshot being prepared. */
    uint64_t token = 0;
    const query_ranges_t *ranges = nullptr;
    const query_snapshot_t *snapshot =
        query_registry::instance().find_by_connection(scope->m_connection,
                                                      token, ranges);

    return (token != 0 && token == scope->m_token &&
            snapshot == scope->m_snapshot && ranges != nullptr);
  }

 private:
  /** Innermost preparation on this thread; never retained by a plan. */
  static thread_local const query_prepare_scope *m_current;

  /** Database-local connection identity. */
  const query_connection_key_t m_connection;

  /** Registration of the query being prepared. */
  const uint64_t m_token;

  /** Snapshot retained by the owning database cursor. */
  const query_snapshot_t *m_snapshot;

  /** Explicit transaction already established before preparing. */
  const duckdb::MetaTransaction *m_transaction{nullptr};

  /** Enclosing preparation restored on exit. */
  const query_prepare_scope *m_previous;
};

/** A copyable plan binding sharing only immutable C bind data.
DuckDB requires this inheritance and these virtual methods. Each scan still
creates independent execution state through the original C init callback. */
class query_memtable_binding final : public duckdb::CTableBindData,
                                     private ut_noncopyable {
 public:
  /** Share the original owner and copy the current plan's metadata. */
  query_memtable_binding(std::shared_ptr<duckdb::CTableBindData> owner,
                         const duckdb::CTableBindData &source)
      : CTableBindData(source.info), m_owner(std::move(owner)) {
    bind_data = m_owner->bind_data;

    /* Only m_owner invokes the C deletion callback. */
    delete_callback = nullptr;
    column_ids = source.column_ids;
    if (source.stats) {
      stats = duckdb::make_uniq<duckdb::NodeStatistics>(*source.stats);
    }
  }

  /** Eligibility was established at bind time, not at execution time. */
  bool SupportStatementCache() const override { return (true); }

  /** Copy plan metadata without sharing a scan's mutable progress. */
  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
    return (duckdb::make_uniq<query_memtable_binding>(m_owner, *this));
  }

  /** Only copies of the same immutable binding and projection are equal. */
  bool Equals(const duckdb::FunctionData &other) const override {
    const query_memtable_binding *binding =
        dynamic_cast<const query_memtable_binding *>(&other);
    return (binding != nullptr && m_owner == binding->m_owner &&
            column_ids == binding->column_ids);
  }

 private:
  friend void query_enable_memtable_reuse(duckdb_table_function function);
#ifdef PT_TESTING
  friend class query_bind_counter;
#endif /* PT_TESTING */

  /** Wrap bindings only inside the registered preparation scope. */
  static duckdb::unique_ptr<duckdb::FunctionData> bind(
      duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
      duckdb::vector<duckdb::LogicalType> &types,
      duckdb::vector<duckdb::Identifier> &names);

  /** Fixed C API trampoline, checked once across database registrations. */
  static std::atomic<duckdb::table_function_bind_t> m_original_bind;

  /** Last wrapper releases the original C bind object and its callback. */
  const std::shared_ptr<duckdb::CTableBindData> m_owner;
};

#endif /* query_duckdb_priv_h */
