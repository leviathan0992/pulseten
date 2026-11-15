/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_query_access.h
 *    Private plan inspection and scoped binding counts.
 *
 *-------------------------------------------------------------------------
 */
#ifndef test_query_access_h
#define test_query_access_h

#ifndef PT_TESTING
#error "test_query_access.h requires the PulseTen test library"
#endif

#include <cstddef>
#include <memory>

#include "duckdb/main/capi/capi_internal_table.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

#include "query.h"

/** Forward declaration. */
class query_prepare_scope;

/** Create a plan binding sharing the supplied C bind data. */
duckdb::unique_ptr<duckdb::FunctionData> query_test_create_binding(
    std::shared_ptr<duckdb::CTableBindData> owner,
    const duckdb::CTableBindData &source);

/** Scoped access to the adapter's preparation state. */
class query_test_scope final : private ut_noncopyable {
 public:
  /** Enter a preparation for this snapshot; restore the outer one on exit. */
  query_test_scope(duckdb_connection connection, uint64_t token,
                   const query_snapshot_t &snapshot);
  ~query_test_scope();

  /** Check connection, transaction and snapshot identity in this scope. */
  static bool matches(duckdb_connection connection);

 private:
  /** Adapter scope owned by this guard. */
  query_prepare_scope *m_scope{nullptr};
};

/** Count bindings in serial adapter tests; restore the callback on scope exit.
The caller excludes all other database opening and query preparation. */
class query_bind_counter final : private ut_noncopyable {
 public:
  query_bind_counter();
  ~query_bind_counter();

  /** Return the number of bind calls observed by this guard. */
  size_t calls() const { return (m_calls); }

 private:
  static duckdb::unique_ptr<duckdb::FunctionData> count_bind(
      duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
      duckdb::vector<duckdb::LogicalType> &types,
      duckdb::vector<duckdb::Identifier> &names);

  /** Sole active counter; serial tests only. */
  static query_bind_counter *m_current;

  /** Callback restored when this counter leaves scope. */
  duckdb::table_function_bind_t m_original{nullptr};

  /** Bind calls observed during this scope. */
  size_t m_calls{0};
};

/** Return prepared metadata; keep the handle and connection open while using
it. */
const duckdb::PreparedStatementData &query_test_prepared_data(
    duckdb_prepared_statement handle);

#endif /* test_query_access_h */
