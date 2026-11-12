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

#include "duckdb/main/prepared_statement_data.hpp"

#include "query_duckdb_priv.h"

/** Count bindings in serial adapter tests; restore the callback on scope exit.
The caller excludes all other database opening and query preparation. */
class query_bind_counter final : private ut_noncopyable {
 public:
  query_bind_counter();
  ~query_bind_counter();

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

/** Borrow prepared metadata while the test owns its handle and connection. */
const duckdb::PreparedStatementData &query_test_prepared_data(
    duckdb_prepared_statement handle);

#endif /* test_query_access_h */
