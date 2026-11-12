/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_query_access.cc
 *    Test-only access to the pinned DuckDB adapter.
 *
 *-------------------------------------------------------------------------
 */
#include "test_query_access.h"

#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/prepared_statement.hpp"

query_bind_counter *query_bind_counter::m_current = nullptr;

query_bind_counter::query_bind_counter() {
  ut_a(m_current == nullptr);
  m_current = this;
  m_original = query_memtable_binding::m_original_bind.exchange(count_bind);
  ut_a(m_original != nullptr);
}

query_bind_counter::~query_bind_counter() {
  query_memtable_binding::m_original_bind.store(m_original);
  m_current = nullptr;
}

duckdb::unique_ptr<duckdb::FunctionData> query_bind_counter::count_bind(
    duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &types,
    duckdb::vector<duckdb::Identifier> &names) {
  ++m_current->m_calls;

  return (m_current->m_original(context, input, types, names));
}

const duckdb::PreparedStatementData &query_test_prepared_data(
    duckdb_prepared_statement handle) {
  duckdb::PreparedStatementWrapper &wrapper =
      *reinterpret_cast<duckdb::PreparedStatementWrapper *>(handle);
  duckdb::shared_ptr<duckdb::ClientContext> context =
      wrapper.statement->TryGetContext();

  return (*duckdb::ClientData::Get(*context).prepared_statements.at(
      duckdb::Identifier(wrapper.statement->GetName())));
}
