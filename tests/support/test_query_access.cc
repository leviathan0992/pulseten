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

/** Return prepared statement data retained by the connection. */
const duckdb::PreparedStatementData &query_test_prepared_data(
    duckdb_prepared_statement handle) {
  duckdb::PreparedStatementWrapper &wrapper =
      *reinterpret_cast<duckdb::PreparedStatementWrapper *>(handle);
  duckdb::shared_ptr<duckdb::ClientContext> context =
      wrapper.statement->TryGetContext();

  return (*duckdb::ClientData::Get(*context).prepared_statements.at(
      duckdb::Identifier(wrapper.statement->GetName())));
}
