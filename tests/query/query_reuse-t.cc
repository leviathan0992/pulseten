/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query_reuse-t.cc
 *    Snapshot-local plan retention and C binding ownership.
 *
 * Tests link the normal test-library adapter through explicit test support.
 *-------------------------------------------------------------------------
 */
#include "query.h"

#include "duckdb_database.h"
#include "test_query_access.h"
#include "test_util.h"

/** Original C bind objects destroyed by the ownership test. */
static size_t deleted_bindings;

/** Observe final ownership release without allocating a test payload. */
static void delete_binding(void *) {
  ++deleted_bindings;
}

/** Leave this ordinary C binding non-cacheable to exercise DuckDB's fallback.
 */
static void bind_uncached(duckdb_bind_info info) {
  duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_bind_add_result_column(info, "value", type);
  duckdb_destroy_logical_type(&type);
}

/** The fallback test returns no rows and needs no scan state. */
static void init_uncached(duckdb_init_info) {}
static void scan_uncached(duckdb_function_info, duckdb_data_chunk) {}

/** Copies own independent metadata and destroy their shared C data once. */
static void test_binding_copies() {
  duckdb::CTableFunctionInfo info;
  std::shared_ptr<duckdb::CTableBindData> owner =
      std::make_shared<duckdb::CTableBindData>(info);
  owner->bind_data = &deleted_bindings;
  owner->delete_callback = delete_binding;
  owner->stats = duckdb::make_uniq<duckdb::NodeStatistics>(1, 2);
  duckdb::unique_ptr<duckdb::FunctionData> copy;
  {
    query_memtable_binding binding(owner, *owner);
    owner.reset();
    binding.column_ids.push_back(3);
    binding.stats->estimated_cardinality = 7;
    copy = binding.Copy();
    CHECK(copy->Equals(binding));
    query_memtable_binding &copied = copy->Cast<query_memtable_binding>();
    CHECK(copied.stats->estimated_cardinality == 7);
    CHECK(copied.stats.get() != binding.stats.get());
    copied.column_ids.push_back(4);
    CHECK(!copy->Equals(binding));
    CHECK(deleted_bindings == 0);
  }
  CHECK(deleted_bindings == 0);
  copy.reset();
  CHECK(deleted_bindings == 1);
}

/** Nested or mismatched scopes cannot authorize another registered query. */
static void test_scope_and_parameters() {
  duckdb_backend *backend = nullptr;
  CHECK(duckdb_backend::open(nullptr, &backend, 256ULL << 20).ok());
  CHECK(query_prepare_database(backend->connection()).ok());
  duckdb_table_function function = duckdb_create_table_function();
  duckdb_table_function_set_name(function, "uncached_scan");
  duckdb_table_function_set_bind(function, bind_uncached);
  duckdb_table_function_set_init(function, init_uncached);
  duckdb_table_function_set_function(function, scan_uncached);
  CHECK(duckdb_register_table_function(backend->connection(), function) ==
        DuckDBSuccess);
  duckdb_destroy_table_function(&function);

  CHECK(backend->session().begin().ok());
  duckdb_connection connection = backend->connection();
  duckdb::ClientContext &context =
      *reinterpret_cast<duckdb::Connection *>(connection)->context;
  query_snapshot_t snapshot, other_snapshot;

  query_ranges_t ranges;
  const uint64_t token = query_registry::instance().register_query(
      query_connection_key(connection), snapshot, ranges);
  CHECK(!query_prepare_scope::matches(context));
  {
    query_prepare_scope outer(connection, token, snapshot);
    CHECK(query_prepare_scope::matches(context));
    {
      query_prepare_scope nested(connection, token, other_snapshot);
      CHECK(!query_prepare_scope::matches(context));
    }
    CHECK(query_prepare_scope::matches(context));
    duckdb_connection other = nullptr;
    CHECK(duckdb_connect(backend->database(), &other) == DuckDBSuccess);
    CHECK(!query_prepare_scope::matches(
        *reinterpret_cast<duckdb::Connection *>(other)->context));
    duckdb_disconnect(&other);
  }
  CHECK(!query_prepare_scope::matches(context));

  duckdb_prepared_statement prepared = nullptr;
  /* A binder-discarded child must not be restored by our optimizer hook. */
  CHECK(query_prepare(connection, "SELECT * FROM uncached_scan()", token,
                      snapshot, &prepared) == DuckDBSuccess);
  CHECK(!query_test_prepared_data(prepared).physical_plan);
  CHECK(query_test_prepared_data(prepared).properties.always_require_rebind);

  duckdb_result result{};
  CHECK(duckdb_execute_prepared(prepared, &result) == DuckDBSuccess);

  duckdb_destroy_result(&result);
  duckdb_destroy_prepare(&prepared);

  CHECK(query_prepare(connection, "SELECT ?", token, snapshot, &prepared) ==
        DuckDBSuccess);
  CHECK(!query_test_prepared_data(prepared).physical_plan);
  CHECK(!query_test_prepared_data(prepared).properties.bound_all_parameters);
  duckdb_destroy_prepare(&prepared);
  CHECK(!query_prepare_scope::matches(context));
  CHECK(query_prepare(connection, "invalid syntax", token, snapshot,
                      &prepared) == DuckDBError);
  duckdb_destroy_prepare(&prepared);
  CHECK(!query_prepare_scope::matches(context));

  CHECK(backend->session().rollback().ok());
  query_registry::instance().unregister(token);

  duckdb_backend::destroy(backend);
}

/** Data sources visible to this query. */
enum class query_source_t { MEMTABLE, DISK, MIXED };

/** Preparation scope or the condition that requires fallback. */
enum class query_prepare_mode_t {
  SCOPED,
  UNSCOPED,
  AUTOCOMMIT,
  MISMATCHED_TOKEN
};

/** Prepare and execute once; fallback must still return the same rows. */
static void test_plan(query_source_t source, query_prepare_mode_t mode) {
  const bool hot = source != query_source_t::DISK;
  const bool stored = source != query_source_t::MEMTABLE;
  const bool transaction = mode != query_prepare_mode_t::AUTOCOMMIT;
  const bool scoped = mode != query_prepare_mode_t::UNSCOPED;
  const bool matching_token = mode != query_prepare_mode_t::MISMATCHED_TOKEN;

  duckdb_backend *backend = nullptr;
  CHECK(duckdb_backend::open(nullptr, &backend, 256ULL << 20).ok());
  duckdb_connection connection = backend->connection();
  CHECK(query_prepare_database(connection).ok());
  CHECK(
      duckdb_exec(connection, "CREATE TABLE cold AS SELECT 41::BIGINT AS value")
          .ok());

  if (transaction) {
    CHECK(backend->session().begin().ok());
    CHECK(duckdb_exec(connection, "SELECT count(*) FROM cold").ok());
  }

  batch_t batch;
  batch.m_n_rows = 1;
  batch.m_cols.resize(1);
  batch.m_cols[0].reset(0, col_type_t::BIGINT, 1);
  batch.m_cols[0].m_ints[0] = 42;
  batch.m_cols[0].m_nulls[0] = 0;

  query_snapshot_t snapshot;
  query_table_t &table = snapshot.m_tables["hot"];
  col_def_t column;
  column.m_name = "value";
  column.m_type = col_type_t::BIGINT;
  column.m_kind = col_kind_t::FIELD;
  table.m_schema.m_cols.push_back(column);
  table.m_batches.push_back(&batch);

  query_ranges_t ranges;
  uint64_t token = query_registry::instance().register_query(
      query_connection_key(connection), snapshot, ranges);

  std::string sql = hot ? "SELECT * FROM hot" : "SELECT * FROM cold";
  if (hot && stored) {
    sql += " UNION ALL SELECT * FROM cold";
  }

  /* Observe prepare and pending without leaving a process-wide callback behind.
   */
  query_bind_counter bindings;
  duckdb_prepared_statement prepared = nullptr;
  const duckdb_state state =
      scoped ? query_prepare(connection, sql.c_str(),
                             matching_token ? token : token + 1, snapshot,
                             &prepared)
             : duckdb_prepare(connection, sql.c_str(), &prepared);
  CHECK(state == DuckDBSuccess);

  const bool reusable = scoped && transaction && matching_token;
  CHECK(bool(query_test_prepared_data(prepared).physical_plan) == reusable);

  duckdb_pending_result pending = nullptr;
  CHECK(duckdb_pending_prepared_streaming(prepared, &pending) == DuckDBSuccess);
  CHECK(bindings.calls() == (hot ? (reusable ? 1 : 2) : 0));

  duckdb_result result{};
  CHECK(duckdb_execute_pending(pending, &result) == DuckDBSuccess);
  duckdb_destroy_pending(&pending);

  size_t count = 0;
  int64_t sum = 0;
  for (;;) {
    duckdb_data_chunk chunk = duckdb_fetch_chunk(result);
    if (chunk == nullptr) {
      break;
    }

    const idx_t size = duckdb_data_chunk_get_size(chunk);
    count += size;
    const int64_t *values = static_cast<int64_t *>(
        duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 0)));
    for (idx_t row = 0; row < size; ++row) {
      sum += values[row];
    }

    duckdb_destroy_data_chunk(&chunk);
  }

  CHECK(count == (hot && stored ? 2 : 1));
  CHECK(sum == (hot ? (stored ? 83 : 42) : 41));
  CHECK(duckdb_result_error(&result) == nullptr);

  duckdb_destroy_result(&result);
  duckdb_destroy_prepare(&prepared);
  query_registry::instance().unregister(token);
  if (transaction) {
    CHECK(backend->session().rollback().ok());
  }

  duckdb_backend::destroy(backend);
}

/** Exercise cache eligibility, fallback, and shared binding lifetime. */
int main() {
  test_binding_copies();
  test_scope_and_parameters();
  test_plan(query_source_t::MEMTABLE, query_prepare_mode_t::SCOPED);
  test_plan(query_source_t::MIXED, query_prepare_mode_t::SCOPED);
  test_plan(query_source_t::DISK, query_prepare_mode_t::SCOPED);
  test_plan(query_source_t::MIXED, query_prepare_mode_t::UNSCOPED);
  test_plan(query_source_t::MIXED, query_prepare_mode_t::AUTOCOMMIT);
  test_plan(query_source_t::MIXED, query_prepare_mode_t::MISMATCHED_TOKEN);
  return (test_result("query_reuse-t"));
}
