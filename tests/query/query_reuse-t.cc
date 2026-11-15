/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query_reuse-t.cc
 *    Tests for plan reuse: plans kept within one snapshot, fallback when a
 *    plan cannot be reused, and ownership of C bindings.
 *
 * The test links the white-box adapter access through
 * pulseten_query_test_support.
 *
 * Covers: src/query/query_duckdb.cc, src/query/query_memtable.cc
 *
 *-------------------------------------------------------------------------
 */
#include "query.h"

#include "pt_duckdb.h"
#include "store.h"
#include "test_fixture.h"
#include "test_memtable.h"
#include "test_point.h"
#include "test_query_access.h"
#include "test_util.h"

/** Original C bind objects destroyed by the ownership test. */
static size_t deleted_bindings;

/** Observe final ownership release without allocating a test payload. */
static void record_binding_destruction(void *) {
  ++deleted_bindings;
}

/** Keep this ordinary C binding uncacheable to exercise DuckDB's fallback. */
static void bind_uncached(duckdb_bind_info info) {
  duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_bind_add_result_column(info, "value", type);
  duckdb_destroy_logical_type(&type);
}

/** The fallback test returns no rows and needs no scan state. */
static void init_uncached(duckdb_init_info) {}

/** Return no rows when the test exercises an uncached table binding. */
static void scan_uncached(duckdb_function_info, duckdb_data_chunk) {}

/** Copies own independent metadata and destroy their shared C data once. */
static void test_binding_copies() {
  duckdb::CTableFunctionInfo info;
  std::shared_ptr<duckdb::CTableBindData> owner =
      std::make_shared<duckdb::CTableBindData>(info);
  owner->bind_data = &deleted_bindings;
  owner->delete_callback = record_binding_destruction;
  owner->stats = duckdb::make_uniq<duckdb::NodeStatistics>(1, 2);

  duckdb::unique_ptr<duckdb::FunctionData> copy;

  {
    duckdb::unique_ptr<duckdb::FunctionData> binding =
        query_test_create_binding(owner, *owner);

    duckdb::CTableBindData &data = binding->Cast<duckdb::CTableBindData>();
    owner.reset();
    data.column_ids.push_back(3);
    data.stats->estimated_cardinality = 7;
    copy = binding->Copy();
    CHECK(copy->Equals(*binding));

    duckdb::CTableBindData &copied = copy->Cast<duckdb::CTableBindData>();
    CHECK(copied.stats->estimated_cardinality == 7);
    CHECK(copied.stats.get() != data.stats.get());

    copied.column_ids.push_back(4);
    CHECK(!copy->Equals(*binding));
    CHECK(deleted_bindings == 0);
  }

  CHECK(deleted_bindings == 0);

  copy.reset();
  CHECK(deleted_bindings == 1);
}

/** Nested or mismatched scopes cannot authorize another registered query. */
static void test_scope_and_parameters() {
  test_dir_t store_dir;
  store *st = nullptr;
  CHECK(store::open(store_dir.m_path, 24, &st, 256ULL << 20).ok());

  pt_duckdb_session session(st->connection());
  CHECK(query_init(st->connection()).ok());

  duckdb_table_function function = duckdb_create_table_function();
  duckdb_table_function_set_name(function, "uncached_scan");
  duckdb_table_function_set_bind(function, bind_uncached);
  duckdb_table_function_set_init(function, init_uncached);
  duckdb_table_function_set_function(function, scan_uncached);
  CHECK(duckdb_register_table_function(st->connection(), function) ==
        DuckDBSuccess);

  duckdb_destroy_table_function(&function);

  CHECK(session.begin().ok());

  duckdb_connection connection = st->connection();
  query_snapshot_t snapshot, other_snapshot;

  query_ranges_t ranges;
  const uint64_t token = query_registry::instance().register_query(
      query_connection_key(connection), snapshot, ranges);
  CHECK(!query_test_scope::matches(connection));

  {
    query_test_scope outer(connection, token, snapshot);
    CHECK(query_test_scope::matches(connection));

    {
      query_test_scope nested(connection, token, other_snapshot);
      CHECK(!query_test_scope::matches(connection));
    }

    CHECK(query_test_scope::matches(connection));

    duckdb_connection other = nullptr;
    CHECK(duckdb_connect(st->database(), &other) == DuckDBSuccess);
    CHECK(!query_test_scope::matches(other));

    duckdb_disconnect(&other);
  }

  CHECK(!query_test_scope::matches(connection));

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
  CHECK(!query_test_scope::matches(connection));
  CHECK(query_prepare(connection, "invalid syntax", token, snapshot,
                      &prepared) == DuckDBError);

  duckdb_destroy_prepare(&prepared);
  CHECK(!query_test_scope::matches(connection));

  CHECK(session.rollback().ok());

  query_registry::instance().unregister_query(token);

  store::destroy(st);
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
static void test_plan_reuse_and_fallback(query_source_t source,
                                         query_prepare_mode_t mode) {
  const bool hot = source != query_source_t::DISK;
  const bool stored = source != query_source_t::MEMTABLE;
  const bool transaction = mode != query_prepare_mode_t::AUTOCOMMIT;
  const bool scoped = mode != query_prepare_mode_t::UNSCOPED;
  const bool matching_token = mode != query_prepare_mode_t::MISMATCHED_TOKEN;

  test_dir_t store_dir;
  store *st = nullptr;
  CHECK(store::open(store_dir.m_path, 24, &st, 256ULL << 20).ok());

  duckdb_connection connection = st->connection();
  pt_duckdb_session session(connection);
  CHECK(query_init(connection).ok());
  CHECK(pt_duckdb_exec(connection,
                       "CREATE TABLE cold AS SELECT 41::BIGINT AS value")
            .ok());

  if (transaction) {
    CHECK(session.begin().ok());
    CHECK(pt_duckdb_exec(connection, "SELECT count(*) FROM cold").ok());
  }

  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  ingest_field_t field = field_int("value", 42);
  ingest_point_t input = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
  ingest_plan_t plan;
  CHECK(plan_for(nullptr, "hot", &input, 1, plan).ok());
  CHECK(mt->reserve(1, plan).ok());

  mt->publish(1);

  memtable::snapshot_t memory;
  snapshot_all(*mt, memory);

  query_snapshot_t snapshot;
  query_table_t &table = snapshot.m_tables["hot"];
  table.m_schema = *snapshot_schema(*mt, memory, "hot");
  table.m_memtable_snapshot = &memory;

  query_ranges_t ranges;
  uint64_t token = query_registry::instance().register_query(
      query_connection_key(connection), snapshot, ranges);

  std::string sql = hot ? "SELECT value FROM hot" : "SELECT * FROM cold";

  if (hot && stored) {
    sql += " UNION ALL SELECT * FROM cold";
  }

  /* Observe prepare and pending without leaving a process-wide callback. */
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
  query_registry::instance().unregister_query(token);
  mt->release_snapshot(memory);
  test_memtable::destroy(mt);

  if (transaction) {
    CHECK(session.rollback().ok());
  }

  store::destroy(st);
}

/** Exercise cache eligibility, fallback, and shared binding lifetime. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"binding_copies", test_binding_copies},
      {"scope_and_parameters", test_scope_and_parameters},
      {"plan_reuse/memtable_scoped",
       [] {
         test_plan_reuse_and_fallback(query_source_t::MEMTABLE,
                                      query_prepare_mode_t::SCOPED);
       }},
      {"plan_reuse/mixed_scoped",
       [] {
         test_plan_reuse_and_fallback(query_source_t::MIXED,
                                      query_prepare_mode_t::SCOPED);
       }},
      {"plan_reuse/disk_scoped",
       [] {
         test_plan_reuse_and_fallback(query_source_t::DISK,
                                      query_prepare_mode_t::SCOPED);
       }},
      {"plan_reuse/mixed_unscoped",
       [] {
         test_plan_reuse_and_fallback(query_source_t::MIXED,
                                      query_prepare_mode_t::UNSCOPED);
       }},
      {"plan_reuse/mixed_autocommit",
       [] {
         test_plan_reuse_and_fallback(query_source_t::MIXED,
                                      query_prepare_mode_t::AUTOCOMMIT);
       }},
      {"plan_reuse/mixed_mismatched_token",
       [] {
         test_plan_reuse_and_fallback(query_source_t::MIXED,
                                      query_prepare_mode_t::MISMATCHED_TOKEN);
       }},
  };

  return (test_main("query_reuse-t", argc, argv, cases));
}
