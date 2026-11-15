/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_store.h
 *    Storage helpers: scalar queries and expiry through the store
 *    connection, and writes planned against the stored schemas.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_store_h
#define test_store_h

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "ingest.h"
#include "pt_duckdb.h"
#include "store.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"

/** Read a scalar integer; the test caller excludes storage mutations. */
static inline db_status_t test_store_query_int(store &st,
                                               const std::string &sql,
                                               int64_t &value) {
  pt_duckdb_rows_t rows;
  const db_status_t err = pt_duckdb_exec(st.connection(), sql, rows);
  if (!err.ok()) {
    return (err);
  }

  if (rows.size() != 1 || rows[0].size() != 1 || rows[0][0].m_is_null) {
    return (DB_ERROR);
  }

  value = rows[0][0].m_integer;

  return (DB_SUCCESS);
}

/** Select and expire partitions while the test owns the store. */
static inline db_status_t test_store_expire(store &st, int64_t end_day,
                                            uint64_t &removed) {
  std::vector<store_partition_key_t> expired_partitions;
  st.collect_expired_partitions((end_day * 24 + 1) * UT_HOUR_US, 1,
                                expired_partitions);

  return (st.expire_partitions(expired_partitions, removed));
}

/** Seed durable table declarations without invoking a future management API.
Close and reopen the store before using its in-memory catalog. */
static inline void test_store_declare(
    store &st, const std::vector<std::pair<std::string, uint32_t>> &tables) {
  REQUIRE(pt_duckdb_exec(st.connection(), "BEGIN TRANSACTION").ok());

  for (const auto &[name, hours] : tables) {
    REQUIRE(pt_duckdb_exec(st.connection(),
                           "INSERT INTO _pt_tables SELECT next_table_id, " +
                               pt_duckdb_literal(name) + ", false, " +
                               std::to_string(hours) + ", NULL FROM _pt_state")
                .ok());
    REQUIRE(
        pt_duckdb_exec(st.connection(),
                       "UPDATE _pt_state SET next_table_id = next_table_id + 1")
            .ok());
  }

  REQUIRE(pt_duckdb_exec(st.connection(), "COMMIT").ok());
}

/** Current directories and active writes owned by a storage test. */
struct test_catalog_t {
  std::map<std::string, schema_ref_t> m_schemas;
  std::vector<schema_ref_t> m_write_schemas;
  std::vector<ingest_plan_t *> m_plans;

  /** Destroy the catalog retained by this test guard. */
  ~test_catalog_t() {
    for (ingest_plan_t *plan : m_plans) {
      delete plan;
    }
  }
};

/** Start planning from only the latest stored column directories. */
static inline void catalog_load(test_catalog_t &cat,
                                const store_catalog_t &state) {
  for (const auto &entry : state.m_tables) {
    cat.m_schemas[entry.first] =
        std::make_shared<schema_t>(entry.second.m_schema);
  }

  return;
}

/** Plan a write and keep its schema valid throughout the append. */
static inline store_write_t prepare_catalog_write(test_catalog_t &cat,
                                                  batch_seq_t seq,
                                                  const char *table_name,
                                                  const ingest_point_t *points,
                                                  uint32_t n) {
  schema_ref_t &schema = cat.m_schemas[table_name];
  ingest_plan_t *plan = new ingest_plan_t;
  cat.m_plans.push_back(plan);
  CHECK(plan_for(schema.get(), table_name, points, n, *plan).ok());

  if (!plan->m_new_columns.empty()) {
    schema = std::make_shared<schema_t>(schema_after(schema.get(), *plan));
  }

  cat.m_write_schemas.push_back(schema);

  store_write_t w;
  w.m_seq = seq;
  w.m_batch = &plan->m_batch;
  w.m_schema = schema.get();

  return (w);
}

#endif /* test_store_h */
