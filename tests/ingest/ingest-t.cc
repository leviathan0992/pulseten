/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ingest-t.cc
 *    Tests for write planning: schema inference, conflicts, limits and the
 *    resulting column batch.
 *
 *-------------------------------------------------------------------------
 */

#include "ingest.h"

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "batch.h"
#include "schema.h"
#include "test_point.h"
#include "test_util.h"
#include "ut_err.h"

/** Plan a single point with one field and return the result code.
@param[in]  schema  schema the write follows; may be NULL when creating a
                    table
@return the status from ingest_plan() */
static db_status_t plan_one_field(const schema_t *schema, ingest_field_t f) {
  ingest_point_t p = point(T0, nullptr, 0, &f, 1);
  ingest_plan_t plan;

  return (plan_for(schema, "m", &p, 1, plan));
}

/** A first write plans the table with its tags and fields in order. */
static void test_new_table() {
  ingest_tag_t tags0[] = {tag("host", "a")};
  ingest_field_t fields0[] = {field_double("cpu", 0.5), field_bool("up", true)};
  ingest_tag_t tags1[] = {tag("host", "b"), tag("region", "us")};
  ingest_field_t fields1[] = {field_double("cpu", 0.75),
                              field_str("msg", "hi")};
  ingest_point_t points[] = {point(T0, tags0, 1, fields0, 2),
                             point(T0 + 1, tags1, 2, fields1, 2)};

  ingest_plan_t plan;
  const db_status_t status = plan_for(nullptr, "metrics", points, 2, plan);
  CHECK(status.ok());
  CHECK(status.message().empty());
  CHECK(plan.m_table_name == "metrics");
  CHECK(plan.m_creates_table);
  CHECK(plan.m_base_version == 0);

  /* Columns in order of first appearance. */
  const char *names[] = {"time", "host", "cpu", "up", "region", "msg"};
  const col_kind_t kinds[] = {col_kind_t::TIME,  col_kind_t::TAG,
                              col_kind_t::FIELD, col_kind_t::FIELD,
                              col_kind_t::TAG,   col_kind_t::FIELD};
  const col_type_t types[] = {col_type_t::TIMESTAMP, col_type_t::VARCHAR,
                              col_type_t::DOUBLE,    col_type_t::BOOLEAN,
                              col_type_t::VARCHAR,   col_type_t::VARCHAR};
  CHECK(plan.m_new_cols.size() == 6);
  for (size_t i = 0; i < plan.m_new_cols.size() && i < 6; ++i) {
    CHECK(plan.m_new_cols[i].m_id == i);
    CHECK(plan.m_new_cols[i].m_name == names[i]);
    CHECK(plan.m_new_cols[i].m_kind == kinds[i]);
    CHECK(plan.m_new_cols[i].m_type == types[i]);
  }

  /* The batch: one column per schema column, NULL where not given. */
  const batch_t &b = plan.m_batch;
  CHECK(b.m_table_name == "metrics");
  CHECK(b.m_schema_version == 1);
  CHECK(b.m_n_rows == 2);
  CHECK(b.m_cols.size() == 6);
  if (b.m_cols.size() != 6) {
    return;
  }

  CHECK(b.m_cols[0].m_ints[0] == T0 && b.m_cols[0].m_ints[1] == T0 + 1);
  CHECK(b.m_cols[0].m_nulls[0] == 0 && b.m_cols[0].m_nulls[1] == 0);

  CHECK(str_at(b.m_cols[1], 0) == "a" && str_at(b.m_cols[1], 1) == "b");

  CHECK(b.m_cols[2].m_doubles[0] == 0.5 && b.m_cols[2].m_doubles[1] == 0.75);

  CHECK(b.m_cols[3].m_nulls[0] == 0 && b.m_cols[3].m_bools[0] == 1);
  CHECK(b.m_cols[3].m_nulls[1] == 1);

  CHECK(b.m_cols[4].m_nulls[0] == 1 && str_at(b.m_cols[4], 0).empty());
  CHECK(b.m_cols[4].m_nulls[1] == 0 && str_at(b.m_cols[4], 1) == "us");

  CHECK(b.m_cols[5].m_nulls[0] == 1);
  CHECK(str_at(b.m_cols[5], 1) == "hi");
}

/** Writes to an existing table add only the new columns. */
static void test_existing_table() {
  ingest_field_t f[] = {field_int("n", 1)};
  ingest_point_t p = point(T0, nullptr, 0, f, 1);

  ingest_plan_t first;
  CHECK(plan_for(nullptr, "m", &p, 1, first).ok());
  const schema_t v1 = schema_after(nullptr, first);
  CHECK(v1.m_version == 1 && v1.m_cols.size() == 2);

  /* Same columns: no change, same version. */
  ingest_plan_t same;
  CHECK(plan_for(&v1, "m", &p, 1, same).ok());
  CHECK(!same.m_creates_table);
  CHECK(same.m_base_version == 1);
  CHECK(same.m_new_cols.empty());
  CHECK(same.m_batch.m_schema_version == 1);

  /* A new field adds a column with the next id and a new version. Names
  match case-insensitively. */
  ingest_field_t f2[] = {field_int("N", 2), field_double("extra", 1.5)};
  ingest_point_t p2 = point(T0, nullptr, 0, f2, 2);
  ingest_plan_t grow;
  CHECK(plan_for(&v1, "m", &p2, 1, grow).ok());
  CHECK(grow.m_new_cols.size() == 1);
  CHECK(grow.m_new_cols[0].m_id == 2 && grow.m_new_cols[0].m_name == "extra");
  CHECK(grow.m_batch.m_schema_version == 2);
  CHECK(grow.m_batch.m_cols.size() == 3);
  CHECK(grow.m_batch.m_cols[1].m_ints[0] == 2);
}

/** A name that changes kind or type is a schema conflict. */
static void test_schema_conflicts() {
  ingest_field_t f[] = {field_int("n", 1)};
  ingest_point_t p = point(T0, nullptr, 0, f, 1);
  ingest_plan_t plan;
  CHECK(plan_for(nullptr, "m", &p, 1, plan).ok());
  const schema_t v1 = schema_after(nullptr, plan);

  /* Type differs from the table. */
  CHECK(plan_one_field(&v1, field_double("n", 1.0)).code() ==
        DB_SCHEMA_CONFLICT);

  /* Kind differs from the table. */
  ingest_tag_t t[] = {tag("n", "x")};
  ingest_point_t pt = point(T0, t, 1, f, 1);
  CHECK(plan_for(&v1, "m", &pt, 1, plan).code() == DB_SCHEMA_CONFLICT);

  /* Type differs between points of one new batch. */
  ingest_field_t a[] = {field_int("v", 1)};
  ingest_field_t b[] = {field_str("v", "1")};
  ingest_point_t two[] = {point(T0, nullptr, 0, a, 1),
                          point(T0, nullptr, 0, b, 1)};
  const db_status_t status = plan_for(nullptr, "m", two, 2, plan);
  CHECK(status.code() == DB_SCHEMA_CONFLICT);

  /* The result owns the reason and the failed plan is empty. */
  CHECK(!status.message().empty());
  CHECK(plan.m_new_cols.empty() && plan.m_batch.m_n_rows == 0);
  CHECK(plan.m_table_name.empty());
}

/** Reserved, non-identifier and too long names are rejected. */
static void test_invalid_names() {
  CHECK(plan_one_field(nullptr, field_int("time", 1)).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(plan_one_field(nullptr, field_int("TIME", 1)).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(plan_one_field(nullptr, field_int("_pt_x", 1)).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(plan_one_field(nullptr, field_int("1abc", 1)).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(plan_one_field(nullptr, field_int("a-b", 1)).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(plan_one_field(nullptr, field_int("", 1)).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(plan_one_field(nullptr, field_int("_ok_1", 1)).ok());

  std::string long_name(SCHEMA_MAX_NAME_LEN, 'a');
  CHECK(plan_one_field(nullptr, field_int(long_name.c_str(), 1)).ok());
  long_name += "a";
  CHECK(plan_one_field(nullptr, field_int(long_name.c_str(), 1)).code() ==
        DB_INVALID_ARGUMENT);
}

/** Empty batches, points without fields, a name twice in a point, non-finite
doubles and TIMESTAMP fields are rejected. */
static void test_invalid_points() {
  ingest_plan_t plan;

  /* Empty batch, and a point without fields. */
  CHECK(plan_for(nullptr, "m", nullptr, 0, plan).code() == DB_INVALID_ARGUMENT);
  ingest_tag_t t[] = {tag("host", "a")};
  ingest_point_t no_fields = point(T0, t, 1, nullptr, 0);
  CHECK(plan_for(nullptr, "m", &no_fields, 1, plan).code() ==
        DB_INVALID_ARGUMENT);

  /* The same name twice in one point. */
  ingest_field_t dup[] = {field_int("n", 1), field_int("N", 2)};
  ingest_point_t p = point(T0, nullptr, 0, dup, 2);
  CHECK(plan_for(nullptr, "m", &p, 1, plan).code() == DB_INVALID_ARGUMENT);

  /* Non-finite doubles and a TIMESTAMP field. */
  const double inf = std::numeric_limits<double>::infinity();
  CHECK(plan_one_field(nullptr, field_double("d", std::nan(""))).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(plan_one_field(nullptr, field_double("d", inf)).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(plan_one_field(nullptr, field_double("d", -inf)).code() ==
        DB_INVALID_ARGUMENT);
  ingest_field_t ts = field_int("t", 1);
  ts.m_value.m_type = col_type_t::TIMESTAMP;
  CHECK(plan_one_field(nullptr, ts).code() == DB_INVALID_ARGUMENT);
}

/** Valid UTF-8 is accepted and every kind of invalid UTF-8 rejected. */
static void test_utf8() {
  CHECK(plan_one_field(nullptr, field_str("s", "")).ok());
  CHECK(
      plan_one_field(nullptr, field_str("s", "\xE6\xB8\xA9\xE5\xBA\xA6")).ok());
  CHECK(plan_one_field(nullptr, field_str("s", "\xF0\x9F\x98\x80")).ok());

  /* Overlong '/', a surrogate, a cut sequence, above U+10FFFF, a lone
  continuation byte. */
  const char *bad[] = {"\xC0\xAF", "\xED\xA0\x80", "\xE2\x82",
                       "\xF4\x90\x80\x80", "\x80"};
  for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); ++i) {
    CHECK(plan_one_field(nullptr, field_str("s", bad[i])).code() ==
          DB_INVALID_ARGUMENT);

    ingest_tag_t t[] = {tag("host", bad[i])};
    ingest_field_t f[] = {field_int("n", 1)};
    ingest_point_t p = point(T0, t, 1, f, 1);
    ingest_plan_t plan;
    CHECK(plan_for(nullptr, "m", &p, 1, plan).code() == DB_INVALID_ARGUMENT);
  }
}

/** Times must be in [1900, 2200) and a batch may span INGEST_MAX_DAYS days. */
static void test_time_range_and_days() {
  ingest_field_t f[] = {field_int("n", 1)};
  ingest_plan_t plan;

  /* The bounds of [1900, 2200). */
  const int64_t ok_times[] = {INGEST_MIN_TIME_US, INGEST_MAX_TIME_US - 1};
  const int64_t bad_times[] = {INGEST_MIN_TIME_US - 1, INGEST_MAX_TIME_US};
  for (int i = 0; i < 2; ++i) {
    ingest_point_t ok = point(ok_times[i], nullptr, 0, f, 1);
    CHECK(plan_for(nullptr, "m", &ok, 1, plan).ok());

    ingest_point_t bad = point(bad_times[i], nullptr, 0, f, 1);
    CHECK(plan_for(nullptr, "m", &bad, 1, plan).code() == DB_INVALID_ARGUMENT);
  }

  /* 32 UTC days pass, 33 do not. Times before 1970 count their own day:
  -1 us is day -1, 0 is day 0. */
  std::vector<ingest_point_t> points;
  for (int d = -16; d < 16; ++d) {
    points.push_back(point(d * DAY + (d < 0 ? DAY - 1 : 0), nullptr, 0, f, 1));
  }
  /* A second point on an existing day. */
  points.push_back(point(-1, nullptr, 0, f, 1));
  CHECK(plan_for(nullptr, "m", &points[0], static_cast<uint32_t>(points.size()),
                 plan)
            .ok());

  points.push_back(point(16 * DAY, nullptr, 0, f, 1));
  CHECK(plan_for(nullptr, "m", &points[0], static_cast<uint32_t>(points.size()),
                 plan)
            .code() == DB_LIMIT_EXCEEDED);
}

/** The limits on points, columns, string length and batch size. */
static void test_limits() {
  ingest_plan_t plan;

  /* Points per batch. */
  ingest_field_t f[] = {field_int("n", 1)};
  std::vector<ingest_point_t> many(INGEST_MAX_POINTS + 1,
                                   point(T0, nullptr, 0, f, 1));
  CHECK(plan_for(nullptr, "m", &many[0], INGEST_MAX_POINTS, plan).ok());
  CHECK(plan.m_batch.m_n_rows == INGEST_MAX_POINTS);
  CHECK(plan_for(nullptr, "m", &many[0], INGEST_MAX_POINTS + 1, plan).code() ==
        DB_LIMIT_EXCEEDED);

  /* Columns per table: "time" plus 63 fields fit, one more does not. */
  std::vector<std::string> names;
  for (int i = 0; i < SCHEMA_MAX_COLUMNS; ++i) {
    names.push_back("f" + std::to_string(i));
  }
  std::vector<ingest_field_t> fields;
  for (int i = 0; i < SCHEMA_MAX_COLUMNS - 1; ++i) {
    fields.push_back(field_int(names[i].c_str(), i));
  }
  ingest_point_t wide =
      point(T0, nullptr, 0, &fields[0], static_cast<uint32_t>(fields.size()));
  CHECK(plan_for(nullptr, "m", &wide, 1, plan).ok());
  const schema_t full = schema_after(nullptr, plan);
  CHECK(full.m_cols.size() == SCHEMA_MAX_COLUMNS);

  ingest_field_t one_more[] = {field_int("another", 1)};
  ingest_point_t p = point(T0, nullptr, 0, one_more, 1);
  CHECK(plan_for(&full, "m", &p, 1, plan).code() == DB_LIMIT_EXCEEDED);

  /* String length. */
  std::string big(INGEST_MAX_STRING, 's');
  CHECK(plan_one_field(nullptr, field_str("s", big.c_str())).ok());
  big += "s";
  CHECK(plan_one_field(nullptr, field_str("s", big.c_str())).code() ==
        DB_LIMIT_EXCEEDED);

  /* Data per batch: nine 1 MiB strings exceed the batch limit. */
  std::string mib(INGEST_MAX_STRING, 'x');
  ingest_field_t s[] = {field_str("s", mib.c_str())};
  std::vector<ingest_point_t> heavy(9, point(T0, nullptr, 0, s, 1));
  CHECK(plan_for(nullptr, "m", &heavy[0], 7, plan).ok());
  CHECK(plan_for(nullptr, "m", &heavy[0], 9, plan).code() == DB_LIMIT_EXCEEDED);
}

/** String fields own their bytes, including embedded NULs. */
static void test_owned_string() {
  std::string input("a\0b", 3);
  ingest_field_t field = field_str("message", "");
  field.m_value.m_string = input;
  input.assign("changed");

  ingest_point_t p = point(T0, nullptr, 0, &field, 1);
  ingest_plan_t plan;
  CHECK(plan_for(nullptr, "m", &p, 1, plan).ok());
  CHECK(field.m_value.m_string == std::string("a\0b", 3));
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_owned_string();
  test_new_table();
  test_existing_table();
  test_schema_conflicts();
  test_invalid_names();
  test_invalid_points();
  test_utf8();
  test_time_range_and_days();
  test_limits();

  return (test_result("ingest-t"));
}
