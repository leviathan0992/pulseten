/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ingest0codec-t.cc
 *    Tests for the WAL payload of a write: round trips, and rejection of
 *    cut, extended and damaged payloads.
 *
 *-------------------------------------------------------------------------
 */

#include <cmath>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "ingest0codec.h"
#include "test0point.h"
#include "test0util.h"

/** Compare column values, including the exact representation of doubles.
@param[in]  a  first column to compare
@param[in]  b  second column to compare
@return true if two columns hold the same values, comparing doubles bit for
  bit */
static bool cols_equal(const batch_col_t &a, const batch_col_t &b) {
  if (a.col_id != b.col_id || a.type != b.type || a.nulls != b.nulls) {
    return (false);
  }

  if (a.bools != b.bools || a.ints != b.ints || a.chars != b.chars ||
      a.offsets != b.offsets) {
    return (false);
  }

  if (a.doubles.size() != b.doubles.size()) {
    return (false);
  }

  for (size_t i = 0; i < a.doubles.size(); ++i) {
    if (memcmp(&a.doubles[i], &b.doubles[i], sizeof(double)) != 0) {
      return (false);
    }
  }

  return (true);
}

/** Compare planned schema changes and column batches.
@param[in]  a  first plan to compare
@param[in]  b  second plan to compare
@return true if two plans have the same schema change and rows */
static bool plans_equal(const ingest_plan_t &a, const ingest_plan_t &b) {
  if (a.table != b.table || a.creates_table != b.creates_table ||
      a.base_version != b.base_version ||
      a.new_cols.size() != b.new_cols.size()) {
    return (false);
  }

  for (size_t i = 0; i < a.new_cols.size(); ++i) {
    const col_def_t &x = a.new_cols[i];
    const col_def_t &y = b.new_cols[i];

    if (x.id != y.id || x.name != y.name || x.role != y.role ||
        x.type != y.type) {
      return (false);
    }
  }

  const batch_t &p = a.batch;
  const batch_t &q = b.batch;

  if (p.table != q.table || p.schema_version != q.schema_version ||
      p.n_rows != q.n_rows || p.cols.size() != q.cols.size()) {
    return (false);
  }

  for (size_t i = 0; i < p.cols.size(); ++i) {
    if (!cols_equal(p.cols[i], q.cols[i])) {
      return (false);
    }
  }

  return (true);
}

/** A plan for a new table using every type, NULLs, empty and multi-byte
strings, and awkward numbers.
@param[out]  plan  new-table plan exercising every supported value type */
static void make_rich_plan(ingest_plan_t *plan) {
  ingest_tag_t tags0[] = {tag("host", "a"), tag("zone", "")};
  ingest_field_t fields0[] = {
      field_int("n", std::numeric_limits<int64_t>::min()),
      field_double("d", -0.0), field_bool("b", true),
      field_str("s", "\xE6\xB8\xA9\xE5\xBA\xA6")};

  ingest_tag_t tags1[] = {tag("host", "b")};
  ingest_field_t fields1[] = {
      field_int("n", std::numeric_limits<int64_t>::max()),
      field_double("d", 1e-310), field_bool("b", false)};

  ingest_field_t fields2[] = {field_str("s", "")};

  ingest_point_t points[] = {point(T0, tags0, 2, fields0, 4),
                             point(-1, tags1, 1, fields1, 3),
                             point(INGEST_MIN_TIME_US, nullptr, 0, fields2, 1)};

  CHECK(plan_for(nullptr, "rich", points, 3, plan) == DB_SUCCESS);
}

/** Encode, decode and compare.
@param[in]  plan  plan to encode, decode and compare */
static void check_round_trip(const ingest_plan_t &plan) {
  std::string payload;
  ingest_encode(plan, payload);

  ingest_plan_t decoded;
  CHECK(ingest_decode((const unsigned char *)payload.data(), payload.size(),
                      decoded) == DB_SUCCESS);
  CHECK(plans_equal(plan, decoded));
  CHECK(decoded.error.empty());
}

/** Plans of every shape decode to what was encoded. */
static void test_round_trip() {
  ingest_plan_t rich;
  make_rich_plan(&rich);
  check_round_trip(rich);

  /* A write to an existing table, with and without new columns. */
  const schema_t v1 = schema_after(nullptr, rich);

  ingest_field_t same[] = {field_int("n", 7)};
  ingest_point_t p1 = point(T0, nullptr, 0, same, 1);
  ingest_plan_t plain;
  CHECK(plan_for(&v1, "rich", &p1, 1, &plain) == DB_SUCCESS);
  CHECK(!plain.creates_table && plain.new_cols.empty());
  check_round_trip(plain);

  ingest_field_t grow[] = {field_double("extra", 2.5)};
  ingest_point_t p2 = point(T0, nullptr, 0, grow, 1);
  ingest_plan_t grown;
  CHECK(plan_for(&v1, "rich", &p2, 1, &grown) == DB_SUCCESS);
  CHECK(grown.new_cols.size() == 1);
  check_round_trip(grown);

  /* Row counts around a byte boundary of the NULL bitmap. */
  const uint32_t counts[] = {1, 7, 8, 9, 16, 17};
  for (size_t k = 0; k < sizeof(counts) / sizeof(counts[0]); ++k) {
    std::vector<ingest_field_t> f;
    std::vector<ingest_point_t> pts;
    f.push_back(field_int("a", 1));
    f.push_back(field_int("b", 2));
    for (uint32_t i = 0; i < counts[k]; ++i) {
      /* Alternate between field a and field b, so both have NULLs. */
      pts.push_back(point(T0 + i, nullptr, 0, &f[i % 2], 1));
    }
    ingest_plan_t plan;
    CHECK(plan_for(nullptr, "m", &pts[0], counts[k], &plan) == DB_SUCCESS);
    check_round_trip(plan);
  }
}

/** A payload cut short or with bytes appended does not decode. */
static void test_cut_and_extended() {
  ingest_plan_t rich;
  make_rich_plan(&rich);

  std::string payload;
  ingest_encode(rich, payload);

  /* Every proper prefix is rejected. */
  for (size_t n = 0; n < payload.size(); ++n) {
    ingest_plan_t decoded;
    CHECK(ingest_decode((const unsigned char *)payload.data(), n, decoded) ==
          DB_CORRUPTION);
  }

  /* So is a trailing byte. */
  std::string longer = payload + "x";
  ingest_plan_t decoded;
  CHECK(ingest_decode((const unsigned char *)longer.data(), longer.size(),
                      decoded) == DB_CORRUPTION);
}

/** A payload with any single bit flipped is rejected, or decodes to a plan
that encodes back to it; it is never read out of bounds. */
static void test_damaged() {
  ingest_plan_t rich;
  make_rich_plan(&rich);

  std::string payload;
  ingest_encode(rich, payload);

  /* Flip single bits everywhere. A damaged payload must either be
  rejected, or decode to a plan that encodes back to exactly the damaged
  bytes; it must never crash or read out of bounds. */
  int accepted = 0;
  int rejected = 0;

  for (size_t i = 0; i < payload.size(); ++i) {
    for (int bit = 0; bit < 8; ++bit) {
      std::string damaged = payload;
      damaged[i] = (char)(damaged[i] ^ (1 << bit));

      ingest_plan_t decoded;
      const dberr_t err = ingest_decode((const unsigned char *)damaged.data(),
                                        damaged.size(), decoded);

      if (err == DB_SUCCESS) {
        std::string again;
        ingest_encode(decoded, again);
        CHECK(again == damaged);
        ++accepted;
      } else {
        CHECK(err == DB_CORRUPTION);
        ++rejected;
      }
    }
  }

  printf("damaged payloads: %d rejected, %d decoded consistently\n", rejected,
         accepted);
}

/** A header that disagrees with the rows does not decode. */
static void test_inconsistent_header() {
  ingest_plan_t rich;
  make_rich_plan(&rich);

  std::string payload;
  ingest_encode(rich, payload);

  /* Byte 0 is the version; the "creates table" flag follows the name. */
  const size_t creates_at = 1 + 2 + rich.table.size();
  ingest_plan_t decoded;

  std::string bad_version = payload;
  bad_version[0] = 2;
  CHECK(ingest_decode((const unsigned char *)bad_version.data(),
                      bad_version.size(), decoded) == DB_CORRUPTION);

  /* Not creating the table, yet based on version 0. */
  std::string not_create = payload;
  not_create[creates_at] = 0;
  CHECK(ingest_decode((const unsigned char *)not_create.data(),
                      not_create.size(), decoded) == DB_CORRUPTION);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_round_trip();
  test_cut_and_extended();
  test_damaged();
  test_inconsistent_header();

  return (test_result("ingest0codec-t"));
}
