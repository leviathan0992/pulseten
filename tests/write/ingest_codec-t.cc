/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ingest_codec-t.cc
 *    Tests for the WAL payload of a write: round trips, and rejection of cut,
 *    extended and damaged payloads.
 *
 * Covers: WAL payload encoding and decoding in src/ingest/ingest.cc
 *
 *-------------------------------------------------------------------------
 */

#include "ingest.h"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <signal.h>
#include <string>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include "db_batch.h"
#include "db_schema.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"

/** Compare column values, including the exact representation of doubles.
@return true if two columns hold the same values, comparing doubles bit for
  bit */
static bool columns_equal(const batch_column_t &a, const batch_column_t &b) {
  if (a.m_column_id != b.m_column_id || a.m_data_type != b.m_data_type ||
      a.m_nulls != b.m_nulls) {
    return (false);
  }

  if (a.m_booleans != b.m_booleans || a.m_integers != b.m_integers ||
      a.m_string_data != b.m_string_data ||
      a.m_string_offsets != b.m_string_offsets) {
    return (false);
  }

  if (a.m_doubles.size() != b.m_doubles.size()) {
    return (false);
  }

  for (size_t i = 0; i < a.m_doubles.size(); ++i) {
    if (memcmp(&a.m_doubles[i], &b.m_doubles[i], sizeof(double)) != 0) {
      return (false);
    }
  }

  return (true);
}

/** Compare planned schema changes and column batches.
@return true if two plans have the same schema change and rows */
static bool plans_equal(const ingest_plan_t &a, const ingest_plan_t &b) {
  if (a.m_table_name != b.m_table_name ||
      a.m_creates_table != b.m_creates_table ||
      a.m_base_column_count != b.m_base_column_count ||
      a.m_new_columns.size() != b.m_new_columns.size()) {
    return (false);
  }

  for (size_t i = 0; i < a.m_new_columns.size(); ++i) {
    const schema_column_t &x = a.m_new_columns[i];
    const schema_column_t &y = b.m_new_columns[i];

    if (x.m_id != y.m_id || x.m_name != y.m_name ||
        x.m_column_type != y.m_column_type || x.m_data_type != y.m_data_type) {
      return (false);
    }
  }

  const batch_t &p = a.m_batch;
  const batch_t &q = b.m_batch;

  if (p.m_table_name != q.m_table_name || p.m_row_count != q.m_row_count ||
      p.m_columns.size() != q.m_columns.size()) {
    return (false);
  }

  for (size_t i = 0; i < p.m_columns.size(); ++i) {
    if (!columns_equal(p.m_columns[i], q.m_columns[i])) {
      return (false);
    }
  }

  return (true);
}

/** A plan for a new table using every type, NULLs, empty and multi-byte
strings, and awkward numbers. */
static void make_all_types_plan(ingest_plan_t &plan) {
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

  ingest_point_t points[] = {point(TEST_BASE_TIME_US, tags0, 2, fields0, 4),
                             point(-1, tags1, 1, fields1, 3),
                             point(INGEST_MIN_TIME_US, nullptr, 0, fields2, 1)};

  CHECK(plan_for(nullptr, "rich", points, 3, plan).ok());

  return;
}

/** Encode, decode and compare. */
static void assert_round_trip(const ingest_plan_t &plan) {
  std::string payload;
  ingest_encode(plan, payload);

  ingest_plan_t decoded;
  const db_status_t status =
      ingest_decode(reinterpret_cast<const unsigned char *>(payload.data()),
                    payload.size(), decoded);
  CHECK(status.ok());
  CHECK(plans_equal(plan, decoded));
  CHECK(status.message().empty());

  return;
}

/** Plans of every shape decode to what was encoded. */
static void test_round_trip() {
  ingest_plan_t rich;
  make_all_types_plan(rich);
  assert_round_trip(rich);

  /* A write to an existing table, with and without new columns. */
  const schema_t initial_schema = schema_after(nullptr, rich);

  ingest_field_t same[] = {field_int("n", 7)};

  ingest_point_t p1 = point(TEST_BASE_TIME_US, nullptr, 0, same, 1);
  ingest_plan_t plain;
  CHECK(plan_for(&initial_schema, "rich", &p1, 1, plain).ok());
  CHECK(!plain.m_creates_table && plain.m_new_columns.empty());

  assert_round_trip(plain);

  ingest_field_t grow[] = {field_double("extra", 2.5)};

  ingest_point_t p2 = point(TEST_BASE_TIME_US, nullptr, 0, grow, 1);
  ingest_plan_t grown;
  CHECK(plan_for(&initial_schema, "rich", &p2, 1, grown).ok());
  CHECK(grown.m_new_columns.size() == 1);

  assert_round_trip(grown);

  /* Row counts around a byte boundary of the NULL bitmap. */
  const uint32_t counts[] = {1, 7, 8, 9, 16, 17};

  for (size_t k = 0; k < sizeof(counts) / sizeof(counts[0]); ++k) {
    std::vector<ingest_field_t> f;
    std::vector<ingest_point_t> pts;
    f.push_back(field_int("a", 1));
    f.push_back(field_int("b", 2));

    for (uint32_t i = 0; i < counts[k]; ++i) {
      /* Alternate between field a and field b, so both have NULLs. */
      pts.push_back(point(TEST_BASE_TIME_US + i, nullptr, 0, &f[i % 2], 1));
    }

    ingest_plan_t plan;
    CHECK(plan_for(nullptr, "m", &pts[0], counts[k], plan).ok());

    assert_round_trip(plan);
  }

  return;
}

/** Recovery accepts the absolute time boundaries and rejects values outside. */
static void test_timestamp_range() {
  ingest_plan_t rich;
  make_all_types_plan(rich);
  const schema_t schema = schema_after(nullptr, rich);
  ingest_field_t field = field_int("n", 1);
  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);

  const int64_t times[] = {INGEST_MIN_TIME_US - 1,
                           INGEST_MIN_TIME_US,
                           INGEST_MIN_TIME_US + 1,
                           INGEST_MAX_TIME_US - 1,
                           INGEST_MAX_TIME_US,
                           std::numeric_limits<int64_t>::min(),
                           std::numeric_limits<int64_t>::max()};

  for (bool existing : {false, true}) {
    for (int64_t time : times) {
      ingest_plan_t plan;
      if (existing) {
        REQUIRE(plan_for(&schema, "rich", &p, 1, plan).ok());
      } else {
        plan = rich;
      }

      /* Model a structurally valid WAL payload with a changed timestamp. */
      plan.m_batch.m_columns[0].m_integers.back() = time;
      std::string payload;
      ingest_encode(plan, payload);

      ingest_plan_t decoded;
      const db_status_t status =
          ingest_decode(reinterpret_cast<const unsigned char *>(payload.data()),
                        payload.size(), decoded);
      if (time >= INGEST_MIN_TIME_US && time < INGEST_MAX_TIME_US) {
        REQUIRE(status.ok());
        CHECK(plans_equal(plan, decoded));
      } else {
        CHECK(status.code() == DB_CORRUPTION);
        CHECK(decoded.m_table_name.empty());
        CHECK(decoded.m_batch.m_columns.empty());
      }
    }
  }

  return;
}

/** A data-only payload identifies its columns by count and position. */
static void test_column_prefix_header() {
  ingest_field_t field = field_int("n", 7);
  ingest_point_t point_value = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
  ingest_plan_t create;
  CHECK(plan_for(nullptr, "m", &point_value, 1, create).ok());

  const schema_t schema = schema_after(nullptr, create);
  ingest_plan_t write;
  CHECK(plan_for(&schema, "m", &point_value, 1, write).ok());
  CHECK(write.m_base_column_count == 2 && write.m_new_columns.empty());

  std::string payload;
  ingest_encode(write, payload);

  /* Format, name, creation flag, new-column count, row/column counts,
  then two typed, non-NULL 8-byte values. */
  CHECK(payload.size() == 33);

  assert_round_trip(write);

  return;
}

/** A payload cut short or with bytes appended does not decode. */
static void test_truncated_and_extended_payloads() {
  ingest_plan_t rich;
  make_all_types_plan(rich);

  std::string payload;
  ingest_encode(rich, payload);

  /* Every proper prefix is rejected. */
  for (size_t n = 0; n < payload.size(); ++n) {
    ingest_plan_t decoded;
    CHECK(ingest_decode(reinterpret_cast<const unsigned char *>(payload.data()),
                        n, decoded)
              .code() == DB_CORRUPTION);
  }

  /* So is a trailing byte. */
  std::string longer = payload + "x";
  ingest_plan_t decoded;
  CHECK(ingest_decode(reinterpret_cast<const unsigned char *>(longer.data()),
                      longer.size(), decoded)
            .code() == DB_CORRUPTION);

  return;
}

/** A payload with any single bit flipped is rejected, or decodes to a plan
that encodes back to it; it is never read out of bounds. */
static void test_payload_bit_flips() {
  ingest_plan_t rich;
  make_all_types_plan(rich);

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
      damaged[i] = static_cast<char>(damaged[i] ^ (1 << bit));

      ingest_plan_t decoded;
      const db_status_t err =
          ingest_decode(reinterpret_cast<const unsigned char *>(damaged.data()),
                        damaged.size(), decoded);

      if (err.ok()) {
        std::string again;
        ingest_encode(decoded, again);
        CHECK(again == damaged);

        ++accepted;
      } else {
        CHECK(err.code() == DB_CORRUPTION);

        ++rejected;
      }
    }
  }

  printf("damaged payloads: %d rejected, %d decoded consistently\n", rejected,
         accepted);

  return;
}

/** A header that disagrees with the rows does not decode. */
static void test_inconsistent_header() {
  ingest_plan_t rich;
  make_all_types_plan(rich);

  std::string payload;
  ingest_encode(rich, payload);

  /* Byte 0 is the version; the "creates table" flag follows the name. */
  const size_t creates_at = 1 + 2 + rich.m_table_name.size();
  ingest_plan_t decoded;

  std::string bad_version = payload;
  bad_version[0] = static_cast<char>(0xff);
  CHECK(
      ingest_decode(reinterpret_cast<const unsigned char *>(bad_version.data()),
                    bad_version.size(), decoded)
          .code() == DB_CORRUPTION);

  /* Not creating the table, yet no existing columns precede the new ones. */
  std::string not_create = payload;
  not_create[creates_at] = 0;
  CHECK(
      ingest_decode(reinterpret_cast<const unsigned char *>(not_create.data()),
                    not_create.size(), decoded)
          .code() == DB_CORRUPTION);

  return;
}

#ifdef PT_DEBUG
/** Reject malformed internal columns before encoding reads their arrays. */
static void test_invalid_batch_layout() {
  for (int fault = 0; fault < 6; ++fault) {
    ingest_plan_t plan;
    make_all_types_plan(plan);
    batch_column_t &time = plan.m_batch.m_columns[0];

    if (fault == 0) {
      time.m_nulls.pop_back();
    } else if (fault == 1) {
      time.m_integers.pop_back();
    } else {
      for (batch_column_t &col : plan.m_batch.m_columns) {
        if (col.m_data_type != data_type_t::VARCHAR) {
          continue;
        }

        if (fault == 2) {
          col.m_string_offsets.pop_back();
        } else if (fault == 3) {
          col.m_string_offsets[0] = 1;
        } else if (fault == 4) {
          col.m_string_offsets[1] = col.m_string_offsets.back() + 1;
        } else {
          col.m_string_data.push_back('x');
        }

        break;
      }
    }

    const pid_t pid = fork();
    REQUIRE(pid >= 0);
    if (pid == 0) {
      std::string payload;
      ingest_encode(plan, payload);
      _exit(0);
    }

    int status = 0;
    REQUIRE(waitpid(pid, &status, 0) == pid);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
  }

  return;
}
#endif /* PT_DEBUG */

/** Run WAL payload round-trip and corruption tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"round_trip", test_round_trip},
      {"timestamp_range", test_timestamp_range},
      {"column_prefix_header", test_column_prefix_header},
      {"truncated_and_extended_payloads", test_truncated_and_extended_payloads},
      {"payload_bit_flips", test_payload_bit_flips},
      {"inconsistent_header", test_inconsistent_header},
#ifdef PT_DEBUG
      {"invalid_batch_layout", test_invalid_batch_layout},
#endif /* PT_DEBUG */
  };

  return (test_main("ingest_codec-t", argc, argv, cases));
}
