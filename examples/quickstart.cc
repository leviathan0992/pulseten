/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * quickstart.cc
 *    Open a database, write points and query them with SQL.
 *
 * Usage: quickstart <dir>
 *
 *-------------------------------------------------------------------------
 */

#include "pulseten.h"

#include <cstdint>
#include <cstdio>
#include <cstring>

/** 2026-01-01 00:00:00 UTC in microseconds. */
static const int64_t T0 = 1767225600000000LL;

/** Report a failed call.
@return 1, the exit status */
static int fail(const char *what, pulseten_status_t state) {
  fprintf(stderr, "%s: %s: %s\n", what, pulseten_status_name(state),
          pulseten_error_message());

  return (1);
}

/** Own a query result until the example has finished reading it. */
struct quickstart_result_guard_t {
  /** Result returned by pulseten_query(). */
  pulseten_result_t *m_result{nullptr};

  /** Release the result on success and failure. */
  ~quickstart_result_guard_t() { pulseten_result_destroy(m_result); }
};

/** Write and query the example data on a database owned by the caller.
@return 0 on success, 1 after reporting a write or query failure */
static int run_example(pulseten_db_t *db) {
  /* One point per host and minute for an hour, written as one batch per
  minute. The cpu table and its columns are created by the first write. */
  const char *hosts[] = {"web-1", "web-2", "db-1"};

  for (int i = 0; i < 60; ++i) {
    pulseten_tag_t tags[3];
    pulseten_field_t fields[3];
    pulseten_point_t points[3];

    for (int h = 0; h < 3; ++h) {
      tags[h].m_name = "host";
      tags[h].m_name_length = strlen("host");
      tags[h].m_value = hosts[h];
      tags[h].m_value_length = strlen(hosts[h]);

      fields[h].m_name = "usage";
      fields[h].m_name_length = strlen("usage");
      fields[h].m_type = PULSETEN_TYPE_DOUBLE;
      fields[h].m_value.m_double = 10.0 * (h + 1) + (i % 10);

      points[h].m_time_us = T0 + static_cast<int64_t>(i) * 60 * 1000000;
      points[h].m_tags = &tags[h];
      points[h].m_tag_count = 1;
      points[h].m_fields = &fields[h];
      points[h].m_field_count = 1;
    }

    pulseten_status_t state = pulseten_write(db, "cpu", points, 3);
    if (state != PULSETEN_OK) {
      return (fail("write", state));
    }
  }

  /* Every write is already logged and visible to queries. */
  quickstart_result_guard_t owner;
  pulseten_result_t *&result = owner.m_result;
  pulseten_status_t state =
      pulseten_query(db,
                     "SELECT host, count(*) AS n, avg(usage) AS avg_usage "
                     "FROM cpu "
                     "WHERE time >= '2026-01-01 00:30:00' "
                     "GROUP BY host ORDER BY host",
                     &result);
  if (state != PULSETEN_OK) {
    return (fail("query", state));
  }

  printf("%-8s %4s %10s\n", pulseten_result_column_name(result, 0),
         pulseten_result_column_name(result, 1),
         pulseten_result_column_name(result, 2));

  for (;;) {
    uint32_t n = 0;
    state = pulseten_result_fetch(result, &n);
    if (state != PULSETEN_OK) {
      return (fail("fetch", state));
    }

    if (n == 0) {
      break;
    }

    for (uint32_t r = 0; r < n; ++r) {
      printf("%-8s %4lld %10.2f\n",
             pulseten_read_varchar(result, 0, r, nullptr),
             static_cast<long long>(pulseten_read_int64(result, 1, r)),
             pulseten_read_double(result, 2, r));
    }
  }

  return (0);
}

/** Write an hour of cpu points for three hosts into the directory named by
the first argument, then print each host's average usage.
@param[in]  argv  argument strings; argv[0] names the program
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "usage: %s <dir>\n", argv[0]);

    return (2);
  }

  pulseten_db_t *db = nullptr;
  pulseten_status_t state = pulseten_open(argv[1], nullptr, &db);
  if (state != PULSETEN_OK) {
    return (fail("open", state));
  }

  const int result = run_example(db);
  state = pulseten_close(db);
  if (state != PULSETEN_OK) {
    return (fail("close", state));
  }

  return (result);
}
