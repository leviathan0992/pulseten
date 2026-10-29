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

#include <cstdint>
#include <cstdio>
#include <cstring>

#include "pulseten.h"

/** 2026-01-01 00:00:00 UTC in microseconds. */
static const int64_t T0 = 1767225600000000LL;

/** Report a failed call.
@param[in]  what   name of the failed API call
@param[in]  state  public status returned by that call
@return 1, the exit status */
static int fail(const char *what, pulseten_state_t state) {
  fprintf(stderr, "%s: %s: %s\n", what, pulseten_state_name(state),
          pulseten_errmsg());

  return (1);
}

/** Write an hour of cpu points for three hosts into the directory named by
the first argument, then print each host's average usage.
@param[in]  argc  number of command-line arguments, including the program name
@param[in]  argv  argument strings; argv[0] names the program
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "usage: %s <dir>\n", argv[0]);

    return (2);
  }

  pulseten_db_t *db = nullptr;
  pulseten_state_t state = pulseten_open(argv[1], nullptr, &db);
  if (state != PULSETEN_OK) {
    return (fail("open", state));
  }

  /* One point per host and minute for an hour, written as one batch per
  minute. The cpu table and its columns are created by the first write. */
  const char *hosts[] = {"web-1", "web-2", "db-1"};

  for (int i = 0; i < 60; ++i) {
    pulseten_tag_t tags[3];
    pulseten_field_t fields[3];
    pulseten_point_t points[3];

    for (int h = 0; h < 3; ++h) {
      tags[h].name = "host";
      tags[h].name_len = strlen("host");
      tags[h].value = hosts[h];
      tags[h].value_len = strlen(hosts[h]);

      fields[h].name = "usage";
      fields[h].name_len = strlen("usage");
      fields[h].type = PULSETEN_TYPE_DOUBLE;
      fields[h].value.d = 10.0 * (h + 1) + (i % 10);

      points[h].time_us = T0 + (int64_t)i * 60 * 1000000;
      points[h].tags = &tags[h];
      points[h].n_tags = 1;
      points[h].fields = &fields[h];
      points[h].n_fields = 1;
    }

    state = pulseten_write(db, "cpu", points, 3, nullptr);
    if (state != PULSETEN_OK) {
      return (fail("write", state));
    }
  }

  /* Every write is already durable and visible to queries. */
  pulseten_result_t *result = nullptr;
  state = pulseten_query(db,
                         "SELECT host, count(*) AS n, avg(usage) AS avg_usage "
                         "FROM cpu "
                         "WHERE time >= '2026-01-01 00:30:00' "
                         "GROUP BY host ORDER BY host",
                         &result);
  if (state != PULSETEN_OK) {
    return (fail("query", state));
  }

  printf("%-8s %4s %10s\n", pulseten_column_name(result, 0),
         pulseten_column_name(result, 1), pulseten_column_name(result, 2));

  for (;;) {
    uint32_t n = 0;
    state = pulseten_fetch(result, &n);
    if (state != PULSETEN_OK) {
      return (fail("fetch", state));
    }

    if (n == 0) {
      break;
    }

    for (uint32_t r = 0; r < n; ++r) {
      printf("%-8s %4lld %10.2f\n", pulseten_get_varchar(result, 0, r, nullptr),
             (long long)pulseten_get_int64(result, 1, r),
             pulseten_get_double(result, 2, r));
    }
  }

  pulseten_result_destroy(result);

  state = pulseten_close(db);
  if (state != PULSETEN_OK) {
    return (fail("close", state));
  }

  return (0);
}
