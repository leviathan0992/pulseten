/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * consumer.c
 *    A C program using an installed PulseTen, built by a project that
 *    enables only the C language: write one point and read it back.
 *
 * Usage: consumer <dir>
 *
 *-------------------------------------------------------------------------
 */

#include "pulseten.h"

#include <stdint.h>
#include <stdio.h>

/** Report a failed call.
@return 1, the exit status */
static int fail(const char *what, pulseten_status_t status) {
  fprintf(stderr, "%s: %s: %s\n", what, pulseten_status_name(status),
          pulseten_errmsg());

  return (1);
}

/** Write and query one point on a database owned by the caller.
@param[out]  count  number of rows returned by the query
@return 0 on success, 1 after reporting an operation or row count failure */
static int run_consumer(pulseten_db_t *db, int64_t *count) {
  pulseten_result_t *result = NULL;
  pulseten_field_t field;
  pulseten_point_t point;
  pulseten_status_t status;
  uint32_t n = 0;

  field.m_name = "v";
  field.m_name_len = 1;
  field.m_type = PULSETEN_TYPE_BIGINT;
  field.m_value.m_i = 1;

  point.m_time_us = 1767225600000000LL;
  point.m_tags = NULL;
  point.m_n_tags = 0;
  point.m_fields = &field;
  point.m_n_fields = 1;

  status = pulseten_write(db, "m", &point, 1);
  if (status != PULSETEN_OK) {
    return (fail("write", status));
  }

  status = pulseten_exec_ddl(db, "CREATE TABLE copied AS SELECT * FROM m");
  if (status != PULSETEN_OK) {
    return (fail("exec", status));
  }

  status = pulseten_query(db, "SELECT count(*) FROM copied", &result);
  if (status != PULSETEN_OK) {
    return (fail("query", status));
  }

  status = pulseten_result_fetch(result, &n);
  if (status != PULSETEN_OK) {
    const int failed = fail("fetch", status);
    pulseten_result_destroy(result);

    return (failed);
  }
  if (n == 1) {
    *count = pulseten_result_get_int64(result, 0, 0);
  }
  pulseten_result_destroy(result);

  status = pulseten_drop_table(db, "copied");
  if (status != PULSETEN_OK) {
    return (fail("drop ordinary table", status));
  }

  return (*count == 1 ? 0 : 1);
}

/** Write one point into the directory named by the first argument and check
that a query counts it.
@param[in]  argv  argument strings; argv[0] names the program
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main(int argc, char **argv) {
  pulseten_db_t *db = NULL;
  int64_t count = 0;
  int result;
  pulseten_status_t status;
  if (argc != 2) {
    fprintf(stderr, "usage: %s <dir>\n", argv[0]);

    return (2);
  }

  status = pulseten_open(argv[1], NULL, &db);
  if (status != PULSETEN_OK) {
    return (fail("open", status));
  }

  result = run_consumer(db, &count);
  status = pulseten_close(db);
  if (status != PULSETEN_OK) {
    return (fail("close", status));
  }
  if (result != 0) {
    return (result);
  }

  printf("%s: %lld row\n", pulseten_version(), (long long)count);

  return (0);
}
