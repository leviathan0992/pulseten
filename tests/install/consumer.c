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

#include <stdio.h>

#include "pulseten.h"

/** Report a failed call.
@param[in]  what   name of the failed API call
@param[in]  state  public status returned by that call
@return 1, the exit status */
static int fail(const char *what, pulseten_state_t state) {
  fprintf(stderr, "%s: %s: %s\n", what, pulseten_state_name(state),
          pulseten_errmsg());

  return (1);
}

/** Write one point into the directory named by the first argument and check
that a query counts it.
@param[in]  argc  number of command-line arguments, including the program name
@param[in]  argv  argument strings; argv[0] names the program
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main(int argc, char **argv) {
  pulseten_db_t *db = NULL;
  pulseten_result_t *result = NULL;
  pulseten_field_t field;
  pulseten_point_t point;
  pulseten_state_t state;
  uint32_t n = 0;
  int64_t count = 0;

  if (argc != 2) {
    fprintf(stderr, "usage: %s <dir>\n", argv[0]);

    return (2);
  }

  state = pulseten_open(argv[1], NULL, &db);
  if (state != PULSETEN_OK) {
    return (fail("open", state));
  }

  field.name = "v";
  field.name_len = 1;
  field.type = PULSETEN_TYPE_BIGINT;
  field.value.i = 1;

  point.time_us = 1767225600000000LL;
  point.tags = NULL;
  point.n_tags = 0;
  point.fields = &field;
  point.n_fields = 1;

  state = pulseten_write(db, "m", &point, 1, NULL);
  if (state != PULSETEN_OK) {
    return (fail("write", state));
  }

  state = pulseten_query(db, "SELECT count(*) FROM m", &result);
  if (state != PULSETEN_OK) {
    return (fail("query", state));
  }

  state = pulseten_fetch(result, &n);
  if (state != PULSETEN_OK) {
    return (fail("fetch", state));
  }
  if (n == 1) {
    count = pulseten_get_int64(result, 0, 0);
  }
  pulseten_result_destroy(result);

  state = pulseten_close(db);
  if (state != PULSETEN_OK) {
    return (fail("close", state));
  }

  printf("%s: %lld row\n", pulseten_version(), (long long)count);

  return (count == 1 ? 0 : 1);
}
