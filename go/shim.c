/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * shim.c
 *    C calls for the Go package that also copy the error message.
 *
 *-------------------------------------------------------------------------
 */

#include "pulseten.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "shim.h"

/** Copy the calling thread's message after a call.
@return NULL on success, otherwise a malloc() copy of the message; NULL also
  if the copy cannot be allocated */
static char *pt_go_msg(pulseten_status_t status) {
  if (status == PULSETEN_OK) {
    return (NULL);
  }

  const char *msg = pulseten_error_message();
  const size_t len = strlen(msg);
  char *copy = malloc(len + 1);

  if (copy != NULL) {
    memcpy(copy, msg, len + 1);
  }

  return (copy);
}

/** As pulseten_open(). */
pulseten_status_t pt_go_open(const char *dir, const pulseten_options_t *options,
                             pulseten_db_t **db, char **msg) {
  const pulseten_status_t status = pulseten_open(dir, options, db);
  *msg = pt_go_msg(status);

  return (status);
}

/** As pulseten_write().
@param[in]  table  table name, NUL-terminated */
pulseten_status_t pt_go_write(pulseten_db_t *db, const char *table_name,
                              const pulseten_point_t *points, uint32_t n_points,
                              char **msg) {
  const pulseten_status_t status =
      pulseten_write(db, table_name, points, n_points);
  *msg = pt_go_msg(status);

  return (status);
}

/** As pulseten_catalog_snapshot(). */
pulseten_status_t pt_go_catalog(pulseten_db_t *db, pulseten_catalog_t **catalog,
                                char **msg) {
  const pulseten_status_t status = pulseten_catalog_snapshot(db, catalog);
  *msg = pt_go_msg(status);

  return (status);
}

/** As pulseten_query_with_cancel().
@param[in]  sql  a single SELECT, NUL-terminated */
pulseten_status_t pt_go_query(pulseten_db_t *db, const char *sql,
                              pulseten_cancel_t *cancel,
                              pulseten_result_t **result, char **msg) {
  const pulseten_status_t status =
      pulseten_query_with_cancel(db, sql, cancel, result);
  *msg = pt_go_msg(status);

  return (status);
}

/** As pulseten_cancel_create(). */
pulseten_status_t pt_go_cancel_create(pulseten_cancel_t **cancel, char **msg) {
  const pulseten_status_t status = pulseten_cancel_create(cancel);
  *msg = pt_go_msg(status);
  return (status);
}

/** As pulseten_result_fetch(). */
pulseten_status_t pt_go_result_fetch(pulseten_result_t *result,
                                     uint32_t *n_rows, char **msg) {
  const pulseten_status_t status = pulseten_result_fetch(result, n_rows);
  *msg = pt_go_msg(status);

  return (status);
}

/** As pulseten_explain().
@param[in]   sql   a single SELECT, NUL-terminated
@param[out]  plan  the plan; free it with pulseten_free() */
pulseten_status_t pt_go_explain(pulseten_db_t *db, const char *sql, char **plan,
                                char **msg) {
  const pulseten_status_t status = pulseten_explain(db, sql, plan);
  *msg = pt_go_msg(status);

  return (status);
}

/** As pulseten_copy_background_error().
@param[out]  message  the message or NULL; free it with pulseten_free() */
pulseten_status_t pt_go_copy_background_error(pulseten_db_t *db, char **message,
                                              char **msg) {
  const pulseten_status_t status = pulseten_copy_background_error(db, message);
  *msg = pt_go_msg(status);

  return (status);
}
