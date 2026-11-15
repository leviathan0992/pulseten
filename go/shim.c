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
static char *pt_go_copy_error_message(pulseten_status_t status) {
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

/** Open a database and copy the calling thread's error message. */
pulseten_status_t pt_go_open(const char *dir, const pulseten_options_t *options,
                             pulseten_db_t **db, char **msg) {
  const pulseten_status_t status = pulseten_open(dir, options, db);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Write a point batch and copy any failure message for Go. */
pulseten_status_t pt_go_write(pulseten_db_t *db, const char *table_name,
                              const pulseten_point_t *points, uint32_t n_points,
                              char **msg) {
  const pulseten_status_t status =
      pulseten_write(db, table_name, points, n_points);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Copy the table catalog and any failure message for Go. */
pulseten_status_t pt_go_catalog(pulseten_db_t *db, pulseten_catalog_t **catalog,
                                char **msg) {
  const pulseten_status_t status = pulseten_catalog_snapshot(db, catalog);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Execute SQL and copy its diagnostic in the same C call. */
pulseten_status_t pt_go_exec(pulseten_db_t *db, const char *sql,
                             pulseten_cancel_t *cancel, char **msg) {
  const pulseten_status_t status = pulseten_exec_with_cancel(db, sql, cancel);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Open a cancellable query and copy any failure message for Go. */
pulseten_status_t pt_go_query(pulseten_db_t *db, const char *sql,
                              pulseten_cancel_t *cancel,
                              pulseten_result_t **result, char **msg) {
  const pulseten_status_t status =
      pulseten_query_with_cancel(db, sql, cancel, result);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Allocate a cancellation handle and copy any failure message for Go. */
pulseten_status_t pt_go_cancel_create(pulseten_cancel_t **cancel, char **msg) {
  const pulseten_status_t status = pulseten_cancel_create(cancel);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Fetch the next result chunk and copy any failure message for Go. */
pulseten_status_t pt_go_result_fetch(pulseten_result_t *result,
                                     uint32_t *n_rows, char **msg) {
  const pulseten_status_t status = pulseten_result_fetch(result, n_rows);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Read a query plan and copy any failure message for Go. */
pulseten_status_t pt_go_explain(pulseten_db_t *db, const char *sql, char **plan,
                                char **msg) {
  const pulseten_status_t status = pulseten_explain(db, sql, plan);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Copy background diagnostics and any failure message for Go. */
pulseten_status_t pt_go_copy_background_error(pulseten_db_t *db, char **message,
                                              char **msg) {
  const pulseten_status_t status = pulseten_copy_background_error(db, message);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Replace table settings and copy any failure message for Go. */
pulseten_status_t pt_go_set_table_options(
    pulseten_db_t *db, const char *name,
    const pulseten_table_options_t *options, char **msg) {
  const pulseten_status_t status =
      pulseten_set_table_options(db, name, options);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Declare a table and copy any failure message in the same C call. */
pulseten_status_t pt_go_create_table(
    pulseten_db_t *db, const char *name,
    const pulseten_create_table_options_t *options, char **msg) {
  const pulseten_status_t status = pulseten_create_table(db, name, options);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Drop a table and copy any error before the goroutine can change threads. */
pulseten_status_t pt_go_drop_table(pulseten_db_t *db, const char *name,
                                   char **msg) {
  const pulseten_status_t status = pulseten_drop_table(db, name);
  *msg = pt_go_copy_error_message(status);

  return (status);
}

/** Delete a closed database and capture its diagnostic for Go. */
pulseten_status_t pt_go_drop_database(const char *dir, char **msg) {
  const pulseten_status_t status = pulseten_drop_database(dir);
  *msg = pt_go_copy_error_message(status);

  return (status);
}
