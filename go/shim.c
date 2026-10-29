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

#include "shim.h"

#include <stdlib.h>
#include <string.h>

/** Copy the calling thread's message after a call.
@param[in]  state  status of the call
@return NULL on success, otherwise a malloc() copy of the message; NULL also
  if the copy cannot be allocated */
static char *pt_go_msg(pulseten_state_t state) {
  if (state == PULSETEN_OK) {
    return (NULL);
  }

  const char *msg = pulseten_errmsg();
  const size_t len = strlen(msg);
  char *copy = malloc(len + 1);

  if (copy != NULL) {
    memcpy(copy, msg, len + 1);
  }

  return (copy);
}

/** As pulseten_open().
@param[in]   dir      directory path
@param[in]   options  settings
@param[out]  db       open database
@param[out]  msg      message on failure
@return the status */
pulseten_state_t pt_go_open(const char *dir, const pulseten_options_t *options,
                            pulseten_db_t **db, char **msg) {
  const pulseten_state_t state = pulseten_open(dir, options, db);
  *msg = pt_go_msg(state);

  return (state);
}

/** As pulseten_close().
@param[in]   db   database
@param[out]  msg  message on failure
@return the status */
pulseten_state_t pt_go_close(pulseten_db_t *db, char **msg) {
  const pulseten_state_t state = pulseten_close(db);
  *msg = pt_go_msg(state);

  return (state);
}

/** As pulseten_write().
@param[in]   db        database
@param[in]   table     table name, NUL-terminated
@param[in]   points    the points
@param[in]   n_points  number of points
@param[out]  seq       sequence number of the write
@param[out]  msg       message on failure
@return the status */
pulseten_state_t pt_go_write(pulseten_db_t *db, const char *table,
                             const pulseten_point_t *points, uint32_t n_points,
                             uint64_t *seq, char **msg) {
  const pulseten_state_t state =
      pulseten_write(db, table, points, n_points, seq);
  *msg = pt_go_msg(state);

  return (state);
}

/** As pulseten_flush().
@param[in]   db   database
@param[out]  msg  message on failure
@return the status */
pulseten_state_t pt_go_flush(pulseten_db_t *db, char **msg) {
  const pulseten_state_t state = pulseten_flush(db);
  *msg = pt_go_msg(state);

  return (state);
}

/** As pulseten_expire().
@param[in]   db       database
@param[out]  removed  partitions dropped
@param[out]  msg      message on failure
@return the status */
pulseten_state_t pt_go_expire(pulseten_db_t *db, uint64_t *removed,
                              char **msg) {
  const pulseten_state_t state = pulseten_expire(db, removed);
  *msg = pt_go_msg(state);

  return (state);
}

/** As pulseten_catalog_get().
@param[in]   db       database
@param[out]  catalog  the snapshot
@param[out]  msg      message on failure
@return the status */
pulseten_state_t pt_go_catalog_get(pulseten_db_t *db,
                                   pulseten_catalog_t **catalog, char **msg) {
  const pulseten_state_t state = pulseten_catalog_get(db, catalog);
  *msg = pt_go_msg(state);

  return (state);
}

/** As pulseten_drop_table().
@param[in]   db     database
@param[in]   table  table name, NUL-terminated
@param[out]  msg    message on failure
@return the status */
pulseten_state_t pt_go_drop_table(pulseten_db_t *db, const char *table,
                                  char **msg) {
  const pulseten_state_t state = pulseten_drop_table(db, table);
  *msg = pt_go_msg(state);

  return (state);
}

/** As pulseten_query().
@param[in]   db      database
@param[in]   sql     a single SELECT, NUL-terminated
@param[out]  result  the result
@param[out]  msg     message on failure
@return the status */
pulseten_state_t pt_go_query(pulseten_db_t *db, const char *sql,
                             pulseten_result_t **result, char **msg) {
  const pulseten_state_t state = pulseten_query(db, sql, result);
  *msg = pt_go_msg(state);

  return (state);
}

/** As pulseten_fetch().
@param[in]   result  result
@param[out]  n_rows  rows in the chunk
@param[out]  msg     message on failure
@return the status */
pulseten_state_t pt_go_fetch(pulseten_result_t *result, uint32_t *n_rows,
                             char **msg) {
  const pulseten_state_t state = pulseten_fetch(result, n_rows);
  *msg = pt_go_msg(state);

  return (state);
}

/** As pulseten_explain().
@param[in]   db    database
@param[in]   sql   a single SELECT, NUL-terminated
@param[out]  plan  the plan; free it with pulseten_free()
@param[out]  msg   message on failure
@return the status */
pulseten_state_t pt_go_explain(pulseten_db_t *db, const char *sql, char **plan,
                               char **msg) {
  const pulseten_state_t state = pulseten_explain(db, sql, plan);
  *msg = pt_go_msg(state);

  return (state);
}
