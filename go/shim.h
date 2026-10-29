/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * shim.h
 *    C calls for the Go package that also copy the error message.
 *
 * pulseten_errmsg() belongs to the calling thread, and a goroutine may move
 * to another thread between two cgo calls. Each call below therefore reads
 * the message in the same C call as the function it wraps. The message is
 * NULL on success, otherwise a malloc() copy the caller frees.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PULSETEN_GO_SHIM_H
#define PULSETEN_GO_SHIM_H

#include <pulseten.h>

/** As pulseten_open().
@param[in]   dir      directory path
@param[in]   options  settings
@param[out]  db       open database
@param[out]  msg      message on failure
@return the status */
pulseten_state_t pt_go_open(const char *dir, const pulseten_options_t *options,
                            pulseten_db_t **db, char **msg);

/** As pulseten_close().
@param[in]   db   database
@param[out]  msg  message on failure
@return the status */
pulseten_state_t pt_go_close(pulseten_db_t *db, char **msg);

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
                             uint64_t *seq, char **msg);

/** As pulseten_flush().
@param[in]   db   database
@param[out]  msg  message on failure
@return the status */
pulseten_state_t pt_go_flush(pulseten_db_t *db, char **msg);

/** As pulseten_expire().
@param[in]   db       database
@param[out]  removed  partitions dropped
@param[out]  msg      message on failure
@return the status */
pulseten_state_t pt_go_expire(pulseten_db_t *db, uint64_t *removed, char **msg);

/** As pulseten_catalog_get().
@param[in]   db       database
@param[out]  catalog  the snapshot
@param[out]  msg      message on failure
@return the status */
pulseten_state_t pt_go_catalog_get(pulseten_db_t *db,
                                   pulseten_catalog_t **catalog, char **msg);

/** As pulseten_drop_table().
@param[in]   db     database
@param[in]   table  table name, NUL-terminated
@param[out]  msg    message on failure
@return the status */
pulseten_state_t pt_go_drop_table(pulseten_db_t *db, const char *table,
                                  char **msg);

/** As pulseten_query().
@param[in]   db      database
@param[in]   sql     a single SELECT, NUL-terminated
@param[out]  result  the result
@param[out]  msg     message on failure
@return the status */
pulseten_state_t pt_go_query(pulseten_db_t *db, const char *sql,
                             pulseten_result_t **result, char **msg);

/** As pulseten_fetch().
@param[in]   result  result
@param[out]  n_rows  rows in the chunk
@param[out]  msg     message on failure
@return the status */
pulseten_state_t pt_go_fetch(pulseten_result_t *result, uint32_t *n_rows,
                             char **msg);

/** As pulseten_explain().
@param[in]   db    database
@param[in]   sql   a single SELECT, NUL-terminated
@param[out]  plan  the plan; free it with pulseten_free()
@param[out]  msg   message on failure
@return the status */
pulseten_state_t pt_go_explain(pulseten_db_t *db, const char *sql, char **plan,
                               char **msg);

#endif
