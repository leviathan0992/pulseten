/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * shim.h
 *    C calls for the Go package that also copy the error message.
 *
 * pulseten_error_message() belongs to the calling thread, and a goroutine may
 * move to another thread between two cgo calls. Each call below therefore reads
 * the message in the same C call as the function it wraps. The message is
 * NULL on success, otherwise a malloc() copy the caller frees.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PULSETEN_GO_SHIM_H
#define PULSETEN_GO_SHIM_H

#include "pulseten.h"

#include <stdint.h>

/** Allocate a C buffer, returning NULL on out of memory. */
void *pt_go_malloc(size_t size);

/** As pulseten_open(). */
pulseten_status_t pt_go_open(const char *dir, const pulseten_options_t *options,
                             pulseten_db_t **db, char **msg);

/** As pulseten_drop_database(). */
pulseten_status_t pt_go_drop_database(const char *dir, char **msg);

/** As pulseten_drop_table(). */
pulseten_status_t pt_go_drop_table(pulseten_db_t *db, const char *name,
                                   char **msg);

/** As pulseten_create_table(). */
pulseten_status_t pt_go_create_table(
    pulseten_db_t *db, const char *name,
    const pulseten_create_table_options_t *options, char **msg);

/** As pulseten_set_table_options(). */
pulseten_status_t pt_go_set_table_options(
    pulseten_db_t *db, const char *name,
    const pulseten_table_options_t *options, char **msg);

/** As pulseten_write().
@param[in]  table_name  table name, NUL-terminated */
pulseten_status_t pt_go_write(pulseten_db_t *db, const char *table_name,
                              const pulseten_point_t *points, uint32_t n_points,
                              char **msg);

/** As pulseten_catalog_snapshot(). */
pulseten_status_t pt_go_catalog(pulseten_db_t *db, pulseten_catalog_t **catalog,
                                char **msg);

/** As pulseten_exec_with_params(). */
pulseten_status_t pt_go_exec(pulseten_db_t *db, const char *sql,
                             const pulseten_param_t *params, uint32_t n_params,
                             pulseten_cancel_t *cancel, char **msg);

/** As pulseten_query_with_params().
@param[in]  sql  a single SELECT, NUL-terminated */
pulseten_status_t pt_go_query(pulseten_db_t *db, const char *sql,
                              const pulseten_param_t *params, uint32_t n_params,
                              pulseten_cancel_t *cancel,
                              pulseten_result_t **result, char **msg);

/** As pulseten_cancel_create(). */
pulseten_status_t pt_go_cancel_create(pulseten_cancel_t **cancel, char **msg);

/** As pulseten_result_fetch(). */
pulseten_status_t pt_go_result_fetch(pulseten_result_t *result,
                                     uint32_t *n_rows, char **msg);

/** As pulseten_explain_with_params().
@param[in]   sql   a single SELECT, NUL-terminated
@param[out]  plan  the plan; free it with pulseten_free() */
pulseten_status_t pt_go_explain(pulseten_db_t *db, const char *sql,
                                const pulseten_param_t *params,
                                uint32_t n_params, char **plan, char **msg);

/** As pulseten_copy_background_error().
@param[out]  message  the message or NULL; free it with pulseten_free() */
pulseten_status_t pt_go_copy_background_error(pulseten_db_t *db, char **message,
                                              char **msg);

#endif /* PULSETEN_GO_SHIM_H */
