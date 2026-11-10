/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * api-t.c
 *    Tests for the C API, written in C so that pulseten.h is checked as a
 *    C header: opening, writing, reading results chunk by chunk, result
 *    snapshots, errors, the table catalog, rejected table drops, expiry,
 *    interrupting queries and reopening.
 *
 *-------------------------------------------------------------------------
 */

/** POSIX.1-2008 with XSI: nftw() and mkdtemp(). */
#define _XOPEN_SOURCE 700

/** mkdtemp() on macOS. */
#define _DARWIN_C_SOURCE

#include "pulseten.h"

#include <ftw.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;

#define CHECK(cond)                                                   \
  do {                                                                \
    if (!(cond)) {                                                    \
      fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
      failures++;                                                     \
    }                                                                 \
  } while (0)

/** 2026-01-01 00:00:00 UTC in microseconds. */
#define T0 1767225600000000LL

/** One second in microseconds. */
#define SECOND 1000000LL

/** Scratch directory of the test, under $TMPDIR or /tmp. */
static char scratch[512];

/** nftw() callback: remove one file or directory.
@return the result of removing the entry; 0 on success, nonzero to stop the
  walk */
static int remove_entry(const char *path, const struct stat *st, int flag,
                        struct FTW *ftw) {
  (void)st;
  (void)flag;
  (void)ftw;

  return (remove(path));
}

/** Create the scratch directory of the test. */
static void scratch_create(void) {
  const char *tmp = getenv("TMPDIR");

  snprintf(scratch, sizeof(scratch), "%s/pulseten-api-XXXXXX",
           tmp != NULL ? tmp : "/tmp");

  if (mkdtemp(scratch) == NULL) {
    perror("mkdtemp");
    exit(2);
  }
}

/** Remove the scratch directory and everything in it. */
static void scratch_remove(void) {
  nftw(scratch, remove_entry, 16, FTW_DEPTH | FTW_PHYS);
}

/** Build a path within the test's scratch directory.
@return a path inside the scratch directory */
static const char *path_of(const char *name) {
  static char path[600];

  snprintf(path, sizeof(path), "%s/%s", scratch, name);

  return (path);
}

/** Open a database in the scratch directory, checking that it opens.
@return the database */
static pulseten_db_t *open_db(const char *name) {
  pulseten_options_t options;
  pulseten_db_t *db = NULL;

  pulseten_options_init(&options);
  options.m_flush_interval_ms = 20;

  CHECK(pulseten_open(path_of(name), &options, &db) == PULSETEN_OK);
  CHECK(db != NULL);

  return (db);
}

/** Wait for the background thread to store all currently visible writes.
@return nonzero if the writes were stored before the timeout */
static int wait_until_flushed(pulseten_db_t *db) {
  pulseten_stats_t stats;
  pulseten_get_stats(db, &stats);
  const uint64_t target = stats.m_visible_seq;
  const struct timespec delay = {0, 10 * 1000000};

  for (int waited = 0; waited < 5000; waited += 10) {
    pulseten_get_stats(db, &stats);
    if (stats.m_flushed_seq >= target) {
      return (1);
    }
    nanosleep(&delay, NULL);
  }

  return (0);
}

/** Write one cpu point: host tag, usage DOUBLE, cores BIGINT, up BOOLEAN,
note VARCHAR.
@param[in]  time_us  UTC timestamp in microseconds since 1970-01-01
@return the status from pulseten_write() */
static pulseten_status_t write_cpu(pulseten_db_t *db, int64_t time_us,
                                   const char *host, double usage) {
  pulseten_tag_t tag;
  pulseten_field_t fields[4];
  pulseten_point_t point;

  tag.m_name = "host";
  tag.m_name_len = 4;
  tag.m_value = host;
  tag.m_value_len = strlen(host);

  fields[0].m_name = "usage";
  fields[0].m_name_len = 5;
  fields[0].m_type = PULSETEN_TYPE_DOUBLE;
  fields[0].m_value.m_d = usage;

  fields[1].m_name = "cores";
  fields[1].m_name_len = 5;
  fields[1].m_type = PULSETEN_TYPE_BIGINT;
  fields[1].m_value.m_i = 8;

  fields[2].m_name = "up";
  fields[2].m_name_len = 2;
  fields[2].m_type = PULSETEN_TYPE_BOOLEAN;
  fields[2].m_value.m_b = 1;

  fields[3].m_name = "note";
  fields[3].m_name_len = 4;
  fields[3].m_type = PULSETEN_TYPE_VARCHAR;
  fields[3].m_value.m_str.m_s = "ok";
  fields[3].m_value.m_str.m_len = 2;

  point.m_time_us = time_us;
  point.m_tags = &tag;
  point.m_n_tags = 1;
  point.m_fields = fields;
  point.m_n_fields = 4;

  return (pulseten_write(db, "cpu", &point, 1));
}

/** Write n points of one BIGINT field v in one batch.
@param[in]  from  first row or id to generate, inclusive */
static void write_many(pulseten_db_t *db, const char *table, int64_t from,
                       uint32_t n) {
  pulseten_field_t *fields = malloc(n * sizeof(pulseten_field_t));
  pulseten_point_t *points = malloc(n * sizeof(pulseten_point_t));
  uint32_t i;

  for (i = 0; i < n; ++i) {
    fields[i].m_name = "v";
    fields[i].m_name_len = 1;
    fields[i].m_type = PULSETEN_TYPE_BIGINT;
    fields[i].m_value.m_i = (int64_t)i;

    points[i].m_time_us = T0 + (from + (int64_t)i) * SECOND;
    points[i].m_tags = NULL;
    points[i].m_n_tags = 0;
    points[i].m_fields = &fields[i];
    points[i].m_n_fields = 1;
  }

  CHECK(pulseten_write(db, table, points, n) == PULSETEN_OK);

  free(fields);
  free(points);
}

/** Execute a query and read its single integer result.
@return the single BIGINT a query returns, or -1 */
static int64_t query_int(pulseten_db_t *db, const char *sql) {
  pulseten_result_t *result = NULL;
  uint32_t n = 0;
  int64_t value = -1;

  if (pulseten_query(db, sql, &result) != PULSETEN_OK) {
    fprintf(stderr, "query failed: %s: %s\n", sql, pulseten_errmsg());

    return (-1);
  }

  if (pulseten_result_fetch(result, &n) == PULSETEN_OK && n == 1) {
    value = pulseten_result_get_int64(result, 0, 0);
  }

  pulseten_result_destroy(result);

  return (value);
}

/** Fetch all chunks and count their rows.
@param[in,out]  result  cursor whose remaining chunks are consumed
@return the rows a query returns, reading every chunk; counts chunks */
static uint64_t count_rows(pulseten_result_t *result, int *chunks) {
  uint64_t rows = 0;
  uint32_t n = 0;

  *chunks = 0;

  while (pulseten_result_fetch(result, &n) == PULSETEN_OK && n > 0) {
    rows += n;
    (*chunks)++;
  }

  return (rows);
}

/** pulseten_options_init() fills in the defaults. */
static void test_options(void) {
  pulseten_options_t options;
  pulseten_options_init(&options);

  CHECK(options.m_flush_interval_ms == 200);
  CHECK(options.m_partition_days == 30);
  CHECK(options.m_retention_days == 0);
  CHECK(options.m_wal_sync == 0);
  CHECK(options.m_memtable_limit_bytes > 0);
}

/** Every type written comes back through SQL with its value, and missing
columns read NULL. */
static void test_write_and_read(void) {
  pulseten_db_t *db = open_db("rw");
  pulseten_result_t *result = NULL;
  uint32_t n = 0;
  size_t len = 0;

  CHECK(write_cpu(db, T0, "a", 0.5) == PULSETEN_OK);
  CHECK(write_cpu(db, T0 + SECOND, "b", 1.5) == PULSETEN_OK);

  /* A later point without note: NULL there. */
  {
    pulseten_field_t f;
    pulseten_point_t p;

    f.m_name = "usage";
    f.m_name_len = 5;
    f.m_type = PULSETEN_TYPE_DOUBLE;
    f.m_value.m_d = 2.5;

    p.m_time_us = T0 + 2 * SECOND;
    p.m_tags = NULL;
    p.m_n_tags = 0;
    p.m_fields = &f;
    p.m_n_fields = 1;

    CHECK(pulseten_write(db, "CPU", &p, 1) == PULSETEN_OK);
  }

  CHECK(pulseten_query(db,
                       "SELECT time, host, usage, cores, up, note FROM cpu "
                       "ORDER BY time",
                       &result) == PULSETEN_OK);

  CHECK(pulseten_result_column_count(result) == 6);
  CHECK(strcmp(pulseten_result_column_name(result, 0), "time") == 0);
  CHECK(strcmp(pulseten_result_column_name(result, 5), "note") == 0);
  CHECK(pulseten_result_column_name(result, 6) == NULL);
  CHECK(pulseten_result_column_type(result, 0) == PULSETEN_TYPE_TIMESTAMP);
  CHECK(pulseten_result_column_type(result, 1) == PULSETEN_TYPE_VARCHAR);
  CHECK(pulseten_result_column_type(result, 2) == PULSETEN_TYPE_DOUBLE);
  CHECK(pulseten_result_column_type(result, 3) == PULSETEN_TYPE_BIGINT);
  CHECK(pulseten_result_column_type(result, 4) == PULSETEN_TYPE_BOOLEAN);

  CHECK(pulseten_result_fetch(result, &n) == PULSETEN_OK);
  CHECK(n == 3);

  if (n == 3) {
    CHECK(pulseten_result_get_int64(result, 0, 0) == T0);
    CHECK(strcmp(pulseten_result_get_varchar(result, 1, 0, &len), "a") == 0);
    CHECK(len == 1);
    CHECK(pulseten_result_get_double(result, 2, 1) == 1.5);
    CHECK(pulseten_result_get_int64(result, 3, 0) == 8);
    CHECK(pulseten_result_get_bool(result, 4, 0) == 1);
    CHECK(strcmp(pulseten_result_get_varchar(result, 5, 1, NULL), "ok") == 0);

    /* The third point has no host, cores, up or note. */
    CHECK(pulseten_result_is_null(result, 1, 2));
    CHECK(pulseten_result_is_null(result, 5, 2));
    CHECK(pulseten_result_get_varchar(result, 5, 2, &len) == NULL && len == 0);
    CHECK(!pulseten_result_is_null(result, 2, 2));

    /* Wrong type or outside the chunk: zero values. */
    CHECK(pulseten_result_get_double(result, 3, 0) == 0);
    CHECK(pulseten_result_get_varchar(result, 2, 0, NULL) == NULL);
    CHECK(pulseten_result_get_int64(result, 0, 3) == 0);
    CHECK(pulseten_result_is_null(result, 9, 0));
  }

  /* The end, and staying at the end. */
  CHECK(pulseten_result_fetch(result, &n) == PULSETEN_OK && n == 0);
  CHECK(pulseten_result_fetch(result, &n) == PULSETEN_OK && n == 0);
  pulseten_result_destroy(result);

  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** Results larger than a chunk, and than the engine's materialized limit, are
read chunk by chunk. */
static void test_chunks(void) {
  pulseten_db_t *db = open_db("chunks");
  pulseten_result_t *result = NULL;
  int chunks = 0;

  /* 10000 rows stored and 5000 in memory: several chunks. */
  write_many(db, "m", 0, 10000);
  CHECK(wait_until_flushed(db));
  write_many(db, "m", 10000, 5000);

  CHECK(pulseten_query(db, "SELECT time, v FROM m", &result) == PULSETEN_OK);
  CHECK(count_rows(result, &chunks) == 15000);
  CHECK(chunks >= 8);
  pulseten_result_destroy(result);

  /* More rows than the materialized limit of the engine are fine. */
  CHECK(query_int(db, "SELECT count(*) FROM m") == 15000);

  /* A result destroyed before its end. */
  CHECK(pulseten_query(db, "SELECT * FROM m", &result) == PULSETEN_OK);
  {
    uint32_t n = 0;
    CHECK(pulseten_result_fetch(result, &n) == PULSETEN_OK && n > 0);
  }
  pulseten_result_destroy(result);

  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** A result keeps the snapshot it started with, whatever is written or
flushed after. */
static void test_result_snapshot(void) {
  pulseten_db_t *db = open_db("snapshot");
  pulseten_result_t *result = NULL;
  pulseten_stats_t stats;
  int chunks = 0;

  write_many(db, "m", 0, 3000);

  /* The result sees the 3000 rows, not the writes and the flush after
  it started, though they happen before its rows are fetched. */
  CHECK(pulseten_query(db, "SELECT v FROM m", &result) == PULSETEN_OK);

  write_many(db, "m", 3000, 3000);
  CHECK(wait_until_flushed(db));
  write_many(db, "m", 6000, 100);

  CHECK(count_rows(result, &chunks) == 3000);
  pulseten_result_destroy(result);

  CHECK(query_int(db, "SELECT count(*) FROM m") == 6100);

  pulseten_get_stats(db, &stats);
  CHECK(stats.m_visible_seq == 3);
  CHECK(stats.m_flushed_seq >= 2 && stats.m_flushed_seq <= stats.m_visible_seq);
  CHECK(stats.m_background_flushes > 0);
  CHECK(stats.m_wal_segments >= 1);

  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** Refused statements, SQL errors, rejected writes and NULL arguments return
the right status and a message. */
static void test_errors(void) {
  pulseten_db_t *db = open_db("errors");
  pulseten_db_t *again = NULL;
  pulseten_result_t *result = NULL;
  char *plan = NULL;

  CHECK(write_cpu(db, T0, "a", 0.5) == PULSETEN_OK);
  CHECK(strcmp(pulseten_errmsg(), "") == 0);

  /* Refused statements and SQL errors. */
  CHECK(pulseten_query(db, "DELETE FROM cpu", &result) ==
        PULSETEN_INVALID_ARGUMENT);
  CHECK(result == NULL);
  CHECK(strlen(pulseten_errmsg()) > 0);

  CHECK(pulseten_query(db, "SELECT nothing FROM cpu", &result) ==
        PULSETEN_ERROR);
  CHECK(strlen(pulseten_errmsg()) > 0);

  CHECK(pulseten_query(db, "SELECT (", &result) == PULSETEN_INVALID_ARGUMENT);
  CHECK(strlen(pulseten_errmsg()) > 0);

  CHECK(pulseten_query(db, "SELECT [1, 2]", &result) ==
        PULSETEN_INVALID_ARGUMENT);

  /* An error while running shows up in fetch. */
  CHECK(pulseten_query(db, "SELECT error('boom') FROM cpu", &result) ==
            PULSETEN_OK ||
        result == NULL);
  if (result != NULL) {
    uint32_t n = 0;
    CHECK(pulseten_result_fetch(result, &n) == PULSETEN_ERROR && n == 0);
    CHECK(strstr(pulseten_errmsg(), "boom") != NULL);
    pulseten_result_destroy(result);
    result = NULL;
  }

  /* Rejected writes. */
  {
    pulseten_field_t f;
    pulseten_point_t p;

    f.m_name = "usage";
    f.m_name_len = 5;
    f.m_type = PULSETEN_TYPE_BIGINT;
    f.m_value.m_i = 1;

    p.m_time_us = T0;
    p.m_tags = NULL;
    p.m_n_tags = 0;
    p.m_fields = &f;
    p.m_n_fields = 1;

    CHECK(pulseten_write(db, "cpu", &p, 1) == PULSETEN_SCHEMA_CONFLICT);

    f.m_type = PULSETEN_TYPE_TIMESTAMP;
    CHECK(pulseten_write(db, "cpu", &p, 1) == PULSETEN_INVALID_ARGUMENT);

    f.m_type = PULSETEN_TYPE_BIGINT;
    f.m_name = "other";
    f.m_name_len = 5;
    CHECK(pulseten_write(db, "_pt_x", &p, 1) == PULSETEN_INVALID_ARGUMENT);
    CHECK(pulseten_write(db, "cpu", &p, 0) == PULSETEN_INVALID_ARGUMENT);
    CHECK(pulseten_write(db, NULL, &p, 1) == PULSETEN_INVALID_ARGUMENT);
  }

  /* NULL arguments. */
  CHECK(pulseten_query(NULL, "SELECT 1", &result) == PULSETEN_INVALID_ARGUMENT);
  CHECK(pulseten_result_fetch(NULL, NULL) == PULSETEN_INVALID_ARGUMENT);
  pulseten_result_destroy(NULL);

  /* A second open of the same directory. */
  CHECK(pulseten_open(path_of("errors"), NULL, &again) == PULSETEN_LOCKED);
  CHECK(again == NULL);

  /* A parent that does not exist. */
  CHECK(pulseten_open(path_of("no/such/dir"), NULL, &again) ==
        PULSETEN_IO_ERROR);

  /* EXPLAIN. */
  CHECK(pulseten_explain(db, "SELECT count(*) FROM cpu", &plan) == PULSETEN_OK);
  CHECK(plan != NULL && strlen(plan) > 0);
  pulseten_free(plan);

  /* No background failure. */
  char sentinel = 0;
  char *background = &sentinel;
  CHECK(pulseten_copy_background_error(db, &background) == PULSETEN_OK);
  CHECK(background == NULL);
  CHECK(pulseten_copy_background_error(db, NULL) == PULSETEN_INVALID_ARGUMENT);
  CHECK(pulseten_copy_background_error(NULL, &background) ==
        PULSETEN_INVALID_ARGUMENT);

  /* A success clears the message. */
  CHECK(query_int(db, "SELECT count(*) FROM cpu") == 1);
  CHECK(strcmp(pulseten_errmsg(), "") == 0);

  CHECK(strcmp(pulseten_status_name(PULSETEN_LOCKED), "LOCKED") == 0);
  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** A write older than the retention period is rejected. */
static void test_retention(void) {
  pulseten_options_t options;
  pulseten_db_t *db = NULL;
  pulseten_stats_t stats;

  pulseten_options_init(&options);
  options.m_retention_days = 7;

  CHECK(pulseten_open(path_of("retention"), &options, &db) == PULSETEN_OK);

  /* 2026-01-01 is long past a 7-day retention period. */
  CHECK(write_cpu(db, T0, "a", 0.5) == PULSETEN_INVALID_ARGUMENT);
  CHECK(strstr(pulseten_errmsg(), "retention") != NULL);

  pulseten_get_stats(db, &stats);
  CHECK(stats.m_expired_partitions == 0);
  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** The library's version matches the header's. */
static void test_version(void) {
  CHECK(strcmp(pulseten_version(), PULSETEN_VERSION_STRING) == 0);
  CHECK(pulseten_version_number() == PULSETEN_VERSION_NUMBER);
  CHECK(PULSETEN_VERSION_NUMBER == PULSETEN_VERSION_MAJOR * 10000 +
                                       PULSETEN_VERSION_MINOR * 100 +
                                       PULSETEN_VERSION_PATCH);
  CHECK(strcmp(pulseten_status_name(PULSETEN_NOT_FOUND), "NOT_FOUND") == 0);
  CHECK(strcmp(pulseten_status_name(PULSETEN_INTERRUPTED), "INTERRUPTED") == 0);
}

/** The limits in pulseten.h are the ones enforced. */
static void test_limits(void) {
  pulseten_db_t *db = open_db("limits");
  pulseten_field_t f;
  pulseten_point_t *points =
      malloc((PULSETEN_MAX_POINTS + 1) * sizeof(pulseten_point_t));
  char name[PULSETEN_MAX_NAME_LEN + 2];
  uint32_t i;

  f.m_name = "v";
  f.m_name_len = 1;
  f.m_type = PULSETEN_TYPE_BIGINT;
  f.m_value.m_i = 1;

  for (i = 0; i <= PULSETEN_MAX_POINTS; ++i) {
    points[i].m_time_us = T0 + i;
    points[i].m_tags = NULL;
    points[i].m_n_tags = 0;
    points[i].m_fields = &f;
    points[i].m_n_fields = 1;
  }

  /* The limits in the header are the ones enforced. */
  CHECK(pulseten_write(db, "m", points, PULSETEN_MAX_POINTS) == PULSETEN_OK);
  CHECK(pulseten_write(db, "m", points, PULSETEN_MAX_POINTS + 1) ==
        PULSETEN_LIMIT_EXCEEDED);

  points[0].m_time_us = PULSETEN_MAX_TIME_US;
  CHECK(pulseten_write(db, "m", points, 1) == PULSETEN_INVALID_ARGUMENT);
  points[0].m_time_us = PULSETEN_MAX_TIME_US - 1;
  CHECK(pulseten_write(db, "m", points, 1) == PULSETEN_OK);

  for (i = 0; i <= PULSETEN_MAX_BATCH_DAYS; ++i) {
    points[i].m_time_us = T0 + (int64_t)i * 86400 * SECOND;
  }
  CHECK(pulseten_write(db, "m", points, PULSETEN_MAX_BATCH_DAYS) ==
        PULSETEN_OK);
  CHECK(pulseten_write(db, "m", points, PULSETEN_MAX_BATCH_DAYS + 1) ==
        PULSETEN_LIMIT_EXCEEDED);

  memset(name, 'a', sizeof(name));
  name[PULSETEN_MAX_NAME_LEN] = '\0';
  CHECK(pulseten_write(db, name, points, 1) == PULSETEN_OK);
  name[PULSETEN_MAX_NAME_LEN] = 'a';
  name[PULSETEN_MAX_NAME_LEN + 1] = '\0';
  CHECK(pulseten_write(db, name, points, 1) == PULSETEN_INVALID_ARGUMENT);

  free(points);
  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** The catalog lists stored and unflushed tables with their columns, as a
snapshot. */
static void test_catalog(void) {
  pulseten_db_t *db = open_db("catalog");
  pulseten_catalog_t *catalog = NULL;
  const pulseten_column_t *col;
  uint32_t t = 99;

  /* Empty. */
  CHECK(pulseten_catalog_snapshot(db, &catalog) == PULSETEN_OK);
  CHECK(pulseten_catalog_table_count(catalog) == 0);
  CHECK(pulseten_catalog_find_table(catalog, "cpu", &t) == PULSETEN_NOT_FOUND);
  pulseten_catalog_destroy(catalog);

  /* One stored table and one only in memory. */
  CHECK(write_cpu(db, T0, "a", 0.5) == PULSETEN_OK);
  CHECK(wait_until_flushed(db));
  write_many(db, "mem", 0, 3);

  CHECK(pulseten_catalog_snapshot(db, &catalog) == PULSETEN_OK);
  CHECK(pulseten_catalog_table_count(catalog) == 2);
  CHECK(strcmp(pulseten_catalog_table_name(catalog, 0), "cpu") == 0);
  CHECK(strcmp(pulseten_catalog_table_name(catalog, 1), "mem") == 0);
  CHECK(pulseten_catalog_table_name(catalog, 2) == NULL);

  CHECK(pulseten_catalog_find_table(catalog, "CPU", &t) == PULSETEN_OK &&
        t == 0);
  CHECK(pulseten_catalog_find_table(catalog, "Mem", &t) == PULSETEN_OK &&
        t == 1);

  CHECK(pulseten_catalog_column_count(catalog, 0) == 6);
  col = pulseten_catalog_column(catalog, 0, 0);
  CHECK(col != NULL && strcmp(col->m_name, "time") == 0 &&
        col->m_kind == PULSETEN_COLUMN_TIME &&
        col->m_type == PULSETEN_TYPE_TIMESTAMP);
  col = pulseten_catalog_column(catalog, 0, 1);
  CHECK(col != NULL && strcmp(col->m_name, "host") == 0 &&
        col->m_kind == PULSETEN_COLUMN_TAG &&
        col->m_type == PULSETEN_TYPE_VARCHAR);
  col = pulseten_catalog_column(catalog, 0, 2);
  CHECK(col != NULL && strcmp(col->m_name, "usage") == 0 &&
        col->m_kind == PULSETEN_COLUMN_FIELD &&
        col->m_type == PULSETEN_TYPE_DOUBLE);
  col = pulseten_catalog_column(catalog, 0, 5);
  CHECK(col != NULL && strcmp(col->m_name, "note") == 0 &&
        col->m_type == PULSETEN_TYPE_VARCHAR);
  CHECK(pulseten_catalog_column(catalog, 0, 6) == NULL);
  CHECK(pulseten_catalog_column(catalog, 2, 0) == NULL);
  CHECK(pulseten_catalog_column_count(catalog, 2) == 0);

  col = pulseten_catalog_column(catalog, 1, 1);
  CHECK(col != NULL && strcmp(col->m_name, "v") == 0 &&
        col->m_type == PULSETEN_TYPE_BIGINT);

  /* A snapshot: a later column is not in it. */
  {
    pulseten_field_t f;
    pulseten_point_t p;

    f.m_name = "extra";
    f.m_name_len = 5;
    f.m_type = PULSETEN_TYPE_BOOLEAN;
    f.m_value.m_b = 1;

    p.m_time_us = T0;
    p.m_tags = NULL;
    p.m_n_tags = 0;
    p.m_fields = &f;
    p.m_n_fields = 1;
    CHECK(pulseten_write(db, "mem", &p, 1) == PULSETEN_OK);
  }
  CHECK(pulseten_catalog_column_count(catalog, 1) == 2);
  pulseten_catalog_destroy(catalog);

  CHECK(pulseten_catalog_snapshot(db, &catalog) == PULSETEN_OK);
  CHECK(pulseten_catalog_column_count(catalog, 1) == 3);
  pulseten_catalog_destroy(catalog);

  CHECK(pulseten_catalog_snapshot(NULL, &catalog) == PULSETEN_INVALID_ARGUMENT);
  CHECK(catalog == NULL);
  CHECK(pulseten_catalog_table_count(NULL) == 0);
  pulseten_catalog_destroy(NULL);
  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** Users cannot drop managed time-series tables or replace their schemas. */
static void test_drop_table(void) {
  pulseten_db_t *db = open_db("drop");
  pulseten_catalog_t *catalog = NULL;
  pulseten_result_t *result = NULL;
  uint32_t t = 0;

  write_many(db, "m", 0, 100);
  CHECK(wait_until_flushed(db));
  write_many(db, "m", 100, 50);
  write_many(db, "k", 0, 10);

  CHECK(pulseten_drop_table(db, "M") == PULSETEN_INVALID_ARGUMENT);
  CHECK(strcmp(pulseten_errmsg(), "time-series tables cannot be dropped") == 0);
  CHECK(query_int(db, "SELECT count(*) FROM m") == 150);
  CHECK(query_int(db, "SELECT count(*) FROM k") == 10);
  CHECK(pulseten_query(db, "DROP TABLE m", &result) ==
        PULSETEN_INVALID_ARGUMENT);
  CHECK(result == NULL);

  CHECK(pulseten_catalog_snapshot(db, &catalog) == PULSETEN_OK);
  CHECK(pulseten_catalog_table_count(catalog) == 2);
  CHECK(pulseten_catalog_find_table(catalog, "m", &t) == PULSETEN_OK);
  pulseten_catalog_destroy(catalog);

  CHECK(pulseten_drop_table(db, "missing") == PULSETEN_NOT_FOUND);
  CHECK(strlen(pulseten_errmsg()) > 0);
  CHECK(pulseten_drop_table(db, "_pt_state") == PULSETEN_INVALID_ARGUMENT);
  CHECK(pulseten_drop_table(NULL, "m") == PULSETEN_INVALID_ARGUMENT);
  CHECK(pulseten_drop_table(db, NULL) == PULSETEN_INVALID_ARGUMENT);

  /* The rejected drop did not release the original column type. */
  {
    pulseten_field_t f;
    pulseten_point_t p;

    f.m_name = "v";
    f.m_name_len = 1;
    f.m_type = PULSETEN_TYPE_VARCHAR;
    f.m_value.m_str.m_s = "x";
    f.m_value.m_str.m_len = 1;

    p.m_time_us = T0;
    p.m_tags = NULL;
    p.m_n_tags = 0;
    p.m_fields = &f;
    p.m_n_fields = 1;
    CHECK(pulseten_write(db, "m", &p, 1) == PULSETEN_SCHEMA_CONFLICT);
  }
  CHECK(pulseten_close(db) == PULSETEN_OK);

  db = open_db("drop");
  CHECK(query_int(db, "SELECT count(*) FROM m") == 150);
  CHECK(query_int(db, "SELECT count(*) FROM k") == 10);
  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** The background thread flushes writes and drops expired partitions. */
static void test_expire(void) {
  pulseten_options_t options;
  pulseten_db_t *db = NULL;
  pulseten_stats_t stats;
  const int64_t now = (int64_t)time(NULL) * SECOND;

  pulseten_options_init(&options);
  options.m_flush_interval_ms = 20;
  options.m_partition_days = 1;

  /* Old data written while there is no retention period. */
  CHECK(pulseten_open(path_of("expire"), &options, &db) == PULSETEN_OK);
  write_many(db, "m", 0, 10);
  CHECK(write_cpu(db, now, "a", 0.5) == PULSETEN_OK);
  CHECK(wait_until_flushed(db));
  pulseten_get_stats(db, &stats);
  CHECK(stats.m_expired_partitions == 0);
  CHECK(pulseten_close(db) == PULSETEN_OK);

  /* Reopening with retention lets the background thread remove old data. */
  options.m_retention_days = 7;
  CHECK(pulseten_open(path_of("expire"), &options, &db) == PULSETEN_OK);
  const struct timespec delay = {0, 10 * 1000000};
  for (int waited = 0; waited < 5000; waited += 10) {
    pulseten_get_stats(db, &stats);
    if (stats.m_expired_partitions >= 1) {
      break;
    }
    nanosleep(&delay, NULL);
  }
  CHECK(query_int(db, "SELECT count(*) FROM m") == 0);
  CHECK(query_int(db, "SELECT count(*) FROM cpu") == 1);

  pulseten_get_stats(db, &stats);
  CHECK(stats.m_expired_partitions == 1);

  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** Interrupts a result after a delay. */
struct interrupter {
  /** Result to interrupt. */
  pulseten_result_t *m_result;

  /** Milliseconds to wait first. */
  int m_delay_ms;
};

/** Thread: wait, then interrupt a result.
@param[in]  arg  result to interrupt after the test delay
@return NULL after requesting the interrupt */
static void *interrupt_main(void *arg) {
  struct interrupter *it = arg;
  struct timespec delay;

  delay.tv_sec = it->m_delay_ms / 1000;
  delay.tv_nsec = (long)(it->m_delay_ms % 1000) * 1000000L;
  nanosleep(&delay, NULL);
  pulseten_result_interrupt(it->m_result);

  return (NULL);
}

/** Read the monotonic clock in seconds.
@return the monotonic clock in seconds */
static double now_seconds(void) {
  struct timespec ts;

  clock_gettime(CLOCK_MONOTONIC, &ts);

  return ((double)ts.tv_sec + (double)ts.tv_nsec / 1e9);
}

/** A query can be interrupted while it runs, before it runs, and not after it
ended. */
static void test_interrupt(void) {
  pulseten_db_t *db = open_db("interrupt");
  pulseten_result_t *result = NULL;
  struct interrupter it;
  pthread_t thread;
  uint32_t n = 99;
  double start;

  write_many(db, "m", 0, 3000);
  CHECK(wait_until_flushed(db));

  /* About 2.7e10 rows to aggregate: minutes, unless stopped. */
  CHECK(pulseten_query(db, "SELECT sum(a.v * b.v + c.v) FROM m a, m b, m c",
                       &result) == PULSETEN_OK);

  it.m_result = result;
  it.m_delay_ms = 200;
  start = now_seconds();
  CHECK(pthread_create(&thread, NULL, interrupt_main, &it) == 0);

  CHECK(pulseten_result_fetch(result, &n) == PULSETEN_INTERRUPTED && n == 0);
  CHECK(now_seconds() - start < 10);
  pthread_join(thread, NULL);

  /* Every later fetch says so too. */
  CHECK(pulseten_result_fetch(result, &n) == PULSETEN_INTERRUPTED);
  CHECK(strlen(pulseten_errmsg()) > 0);
  pulseten_result_destroy(result);

  /* Before the first fetch. */
  CHECK(pulseten_query(db, "SELECT v FROM m", &result) == PULSETEN_OK);
  pulseten_result_interrupt(result);
  CHECK(pulseten_result_fetch(result, &n) == PULSETEN_INTERRUPTED);
  pulseten_result_destroy(result);

  /* After the last row it changes nothing. */
  CHECK(pulseten_query(db, "SELECT count(*) FROM m", &result) == PULSETEN_OK);
  CHECK(pulseten_result_fetch(result, &n) == PULSETEN_OK && n == 1);
  CHECK(pulseten_result_fetch(result, &n) == PULSETEN_OK && n == 0);
  pulseten_result_interrupt(result);
  CHECK(pulseten_result_fetch(result, &n) == PULSETEN_OK && n == 0);
  pulseten_result_destroy(result);
  pulseten_result_interrupt(NULL);

  /* The database goes on. */
  CHECK(query_int(db, "SELECT count(*) FROM m") == 3000);
  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** Stored and logged writes are both there after reopening. */
static void test_reopen(void) {
  pulseten_db_t *db = open_db("reopen");

  write_many(db, "m", 0, 100);
  CHECK(wait_until_flushed(db));
  write_many(db, "m", 100, 50);
  CHECK(pulseten_close(db) == PULSETEN_OK);

  /* Stored and logged writes are both back. */
  db = open_db("reopen");
  CHECK(query_int(db, "SELECT count(*) FROM m") == 150);
  CHECK(query_int(db, "SELECT max(v) FROM m") == 99);
  CHECK(pulseten_close(db) == PULSETEN_OK);

  CHECK(pulseten_close(NULL) == PULSETEN_OK);
}

/** A NULL output handle is rejected before creating a directory. */
static void test_open_requires_output(void) {
  const char *path = path_of("null-output");
  CHECK(access(path, F_OK) != 0);
  CHECK(pulseten_open(path, NULL, NULL) == PULSETEN_INVALID_ARGUMENT);
  CHECK(strcmp(pulseten_errmsg(), "out must not be NULL") == 0);
  CHECK(access(path, F_OK) != 0);
}

/** Both WAL sync modes keep writes visible and recover them after close. */
static void test_wal_sync_options(void) {
  for (int sync = 0; sync <= 1; ++sync) {
    pulseten_options_t options;
    pulseten_options_init(&options);
    options.m_wal_sync = sync;
    options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
    const char *path = path_of(sync ? "wal-sync" : "wal-page-cache");
    pulseten_db_t *db = NULL;
    CHECK(pulseten_open(path, &options, &db) == PULSETEN_OK);
    write_many(db, "m", 0, 3);
    CHECK(query_int(db, "SELECT count(*) FROM m") == 3);
    CHECK(pulseten_close(db) == PULSETEN_OK);

    /* The option is per open; reopening may select the other mode. */
    options.m_wal_sync = !sync;
    CHECK(pulseten_open(path, &options, &db) == PULSETEN_OK);
    CHECK(query_int(db, "SELECT count(*) FROM m") == 3);
    write_many(db, "m", 3, 2);
    CHECK(query_int(db, "SELECT count(*) FROM m") == 5);
    CHECK(pulseten_close(db) == PULSETEN_OK);
  }
}

/** Invalid options leave no directory, and a later valid open still works. */
static void test_open_invalid_options(void) {
  pulseten_options_t options;
  pulseten_db_t *db = NULL;
  const char *path = path_of("invalid-options");
  pulseten_options_init(&options);
  options.m_flush_interval_ms = 0;
  CHECK(pulseten_open(path, &options, &db) == PULSETEN_INVALID_ARGUMENT);
  CHECK(db == NULL && access(path, F_OK) != 0);
  pulseten_options_init(&options);
  options.m_retention_days = PULSETEN_MAX_RETENTION_DAYS + 1;
  CHECK(pulseten_open(path, &options, &db) == PULSETEN_INVALID_ARGUMENT);
  CHECK(db == NULL && access(path, F_OK) != 0);
  pulseten_options_init(&options);
  options.m_partition_days = 0;
  CHECK(pulseten_open(path, &options, &db) == PULSETEN_INVALID_ARGUMENT);
  CHECK(db == NULL && access(path, F_OK) != 0);
  options.m_partition_days = PULSETEN_MAX_PARTITION_DAYS + 1;
  CHECK(pulseten_open(path, &options, &db) == PULSETEN_INVALID_ARGUMENT);
  CHECK(db == NULL && access(path, F_OK) != 0);
  pulseten_options_init(&options);
  CHECK(pulseten_open(path, &options, &db) == PULSETEN_OK);
  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** Ordinary tables can be created, joined, persisted and dropped. */
static void test_ordinary_tables(void) {
  pulseten_db_t *db = open_db("ordinary");
  write_many(db, "m", 0, 3);
  CHECK(pulseten_exec_ddl(db, "CREATE TABLE copied AS SELECT * FROM m") ==
        PULSETEN_OK);
  CHECK(query_int(db, "SELECT count(*) FROM copied c JOIN m ON c.v = m.v") ==
        3);
  CHECK(pulseten_exec_ddl(db, "DROP TABLE m") == PULSETEN_INVALID_ARGUMENT);
  CHECK(pulseten_drop_table(db, "m") == PULSETEN_INVALID_ARGUMENT);
  CHECK(pulseten_exec_ddl(db, "CREATE TABLE m (v BIGINT)") ==
        PULSETEN_INVALID_ARGUMENT);
  CHECK(pulseten_exec_ddl(db, "DROP TABLE _pt_state") ==
        PULSETEN_INVALID_ARGUMENT);
  CHECK(pulseten_exec_ddl(NULL, "CREATE TABLE x (v BIGINT)") ==
        PULSETEN_INVALID_ARGUMENT);
  CHECK(pulseten_exec_ddl(db, NULL) == PULSETEN_INVALID_ARGUMENT);
  CHECK(pulseten_close(db) == PULSETEN_OK);
  db = open_db("ordinary");
  CHECK(query_int(db, "SELECT count(*) FROM copied") == 3);
  CHECK(pulseten_drop_table(db, "COPIED") == PULSETEN_OK);
  CHECK(pulseten_drop_table(db, "copied") == PULSETEN_NOT_FOUND);
  CHECK(query_int(db, "SELECT count(*) FROM m") == 3);
  CHECK(pulseten_close(db) == PULSETEN_OK);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main(void) {
  scratch_create();
  test_open_requires_output();
  test_open_invalid_options();

  test_version();
  test_options();
  test_wal_sync_options();
  test_write_and_read();
  test_chunks();
  test_result_snapshot();
  test_errors();
  test_retention();
  test_limits();
  test_catalog();
  test_drop_table();
  test_ordinary_tables();
  test_expire();
  test_interrupt();
  test_reopen();

  scratch_remove();

  if (failures > 0) {
    fprintf(stderr, "api-t: %d check(s) failed\n", failures);

    return (1);
  }

  printf("api-t: all checks passed\n");

  return (0);
}
