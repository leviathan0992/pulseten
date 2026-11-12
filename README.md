<div align="center">

<h1>PulseTen</h1>

<p><b>An Embedded Real-Time Analytical Database</b></p>

<p>English | <a href="README.zh-CN.md">简体中文</a></p>

<br>

<img src="docs/assets/features.en.svg" width="720" alt="SQLite-like: in-process, zero deployment · DuckDB-like: columnar, vectorized · TSDB-like: time-series model, time partitions">

</div>

## Overview

PulseTen is an embedded real-time analytical database. Like SQLite, it runs as a library inside the application process, with no server to deploy. Data is partitioned by time and stored in columns, and analytical queries are written in SQL and executed by [DuckDB](https://duckdb.org).

It is built for time-series data that is written continuously and analyzed right away: metrics, log statistics, device telemetry.

## Features

- **WAL and immediate visibility**: batches go to a write-ahead log with group commit and become visible to later queries when the write returns `PULSETEN_OK`. By default, writes go to the operating system's page cache without waiting for a WAL sync. Enable `m_wal_sync` (Go: `WALSync`) to make each successful write durable before it returns.
- **Automatic maintenance**: every open database runs a background thread that flushes writes to analytical storage and removes partitions older than the configured retention period. Applications configure the interval and retention period without calling maintenance functions.
- **Schema-on-write**: tables and columns are created by the writes themselves, following a tag / field time-series data model.
- **Real-time analytics**: a query reads the columnar data on disk and the writes not yet flushed from memory, as one consistent snapshot.
- **Time partitions**: data is split into partitions of whole days, queries skip partitions outside their time range, and a retention period (TTL) drops expired partitions whole.
- **SQL analytics**: DuckDB executes a single read-only SELECT over logical tables, with CTEs, subqueries and joins. Table functions, qualified table names and internal or system objects are rejected; result types must fit the C API. Results are streamed chunk by chunk, and queries can be interrupted from another thread.
- **Plain C API**: a single header, `pulseten.h`, usable from C, C++ and any language that can call C. A Go package is in [go/](go).

## Building

Requires CMake 3.24 or later and a C++20 compiler (Clang or GCC). Tested on Linux and macOS. DuckDB is a submodule pinned to v2.0.0.

```bash
git submodule update --init third_party/duckdb
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build && ctest --test-dir build
```

Tests are enabled by default. `PT_DEBUG` controls assertions and lock diagnostics. White-box tests link a separate `pulseten_testlib` built with `PT_TESTING`, so failure injection and concurrency checkpoints also run in Release. C API tests, Go and benchmarks use the normal library. Set `-DBUILD_TESTING=OFF` to build only the normal library and examples; installation always uses the normal library.

## Installing

```bash
cmake --install build --prefix /opt/pulseten
```

This installs `pulseten.h` and a single static library, `libpulseten.a`, that includes DuckDB, together with a CMake package (`PulseTen::pulseten`) and a pkg-config file (`pulseten.pc`).

## Quick start

```cpp
#include "pulseten.h"

#include <cstdio>

int main() {
  pulseten_db_t *db = nullptr;
  pulseten_open("./metrics", nullptr, &db);

  /* One point: cpu,host=web-1 usage=12.5 */
  pulseten_tag_t   tag   = {"host", 4, "web-1", 5};
  pulseten_field_t field = {"usage", 5, PULSETEN_TYPE_DOUBLE, {.m_double = 12.5}};
  pulseten_point_t point = {1767225600000000LL, &tag, 1, &field, 1};
  pulseten_write(db, "cpu", &point, 1);

  /* Query with SQL and read the result chunk by chunk. */
  pulseten_result_t *result = nullptr;
  pulseten_query(db, "SELECT host, avg(usage) FROM cpu GROUP BY host", &result);

  uint32_t n;
  while (pulseten_result_fetch(result, &n) == PULSETEN_OK && n > 0) {
    for (uint32_t r = 0; r < n; r++) {
      printf("%s %.2f\n", pulseten_read_varchar(result, 0, r, nullptr),
             pulseten_read_double(result, 1, r));
    }
  }

  pulseten_result_destroy(result);
  pulseten_close(db);

  return (0);
}
```

A complete example, with error handling, is in [examples/quickstart.cc](examples/quickstart.cc).

## WAL sync

The default `m_wal_sync = 0` (Go: `WALSync: false`) acknowledges writes after writing the WAL to the operating system's page cache. A system crash or power loss may lose acknowledged writes or corrupt unsynced WAL data. Closing the database does not force unsynced writes to disk.

Set `m_wal_sync` to a nonzero value before opening the database to wait for a WAL sync on each group commit:

```cpp
pulseten_options_t options;
pulseten_options_init(&options);
options.m_wal_sync = 1;
```

In Go, set `opts.WALSync = true` on `DefaultOptions()` and pass it to `Open`. The setting applies to that open only. Segment creation, rotation and recovery may still sync files with WAL sync disabled. Background flushes sync the WAL before committing analytical storage, so this setting does not disable all disk I/O.

## DuckDB memory budget

Set `m_duckdb_memory_limit_bytes` (Go: `DuckDBMemoryLimitBytes`) before opening. It configures DuckDB's `memory_limit`, shared by all query and storage connections of that database instance.

```go
opts := pulseten.DefaultOptions()
opts.DuckDBMemoryLimitBytes = 2 << 30
```

The default `0` computes `min(effective memory / 8, 2 GiB)` once per open. Effective memory is physical capacity reduced by the process's Linux cgroup v1/v2 hard limits and visible ancestor limits, not currently free memory. Detection failures return an error requiring an explicit budget; they never fall back to DuckDB's roughly 80% default. Ancestor limits hidden by a container namespace cannot be inspected; configure an explicit budget for such deployments.

Positive values are exact bytes, up to `2^53` to avoid rounding in DuckDB's setting parser, and are not capped to 2 GiB. Out-of-range values are rejected before directory creation. A valid but insufficient budget can still fail database startup with an allocation error. The setting applies to this open only; it is not persisted or adjustable at runtime.

Read the effective budget from `pulseten_read_stats().m_duckdb_memory_limit_bytes` or Go's `db.Stats().DuckDBMemoryLimitBytes`. Multiple database instances compute independent budgets; allocate them explicitly when opening several databases.

The budget excludes memtables, the Go heap and some native allocations, so it is not a process RSS limit. `MemtableLimitBytes` remains independent. The automatic formula is a conservative policy; sustained ingestion and concurrent analytics in small containers still require workload-specific acceptance tests. The C options/stats structures were extended: rebuild C/C++ and Go callers when upgrading the library.

## Documentation

- [include/pulseten.h](include/pulseten.h): the C API, with every function documented.
- [go/](go): the Go package; `go doc` shows its API.

## License

[Apache License 2.0](LICENSE)
