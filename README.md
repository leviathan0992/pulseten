<div align="center">

<h1>PulseTen</h1>

<p><b>The embedded time-series database built on DuckDB.</b></p>

<p>
<a href="https://github.com/leviathan0992/pulseten/actions/workflows/ci.yml"><img src="https://github.com/leviathan0992/pulseten/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
<a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0"></a>
</p>

<p>English | <a href="README.zh-CN.md">简体中文</a></p>

<br>

<img src="docs/assets/features.en.svg" width="720" alt="TSDB-like: time-series model, time partitions · SQLite-like: in-process, zero deployment · DuckDB-like: columnar, vectorized">

</div>

## Overview

PulseTen is an embedded time-series database built on [DuckDB](https://duckdb.org). Time-series data is written through a native time-series API, and SQL tables are read and written through SQL.

## Highlights

- **Time-series and SQL tables, one database.** Metrics are written to time-series tables through a native API; hosts, devices and other reference data live in SQL tables with ACID transactions. A single SQL query joins both, with no data movement.
- **Fast ingestion.** A write appends one batch to the write-ahead log and to a columnar memtable; a background thread flushes it into DuckDB in bulk.
- **Real-time.** Writes are queryable as soon as they return; every query reads stored and in-memory data as one consistent snapshot.
- **Time partitioning.** Time-series data is organized into hour-aligned partitions. Queries prune partitions outside their time range, and retention drops expired partitions whole.
- **Compact storage.** Flushed data is kept in DuckDB's columnar format, where each column is compressed with lightweight encodings such as RLE, bit-packing, dictionary/FSST and ALP for floating-point values.
- **Crash recovery.** Time-series writes are group-committed to a write-ahead log and replayed on open. Optional WAL sync makes each write durable before it returns.
- **Embeddable.** A single header and a self-contained static library. Ships with native Go bindings.

## Architecture

```mermaid
%%{init: {"flowchart": {"subGraphTitleMargin": {"top": 12, "bottom": 8}}}}%%
flowchart LR
  write(["pulseten_write"]) -- Time Series --> wal

  subgraph engine ["PulseTen"]
    direction LR
    wal["WAL"] --> mem["Memtable"]
  end

  subgraph duckdb ["DuckDB"]
    direction TB
    part[("Time partitions")]
    rel[("SQL tables")]
  end

  mem -- "Flush (Appender)" --> part
  exec(["pulseten_exec"]) -- SQL --> rel

  style engine fill:none,stroke:#8b949e,stroke-dasharray:4 3,color:#8b949e
  style duckdb fill:none,stroke:#8b949e,stroke-dasharray:4 3,color:#8b949e
```

Time-series writes are appended to a write-ahead log and buffered in a columnar memtable, which a background thread flushes into hour-aligned DuckDB partitions in bulk, through DuckDB's Appender. SQL tables are native DuckDB tables. Each query runs in DuckDB against one consistent snapshot of the partitions, the SQL tables and the memtable.

## Performance

PulseTen is compared with Prometheus TSDB and SQLite on the TSBS `cpu-only` use case: 1,000 hosts reporting ten CPU usage fields every 10 seconds for 3 days, about 26 million rows. Each engine ingests the data in bulk, then answers queries over one host, a few hosts, and every host. The chart shows four of the 16 TSBS query types.

![Ingestion throughput, disk usage and query latency at 1,000 hosts](docs/assets/benchmark.svg)

See [docs/benchmark.md](docs/benchmark.md) for the setup and the full results, including all 16 query types at 100 and 1,000 hosts.

## Example

```cpp
#include "pulseten.h"

#include <cstdio>

int main() {
  pulseten_db_t *db = nullptr;
  pulseten_open("./metrics", nullptr, &db);

  /* cpu,host=web-1 usage=12.5 at 2026-01-01 00:00 UTC. The cpu table and
  its columns are created by the write itself. */
  pulseten_tag_t   tag   = {"host", 4, "web-1", 5};
  pulseten_field_t field = {"usage", 5, PULSETEN_TYPE_DOUBLE, {.m_double = 12.5}};
  pulseten_point_t point = {1767225600000000LL, &tag, 1, &field, 1};
  pulseten_write(db, "cpu", &point, 1);

  /* The write is visible to queries as soon as it returns. */
  pulseten_result_t *result = nullptr;
  pulseten_query(db,
                 "SELECT host, avg(usage) FROM cpu "
                 "WHERE time >= TIMESTAMP '2026-01-01' "
                 "GROUP BY host",
                 &result);

  uint32_t n = 0;
  while (pulseten_result_fetch(result, &n) == PULSETEN_OK && n > 0) {
    for (uint32_t r = 0; r < n; r++) {
      printf("%s %.2f\n", pulseten_result_varchar(result, 0, r, nullptr),
             pulseten_result_double(result, 1, r));
    }
  }

  pulseten_result_destroy(result);
  pulseten_close(db);

  return (0);
}
```

The same flow in Go:

```go
db, err := pulseten.Open("./metrics", nil)
if err != nil {
	return err
}
defer db.Close()

// The cpu table and its columns are created by the write itself.
if err := db.Write("cpu", pulseten.Point{
	Time:   time.Now(),
	Tags:   []pulseten.Tag{{Name: "host", Value: "web-1"}},
	Fields: []pulseten.Field{{Name: "usage", Value: 12.5}},
}); err != nil {
	return err
}

rows, err := db.Query(`SELECT host, avg(usage) FROM cpu GROUP BY host`)
if err != nil {
	return err
}
defer rows.Close()

for rows.Next() {
	fmt.Println(rows.String(0), rows.Float64(1))
}
return rows.Err()
```

See [examples/quickstart.cc](examples/quickstart.cc) for a complete program with error handling.

## Installing

The Go package is installed with a single command:

```bash
go get github.com/leviathan0992/pulseten/go
```

It links prebuilt static libraries, so neither CMake nor pkg-config is needed. Building requires Go 1.22+, cgo and a C toolchain with the C++ standard library. Prebuilt libraries are available for these platforms:

| Platform | `GOOS/GOARCH` |
| --- | --- |
| macOS 12+ on Apple silicon | `darwin/arm64` |
| Linux on x86-64, glibc | `linux/amd64` |
| Linux on ARM64, glibc | `linux/arm64` |

On other platforms, and for C and C++, build PulseTen from source.

## Building from Source

Requires CMake 3.24+, Ninja and a C++20 compiler. Tested on Linux and macOS.

```bash
git submodule update --init third_party/duckdb
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build && ctest --test-dir build
cmake --install build --prefix /opt/pulseten
```

Installation provides `pulseten.h` and `libpulseten.a`, a self-contained static library that bundles DuckDB, together with a CMake package (`PulseTen::pulseten`) and a pkg-config file (`pulseten.pc`).

To link the Go package against this build instead of the prebuilt libraries, use the `pulseten_use_pkgconfig` build tag. It is the default on platforms without prebuilt libraries.

```bash
export PKG_CONFIG_PATH=/opt/pulseten/lib/pkgconfig
go build -tags pulseten_use_pkgconfig ./...
```

## Documentation

- [include/pulseten.h](include/pulseten.h): the complete C API reference, covering durability, recovery, locking and memory limits.
- [go/](go): the Go package; run `go doc -all` there for its full API.
- [docs/benchmark.md](docs/benchmark.md): benchmark setup and full results.

## Status

PulseTen is in its 0.x series. Until 1.0, a minor release may change the API or the on-disk format; the release notes describe every such change.

## License

[Apache License 2.0](LICENSE)
