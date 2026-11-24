# Benchmark

This benchmark compares PulseTen with Prometheus TSDB and SQLite on the TSBS
`cpu-only` use case: host metrics from 100 and 1,000 hosts, each reporting ten
CPU usage fields every 10 seconds for 3 days. The data and the queries both
come from the TSBS generators, with a fixed seed. Each engine ingests the data
in bulk, then answers 20 queries of each of the 16 TSBS query types.

![TSBS cpu-only ingestion, disk usage and query latency at 100 and 1,000 hosts](assets/benchmark-tsbs.svg)

## Summary

- **Ingestion at the page cache.** At 100 hosts PulseTen writes about 8 times as
  many rows per second as Prometheus TSDB and 3.5 times as many as SQLite
  through its C API. At 1,000 hosts it writes 1.9 times as many as Prometheus
  TSDB through its C API and 1.4 times through its Go bindings, and 10 and 7
  times as many as SQLite.
- **Ingestion with every batch durable.** At 1,000 hosts PulseTen writes 30%
  fewer rows per second than at the page cache through its C API, and 22% fewer
  through its Go bindings: 8 and 6 times as many as SQLite. Prometheus TSDB
  cannot sync each commit.
- **Disk usage.** Prometheus TSDB stores the data most compactly. PulseTen uses
  54% to 89% more at 100 hosts and 11% to 36% more at 1,000 hosts: a quarter to
  a third of SQLite's space at 100 hosts, and a sixth to a fifth at 1,000. Its
  size varies between runs.
- **Queries across many hosts.** At 1,000 hosts, on queries over 32 hosts or
  more except `lastpoint`, PulseTen answers 4 to 32 times faster than Prometheus
  TSDB. It is 16 to 270 times faster than SQLite through its C API and 7 to 260
  times through its Go bindings.
- **Single-host queries.** On the `single-groupby` queries of one host,
  Prometheus TSDB answers in 0.04 to 1.4 ms and SQLite in 0.1 to 1.7 ms, while
  PulseTen takes 2.3 to 8 ms. On `high-cpu-1` at 1,000 hosts PulseTen takes 46
  to 47 ms, against 2.5 ms for SQLite.
- **The latest row of every host.** PulseTen and SQLite are close: 3.0 and 3.2 s
  at 1,000 hosts, and 0.26 to 0.27 s at 100.

## Setup

### Engines

| Engine | Version | Interface | Layout |
| --- | --- | --- | --- |
| PulseTen | `main` branch | C API and Go bindings | 24-hour partitions |
| Prometheus TSDB | v0.316.0 | Go library with its PromQL engine | 2-hour head blocks, compacted up to 24 hours |
| SQLite | 3.54.1, through `mattn/go-sqlite3` | Go `database/sql` with prepared statements | One `WITHOUT ROWID` table keyed by `(hostname, time)`, and an index on `time` |

PulseTen runs twice, through its C API and through its Go bindings, so that it
also meets the two Go engines on their own terms. SQLite has the indexes TSBS
creates for TimescaleDB, and a 256 MiB page cache. DuckDB is left out: it is
where PulseTen keeps its data, not an alternative to it.

### Durability

Ingestion runs in two groups. In the first, every engine acknowledges a write as
soon as it reaches the operating system's page cache: a process crash loses
nothing, and a power loss may lose recent writes. In the second, every batch
survives a power loss before it is acknowledged. The queries run on the
databases of the first group.

| Engine | Page cache | Every batch durable |
| --- | --- | --- |
| PulseTen | Default options, `m_wal_sync = 0` | `m_wal_sync = 1` |
| Prometheus TSDB | Default options | Not available: commits cannot be synced |
| SQLite | `journal_mode = WAL` and `synchronous = NORMAL` | `synchronous = FULL`, `fullfsync` and `checkpoint_fullfsync` |

On macOS `fsync` does not flush the drive's cache, so the second group uses
`F_FULLFSYNC`, which PulseTen calls by itself and SQLite calls with `fullfsync`.

In the first group the engines still sync in the background. PulseTen syncs its
WAL before each flush, so that the log always covers what storage holds, and
its internal DuckDB syncs each flush. Prometheus syncs finished WAL segments and
the blocks it writes during compaction, and SQLite syncs at checkpoints.

### Data

`tsbs_generate_data --use-case=cpu-only --seed=123`, 3 days from 2016-01-01
UTC at a 10-second interval. Each row carries ten tags, `hostname`, `region`,
`datacenter`, `rack`, `os`, `arch`, `team`, `service`, `service_version` and
`service_environment`, and ten integer fields, `usage_user` to
`usage_guest_nice`.

| Scale | Rows | Values |
| --- | --- | --- |
| 100 hosts | 2,592,000 | 25,920,000 |
| 1,000 hosts | 25,920,000 | 259,200,000 |

PulseTen and SQLite store the fields as 64-bit integers; Prometheus stores
every sample as a 64-bit float. Every engine reads the same pre-generated file,
and reading it is not timed.

### Ingestion

Rows arrive in time order, 10,000 at a time. A batch is one `pulseten_write`
call, one Prometheus appender commit, or one SQLite transaction. Prometheus
keeps each field of each host as a series of its own, so for it a batch holds
100,000 samples.

Prometheus compacts its head into a block once the head spans one and a half
blocks, the condition it uses in normal operation. Because 3 days of data
arrive here in under a minute, all of that compaction falls within the
measured ingestion instead of spreading over two-hour intervals.

Each run reports two times:

- **Ingest**: from the first write until the last one returns.
- **Stored**: until every row has reached its final storage and the database
  has closed. For PulseTen this includes the background flush into DuckDB, and
  for SQLite a WAL checkpoint.

### Queries

`tsbs_generate_queries` with the same seed generates 20 queries of each type.

| Query type | What it computes |
| --- | --- |
| `single-groupby-1-1-1` | Max of 1 field of 1 host, 1 hour, per minute |
| `single-groupby-1-1-12` | Max of 1 field of 1 host, 12 hours, per minute |
| `single-groupby-1-8-1` | Max of 1 field of 8 hosts, 1 hour, per minute |
| `single-groupby-5-1-1` | Max of 5 fields of 1 host, 1 hour, per minute |
| `single-groupby-5-1-12` | Max of 5 fields of 1 host, 12 hours, per minute |
| `single-groupby-5-8-1` | Max of 5 fields of 8 hosts, 1 hour, per minute |
| `cpu-max-all-1` | Max of every field of 1 host, 8 hours, per hour |
| `cpu-max-all-8` | Max of every field of 8 hosts, 8 hours, per hour |
| `cpu-max-all-32-24` | Max of every field of 32 hosts, 24 hours, per hour |
| `double-groupby-1` | Mean of 1 field of every host, 12 hours, per host and hour |
| `double-groupby-5` | Mean of 5 fields of every host, 12 hours, per host and hour |
| `double-groupby-all` | Mean of every field of every host, 12 hours, per host and hour |
| `high-cpu-1` | Rows of 1 host with `usage_user` above 90, 12 hours |
| `high-cpu-all` | Rows of every host with `usage_user` above 90, 12 hours |
| `groupby-orderby-limit` | Max of 1 field across every host per minute, the last 5 minutes before a random time |
| `lastpoint` | The latest row of every host |

PulseTen and SQLite run the SQL that TSBS generates for TimescaleDB, rewritten
for each dialect. Prometheus runs the PromQL that TSBS generates, as range
queries. Three adjustments make every engine compute the same result:

- Queries that group into time buckets have their start and end rounded down to
  a bucket boundary, and Prometheus evaluates each bucket 1 ms before it ends,
  so that SQL buckets and PromQL windows cover the same rows.
- PromQL drops the metric name inside functions and cannot group by it, so a
  query over several fields runs as one PromQL query per field, and the
  reported time is their sum.
- `high-cpu-*`, `groupby-orderby-limit` and `lastpoint` have no PromQL form in
  TSBS, so Prometheus answers 12 of the 16 types.

After ingestion the database is reopened and its files are read once, so that
every engine starts from a warm page cache. Each type then runs once untimed
and 20 times for measurement; the mean is reported.

Every result is checked against PulseTen through its C API, by row count and
the sum of its values. In every run, all 320 results from the Go bindings and
SQLite match, and so do all 240 from Prometheus.

### Environment

An Apple M1 with 8 cores and 16 GB of memory, running macOS 15.7.7, Apple clang
16.0.0 and Go 1.27.1. Each engine runs in-process, one at a time, on a desktop
with its usual applications open. Each run starts once the one-minute load
average is below 6, or after waiting 5 minutes. SQLite's first run at 100 hosts
was repeated, since other activity on the machine had disturbed its queries.

## Results

The 100-host scale runs twice, shown as `first / second`; the 1,000-host scale
runs once.

### Ingestion at the page cache

100 hosts:

| Engine | Rows per second | Ingest (s) | Stored (s) | Disk (MiB) |
| --- | --- | --- | --- | --- |
| PulseTen (C) | 751,257 / 855,040 | 3.5 / 3.0 | 4.4 / 4.3 | 106.1 / 106.4 |
| PulseTen (Go) | 621,993 / 648,518 | 4.2 / 4.0 | 4.8 / 5.0 | 86.9 / 88.9 |
| Prometheus TSDB | 92,406 / 110,598 | 28.1 / 23.4 | 28.1 / 23.4 | 56.3 / 56.3 |
| SQLite | 236,466 / 218,737 | 11.0 / 11.8 | 11.0 / 11.9 | 336.5 / 337.0 |

1,000 hosts:

| Engine | Rows per second | Ingest (s) | Stored (s) | Disk (MiB) |
| --- | --- | --- | --- | --- |
| PulseTen (C) | 856,624 | 30.3 | 32.9 | 692.4 |
| PulseTen (Go) | 605,522 | 42.8 | 43.7 | 564.1 |
| Prometheus TSDB | 441,344 | 58.7 | 58.8 | 507.8 |
| SQLite | 85,268 | 304.0 | 304.0 | 3,424.9 |

### Ingestion with every batch durable

100 hosts:

| Engine | Rows per second | Ingest (s) | Stored (s) | Disk (MiB) |
| --- | --- | --- | --- | --- |
| PulseTen (C) | 661,843 / 651,782 | 3.9 / 4.0 | 4.7 / 4.9 | 87.4 / 87.4 |
| PulseTen (Go) | 568,335 / 558,287 | 4.6 / 4.6 | 5.8 / 5.1 | 106.1 / 80.4 |
| SQLite | 190,131 / 190,930 | 13.6 / 13.6 | 13.7 / 13.6 | 336.8 / 336.8 |

1,000 hosts:

| Engine | Rows per second | Ingest (s) | Stored (s) | Disk (MiB) |
| --- | --- | --- | --- | --- |
| PulseTen (C) | 603,036 | 43.0 | 44.3 | 564.1 |
| PulseTen (Go) | 472,796 | 54.8 | 56.3 | 627.9 |
| SQLite | 73,066 | 354.8 | 354.8 | 3,424.6 |

### Query latency

Mean latency over 20 queries, in milliseconds. A dash marks a query type with
no PromQL form.

100 hosts:

| Query type | PulseTen (C) | PulseTen (Go) | Prometheus TSDB | SQLite |
| --- | --- | --- | --- | --- |
| `single-groupby-1-1-1` | 2.9 / 2.5 | 2.7 / 2.3 | 0.04 / 0.04 | 0.10 / 0.10 |
| `single-groupby-1-1-12` | 4.3 / 3.9 | 4.2 / 3.2 | 0.27 / 0.33 | 1.1 / 1.1 |
| `single-groupby-1-8-1` | 3.8 / 3.3 | 3.7 / 3.0 | 0.20 / 0.20 | 0.67 / 0.67 |
| `single-groupby-5-1-1` | 3.3 / 2.7 | 3.5 / 2.9 | 0.18 / 0.20 | 0.15 / 0.15 |
| `single-groupby-5-1-12` | 5.7 / 4.7 | 5.6 / 4.5 | 1.4 / 1.4 | 1.7 / 1.7 |
| `single-groupby-5-8-1` | 4.3 / 3.5 | 4.3 / 3.5 | 0.98 / 1.1 | 1.0 / 0.99 |
| `cpu-max-all-1` | 5.8 / 5.6 | 5.7 / 5.2 | 1.3 / 1.3 | 1.3 / 1.4 |
| `cpu-max-all-8` | 6.5 / 6.5 | 6.7 / 5.7 | 9.2 / 9.5 | 10.8 / 11.1 |
| `cpu-max-all-32-24` | 11.0 / 10.9 | 10.8 / 10.8 | 114 / 118 | 140 / 147 |
| `double-groupby-1` | 7.3 / 7.0 | 8.0 / 6.8 | 16.4 / 17.3 | 451 / 512 |
| `double-groupby-5` | 8.7 / 8.8 | 9.0 / 8.8 | 82.1 / 84.6 | 488 / 490 |
| `double-groupby-all` | 10.6 / 10.5 | 11.0 / 10.9 | 164 / 168 | 531 / 535 |
| `high-cpu-1` | 7.4 / 7.0 | 7.7 / 7.7 | — | 1.1 / 1.2 |
| `high-cpu-all` | 26.3 / 26.0 | 57.4 / 58.8 | — | 333 / 336 |
| `groupby-orderby-limit` | 9.0 / 9.1 | 9.1 / 9.4 | — | 965 / 971 |
| `lastpoint` | 260 / 262 | 265 / 270 | — | 258 / 262 |

1,000 hosts:

| Query type | PulseTen (C) | PulseTen (Go) | Prometheus TSDB | SQLite |
| --- | --- | --- | --- | --- |
| `single-groupby-1-1-1` | 2.8 | 2.9 | 0.04 | 0.11 |
| `single-groupby-1-1-12` | 5.8 | 5.5 | 0.28 | 1.2 |
| `single-groupby-1-8-1` | 3.4 | 3.1 | 0.20 | 0.73 |
| `single-groupby-5-1-1` | 3.4 | 3.0 | 0.19 | 0.15 |
| `single-groupby-5-1-12` | 7.9 | 8.0 | 1.4 | 1.7 |
| `single-groupby-5-8-1` | 4.2 | 3.8 | 0.99 | 0.98 |
| `cpu-max-all-1` | 9.5 | 11.1 | 1.3 | 8.5 |
| `cpu-max-all-8` | 10.4 | 10.5 | 9.3 | 60.1 |
| `cpu-max-all-32-24` | 26.9 | 28.2 | 117 | 807 |
| `double-groupby-1` | 27.7 | 28.5 | 164 | 7,404 |
| `double-groupby-5` | 36.8 | 39.6 | 822 | 5,839 |
| `double-groupby-all` | 50.8 | 56.7 | 1,642 | 6,952 |
| `high-cpu-1` | 46.0 | 47.1 | — | 2.5 |
| `high-cpu-all` | 255 | 609 | — | 4,130 |
| `groupby-orderby-limit` | 50.8 | 50.2 | — | 13,037 |
| `lastpoint` | 3,037 | 3,027 | — | 3,240 |
