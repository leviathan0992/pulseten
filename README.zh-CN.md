<div align="center">

<h1>PulseTen</h1>

<p><b>An Embedded Real-Time Analytical Database</b></p>

<p><a href="README.md">English</a> | 简体中文</p>

<br>

<img src="docs/assets/features.svg" width="720" alt="SQLite-like 进程内嵌入 · 零部署 · DuckDB-like 列式存储 · 向量化执行 · TSDB-like 支持时序数据模型 · 时间分区">

</div>

## 简介

PulseTen 是一个嵌入式的实时分析数据库：像 SQLite 一样以库的形式运行在应用进程内，不需要部署服务；数据按时间分区、列式存储，用 SQL 做分析查询，查询由 [DuckDB](https://duckdb.org) 执行。

它面向持续写入、随即分析的时序数据，例如指标、日志统计、设备遥测。

## 特性

- **WAL 与即时可见**：批量写入先进 WAL（组提交），返回 `PULSETEN_OK` 后对之后的查询可见。默认写入操作系统的 page cache，不等待 WAL 同步。开启 `m_wal_sync`（Go：`WALSync`）后，每次成功写入在返回前完成持久化。
- **自动维护**：数据库打开后始终运行后台线程，自动将写入存入分析存储，并按配置的保留期删除过期分区。应用只需配置执行间隔和保留期，无需手动调用维护接口。
- **Schema-on-write**：表和列由写入自动创建，按 tag / field 的时序数据模型组织。
- **实时分析**：查询同时读取已落盘的列存数据和内存中尚未 Flush 的写入，看到的是一个一致的快照。
- **时间分区**：按天对齐的时间分区，查询按时间条件裁剪分区；支持按保留期（TTL）整块删除过期数据。
- **SQL 分析**：由 DuckDB 执行逻辑表上的单条只读 SELECT，支持 CTE、子查询和连接。拒绝表函数、带 catalog/schema 限定的表名及内部或系统对象；结果类型须能由 C API 表示。结果按块流式读取，可从其他线程中断。
- **纯 C 接口**：一个头文件 `pulseten.h`，可被 C、C++ 以及任何能调用 C 的语言使用。Go 包在 [go/](go) 目录。

## 构建

需要 CMake ≥ 3.24 和支持 C++20 的编译器（Clang 或 GCC），在 Linux 和 macOS 上测试。DuckDB 以子模块形式固定在 v2.0.0。

```bash
git submodule update --init third_party/duckdb
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build && ctest --test-dir build
```

测试默认开启。`PT_DEBUG` 只负责断言和锁诊断；白盒测试使用单独定义 `PT_TESTING` 的 `pulseten_testlib`，Release 模式也会运行故障注入和并发暂停用例。C API 测试、Go 和基准程序使用正式库。添加 `-DBUILD_TESTING=OFF` 可只构建正式库及示例；安装始终只包含正式库。

## 安装

```bash
cmake --install build --prefix /opt/pulseten
```

安装 `pulseten.h` 和一个已经包含 DuckDB 的静态库 `libpulseten.a`，并提供 CMake 包 `PulseTen::pulseten` 和 pkg-config 文件 `pulseten.pc`。

## 快速开始

```cpp
#include "pulseten.h"

#include <cstdio>

int main() {
  pulseten_db_t *db = nullptr;
  pulseten_open("./metrics", nullptr, &db);

  /* 一个点：cpu,host=web-1 usage=12.5 */
  pulseten_tag_t   tag   = {"host", 4, "web-1", 5};
  pulseten_field_t field = {"usage", 5, PULSETEN_TYPE_DOUBLE, {.m_double = 12.5}};
  pulseten_point_t point = {1767225600000000LL, &tag, 1, &field, 1};
  pulseten_write(db, "cpu", &point, 1);

  /* SQL 查询，结果按块读取 */
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

完整示例（含错误处理）见 [examples/quickstart.cc](examples/quickstart.cc)。

## WAL 同步

默认 `m_wal_sync = 0`（Go：`WALSync: false`）。WAL 写入操作系统的 page cache 后即可确认成功。系统崩溃或断电可能丢失已确认的写入，也可能损坏尚未同步的 WAL 数据。关闭数据库不会强制同步尚未持久化的写入。

需要每组写入等待 WAL 同步时，在打开数据库前将 `m_wal_sync` 设为非零值：

```cpp
pulseten_options_t options;
pulseten_options_init(&options);
options.m_wal_sync = 1;
```

Go 调用方在 `DefaultOptions()` 的返回值上设置 `opts.WALSync = true`，再将其传给 `Open`。该设置仅对本次打开生效。关闭 WAL 同步后，文件创建、轮转和恢复仍可能同步文件。后台刷盘会先同步 WAL，再提交分析存储，因此该参数不会关闭所有磁盘 I/O。

## DuckDB 内存预算

在打开数据库前设置 `m_duckdb_memory_limit_bytes`（Go：`DuckDBMemoryLimitBytes`）。这是同一数据库实例内所有查询连接和存储连接共享的 DuckDB `memory_limit`，不为每个连接单独分配额度。

```go
opts := pulseten.DefaultOptions()
opts.DuckDBMemoryLimitBytes = 2 << 30
```

默认值 `0` 在每次打开时计算 `min(有效内存 / 8, 2 GiB)`。有效内存取物理容量与 Linux 进程所在 cgroup v1/v2 及可见祖先的硬限额的较小值，不使用当前空闲内存。探测失败会返回错误，调用方可以显式指定预算；不会回退到 DuckDB 默认的约 80%。容器命名空间隐藏的祖先限制无法读取，部署方应显式分配预算。

正数按字节传入，不受自动值的 2 GiB 上限约束；最大支持 `2^53` 字节，确保 DuckDB 的配置解析不会舍入整数。过大的值在创建目录前被拒绝；合法但不足以启动数据库的预算仍可能返回内存不足。预算只对本次打开生效，不写入数据库，也不支持运行时修改。

通过 `pulseten_read_stats()` 的 `m_duckdb_memory_limit_bytes` 或 Go 的 `db.Stats().DuckDBMemoryLimitBytes` 查看实际预算。多个数据库实例分别计算预算，应用同时打开多个库时应显式分配各自额度。

该预算不包含 memtable、Go heap，也不覆盖所有原生分配，因此不是进程 RSS 硬限制。memtable 仍由独立的 `MemtableLimitBytes` 控制。自动公式是保守策略；小内存容器中的持续写入和并发分析负载仍需按部署场景验收。此次扩展了 C options/stats 结构体，升级库时必须重新编译 C/C++ 和 Go 调用方。

## 文档

- [include/pulseten.h](include/pulseten.h)：C API，每个函数都有注释。
- [go/](go)：Go 包，用 `go doc` 查看接口。

## 许可证

[Apache License 2.0](LICENSE)
