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

- **写入即持久、即可见**：批量写入先进 WAL（组提交），写入返回 `PULSETEN_OK` 时数据已持久，并对之后的查询可见。
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

## 安装

```bash
cmake --install build --prefix /opt/pulseten
```

安装 `pulseten.h` 和一个已经包含 DuckDB 的静态库 `libpulseten.a`，并提供 CMake 包 `PulseTen::pulseten` 和 pkg-config 文件 `pulseten.pc`。

## 快速开始

```cpp
#include <cstdio>

#include "pulseten.h"

int main() {
  pulseten_db_t *db = nullptr;
  pulseten_open("./metrics", nullptr, &db);

  /* 一个点：cpu,host=web-1 usage=12.5 */
  pulseten_tag_t   tag   = {"host", 4, "web-1", 5};
  pulseten_field_t field = {"usage", 5, PULSETEN_TYPE_DOUBLE, {.d = 12.5}};
  pulseten_point_t point = {1767225600000000LL, &tag, 1, &field, 1};
  pulseten_write(db, "cpu", &point, 1, nullptr);

  /* SQL 查询，结果按块读取 */
  pulseten_result_t *result = nullptr;
  pulseten_query(db, "SELECT host, avg(usage) FROM cpu GROUP BY host", &result);

  uint32_t n;
  while (pulseten_fetch(result, &n) == PULSETEN_OK && n > 0) {
    for (uint32_t r = 0; r < n; r++) {
      printf("%s %.2f\n", pulseten_get_varchar(result, 0, r, nullptr),
             pulseten_get_double(result, 1, r));
    }
  }

  pulseten_result_destroy(result);
  pulseten_close(db);

  return (0);
}
```

完整示例（含错误处理）见 [examples/quickstart.cc](examples/quickstart.cc)。

## 文档

- [include/pulseten.h](include/pulseten.h)：C API，每个函数都有注释。
- [go/](go)：Go 包，用 `go doc` 查看接口。

## 许可证

[Apache License 2.0](LICENSE)
