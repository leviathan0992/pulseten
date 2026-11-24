/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_directory-t.cc
 *    Directory ownership, safe deletion, lifecycle locks and interrupted I/O.
 *
 * Covers: src/db/db_directory.cc, src/db/db.cc
 *
 *-------------------------------------------------------------------------
 */

#include "db_directory.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "db.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_fixture.h"
#include "test_util.h"
#include "ut.h"
#include "wal.h"

/** Bound a hook wait so a failed interleaving can unwind. */
static bool wait_for_worker(const std::atomic<bool> &done) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!done.load() && std::chrono::steady_clock::now() < deadline)
    std::this_thread::yield();

  return (done.load());
}

/** Seed a database with WAL-only data and a SQL table, then release all
locks. */
static void seed_directory(const std::string &path, int64_t id = 1) {
  db *database = nullptr;
  ut_a(db_open(path.c_str(), deferred_flush_options(), &database).ok());
  db_test_access::stop_flusher(*database);
  CHECK(write_id(*database, id).ok());
  CHECK(database
            ->exec(
                "CREATE TABLE ordinary(v INT); INSERT INTO ordinary VALUES (7)")
            .ok());

  db_close(database);
}

/** Write a scratch file without following or replacing an existing entry. */
static void directory_fixture_file(const std::string &path,
                                   const std::string &text = "owned") {
  os_file_t file = OS_FILE_CLOSED;
  CHECK(os_file_open(path.c_str(), OS_FILE_CREATE, file).ok());
  CHECK(os_file_write(file, text.data(), text.size(), 0).ok());
  CHECK(os_file_close(file).ok());
}

/** A normal closed database, missing path, and empty directory are
deletable. */
static void test_drop_database() {
  test_dir_t parent;
  const std::string path = parent.file("database");
  seed_directory(path);
  for (const char *name :
       {"IDENTITY", "INITIALIZING", "IDENTITY.tmp", "DELETING"}) {
    CHECK(access((path + "/" + name).c_str(), F_OK) != 0);
  }

  db *active = nullptr;
  REQUIRE(db_open(path.c_str(), deferred_flush_options(), &active).ok());
  CHECK(db_drop_database(path.c_str()).code() == DB_LOCKED);
  CHECK(all_ids(*active) == std::vector<int64_t>({1}));

  db_close(active);

  /* Include crash leftovers without replaying a damaged native WAL. */
  directory_fixture_file(path + "/_pt_.duckdb.wal", "damaged native WAL");
  directory_fixture_file(path + "/wal-00000000000000000002.pt.init",
                         "torn init");
  CHECK(mkdir((path + "/_pt_.duckdb.tmp").c_str(), 0755) == 0);
  for (const char *name :
       {"duckdb_temp_123_0_owner", "duckdb_temp_123_0_storage_DEFAULT-0.tmp",
        "duckdb_temp_123_0_block-1.block"}) {
    directory_fixture_file(path + "/_pt_.duckdb.tmp/" + name);
  }

  CHECK(db_drop_database((path + "///").c_str()).ok());
  CHECK(access(path.c_str(), F_OK) != 0);
  CHECK(db_drop_database(path.c_str()).ok());
  CHECK(db_drop_database(parent.file("missing/also_missing").c_str()).ok());
  CHECK(mkdir(path.c_str(), 0755) == 0);
  CHECK(db_drop_database(path.c_str()).ok());
  CHECK(mkdir(path.c_str(), 0755) == 0);

  ut_debug_global().arm("directory.drop.parent_sync", DB_IO_ERROR);
  CHECK(db_drop_database(path.c_str()).code() == DB_OUTCOME_UNKNOWN);
  CHECK(access(path.c_str(), F_OK) != 0);
  CHECK(db_drop_database(path.c_str()).ok());
  REQUIRE(db_open(path.c_str(), deferred_flush_options(), &active).ok());

  std::vector<db_table_info_t> tables;
  active->collect_tables(tables);
  CHECK(tables.empty());

  test_query_t result;
  db_cursor *cursor = nullptr;
  CHECK(active->open_cursor("SELECT * FROM ordinary", false, &cursor).code() ==
        DB_ERROR);
  CHECK(write_id(*active, 9).ok());

  db_close(active);
  REQUIRE(db_open(path.c_str(), deferred_flush_options(), &active).ok());
  CHECK(all_ids(*active) == std::vector<int64_t>({9}));

  db_close(active);
  CHECK(db_drop_database(path.c_str()).ok());
}

/** Unknown files, noncanonical names and nested/link entries fail before
marking. */
static void test_drop_database_refusal() {
  test_dir_t parent;
  const std::string path = parent.file("database");
  const std::string outside = parent.file("outside");
  directory_fixture_file(outside, "preserve");
  seed_directory(path);

  for (int kind = 0; kind < 6; ++kind) {
    const bool spill = kind >= 3;
    const std::string container = spill ? path + "/_pt_.duckdb.tmp" : path;

    if (spill) {
      CHECK(mkdir(container.c_str(), 0755) == 0);
    }

    const std::string extra =
        container + (spill       ? "/duckdb_temp_123_0_block-1.block"
                     : kind == 0 ? "/notes.txt"
                                 : "/wal-00000000000000000002.pt");

    if (kind == 0 || kind == 3) {
      directory_fixture_file(extra);
    } else if (kind == 1 || kind == 4) {
      CHECK(symlink(outside.c_str(), extra.c_str()) == 0);
    } else if (kind == 2) {
      CHECK(link(outside.c_str(), extra.c_str()) == 0);
    } else {
      CHECK(mkdir(extra.c_str(), 0755) == 0);
    }

    /* A recognized spill name is allowed; use an unknown suffix for the
    regular-file case. */
    std::string actual = extra;

    if (kind == 3) {
      actual = container + "/notes.txt";
      CHECK(rename(extra.c_str(), actual.c_str()) == 0);
    }

    CHECK(db_drop_database(path.c_str()).code() == DB_INVALID_ARGUMENT);
    CHECK(access((path + "/_pt_.duckdb").c_str(), F_OK) == 0);
    CHECK(access(outside.c_str(), F_OK) == 0);

    if (kind == 5) {
      CHECK(rmdir(actual.c_str()) == 0);
    } else {
      CHECK(unlink(actual.c_str()) == 0);
    }

    if (spill) {
      CHECK(rmdir(container.c_str()) == 0);
    }
  }

  /* A malformed file name is not covered by the WAL filename rule. */
  directory_fixture_file(path + "/wal-1.pt");
  CHECK(db_drop_database(path.c_str()).code() == DB_INVALID_ARGUMENT);
  CHECK(unlink((path + "/wal-1.pt").c_str()) == 0);

  db *database = nullptr;
  REQUIRE(db_open(path.c_str(), deferred_flush_options(), &database).ok());
  CHECK(all_ids(*database) == std::vector<int64_t>({1}));

  db_close(database);
  CHECK(db_drop_database(path.c_str()).ok());

  const std::string foreign = parent.file("foreign");
  CHECK(mkdir(foreign.c_str(), 0755) == 0);
  directory_fixture_file(foreign + "/notes.txt", "not a database");
  CHECK(db_drop_database(foreign.c_str()).code() == DB_INVALID_ARGUMENT);
  CHECK(access((foreign + "/LOCK").c_str(), F_OK) != 0);
  CHECK(unlink((foreign + "/notes.txt").c_str()) == 0);
  CHECK(rmdir(foreign.c_str()) == 0);
  CHECK(db_drop_database(nullptr).code() == DB_INVALID_ARGUMENT);
  CHECK(db_drop_database("").code() == DB_INVALID_ARGUMENT);
  CHECK(db_drop_database("/").code() == DB_INVALID_ARGUMENT);
  CHECK(db_drop_database(outside.c_str()).code() == DB_INVALID_ARGUMENT);
}

/** A final symlink cannot be used to open or drop the target database. */
static void test_drop_database_alias() {
  test_dir_t parent;
  const std::string path = parent.file("database"),
                    alias = parent.file("alias");
  seed_directory(path);
  CHECK(symlink(path.c_str(), alias.c_str()) == 0);
  CHECK(db_drop_database(alias.c_str()).code() == DB_INVALID_ARGUMENT);
  CHECK(db_drop_database((alias + "/").c_str()).code() == DB_INVALID_ARGUMENT);

  db *database = nullptr;
  CHECK(db_open(alias.c_str(), deferred_flush_options(), &database).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(database == nullptr);
  CHECK(unlink(alias.c_str()) == 0);
  CHECK(db_drop_database(path.c_str()).ok());
}

/** Failures cannot remove WAL until after the main-file unlink is synced. */
static void test_drop_database_failures() {
  const char *points[] = {
      "directory.drop.begin",      "directory.drop.file",
      "directory.drop.after_file", "directory.drop.storage_sync",
      "directory.drop.data_sync",  "directory.drop.finish",
      "directory.drop.remove_dir", "directory.drop.parent_sync"};

  for (size_t i = 0; i < sizeof(points) / sizeof(points[0]); ++i) {
    test_dir_t parent;
    const std::string path = parent.file("database");
    seed_directory(path);
    const std::string wal = path + "/" + wal_file_name(1);
    const std::string native_wal = path + "/_pt_.duckdb.wal";
    if (i == 2 || i == 3) {
      directory_fixture_file(native_wal,
                             "native WAL to keep until storage sync");
    }

    ut_debug_scope debug_scope;
    ut_debug_global().arm(points[i], DB_IO_ERROR);
    CHECK(db_drop_database(path.c_str()).code() ==
          (i == 0 ? DB_IO_ERROR : DB_OUTCOME_UNKNOWN));

    if (i < 4) {
      CHECK(access(wal.c_str(), F_OK) == 0);
      db *database = nullptr;
      const db_status_t opened =
          db_open(path.c_str(), deferred_flush_options(), &database);

      if (i < 2) {
        REQUIRE(opened.ok());
        CHECK(all_ids(*database) == std::vector<int64_t>({1}));
        db_close(database);
      } else {
        CHECK(opened.code() == DB_CORRUPTION);
        CHECK(database == nullptr);
        CHECK(access((path + "/_pt_.duckdb").c_str(), F_OK) != 0);
        CHECK(access(native_wal.c_str(), F_OK) == 0);
      }
    } else {
      db::options_t options = *deferred_flush_options();
      options.m_create_if_missing = false;
      db *database = nullptr;
      REQUIRE(db_open(path.c_str(), &options, &database).code() ==
              DB_INVALID_ARGUMENT);
      CHECK(database == nullptr);
      CHECK(access((path + "/_pt_.duckdb").c_str(), F_OK) != 0);
    }

    CHECK(db_drop_database(path.c_str()).ok());
    CHECK(access(path.c_str(), F_OK) != 0);

    db *database = nullptr;
    REQUIRE(db_open(path.c_str(), deferred_flush_options(), &database).ok());

    std::vector<db_table_info_t> tables;
    database->collect_tables(tables);
    CHECK(tables.empty());

    db_close(database);
    CHECK(db_drop_database(path.c_str()).ok());
  }
}

/** Existing-only open can acquire locks without initializing a directory. */
static void test_open_without_creation() {
  test_dir_t parent;
  const std::string path = parent.file("database");
  const std::string lock = path + "/LOCK";
  db::options_t options = *deferred_flush_options();
  options.m_create_if_missing = false;
  db *database = nullptr;

  for (const std::string &missing : {path, path + "/child", path + "/."}) {
    CHECK(db_open(missing.c_str(), &options, &database).code() ==
          DB_INVALID_ARGUMENT);
    CHECK(database == nullptr);
    CHECK(access(path.c_str(), F_OK) != 0);
  }

  REQUIRE(mkdir(path.c_str(), 0755) == 0);
  CHECK(db_open(path.c_str(), &options, &database).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(database == nullptr);
  std::vector<std::string> entries;
  REQUIRE(os_dir_list(path.c_str(), entries).ok());
  CHECK(entries.empty());

  seed_directory(path, 9);
  REQUIRE(unlink(lock.c_str()) == 0);
  REQUIRE(db_open(path.c_str(), &options, &database).ok());
  CHECK(access(lock.c_str(), F_OK) == 0);
  CHECK(all_ids(*database) == std::vector<int64_t>({9}));
  CHECK(db_drop_database(path.c_str()).code() == DB_LOCKED);
  db_close(database);
}

/** Existing directories remain usable and a legacy deletion stays resumable. */
static void test_legacy_directory_files() {
  test_dir_t parent;
  const std::string path = parent.file("database");
  seed_directory(path);
  directory_fixture_file(path + "/IDENTITY");

  db *database = nullptr;
  REQUIRE(db_open(path.c_str(), deferred_flush_options(), &database).ok());
  CHECK(all_ids(*database) == std::vector<int64_t>({1}));
  db_close(database);

  REQUIRE(rename((path + "/IDENTITY").c_str(), (path + "/DELETING").c_str()) ==
          0);
  CHECK(db_open(path.c_str(), deferred_flush_options(), &database).code() ==
        DB_FENCED);
  CHECK(database == nullptr);
  CHECK(db_drop_database(path.c_str()).ok());
}

/** A failed first catalog commit can be retried or cleaned up by Drop. */
static void test_initialization_cleanup() {
  test_dir_t parent;
  const std::string path = parent.file("database");
  auto options = *deferred_flush_options();
  ut_debug_global().arm("store.commit", DB_IO_ERROR);

  db *database = nullptr;
  CHECK(db_open(path.c_str(), &options, &database).code() ==
        DB_OUTCOME_UNKNOWN);
  CHECK(database == nullptr);
  CHECK(access((path + "/_pt_.duckdb").c_str(), F_OK) == 0);
  REQUIRE(db_open(path.c_str(), &options, &database).ok());
  CHECK(db_test_access::store_handle(*database).catalog().m_wal_id != 0);
  db_close(database);
  CHECK(db_drop_database(path.c_str()).ok());

  CHECK(mkdir(path.c_str(), 0755) == 0);
  directory_fixture_file(path + "/_pt_.duckdb", "incomplete storage");
  CHECK(!db_open(path.c_str(), &options, &database).ok());
  CHECK(database == nullptr);
  CHECK(db_drop_database(path.c_str()).ok());
}

/** Unmarked nonempty directories never bootstrap storage or a PulseTen
catalog. */
static void test_unmarked_nonempty_directory() {
  for (int kind = 0; kind < 4; ++kind) {
    test_dir_t dir;
    const std::string notes = dir.file("notes.txt");
    const std::string storage = dir.file("_pt_.duckdb");
    directory_fixture_file(notes, "preserve");

    if (kind == 1) {
      directory_fixture_file(storage, "");
    } else if (kind >= 2) {
      duckdb_database native = nullptr;
      REQUIRE(duckdb_open(storage.c_str(), &native) == DuckDBSuccess);

      duckdb_connection conn = nullptr;
      REQUIRE(duckdb_connect(native, &conn) == DuckDBSuccess);

      duckdb_result result{};
      const char *sql = kind == 2
                            ? "CREATE TABLE scratch(v INT); DROP TABLE scratch"
                            : "CREATE TABLE ordinary(v INT)";
      CHECK(duckdb_query(conn, sql, &result) == DuckDBSuccess);
      duckdb_destroy_result(&result);
      duckdb_disconnect(&conn);
      duckdb_close(&native);
    }

    db *database = nullptr;
    CHECK(!db_open(dir.m_path, deferred_flush_options(), &database).ok());
    CHECK(database == nullptr);
    CHECK(access(dir.file("INITIALIZING").c_str(), F_OK) != 0);
    CHECK(access(dir.file("IDENTITY").c_str(), F_OK) != 0);

    if (kind == 0) {
      CHECK(access(storage.c_str(), F_OK) != 0);
    } else if (kind >= 2) {
      duckdb_database native = nullptr;
      REQUIRE(duckdb_open(storage.c_str(), &native) == DuckDBSuccess);

      duckdb_connection conn = nullptr;
      REQUIRE(duckdb_connect(native, &conn) == DuckDBSuccess);

      duckdb_result result{};
      CHECK(duckdb_query(
                conn,
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema='main' AND left(table_name,4)='_pt_'",
                &result) == DuckDBSuccess);
      CHECK_EQ(duckdb_value_int64(&result, 0, 0), int64_t{0});
      duckdb_destroy_result(&result);
      duckdb_disconnect(&conn);
      duckdb_close(&native);
    }

    os_file_t file = OS_FILE_CLOSED;
    REQUIRE(os_file_open(notes.c_str(), OS_FILE_OPEN, file).ok());

    char content[8];
    CHECK(os_file_read(file, content, sizeof(content), 0).ok());
    CHECK(memcmp(content, "preserve", sizeof(content)) == 0);
    CHECK(os_file_close(file).ok());
  }

  /* Reopening an existing catalog keeps its data and needs no markers. */
  test_dir_t dir;
  seed_directory(dir.m_path, 7);

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, deferred_flush_options(), &database).ok());
  CHECK(all_ids(*database) == std::vector<int64_t>({7}));
  CHECK(access(dir.file("IDENTITY").c_str(), F_OK) != 0);
  CHECK(access(dir.file("INITIALIZING").c_str(), F_OK) != 0);

  db_close(database);
}

/** Missing or replaced completed storage must not replay rows removed by
Drop. */
static void test_wal_requires_storage() {
  for (int damage = 0; damage < 3; ++damage) {
    test_dir_t parent;
    const std::string path = parent.file("database");
    auto options = *deferred_flush_options();
    options.m_wal_sync = true;

    db *database = nullptr;
    REQUIRE(db_open(path.c_str(), &options, &database).ok());
    db_test_access::stop_flusher(*database);
    CHECK(write_id(*database, 7).ok());
    CHECK(database->drop_table("k").ok());

    db_close(database);
    database = nullptr;

    const std::string storage = path + "/_pt_.duckdb";

    if (damage == 1) {
      os_file_t file = OS_FILE_CLOSED;
      REQUIRE(os_file_open(storage.c_str(), OS_FILE_OPEN, file).ok());
      CHECK(os_file_truncate(file, 0).ok());
      CHECK(os_file_close(file).ok());
    } else {
      CHECK(unlink(storage.c_str()) == 0);

      if (damage == 2) {
        /* A valid but empty replacement file must not acquire a new catalog. */
        duckdb_database native = nullptr;
        REQUIRE(duckdb_open(storage.c_str(), &native) == DuckDBSuccess);

        duckdb_connection conn = nullptr;
        REQUIRE(duckdb_connect(native, &conn) == DuckDBSuccess);

        duckdb_result result{};
        CHECK(duckdb_query(conn,
                           "CREATE TABLE scratch(v INT); DROP TABLE scratch",
                           &result) == DuckDBSuccess);
        duckdb_destroy_result(&result);
        duckdb_disconnect(&conn);
        duckdb_close(&native);
      }
    }

    CHECK(db_open(path.c_str(), &options, &database).code() == DB_CORRUPTION);
    CHECK(database == nullptr);
    CHECK(access((path + "/INITIALIZING").c_str(), F_OK) != 0);

    if (damage == 0) {
      CHECK(access(storage.c_str(), F_OK) != 0);
    }

    /* Reopening again must keep refusing; it cannot convert damage to a new
    database. */
    CHECK(db_open(path.c_str(), &options, &database).code() == DB_CORRUPTION);
    CHECK(database == nullptr);
    CHECK(db_drop_database(path.c_str()).ok());
    REQUIRE(db_open(path.c_str(), &options, &database).ok());
    db_test_access::stop_flusher(*database);
    CHECK(write_id(*database, 9).ok());

    db_close(database);
    REQUIRE(db_open(path.c_str(), &options, &database).ok());
    CHECK(all_ids(*database) == std::vector<int64_t>({9}));

    db_close(database);
  }
}

/** An opener holding a descriptor to an old directory cannot touch its
replacement. */
static void test_directory_replacement() {
  test_dir_t parent;
  const std::string path = parent.file("database");
  seed_directory(path);

  std::atomic<bool> paused{false}, resume{false};
  ut_debug_global().set(
      "directory.before_lock",
      [&] {
        paused = true;

        while (!resume.load()) {
          std::this_thread::yield();
        }
      },
      1);

  db_status_t attempted;

  std::thread opener([&] {
    db::options_t options = *deferred_flush_options();
    options.m_create_if_missing = false;
    db *database = nullptr;
    attempted = db_open(path.c_str(), &options, &database);
    db_close(database);
  });
  CHECK(wait_for_worker(paused));
  CHECK(db_drop_database(path.c_str()).ok());
  seed_directory(path, 9);
  resume = true;
  opener.join();
  CHECK(attempted.code() == DB_LOCKED);

  db *database = nullptr;
  REQUIRE(db_open(path.c_str(), deferred_flush_options(), &database).ok());
  CHECK(all_ids(*database) == std::vector<int64_t>({9}));

  db_close(database);
  CHECK(db_drop_database(path.c_str()).ok());
}

/** Drop is exclusive across processes and independent of later chdir calls. */
static void test_drop_database_process_and_paths() {
  test_dir_t parent;
  const std::string path = parent.file("database");
  seed_directory(path);

  db *database = nullptr;
  REQUIRE(db_open(path.c_str(), deferred_flush_options(), &database).ok());

  pid_t pid = fork();
  if (pid == 0) {
    _exit(db_drop_database(path.c_str()).code() == DB_LOCKED ? 0 : 1);
  }

  REQUIRE(pid > 0);

  int status = 0;
  CHECK(waitpid(pid, &status, 0) == pid && WIFEXITED(status) &&
        WEXITSTATUS(status) == 0);

  db_close(database);

  const int cwd = open(".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  REQUIRE(cwd >= 0);
  CHECK(chdir(parent.m_path) == 0);

  ut_debug_global().set(
      "directory.drop.before_remove", [] { CHECK(chdir("/") == 0); }, 1);
  CHECK(db_drop_database("database").ok());
  CHECK(fchdir(cwd) == 0);
  CHECK(close(cwd) == 0);
  CHECK(access(path.c_str(), F_OK) != 0);
}

/** Preflight rejects unrecognized files before deleting data. */
static void test_drop_database_preflight_change() {
  test_dir_t parent;
  const std::string path = parent.file("database");
  seed_directory(path);

  ut_debug_global().set(
      "directory.drop.before_remove",
      [&] { directory_fixture_file(path + "/notes.txt"); }, 1);
  CHECK(db_drop_database(path.c_str()).code() == DB_INVALID_ARGUMENT);
  CHECK(access((path + "/_pt_.duckdb").c_str(), F_OK) == 0);
  CHECK(unlink((path + "/notes.txt").c_str()) == 0);
  CHECK(db_drop_database(path.c_str()).ok());
}

/** Count live descriptors after the listing's own directory stream closes. */
static size_t open_descriptor_count() {
  std::vector<std::string> names;
  CHECK(os_dir_list("/dev/fd", names).ok());

  size_t count = 0;

  for (const std::string &name : names) {
    const int fd = static_cast<int>(strtol(name.c_str(), nullptr, 10));
    count += fcntl(fd, F_GETFD) != -1;
  }

  return (count);
}

/** Repeated failed initialization, preflight and WAL recovery release files. */
static void test_failed_operations_release_files() {
  test_dir_t parent;
  const std::string path = parent.file("database");
  seed_directory(path);

  const std::string spill = path + "/_pt_.duckdb.tmp";
  REQUIRE(mkdir(spill.c_str(), 0755) == 0);
  directory_fixture_file(spill + "/foreign", "preserve");

  const size_t baseline = open_descriptor_count();
  const std::string broken = parent.file("broken");
  REQUIRE(mkdir(broken.c_str(), 0755) == 0);
  directory_fixture_file(broken + "/" + wal_file_name(1), "broken WAL");
  directory_fixture_file(parent.file(wal_file_name(1).c_str()), "broken WAL");

  for (int i = 0; i < 32; ++i) {
    CHECK(db_drop_database(path.c_str()).code() == DB_INVALID_ARGUMENT);

    db *database = nullptr;
    CHECK(db_open(broken.c_str(), deferred_flush_options(), &database).code() ==
          DB_CORRUPTION);
    CHECK(database == nullptr);

    wal *log = nullptr;
    wal_recovery_t recovery;
    CHECK(wal::open(parent.m_path, nullptr, nullptr, &log, recovery).code() ==
          DB_CORRUPTION);
    CHECK(log == nullptr);
    CHECK_EQ(open_descriptor_count(), baseline);
  }
}

/** Run directory lifecycle and filesystem failure tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"failed_operations_release_files", test_failed_operations_release_files},
      {"unmarked_nonempty_directory", test_unmarked_nonempty_directory},
      {"initialization_cleanup", test_initialization_cleanup},
      {"wal_requires_storage", test_wal_requires_storage},
      {"drop_database", test_drop_database},
      {"drop_database_refusal", test_drop_database_refusal},
      {"drop_database_alias", test_drop_database_alias},
      {"drop_database_failures", test_drop_database_failures},
      {"open_without_creation", test_open_without_creation},
      {"legacy_directory_files", test_legacy_directory_files},
      {"directory_replacement", test_directory_replacement},
      {"drop_database_process_and_paths", test_drop_database_process_and_paths},
      {"drop_database_preflight_change", test_drop_database_preflight_change},
  };

  return (test_main("db_directory-t", argc, argv, cases));
}
