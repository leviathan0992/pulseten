/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * os0file-t.cc
 *    Directory creation persists its parent and propagates sync failures.
 *
 *-------------------------------------------------------------------------
 */

#include <dlfcn.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>

#include "os0file.h"
#include "pulseten.h"
#include "test0util.h"

/** Interpose fsync in this executable only, to check the actual directory
fd and inject an I/O error without a production test hook. */
static struct stat expected_parent = {};
static int sync_calls = 0;
static int wrong_parent = 0;
static bool fail_sync = false;

typedef int (*fsync_fn_t)(int);

/** Observe the directory descriptor and inject the test's sync failure.
@param[in]  fd  directory descriptor observed before syncing or injecting EIO
@return 0 on successful fsync, or -1 with errno set for the injected/real
  failure */
extern "C" int fsync(int fd) {
  struct stat actual;
  if (fstat(fd, &actual) != 0 || actual.st_dev != expected_parent.st_dev ||
      actual.st_ino != expected_parent.st_ino) {
    ++wrong_parent;
  }
  ++sync_calls;

  if (fail_sync) {
    errno = EIO;

    return (-1);
  }

  static fsync_fn_t real_fsync =
      reinterpret_cast<fsync_fn_t>(dlsym(RTLD_NEXT, "fsync"));
  if (real_fsync == nullptr) {
    errno = ENOSYS;

    return (-1);
  }

  return (real_fsync(fd));
}

/** Expect synchronization of this parent directory.
@param[in]  path  directory whose device and inode the next fsync calls must
                  match */
static void expect_parent(const char *path) {
  CHECK(stat(path, &expected_parent) == 0);
  sync_calls = 0;
  wrong_parent = 0;
  fail_sync = false;
}

/** Creating a directory syncs its parent, also with trailing slashes and when
it already exists. */
static void test_parent_paths() {
  test_dir_t parent;
  expect_parent(parent.path);
  const std::string first = parent.file("first");
  const std::string trailing = parent.file("trailing");

  CHECK(os_dir_create(first.c_str()) == DB_SUCCESS);
  CHECK(os_dir_create(first.c_str()) == DB_SUCCESS);
  CHECK(os_dir_create((trailing + "///").c_str()) == DB_SUCCESS);
  CHECK(sync_calls == 3 && wrong_parent == 0);

  /* A relative name without a slash must sync ".". */
  const int cwd = open(".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  CHECK(cwd >= 0);
  if (cwd >= 0) {
    CHECK(chdir(parent.path) == 0);
    CHECK(os_dir_create("relative///") == DB_SUCCESS);
    CHECK(sync_calls == 4 && wrong_parent == 0);
    CHECK(rmdir("relative") == 0);
    CHECK(fchdir(cwd) == 0);
    CHECK(close(cwd) == 0);
  }

  CHECK(rmdir(first.c_str()) == 0);
  CHECK(rmdir(trailing.c_str()) == 0);
}

/** A failed parent sync is reported, and a retry syncs again. */
static void test_failed_sync_and_retry() {
  test_dir_t parent;
  const std::string path = parent.file("retry");
  expect_parent(parent.path);
  fail_sync = true;
  CHECK(os_dir_create(path.c_str()) == DB_IO_ERROR);
  CHECK(os_last_errno() == EIO);
  struct stat created;
  CHECK(stat(path.c_str(), &created) == 0 && S_ISDIR(created.st_mode));

  /* The failed attempt left the directory; EEXIST must retry the sync. */
  fail_sync = false;
  CHECK(os_dir_create(path.c_str()) == DB_SUCCESS);
  CHECK(sync_calls == 2 && wrong_parent == 0);
  CHECK(rmdir(path.c_str()) == 0);
}

/** Creating a directory where a file is fails with ENOTDIR. */
static void test_failed_create() {
  test_dir_t parent;
  expect_parent(parent.path);
  const std::string path = parent.file("file");
  os_file_t file = OS_FILE_CLOSED;
  CHECK(os_file_open(path.c_str(), OS_FILE_CREATE, file) == DB_SUCCESS);
  CHECK(os_file_close(file) == DB_SUCCESS);
  CHECK(os_dir_create(path.c_str()) == DB_IO_ERROR);
  CHECK(os_last_errno() == ENOTDIR);
  CHECK(os_dir_create(parent.file("missing/child").c_str()) == DB_IO_ERROR);
  CHECK(sync_calls == 0);
}

/** pulseten_open() reports a failed directory sync. */
static void test_open_propagates_sync_error() {
  test_dir_t parent;
  const std::string path = parent.file("db");
  expect_parent(parent.path);
  fail_sync = true;
  pulseten_db_t *db = nullptr;
  CHECK(pulseten_open(path.c_str(), nullptr, &db) == PULSETEN_IO_ERROR);
  CHECK(db == nullptr);
  CHECK(sync_calls == 1 && wrong_parent == 0);
  fail_sync = false;
  CHECK(rmdir(path.c_str()) == 0);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_parent_paths();
  test_failed_sync_and_retry();
  test_failed_create();
  test_open_propagates_sync_error();

  return (test_result("os0file-t"));
}
