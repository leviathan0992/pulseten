/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * os_file-t.cc
 *    File I/O, failure outputs, and durable directory creation.
 *
 *-------------------------------------------------------------------------
 */

#include "os_file.h"
#include "pulseten.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>

#include <dlfcn.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "test_util.h"
#include "ut_err.h"

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

  return;
}

/** Creating a directory syncs its parent, also with trailing slashes and when
it already exists. */
static void test_parent_paths() {
  test_dir_t parent;
  expect_parent(parent.m_path);
  const std::string first = parent.file("first");
  const std::string trailing = parent.file("trailing");

  CHECK(os_dir_create(first.c_str()).ok());
  CHECK(os_dir_create(first.c_str()).ok());
  CHECK(os_dir_create((trailing + "///").c_str()).ok());
  CHECK(sync_calls == 3 && wrong_parent == 0);

  /* A relative name without a slash must sync ".". */
  const int cwd = open(".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  CHECK(cwd >= 0);
  if (cwd >= 0) {
    CHECK(chdir(parent.m_path) == 0);
    CHECK(os_dir_create("relative///").ok());
    CHECK(sync_calls == 4 && wrong_parent == 0);
    CHECK(rmdir("relative") == 0);
    CHECK(fchdir(cwd) == 0);
    CHECK(close(cwd) == 0);
  }

  CHECK(rmdir(first.c_str()) == 0);
  CHECK(rmdir(trailing.c_str()) == 0);

  return;
}

/** A failed parent sync is reported, and a retry syncs again. */
static void test_failed_sync_and_retry() {
  db_status_t status;

  test_dir_t parent;
  const std::string path = parent.file("retry");
  expect_parent(parent.m_path);
  fail_sync = true;
  status = os_dir_create(path.c_str());
  CHECK(status.code() == DB_IO_ERROR);
  CHECK(status.message().find(strerror(EIO)) != std::string::npos);
  CHECK(status.message().find(parent.m_path) != std::string::npos);
  CHECK(status.message().find("sync directory") != std::string::npos);
  struct stat created;
  CHECK(stat(path.c_str(), &created) == 0 && S_ISDIR(created.st_mode));

  /* The failed attempt left the directory; EEXIST must retry the sync. */
  fail_sync = false;
  status = os_dir_create(path.c_str());
  CHECK(status.ok());
  CHECK(sync_calls == 2 && wrong_parent == 0);
  CHECK(rmdir(path.c_str()) == 0);

  return;
}

/** Creating a directory where a file is fails with ENOTDIR. */
static void test_failed_create() {
  db_status_t status;

  test_dir_t parent;
  expect_parent(parent.m_path);
  const std::string path = parent.file("file");
  os_file_t file = OS_FILE_CLOSED;
  status = os_file_open(path.c_str(), OS_FILE_CREATE, file);
  CHECK(status.ok());
  status = os_file_close(file);
  CHECK(status.ok());
  status = os_dir_create(path.c_str());
  CHECK(status.code() == DB_IO_ERROR);
  CHECK(status.message().find(strerror(ENOTDIR)) != std::string::npos);
  CHECK(status.message().find(path) != std::string::npos);
  status = os_dir_create(parent.file("missing/child").c_str());
  CHECK(status.code() == DB_IO_ERROR);
  CHECK(sync_calls == 0);

  return;
}

/** pulseten_open() reports a failed directory sync. */
static void test_open_propagates_sync_error() {
  test_dir_t parent;
  const std::string path = parent.file("db");
  expect_parent(parent.m_path);
  fail_sync = true;
  pulseten_db_t *database = nullptr;
  CHECK(pulseten_open(path.c_str(), nullptr, &database) == PULSETEN_IO_ERROR);
  CHECK(database == nullptr);
  CHECK(sync_calls == 1 && wrong_parent == 0);
  fail_sync = false;
  CHECK(rmdir(path.c_str()) == 0);

  return;
}

/** Failed operations initialize their outputs and record the relevant error;
an exact read distinguishes a system failure from an unexpected EOF. */
static void test_file_errors_and_outputs() {
  db_status_t status;

  test_dir_t dir;
  os_file_t file = 42;
  status = os_file_open(dir.file("missing").c_str(), OS_FILE_OPEN, file);
  CHECK(status.code() == DB_IO_ERROR);
  CHECK(file == OS_FILE_CLOSED);
  CHECK(status.message().find(strerror(ENOENT)) != std::string::npos);
  CHECK(status.message().find(dir.file("missing")) != std::string::npos);
  CHECK(status.message().find("open file") != std::string::npos);

  uint64_t size = 99;
  status = os_file_get_size(OS_FILE_CLOSED, size);
  CHECK(status.code() == DB_IO_ERROR);
  CHECK(size == 0 && status.message() == strerror(EBADF));
  status = os_file_sync(OS_FILE_CLOSED);
  CHECK(status.code() == DB_IO_ERROR);
  CHECK(status.message() == strerror(EBADF));

  status = os_file_open(dir.file("data").c_str(), OS_FILE_CREATE, file);
  CHECK(status.ok());
  if (file == OS_FILE_CLOSED) {
    return;
  }

  const char data[] = "value";
  status = os_file_write(file, data, sizeof(data) - 1, 0);
  CHECK(status.ok());
  status = os_file_get_size(file, size);
  CHECK(status.ok() && size == 5);
  char readback[6] = {};
  status = os_file_read(file, readback, 3, 2);
  CHECK(status.ok());
  CHECK(memcmp(readback, "lue", 3) == 0);

  status = os_file_read(OS_FILE_CLOSED, readback, 1, 0);
  CHECK(status.code() == DB_IO_ERROR);
  CHECK(status.message() == strerror(EBADF));
  status = os_file_read(file, readback, sizeof(readback), 0);
  CHECK(status.code() == DB_IO_ERROR);
  CHECK(status.message() == "unexpected end of file");
  CHECK(memcmp(readback, data, sizeof(data) - 1) == 0);

  status = os_file_close(file);
  CHECK(status.ok());
  CHECK(file == OS_FILE_CLOSED);
  status = os_file_close(file);
  CHECK(status.ok());

  return;
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_parent_paths();
  test_failed_sync_and_retry();
  test_failed_create();
  test_open_propagates_sync_error();
  test_file_errors_and_outputs();

  return (test_result("os_file-t"));
}
