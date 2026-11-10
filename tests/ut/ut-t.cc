/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut-t.cc
 *    Tests for the ut module.
 *
 * Covers assertion macros, error codes and CRC-32C.
 *
 *-------------------------------------------------------------------------
 */

#include "ut.h"

#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <type_traits>

#include "ut_crc32.h"
#include "ut_err.h"
#include "ut_mutex.h"

static int failures = 0;

/** Record a failed check without stopping the test. */
static void check(bool ok, const char *what, int line) {
  if (!ok) {
    fprintf(stderr, "FAIL line %d: %s\n", line, what);
    ++failures;
  }
}

#define CHECK(EXPR) check((EXPR), #EXPR, __LINE__)

/** Run fn in a child process and capture its standard error.
@return true if the child was terminated by SIGABRT */
static bool aborts(void (*fn)(), std::string &output) {
  int fds[2];
  if (pipe(fds) != 0) {
    return (false);
  }
  pid_t pid = fork();
  if (pid < 0) {
    return (false);
  }
  if (pid == 0) {
    dup2(fds[1], STDERR_FILENO);
    close(fds[0]);
    close(fds[1]);
    fn();
    _exit(0);
  }
  close(fds[1]);
  char buf[512];
  ssize_t n;
  while ((n = read(fds[0], buf, sizeof(buf))) > 0) {
    output.append(buf, static_cast<size_t>(n));
  }
  close(fds[0]);
  int status = 0;
  waitpid(pid, &status, 0);

  return (WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
}

/** Fail an assertion; run in a child process. */
static void ut_a_false() {
  ut_a(1 + 1 == 3);
}
/** Pass an assertion. */
static void ut_a_true() {
  ut_a(1 + 1 == 2);
}
/** Reach ut_error; run in a child process. */
static void ut_error_reached() {
  ut_error;
}
/** Fail a debug assertion; run in a child process. */
static void ut_ad_false() {
  ut_ad(false);
}

/** ut_a() aborts with the condition, file and line when false. */
static void test_ut_a() {
  std::string out;
  CHECK(aborts(ut_a_false, out));
  CHECK(out.find("1 + 1 == 3") != std::string::npos);
  CHECK(out.find("ut-t.cc") != std::string::npos);

  out.clear();
  CHECK(!aborts(ut_a_true, out));
}

/** ut_error aborts and says the code was unreachable. */
static void test_ut_error() {
  std::string out;
  CHECK(aborts(ut_error_reached, out));
  CHECK(out.find("unreachable") != std::string::npos);
}

/** ut_ad() checks only in debug builds and evaluates nothing otherwise. */
static void test_ut_ad() {
  std::string out;
  int evaluated = 0;
  ut_ad(++evaluated > 0);
#ifdef PT_DEBUG
  CHECK(evaluated == 1);
  CHECK(aborts(ut_ad_false, out));
#else
  CHECK(evaluated == 0);
  CHECK(!aborts(ut_ad_false, out));
#endif
}

/** Condition-variable waits release ownership and restore it on return. */
static void test_mutex_wait() {
  ut_mutex_t mutex;
  ut_condition_variable_t ready_cv;
  bool ready = false;
  ut_unique_lock_t lock(mutex);
  ut_ad(mutex.is_owned());

  std::thread producer([&] {
    ut_lock_guard_t producer_lock(mutex);
    ut_ad(mutex.is_owned());
    ready = true;
    ready_cv.notify_one();
  });

  CHECK(ready_cv.wait_for(lock, std::chrono::seconds(5), [&] {
    ut_ad(mutex.is_owned());

    return (ready);
  }));
  ut_ad(mutex.is_owned());
  lock.unlock();
  producer.join();
  ut_ad(!mutex.is_owned());
}

#ifdef PT_DEBUG
/** The owner is distinguished from another thread while the mutex is locked. */
static void test_mutex_owner() {
  ut_mutex_t mutex;
  CHECK(!mutex.is_owned());
  mutex.lock();
  CHECK(mutex.is_owned());

  std::thread other([&] {
    CHECK(!mutex.is_owned());
    CHECK(!mutex.try_lock());
  });
  other.join();

  CHECK(mutex.is_owned());
  mutex.unlock();
  CHECK(!mutex.is_owned());
  if (mutex.try_lock()) {
    CHECK(mutex.is_owned());
    mutex.unlock();
    CHECK(!mutex.is_owned());
  }
}

/** Check an unheld lock; run in a child process. */
static void mutex_missing_lock() {
  ut_mutex_t mutex;
  ut_ad(mutex.is_owned());
}

/** Recursively acquire a non-recursive mutex; run in a child process. */
static void mutex_recursive_lock() {
  ut_mutex_t mutex;
  mutex.lock();
  mutex.lock();
}

/** Try to unlock from another thread; run in a child process. */
static void mutex_wrong_unlock() {
  ut_mutex_t mutex;
  mutex.lock();
  std::thread other([&] { mutex.unlock(); });
  other.join();
}

/** Missing ownership, recursive locking and foreign unlocking fail early. */
static void test_mutex_assertions() {
  std::string out;
  CHECK(aborts(mutex_missing_lock, out));
  CHECK(out.find("mutex.is_owned()") != std::string::npos);
  out.clear();
  CHECK(aborts(mutex_recursive_lock, out));
  out.clear();
  CHECK(aborts(mutex_wrong_unlock, out));
}
#else
static_assert(std::is_same<ut_mutex_t, std::mutex>::value,
              "release mutex must remain the standard type");
static_assert(
    std::is_same<ut_condition_variable_t, std::condition_variable>::value,
    "release condition variable must remain the standard type");
#endif

/** Every error code has its own non-empty description. */
static void test_strerr() {
  const dberr_t codes[] = {
      DB_SUCCESS,        DB_ERROR,           DB_OUT_OF_MEMORY,
      DB_IO_ERROR,       DB_CORRUPTION,      DB_INVALID_ARGUMENT,
      DB_FENCED,         DB_LOCKED,          DB_SCHEMA_CONFLICT,
      DB_LIMIT_EXCEEDED, DB_OUTCOME_UNKNOWN, DB_NOT_FOUND,
      DB_INTERRUPTED};
  std::set<std::string> seen;
  for (size_t i = 0; i < sizeof(codes) / sizeof(codes[0]); ++i) {
    const char *text = ut_strerr(codes[i]);
    CHECK(text != nullptr && text[0] != '\0');
    CHECK(seen.insert(text).second);
    CHECK(strcmp(text, "unknown error") != 0);
  }
}

/** Status values own their diagnostics, copy safely and keep code and message
 * together. */
static void test_status() {
  const db_status_t success;
  CHECK(success.ok());
  CHECK(success.code() == DB_SUCCESS);
  CHECK(success.message().empty());
  CHECK(success.with_context("ignored").ok());

  const db_status_t generic = DB_IO_ERROR;
  CHECK(!generic.ok());
  CHECK(generic.message() == ut_strerr(DB_IO_ERROR));

  std::string text = "original cause";
  const db_status_t original(DB_IO_ERROR, text);
  text = "changed by caller";
  const db_status_t copy = original;
  CHECK(copy.message() == "original cause");

  db_status_t transferred;
  std::thread worker([&] {
    const db_status_t local = original.with_context("write WAL");
    transferred = local;
  });
  worker.join();
  CHECK(transferred.code() == DB_IO_ERROR);
  CHECK(transferred.message() == "write WAL: original cause");
  CHECK(original.message() == "original cause");

  const db_status_t fenced = transferred.with_code(DB_FENCED);
  CHECK(fenced.code() == DB_FENCED);
  CHECK(fenced.message() == transferred.message());
  CHECK(fenced.with_code(DB_SUCCESS).message().empty());
  CHECK(db_status_t(DB_SUCCESS, "ignored").message().empty());
  CHECK(db_status_t(DB_CORRUPTION, "").message() == ut_strerr(DB_CORRUPTION));
}

/** CRC-32C gives the standard check value, also when chained. */
static void test_crc32c() {
  /* Standard check value of CRC-32C. */
  CHECK(ut_crc32c("123456789", 9, 0) == 0xE3069283);
  CHECK(ut_crc32c("", 0, 0) == 0);
  /* Chaining over a split buffer gives the same result. */
  CHECK(ut_crc32c("6789", 4, ut_crc32c("12345", 5, 0)) == 0xE3069283);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_ut_a();
  test_ut_error();
  test_ut_ad();
  test_mutex_wait();
#ifdef PT_DEBUG
  test_mutex_owner();
  test_mutex_assertions();
#endif
  test_strerr();
  test_status();
  test_crc32c();
  if (failures != 0) {
    fprintf(stderr, "%d check(s) failed\n", failures);

    return (1);
  }
  printf("ut-t: all checks passed\n");

  return (0);
}
