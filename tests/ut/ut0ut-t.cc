/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut0ut-t.cc
 *    Tests for the ut module.
 *
 * Covers assertion macros, error codes and CRC-32C.
 *
 *-------------------------------------------------------------------------
 */

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <set>
#include <string>

#include "ut0crc32.h"
#include "ut0err.h"
#include "ut0ut.h"

static int failures = 0;

/** Record a failed check without stopping the test.
@param[in]  ok    check result
@param[in]  what  text of the checked expression
@param[in]  line  source line of the check */
static void check(bool ok, const char *what, int line) {
  if (!ok) {
    fprintf(stderr, "FAIL line %d: %s\n", line, what);
    ++failures;
  }
}

#define CHECK(EXPR) check((EXPR), #EXPR, __LINE__)

/** Run fn in a child process and capture its standard error.
@param[in]   fn      function to run in the child
@param[out]  output  everything the child wrote to standard error
@return true if the child was terminated by SIGABRT */
static bool aborts(void (*fn)(), std::string *output) {
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
    output->append(buf, (size_t)n);
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
  CHECK(aborts(ut_a_false, &out));
  CHECK(out.find("1 + 1 == 3") != std::string::npos);
  CHECK(out.find("ut0ut-t.cc") != std::string::npos);

  out.clear();
  CHECK(!aborts(ut_a_true, &out));
}

/** ut_error aborts and says the code was unreachable. */
static void test_ut_error() {
  std::string out;
  CHECK(aborts(ut_error_reached, &out));
  CHECK(out.find("unreachable") != std::string::npos);
}

/** ut_ad() checks only in debug builds and evaluates nothing otherwise. */
static void test_ut_ad() {
  std::string out;
  int evaluated = 0;
  ut_ad(++evaluated > 0);
#ifdef PT_DEBUG
  CHECK(evaluated == 1);
  CHECK(aborts(ut_ad_false, &out));
#else
  CHECK(evaluated == 0);
  CHECK(!aborts(ut_ad_false, &out));
#endif
}

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
    CHECK(strcmp(text, "Unknown error") != 0);
  }
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
  test_strerr();
  test_crc32c();
  if (failures != 0) {
    fprintf(stderr, "%d check(s) failed\n", failures);

    return (1);
  }
  printf("ut0ut-t: all checks passed\n");

  return (0);
}
