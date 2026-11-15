/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_crash.h
 *    Crash tests: a child process works and acknowledges its progress on a
 *    pipe, the parent kills it with SIGKILL at a chosen point, then collects
 *    every acknowledgement the child wrote before it died.
 *
 * An acknowledgement is a fixed-size value. Pipe writes of at most PIPE_BUF
 * bytes are atomic, so acknowledgements from several child threads never
 * interleave. The parent checks recovery against what was acknowledged.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_crash_h
#define test_crash_h

#include <climits>
#include <csignal>
#include <cstdlib>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "test_util.h"

/** Acknowledge one step from a child process; a failed write ends the child
with exit status 4. */
template <typename T>
static inline void test_crash_ack(int fd, const T &ack) {
  static_assert(sizeof(T) <= PIPE_BUF, "acknowledgements must stay atomic");

  if (write(fd, &ack, sizeof(ack)) != static_cast<ssize_t>(sizeof(ack))) {
    _exit(4);
  }

  return;
}

/** Fork a child that runs child(fd) and acknowledges values of type T on fd.
Read wanted acknowledgements, kill the child with SIGKILL, then read those it
wrote before dying. on_ack(const T &) sees each one in the order read. A child
that fails early sends fewer than wanted acknowledgements and exits without
SIGKILL; both are reported as failed checks.
@param[in]  wanted  acknowledgements to read before the kill
@param[in]  child   void(int fd), run in the child; returning exits with 0 */
template <typename T, typename Child, typename OnAck>
static inline void test_kill_child(int wanted, Child child, OnAck on_ack) {
  int fds[2];
  REQUIRE(pipe(fds) == 0);

  const pid_t pid = fork();
  if (pid == 0) {
    close(fds[0]);
    child(fds[1]);
    _exit(0);
  }

  close(fds[1]);

  if (pid < 0) {
    close(fds[0]);
  }

  REQUIRE(pid > 0);

  int received = 0;
  T ack;

  while (received < wanted &&
         read(fds[0], &ack, sizeof(ack)) == static_cast<ssize_t>(sizeof(ack))) {
    on_ack(ack);
    ++received;
  }

  kill(pid, SIGKILL);

  int status = 0;
  CHECK_EQ(waitpid(pid, &status, 0), pid);
  CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);

  /* Acknowledgements written between the last read and the kill. */
  while (read(fds[0], &ack, sizeof(ack)) == static_cast<ssize_t>(sizeof(ack))) {
    on_ack(ack);
  }

  close(fds[0]);
  CHECK_EQ(received, wanted);

  return;
}

#endif /* test_crash_h */
