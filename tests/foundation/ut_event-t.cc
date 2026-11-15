/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_event-t.cc
 *    Tests for ut_event: retained signals, resets racing with waits,
 *    concurrent waiters, and handoff between threads.
 *
 * Covers: src/ut/ut.h, src/ut/ut.cc
 *
 *-------------------------------------------------------------------------
 */

#include "ut.h"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "test_util.h"

/** Signals persist for late waiters until explicitly reset. */
static void test_retained_signal() {
  ut_event_t event;
  CHECK(!event.wait_for(std::chrono::milliseconds(0)));

  event.set();
  CHECK(event.wait_for(std::chrono::milliseconds(0)));
  CHECK(event.wait_for(std::chrono::milliseconds(0)));
  event.wait();

  const uint64_t count = event.reset();
  CHECK(!event.wait_for(std::chrono::milliseconds(0), count));

  const auto start = std::chrono::steady_clock::now();
  CHECK(!event.wait_for(std::chrono::milliseconds(5), count));
  CHECK(std::chrono::steady_clock::now() - start >=
        std::chrono::milliseconds(5));
}

/** Another waiter's reset cannot erase a signal after our reset. */
static void test_intervening_reset() {
  ut_event_t event;
  const uint64_t before = event.reset();
  event.set();

  const uint64_t after = event.reset();

  CHECK(after != before);
  CHECK(event.wait_for(std::chrono::milliseconds(0), before));
  event.wait(before);
  CHECK(!event.wait_for(std::chrono::milliseconds(0), after));
}

/** One signal releases every waiter, even if another thread resets it. */
static void test_waiters() {
  ut_event_t event;
  std::atomic<unsigned> ready{0};
  std::atomic<unsigned> completed{0};
  std::vector<std::thread> threads;

  for (unsigned i = 0; i < 16; ++i) {
    threads.emplace_back([&] {
      const uint64_t count = event.reset();
      ++ready;

      if (event.wait_for(std::chrono::seconds(5), count)) {
        ++completed;
      }
    });
  }

  while (ready.load() != threads.size()) {
    std::this_thread::yield();
  }

  event.set();
  event.reset();

  for (std::thread &thread : threads) {
    thread.join();
  }

  CHECK(completed == threads.size());
}

/** Repeated handoffs publish payloads and tolerate early notifications. */
static void test_handoff() {
  ut_event_t request;
  ut_event_t response;
  unsigned payload = 0;
  std::atomic<bool> valid{true};

  std::thread worker([&] {
    for (unsigned i = 1; i <= 10000; ++i) {
      if (!request.wait_for(std::chrono::seconds(5))) {
        valid = false;

        return;
      }

      request.reset();

      if (payload != i) {
        valid = false;
      }

      response.set();
    }
  });

  for (unsigned i = 1; i <= 10000; ++i) {
    const uint64_t count = response.reset();
    payload = i;
    request.set();

    if (!response.wait_for(std::chrono::seconds(5), count)) {
      valid = false;
      break;
    }
  }

  worker.join();
  CHECK(valid);
}

/** Run event signaling, timeout and missed-notification tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"retained_signal", test_retained_signal},
      {"intervening_reset", test_intervening_reset},
      {"waiters", test_waiters},
      {"handoff", test_handoff},
  };

  return (test_main("ut_event-t", argc, argv, cases));
}
