/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_debug-t.cc
 *    Global fault counts, reset boundaries and reentrant synchronization.
 *
 * Covers: src/ut/ut_debug.h, src/ut/ut_debug.cc
 *
 *-------------------------------------------------------------------------
 */

#include "ut_debug.h"

#include <atomic>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

#include "test_db.h"
#include "test_db_access.h"
#include "test_fixture.h"
#include "test_util.h"

/** Abort promptly if callback reentry prevents the operation from returning. */
template <typename Function>
static void run_bounded(Function action) {
  ut_event_t finished;
  std::thread watchdog([&] {
    if (!finished.wait_for(std::chrono::seconds(5))) {
      fprintf(stderr, "callback operation exceeded its deadline\n");
      std::abort();
    }
  });

  action();
  finished.set();
  watchdog.join();

  return;
}

/** Clearing or replacing a callback may release resources that reenter. */
static void test_callback_release(bool replace) {
  run_bounded([&] {
    ut_debug &debug = ut_debug_global();
    int releases = 0, replacements = 0;
    auto resource = std::shared_ptr<int>(new int(7), [&](int *value) {
      delete value;
      ++releases;
      debug.arm("released", DB_IO_ERROR);
    });

    debug.set("point", [resource] {});
    resource.reset();

    if (replace) {
      debug.set("point", [&] { ++replacements; }, 1);
    } else {
      debug.clear("point");
    }

    CHECK_EQ(releases, 1);
    CHECK(UT_DEBUG_FAULT("released").code() == DB_IO_ERROR);

    UT_DEBUG_SYNC("point");
    CHECK_EQ(replacements, replace ? 1 : 0);

    debug.clear("point");
  });

  return;
}

/** Copying the registered function may configure the same test context. */
struct reentrant_copy_t {
  /** Enables reentry for the execution copy. */
  const bool &m_active;

  /** Keep the activation flag for subsequent execution copies. */
  explicit reentrant_copy_t(const bool &active) : m_active(active) {}

  /** Exercise copy construction outside the context mutex. */
  reentrant_copy_t(const reentrant_copy_t &other) : m_active(other.m_active) {
    if (m_active) {
      ut_debug_global().arm("copied", DB_IO_ERROR);
    }
  }

  /** The copy must finish before the callback is invoked. */
  void operator()() const {
    CHECK(UT_DEBUG_FAULT("copied").code() == DB_IO_ERROR);
  }
};

/** A callable's copy constructor may reenter during run(). */
static void test_callback_copy_reentry() {
  run_bounded([] {
    ut_debug &debug = ut_debug_global();
    bool active = false;

    debug.set("point", reentrant_copy_t(active), 1);
    active = true;

    UT_DEBUG_SYNC("point");
    CHECK_EQ(debug.failures("copied"), uint64_t{1});

    debug.clear("point");
  });

  return;
}

/** Coordinate a copy while another thread removes its registration. */
struct callback_copy_state_t {
  /** Set before starting the execution thread. */
  bool m_pause{false};

  /** Execution has selected the original callable for copying. */
  ut_event_t m_entered;

  /** The original callable may finish copying its resource. */
  ut_event_t m_resume;

  /** Value read when the execution copy runs. */
  std::atomic<int> m_value{0};
};

/** Read the original capture after a concurrent clear or replacement. */
struct paused_copy_t {
  /** State kept alive until execution ends. */
  callback_copy_state_t &m_state;

  /** Resource whose ownership must survive selection. */
  std::shared_ptr<int> m_resource;

  /** Construct the first callable before enabling the copy pause. */
  paused_copy_t(callback_copy_state_t &state, std::shared_ptr<int> resource)
      : m_state(state), m_resource(std::move(resource)) {}

  /** Delay reading the capture until its registration has been removed. */
  paused_copy_t(const paused_copy_t &other) : m_state(other.m_state) {
    if (m_state.m_pause) {
      m_state.m_entered.set();
      m_state.m_resume.wait();
    }

    m_resource = other.m_resource;
  }

  /** Publish the original capture's value through the execution copy. */
  void operator()() const { m_state.m_value = *m_resource; }
};

/** Keep selected callbacks alive through concurrent clear or replacement. */
static void test_callback_copy_race(bool replace) {
  run_bounded([&] {
    ut_debug &debug = ut_debug_global();
    callback_copy_state_t state;
    std::atomic<int> releases{0};
    int replacements = 0;
    auto resource = std::shared_ptr<int>(new int(7), [&](int *value) {
      delete value;
      ++releases;
      debug.arm("released", DB_IO_ERROR);
    });

    debug.set("point", paused_copy_t(state, resource), 1);
    resource.reset();
    state.m_pause = true;

    std::thread worker([&] { UT_DEBUG_SYNC("point"); });
    state.m_entered.wait();

    if (replace) {
      debug.set("point", [&] { ++replacements; }, 1);
    } else {
      debug.clear("point");
    }

    CHECK_EQ(releases.load(), 0);

    UT_DEBUG_SYNC("point");
    CHECK_EQ(replacements, replace ? 1 : 0);

    state.m_resume.set();
    worker.join();

    CHECK_EQ(state.m_value.load(), 7);
    CHECK_EQ(releases.load(), 1);
    CHECK(UT_DEBUG_FAULT("released").code() == DB_IO_ERROR);

    debug.clear("point");
  });

  return;
}

/** Each execution receives its own copy of mutable callable state. */
static void test_callback_mutable_copy() {
  ut_debug &debug = ut_debug_global();
  std::atomic<int> reused{0}, calls{0};

  debug.set("point", [value = 0, &reused, &calls]() mutable {
    if (++value != 1) {
      ++reused;
    }

    ++calls;
  });

  std::vector<std::thread> workers;
  for (int i = 0; i < 4; ++i) {
    workers.emplace_back([&] {
      for (int j = 0; j < 100; ++j) {
        UT_DEBUG_SYNC("point");
      }
    });
  }

  for (std::thread &worker : workers) {
    worker.join();
  }

  CHECK_EQ(calls.load(), 400);
  CHECK_EQ(reused.load(), 0);

  return;
}

/** Fault counts survive rearming until the global controls are reset. */
static void test_fault_counts() {
  ut_debug &debug = ut_debug_global();

  CHECK(UT_DEBUG_FAULT("point").ok());
  UT_DEBUG_SYNC("point");

  debug.arm("point", DB_IO_ERROR, 2);
  const db_status_t err = UT_DEBUG_FAULT("point");
  CHECK(err.code() == DB_IO_ERROR);
  CHECK(err.message() == "injected point");
  CHECK(UT_DEBUG_FAULT("point").code() == DB_IO_ERROR);
  CHECK(UT_DEBUG_FAULT("point").ok());
  CHECK_EQ(debug.failures("point"), uint64_t{2});

  debug.arm("point", DB_OUT_OF_MEMORY, -1);
  CHECK(UT_DEBUG_FAULT("point").code() == DB_OUT_OF_MEMORY);
  CHECK(UT_DEBUG_FAULT("point").code() == DB_OUT_OF_MEMORY);
  debug.disarm("point");
  CHECK(UT_DEBUG_FAULT("point").ok());
  debug.arm("point", DB_ERROR, 0);
  CHECK(UT_DEBUG_FAULT("point").ok());
  CHECK_EQ(debug.failures("point"), uint64_t{4});

  return;
}

/** Callbacks may clear themselves and arm faults without holding the mutex. */
static void test_sync_reentry() {
  ut_debug &debug = ut_debug_global();
  int hits = 0;

  debug.set("pause", [&] {
    ++hits;
    debug.clear("pause");
    debug.arm("pause", DB_IO_ERROR);
  });

  UT_DEBUG_SYNC("pause");
  UT_DEBUG_SYNC("pause");
  CHECK_EQ(hits, 1);
  CHECK(UT_DEBUG_FAULT("pause").code() == DB_IO_ERROR);

  debug.set("pause", [&] { ++hits; }, 2);
  UT_DEBUG_SYNC("pause");
  UT_DEBUG_SYNC("pause");
  UT_DEBUG_SYNC("pause");
  CHECK_EQ(hits, 3);

  debug.set("pause", [&] { ++hits; });
  UT_DEBUG_SYNC("pause");
  UT_DEBUG_SYNC("pause");
  debug.clear("pause");
  UT_DEBUG_SYNC("pause");
  CHECK_EQ(hits, 5);

  return;
}

/** Test macros evaluate the point name exactly once. */
static void test_macro_arguments() {
  int names = 0;
  const auto name = [&] {
    ++names;

    return ("point");
  };

  CHECK(UT_DEBUG_FAULT(name()).ok());
  CHECK_EQ(names, 1);

  UT_DEBUG_SYNC(name());
  CHECK_EQ(names, 2);

  return;
}

/** Concurrent hits consume each armed fault and callback only once. */
static void test_concurrent_hits() {
  ut_debug &debug = ut_debug_global();
  debug.arm("fault", DB_IO_ERROR, 101);
  std::atomic<int> faults{0}, callbacks{0};

  debug.set("sync", [&] { ++callbacks; }, 37);

  std::vector<std::thread> workers;
  for (int i = 0; i < 8; ++i) {
    workers.emplace_back([&] {
      for (int j = 0; j < 100; ++j) {
        if (!UT_DEBUG_FAULT("fault").ok()) {
          ++faults;
        }

        UT_DEBUG_SYNC("sync");
      }
    });
  }

  for (std::thread &worker : workers) {
    worker.join();
  }

  CHECK_EQ(faults.load(), 101);
  CHECK_EQ(debug.failures("fault"), uint64_t{101});
  CHECK_EQ(callbacks.load(), 37);

  return;
}

/** Scope entry and exit clear faults, counts and callbacks. */
static void test_scope_reset() {
  ut_debug &debug = ut_debug_global();
  int calls = 0;
  debug.arm("old", DB_IO_ERROR, -1);
  CHECK(!UT_DEBUG_FAULT("old").ok());
  debug.set("old", [&] { ++calls; });

  {
    ut_debug_scope scope;
    CHECK(UT_DEBUG_FAULT("old").ok());
    CHECK_EQ(debug.failures("old"), uint64_t{0});
    UT_DEBUG_SYNC("old");
    CHECK_EQ(calls, 0);

    debug.arm("new", DB_IO_ERROR, -1);
    CHECK(!UT_DEBUG_FAULT("new").ok());
    debug.set("new", [&] { ++calls; });
  }

  CHECK(UT_DEBUG_FAULT("new").ok());
  CHECK_EQ(debug.failures("new"), uint64_t{0});
  UT_DEBUG_SYNC("new");
  CHECK_EQ(calls, 0);

  return;
}

/** Reset releases callbacks unlocked and discards controls they register. */
static void test_reset_reentry() {
  run_bounded([] {
    ut_debug &debug = ut_debug_global();
    int releases = 0, calls = 0;
    auto resource = std::shared_ptr<int>(new int(7), [&](int *value) {
      delete value;
      ++releases;
      debug.arm("released", DB_IO_ERROR);
      debug.set("released", [&] { ++calls; });
    });
    debug.set("point", [resource] {});
    resource.reset();

    debug.reset();
    CHECK_EQ(releases, 1);
    CHECK(UT_DEBUG_FAULT("released").ok());
    CHECK_EQ(debug.failures("released"), uint64_t{0});
    UT_DEBUG_SYNC("point");
    UT_DEBUG_SYNC("released");
    CHECK_EQ(calls, 0);
  });

  return;
}

/** Leave registrations for the runner to clear before the next case. */
static void test_case_reset_arm() {
  ut_debug_global().arm("leftover", DB_IO_ERROR, -1);
  CHECK(!UT_DEBUG_FAULT("leftover").ok());
  ut_debug_global().set("leftover", [] { std::abort(); });

  return;
}

/** A later case starts without faults, counts or callbacks from its
 * predecessor. */
static void test_case_reset_check() {
  CHECK(UT_DEBUG_FAULT("leftover").ok());
  CHECK_EQ(ut_debug_global().failures("leftover"), uint64_t{0});
  UT_DEBUG_SYNC("leftover");

  return;
}

/** Normal open paths use global controls, retaining counts across reopens. */
static void test_database_controls() {
  test_dir_t first_dir, second_dir;
  ut_debug &debug = ut_debug_global();
  db *a = nullptr, *b = nullptr;

  debug.arm("wal.write_header", DB_IO_ERROR);
  CHECK(db_open(first_dir.m_path, deferred_flush_options(), &a).code() ==
        DB_IO_ERROR);
  CHECK(a == nullptr);
  REQUIRE(db_open(first_dir.m_path, deferred_flush_options(), &a).ok());
  REQUIRE(db_open(second_dir.m_path, deferred_flush_options(), &b).ok());
  db_test_access::stop_flusher(*a);
  db_test_access::stop_flusher(*b);

  CHECK(write_id(*a, 1).ok());
  CHECK(write_id(*b, 2).ok());
  debug.arm("store.append", DB_OUT_OF_MEMORY);
  db::flush_info_t flushed;
  CHECK(b->flush(flushed).code() == DB_OUT_OF_MEMORY);
  CHECK(a->flush(flushed).ok());
  CHECK_EQ(debug.failures("store.append"), uint64_t{1});

  debug.arm("wal.partial_write", DB_IO_ERROR);
  CHECK(write_id(*a, 3).code() == DB_OUTCOME_UNKNOWN);
  CHECK(write_id(*b, 4).ok());
  CHECK(b->flush(flushed).ok());
  CHECK_EQ(debug.failures("wal.partial_write"), uint64_t{1});

  db_close(a);
  db_close(b);
  REQUIRE(db_open(first_dir.m_path, deferred_flush_options(), &a).ok());
  CHECK_EQ(debug.failures("wal.write_header"), uint64_t{1});
  CHECK_EQ(debug.failures("store.append"), uint64_t{1});
  CHECK_EQ(debug.failures("wal.partial_write"), uint64_t{1});
  db_close(a);

  debug.arm("directory.drop.begin", DB_IO_ERROR);
  CHECK(db_drop_database(second_dir.m_path).code() == DB_IO_ERROR);
  CHECK(db_drop_database(first_dir.m_path).ok());
  CHECK(db_drop_database(second_dir.m_path).ok());

  return;
}

/** Run the test-only global debug regressions. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"callback_release/clear", [] { test_callback_release(false); }},
      {"callback_release/replace", [] { test_callback_release(true); }},
      {"callback_copy_reentry", test_callback_copy_reentry},
      {"callback_copy_race/clear", [] { test_callback_copy_race(false); }},
      {"callback_copy_race/replace", [] { test_callback_copy_race(true); }},
      {"callback_mutable_copy", test_callback_mutable_copy},
      {"fault_counts", test_fault_counts},
      {"sync_reentry", test_sync_reentry},
      {"macro_arguments", test_macro_arguments},
      {"concurrent_hits", test_concurrent_hits},
      {"scope_reset", test_scope_reset},
      {"reset_reentry", test_reset_reentry},
      {"case_reset/arm", test_case_reset_arm},
      {"case_reset/check", test_case_reset_check},
      {"database_controls", test_database_controls},
  };

  return (test_main("ut_debug-t", argc, argv, cases));
}
