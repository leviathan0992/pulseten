/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal0recv-t.cc
 *    Tests for opening and recovering an existing WAL.
 *
 * Crash states are produced two ways: by editing the file to look like an
 * interrupted write (cut records, garbage tails, partly written groups),
 * and by killing a writer process with SIGKILL while it appends.
 *
 *-------------------------------------------------------------------------
 */

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "os0file.h"
#include "test0util.h"
#include "test0wal.h"
#include "wal0wal.h"

/** Collects recovered payloads; payloads[seq - 1] is record seq. */
struct collect_t {
  /** Recovered payloads in sequence order. */
  std::vector<std::string> payloads;

  /** Return this error at record fail_at instead of collecting it. */
  batch_seq_t fail_at{0};
};

/** Replay callback: collect payloads in order, or fail at fail_at.
@param[in,out]  ctx      replay collector receiving record sequences and
                         payloads
@param[in]      seq      sequence number being replayed
@param[in]      payload  encoded record payload
@param[in]      len      input length in bytes
@return DB_SUCCESS, or DB_ERROR when this test injects a replay failure */
static dberr_t collect_apply(void *ctx, batch_seq_t seq,
                             const unsigned char *payload, size_t len) {
  collect_t *c = static_cast<collect_t *>(ctx);
  if (seq == c->fail_at) {
    return (DB_ERROR);
  }
  if (seq != c->payloads.size() + 1) {
    return (DB_CORRUPTION);
  }
  c->payloads.push_back(std::string((const char *)payload, len));

  return (DB_SUCCESS);
}

/** Build the deterministic payload of a test record.
@param[in]  i  zero-based row or record index
@return the payload of record i; lengths vary with i */
static std::string payload_for(int i) {
  char buf[64];
  snprintf(buf, sizeof(buf), "record-%d-%s", i,
           std::string(i % 50, 'x').c_str());

  return (buf);
}

/** Append records first..last (by payload_for) with a single writer, so every
record is a group of its own.
@param[in,out]  wal    write-ahead log
@param[in]      first  first payload index, inclusive
@param[in]      last   last payload index, inclusive */
static void append_range(wal_t *wal, int first, int last) {
  for (int i = first; i <= last; ++i) {
    std::string p = payload_for(i);
    batch_seq_t seq = 0;
    CHECK(append_record(*wal, p.data(), p.size(), seq) == DB_SUCCESS);
    CHECK(seq == (batch_seq_t)i);
  }
}

/** Create a WAL holding records 1..n.
@param[in]  dir  database or WAL directory
@param[in]  n    number of records to create */
static void make_wal(const test_dir_t &dir, int n) {
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.path, 0, &wal) == DB_SUCCESS);
  append_range(wal, 1, n);
  CHECK(wal_close(wal) == DB_SUCCESS);
}

/** Compute the WAL offset immediately after a generated record.
@param[in]  n  record number whose end offset is requested
@return file offset just after record n written by make_wal() */
static uint64_t end_of_record(int n) {
  uint64_t at = WAL_FILE_HEADER_SIZE;
  for (int i = 1; i <= n; ++i) {
    at += WAL_RECORD_HEADER_SIZE + payload_for(i).size();
  }

  return (at);
}

/** Read the bytes of a test file.
@param[in]  dir  database or WAL directory
@return the bytes of the first segment, or nothing if it cannot be read */
static std::vector<unsigned char> read_file(const test_dir_t &dir) {
  std::vector<unsigned char> data;
  os_file_t file;
  uint64_t size = 0;
  if (os_file_open(dir.file(wal_segment_name(1).c_str()).c_str(), OS_FILE_OPEN,
                   file) != DB_SUCCESS) {
    return (data);
  }
  if (os_file_get_size(file, size) == DB_SUCCESS && size > 0) {
    data.resize(size);
    if (os_file_read(file, &data[0], size, 0) != DB_SUCCESS) {
      data.clear();
    }
  }
  (void)os_file_close(file);

  return (data);
}

/** Replace the WAL file contents.
@param[in]  dir   database or WAL directory
@param[in]  data  replacement file contents */
static void write_file(const test_dir_t &dir,
                       const std::vector<unsigned char> &data) {
  os_file_t file;
  CHECK(os_file_open(dir.file(wal_segment_name(1).c_str()).c_str(),
                     OS_FILE_OPEN, file) == DB_SUCCESS);
  CHECK(os_file_truncate(file, 0) == DB_SUCCESS);
  if (!data.empty()) {
    CHECK(os_file_write(file, &data[0], data.size(), 0) == DB_SUCCESS);
  }
  CHECK(os_file_close(file) == DB_SUCCESS);
}

/** Append one encoded record to a byte vector.
@param[in,out]  data         file image
@param[in]      seq          sequence number
@param[in]      group_index  position in its group
@param[in]      payload      payload */
static void push_record(std::vector<unsigned char> *data, batch_seq_t seq,
                        uint32_t group_index, const std::string &payload) {
  const size_t at = data->size();
  data->resize(at + WAL_RECORD_HEADER_SIZE + payload.size());
  wal_encode_record(&(*data)[at], seq, group_index,
                    (const unsigned char *)payload.data(), payload.size());
}

/** Open the WAL, collect its records and close it again.
@param[in]      dir   database or WAL directory
@param[in,out]  c     collector receiving the recovered records
@param[out]     recv  WAL scan counters and boundaries
@return result of wal_open() */
static dberr_t recover(const test_dir_t &dir, collect_t *c, wal_recv_t *recv) {
  wal_t *wal = nullptr;
  const dberr_t err = wal_open(dir.path, 0, collect_apply, c, &wal, *recv);
  CHECK((err == DB_SUCCESS) == (wal != nullptr));
  if (wal != nullptr) {
    CHECK(wal_close(wal) == DB_SUCCESS);
  }

  return (err);
}

/** Check that recovery produced exactly the expected records.
@param[in]  c  recovered sequences and payloads to inspect
@param[in]  n  expected number of records
@return true if c holds exactly records 1..n of payload_for() */
static bool has_records(const collect_t &c, int n) {
  if (c.payloads.size() != (size_t)n) {
    return (false);
  }
  for (int i = 1; i <= n; ++i) {
    if (c.payloads[i - 1] != payload_for(i)) {
      return (false);
    }
  }

  return (true);
}

/** Reopening an empty WAL recovers nothing and appends from 1. */
static void test_reopen_empty() {
  test_dir_t dir;
  make_wal(dir, 0);
  collect_t c;
  wal_recv_t recv;
  wal_t *wal = nullptr;
  CHECK(wal_open(dir.path, 0, collect_apply, &c, &wal, recv) == DB_SUCCESS);
  CHECK(recv.records == 0 && recv.last_seq == 0);
  CHECK(recv.end_offset == WAL_FILE_HEADER_SIZE && recv.tail_bytes == 0);
  append_range(wal, 1, 1);
  CHECK(wal_close(wal) == DB_SUCCESS);
}

/** Reopening recovers every record, and appending continues after them. */
static void test_reopen_and_continue() {
  test_dir_t dir;
  make_wal(dir, 50);

  collect_t c;
  wal_recv_t recv;
  wal_t *wal = nullptr;
  CHECK(wal_open(dir.path, 0, collect_apply, &c, &wal, recv) == DB_SUCCESS);
  CHECK(has_records(c, 50));
  CHECK(recv.last_seq == 50 && recv.end_offset == end_of_record(50));
  /* Sequence numbers continue after the recovered records. */
  append_range(wal, 51, 60);
  CHECK(wal_close(wal) == DB_SUCCESS);

  collect_t again;
  CHECK(recover(dir, &again, &recv) == DB_SUCCESS);
  CHECK(has_records(again, 60));
}

/** A cut anywhere inside the last record drops just that record. */
static void test_cut_last_record() {
  test_dir_t dir;
  make_wal(dir, 20);
  const std::vector<unsigned char> full = read_file(dir);
  const uint64_t end19 = end_of_record(19);
  CHECK(full.size() == end_of_record(20));

  /* Cut the file at every byte inside the last record, as if the crash
  happened while it was being written. */
  for (uint64_t cut = end19; cut < full.size(); ++cut) {
    write_file(dir, std::vector<unsigned char>(full.begin(),
                                               full.begin() + (long)cut));
    collect_t c;
    wal_recv_t recv;
    CHECK(recover(dir, &c, &recv) == DB_SUCCESS);
    CHECK(has_records(c, 19));
    CHECK(recv.tail_bytes == cut - end19);
    /* The unfinished tail is cut off on open. */
    CHECK(read_file(dir).size() == end19);
  }
}

/** After the last record, zeros or an incomplete header are cut off; a
complete invalid header is corruption. */
static void test_garbage_tail() {
  const size_t lengths[] = {1,
                            WAL_RECORD_HEADER_SIZE - 1,
                            WAL_RECORD_HEADER_SIZE,
                            WAL_RECORD_HEADER_SIZE + 1,
                            100,
                            4096};
  for (size_t k = 0; k < sizeof(lengths) / sizeof(lengths[0]); ++k) {
    for (int fill = 0; fill < 2; ++fill) {
      test_dir_t dir;
      make_wal(dir, 5);
      std::vector<unsigned char> data = read_file(dir);
      /* Zeros, as left by a file extended before its data reached disk,
      or arbitrary bytes. */
      for (size_t i = 0; i < lengths[k]; ++i) {
        data.push_back(fill == 0 ? 0 : (unsigned char)(i * 131 + 7));
      }
      write_file(dir, data);

      collect_t c;
      wal_recv_t recv;
      if (lengths[k] < WAL_RECORD_HEADER_SIZE) {
        /* A physically incomplete final header can be truncated. */
        CHECK(recover(dir, &c, &recv) == DB_SUCCESS);
        CHECK(has_records(c, 5));
        CHECK(recv.tail_bytes == lengths[k]);
        CHECK(read_file(dir).size() == end_of_record(5));
      } else {
        /* A complete invalid header is corruption; preserve the WAL. */
        CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);
        CHECK(has_records(c, 5));
        CHECK(read_file(dir) == data);
      }
    }
  }
}

/** Write records 1..5, then a group of records 6, 7, 8 in which record
bad_seq is damaged, as if the group's write was only partly persisted.
@param[in]  dir      database or WAL directory
@param[in]  bad_seq  sequence number of the record to damage */
static void make_torn_group(const test_dir_t &dir, batch_seq_t bad_seq) {
  make_wal(dir, 5);
  std::vector<unsigned char> data = read_file(dir);
  for (batch_seq_t seq = 6; seq <= 8; ++seq) {
    const size_t at = data.size();
    push_record(&data, seq, (uint32_t)(seq - 6), payload_for((int)seq));
    if (seq == bad_seq) {
      data[at + WAL_RECORD_HEADER_SIZE] ^= 0x01;
    }
  }
  write_file(dir, data);
}

/** In the last group, a damaged complete record is corruption, a cut one is
an unfinished write. */
static void test_torn_group() {
  /* A complete damaged record is corruption even in the final group.
  Preserve the whole WAL, including records after the damaged record. */
  {
    test_dir_t dir;
    make_torn_group(dir, 6);
    const std::vector<unsigned char> before = read_file(dir);
    collect_t c;
    wal_recv_t recv;
    CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);
    CHECK(has_records(c, 5));
    CHECK(read_file(dir) == before);
  }
  /* Damage in the middle of the final group must also preserve the WAL. */
  {
    test_dir_t dir;
    make_torn_group(dir, 7);
    const std::vector<unsigned char> before = read_file(dir);
    collect_t c;
    wal_recv_t recv;
    CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);
    CHECK(has_records(c, 6));
    CHECK(read_file(dir) == before);
  }
}

/** Damage to a record before the last group is corruption. */
static void test_damage_in_durable_data() {
  test_dir_t dir;
  make_wal(dir, 10);
  std::vector<unsigned char> data = read_file(dir);
  /* Damage record 3. Records 4..10 are later groups, so record 3 had
  been flushed: this is corruption, not an unfinished write. */
  data[end_of_record(2) + WAL_RECORD_HEADER_SIZE] ^= 0x01;
  write_file(dir, data);

  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);
  /* Nothing is cut off on corruption. */
  CHECK(read_file(dir) == data);
}

/** Damage to a complete record of the last group is corruption too. */
static void test_damage_in_last_group() {
  test_dir_t dir;
  make_wal(dir, 10);
  std::vector<unsigned char> data = read_file(dir);
  /* Record 10 was acknowledged. Its position at the end of the WAL
  does not permit discarding it when its checksum fails. */
  data[end_of_record(9) + WAL_RECORD_HEADER_SIZE] ^= 0x01;
  write_file(dir, data);

  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);
  CHECK(has_records(c, 9));
  CHECK(read_file(dir) == data);
}

/** A damaged segment header is corruption. */
static void test_bad_header() {
  test_dir_t dir;
  make_wal(dir, 3);
  std::vector<unsigned char> data = read_file(dir);
  data[10] ^= 0x01;
  write_file(dir, data);
  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);

  /* Shorter than a header. */
  write_file(dir, std::vector<unsigned char>(data.begin(), data.begin() + 10));
  CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);
}

/** A valid record with a skipped sequence number is corruption. */
static void test_misplaced_record() {
  test_dir_t dir;
  make_wal(dir, 5);
  std::vector<unsigned char> data = read_file(dir);
  /* A valid record whose sequence number skips 6. */
  push_record(&data, 7, 0, payload_for(7));
  write_file(dir, data);
  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);
}

/** An error from the replay callback stops the open and changes no file. */
static void test_apply_error() {
  test_dir_t dir;
  make_wal(dir, 5);
  const std::vector<unsigned char> before = read_file(dir);

  collect_t c;
  c.fail_at = 3;
  wal_recv_t recv;
  CHECK(recover(dir, &c, &recv) == DB_ERROR);
  CHECK(read_file(dir) == before);

  collect_t ok;
  CHECK(recover(dir, &ok, &recv) == DB_SUCCESS);
  CHECK(has_records(ok, 5));
}

/** Acknowledgement sent from the writer process to the test. */
struct crash_ack_t {
  /** Sequence number of the acknowledged record. */
  uint64_t seq{0};

  /** Thread that wrote it. */
  int32_t thread_no{0};

  /** Its index among that thread's records. */
  int32_t i{0};
};

/** Build a payload identifying the crash round, writer and record.
@param[in]  round      crash-test round index
@param[in]  thread_no  writer thread index
@param[in]  i          zero-based row or record index
@return the payload of record i of a thread in a kill round */
static std::string crash_payload(int round, int thread_no, int i) {
  char buf[64];
  snprintf(buf, sizeof(buf), "round-%d-thread-%d-record-%d", round, thread_no,
           i);

  return (buf);
}

struct crash_writer_t {
  /** WAL under test. */
  wal_t *wal{nullptr};

  /** Kill round; keeps payloads apart across rounds. */
  int round{0};

  /** Number of this thread; keeps its payloads apart from the others'. */
  int thread_no{0};

  /** Pipe end the acknowledgements are written to. */
  int fd{-1};
};

/** Append until killed; report every acknowledged record on the pipe.
@param[in,out]  w  writer configuration and acknowledgment pipe */
static void crash_writer_main(crash_writer_t *w) {
  for (int i = 0;; ++i) {
    std::string p = crash_payload(w->round, w->thread_no, i);
    batch_seq_t seq = 0;
    if (append_record(*w->wal, p.data(), p.size(), seq) != DB_SUCCESS) {
      _exit(3);
    }
    crash_ack_t ack;
    ack.seq = seq;
    ack.thread_no = w->thread_no;
    ack.i = i;
    /* Small pipe writes are atomic, so acknowledgements never mix. */
    if (write(w->fd, &ack, sizeof(ack)) != (ssize_t)sizeof(ack)) {
      _exit(4);
    }
  }
}

/** Child process: open the WAL and append from four threads.
@param[in]  dir    database or WAL directory
@param[in]  round  crash-test round index
@param[in]  fd     pipe descriptor for reporting acknowledged writes */
static void crash_child(const char *dir, int round, int fd) {
  wal_t *wal = nullptr;
  wal_recv_t recv;
  if (wal_open(dir, 0, nullptr, nullptr, &wal, recv) != DB_SUCCESS) {
    _exit(2);
  }
  crash_writer_t writers[4];
  std::thread threads[4];
  for (int t = 0; t < 4; ++t) {
    writers[t].wal = wal;
    writers[t].round = round;
    writers[t].thread_no = t;
    writers[t].fd = fd;
    threads[t] = std::thread(crash_writer_main, &writers[t]);
  }
  for (int t = 0; t < 4; ++t) {
    threads[t].join();
  }
  _exit(0);
}

/** Kill a process appending from several threads, over and over: every
acknowledged record survives. */
static void test_kill_during_appends() {
  test_dir_t dir;
  make_wal(dir, 0);

  /* Every record acknowledged so far, from all rounds: (seq, payload). */
  std::vector<std::pair<uint64_t, std::string>> acked;
  for (int round = 0; round < 10; ++round) {
    int fds[2];
    CHECK(pipe(fds) == 0);
    const pid_t pid = fork();
    if (pid == 0) {
      close(fds[0]);
      crash_child(dir.path, round, fds[1]);
    }
    close(fds[1]);

    /* Let a varying number of appends complete, then kill the writer
    while it is in the middle of further appends. */
    const int wanted = 20 + round * 17;
    int received = 0;
    crash_ack_t ack;
    while (received < wanted &&
           read(fds[0], &ack, sizeof(ack)) == (ssize_t)sizeof(ack)) {
      acked.push_back(
          std::make_pair(ack.seq, crash_payload(round, ack.thread_no, ack.i)));
      ++received;
    }
    kill(pid, SIGKILL);
    int status = 0;
    waitpid(pid, &status, 0);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);
    /* Acknowledgements written before the kill. */
    while (read(fds[0], &ack, sizeof(ack)) == (ssize_t)sizeof(ack)) {
      acked.push_back(
          std::make_pair(ack.seq, crash_payload(round, ack.thread_no, ack.i)));
    }
    close(fds[0]);
    CHECK(received == wanted);

    /* Every acknowledged record must be recovered with its payload. */
    collect_t c;
    wal_recv_t recv;
    CHECK(recover(dir, &c, &recv) == DB_SUCCESS);
    for (size_t k = 0; k < acked.size(); ++k) {
      const uint64_t seq = acked[k].first;
      CHECK(seq >= 1 && seq <= c.payloads.size() &&
            c.payloads[seq - 1] == acked[k].second);
    }
  }
  printf("kill test: %zu acknowledged records survived 10 kills\n",
         acked.size());
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_reopen_empty();
  test_reopen_and_continue();
  test_cut_last_record();
  test_garbage_tail();
  test_torn_group();
  test_damage_in_durable_data();
  test_damage_in_last_group();
  test_bad_header();
  test_misplaced_record();
  test_apply_error();
  test_kill_during_appends();

  return (test_result("wal0recv-t"));
}
