/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal-fuzz.cc
 *    libFuzzer target for WAL records, files and recovery directories.
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <unistd.h>

#include "os_file.h"

namespace {
/** Encoded file bytes. */
using fuzz_bytes_t = std::vector<unsigned char>;

/** File header bytes in the independent format model. */
constexpr size_t FUZZ_FILE_HEADER_BYTES = 40;

/** Record header bytes in the independent format model. */
constexpr size_t FUZZ_RECORD_HEADER_BYTES = 28;

/** Largest payload accepted by the format model. */
constexpr size_t FUZZ_MAX_PAYLOAD_BYTES =
    8 * 1024 * 1024 - FUZZ_RECORD_HEADER_BYTES;

/** Database identity used by generated files. */
constexpr uint64_t FUZZ_WAL_ID = 123;

/* Assertions remain active in optimized builds and preserve a fuzz artifact. */
#define REQUIRE(expr)                                                      \
  do {                                                                     \
    if (!(expr)) {                                                         \
      fprintf(stderr, "WAL oracle failed: %s at %s:%d\n", #expr, __FILE__, \
              __LINE__);                                                   \
      abort();                                                             \
    }                                                                      \
  } while (false)

/** Read a little-endian integer independently of the production codec. */
uint64_t read_le(const unsigned char *p, size_t n) {
  uint64_t value = 0;
  for (size_t i = 0; i < n; ++i)
    value |= uint64_t(p[i]) << (8 * i);

  return value;
}

/** Write a little-endian integer for controlled corruption. */
void write_le(unsigned char *p, uint64_t v, size_t n) {
  for (size_t i = 0; i < n; ++i)
    p[i] = static_cast<unsigned char>(v >> (8 * i));
}

/* Independent bitwise CRC-32C: never ask the production decoder for the
answer. */
uint32_t crc32c(const unsigned char *p, size_t n, uint32_t previous = 0) {
  uint32_t c = ~previous;

  for (size_t i = 0; i < n; ++i) {
    c ^= p[i];
    for (int bit = 0; bit < 8; ++bit)
      c = (c >> 1) ^ (0x82f63b78U & (0U - (c & 1U)));
  }

  return ~c;
}

/** Check header framing before trusting its payload length. */
bool validate_record_header(const unsigned char *p) {
  return std::memcmp(p, "PTRC", 4) == 0 &&
         read_le(p + 24, 4) == crc32c(p, 24) && read_le(p + 4, 4) > 0 &&
         read_le(p + 4, 4) <= FUZZ_MAX_PAYLOAD_BYTES;
}

/** Check record boundaries, sequence metadata and checksums. */
bool validate_record(const unsigned char *p, size_t n) {
  if (n < FUZZ_RECORD_HEADER_BYTES || !validate_record_header(p))

    return false;
  const size_t len = read_le(p + 4, 4);

  return len <= n - FUZZ_RECORD_HEADER_BYTES && read_le(p + 8, 8) != 0 &&
         read_le(p + 16, 4) < read_le(p + 8, 8) &&
         read_le(p + 20, 4) ==
             crc32c(p + FUZZ_RECORD_HEADER_BYTES, len, crc32c(p, 20));
}

/** One record expected from replay. */
struct fuzz_record_t {
  /** Sequence expected from the callback. */
  uint64_t m_seq;

  /** Payload expected from the callback. */
  std::string m_payload;
};

/** Recovery result predicted by the independent model. */
struct fuzz_recovery_t {
  /** Predicted recovery status. */
  dberr_t m_code{DB_SUCCESS};

  /** Identity decoded from the file header. */
  uint64_t m_wal_id{0};

  /** End of the last complete record, in bytes. */
  size_t m_end_offset{FUZZ_FILE_HEADER_BYTES};

  /** Records expected before recovery stops. */
  std::vector<fuzz_record_t> m_records;
};

/** Predict replayed records and errors without using the production reader. */
fuzz_recovery_t predict_recovery(const fuzz_bytes_t &data, uint64_t first,
                                 bool last) {
  fuzz_recovery_t out;
  if (data.size() < FUZZ_FILE_HEADER_BYTES ||
      std::memcmp(data.data(), "PTWAL001", 8) != 0 ||
      read_le(data.data() + 8, 4) != 1 ||
      read_le(data.data() + 12, 4) != FUZZ_FILE_HEADER_BYTES ||
      read_le(data.data() + 32, 4) != 0 ||
      read_le(data.data() + 36, 4) != crc32c(data.data(), 36) || first == 0 ||
      read_le(data.data() + 24, 8) != first) {
    out.m_code = DB_CORRUPTION;

    return out;
  }

  out.m_wal_id = read_le(data.data() + 16, 8);

  uint64_t seq = first;

  while (out.m_end_offset < data.size()) {
    const auto *p = data.data() + out.m_end_offset;
    const size_t avail = data.size() - out.m_end_offset;
    if (avail < FUZZ_RECORD_HEADER_BYTES)
      break;
    if (!validate_record_header(p) || read_le(p + 8, 8) != seq ||
        read_le(p + 16, 4) >= seq) {
      break;
    }

    const size_t len = read_le(p + 4, 4);
    if (len > avail - FUZZ_RECORD_HEADER_BYTES)
      break;
    if (!validate_record(p, avail))
      break;
    out.m_records.push_back({seq, std::string(reinterpret_cast<const char *>(
                                                  p + FUZZ_RECORD_HEADER_BYTES),
                                              len)});
    out.m_end_offset += FUZZ_RECORD_HEADER_BYTES + len;
    ++seq;
  }

  if (out.m_end_offset < data.size() && !last)
    out.m_code = DB_CORRUPTION;

  return out;
}

/** Expected callback order and optional replay failure. */
struct fuzz_collector_t {
  /** Expected records, retained by the test. */
  const std::vector<fuzz_record_t> &m_expected_records;

  /** Callbacks successfully verified so far. */
  size_t m_replayed_records{0};

  /** Callback index that fails; SIZE_MAX disables injection. */
  size_t m_failure_index{SIZE_MAX};
};

/** Check the replayed record and inject failure at the selected index. */
db_status_t check_replayed_record(void *ctx, batch_seq_t seq,
                                  const unsigned char *p, size_t n) {
  auto &c = *static_cast<fuzz_collector_t *>(ctx);
  REQUIRE(c.m_replayed_records < c.m_expected_records.size());

  const auto &want = c.m_expected_records[c.m_replayed_records];
  REQUIRE(seq == want.m_seq);
  REQUIRE(n == want.m_payload.size());
  REQUIRE(std::memcmp(p, want.m_payload.data(), n) == 0);
  if (c.m_replayed_records == c.m_failure_index)

    return DB_ERROR;
  ++c.m_replayed_records;

  return DB_SUCCESS;
}

/** Temporary file removed from the directory while open. */
struct fuzz_file_t {
  /** Open temporary file descriptor. */
  os_file_t m_file{OS_FILE_CLOSED};
  fuzz_file_t() {
    const char *tmp = std::getenv("TMPDIR");
    std::string name = std::string(tmp ? tmp : "/tmp") + "/pt-wal-fuzz-XXXXXX";
    m_file = mkstemp(name.data());
    REQUIRE(m_file >= 0);
    REQUIRE(unlink(name.c_str()) == 0);
  }

  ~fuzz_file_t() { REQUIRE(os_file_close(m_file).ok()); }
  void write(const fuzz_bytes_t &data) {
    REQUIRE(os_file_truncate(m_file, 0).ok());
    if (!data.empty())
      REQUIRE(os_file_write(m_file, data.data(), data.size(), 0).ok());
  }
};

/** Compare file replay with the model and verify that input is unchanged. */
void check_file_recovery(const fuzz_bytes_t &data, uint64_t first, bool last,
                         size_t fail_at = SIZE_MAX) {
  const fuzz_recovery_t want = predict_recovery(data, first, last);
  fuzz_collector_t c{want.m_records, 0, fail_at};
  fuzz_file_t file;
  file.write(data);

  wal_recovery_t recovery;
  const auto status =
      wal_file_reader(file.m_file)
          .replay(first, last, check_replayed_record, &c, recovery);

  const bool failed_apply = fail_at < want.m_records.size();
  REQUIRE(status.code() == (failed_apply ? DB_ERROR : want.m_code));
  REQUIRE(c.m_replayed_records ==
          (failed_apply ? fail_at : want.m_records.size()));
  REQUIRE(recovery.m_records == c.m_replayed_records);

  if (status.ok()) {
    REQUIRE(recovery.m_wal_id == want.m_wal_id);
    REQUIRE(recovery.m_first_seq == first);
    REQUIRE(recovery.m_last_seq == first + want.m_records.size() - 1);
    REQUIRE(recovery.m_end_offset == want.m_end_offset);
    REQUIRE(recovery.m_tail_bytes == data.size() - want.m_end_offset);
    REQUIRE(recovery.m_file_count == 1);
  }

  uint64_t size = 0;
  REQUIRE(os_file_size(file.m_file, size).ok() && size == data.size());

  fuzz_bytes_t after(data.size());
  if (!after.empty())
    REQUIRE(os_file_read(file.m_file, after.data(), after.size(), 0).ok());

  /* A reader must not repair or truncate input. */
  REQUIRE(after == data);
}

/** Start a file with the selected identity and first sequence. */
fuzz_bytes_t make_file_header(uint64_t first, uint64_t id = FUZZ_WAL_ID) {
  fuzz_bytes_t data(FUZZ_FILE_HEADER_BYTES);
  wal_encode_file_header(data.data(), id, first);

  return data;
}

/** Append an encoded record to the generated file. */
void append_record(fuzz_bytes_t &data, uint64_t seq, uint32_t index,
                   const std::string &payload) {
  const size_t at = data.size();
  data.resize(at + FUZZ_RECORD_HEADER_BYTES + payload.size());
  wal_encode_record(data.data() + at, seq, index,
                    reinterpret_cast<const unsigned char *>(payload.data()),
                    payload.size());
}

/** Byte stream selecting fuzz scenarios and mutations. */
struct fuzz_input_t {
  /** Fuzzer-owned input bytes. */
  const uint8_t *m_data;

  /** Input length in bytes. */
  size_t m_size;

  /** Next input byte to consume. */
  size_t m_offset{0};
  unsigned read_byte() { return m_offset < m_size ? m_data[m_offset++] : 0; }
};

/** Generate a file and apply one controlled mutation. */
void test_structured_file(fuzz_input_t in) {
  const unsigned scenario = in.read_byte() % 16;
  const uint64_t starts[] = {1, 2, 255, UINT32_MAX, UINT64_MAX - 32};
  const uint64_t first = starts[in.read_byte() % 5];
  const size_t count = 1 + in.read_byte() % 8;
  const unsigned change = in.read_byte() | (in.read_byte() << 8);
  fuzz_bytes_t data = make_file_header(first);
  std::vector<size_t> offsets;
  uint32_t group_index = 0;

  for (size_t i = 0; i < count; ++i) {
    if (in.read_byte() % 2 == 0)
      group_index = 0;
    offsets.push_back(data.size());

    const size_t len = 1 + in.read_byte() + 256 * (in.read_byte() % 3);
    std::string payload(len, '\0');
    for (char &ch : payload)
      ch = static_cast<char>(in.read_byte());
    append_record(data, first + i, group_index++, payload);
  }

  const size_t at = offsets.back();
  bool last = true;
  size_t fail_at = SIZE_MAX;

  switch (scenario) {
    case 0:
      break;
    case 1:
      /* Header or payload cut, including exact record boundary. */
      data.resize(at + change % (data.size() - at));
      break;
    case 2:
      data[at + FUZZ_RECORD_HEADER_BYTES] ^= 1;
      break;
    case 3:
      data[at + change % FUZZ_RECORD_HEADER_BYTES] ^= 1;
      break;
    case 4:
      /* Valid checksum, wrong sequence. */
      append_record(data, first + count + 1, 0, "gap");
      break;
    case 5:
      data.resize(at + 1 + change % (data.size() - at - 1));
      last = false;
      break;
    case 6:
      fail_at = change % count;
      break;
    case 7:
      data[change % FUZZ_FILE_HEADER_BYTES] ^= 1;
      break;
    case 8: {
      /* Incomplete declared payload containing a valid later group. */
      data.resize(at);

      std::string payload(200, 'a');
      append_record(data, first + count - 1, 0, payload);
      data.resize(at + FUZZ_RECORD_HEADER_BYTES + 1);
      append_record(data, first + count, 0, "later-group");
      break;
    }
    case 9:
      /* Same-group suffix is discarded too. */
      data.resize(at);
      append_record(data, first + count - 1, 0, std::string(200, 'a'));
      data.resize(at + FUZZ_RECORD_HEADER_BYTES + 1);
      append_record(data, first + count, 1, "same-group");
      break;
    case 10:
      /* Plausibility boundary with an independently valid header CRC. */
      write_le(data.data() + at + 4,
               change % 2 ? 0 : FUZZ_MAX_PAYLOAD_BYTES + 1, 4);
      write_le(data.data() + at + 24, crc32c(data.data() + at, 24), 4);
      break;
    case 11:
      append_record(data, first + count, 0, "duplicate");
      append_record(data, first + count, 0, "duplicate");
      break;
    case 12:
      data.resize(at + change % FUZZ_RECORD_HEADER_BYTES);
      last = (change % 2 == 0);
      break;

    /* Recompute both checksums so sequence validation is exercised. */
    case 13:
    case 14:
      write_le(data.data() + at + (scenario == 13 ? 8 : 16),
               scenario == 13 ? 0 : first + count - 1, scenario == 13 ? 8 : 4);
      write_le(data.data() + at + 20,
               crc32c(data.data() + at + FUZZ_RECORD_HEADER_BYTES,
                      data.size() - at - FUZZ_RECORD_HEADER_BYTES,
                      crc32c(data.data() + at, 20)),
               4);
      write_le(data.data() + at + 24, crc32c(data.data() + at, 24), 4);
      break;
    case 15:
      wal_encode_file_header(data.data(), FUZZ_WAL_ID, first + 1);
      break;
  }

  check_file_recovery(data, first, last, fail_at);
}

/** Temporary WAL directory removed at scope exit. */
struct fuzz_directory_t {
  /** Owned temporary directory path. */
  std::string m_path;
  fuzz_directory_t() {
    const char *tmp = std::getenv("TMPDIR");
    m_path = std::string(tmp ? tmp : "/tmp") + "/pt-wal-dir-XXXXXX";
    REQUIRE(mkdtemp(m_path.data()) != nullptr);
  }

  ~fuzz_directory_t() {
    std::vector<std::string> names;
    REQUIRE(os_dir_list(m_path.c_str(), names).ok());
    for (const auto &name : names)
      REQUIRE(unlink((m_path + "/" + name).c_str()) == 0);
    REQUIRE(rmdir(m_path.c_str()) == 0);
  }

  std::string file_path(uint64_t first) const {
    return m_path + "/" + wal_file_name(first);
  }

  void write_file(uint64_t first, const fuzz_bytes_t &data) const {
    os_file_t fd;
    REQUIRE(os_file_open(file_path(first).c_str(), OS_FILE_OPEN_OR_CREATE, fd)
                .ok());
    REQUIRE(os_file_truncate(fd, 0).ok());
    REQUIRE(os_file_write(fd, data.data(), data.size(), 0).ok());
    REQUIRE(os_file_close(fd).ok());
  }

  fuzz_bytes_t read_file(uint64_t first) const {
    os_file_t fd;
    REQUIRE(os_file_open(file_path(first).c_str(), OS_FILE_OPEN, fd).ok());

    uint64_t size;
    REQUIRE(os_file_size(fd, size).ok());

    fuzz_bytes_t data(size);
    REQUIRE(os_file_read(fd, data.data(), data.size(), 0).ok());
    REQUIRE(os_file_close(fd).ok());

    return data;
  }
};

/** Check recovery, truncation and retry across several files. */
void test_directory_recovery(fuzz_input_t in) {
  const unsigned scenario = in.read_byte() % 8;

  /* Also represents a purged prefix. */
  const uint64_t first = 1 + in.read_byte();

  const std::string payload(1 + in.read_byte(),
                            static_cast<char>(in.read_byte()));

  fuzz_directory_t dir;
  std::vector<fuzz_record_t> wanted;
  std::vector<fuzz_bytes_t> files;

  for (size_t i = 0; i < 3; ++i) {
    files.push_back(make_file_header(first + i));
    append_record(files.back(), first + i, 0, payload + std::to_string(i));
    wanted.push_back({first + i, payload + std::to_string(i)});
  }

  const size_t end = files.back().size();
  dberr_t code = DB_SUCCESS;
  uint64_t expected_id = FUZZ_WAL_ID;

  switch (scenario) {
    case 0:
      break;
    case 1:
      /* Truncate, append, reopen below. */
      files.back().push_back(0);
      break;
    case 2:
      files[0].pop_back();
      code = DB_CORRUPTION;
      break;
    case 3:
      /* Missing middle file. */
      code = DB_CORRUPTION;
      break;
    case 4:
      wal_encode_file_header(files[1].data(), FUZZ_WAL_ID + 1, first + 1);
      code = DB_CORRUPTION;
      break;
    case 5:
      expected_id++;
      code = DB_CORRUPTION;
      break;
    case 6:
      files.back().back() ^= 1;
      break;
    case 7:
      /* Callback fails in the second file. */
      break;
  }

  for (size_t i = 0; i < files.size(); ++i)
    if (!(scenario == 3 && i == 1))
      dir.write_file(first + i, files[i]);

  fuzz_collector_t c{wanted, 0, scenario == 7 ? size_t(1) : SIZE_MAX};
  wal *log = nullptr;
  wal_recovery_t recovery;
  const auto status = wal::open(dir.m_path.c_str(), check_replayed_record, &c,
                                &log, recovery, expected_id, false);
  REQUIRE(status.code() == (scenario == 7 ? DB_ERROR : code));
  REQUIRE((log != nullptr) == status.ok());

  const size_t counts[] = {3, 3, 0, 1, 1, 0, 2, 1};
  REQUIRE(c.m_replayed_records == counts[scenario]);

  if (!status.ok()) {
    for (size_t i = 0; i < files.size(); ++i)
      if (!(scenario == 3 && i == 1))
        REQUIRE(dir.read_file(first + i) == files[i]);

    return;
  }

  const size_t recovered = counts[scenario];
  const size_t kept = scenario == 6 ? FUZZ_FILE_HEADER_BYTES : end;
  REQUIRE(recovery.m_records == recovered && recovery.m_file_count == 3);
  REQUIRE(recovery.m_first_seq == first &&
          recovery.m_last_seq == first + recovered - 1);
  REQUIRE(recovery.m_wal_id == FUZZ_WAL_ID && recovery.m_end_offset == kept);
  REQUIRE(recovery.m_tail_bytes == files.back().size() - kept);
  files.back().resize(kept);
  wanted.resize(recovered);
  for (size_t i = 0; i < files.size(); ++i)
    REQUIRE(dir.read_file(first + i) == files[i]);

  const std::string extra = "after-recovery" + payload;
  wal_record_t rec{first + recovered,
                   reinterpret_cast<const unsigned char *>(extra.data()),
                   extra.size()};
  REQUIRE(log->append(&rec, 1).ok());
  REQUIRE(wal::destroy(log).ok());
  wanted.push_back({first + recovered, extra});

  fuzz_collector_t again{wanted};
  log = nullptr;
  REQUIRE(wal::open(dir.m_path.c_str(), check_replayed_record, &again, &log,
                    recovery, FUZZ_WAL_ID, false)
              .ok());
  REQUIRE(again.m_replayed_records == wanted.size() &&
          recovery.m_records == wanted.size());
  REQUIRE(recovery.m_tail_bytes == 0 &&
          recovery.m_last_seq == first + recovered);
  REQUIRE(wal::destroy(log).ok());
}

/** Check recovery after partial writes or failed synchronization. */
void test_write_failure(fuzz_input_t in) {
  const bool partial = in.read_byte() % 2;
  fuzz_directory_t dir;
  wal *log = nullptr;
  REQUIRE(wal::create(dir.m_path.c_str(), &log, FUZZ_WAL_ID, true).ok());

  std::string payload(32 + in.read_byte(), 'x');
  wal_record_t rec{1, reinterpret_cast<const unsigned char *>(payload.data()),
                   payload.size()};
  if (partial)
    ut_debug_global().arm("wal.partial_write", DB_IO_ERROR);
  else
    ut_debug_global().arm("wal.sync", DB_IO_ERROR);
  REQUIRE(log->append(&rec, 1).code() == DB_OUTCOME_UNKNOWN);
  REQUIRE(wal::destroy(log).ok());

  const fuzz_bytes_t data = dir.read_file(1);
  const fuzz_recovery_t want = predict_recovery(data, 1, true);
  REQUIRE(want.m_code == DB_SUCCESS);
  REQUIRE(want.m_records.size() == (partial ? 0 : 1));

  fuzz_collector_t c{want.m_records};
  wal_recovery_t recovery;
  REQUIRE(wal::open(dir.m_path.c_str(), check_replayed_record, &c, &log,
                    recovery, FUZZ_WAL_ID, false)
              .ok());
  REQUIRE(c.m_replayed_records == want.m_records.size() &&
          recovery.m_records == c.m_replayed_records);
  REQUIRE(recovery.m_last_seq == c.m_replayed_records &&
          recovery.m_end_offset == want.m_end_offset);
  REQUIRE(recovery.m_tail_bytes == data.size() - want.m_end_offset);
  REQUIRE(wal::destroy(log).ok());
  REQUIRE(dir.read_file(1) ==
          fuzz_bytes_t(data.begin(), data.begin() + want.m_end_offset));
}
} /* namespace */

/** Dispatch one fuzz input to a record, file or directory check. */
extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  ut_debug_scope input_scope;
  if (size == 0) {
    check_file_recovery({}, 1, true);

    return 0;
  }

  fuzz_input_t in{data, size};
  switch (in.read_byte() % 4) {
    case 0: {
      fuzz_bytes_t raw(data + 1, data + size);
      const uint64_t first = raw.size() >= FUZZ_FILE_HEADER_BYTES
                                 ? read_le(raw.data() + 24, 8)
                                 : 1;
      check_file_recovery(raw, first, true);
      check_file_recovery(raw, first, false);

      if (raw.size() >= FUZZ_RECORD_HEADER_BYTES) {
        batch_seq_t seq;
        uint32_t index;
        size_t len;
        const bool valid = validate_record(raw.data(), raw.size());
        REQUIRE(wal_decode_record(raw.data(), raw.size(), seq, index, len) ==
                valid);

        if (valid) {
          REQUIRE(seq == read_le(raw.data() + 8, 8));
          REQUIRE(index == read_le(raw.data() + 16, 4));
          REQUIRE(len == read_le(raw.data() + 4, 4));
        }
      }

      break;
    }
    case 1:
      test_structured_file(in);
      break;
    case 2:
      test_directory_recovery(in);
      break;
    case 3:
      test_write_failure(in);
      break;
  }

  return 0;
}

/* Explicit 8 MiB boundary checks avoid paying their cost on every mutation. */
extern "C" void wal_fuzz_boundary_tests() {
  REQUIRE(crc32c(reinterpret_cast<const unsigned char *>("123456789"), 9) ==
          0xe3069283U);

  fuzz_bytes_t cut = make_file_header(17);
  append_record(cut, 17, 0, "preserved");

  const size_t previous_end = cut.size();
  append_record(cut, 18, 0, "record-cut-at-every-byte");

  for (size_t at = previous_end; at < cut.size(); ++at) {
    const fuzz_bytes_t prefix(cut.begin(), cut.begin() + at);
    check_file_recovery(prefix, 17, true);
    check_file_recovery(prefix, 17, false);
  }

  for (size_t len : {size_t(1), size_t(65536), FUZZ_MAX_PAYLOAD_BYTES - 1,
                     FUZZ_MAX_PAYLOAD_BYTES}) {
    fuzz_bytes_t data = make_file_header(1);
    append_record(data, 1, 0, std::string(len, 'b'));
    check_file_recovery(data, 1, true);
    data.pop_back();
    check_file_recovery(data, 1, true);
    check_file_recovery(data, 1, false);
  }

  for (uint64_t len : {uint64_t(0), uint64_t(FUZZ_MAX_PAYLOAD_BYTES + 1),
                       uint64_t(UINT32_MAX)}) {
    fuzz_bytes_t data = make_file_header(1);
    append_record(data, 1, 0, "x");
    write_le(data.data() + FUZZ_FILE_HEADER_BYTES + 4, len, 4);
    write_le(data.data() + FUZZ_FILE_HEADER_BYTES + 24,
             crc32c(data.data() + FUZZ_FILE_HEADER_BYTES, 24), 4);
    check_file_recovery(data, 1, true);
  }

  wal_recovery_t recovery;
  REQUIRE(wal_file_reader(OS_FILE_CLOSED)
              .replay(1, true, nullptr, nullptr, recovery)
              .code() == DB_IO_ERROR);
}
