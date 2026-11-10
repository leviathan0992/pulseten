/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal_format.h
 *    On-disk format of the WAL.
 *
 * The WAL is a run of segment files in one directory, named
 * wal-<first sequence number, 20 digits>.pt. Each segment starts with a
 * file header followed by records; the records of consecutive segments
 * continue the sequence numbers without gaps. All integers are
 * little-endian.
 *
 *   File header (40 bytes)
 *     0  "PTWAL001"
 *     8  u32 format version (1)
 *    12  u32 header size (40)
 *    16  u64 WAL id, the same for every segment of one WAL
 *    24  u64 sequence number of the segment's first record
 *    32  u32 reserved (0)
 *    36  u32 CRC-32C of bytes [0, 36)
 *
 *   Record (28-byte header followed by the payload)
 *     0  u32 magic "PTRC"
 *     4  u32 payload length
 *     8  u64 batch sequence number, 1, 2, 3, ... without gaps
 *    16  u32 position of the record in its group, 0 for the first
 *    20  u32 CRC-32C of bytes [0, 20) followed by the payload
 *    24  u32 CRC-32C of header bytes [0, 24), including the data CRC
 *
 * The separate header CRC is checked before trusting payload length, so a
 * damaged length cannot masquerade as an incomplete final payload.
 *
 * A group is the set of records written by one write and an optional sync.
 * Its position field lets recovery distinguish an unfinished last group
 * from damage inside a completed group. With sync enabled, completed groups
 * are durable. Unsynced groups may be lost or corrupted on a system crash
 * or power loss. A group never spans two segments.
 *
 *-------------------------------------------------------------------------
 */

#ifndef wal_format_h
#define wal_format_h

#include <cstddef>
#include <cstdint>
#include <string>

#include "ut.h"

/** Version written in the header of every segment. */
#define WAL_FORMAT_VERSION 1

/** Bytes of a segment header. */
#define WAL_FILE_HEADER_SIZE 40

/** Bytes of a record header. */
#define WAL_RECORD_HEADER_SIZE 28

/** "PTRC" read as a little-endian u32. */
#define WAL_RECORD_MAGIC 0x43525450

/** Largest record, header included. */
#define WAL_MAX_RECORD_SIZE (8 * 1024 * 1024)

/** Largest record payload. */
#define WAL_MAX_PAYLOAD (WAL_MAX_RECORD_SIZE - WAL_RECORD_HEADER_SIZE)

/** Build a WAL segment file name from its first sequence number.
@return the file name of the segment whose first record is first_seq */
std::string wal_segment_name(batch_seq_t first_seq);

/** Recognize a segment file name.
@return true if name is a segment file name */
bool wal_segment_parse_name(const char *name, batch_seq_t &first_seq);

/** Fill a file header. */
void wal_encode_file_header(unsigned char *b, uint64_t id,
                            batch_seq_t first_seq);

/** Decode a file header.
@return true if b holds a valid file header of this format version */
bool wal_decode_file_header(const unsigned char *b, uint64_t &id,
                            batch_seq_t &first_seq);

/** Encode one record. */
void wal_encode_record(unsigned char *b, batch_seq_t seq, uint32_t group_index,
                       const unsigned char *payload, size_t len);

/** Payload length announced by a record header, if it is plausible.
@param[in]  b  at least WAL_RECORD_HEADER_SIZE bytes
@return true if the header CRC and magic match and 0 < len <= WAL_MAX_PAYLOAD */
bool wal_record_header_len(const unsigned char *b, size_t &len);

/** Check a complete record.
@return true if a record with valid magic, length, group position and CRC
  starts at b and fits in avail bytes */
bool wal_record_is_valid(const unsigned char *b, size_t avail, batch_seq_t &seq,
                         uint32_t &group_index, size_t &len);

#endif
