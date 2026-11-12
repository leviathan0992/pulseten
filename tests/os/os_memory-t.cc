/*-------------------------------------------------------------------------
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *-------------------------------------------------------------------------
 */
#include "os_memory.h"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include "test_util.h"

/** Synthetic proc entries and real limit files, independent of host cgroups. */
static void test_cgroups() {
  test_dir_t dir;
  const std::string root = dir.file("memory space");
  std::filesystem::create_directories(root + "/parent/child");
  std::string escaped = root;
  escaped.replace(escaped.find(' '), 1, "\\040");
  const std::string mount =
      "1 0 0:1 / " + escaped + " rw - cgroup2 cgroup rw\n";
  const std::string member = "0::/parent/child\n";
  auto write = [](const std::string &path, const std::string &value) {
    std::ofstream file(path);
    file << value;
    CHECK(file.good());
  };
  const uint64_t gib = uint64_t{1} << 30;
  write(root + "/memory.max", std::to_string(4 * gib));
  write(root + "/parent/memory.max", std::to_string(gib));
  write(root + "/parent/child/memory.max", "max\n");
  uint64_t bytes = 8 * gib;
  CHECK(os_cgroup_memory(member, mount, bytes).ok() && bytes == gib);
  write(root + "/parent/child/memory.max", std::to_string(gib / 2));
  bytes = 8 * gib;
  CHECK(os_cgroup_memory(member, mount, bytes).ok() && bytes == gib / 2);
  bytes = gib / 4;
  CHECK(os_cgroup_memory(member, mount, bytes).ok() && bytes == gib / 4);
  for (const char *invalid :
       {"", "bad", "-1", "18446744073709551616", "123 extra"}) {
    write(root + "/parent/child/memory.max", invalid);
    bytes = 8 * gib;
    CHECK(!os_cgroup_memory(member, mount, bytes).ok());
    CHECK(bytes == 8 * gib);
  }
  write(root + "/parent/child/memory.max", "max");
  CHECK(!os_cgroup_memory(member, "", bytes).ok());
  CHECK(!os_cgroup_memory("0::/../escape\n", mount, bytes).ok());
  CHECK(!os_cgroup_memory("malformed\n", mount, bytes).ok());

  /* A bind-mounted v1 hierarchy includes its visible parent's hard limit. */
  write(root + "/memory.limit_in_bytes", "9223372036854771712");
  write(root + "/parent/memory.limit_in_bytes", std::to_string(2 * gib));
  write(root + "/parent/child/memory.limit_in_bytes", std::to_string(3 * gib));
  const std::string v1mount =
      "2 0 0:2 /tenant " + escaped + " rw - cgroup cgroup rw,memory\n";
  bytes = 8 * gib;
  CHECK(os_cgroup_memory("5:cpu:/elsewhere\n7:memory:/tenant/parent/child\n",
                         v1mount, bytes)
            .ok());
  CHECK(bytes == 2 * gib);
  bytes = gib;
  CHECK(os_cgroup_memory("7:memory:/\n", v1mount, bytes).ok() && bytes == gib);
  /* v2 without an enabled memory controller leaves physical capacity intact. */
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root);
  bytes = 8 * gib;
  CHECK(os_cgroup_memory("0::/\n", mount, bytes).ok() && bytes == 8 * gib);
  std::filesystem::remove_all(root);
}

int main() {
  test_cgroups();
  uint64_t bytes = 0;
  CHECK(os_effective_memory(bytes).ok() && bytes >= 8);
  printf("effective capacity: %llu bytes\n",
         static_cast<unsigned long long>(bytes));
  return (test_result("os_memory-t"));
}
