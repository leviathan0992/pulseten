/*-------------------------------------------------------------------------
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * os_memory.cc
 *    Memory capacity detection at database open.
 *-------------------------------------------------------------------------
 */
#include "os_memory.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include <unistd.h>
#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

/** Read proc/cgroup files, whose stat size is often zero. */
static db_status_t os_memory_read_text(const std::string &path,
                                       std::string &text,
                                       bool missing_ok = false) {
  text.clear();

  FILE *file = fopen(path.c_str(), "r");
  if (file == nullptr) {
    const int error = errno;
    if (missing_ok && error == ENOENT) {
      text = "max";
      return (DB_SUCCESS);
    }

    return (db_status_t(DB_IO_ERROR,
                        "cannot read " + path + ": " + strerror(error)));
  }

  char buffer[4096];
  size_t n;
  while ((n = fread(buffer, 1, sizeof(buffer), file)) != 0) {
    text.append(buffer, n);
  }

  const bool failed = ferror(file) != 0;
  const int error = errno;
  fclose(file);

  return (failed ? db_status_t(DB_IO_ERROR,
                               "cannot read " + path + ": " + strerror(error))
                 : db_status_t(DB_SUCCESS));
}

/** Match one controller without accepting a substring of its name. */
static bool os_cgroup_has_controller(const std::string &list,
                                     const char *name) {
  return (("," + list + ",").find("," + std::string(name) + ",") !=
          std::string::npos);
}

/** Decode mountinfo's escaped spaces, tabs, newlines and backslashes. */
static std::string os_cgroup_decode_path(const std::string &text) {
  std::string path;
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] == '\\' && i + 3 < text.size() && text[i + 1] >= '0' &&
        text[i + 1] <= '7' && text[i + 2] >= '0' && text[i + 2] <= '7' &&
        text[i + 3] >= '0' && text[i + 3] <= '7') {
      path += static_cast<char>((text[i + 1] - '0') * 64 +
                                (text[i + 2] - '0') * 8 + text[i + 3] - '0');
      i += 3;
    } else {
      path += text[i];
    }
  }

  while (path.size() > 1 && path.back() == '/') {
    path.pop_back();
  }

  return (path);
}

/** Only absolute paths confined to a cgroup mount may be traversed. */
static bool os_cgroup_path_is_valid(const std::string &path) {
  return (!path.empty() && path[0] == '/' &&
          path.find('\0') == std::string::npos &&
          (path + "/").find("/../") == std::string::npos &&
          (path + "/").find("/./") == std::string::npos);
}

/** Reduce a capacity by a member's limit and every visible ancestor limit. */
static db_status_t os_cgroup_apply_limits(const std::string &base,
                                          std::string path, bool unified,
                                          uint64_t &bytes) {
  for (;;) {
    std::string text;
    const std::string file =
        path + (unified ? "/memory.max" : "/memory.limit_in_bytes");
    if (const db_status_t err = os_memory_read_text(file, text, unified);
        !err.ok()) {
      return (err);
    }
    /* A v2 hierarchy need not enable the memory controller; the reader maps
    its absent limit file to "max". Empty or malformed files remain errors. */
    if (text.empty()) {
      return (db_status_t(DB_IO_ERROR, "empty memory limit in " + file));
    }

    std::istringstream input(text);
    std::string value, extra;
    input >> value;
    uint64_t limit = 0;
    const auto parsed =
        std::from_chars(value.data(), value.data() + value.size(), limit);
    if ((input >> extra) ||
        (value != "max" && (parsed.ec != std::errc() ||
                            parsed.ptr != value.data() + value.size())) ||
        (value == "max" && !unified)) {
      return (db_status_t(DB_IO_ERROR, "invalid memory limit in " + file));
    }

    if (value != "max") {
      /* v1's unlimited sentinel naturally exceeds physical capacity. */
      bytes = std::min(bytes, limit);
    }

    if (path == base) {
      return (DB_SUCCESS);
    }
    path = path.substr(0, path.rfind('/'));
    if (path.empty()) {
      path = "/";
    }
  }
}

/** One visible cgroup mount, decoded from mountinfo. */
struct os_cgroup_mount_t {
  /** Hierarchy path exposed at this mount point. */
  std::string m_root;

  /** Absolute filesystem mount point. */
  std::string m_point;
};

/** Read a mount for the requested controller; unrelated lines are skipped. */
static bool os_cgroup_parse_mount(const std::string &line, bool unified,
                                  os_cgroup_mount_t &mount) {
  const size_t split = line.find(" - ");
  if (split == std::string::npos) {
    return (false);
  }

  std::istringstream left(line.substr(0, split));
  std::istringstream right(line.substr(split + 3));
  std::string id, parent, device, options, type, source, super;
  if (!(left >> id >> parent >> device >> mount.m_root >> mount.m_point >>
        options) ||
      !(right >> type >> source >> super)) {
    return (false);
  }

  if (unified
          ? type != "cgroup2"
          : type != "cgroup" || !os_cgroup_has_controller(super, "memory")) {
    return (false);
  }

  mount.m_root = os_cgroup_decode_path(mount.m_root);
  mount.m_point = os_cgroup_decode_path(mount.m_point);
  return (true);
}

/** Resolve a membership beneath a visible mount; false means another subtree.
 */
static bool os_cgroup_member_path(const std::string &member,
                                  const os_cgroup_mount_t &mount,
                                  std::string &path) {
  std::string relative;
  if (mount.m_root == "/") {
    relative = member == "/" ? "" : member;
  } else if (member == mount.m_root || member == "/") {
    relative = "";
  } else if (member.starts_with(mount.m_root + "/")) {
    relative = member.substr(mount.m_root.size());
  } else {
    return (false);
  }

  path = mount.m_point == "/" ? (relative.empty() ? "/" : relative)
                              : mount.m_point + relative;
  return (true);
}

/** Apply all visible mounts of one process memory-controller membership. */
static db_status_t os_cgroup_apply_member(const std::string &member,
                                          bool unified,
                                          const std::string &mountinfo,
                                          uint64_t &bytes) {
  bool found = false;
  std::istringstream mounts(mountinfo);
  std::string line;
  while (std::getline(mounts, line)) {
    os_cgroup_mount_t mount;
    if (!os_cgroup_parse_mount(line, unified, mount)) {
      continue;
    }

    if (!os_cgroup_path_is_valid(mount.m_root) ||
        !os_cgroup_path_is_valid(mount.m_point)) {
      return (db_status_t(DB_IO_ERROR, "invalid cgroup mount path"));
    }

    std::string path;
    if (!os_cgroup_member_path(member, mount, path)) {
      continue;
    }

    /* Walk up to this mount's root so parent limits also constrain capacity. */
    if (const db_status_t err =
            os_cgroup_apply_limits(mount.m_point, path, unified, bytes);
        !err.ok()) {
      return (err);
    }
    found = true;
  }

  if (!found) {
    return (db_status_t(DB_IO_ERROR,
                        "cannot locate the process memory cgroup mount"));
  }
  return (DB_SUCCESS);
}

/** Reduce capacity by each memory membership, preserving it on failure. */
db_status_t os_cgroup_memory(const std::string &membership,
                             const std::string &mountinfo, uint64_t &bytes) {
  uint64_t effective = bytes;
  std::istringstream groups(membership);
  std::string line;
  while (std::getline(groups, line)) {
    const size_t first = line.find(':');
    const size_t second =
        first == std::string::npos ? first : line.find(':', first + 1);
    if (second == std::string::npos) {
      return (db_status_t(DB_IO_ERROR, "invalid /proc/self/cgroup"));
    }

    const std::string controllers = line.substr(first + 1, second - first - 1);
    const bool unified = line.substr(0, first) == "0" && controllers.empty();
    if (!unified && !os_cgroup_has_controller(controllers, "memory")) {
      continue;
    }

    const std::string member = line.substr(second + 1);
    if (!os_cgroup_path_is_valid(member)) {
      return (db_status_t(DB_IO_ERROR, "invalid cgroup membership path"));
    }

    if (const db_status_t err =
            os_cgroup_apply_member(member, unified, mountinfo, effective);
        !err.ok()) {
      return (err);
    }
  }

  bytes = effective;
  return (DB_SUCCESS);
}

/** Read physical capacity and apply the process's container limits. */
db_status_t os_effective_memory(uint64_t &bytes) {
  bytes = 0;
#ifdef __APPLE__
  size_t size = sizeof(bytes);
  if (sysctlbyname("hw.memsize", &bytes, &size, nullptr, 0) != 0) {
    return (db_status_t(DB_IO_ERROR, "cannot read physical memory capacity"));
  }
#else
  const long pages = sysconf(_SC_PHYS_PAGES);
  const long page_size = sysconf(_SC_PAGESIZE);
  if (pages <= 0 || page_size <= 0 ||
      static_cast<uint64_t>(pages) >
          UINT64_MAX / static_cast<uint64_t>(page_size)) {
    return (db_status_t(DB_IO_ERROR, "cannot read physical memory capacity"));
  }
  bytes = static_cast<uint64_t>(pages) * static_cast<uint64_t>(page_size);
#endif
#ifdef __linux__
  std::string groups, mounts;
  if (const db_status_t err = os_memory_read_text("/proc/self/cgroup", groups);
      !err.ok()) {
    return (err);
  }
  if (const db_status_t err =
          os_memory_read_text("/proc/self/mountinfo", mounts);
      !err.ok()) {
    return (err);
  }
  if (const db_status_t err = os_cgroup_memory(groups, mounts, bytes);
      !err.ok()) {
    return (err);
  }
#endif
  if (bytes < 8) {
    return (db_status_t(DB_IO_ERROR, "effective memory capacity is too small"));
  }
  return (DB_SUCCESS);
}
