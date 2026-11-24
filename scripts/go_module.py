#!/usr/bin/env python3
"""Package the Go archives of one platform as a pulseten-go-libs module."""

import argparse
from pathlib import Path
import shutil
import subprocess
import sys

MODULE = "github.com/leviathan0992/pulseten-go-libs"
ARCHIVES = ("libpulseten.a", "libduckdb_core_functions.a", "libduckdb.a")

# Link order matters on Linux: PulseTen, then the function extension, then
# DuckDB. The group lets the linker resolve references between the archives.
LDFLAGS = {
    "darwin": "-L${{SRCDIR}} {libs} -lc++",
    "linux": "-L${{SRCDIR}} -Wl,--start-group {libs} -Wl,--end-group "
             "-lstdc++ -lm -ldl -lpthread",
}

NAMES = {
    "darwin-arm64": "macOS on Apple silicon",
    "linux-amd64": "Linux on x86-64 with glibc",
    "linux-amd64-musl": "Linux on x86-64 with musl",
    "linux-arm64": "Linux on ARM64 with glibc",
    "linux-arm64-musl": "Linux on ARM64 with musl",
}

LIMIT = 95 * 1024 * 1024  # Stay clear of GitHub's 100 MiB file limit.

AR_MAGIC = b"!<arch>\n"


def read_members(path):
    """Read a GNU ar archive.
    Return its table of long names, then its other members. Each one is the
    raw header and data; the symbol table is left out."""
    data = path.read_bytes()
    if not data.startswith(AR_MAGIC):
        sys.exit(f"{path} is not a regular ar archive")
    long_names, members = b"", []
    pos = len(AR_MAGIC)
    while pos < len(data):
        name = data[pos:pos + 16].rstrip()
        size = int(data[pos + 48:pos + 58])
        end = pos + 60 + size + size % 2
        if name == b"//":
            long_names = data[pos:end]
        elif name not in (b"/", b"/SYM64/"):
            members.append(data[pos:end])
        pos = end

    return long_names, members


def split_archive(src, out):
    """Split an archive over the limit into parts of similar size.
    Return the part names."""
    long_names, members = read_members(src)
    total = sum(map(len, members))
    count = total // (LIMIT * 3 // 4) + 1
    parts = [[] for _ in range(count)]
    done = 0
    for member in members:
        parts[min(done * count // total, count - 1)].append(member)
        done += len(member)

    names = []
    for number, part in enumerate(parts, 1):
        name = f"{src.stem}_{number}.a"
        (out / name).write_bytes(AR_MAGIC + long_names + b"".join(part))
        subprocess.run(["ranlib", out / name], check=True)
        names.append(name)

    return names


def main():
    """Copy the archives, split if too large, and write go.mod and the cgo file."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("platform", choices=sorted(NAMES))
    parser.add_argument("archives", type=Path, help="directory of the .a files")
    parser.add_argument("out", type=Path, help="module directory to create")
    args = parser.parse_args()

    goos, goarch = args.platform.split("-")[:2]
    package = args.platform.replace("-", "_")

    args.out.mkdir(parents=True, exist_ok=True)
    names = []
    for name in ARCHIVES:
        src = args.archives / name
        if src.stat().st_size > LIMIT and goos == "linux":
            names += split_archive(src, args.out)
        else:
            shutil.copyfile(src, args.out / name)
            names.append(name)
    for name in names:
        size = (args.out / name).stat().st_size
        if size > LIMIT:
            sys.exit(f"{name} is {size / 1048576:.1f} MiB, over the "
                     f"{LIMIT / 1048576:.0f} MiB limit")
    libs = " ".join(f"-l{name[3:-2]}" for name in names)

    (args.out / "go.mod").write_text(f"module {MODULE}/{args.platform}\n\ngo 1.22\n")
    (args.out / "lib.go").write_text(f"""\
// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

//go:build {goos} && {goarch}

// Package {package} links the prebuilt PulseTen and DuckDB libraries for
// {NAMES[args.platform]}. Import github.com/leviathan0992/pulseten/go instead.
package {package}

// #cgo LDFLAGS: {LDFLAGS[goos].format(libs=libs)}
import "C"
""")
    print(f"packaged {args.platform} in {args.out}")


if __name__ == "__main__":
    main()
