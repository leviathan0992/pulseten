#!/usr/bin/env python3
"""Package the Go archives of one platform as a pulseten-go-libs module."""

import argparse
from pathlib import Path
import shutil
import sys

MODULE = "github.com/leviathan0992/pulseten-go-libs"
ARCHIVES = ("libpulseten.a", "libduckdb_core_functions.a", "libduckdb.a")

# Link order matters on Linux: PulseTen, then the function extension, then
# DuckDB. The group lets the linker resolve references between the archives.
LDFLAGS = {
    "darwin": "-L${SRCDIR} -lpulseten -lduckdb_core_functions -lduckdb -lc++",
    "linux": "-L${SRCDIR} -Wl,--start-group -lpulseten -lduckdb_core_functions "
             "-lduckdb -Wl,--end-group -lstdc++ -lm -ldl -lpthread",
}

NAMES = {
    "darwin-arm64": "macOS on Apple silicon",
    "linux-amd64": "Linux on x86-64",
    "linux-arm64": "Linux on ARM64",
}

LIMIT = 95 * 1024 * 1024  # Stay clear of GitHub's 100 MiB file limit.


def main():
    """Copy the archives and write go.mod and the cgo file."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("platform", choices=sorted(NAMES))
    parser.add_argument("archives", type=Path, help="directory of the .a files")
    parser.add_argument("out", type=Path, help="module directory to create")
    args = parser.parse_args()

    goos, goarch = args.platform.split("-")
    package = args.platform.replace("-", "_")

    args.out.mkdir(parents=True, exist_ok=True)
    for name in ARCHIVES:
        src = args.archives / name
        size = src.stat().st_size
        if size > LIMIT:
            sys.exit(f"{name} is {size / 1048576:.1f} MiB, over the "
                     f"{LIMIT / 1048576:.0f} MiB limit")
        shutil.copyfile(src, args.out / name)

    (args.out / "go.mod").write_text(f"module {MODULE}/{args.platform}\n\ngo 1.22\n")
    (args.out / "lib.go").write_text(f"""\
// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

//go:build {goos} && {goarch}

// Package {package} links the prebuilt PulseTen and DuckDB libraries for
// {NAMES[args.platform]}. Import github.com/leviathan0992/pulseten/go instead.
package {package}

// #cgo LDFLAGS: {LDFLAGS[goos]}
import "C"
""")
    print(f"packaged {args.platform} in {args.out}")


if __name__ == "__main__":
    main()
