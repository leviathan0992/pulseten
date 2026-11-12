#!/usr/bin/env python3
"""Check repository-owned C/C++ and Go files without changing them."""

import argparse
from pathlib import Path
import subprocess
import sys


def main():
    """Use the pinned formatter and exclude generated and third-party files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clang-format", default="clang-format")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    version = subprocess.check_output([args.clang_format, "--version"], text=True)
    if "version 22.1.8" not in version:
        sys.exit("clang-format 22.1.8 is required; select it with --clang-format /path/to/clang-format")

    files = sorted(
        str(path.relative_to(root))
        for directory in ("src", "include", "tests", "examples", "go")
        for path in (root / directory).rglob("*")
        if path.suffix in (".cc", ".c", ".h")
    )
    cpp = subprocess.run(
        [args.clang_format, "--dry-run", "--Werror", *files], cwd=root
    )
    go_files = sorted(str(path.relative_to(root)) for path in (root / "go").glob("*.go"))
    go = subprocess.run(
        ["gofmt", "-l", *go_files], cwd=root, text=True, capture_output=True
    )
    if go.stdout or go.stderr:
        print(go.stdout + go.stderr, end="")
    return int(cpp.returncode != 0 or go.returncode != 0 or bool(go.stdout))


if __name__ == "__main__":
    sys.exit(main())
