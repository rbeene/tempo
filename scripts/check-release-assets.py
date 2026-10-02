#!/usr/bin/env python3
"""Reject incomplete release metadata before making a draft public."""
import json
import re
import sys


def validate(tag, payload):
    if not re.fullmatch(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", tag):
        raise ValueError("invalid stable release version")
    version = tag[1:]
    expected = {f"tempo_{version}_{os}_{arch}.tar.gz"
                for os in ("darwin", "linux") for arch in ("amd64", "arm64")}
    expected.update(("checksums-darwin.txt", "checksums-linux.txt"))
    assets = payload["assets"]
    if len(assets) != len(expected) or {asset["name"] for asset in assets} != expected:
        raise ValueError("release must contain all four versioned archives and both checksums")
    if any(not isinstance(asset["size"], int) or asset["size"] <= 0 for asset in assets):
        raise ValueError("release contains an empty or invalid asset")


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            raise ValueError("expected one version argument")
        validate(sys.argv[1], json.load(sys.stdin))
    except (ValueError, KeyError, TypeError) as error:
        sys.exit(f"release asset check: {error}")
