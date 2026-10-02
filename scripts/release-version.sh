#!/bin/sh
# Pure selection: reads Git metadata, never creates tags or releases.
set -eu
exec python3 - <<'PY'
import re
import subprocess
import sys

def tags(*args):
    return subprocess.check_output(["git", "tag", *args], text=True).splitlines()

def stable(names):
    return [name for name in names if re.fullmatch(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", name)]

try:
    subprocess.run(["git", "rev-parse", "--verify", "HEAD"], check=True, stdout=subprocess.DEVNULL)
    current = stable(tags("--points-at", "HEAD"))
    if len(current) > 1:
        sys.exit("release version: multiple stable version tags point at HEAD")
    if current:
        print(current[0])
    else:
        versions = [tuple(map(int, name[1:].split("."))) for name in stable(tags())]
        if versions:
            major, minor, patch = max(versions)
            print(f"v{major}.{minor}.{patch + 1}")
        else:
            print("v0.1.0")
except subprocess.CalledProcessError:
    sys.exit("release version: cannot read Git history")
PY
