#!/usr/bin/env python3
"""Verify that Clippy rejects every direct handler I/O method in the negative fixture."""

import collections
import json
from pathlib import Path
import re
import subprocess
import sys


def main():
    """Run the negative fixture and require only the configured handler-method errors."""
    root = Path(__file__).resolve().parents[2]
    config = (root / "kernel/clippy.toml").read_text()
    methods = re.findall(r'path = "(delta_kernel::[^"]+)"', config)
    if not methods:
        raise ValueError("No handler I/O lint rules found")
    expected = collections.Counter({method: 2 for method in methods})
    result = subprocess.run(
        [
            "cargo", "clippy", "--offline", "--locked", "-p", "delta_kernel",
            "--lib", "--no-default-features", "--message-format=json",
            "--", "--cfg", "engine_io_lint_test",
        ],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    actual = collections.Counter()
    unexpected = []
    for line in result.stdout.splitlines():
        event = json.loads(line)
        if event.get("reason") != "compiler-message":
            continue
        message = event["message"]
        if message["level"] != "error":
            continue
        code = message.get("code") or {}
        match = re.fullmatch(r"use of a disallowed method `([^`]+)`", message["message"])
        if code.get("code") == "clippy::disallowed_methods" and match:
            actual[match[1]] += 1
        else:
            unexpected.append(message["rendered"])
    if result.returncode == 0 or actual != expected or unexpected:
        print(result.stderr, file=sys.stderr)
        print(f"Expected: {expected}\nActual: {actual}", file=sys.stderr)
        print("".join(unexpected), file=sys.stderr)
        return 1
    print(f"Verified {sum(actual.values())} rejected direct handler calls.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
