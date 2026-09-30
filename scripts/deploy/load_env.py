"""Emit Compose-style env pairs as NUL-delimited records for shell export."""

from pathlib import Path
import sys


for line in Path(sys.argv[1]).read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    if "=" not in line:
        raise ValueError(f"Invalid environment line: {line[:40]}")
    key, value = line.split("=", 1)
    if not key.isidentifier() or not key.isupper():
        raise ValueError(f"Invalid environment key: {key}")
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    sys.stdout.buffer.write(f"{key}={value}".encode() + b"\0")
