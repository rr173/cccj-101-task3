"""Shared low-level helpers: JSON encoding, atomic file writes, fsync."""

from __future__ import annotations

import json
import os


def dumps(obj) -> bytes:
    """Canonical JSON encoding used for WAL frames and segment records."""
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def fsync_dir(path: str) -> None:
    """fsync a directory so file creations/renames/removals are durable."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_json(path: str, obj) -> None:
    """Write JSON via tmp-file + fsync + rename + dir-fsync (crash safe)."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=1)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    fsync_dir(os.path.dirname(path) or ".")


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)
