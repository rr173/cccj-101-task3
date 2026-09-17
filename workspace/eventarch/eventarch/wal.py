"""Write-ahead log: CRC-framed append-only records, rotation, crash recovery.

Frame layout (little-endian):
    [crc32: uint32][length: uint32][json payload: length bytes]
crc32 covers the payload only.

WAL files live in <data_dir>/wal and are named by the offset that was next
when the file was opened:  %020d.wal.  Rotation happens at segment-seal
boundaries, but a file is NOT a 1:1 image of one segment: records already
buffered when rotation happens stay in the previous file, and a batch that
spans several seal boundaries lands entirely in the file active at ingest
time (intermediate rotated files may stay empty).  Readers must therefore
locate records by offset range across files, never by file name alone.
Sealed segments' WAL coverage is kept for a retention window to allow
rebuilding a corrupted segment.
"""

from __future__ import annotations

import json
import logging
import os
import struct
import zlib
from typing import Iterator, List, Optional, Tuple

from .util import dumps, fsync_dir

log = logging.getLogger("eventarch.wal")

HEADER = struct.Struct("<II")  # (crc32, length)
MAX_FRAME = 64 << 20  # hard sanity cap for a single frame (64 MiB)


class TornTail(Exception):
    """Incomplete frame at the end of a file (expected after a crash)."""

    def __init__(self, pos: int):
        super().__init__(f"torn frame at byte {pos}")
        self.pos = pos


class FrameCorrupt(Exception):
    """A complete-looking frame that fails validation."""

    def __init__(self, pos: int, reason: str):
        super().__init__(f"corrupt frame at byte {pos}: {reason}")
        self.pos = pos
        self.reason = reason


def frame(payload: bytes) -> bytes:
    return HEADER.pack(zlib.crc32(payload) & 0xFFFFFFFF, len(payload)) + payload


def iter_frames(path: str) -> Iterator[Tuple[int, bytes]]:
    """Yield (byte_pos, payload) for each valid frame.

    Raises TornTail on a truncated tail, FrameCorrupt on invalid data.
    """
    pos = 0
    with open(path, "rb") as fh:
        while True:
            header = fh.read(HEADER.size)
            if not header:
                return  # clean EOF
            if len(header) < HEADER.size:
                raise TornTail(pos)
            crc, length = HEADER.unpack(header)
            if length > MAX_FRAME:
                raise FrameCorrupt(pos, f"implausible frame length {length}")
            payload = fh.read(length)
            if len(payload) < length:
                raise TornTail(pos)
            if (zlib.crc32(payload) & 0xFFFFFFFF) != crc:
                raise FrameCorrupt(pos, "crc32 mismatch")
            yield pos, payload
            pos += HEADER.size + length


def truncate(path: str, pos: int) -> None:
    with open(path, "r+b") as fh:
        fh.truncate(pos)
        fh.flush()
        os.fsync(fh.fileno())


def read_all_payloads(path: str) -> List[bytes]:
    return [payload for _, payload in iter_frames(path)]


def last_offset(path: str) -> Optional[int]:
    """Offset of the newest record in a WAL file, or None if it is empty.

    Records are appended in offset order (and compact() writes them sorted),
    so the last valid frame carries the file's maximum offset; only that
    payload is decoded.
    """
    last_payload: Optional[bytes] = None
    for _pos, payload in iter_frames(path):
        last_payload = payload
    if last_payload is None:
        return None
    return json.loads(last_payload)["offset"]


def _wal_files(wal_dir: str) -> List[str]:
    names = [n for n in os.listdir(wal_dir) if n.endswith(".wal")]
    return sorted(names)  # zero-padded offset names sort correctly


def recover(wal_dir: str, sealed_through: int) -> Tuple[List[dict], List[dict], List[str]]:
    """Scan all WAL files and return (live_records, gaps, contributing_files).

    live_records: decoded records with offset > sealed_through, deduplicated
    and ordered by offset.  contributing_files: WAL files that contained at
    least one live record (candidates for compaction).  Torn tails of the
    newest file are truncated (normal crash case).  Corrupt older files are
    quarantined (renamed to *.corrupt) and reported as gaps; recovery
    continues from the next valid file so the node keeps serving with an
    explicitly bounded loss window.
    """
    files = _wal_files(wal_dir)
    live: List[dict] = []
    gaps: List[dict] = []
    contributing: List[str] = []
    last_offset = sealed_through

    for i, name in enumerate(files):
        path = os.path.join(wal_dir, name)
        is_last = i == len(files) - 1
        corrupt_reason: Optional[str] = None
        try:
            for _pos, payload in iter_frames(path):
                rec = json.loads(payload)
                off = rec["offset"]
                if off <= last_offset:
                    continue  # already sealed or already collected
                if off > last_offset + 1:
                    gaps.append({
                        "type": "wal_gap",
                        "from_offset": last_offset + 1,
                        "to_offset": off - 1,
                        "resume_offset": off,
                    })
                live.append(rec)
                last_offset = off
                if name not in contributing:
                    contributing.append(name)
        except TornTail as t:
            if is_last:
                log.warning("truncating torn WAL tail of %s at byte %d", name, t.pos)
                truncate(path, t.pos)
            else:
                corrupt_reason = f"torn frame at byte {t.pos} in non-final WAL file"
        except FrameCorrupt as c:
            if is_last:
                log.warning("truncating corrupt WAL tail of %s at byte %d", name, c.pos)
                truncate(path, c.pos)
                gaps.append({
                    "type": "wal_tail_corrupt",
                    "file": name,
                    "byte_pos": c.pos,
                    "reason": c.reason,
                    "resume_offset": last_offset + 1,
                })
            else:
                corrupt_reason = c.reason

        if corrupt_reason is not None:
            bad = path + ".corrupt"
            os.replace(path, bad)
            fsync_dir(wal_dir)
            gaps.append({
                "type": "wal_file_corrupt",
                "file": name,
                "reason": corrupt_reason,
                "resume_offset": last_offset + 1,
            })
            log.error("quarantined corrupt WAL file %s: %s", name, corrupt_reason)

    return live, gaps, contributing


def compact(wal_dir: str, keep_from: int, contributing: List[str]) -> Optional[str]:
    """Merge WAL files that hold unsealed records into one fresh file.

    Records with offset >= keep_from (the retention horizon) are preserved
    in the merged file, so sealed-but-retained records sharing a file with
    the unsealed tail (crash between seal and rotation) keep their rebuild
    coverage.  Dedicated WAL files of older retained segments are left
    untouched.  Crash-safe ordering: write tmp -> fsync -> rename -> delete
    old files -> fsync dir.
    """
    if not contributing:
        return None

    merged: dict = {}
    for name in contributing:
        path = os.path.join(wal_dir, name)
        if not os.path.exists(path):
            continue
        for _pos, payload in iter_frames(path):
            rec = json.loads(payload)
            if rec["offset"] >= keep_from:
                merged[rec["offset"]] = rec

    new_path = None
    if merged:
        records = [merged[o] for o in sorted(merged)]
        base = records[0]["offset"]
        tmp = os.path.join(wal_dir, f"{base:020d}.wal.tmp")
        with open(tmp, "wb") as fh:
            for rec in records:
                fh.write(frame(dumps(rec)))
            fh.flush()
            os.fsync(fh.fileno())
        new_path = os.path.join(wal_dir, f"{base:020d}.wal")
        os.replace(tmp, new_path)

    for name in contributing:
        path = os.path.join(wal_dir, name)
        if new_path is not None and os.path.abspath(path) == os.path.abspath(new_path):
            continue
        if os.path.exists(path):
            os.remove(path)
    fsync_dir(wal_dir)
    return new_path


class WALWriter:
    """Appends records to the active WAL file; rotates on segment seal."""

    def __init__(self, wal_dir: str, base_offset: int, do_fsync: bool = True):
        self._dir = wal_dir
        self._do_fsync = do_fsync
        self._fh = None
        self.base_offset = -1
        self._open(base_offset)

    def _open(self, base_offset: int) -> None:
        self.base_offset = base_offset
        self.path = os.path.join(self._dir, f"{base_offset:020d}.wal")
        self._fh = open(self.path, "ab")

    def append(self, record: dict) -> None:
        self._fh.write(frame(dumps(record)))

    def fsync(self) -> None:
        self._fh.flush()
        if self._do_fsync:
            os.fsync(self._fh.fileno())

    def rotate(self, new_base: int) -> None:
        self.fsync()
        self._fh.close()
        self._open(new_base)
        fsync_dir(self._dir)

    def close(self) -> None:
        try:
            self.fsync()
        finally:
            self._fh.close()
