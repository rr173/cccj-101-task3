"""Immutable event segments.

A segment is a directory <data_dir>/segments/seg-<first_offset>/ containing:

  events.log   CRC-framed records (same framing as the WAL), arrival order
  index.json   per-device index: entries {seq, offset, event_id, device_ts,
               pos, len} enabling business-order (seq) reads
  meta.json    id, offset range, count, sha256 of events.log, ingest window

Segments are immutable once sealed.  Integrity is verified at startup
(sha256) and on every read (per-frame CRC).  A segment that fails
verification is quarantined by the store: it is excluded from queries and
replays, and the first offset after it is reported as the resume position.
"""

from __future__ import annotations

import hashlib
import json
import os
import zlib
from typing import Dict, List, Optional, Tuple

from . import wal
from .models import fmt_ts, utcnow
from .util import atomic_write_json, dumps, fsync_dir, load_json


class SegmentCorrupt(Exception):
    def __init__(self, seg_id: str, resume_offset: Optional[int], reason: str):
        super().__init__(f"segment {seg_id} corrupt: {reason}")
        self.seg_id = seg_id
        self.resume_offset = resume_offset
        self.reason = reason


def seg_dir(seg_root: str, seg_id: str) -> str:
    return os.path.join(seg_root, seg_id)


def events_path(seg_root: str, seg_id: str) -> str:
    return os.path.join(seg_dir(seg_root, seg_id), "events.log")


def write_segment(seg_root: str, seg_id: str, records: List[dict]) -> Tuple[dict, dict]:
    """Write events.log + index.json + meta.json atomically-ish (fsynced).

    Returns (meta, index).  The segment becomes visible only when the caller
    commits it to the manifest afterwards.
    """
    d = seg_dir(seg_root, seg_id)
    os.makedirs(d, exist_ok=True)

    sha = hashlib.sha256()
    devices: Dict[str, dict] = {}
    pos = 0
    with open(os.path.join(d, "events.log"), "wb") as fh:
        for rec in records:
            frm = wal.frame(dumps(rec))
            fh.write(frm)
            sha.update(frm)
            ev = rec["event"]
            dev = devices.setdefault(ev["device_id"], {"entries": []})
            dev["entries"].append({
                "seq": ev["seq"],
                "offset": rec["offset"],
                "event_id": ev["event_id"],
                "device_ts": ev["device_ts"],
                "pos": pos,
                "len": len(frm),
            })
            pos += len(frm)
        fh.flush()
        os.fsync(fh.fileno())

    for dev_id, dev in devices.items():
        entries = dev["entries"]
        dev["count"] = len(entries)
        dev["min_seq"] = min(e["seq"] for e in entries)
        dev["max_seq"] = max(e["seq"] for e in entries)
        dev["max_device_ts"] = max(e["device_ts"] for e in entries)

    index = {"segment": seg_id, "devices": devices}
    atomic_write_json(os.path.join(d, "index.json"), index)

    meta = {
        "id": seg_id,
        "first_offset": records[0]["offset"],
        "last_offset": records[-1]["offset"],
        "count": len(records),
        "sha256": sha.hexdigest(),
        "status": "sealed",
        "min_ingest_ts": records[0]["ingest_ts"],
        "max_ingest_ts": records[-1]["ingest_ts"],
        "created_at": fmt_ts(utcnow()),
    }
    atomic_write_json(os.path.join(d, "meta.json"), meta)
    fsync_dir(d)
    return meta, index


def load_index(seg_root: str, seg_id: str) -> dict:
    return load_json(os.path.join(seg_dir(seg_root, seg_id), "index.json"))


def verify(seg_root: str, meta: dict) -> Tuple[bool, str]:
    """Whole-file integrity check against the manifest sha256."""
    path = events_path(seg_root, meta["id"])
    if not os.path.exists(path):
        return False, "events.log missing"
    sha = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            sha.update(chunk)
    if sha.hexdigest() != meta.get("sha256"):
        return False, "events.log sha256 mismatch"
    return True, ""


def rebuild_index(seg_root: str, meta: dict) -> dict:
    """Reconstruct index.json from a verified events.log."""
    seg_id = meta["id"]
    devices: Dict[str, dict] = {}
    for pos, payload in wal.iter_frames(events_path(seg_root, seg_id)):
        rec = json.loads(payload)
        ev = rec["event"]
        dev = devices.setdefault(ev["device_id"], {"entries": []})
        dev["entries"].append({
            "seq": ev["seq"],
            "offset": rec["offset"],
            "event_id": ev["event_id"],
            "device_ts": ev["device_ts"],
            "pos": pos,
            "len": wal.HEADER.size + len(payload),
        })
    for dev in devices.values():
        entries = dev["entries"]
        dev["count"] = len(entries)
        dev["min_seq"] = min(e["seq"] for e in entries)
        dev["max_seq"] = max(e["seq"] for e in entries)
        dev["max_device_ts"] = max(e["device_ts"] for e in entries)
    index = {"segment": seg_id, "devices": devices}
    atomic_write_json(os.path.join(seg_dir(seg_root, seg_id), "index.json"), index)
    return index


def scan_records(seg_root: str, meta: dict, from_offset: int = 0,
                 limit: Optional[int] = None) -> Tuple[List[dict], bool]:
    """Sequentially read records with offset >= from_offset.

    Returns (records, complete).  complete=False means `limit` was hit and
    more records may follow.  Raises SegmentCorrupt on any frame error.
    """
    seg_id = meta["id"]
    resume = meta["last_offset"] + 1
    out: List[dict] = []
    complete = True
    try:
        for _pos, payload in wal.iter_frames(events_path(seg_root, seg_id)):
            rec = json.loads(payload)
            if rec["offset"] < from_offset:
                continue
            out.append(rec)
            if limit is not None and len(out) >= limit:
                complete = False
                break
    except wal.TornTail as t:
        raise SegmentCorrupt(seg_id, resume, f"torn frame at byte {t.pos}")
    except wal.FrameCorrupt as c:
        raise SegmentCorrupt(seg_id, resume, f"frame corrupt at byte {c.pos}: {c.reason}")
    return out, complete


def read_record_at(seg_root: str, seg_id: str, pos: int, length: int,
                   fh=None) -> dict:
    """Random-access read of one framed record with CRC verification."""
    own = fh is None
    if own:
        fh = open(events_path(seg_root, seg_id), "rb")
    try:
        fh.seek(pos)
        raw = fh.read(length)
    finally:
        if own:
            fh.close()
    if len(raw) != length:
        raise SegmentCorrupt(seg_id, None, "short read")
    crc, plen = wal.HEADER.unpack(raw[: wal.HEADER.size])
    payload = raw[wal.HEADER.size:]
    if plen != len(payload) or crc != (zlib.crc32(payload) & 0xFFFFFFFF):
        raise SegmentCorrupt(seg_id, None, "crc32 mismatch")
    return json.loads(payload)
