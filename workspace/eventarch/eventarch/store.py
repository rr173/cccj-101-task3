"""ArchiveStore: ingest, classification, segments, freeze/replay, recovery.

Threading model
---------------
A single RLock guards all in-memory state and WAL appends.  Read paths that
touch immutable segment files build a plan under the lock, then perform file
I/O *outside* the lock so that long replays never block ingestion.

Durability
----------
Every ingest batch is appended to the WAL and fsync'd before the ACK is
returned.  Segment files are fsync'd, the manifest is atomically replaced
(tmp + rename + dir fsync), and only then is the WAL rotated.  On restart,
recovery verifies every sealed segment (sha256), truncates torn WAL tails,
quarantines corrupt files, and rebuilds in-memory indexes from the manifest
plus the unsealed WAL tail.
"""

from __future__ import annotations

import bisect
import json
import logging
import os
import shutil
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple

from . import segments as segmod
from . import wal as walmod
from .models import fmt_ts, new_flags, parse_ts, utcnow, validate_event
from .util import atomic_write_json, fsync_dir, load_json

log = logging.getLogger("eventarch.store")

OPEN = ""  # seg_id marker for entries still living in the open (unsealed) buffer


class NotFound(Exception):
    pass


class Quarantined(Exception):
    def __init__(self, seg_id: str, resume_offset: int, reason: str):
        super().__init__(f"segment {seg_id} is quarantined: {reason}")
        self.seg_id = seg_id
        self.resume_offset = resume_offset
        self.reason = reason


class WalCoverageGone(Exception):
    def __init__(self, seg_id: str, resume_offset: int):
        super().__init__(f"WAL coverage for {seg_id} is no longer retained")
        self.seg_id = seg_id
        self.resume_offset = resume_offset


class Entry:
    """One event's position in a device's business-ordered index."""

    __slots__ = ("seq", "offset", "seg_id", "pos", "length", "event_id", "device_ts")

    def __init__(self, seq, offset, seg_id, pos, length, event_id, device_ts):
        self.seq = seq
        self.offset = offset
        self.seg_id = seg_id
        self.pos = pos
        self.length = length
        self.event_id = event_id
        self.device_ts = device_ts


class DeviceState:
    __slots__ = ("entries", "event_ids", "seqs", "max_seq", "max_device_ts")

    def __init__(self):
        self.entries: List[Entry] = []   # sorted by (seq, offset)
        self.event_ids: Dict[str, int] = {}
        self.seqs: set = set()
        self.max_seq: Optional[int] = None
        self.max_device_ts = None


def _entry_key(e: Entry):
    return (e.seq, e.offset)


class ArchiveStore:
    def __init__(self, cfg):
        self.cfg = cfg
        self.data_dir = cfg.data_dir
        self.wal_dir = os.path.join(self.data_dir, "wal")
        self.seg_root = os.path.join(self.data_dir, "segments")
        self.state_dir = os.path.join(self.data_dir, "state")
        self._manifest_path = os.path.join(self.state_dir, "manifest.json")
        self._freezes_path = os.path.join(self.state_dir, "freezes.json")

        self._lock = threading.RLock()
        self.manifest = {"next_offset": 0, "sealed_through": -1, "segments": []}
        self._seg_by_id: Dict[str, dict] = {}
        self._devices: Dict[str, DeviceState] = {}
        self._open_records: List[dict] = []
        self._open_first_offset: Optional[int] = None
        self._open_started: Optional[float] = None
        self._next_offset = 0
        self._wal: Optional[walmod.WALWriter] = None
        self._freezes: List[dict] = []
        self._wal_gaps: List[dict] = []
        self._counters = {
            "ingested": 0, "duplicates": 0, "late": 0,
            "clock_rollback": 0, "seq_conflict": 0, "rejected": 0,
        }
        self._started_at = time.monotonic()
        self._janitor_stop = threading.Event()
        self._janitor: Optional[threading.Thread] = None

    # ------------------------------------------------------------------ #
    # recovery                                                            #
    # ------------------------------------------------------------------ #

    def open(self) -> None:
        for d in (self.wal_dir, self.seg_root, self.state_dir):
            os.makedirs(d, exist_ok=True)

        if os.path.exists(self._manifest_path):
            self.manifest = load_json(self._manifest_path)
        self._seg_by_id = {m["id"]: m for m in self.manifest["segments"]}
        sealed_through = self.manifest.get("sealed_through", -1)

        changed = False
        # 1. verify sealed segments, load their indexes
        seg_indexes: Dict[str, dict] = {}
        for meta in self.manifest["segments"]:
            seg_id = meta["id"]
            if meta["status"] == "quarantined":
                try:
                    seg_indexes[seg_id] = segmod.load_index(self.seg_root, seg_id)
                except Exception:
                    pass  # index optional for quarantined segments
                continue
            ok, reason = segmod.verify(self.seg_root, meta)
            if not ok:
                log.error("segment %s failed verification: %s -> quarantine", seg_id, reason)
                self._mark_quarantined(meta, reason)
                changed = True
                # still load its index if possible so device queries can
                # report the gap (with resume position) instead of silently
                # hiding the affected sequence range
                try:
                    seg_indexes[seg_id] = segmod.load_index(self.seg_root, seg_id)
                except Exception:
                    pass
                continue
            try:
                seg_indexes[seg_id] = segmod.load_index(self.seg_root, seg_id)
            except Exception as exc:
                log.warning("index of %s unreadable (%s), rebuilding from events.log", seg_id, exc)
                try:
                    seg_indexes[seg_id] = segmod.rebuild_index(self.seg_root, meta)
                except Exception as exc2:
                    log.error("index rebuild failed for %s: %s -> quarantine", seg_id, exc2)
                    self._mark_quarantined(meta, f"index rebuild failed: {exc2}")
                    changed = True

        # 2. drop orphan segment dirs (crash between file write and manifest commit)
        for name in os.listdir(self.seg_root):
            if name.startswith("seg-") and name not in self._seg_by_id:
                log.warning("removing orphan segment dir %s (never committed)", name)
                shutil.rmtree(os.path.join(self.seg_root, name), ignore_errors=True)
        fsync_dir(self.seg_root)

        # 3. recover WAL tail (records beyond the sealed horizon), then
        #    compact only the files that carried unsealed records; dedicated
        #    WAL files of retained sealed segments stay as rebuild coverage.
        live, gaps, contributing = walmod.recover(self.wal_dir, sealed_through)
        self._wal_gaps = gaps
        merged_path = walmod.compact(self.wal_dir, self._wal_keep_from(), contributing)

        # 4. rebuild in-memory device indexes
        for meta in self.manifest["segments"]:
            idx = seg_indexes.get(meta["id"])
            if idx:
                self._apply_segment_index(meta["id"], idx)
        for rec in live:
            self._apply(rec)

        self._next_offset = max(
            sealed_through + 1,
            (live[-1]["offset"] + 1) if live else 0,
        )
        if self.manifest.get("next_offset") != self._next_offset:
            self.manifest["next_offset"] = self._next_offset
            changed = True
        if changed:
            self._persist_manifest()

        if merged_path is not None:
            base = int(os.path.basename(merged_path)[:-4])
        else:
            base = self._next_offset
        self._wal = walmod.WALWriter(self.wal_dir, base, do_fsync=self.cfg.fsync)
        self._collect_wal()

        if os.path.exists(self._freezes_path):
            self._freezes = load_json(self._freezes_path).get("freezes", [])

        log.info(
            "recovery complete: %d segments (%d quarantined), %d live WAL records, "
            "next_offset=%d, devices=%d, wal_gaps=%d",
            len(self.manifest["segments"]),
            sum(1 for m in self.manifest["segments"] if m["status"] == "quarantined"),
            len(live), self._next_offset, len(self._devices), len(gaps),
        )

    def _apply_segment_index(self, seg_id: str, index: dict) -> None:
        for dev_id, d in index.get("devices", {}).items():
            dev = self._devices.setdefault(dev_id, DeviceState())
            for ie in d.get("entries", []):
                dev.entries.append(Entry(
                    ie["seq"], ie["offset"], seg_id, ie["pos"], ie["len"],
                    ie["event_id"], parse_ts(ie["device_ts"]),
                ))
                dev.event_ids[ie["event_id"]] = ie["offset"]
                dev.seqs.add(ie["seq"])
            dev.entries.sort(key=_entry_key)
            self._refresh_device_extremes(dev)

    @staticmethod
    def _refresh_device_extremes(dev: DeviceState) -> None:
        if not dev.entries:
            return
        dev.max_seq = max(e.seq for e in dev.entries)
        dev.max_device_ts = max(e.device_ts for e in dev.entries)

    # ------------------------------------------------------------------ #
    # ingest                                                              #
    # ------------------------------------------------------------------ #

    def ingest(self, raw_events) -> List[dict]:
        if not isinstance(raw_events, list) or not raw_events:
            raise ValueError("body must contain a non-empty 'events' array")
        if len(raw_events) > self.cfg.max_batch:
            raise ValueError(f"batch too large (>{self.cfg.max_batch} events)")

        now = utcnow()
        results: List[dict] = []
        with self._lock:
            base = self._next_offset
            pending: List[dict] = []
            pending_offsets: Dict[str, int] = {}

            for raw in raw_events:
                ev, dt, err = validate_event(raw)
                if err:
                    self._counters["rejected"] += 1
                    results.append({
                        "event_id": raw.get("event_id") if isinstance(raw, dict) else None,
                        "status": "error", "error": err,
                    })
                    continue

                dev = self._devices.get(ev["device_id"])
                dup_offset = pending_offsets.get(ev["event_id"])
                if dup_offset is None and dev is not None:
                    dup_offset = dev.event_ids.get(ev["event_id"])
                if dup_offset is not None:
                    self._counters["duplicates"] += 1
                    flags = new_flags()
                    flags["duplicate"] = True
                    results.append({
                        "event_id": ev["event_id"], "status": "duplicate",
                        "offset": dup_offset, "flags": flags,
                    })
                    continue

                flags = self._classify(dev, ev, dt, now)
                rec = {
                    "offset": base + len(pending),
                    "ingest_ts": fmt_ts(now),
                    "event": ev,
                    "flags": flags,
                }
                pending.append(rec)
                pending_offsets[ev["event_id"]] = rec["offset"]
                results.append({
                    "event_id": ev["event_id"], "status": "stored",
                    "offset": rec["offset"], "flags": flags,
                })

            if pending:
                # WAL append + fsync BEFORE ack; in-memory state only after success.
                for rec in pending:
                    self._wal.append(rec)
                self._wal.fsync()
                for rec in pending:
                    self._apply(rec)
                self._next_offset = base + len(pending)
                self._counters["ingested"] += len(pending)
                self._maybe_seal()
        return results

    def _classify(self, dev: Optional[DeviceState], ev: dict, dt, now) -> dict:
        flags = new_flags()
        if dev is not None:
            if ev["seq"] in dev.seqs:
                flags["seq_conflict"] = True
            if dev.max_device_ts is not None and dt < dev.max_device_ts:
                flags["clock_rollback"] = True
        if (now - dt).total_seconds() > self.cfg.late_threshold_sec:
            flags["late"] = True
        for k, v in flags.items():
            if v:
                self._counters[k] += 1
        return flags

    def _apply(self, rec: dict) -> None:
        ev = rec["event"]
        dev = self._devices.get(ev["device_id"])
        if dev is None:
            dev = self._devices[ev["device_id"]] = DeviceState()
        entry = Entry(
            ev["seq"], rec["offset"], OPEN, len(self._open_records), 0,
            ev["event_id"], parse_ts(ev["device_ts"]),
        )
        bisect.insort(dev.entries, entry, key=_entry_key)
        dev.event_ids[ev["event_id"]] = rec["offset"]
        dev.seqs.add(ev["seq"])
        dev.max_seq = ev["seq"] if dev.max_seq is None else max(dev.max_seq, ev["seq"])
        if dev.max_device_ts is None or entry.device_ts > dev.max_device_ts:
            dev.max_device_ts = entry.device_ts
        self._open_records.append(rec)
        if self._open_first_offset is None:
            self._open_first_offset = rec["offset"]
            self._open_started = time.monotonic()

    # ------------------------------------------------------------------ #
    # segment lifecycle                                                   #
    # ------------------------------------------------------------------ #

    def _maybe_seal(self) -> None:
        while len(self._open_records) >= self.cfg.segment_max_records:
            self._seal_open(self.cfg.segment_max_records)

    def _seal_open(self, count: Optional[int] = None) -> Optional[dict]:
        """Seal the oldest `count` open records (all of them if None)."""
        if not self._open_records:
            return None
        records = self._open_records if count is None else self._open_records[:count]
        seg_id = f"seg-{records[0]['offset']:020d}"
        meta, index = segmod.write_segment(self.seg_root, seg_id, records)
        self.manifest["segments"].append(meta)
        self.manifest["sealed_through"] = records[-1]["offset"]
        self.manifest["next_offset"] = self._next_offset
        self._seg_by_id[seg_id] = meta
        self._persist_manifest()  # commit point: segment visible from here on

        # re-point device entries from the open buffer to sealed positions;
        # the sealed records are always the oldest ones still OPEN
        for dev_id, d in index["devices"].items():
            dev = self._devices[dev_id]
            open_entries = sorted(
                (e for e in dev.entries if e.seg_id == OPEN), key=lambda e: e.offset)
            idx_entries = sorted(d["entries"], key=lambda e: e["offset"])
            n = len(idx_entries)
            assert [e.offset for e in open_entries[:n]] == \
                   [ie["offset"] for ie in idx_entries]
            for ent, ie in zip(open_entries[:n], idx_entries):
                ent.seg_id, ent.pos, ent.length = seg_id, ie["pos"], ie["len"]

        rest = self._open_records[len(records):]
        self._open_records = rest
        self._open_first_offset = rest[0]["offset"] if rest else None
        if rest:
            self._open_started = time.monotonic()
            # remaining OPEN entries index into the truncated buffer
            shift = len(records)
            for dev in self._devices.values():
                for e in dev.entries:
                    if e.seg_id == OPEN:
                        e.pos -= shift
        self._wal.rotate(self.manifest["sealed_through"] + 1)
        self._collect_wal()
        log.info("sealed %s: offsets [%d..%d], %d records",
                 seg_id, meta["first_offset"], meta["last_offset"], meta["count"])
        return meta

    def _wal_keep_from(self) -> int:
        """Retention horizon: WAL files with base offset below this may go.

        Keeps the last `wal_retain_segments` sealed segments plus anything
        backing a quarantined segment (needed for rebuild)."""
        sealed = sorted(
            (m for m in self.manifest["segments"] if m["status"] == "sealed"),
            key=lambda m: m["first_offset"])
        keep_from = 0
        if len(sealed) > self.cfg.wal_retain_segments:
            keep_from = sealed[-self.cfg.wal_retain_segments]["first_offset"]
        quarantined = [m["first_offset"] for m in self.manifest["segments"]
                       if m["status"] == "quarantined"]
        if quarantined:
            keep_from = min(keep_from, min(quarantined))
        return keep_from

    def _collect_wal(self) -> None:
        """Drop WAL files whose records all lie below the retention horizon.

        A file named by base B can also hold records *above* B (records
        buffered when rotation happened, or a batch spanning several seal
        boundaries), so a below-horizon file is only removed after
        confirming its newest record is below keep_from.
        """
        keep_from = self._wal_keep_from()
        for name in os.listdir(self.wal_dir):
            if not name.endswith(".wal"):
                continue
            base = int(name[:-4])
            if base >= keep_from:
                continue
            path = os.path.join(self.wal_dir, name)
            try:
                newest = walmod.last_offset(path)
            except Exception as exc:
                log.warning("cannot assess WAL file %s (%s); keeping it", name, exc)
                continue
            if newest is not None and newest >= keep_from:
                continue  # still carries records inside the retention window
            os.remove(path)
            log.info("collected WAL file %s (beyond retention)", name)
        fsync_dir(self.wal_dir)

    # ------------------------------------------------------------------ #
    # freeze & replay                                                     #
    # ------------------------------------------------------------------ #

    def freeze(self, note: str = "") -> dict:
        """Pin a consistent view: seal the open segment and record the horizon.

        Everything with offset < end_offset is immutable after this call;
        new data lands in fresh segments and can never leak into this view.
        """
        with self._lock:
            self._seal_open()
            frz = {
                "id": f"frz-{time.strftime('%Y%m%dT%H%M%S', time.gmtime())}-{uuid.uuid4().hex[:8]}",
                "note": note,
                "created_at": fmt_ts(utcnow()),
                "end_offset": self._next_offset,  # exclusive horizon
                "segments": [m["id"] for m in self.manifest["segments"]],
            }
            self._freezes.append(frz)
            self._persist_freezes()
            log.info("freeze %s created at end_offset=%d (%d segments)",
                     frz["id"], frz["end_offset"], len(frz["segments"]))
            return frz

    def list_freezes(self) -> List[dict]:
        with self._lock:
            return list(self._freezes)

    def replay(self, freeze_id: Optional[str] = None, from_offset: int = 0,
               device_id: Optional[str] = None, limit: int = 500) -> dict:
        """Stream a frozen view (or, without freeze_id, the current head).

        New ingestion is unaffected: the plan is a snapshot of immutable
        segments plus a copy of the open buffer.
        """
        with self._lock:
            if freeze_id is not None:
                frz = next((f for f in self._freezes if f["id"] == freeze_id), None)
                if frz is None:
                    raise NotFound(f"freeze {freeze_id} not found")
                metas = [dict(self._seg_by_id[s]) for s in frz["segments"]
                         if s in self._seg_by_id]
                end_offset = frz["end_offset"]
                open_snapshot: List[dict] = []
            else:
                metas = [dict(m) for m in self.manifest["segments"]]
                end_offset = self._next_offset
                open_snapshot = list(self._open_records)

        events: List[dict] = []
        gaps: List[dict] = []
        scanned_through = from_offset - 1
        complete = True

        def emit(rec):
            if device_id is None or rec["event"]["device_id"] == device_id:
                events.append(rec)
                return len(events) >= limit
            return False

        for meta in metas:
            if meta["last_offset"] < from_offset:
                continue
            if meta["status"] == "quarantined":
                gaps.append({
                    "segment": meta["id"],
                    "reason": meta.get("quarantine_reason", ""),
                    "resume_offset": meta["last_offset"] + 1,
                })
                scanned_through = max(scanned_through, meta["last_offset"])
                continue
            try:
                recs, _ = segmod.scan_records(self.seg_root, meta, from_offset)
            except segmod.SegmentCorrupt as c:
                self.quarantine(meta["id"], c.reason)
                gaps.append({"segment": meta["id"], "reason": c.reason,
                             "resume_offset": c.resume_offset})
                scanned_through = max(scanned_through, meta["last_offset"])
                continue
            for rec in recs:
                if rec["offset"] >= end_offset:
                    continue  # frozen horizon safety net
                scanned_through = max(scanned_through, rec["offset"])
                if emit(rec):
                    complete = False
                    break
            if not complete:
                break

        if complete and open_snapshot:
            for rec in open_snapshot:
                if rec["offset"] < from_offset:
                    continue
                scanned_through = max(scanned_through, rec["offset"])
                if emit(rec):
                    complete = False
                    break

        return {
            "freeze_id": freeze_id,
            "end_offset": end_offset,
            "events": events,
            "gaps": gaps,
            "next_from_offset": None if complete else scanned_through + 1,
            "complete": complete,
        }

    # ------------------------------------------------------------------ #
    # queries                                                             #
    # ------------------------------------------------------------------ #

    def list_devices(self) -> List[dict]:
        with self._lock:
            out = []
            for dev_id, dev in sorted(self._devices.items()):
                out.append({
                    "device_id": dev_id,
                    "events": len(dev.entries),
                    "max_seq": dev.max_seq,
                    "max_device_ts": fmt_ts(dev.max_device_ts) if dev.max_device_ts else None,
                })
            return out

    def device_events(self, device_id: str, from_seq: Optional[int] = None,
                      from_offset: int = 0, limit: int = 100) -> dict:
        """Events of one device in business order (seq, then arrival offset)."""
        with self._lock:
            dev = self._devices.get(device_id)
            if dev is None:
                return {"device_id": device_id, "events": [], "gaps": [], "next": None}
            if from_seq is None:
                idx = 0
            else:
                idx = bisect.bisect_left(
                    dev.entries, (from_seq, from_offset), key=_entry_key)
            selected = dev.entries[idx: idx + limit]
            plan: List[tuple] = []
            gaps: List[dict] = []
            gap_segs = set()
            for e in selected:
                if e.seg_id == OPEN:
                    plan.append(("mem", self._open_records[e.pos]))
                    continue
                meta = self._seg_by_id[e.seg_id]
                if meta["status"] == "quarantined":
                    if e.seg_id not in gap_segs:
                        gap_segs.add(e.seg_id)
                        gaps.append({
                            "segment": e.seg_id,
                            "reason": meta.get("quarantine_reason", ""),
                            "resume_offset": meta["last_offset"] + 1,
                        })
                    continue
                plan.append(("seg", e.seg_id, e.pos, e.length))

        # I/O outside the lock; segment files are immutable once sealed.
        events: List[dict] = []
        handles = {}
        try:
            for item in plan:
                if item[0] == "mem":
                    events.append(item[1])
                    continue
                _, seg_id, pos, length = item
                fh = handles.get(seg_id)
                if fh is None:
                    fh = open(segmod.events_path(self.seg_root, seg_id), "rb")
                    handles[seg_id] = fh
                try:
                    events.append(segmod.read_record_at(
                        self.seg_root, seg_id, pos, length, fh=fh))
                except segmod.SegmentCorrupt as c:
                    meta = self._seg_by_id.get(seg_id)
                    resume = (meta["last_offset"] + 1) if meta else 0
                    self.quarantine(seg_id, c.reason)
                    gaps.append({"segment": seg_id, "reason": c.reason,
                                 "resume_offset": resume})
        finally:
            for fh in handles.values():
                fh.close()

        nxt = None
        if len(selected) == limit and selected:
            last = selected[-1]
            nxt = {"from_seq": last.seq, "from_offset": last.offset + 1}
        return {"device_id": device_id, "events": events, "gaps": gaps, "next": nxt}

    def list_segments(self) -> dict:
        with self._lock:
            return {
                "segments": [dict(m) for m in self.manifest["segments"]],
                "open": self._open_info(),
                "sealed_through": self.manifest["sealed_through"],
                "next_offset": self._next_offset,
            }

    def segment_events(self, seg_id: str, from_offset: int = 0,
                       limit: int = 500) -> dict:
        with self._lock:
            meta = self._seg_by_id.get(seg_id)
            if meta is None:
                raise NotFound(f"segment {seg_id} not found")
            meta = dict(meta)
        if meta["status"] == "quarantined":
            raise Quarantined(seg_id, meta["last_offset"] + 1,
                              meta.get("quarantine_reason", ""))
        try:
            recs, complete = segmod.scan_records(self.seg_root, meta, from_offset, limit)
        except segmod.SegmentCorrupt as c:
            self.quarantine(seg_id, c.reason)
            raise Quarantined(seg_id, c.resume_offset, c.reason)
        return {"segment": meta, "events": recs, "complete": complete}

    # ------------------------------------------------------------------ #
    # corruption handling                                                 #
    # ------------------------------------------------------------------ #

    def quarantine(self, seg_id: str, reason: str) -> None:
        with self._lock:
            meta = self._seg_by_id.get(seg_id)
            if meta is None or meta["status"] == "quarantined":
                return
            self._mark_quarantined(meta, reason)
            self._persist_manifest()
            log.warning("segment %s quarantined: %s (resume at offset %d)",
                        seg_id, reason, meta["last_offset"] + 1)

    @staticmethod
    def _mark_quarantined(meta: dict, reason: str) -> None:
        meta["status"] = "quarantined"
        meta["quarantine_reason"] = reason
        meta["quarantined_at"] = fmt_ts(utcnow())

    def rebuild_segment(self, seg_id: str) -> dict:
        """Rebuild a quarantined segment from retained WAL records.

        The records of one segment are not necessarily in the WAL file
        named by its first offset: a batch spanning several seal boundaries
        is appended to the file active at ingest time, and rotation only
        redirects *later* writes.  A file named by base B holds records
        with offset >= B, so collect the segment's range from every
        retained WAL file that may overlap it.
        """
        with self._lock:
            meta = self._seg_by_id.get(seg_id)
            if meta is None:
                raise NotFound(f"segment {seg_id} not found")
            if meta["status"] != "quarantined":
                return dict(meta)
            first, last = meta["first_offset"], meta["last_offset"]
            by_offset: Dict[int, dict] = {}
            for name in sorted(os.listdir(self.wal_dir)):
                if not name.endswith(".wal"):
                    continue
                if int(name[:-4]) > last:
                    continue  # file starts past the segment's range
                path = os.path.join(self.wal_dir, name)
                try:
                    payloads = walmod.read_all_payloads(path)
                except Exception as exc:
                    log.warning("rebuild %s: skipping unreadable WAL file %s: %s",
                                seg_id, name, exc)
                    continue
                for payload in payloads:
                    rec = json.loads(payload)
                    if first <= rec["offset"] <= last:
                        by_offset.setdefault(rec["offset"], rec)
            # keys of by_offset are a subset of [first..last], so a full
            # count means the range is covered contiguously
            if len(by_offset) != meta["count"] or \
                    len(by_offset) != last - first + 1:
                raise WalCoverageGone(seg_id, last + 1)
            records = [by_offset[o] for o in range(first, last + 1)]
            new_meta, index = segmod.write_segment(self.seg_root, seg_id, records)
            meta.clear()
            meta.update(new_meta)  # status=sealed, fresh sha256
            self._persist_manifest()
            # refresh device entries for this segment from the rebuilt index
            for dev_id, d in index["devices"].items():
                dev = self._devices.setdefault(dev_id, DeviceState())
                dev.entries = [e for e in dev.entries if e.seg_id != seg_id]
                for ie in d["entries"]:
                    bisect.insort(dev.entries, Entry(
                        ie["seq"], ie["offset"], seg_id, ie["pos"], ie["len"],
                        ie["event_id"], parse_ts(ie["device_ts"])), key=_entry_key)
                    dev.event_ids[ie["event_id"]] = ie["offset"]
                    dev.seqs.add(ie["seq"])
                self._refresh_device_extremes(dev)
            log.info("segment %s rebuilt from WAL (%d records)", seg_id, len(records))
            return dict(meta)

    # ------------------------------------------------------------------ #
    # stats / lifecycle                                                   #
    # ------------------------------------------------------------------ #

    def stats(self) -> dict:
        with self._lock:
            segs = self.manifest["segments"]
            return {
                "uptime_sec": round(time.monotonic() - self._started_at, 3),
                "next_offset": self._next_offset,
                "sealed_through": self.manifest["sealed_through"],
                "counters": dict(self._counters),
                "devices": len(self._devices),
                "segments": {
                    "total": len(segs),
                    "sealed": sum(1 for m in segs if m["status"] == "sealed"),
                    "quarantined": sum(1 for m in segs if m["status"] == "quarantined"),
                },
                "open": self._open_info(),
                "wal": {
                    "files": sorted(n for n in os.listdir(self.wal_dir)
                                    if n.endswith(".wal")),
                    "gaps": list(self._wal_gaps),
                },
                "freezes": len(self._freezes),
                "config": {
                    "segment_max_records": self.cfg.segment_max_records,
                    "segment_max_age_sec": self.cfg.segment_max_age_sec,
                    "late_threshold_sec": self.cfg.late_threshold_sec,
                    "wal_retain_segments": self.cfg.wal_retain_segments,
                    "fsync": self.cfg.fsync,
                },
            }

    def _open_info(self) -> dict:
        age = None
        if self._open_started is not None:
            age = round(time.monotonic() - self._open_started, 3)
        return {
            "count": len(self._open_records),
            "first_offset": self._open_first_offset,
            "age_sec": age,
        }

    def start_janitor(self) -> None:
        def loop():
            while not self._janitor_stop.wait(self.cfg.janitor_interval_sec):
                try:
                    with self._lock:
                        if (self._open_records and self._open_started is not None
                                and time.monotonic() - self._open_started
                                >= self.cfg.segment_max_age_sec):
                            self._seal_open()
                except Exception:
                    log.exception("janitor seal failed")

        self._janitor = threading.Thread(target=loop, name="seal-janitor", daemon=True)
        self._janitor.start()

    def close(self) -> None:
        self._janitor_stop.set()
        if self._janitor:
            self._janitor.join(timeout=5)
        with self._lock:
            if self._wal is not None:
                self._wal.close()

    # ------------------------------------------------------------------ #
    # persistence helpers                                                 #
    # ------------------------------------------------------------------ #

    def _persist_manifest(self) -> None:
        atomic_write_json(self._manifest_path, self.manifest)

    def _persist_freezes(self) -> None:
        atomic_write_json(self._freezes_path, {"freezes": self._freezes})
