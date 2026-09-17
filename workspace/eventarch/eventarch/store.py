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
from queue import Queue
from typing import Dict, List, Optional, Tuple

from . import segments as segmod
from . import wal as walmod
from .models import fmt_ts, new_flags, parse_ts, utcnow, validate_event
from .util import atomic_write_json, fsync_dir, load_json

log = logging.getLogger("eventarch.store")

OPEN = ""  # seg_id marker for entries still living in the open (unsealed) buffer

# Terminal repair-job statuses.
REPAIR_TERMINAL = ("succeeded", "failed")


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


class RepairTimeout(Exception):
    def __init__(self, job_id: str):
        super().__init__(f"repair job {job_id} did not finish in time")
        self.job_id = job_id


class _RetryRepair(Exception):
    """Internal: abort this repair attempt and replan/retry from scratch."""


class _RepairNoop(Exception):
    """Internal: segment is already sealed with identical content."""


class _RepairSuperseded(Exception):
    """Internal: segment was restored with *other* content; do not overwrite."""


class _RepairNotFound(Exception):
    """Internal: segment vanished between plan and commit."""


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
        self._repairs_path = os.path.join(self.state_dir, "repairs.json")

        self._lock = threading.RLock()
        # Notified on every repair-job terminal transition (used by wait_repair).
        self._repair_cv = threading.Condition()
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

        # ---- background repair jobs -------------------------------------
        # Every long-running archive repair (segment rebuild) runs as a job
        # on a bounded worker pool, doing all heavy I/O *outside* the global
        # lock.  Only plan/commit touch shared state, and they take the lock
        # briefly with optimistic version checks (meta["version"]).
        self._repairs: Dict[str, dict] = {}       # job id -> job state
        self._active_repairs: Dict[str, str] = {}  # seg_id -> job id (dedup)
        self._repair_q: "Queue[Optional[str]]" = Queue()
        self._repair_workers: List[threading.Thread] = []
        self._closing = False
        # Test/observability hook invoked OUTSIDE the lock at repair phases:
        #   hook(job_dict, phase)  phase in {"planned", "staged"}
        self._repair_phase_hook = None

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
        for meta in self.manifest["segments"]:
            meta.setdefault("version", 1)

        # 0. finish or roll back any interrupted background repair (crash
        #    between staging, directory swap and manifest commit), load the
        #    durable job journal, and remove directories that never reached
        #    the manifest (orphans / stale staging dirs).  Must run BEFORE
        #    verification so every decision is based on the reconciled files.
        self._recover_repairs()

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

        # 2. drop orphan directories: segments never committed to the
        #    manifest, and leftover repair staging/backup directories whose
        #    job was not journaled (or whose state is already resolved).
        for name in os.listdir(self.seg_root):
            full = os.path.join(self.seg_root, name)
            if not os.path.isdir(full):
                continue
            if name.startswith("seg-") and name not in self._seg_by_id:
                log.warning("removing orphan segment dir %s (never committed)", name)
                shutil.rmtree(full, ignore_errors=True)
            elif name.startswith(("stage-", "bak-")):
                log.warning("removing stale repair dir %s", name)
                shutil.rmtree(full, ignore_errors=True)
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

        # 5. resume background repairs: every non-terminal journaled job is
        #    requeued (its work is idempotent); terminal jobs are history.
        with self._lock:
            for jid, job in self._repairs.items():
                if job["status"] not in REPAIR_TERMINAL:
                    self._update_job(jid, status="queued", error=None,
                                     stage=None, set_attempt=1)
                    self._active_repairs[job["seg_id"]] = jid
                    self._repair_q.put(jid)
        self._start_repair_workers()

        log.info(
            "recovery complete: %d segments (%d quarantined), %d live WAL records, "
            "next_offset=%d, devices=%d, wal_gaps=%d, repairs_queued=%d",
            len(self.manifest["segments"]),
            sum(1 for m in self.manifest["segments"] if m["status"] == "quarantined"),
            len(live), self._next_offset, len(self._devices), len(gaps),
            sum(1 for j in self._repairs.values() if j["status"] == "queued"),
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
        backing a quarantined segment (needed for rebuild).  Segments with
        a repair job in flight are also pinned, so that log eviction can
        never remove the records a running job is about to stage.
        """
        sealed = sorted(
            (m for m in self.manifest["segments"] if m["status"] == "sealed"),
            key=lambda m: m["first_offset"])
        keep_from = 0
        if len(sealed) > self.cfg.wal_retain_segments:
            keep_from = sealed[-self.cfg.wal_retain_segments]["first_offset"]
        pinned = [m["first_offset"] for m in self.manifest["segments"]
                  if m["status"] == "quarantined" or m["id"] in self._active_repairs]
        if pinned:
            keep_from = min(keep_from, min(pinned))
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
                self.quarantine(meta["id"], c.reason,
                                expected_sha=meta.get("sha256"))
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
                    self.quarantine(seg_id, c.reason,
                                    expected_sha=meta.get("sha256") if meta else None)
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
            self.quarantine(seg_id, c.reason, expected_sha=meta.get("sha256"))
            raise Quarantined(seg_id, c.resume_offset, c.reason)
        return {"segment": meta, "events": recs, "complete": complete}

    # ------------------------------------------------------------------ #
    # corruption handling                                                 #
    # ------------------------------------------------------------------ #

    def quarantine(self, seg_id: str, reason: str,
                   expected_sha: Optional[str] = None) -> bool:
        """Mark a sealed segment quarantined (idempotent, version-checked).

        ``expected_sha`` is a stale-read guard: when given, quarantine is
        applied only if the segment still carries that sha256.  A repair
        job that atomically swapped in fresh bytes therefore wins the race
        against a reader holding an old file handle.  Returns True if the
        segment is quarantined (now or already) when this returns.
        """
        with self._lock:
            meta = self._seg_by_id.get(seg_id)
            if meta is None:
                return False
            if meta["status"] == "quarantined":
                return True
            if expected_sha is not None and meta.get("sha256") != expected_sha:
                log.info("not quarantining %s: sha256 changed "
                         "(repaired concurrently)", seg_id)
                return False
            self._mark_quarantined(meta, reason, bump_version=True)
            self._persist_manifest()
            log.warning("segment %s quarantined: %s (resume at offset %d)",
                        seg_id, reason, meta["last_offset"] + 1)
            return True

    @staticmethod
    def _mark_quarantined(meta: dict, reason: str, bump_version: bool = False) -> None:
        meta["status"] = "quarantined"
        meta["quarantine_reason"] = reason
        meta["quarantined_at"] = fmt_ts(utcnow())
        if bump_version:
            meta["version"] = meta.get("version", 1) + 1

    # ------------------------------------------------------------------ #
    # background repair jobs                                               #
    # ------------------------------------------------------------------ #
    #
    # A repair rebuilds one quarantined segment from retained WAL without
    # ever holding the global lock across the heavy I/O:
    #
    #   plan   (lock, brief): snapshot {id, range, count, version, sha}; the
    #          segment is also registered in _active_repairs, which pins its
    #          WAL coverage against the janitor/eviction.
    #   gather (no lock):  read every retained WAL file that may overlap the
    #          range; a torn tail (concurrent rotation) or any read anomaly
    #          -> retry the whole attempt.
    #   stage  (no lock):  write into a unique sibling directory stage-<jid>
    #          and verify its sha256; the LIVE file is never touched here.
    #   commit (lock, brief, optimistic CAS on meta["version"]):
    #          rename live -> bak-<jid>, stage -> live (atomic, so intact
    #          files are never overwritten/truncated), install the rebuilt
    #          index, bump/persist the manifest, then remove the backup.
    #          Any concurrent state change of the same segment makes the CAS
    #          fail -> the attempt rolls the directory names back and retries.
    #
    # Every transition is appended to the durable journal (repairs.json),
    # so a crash mid-repair is reconciled at startup (see _recover_repairs).

    def rebuild_segment(self, seg_id: str, timeout: Optional[float] = 60.0) -> dict:
        """Synchronous compatibility wrapper: enqueue a repair and block the
        *calling* thread until it finishes (foreground traffic stays live —
        the global lock is held only during brief plan/commit windows)."""
        job, _created = self.start_repair(seg_id)
        job = self.wait_repair(job["id"], timeout=timeout)
        if job["status"] == "succeeded":
            result = job.get("result")
            if result is not None:
                return result
            with self._lock:
                meta = self._seg_by_id.get(seg_id)
                if meta is not None:
                    return dict(meta)
                raise NotFound(f"segment {seg_id} not found")
        err = job.get("error") or {}
        if err.get("type") == "wal_coverage_gone":
            raise WalCoverageGone(seg_id, err.get("resume_offset", 0))
        raise WalCoverageGone(seg_id, err.get("resume_offset", 0))

    def start_repair(self, seg_id: str) -> Tuple[dict, bool]:
        """Enqueue a background repair for one segment.

        Returns (job, created).  ``created=False`` means an identical job was
        already queued/running (idempotent dedup; the same job is returned so
        concurrent callers never produce duplicate repair work).
        """
        with self._lock:
            meta = self._seg_by_id.get(seg_id)
            if meta is None:
                raise NotFound(f"segment {seg_id} not found")
            existing_id = self._active_repairs.get(seg_id)
            if existing_id is not None:
                return self._job_view(self._repairs[existing_id]), False
            if meta["status"] != "quarantined":
                # Nothing to repair: record a terminal no-op job (never
                # touches the healthy segment / its file).
                job = self._new_job(seg_id)
                self._finish_job_locked(
                    job, "succeeded",
                    result=dict(meta),
                    detail="segment already sealed; no repair needed",
                )
                return self._job_view(job), True

            job = self._new_job(seg_id)
            self._active_repairs[seg_id] = job["id"]
            self._update_job_locked(job["id"], status="queued", stage="queued")
        self._repair_q.put(job["id"])
        return self._job_view(job), True

    def wait_repair(self, job_id: str, timeout: Optional[float] = 60.0) -> dict:
        """Block until the job reaches a terminal state (no lock held)."""
        with self._repair_cv:
            ok = self._repair_cv.wait_for(
                lambda: job_id in self._repairs
                and self._repairs[job_id]["status"] in REPAIR_TERMINAL,
                timeout=timeout)
            job = self._repairs.get(job_id)
            if not ok or job is None:
                raise RepairTimeout(job_id)
            return self._job_view(job)

    def get_repair(self, job_id: str) -> dict:
        with self._lock:
            job = self._repairs.get(job_id)
            if job is None:
                raise NotFound(f"repair job {job_id} not found")
            return self._job_view(job)

    def list_repairs(self, limit: int = 100) -> List[dict]:
        with self._lock:
            jobs = sorted(self._repairs.values(),
                          key=lambda j: j["created_at"], reverse=True)
            return [self._job_view(j) for j in jobs[:max(1, limit)]]

    # ---- job state / journal ------------------------------------------ #

    def _new_job(self, seg_id: str) -> dict:
        job = {
            "id": f"job-{time.strftime('%Y%m%dT%H%M%S', time.gmtime())}-{uuid.uuid4().hex[:12]}",
            "seg_id": seg_id,
            "status": "new",
            "attempt": 0,
            "stage": None,
            "created_at": fmt_ts(utcnow()),
            "updated_at": None,
            "error": None,
            "result": None,
            "detail": None,
        }
        self._repairs[job["id"]] = job
        self._persist_repairs_locked()
        return job

    @staticmethod
    def _job_view(job: dict) -> dict:
        view = {k: job.get(k) for k in (
            "id", "seg_id", "status", "attempt", "stage", "created_at",
            "updated_at", "error", "detail")}
        if job.get("result") is not None:
            view["result"] = job["result"]
        return view

    def _update_job_locked(self, jid: str, **fields) -> None:
        job = self._repairs[jid]
        if "attempt" in fields:
            job["attempt"] = fields.pop("attempt")
        if "set_attempt" in fields:
            job["attempt"] = fields.pop("set_attempt")
        if fields.pop("inc_attempt", False):
            job["attempt"] += 1
        for k, v in fields.items():
            job[k] = v
        job["updated_at"] = fmt_ts(utcnow())
        self._persist_repairs_locked()

    # convenience wrapper used from worker threads (takes the lock)
    def _update_job(self, jid: str, **fields) -> None:
        with self._lock:
            self._update_job_locked(jid, **fields)

    def _finish_job_locked(self, job: dict, status: str, result=None,
                           error=None, detail=None) -> None:
        self._active_repairs.pop(job["seg_id"], None)
        job["status"] = status
        job["stage"] = status
        job["error"] = error
        if result is not None:
            job["result"] = result
        if detail is not None:
            job["detail"] = detail
        job["updated_at"] = fmt_ts(utcnow())
        self._persist_repairs_locked()
        # Separate lock from the store lock: notify waiters without
        # re-ordering locking; a waiter's predicate reads plain dicts under
        # the condition's own lock after this memory-visible transition.
        with self._repair_cv:
            self._repair_cv.notify_all()

    def _persist_repairs_locked(self) -> None:
        # Bound journal growth: keep all non-terminal jobs plus the newest N
        # terminal ones; active jobs are never pruned.
        active = [j for j in self._repairs.values()
                  if j["status"] not in REPAIR_TERMINAL]
        done = sorted(
            (j for j in self._repairs.values()
             if j["status"] in REPAIR_TERMINAL),
            key=lambda j: j["updated_at"] or "", reverse=True)
        keep = active + done[:max(0, self.cfg.repair_history)]
        keep.sort(key=lambda j: j["created_at"])
        # self._repairs itself keeps in-memory history as written so far;
        # only the durable file is truncated.
        atomic_write_json(self._repairs_path, {"repairs": keep})

    # ---- worker pool --------------------------------------------------- #

    def _start_repair_workers(self) -> None:
        n = max(1, self.cfg.repair_workers)
        for i in range(n):
            t = threading.Thread(target=self._repair_worker_loop,
                                 name=f"repair-{i}", daemon=True)
            t.start()
            self._repair_workers.append(t)

    def _repair_worker_loop(self) -> None:
        while True:
            jid = self._repair_q.get()
            if jid is None:
                self._repair_q.task_done()
                return
            try:
                self._run_repair(jid)
            except Exception:
                log.exception("repair worker crashed running %s", jid)
                with self._lock:
                    job = self._repairs.get(jid)
                    if job is not None and job["status"] not in REPAIR_TERMINAL:
                        self._finish_job_locked(
                            job, "failed",
                            error={"type": "internal", "reason": "worker exception"})
            finally:
                self._repair_q.task_done()

    # ---- per-job state machine ---------------------------------------- #

    def _run_repair(self, jid: str) -> None:
        with self._lock:
            job = self._repairs.get(jid)
            if job is None or job["status"] in REPAIR_TERMINAL:
                return
            self._update_job_locked(jid, status="running", stage="planning")
        max_attempts = max(1, self.cfg.repair_max_attempts)

        while True:
            if self._janitor_stop.is_set():
                if self._park_for_shutdown(jid):
                    return
                # job already reached a terminal state; nothing more to do
            with self._lock:
                j = self._repairs.get(jid)
                if j is None:
                    return
                if j["attempt"] >= max_attempts:
                    self._finish_job_locked(j, "failed", error=j.get("error") or {
                        "type": "max_attempts",
                        "reason": "repair attempts exhausted"})
                    return
                self._update_job_locked(jid, inc_attempt=True)
                meta = self._seg_by_id.get(j["seg_id"])
                if meta is None:
                    self._finish_job_locked(j, "failed", error={
                        "type": "not_found", "reason": "segment vanished"})
                    return
                plan = {
                    "seg_id": meta["id"],
                    "first": meta["first_offset"],
                    "last": meta["last_offset"],
                    "count": meta["count"],
                    "version": meta.get("version", 1),
                    "old_sha": meta.get("sha256"),
                }
                attempt = j["attempt"]
            self._fire_hook(j, "planned")

            try:
                # Heavy I/O outside the lock.
                with self._lock:
                    self._update_job_locked(jid, stage="gathering_wal")
                records = self._gather_wal_records(jid, plan)

                stage_root = os.path.join(self.seg_root, f"stage-{jid}-{attempt}")
                stage_seg = os.path.join(stage_root, plan["seg_id"])
                bak_dir = os.path.join(self.seg_root, f"bak-{jid}-{attempt}")
                shutil.rmtree(stage_root, ignore_errors=True)
                with self._lock:
                    self._update_job_locked(jid, stage="staging")
                new_meta, index = segmod.write_segment_dir(
                    stage_seg, plan["seg_id"], records)
                # Self-check before publishing: identity must match the plan.
                if (new_meta["first_offset"] != plan["first"]
                        or new_meta["last_offset"] != plan["last"]
                        or new_meta["count"] != plan["count"]):
                    raise _RetryRepair("staged segment identity mismatch")
                ok, why = segmod.verify_file(
                    segmod.events_path(stage_root, plan["seg_id"]),
                    new_meta["sha256"])
                if not ok:
                    raise _RetryRepair(f"staged segment verification failed: {why}")
                self._fire_hook(j, "staged")

                if self._janitor_stop.is_set():
                    self._rollback_attempt(jid, attempt)
                    self._park_for_shutdown(jid)
                    return
                with self._lock:
                    self._update_job_locked(jid, stage="committing")
                    self._commit_repair(j, plan, new_meta, index,
                                        stage_root, bak_dir)
                return  # terminal transition happened inside _commit_repair

            except _RepairNoop:
                with self._lock:
                    j = self._repairs.get(jid)
                    if j is not None and j["status"] not in REPAIR_TERMINAL:
                        self._finish_job_locked(j, "succeeded",
                                                result=None,
                                                detail="segment already sealed; "
                                                       "no repair needed")
                return
            except _RepairSuperseded:
                with self._lock:
                    j = self._repairs.get(jid)
                    if j is not None and j["status"] not in REPAIR_TERMINAL:
                        self._finish_job_locked(j, "succeeded",
                                                result=None,
                                                detail="segment was repaired with "
                                                       "other content concurrently")
                return
            except _RepairNotFound:
                with self._lock:
                    j = self._repairs.get(jid)
                    if j is not None and j["status"] not in REPAIR_TERMINAL:
                        self._finish_job_locked(j, "failed", error={
                            "type": "not_found",
                            "reason": "segment vanished during repair"})
                return
            except WalCoverageGone as exc:
                # Definitive: WAL no longer covers the segment; keep it
                # quarantined and surface the resume position (no retries).
                with self._lock:
                    j = self._repairs.get(jid)
                    if j is not None and j["status"] not in REPAIR_TERMINAL:
                        self._finish_job_locked(j, "failed", error={
                            "type": "wal_coverage_gone",
                            "reason": "retained WAL does not cover the segment",
                            "resume_offset": exc.resume_offset})
                return
            except (_RetryRepair, OSError, ValueError, KeyError,
                    segmod.SegmentCorrupt, walmod.TornTail,
                    walmod.FrameCorrupt) as exc:
                # Transient: concurrent WAL rotation/torn read, directory
                # race, or a version conflict detected at commit.  Roll back
                # filesystem leftovers and replan from current state.
                self._rollback_attempt(jid, attempt)
                with self._lock:
                    j = self._repairs.get(jid)
                    if j is None or j["status"] in REPAIR_TERMINAL:
                        return
                    if j["attempt"] >= max_attempts:
                        self._finish_job_locked(j, "failed", error={
                            "type": "conflict",
                            "reason": f"repair gave up after {j['attempt']} "
                                      f"attempts: {exc}"})
                        return
                    self._update_job_locked(jid, status="running",
                                            stage="retrying",
                                            error={"type": "retry",
                                                   "reason": str(exc)})
                log.info("repair %s attempt %d failed (%s); retrying",
                         jid, attempt, exc)
                self._sleep_backoff(attempt)
                # loop: replan under the lock (fresh version/state snapshot)

    def _sleep_backoff(self, attempt: int) -> None:
        self._janitor_stop.wait(self.cfg.repair_retry_backoff_sec * attempt)

    def _park_for_shutdown(self, jid: str) -> bool:
        """Return a non-terminal job to the durable queue during shutdown.

        Jobs parked this way are requeued by the next process (the work is
        idempotent); marking them terminal would drop acknowledged repair
        intent.  Returns True if this thread should stop running the job.
        """
        with self._lock:
            j = self._repairs.get(jid)
            if j is None or j["status"] in REPAIR_TERMINAL:
                return False
            j["status"] = "queued"
            j["stage"] = "queued"
            j["error"] = None
            j["updated_at"] = fmt_ts(utcnow())
            self._persist_repairs_locked()
            with self._repair_cv:
                self._repair_cv.notify_all()
        return True

    def _fire_hook(self, jid_or_job: object, phase: str) -> None:
        hook = self._repair_phase_hook
        if hook is None:
            return
        if isinstance(jid_or_job, str):
            with self._lock:
                job = self._repairs.get(jid_or_job)
                view = self._job_view(job) if job else None
        else:
            view = jid_or_job
        if view is not None:
            try:
                hook(view, phase)
            except Exception:
                log.exception("repair phase hook raised")

    def _gather_wal_records(self, jid: str, plan: dict) -> List[dict]:
        """Collect the segment's exact offset range from retained WAL files.

        Mirrors rebuild_segment's range rule (records may live in files not
        named after this segment's first offset).  All file I/O happens
        outside the global lock; any torn/corrupt frame (e.g. reading the
        active WAL across a concurrent rotate) raises _RetryRepair.
        """
        first, last = plan["first"], plan["last"]
        by_offset: Dict[int, dict] = {}
        for name in sorted(os.listdir(self.wal_dir)):
            if not name.endswith(".wal"):
                continue
            try:
                base = int(name[:-4])
            except ValueError:
                continue
            if base > last:
                continue  # file starts past the segment's range
            path = os.path.join(self.wal_dir, name)
            try:
                payloads = walmod.read_all_payloads(path)
            except (OSError, walmod.TornTail, walmod.FrameCorrupt) as exc:
                # Might be the live WAL file being rotated concurrently;
                # the whole attempt is idempotent, so retry.
                raise _RetryRepair(f"WAL file {name} unreadable: {exc}")
            for payload in payloads:
                try:
                    rec = json.loads(payload)
                except ValueError as exc:
                    raise _RetryRepair(f"WAL file {name} undecodable: {exc}")
                if first <= rec["offset"] <= last:
                    by_offset.setdefault(rec["offset"], rec)

        if len(by_offset) != plan["count"] or len(by_offset) != last - first + 1:
            raise WalCoverageGone(plan["seg_id"], last + 1)
        return [by_offset[o] for o in range(first, last + 1)]

    def _commit_repair(self, job: dict, plan: dict, new_meta: dict, index: dict,
                       stage_root: str, bak_dir: str) -> None:
        """Publish a staged rebuild (caller HOLDS the lock).

        Optimistic CAS on the planned version: any intervening state change
        of the same segment (rebuild, re-quarantine) aborts this attempt so
        stale bytes can never win.  Directory swap is two renames and thus
        never truncates/overwrites an intact live file in place.
        """
        seg_id = plan["seg_id"]
        stage_seg = os.path.join(stage_root, seg_id)
        meta = self._seg_by_id.get(seg_id)
        if meta is None:
            raise _RepairNotFound()
        if meta.get("version", 1) != plan["version"]:
            if meta["status"] == "sealed" and meta.get("sha256") == new_meta["sha256"]:
                raise _RepairNoop()
            if meta["status"] == "sealed":
                raise _RepairSuperseded()
            # Re-quarantined (new sha256/version): conflict -> retry/replan.
            raise _RetryRepair(
                f"segment version changed {plan['version']} -> {meta.get('version')}")

        live_dir = segmod.seg_dir(self.seg_root, seg_id)
        swapped = False
        try:
            # 1. atomic swap: live aside, staged into place.
            if os.path.exists(bak_dir):
                shutil.rmtree(bak_dir, ignore_errors=True)
            os.rename(live_dir, bak_dir)
            try:
                os.rename(stage_seg, live_dir)
            except OSError:
                # Undo the first rename so the segment is never missing;
                # the attempt is then retried from scratch.
                if not os.path.exists(live_dir) and os.path.exists(bak_dir):
                    os.rename(bak_dir, live_dir)
                raise
            swapped = True
            fsync_dir(self.seg_root)
            shutil.rmtree(stage_root, ignore_errors=True)

            # 2. commit point: manifest now points at the rebuilt bytes.
            preserved_created = meta.get("created_at")
            meta.clear()
            meta.update(new_meta)
            if preserved_created is not None:
                meta["created_at"] = preserved_created
            meta["version"] = plan["version"] + 1
            try:
                self._persist_manifest()
            except OSError:
                # Swap landed but the commit is not durable.  Reverse the
                # swap in-process; startup reconciliation is the backstop if
                # this process dies during the reversal.
                if os.path.isdir(bak_dir):
                    shutil.rmtree(live_dir, ignore_errors=True)
                    os.rename(bak_dir, live_dir)
                    fsync_dir(self.seg_root)
                    swapped = False
                raise

            # 3. swap the in-memory device index for this segment.
            self._install_segment_index(seg_id, index)

            # 4. remove the quarantined backup (its bytes were corrupt).
            shutil.rmtree(bak_dir, ignore_errors=True)
        except OSError as exc:
            if swapped and os.path.isdir(bak_dir):
                # Defensive: still carrying the backup -> restore old live.
                try:
                    shutil.rmtree(live_dir, ignore_errors=True)
                    os.rename(bak_dir, live_dir)
                    fsync_dir(self.seg_root)
                except OSError:
                    pass
            # Filesystem state was rolled back (or reconciled on restart);
            # retry the whole attempt.
            raise _RetryRepair(f"commit swap failed: {exc}")

        result = dict(meta)
        self._finish_job_locked(job, "succeeded", result=result,
                                detail="rebuilt from retained WAL")
        log.info("segment %s rebuilt by %s (version -> %d, %d records)",
                 seg_id, job["id"], meta["version"], meta["count"])

    def _install_segment_index(self, seg_id: str, index: dict) -> None:
        """Replace device entries belonging to seg_id from a rebuilt index."""
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

    def _rollback_attempt(self, jid: str, attempt: int) -> None:
        """Best-effort removal of a failed attempt's stage/bak directories."""
        for d in (os.path.join(self.seg_root, f"stage-{jid}-{attempt}"),
                  os.path.join(self.seg_root, f"bak-{jid}-{attempt}")):
            try:
                if os.path.isdir(d):
                    shutil.rmtree(d, ignore_errors=True)
            except OSError:
                log.warning("could not clean up repair dir %s", d)
        # If a crash (not an in-process exception) leaves a swap half-done,
        # startup reconciliation is the authority — see _recover_repairs().

    # ---- crash recovery ------------------------------------------------ #

    def _recover_repairs(self) -> None:
        """Reconcile repair directories and the durable job journal.

        On-disk protocol per attempt (directories live next to segments):
          stage-<jid>-<n>/<seg-id>/   freshly written candidate
          seg-<off>/                  live (committed manifest points here)
          bak-<jid>-<n>/<seg-id>/     previous live moved aside; it exists
                                      only between the two renames and the
                                      manifest commit.

        Crash windows:
          * before renames: stage-* present, live intact       -> drop stage
          * live->bak only (stage->live missed): bak present,
            live absent, stage present                          -> decide by
            job status: commit (succeeded) or roll back (else)
          * both renames, manifest missed: bak present, live is
            the rebuilt bytes, stage gone                      -> adopt when
            it verifies (succeeded) else restore bak
          * manifest committed (commit point), bak removal missed:
            bak present, live verifies                         -> drop bak
        """
        jobs: Dict[str, dict] = {}
        if os.path.exists(self._repairs_path):
            try:
                for j in load_json(self._repairs_path).get("repairs", []):
                    jobs[j["id"]] = j
            except Exception as exc:
                log.error("cannot read repair journal (%s); starting empty", exc)

        # jid -> directory (take one even if several attempts remain).
        # Names are stage-<jid>-<attempt> / bak-<jid>-<attempt> and jid
        # itself contains hyphens ("job-<ts>-<hex>"), so strip the prefix
        # and drop the trailing attempt number.
        def parse(name: str, prefix: str) -> str:
            return name[len(prefix):].rsplit("-", 1)[0]

        stage_map: Dict[str, str] = {}
        bak_map: Dict[str, str] = {}
        for name in os.listdir(self.seg_root):
            full = os.path.join(self.seg_root, name)
            if not os.path.isdir(full):
                continue
            if name.startswith("stage-"):
                stage_map.setdefault(parse(name, "stage-"), full)
            elif name.startswith("bak-"):
                bak_map.setdefault(parse(name, "bak-"), full)

        manifest_changed = False
        for jid, job in jobs.items():
            stage_root = stage_map.pop(jid, None)
            bak_root = bak_map.pop(jid, None)
            seg_id = job["seg_id"]
            live_dir = segmod.seg_dir(self.seg_root, seg_id)
            stage_seg = os.path.join(stage_root, seg_id) if stage_root else None
            bak_seg = os.path.join(bak_root, seg_id) if bak_root else None
            succeeded = job.get("status") == "succeeded"
            new_sha = (job.get("result") or {}).get("sha256")
            try:
                # Case 1: live moved aside (and maybe stage moved in).
                if bak_seg is not None and os.path.isdir(bak_seg):
                    live_is_new = (
                        os.path.isdir(live_dir) and new_sha is not None
                        and segmod.verify_file(
                            segmod.events_path(self.seg_root, seg_id),
                            new_sha)[0])
                    if succeeded and live_is_new:
                        # Both renames landed; manifest commit is checked below.
                        shutil.rmtree(bak_root, ignore_errors=True)
                        bak_seg = None
                    elif succeeded and stage_seg is not None and os.path.isdir(stage_seg):
                        # live->bak done, stage->live missed: finish the swap.
                        shutil.rmtree(live_dir, ignore_errors=True)
                        os.rename(stage_seg, live_dir)
                        shutil.rmtree(bak_root, ignore_errors=True)
                        shutil.rmtree(stage_root, ignore_errors=True)
                        stage_seg = bak_seg = None
                        fsync_dir(self.seg_root)
                        live_is_new = True
                    else:
                        # Non-terminal/failed job OR unverifiable candidate:
                        # restore the pre-repair bytes.
                        shutil.rmtree(live_dir, ignore_errors=True)
                        os.rename(bak_seg, live_dir)
                        shutil.rmtree(bak_root, ignore_errors=True)
                        fsync_dir(self.seg_root)
                        log.warning("repair %s rolled back at startup", jid)
                        bak_seg = None
                    # Align the manifest with adopted bytes when the swap won.
                    if succeeded and live_is_new:
                        meta = self._seg_by_id.get(seg_id)
                        result = job.get("result") or {}
                        if meta is not None and meta.get("sha256") != result.get("sha256"):
                            preserved = meta.get("created_at")
                            meta.clear()
                            meta.update(result)
                            if preserved is not None:
                                meta["created_at"] = preserved
                            meta["version"] = max(meta.get("version", 1),
                                                  result.get("version", 1))
                            manifest_changed = True

                # Case 2: staged but never moved -> adopt only for a
                # journaled success, otherwise discard.
                if stage_seg is not None and os.path.isdir(stage_seg):
                    if succeeded and new_sha is not None and segmod.verify_file(
                            segmod.events_path(stage_root, seg_id), new_sha)[0] \
                            and not os.path.isdir(live_dir):
                        os.rename(stage_seg, live_dir)
                        fsync_dir(self.seg_root)
                        meta = self._seg_by_id.get(seg_id)
                        result = job.get("result") or {}
                        if meta is not None:
                            preserved = meta.get("created_at")
                            meta.clear()
                            meta.update(result)
                            if preserved is not None:
                                meta["created_at"] = preserved
                            manifest_changed = True
                    shutil.rmtree(stage_root, ignore_errors=True)

                # A non-terminal job survives the crash: run it again.
                if job.get("status") not in REPAIR_TERMINAL:
                    job["status"] = "queued"
                    job["stage"] = "queued"
                    job["error"] = None
                    job["attempt"] = 0
                    job["updated_at"] = fmt_ts(utcnow())
            except OSError as exc:
                log.error("repair reconciliation error for %s: %s", jid, exc)
                if job.get("status") not in REPAIR_TERMINAL:
                    job["status"] = "queued"
                    job["updated_at"] = fmt_ts(utcnow())
            job["updated_at"] = fmt_ts(utcnow())

        # Unclaimed stage/bak directories (no matching journaled job) are
        # removed by the generic orphan sweep in open().
        self._repairs = jobs
        if manifest_changed:
            self._persist_manifest()
        self._persist_repairs_locked()

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
                "repairs": {
                    "active": len(self._active_repairs),
                    "queued": sum(1 for j in self._repairs.values()
                                  if j["status"] in ("new", "queued", "running")),
                    "succeeded": sum(1 for j in self._repairs.values()
                                     if j["status"] == "succeeded"),
                    "failed": sum(1 for j in self._repairs.values()
                                  if j["status"] == "failed"),
                },
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
        # Drain repair workers: queued/in-flight jobs remain journaled and
        # are requeued at the next startup.
        with self._lock:
            self._closing = True
        for _ in self._repair_workers:
            self._repair_q.put(None)
        for t in self._repair_workers:
            t.join(timeout=5)
        with self._lock:
            if self._wal is not None:
                self._wal.close()
        # Wake any synchronous waiters whose job is not going to finish now.
        with self._repair_cv:
            self._repair_cv.notify_all()

    # ------------------------------------------------------------------ #
    # persistence helpers                                                 #
    # ------------------------------------------------------------------ #

    def _persist_manifest(self) -> None:
        atomic_write_json(self._manifest_path, self.manifest)

    def _persist_freezes(self) -> None:
        atomic_write_json(self._freezes_path, {"freezes": self._freezes})
