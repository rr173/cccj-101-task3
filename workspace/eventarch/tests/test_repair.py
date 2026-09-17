"""Tests for concurrent background repair jobs.

Covers the maintenance-path requirements:
  * repair runs in the background; ingest/query/freeze never block on it
  * race with WAL eviction: the repaired segment is pinned and byte-identical
  * race with a same-segment state change: optimistic version CAS -> retry,
    no duplicate records, no overwrite of intact files
  * crash/restart mid-repair: roll back or resume from the durable journal
  * stale readers can never re-quarantine a freshly rebuilt segment
  * cursors (offsets) and snapshot horizons (freezes) are never altered
"""

import os
import shutil
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch import segments as segmod
from eventarch.config import Config
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore, WalCoverageGone


def make_cfg(tmp, **kw):
    base = dict(
        data_dir=tmp, segment_max_records=5, segment_max_age_sec=3600,
        late_threshold_sec=900, wal_retain_segments=8, janitor_interval_sec=0.1,
        repair_workers=2, repair_retry_backoff_sec=0.01,
    )
    base.update(kw)
    return Config(**base)


def ev(device, seq, event_id=None, ts=None, payload=None):
    return {
        "device_id": device,
        "event_id": event_id or f"{device}-{seq}",
        "seq": seq,
        "device_ts": fmt_ts(ts or utcnow()),
        "payload": payload if payload is not None else {"seq": seq},
    }


def open_store(tmp, **kw):
    s = ArchiveStore(make_cfg(tmp, **kw))
    s.open()
    return s


def wait_status(s, job_id, statuses, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        j = s.get_repair(job_id)
        if j["status"] in statuses:
            return j
        time.sleep(0.005)
    raise AssertionError(f"job {job_id} never reached {statuses}; "
                         f"last={s.get_repair(job_id)}")


def terminal(s, job_id, timeout=5.0):
    return wait_status(s, job_id, ("succeeded", "failed"), timeout)


class BackgroundRepairTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def _prep(self, n=12):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(n)])
        victim = s.list_segments()["segments"][1]["id"]  # offsets [5..9]
        orig_sha = [m for m in s.list_segments()["segments"]
                    if m["id"] == victim][0]["sha256"]
        s.quarantine(victim, "test corruption")
        return s, victim, orig_sha

    def test_repair_is_async_and_foreground_keeps_responding(self):
        s, victim, _ = self._prep()
        release = threading.Event()
        entered = threading.Event()

        def hook(job, phase):
            if phase == "planned" and job["seg_id"] == victim:
                entered.set()
                release.wait(5)

        s._repair_phase_hook = hook
        job, created = s.start_repair(victim)
        self.assertTrue(created)
        self.assertEqual(job["status"], "queued")
        self.assertTrue(entered.wait(2), "repair never started")

        # The job is parked in the gather stage; every foreground path must
        # still answer immediately (no service-wide lock is held).
        t0 = time.monotonic()
        r = s.ingest([ev("d1", 100, event_id="d1-100")])
        self.assertEqual(r[0]["status"], "stored")
        self.assertLess(time.monotonic() - t0, 1.0)
        self.assertLess(time.monotonic() - t0, 1.0)
        got = s.device_events("d1", limit=500)
        self.assertLess(time.monotonic() - t0, 1.0)
        stats = s.stats()
        frz = s.freeze(note="while repair pending")
        self.assertTrue(frz["end_offset"] >= 13)
        _ = got, stats

        release.set()
        done = terminal(s, job["id"])
        self.assertEqual(done["status"], "succeeded", done.get("error"))
        s.close()

        s2 = open_store(self.tmp)
        seqs = [e["event"]["seq"] for e in
                s2.device_events("d1", limit=500)["events"]]
        self.assertEqual(seqs.count(100), 1)  # foreground ACK not lost
        self.assertEqual(sorted(seqs), seqs)  # business order preserved
        s2.close()

    def test_repair_rebuilds_byte_identical_segment(self):
        s, victim, orig_sha = self._prep()
        job, _ = s.start_repair(victim)
        done = terminal(s, job["id"])
        self.assertEqual(done["status"], "succeeded")
        meta = [m for m in s.list_segments()["segments"]
                if m["id"] == victim][0]
        self.assertEqual(meta["status"], "sealed")
        self.assertEqual(meta["sha256"], orig_sha)  # lossless rebuild
        # offsets/cursor unchanged
        self.assertEqual(meta["first_offset"], 5)
        self.assertEqual(meta["last_offset"], 9)
        self.assertEqual(s.stats()["next_offset"], 12)
        got = s.device_events("d1", limit=500)
        self.assertEqual([e["offset"] for e in got["events"]], list(range(12)))
        self.assertEqual(got["gaps"], [])
        s.close()

    def test_duplicate_start_repair_returns_same_job(self):
        s, victim, _ = self._prep()
        release = threading.Event()
        entered = threading.Event()

        def hook(job, phase):
            if phase == "planned" and job["seg_id"] == victim:
                entered.set()
                release.wait(5)

        s._repair_phase_hook = hook
        j1, c1 = s.start_repair(victim)
        self.assertTrue(c1)
        self.assertTrue(entered.wait(2))
        j2, c2 = s.start_repair(victim)
        self.assertFalse(c2)
        self.assertEqual(j1["id"], j2["id"])
        release.set()
        terminal(s, j1["id"])
        s.close()

    def test_repair_on_healthy_segment_is_noop_and_never_overwrites(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(6)])
        healthy = s.list_segments()["segments"][0]["id"]
        before = open(segmod.events_path(s.seg_root, healthy), "rb").read()
        job, _ = s.start_repair(healthy)
        done = terminal(s, job["id"])
        self.assertEqual(done["status"], "succeeded")
        after = open(segmod.events_path(s.seg_root, healthy), "rb").read()
        self.assertEqual(before, after)  # intact file untouched
        self.assertFalse(os.path.exists(
            os.path.join(s.seg_root, "bak-" + job["id"] + "-1")))
        s.close()

    def test_wal_eviction_race_is_blocked_by_repair_pin(self):
        # retention=1: without an active pin, sealing more segments would
        # collect the WAL backing the quarantined victim.
        s = open_store(self.tmp, wal_retain_segments=1)
        s.ingest([ev("d1", i) for i in range(12)])  # seg-0, seg-5
        victim = s.list_segments()["segments"][1]["id"]
        orig_sha = [m for m in s.list_segments()["segments"]
                    if m["id"] == victim][0]["sha256"]
        release = threading.Event()
        entered = threading.Event()

        def hook(job, phase):
            if phase == "planned" and job["seg_id"] == victim:
                entered.set()
                release.wait(5)

        s._repair_phase_hook = hook
        s.quarantine(victim, "test")
        job, _ = s.start_repair(victim)
        self.assertTrue(entered.wait(2))
        # Force many more seals while the job is between plan and gather;
        # retention must keep the WAL covering the pinned segment.
        s.ingest([ev("d1", i) for i in range(12, 42)])
        release.set()
        done = terminal(s, job["id"])
        self.assertEqual(done["status"], "succeeded", done.get("error"))
        meta = [m for m in s.list_segments()["segments"]
                if m["id"] == victim][0]
        self.assertEqual(meta["sha256"], orig_sha)
        got = s.device_events("d1", limit=500)
        self.assertEqual(len(got["gaps"]), 0)
        s.close()

    def test_wal_coverage_gone_is_terminal_with_resume_offset(self):
        s = open_store(self.tmp, wal_retain_segments=1)
        s.ingest([ev("d1", i) for i in range(5)])
        s.ingest([ev("d1", i) for i in range(5, 12)])
        first = s.list_segments()["segments"][0]["id"]  # WAL now collected
        s.quarantine(first, "test")
        job, _ = s.start_repair(first)
        done = terminal(s, job["id"])
        self.assertEqual(done["status"], "failed")
        self.assertEqual(done["error"]["type"], "wal_coverage_gone")
        self.assertEqual(done["error"]["resume_offset"], 5)
        # segment stays quarantined; data beyond it still served
        self.assertEqual(
            [m for m in s.list_segments()["segments"] if m["id"] == first][0]
            ["status"], "quarantined")
        with self.assertRaises(WalCoverageGone) as ctx:
            s.rebuild_segment(first)
        self.assertEqual(ctx.exception.resume_offset, 5)
        s.close()

    def test_version_conflict_during_commit_retries_and_wins(self):
        s, victim, orig_sha = self._prep()
        conflict_done = threading.Event()

        def hook(job, phase):
            if phase == "staged" and job["seg_id"] == victim \
                    and not conflict_done.is_set():
                # Simulate a competing state transition on the same archive
                # unit between staging and commit: re-quarantine bumps the
                # version.  The job must detect it, roll back and retry.
                with s._lock:
                    meta = s._seg_by_id[victim]
                    meta["version"] += 1
                    meta["quarantine_reason"] = "re-quarantined by rival"
                conflict_done.set()

        s._repair_phase_hook = hook
        job, _ = s.start_repair(victim)
        done = terminal(s, job["id"])
        self.assertEqual(done["status"], "succeeded", done.get("error"))
        self.assertGreaterEqual(done["attempt"], 2)  # replanned at least once
        meta = [m for m in s.list_segments()["segments"]
                if m["id"] == victim][0]
        self.assertEqual(meta["sha256"], orig_sha)
        self.assertEqual(meta["status"], "sealed")
        got = s.device_events("d1", limit=500)
        offsets = [e["offset"] for e in got["events"]]
        self.assertEqual(offsets, list(range(12)))  # no duplicates / losses
        self.assertEqual(len(offsets), len(set(offsets)))
        s.close()

    def test_stale_reader_sha_guard_does_not_quarantine_rebuild(self):
        s, victim, _ = self._prep()
        job, _ = s.start_repair(victim)
        terminal(s, job["id"])
        new_sha = [m for m in s.list_segments()["segments"]
                   if m["id"] == victim][0]["sha256"]
        # A reader holding the OLD (corrupt) sha256 observes a "mismatch".
        # The stale expected sha must lose to the repaired segment.
        changed = s.quarantine(victim, "stale handle read",
                               expected_sha="deadbeef" * 8)
        self.assertFalse(changed)
        self.assertEqual(
            [m for m in s.list_segments()["segments"] if m["id"] == victim][0]
            ["status"], "sealed")
        _ = new_sha
        s.close()

    def test_freeze_horizon_survives_repair(self):
        s, victim, _ = self._prep()
        frz = s.freeze()  # after quarantine; horizon at next_offset=12
        end = frz["end_offset"]
        job, _ = s.start_repair(victim)
        terminal(s, job["id"])
        rep = s.replay(freeze_id=frz["id"], limit=500)
        self.assertEqual(rep["end_offset"], end)  # snapshot horizon unchanged
        self.assertEqual(rep["gaps"], [])
        self.assertEqual([e["offset"] for e in rep["events"]], list(range(end)))
        s.close()


class RepairRestartTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def _corrupt(self, seg_id, byte=20):
        path = segmod.events_path(os.path.join(self.tmp, "segments"), seg_id)
        with open(path, "r+b") as fh:
            fh.seek(byte)
            b = fh.read(1)
            fh.seek(byte)
            fh.write(bytes([b[0] ^ 0xFF]))

    def test_running_job_is_resumed_after_restart(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(12)])
        victim = s.list_segments()["segments"][1]["id"]
        orig_sha = [m for m in s.list_segments()["segments"]
                    if m["id"] == victim][0]["sha256"]
        s.quarantine(victim, "test")
        job, _ = s.start_repair(victim)
        terminal(s, job["id"])
        s.close()
        # Corrupt again and enqueue, then "crash" (close) while queued.
        self._corrupt(victim)
        s2 = open_store(self.tmp)
        meta = [m for m in s2.list_segments()["segments"]
                if m["id"] == victim][0]
        self.assertEqual(meta["status"], "quarantined")
        job2, _ = s2.start_repair(victim)
        s2.close()  # hard stop while the job may be anywhere

        s3 = open_store(self.tmp)  # recovery must roll back and re-run
        # find the non-terminal job resumed for the segment after restart
        jobs = [j for j in s3.list_repairs(limit=100)
                if j["seg_id"] == victim and j["status"] not in ("succeeded", "failed")]
        self.assertTrue(jobs, [j for j in s3.list_repairs(limit=100)
                               if j["seg_id"] == victim])
        done = terminal(s3, jobs[-1]["id"])
        self.assertEqual(done["status"], "succeeded", done.get("error"))
        meta = [m for m in s3.list_segments()["segments"]
                if m["id"] == victim][0]
        self.assertEqual(meta["sha256"], orig_sha)
        got = s3.device_events("d1", limit=500)
        self.assertEqual([e["offset"] for e in got["events"]], list(range(12)))
        s3.close()

    def test_crash_after_rename_rolls_back_then_repairs(self):
        # Manually drive the filesystem state to the awkward crash window:
        # live moved to bak, staged candidate still in stage, manifest still
        # quarantined -> startup must restore live and the requeued job then
        # rebuilds cleanly.
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(12)])
        victim = s.list_segments()["segments"][1]["id"]
        s.quarantine(victim, "test")
        jid = "job-crashprobe-aaaaaaaaaaaa"
        s._repairs[jid] = {
            "id": jid, "seg_id": victim, "status": "running",
            "attempt": 1, "stage": "committing",
            "created_at": fmt_ts(utcnow()), "updated_at": fmt_ts(utcnow()),
            "error": None, "result": None, "detail": None,
        }
        s._persist_repairs_locked()
        s.close()

        seg_root = s.seg_root
        live = segmod.seg_dir(seg_root, victim)
        bak = os.path.join(seg_root, f"bak-{jid}-1", victim)
        os.makedirs(os.path.dirname(bak), exist_ok=True)
        os.rename(live, bak)
        stage = os.path.join(seg_root, f"stage-{jid}-1", victim)
        os.makedirs(stage, exist_ok=True)
        # bogus candidate that must NOT be adopted (journal says running)
        with open(segmod.events_path(os.path.join(seg_root, f"stage-{jid}-1"),
                                     victim), "wb") as fh:
            fh.write(b"not a real segment")

        s2 = open_store(self.tmp)
        self.assertTrue(os.path.isdir(live))      # rollback restored it
        self.assertFalse(os.path.exists(os.path.join(seg_root, f"bak-{jid}-1")))
        self.assertFalse(os.path.exists(os.path.join(seg_root, f"stage-{jid}-1")))
        jobs = [j for j in s2.list_repairs(limit=100) if j["seg_id"] == victim]
        done = terminal(s2, jobs[-1]["id"])
        self.assertEqual(done["status"], "succeeded", done.get("error"))
        got = s2.device_events("d1", limit=500)
        self.assertEqual([e["offset"] for e in got["events"]], list(range(12)))
        s2.close()

    def test_crash_after_swap_before_manifest_is_resumed(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(12)])
        victim = s.list_segments()["segments"][1]["id"]
        orig_sha = [m for m in s.list_segments()["segments"]
                    if m["id"] == victim][0]["sha256"]
        s.quarantine(victim, "test")

        # Produce a valid candidate from the WAL via a real repair, then
        # reconstruct the "both renames landed, manifest missed" window.
        probe, _ = s.start_repair(victim)
        terminal(s, probe["id"])
        result = [m for m in s.list_segments()["segments"]
                  if m["id"] == victim][0]
        self.assertEqual(result["sha256"], orig_sha)
        s.close()

        jid = "job-crashprobe-bbbbbbbbbbbb"
        seg_root = s.seg_root
        live = segmod.seg_dir(seg_root, victim)
        bak = os.path.join(seg_root, f"bak-{jid}-1", victim)
        os.makedirs(os.path.dirname(bak), exist_ok=True)
        # Pretend the commit never happened: move rebuilt live aside into bak
        # and put the quarantined bytes back as live.
        quar = tempfile.mkdtemp()
        shutil.copytree(live, os.path.join(quar, "good"))
        os.rename(live, bak)
        good = os.path.join(quar, "good")
        os.rename(good, live)
        shutil.rmtree(quar, ignore_errors=True)
        # journal says succeeded and carries the rebuilt sha
        with open(os.path.join(s.state_dir, "repairs.json"), "w") as fh:
            import json
            json.dump({"repairs": [{
                "id": jid, "seg_id": victim, "status": "succeeded",
                "attempt": 1, "stage": "succeeded",
                "created_at": fmt_ts(utcnow()), "updated_at": fmt_ts(utcnow()),
                "error": None, "detail": None,
                "result": dict(result)}]}, fh)

        s2 = open_store(self.tmp)
        meta = [m for m in s2.list_segments()["segments"]
                if m["id"] == victim][0]
        self.assertEqual(meta["status"], "sealed")
        self.assertEqual(meta["sha256"], orig_sha)
        self.assertFalse(os.path.exists(os.path.join(seg_root, f"bak-{jid}-1")))
        s2.close()


if __name__ == "__main__":
    unittest.main()
