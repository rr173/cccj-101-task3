"""Unit tests for the archive store (no network involved)."""

import os
import shutil
import sys
import tempfile
import unittest
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eventarch.config import Config
from eventarch.models import fmt_ts, utcnow
from eventarch.store import ArchiveStore


def make_cfg(tmp, **kw):
    base = dict(
        data_dir=tmp, segment_max_records=5, segment_max_age_sec=3600,
        late_threshold_sec=900, wal_retain_segments=8, janitor_interval_sec=0.1,
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


class ClassificationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.s = open_store(self.tmp)
        self.addCleanup(self.s.close)

    def test_duplicate_by_event_id_is_idempotent(self):
        r1 = self.s.ingest([ev("d1", 1)])[0]
        r2 = self.s.ingest([ev("d1", 1)])[0]  # same event_id
        self.assertEqual(r1["status"], "stored")
        self.assertEqual(r2["status"], "duplicate")
        self.assertEqual(r1["offset"], r2["offset"])
        self.assertTrue(r2["flags"]["duplicate"])
        self.assertEqual(self.s.stats()["counters"]["ingested"], 1)

    def test_late_flag(self):
        old = utcnow() - timedelta(hours=3)  # source came back after outage
        r = self.s.ingest([ev("d1", 1, ts=old)])[0]
        self.assertTrue(r["flags"]["late"])

    def test_clock_rollback_flag(self):
        t0 = utcnow()
        self.s.ingest([ev("d1", 1, ts=t0)])
        r = self.s.ingest([ev("d1", 2, ts=t0 - timedelta(minutes=5))])[0]
        self.assertTrue(r["flags"]["clock_rollback"])

    def test_seq_conflict_stored_but_flagged(self):
        self.s.ingest([ev("d1", 1, event_id="a")])
        r = self.s.ingest([ev("d1", 1, event_id="b")])[0]
        self.assertEqual(r["status"], "stored")
        self.assertTrue(r["flags"]["seq_conflict"])

    def test_invalid_event_rejected_without_failing_batch(self):
        res = self.s.ingest([{"device_id": "d1", "seq": "x"}, ev("d1", 1)])
        self.assertEqual(res[0]["status"], "error")
        self.assertEqual(res[1]["status"], "stored")


class DurabilityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_acknowledged_writes_survive_restart(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(7)])  # crosses a seal boundary (5)
        s.close()  # graceful or not: WAL is fsync'd per batch

        s2 = open_store(self.tmp)
        got = s2.device_events("d1", limit=100)["events"]
        self.assertEqual([e["event"]["seq"] for e in got], list(range(7)))
        self.assertEqual(s2.stats()["next_offset"], 7)
        s2.close()

    def test_torn_wal_tail_is_truncated(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(3)])
        s.close()
        wal_dir = os.path.join(self.tmp, "wal")
        wal_file = os.path.join(wal_dir, os.listdir(wal_dir)[0])
        with open(wal_file, "ab") as fh:  # simulate crash mid-write
            fh.write(b"\xde\xad\xbe")
        s2 = open_store(self.tmp)
        self.assertEqual(len(s2.device_events("d1")["events"]), 3)
        # and the store keeps accepting writes afterwards
        s2.ingest([ev("d1", 3)])
        self.assertEqual(len(s2.device_events("d1")["events"]), 4)
        s2.close()


class FreezeReplayTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_frozen_view_excludes_later_data(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(3)])
        frz = s.freeze(note="checkpoint")
        s.ingest([ev("d1", i) for i in range(3, 8)])  # arrives during/after freeze

        frozen = s.replay(freeze_id=frz["id"], limit=100)
        self.assertEqual([e["offset"] for e in frozen["events"]], [0, 1, 2])
        self.assertTrue(frozen["complete"])
        self.assertTrue(all(o < frz["end_offset"] for o in
                            (e["offset"] for e in frozen["events"])))

        # live replay from the freeze horizon sees only the new data
        live = s.replay(from_offset=frz["end_offset"], limit=100)
        self.assertEqual([e["offset"] for e in live["events"]], [3, 4, 5, 6, 7])
        s.close()

    def test_freeze_survives_restart(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(3)])
        frz = s.freeze()
        s.close()
        s2 = open_store(self.tmp)
        got = s2.replay(freeze_id=frz["id"], limit=100)
        self.assertEqual(len(got["events"]), 3)
        s2.close()


class BusinessOrderTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_backfill_keeps_business_order_across_segments(self):
        s = open_store(self.tmp)  # seals every 5 records
        # device comes back after hours: interleaved arrival, out-of-order seqs
        s.ingest([ev("d1", i) for i in (3, 4, 5, 6, 7)])          # segment 1
        s.ingest([ev("d1", i) for i in (0, 1, 2, 8, 9)])           # backfill -> segment 2
        got = s.device_events("d1", limit=100)["events"]
        self.assertEqual([e["event"]["seq"] for e in got], list(range(10)))
        s.close()


class SegmentationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_large_batch_splits_into_bounded_segments(self):
        s = open_store(self.tmp)  # max 5 records per segment
        s.ingest([ev("d1", i) for i in range(12)])
        segs = s.list_segments()["segments"]
        self.assertEqual([m["count"] for m in segs], [5, 5])
        self.assertEqual(segs[0]["last_offset"] + 1, segs[1]["first_offset"])
        self.assertEqual(s.list_segments()["open"]["count"], 2)
        got = s.device_events("d1", limit=100)["events"]
        self.assertEqual([e["event"]["seq"] for e in got], list(range(12)))
        s.close()

    def test_rebuild_beyond_wal_retention_reports_resume_offset(self):
        s = open_store(self.tmp, wal_retain_segments=1)
        s.ingest([ev("d1", i) for i in range(5)])    # segment A
        s.ingest([ev("d1", i) for i in range(5, 10)])  # segment B, A's WAL collected
        first = s.list_segments()["segments"][0]["id"]
        s.quarantine(first, "test")
        with self.assertRaises(Exception) as ctx:
            s.rebuild_segment(first)
        self.assertEqual(ctx.exception.resume_offset, 5)
        s.close()


class MultiSealRebuildTest(unittest.TestCase):
    """Regression: one batch crossing >=2 seal boundaries puts the records of
    the non-first sealed segments in an *earlier* WAL file (intermediate
    rotated files stay empty).  Rebuild must still find them instead of
    reporting 410 WalCoverageGone, and retention must not collect the shared
    file while it still carries in-window records."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    @staticmethod
    def _corrupt_segment(tmp, seg_id):
        path = os.path.join(tmp, "segments", seg_id, "events.log")
        with open(path, "r+b") as fh:
            fh.seek(20)
            b = fh.read(1)
            fh.seek(20)
            fh.write(bytes([b[0] ^ 0xFF]))

    def test_rebuild_non_first_segment_sealed_by_one_batch(self):
        s = open_store(self.tmp)  # segment_max_records=5
        s.ingest([ev("d1", i) for i in range(12)])  # one batch -> seg-0, seg-5
        segs = s.list_segments()["segments"]
        self.assertEqual([m["count"] for m in segs], [5, 5])
        victim, orig_sha = segs[1]["id"], segs[1]["sha256"]  # offsets [5..9]
        healthy_sha = segs[0]["sha256"]
        s.close()
        self._corrupt_segment(self.tmp, victim)

        s2 = open_store(self.tmp)  # startup verification quarantines it
        meta = [m for m in s2.list_segments()["segments"] if m["id"] == victim][0]
        self.assertEqual(meta["status"], "quarantined")
        got = s2.device_events("d1", limit=100)
        self.assertEqual([e["offset"] for e in got["events"]],
                         [0, 1, 2, 3, 4, 10, 11])
        self.assertEqual(got["gaps"][0]["resume_offset"], 10)

        rebuilt = s2.rebuild_segment(victim)
        self.assertEqual(rebuilt["status"], "sealed")
        self.assertEqual(rebuilt["sha256"], orig_sha)  # byte-identical rebuild
        # the healthy segment is untouched
        segs_after = s2.list_segments()["segments"]
        self.assertEqual(segs_after[0]["sha256"], healthy_sha)
        self.assertEqual(segs_after[0]["status"], "sealed")
        # full device view restored, in business order, no duplicates
        got = s2.device_events("d1", limit=100)
        self.assertEqual([e["event"]["seq"] for e in got["events"]],
                         list(range(12)))
        self.assertEqual([e["offset"] for e in got["events"]], list(range(12)))
        self.assertEqual(got["gaps"], [])
        # the pre-rebuild resume position replays exactly the tail once
        tail = s2.replay(from_offset=10, limit=100)
        self.assertEqual([e["offset"] for e in tail["events"]], [10, 11])
        s2.close()

        # rebuilt state is stable across a restart
        s3 = open_store(self.tmp)
        lst = s3.list_segments()["segments"]
        self.assertTrue(all(m["status"] == "sealed" for m in lst))
        got = s3.device_events("d1", limit=100)
        self.assertEqual([e["event"]["seq"] for e in got["events"]],
                         list(range(12)))
        s3.close()

    def test_rebuild_preserves_freeze_view_and_resume_offset(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(12)])  # seg-0, seg-5, 2 open
        frz = s.freeze()  # seals the tail -> seg-10; end_offset=12
        victim = s.list_segments()["segments"][1]["id"]
        s.quarantine(victim, "simulated corruption")

        frozen = s.replay(freeze_id=frz["id"], limit=100)
        self.assertEqual([e["offset"] for e in frozen["events"]],
                         [0, 1, 2, 3, 4, 10, 11])
        self.assertEqual(frozen["gaps"][0]["resume_offset"], 10)

        s.rebuild_segment(victim)
        frozen2 = s.replay(freeze_id=frz["id"], limit=100)
        self.assertEqual(frozen2["end_offset"], frz["end_offset"])  # boundary
        self.assertEqual(frozen2["gaps"], [])
        self.assertEqual([e["offset"] for e in frozen2["events"]],
                         list(range(12)))
        s.close()

    def test_retention_keeps_shared_wal_file_of_multi_seal_batch(self):
        s = open_store(self.tmp, wal_retain_segments=1)
        s.ingest([ev("d1", i) for i in range(12)])  # seg-0, seg-5; retain last 1
        # collection ran at seal time: the file holding seg-5's records must
        # survive even though its name (base offset) is below the horizon
        victim = s.list_segments()["segments"][1]["id"]
        s.quarantine(victim, "simulated corruption")
        rebuilt = s.rebuild_segment(victim)
        self.assertEqual(rebuilt["status"], "sealed")
        got = s.device_events("d1", limit=100)
        self.assertEqual([e["event"]["seq"] for e in got["events"]],
                         list(range(12)))
        s.close()


class CorruptionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def _corrupt_first_segment(self):
        seg_root = os.path.join(self.tmp, "segments")
        seg = sorted(os.listdir(seg_root))[0]
        path = os.path.join(seg_root, seg, "events.log")
        with open(path, "r+b") as fh:
            fh.seek(20)
            b = fh.read(1)
            fh.seek(20)
            fh.write(bytes([b[0] ^ 0xFF]))
        return seg

    def test_quarantine_isolates_and_reports_resume_offset(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(5)])   # sealed segment [0..4]
        s.ingest([ev("d1", i) for i in range(5, 8)])
        s.close()

        seg = self._corrupt_first_segment()
        s2 = open_store(self.tmp)
        lst = s2.list_segments()
        bad = [m for m in lst["segments"] if m["id"] == seg][0]
        self.assertEqual(bad["status"], "quarantined")
        self.assertEqual(bad["last_offset"] + 1, 5)  # resume position

        # device query isolates the gap but keeps serving healthy data
        got = s2.device_events("d1", limit=100)
        self.assertEqual([e["event"]["seq"] for e in got["events"]], [5, 6, 7])
        self.assertEqual(got["gaps"][0]["resume_offset"], 5)

        # rebuild from retained WAL restores the segment
        meta = s2.rebuild_segment(seg)
        self.assertEqual(meta["status"], "sealed")
        got = s2.device_events("d1", limit=100)
        self.assertEqual([e["event"]["seq"] for e in got["events"]], list(range(8)))
        s2.close()

    def test_replay_marks_gap_and_continues(self):
        s = open_store(self.tmp)
        s.ingest([ev("d1", i) for i in range(5)])
        s.ingest([ev("d1", i) for i in range(5, 10)])
        frz = s.freeze()
        s.close()
        self._corrupt_first_segment()
        s2 = open_store(self.tmp)
        rep = s2.replay(freeze_id=frz["id"], limit=100)
        self.assertEqual([e["offset"] for e in rep["events"]], [5, 6, 7, 8, 9])
        self.assertEqual(rep["gaps"][0]["resume_offset"], 5)
        s2.close()


if __name__ == "__main__":
    unittest.main()
