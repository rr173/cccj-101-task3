#!/usr/bin/env python3
"""End-to-end smoke test against a real server process.

Scenario:
  1.  start server (small segments to force sealing)
  2.  ingest normal traffic from two devices
  3.  ingest a duplicate, a late backfill event, a clock-rollback event,
      and a seq conflict -> verify classification
  4.  freeze the view, ingest more -> replay must not see the new data
  5.  restart the server -> acknowledged data must survive
  6.  corrupt the first sealed segment, restart -> quarantined with a
      resume offset; device queries report the gap but keep serving
  7.  rebuild the segment from retained WAL -> fully restored
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timedelta, timezone

PORT = 18099
BASE = f"http://127.0.0.1:{PORT}"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

failures = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read())


def req_status(method, path, body=None):
    try:
        return req(method, path, body), 200
    except urllib.error.HTTPError as e:
        return json.loads(e.read()), e.code


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def ev(dev, seq, eid=None, ts=None, payload=None):
    return {"device_id": dev, "event_id": eid or f"{dev}-e{seq}", "seq": seq,
            "device_ts": iso(ts or datetime.now(timezone.utc)),
            "payload": payload if payload is not None else {"v": seq}}


def start_server(data_dir):
    env = dict(os.environ)
    env.update({
        "EA_DATA_DIR": data_dir,
        "EA_ADDR": f"127.0.0.1:{PORT}",
        "EA_SEGMENT_MAX_RECORDS": "5",
        "EA_SEGMENT_MAX_AGE_SEC": "3600",
        "EA_LATE_THRESHOLD_SEC": "900",
        "PYTHONPATH": ROOT,
    })
    proc = subprocess.Popen(
        [sys.executable, "-m", "eventarch.server"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for _ in range(100):
        try:
            req("GET", "/v1/healthz")
            return proc
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("server did not start:\n" + proc.stdout.read())


def stop_server(proc):
    proc.terminate()
    proc.wait(timeout=10)


def main():
    data_dir = tempfile.mkdtemp(prefix="eventarch-smoke-")
    proc = start_server(data_dir)
    try:
        print("== 1. ingest normal traffic ==")
        r = req("POST", "/v1/ingest",
                {"events": [ev("dev-A", i) for i in range(1, 6)]
                           + [ev("dev-B", i) for i in range(1, 4)]})
        check("8 events stored", all(x["status"] == "stored" for x in r["results"]))
        check("offsets are sequential",
              [x["offset"] for x in r["results"]] == list(range(8)))

        print("== 2. classification ==")
        r = req("POST", "/v1/ingest", {"events": [ev("dev-A", 3)]})  # same event_id
        check("duplicate detected (idempotent)",
              r["results"][0]["status"] == "duplicate"
              and r["results"][0]["flags"]["duplicate"]
              and r["results"][0]["offset"] == 2)

        old = datetime.now(timezone.utc) - timedelta(hours=3)
        r = req("POST", "/v1/ingest", {"events": [ev("dev-A", 6, ts=old)]})
        check("late backfill flagged", r["results"][0]["flags"]["late"])

        rolled = datetime.now(timezone.utc) - timedelta(minutes=5)
        r = req("POST", "/v1/ingest", {"events": [ev("dev-A", 7, ts=rolled)]})
        check("clock rollback flagged", r["results"][0]["flags"]["clock_rollback"])

        r = req("POST", "/v1/ingest", {"events": [ev("dev-B", 2, eid="dev-B-e2b")]})
        check("seq conflict stored but flagged",
              r["results"][0]["status"] == "stored"
              and r["results"][0]["flags"]["seq_conflict"])

        print("== 3. freeze, then keep ingesting ==")
        frz = req("POST", "/v1/freeze", {"note": "nightly checkpoint"})
        end_off = frz["end_offset"]
        req("POST", "/v1/ingest", {"events": [ev("dev-A", i) for i in range(8, 13)]})

        rep = req("GET", f"/v1/replay?freeze_id={frz['id']}&limit=1000")
        check("frozen replay complete", rep["complete"])
        check("frozen view has only pre-freeze events",
              all(e["offset"] < end_off for e in rep["events"])
              and len(rep["events"]) == end_off,
              f"got {len(rep['events'])} events, end_offset={end_off}")

        live = req("GET", f"/v1/replay?from_offset={end_off}&limit=1000")
        check("live replay from horizon sees only new events",
              [e["offset"] for e in live["events"]] == list(range(end_off, end_off + 5)))

        print("== 4. business order across segments ==")
        got = req("GET", "/v1/devices/dev-A/events?limit=100")
        seqs = [e["event"]["seq"] for e in got["events"]]
        check("dev-A events in seq order", seqs == sorted(seqs), f"seqs={seqs}")

        print("== 5. restart: durability ==")
        stop_server(proc)
        proc = start_server(data_dir)
        got = req("GET", "/v1/devices/dev-A/events?limit=100")
        check("dev-A events survive restart", len(got["events"]) == 12)
        rep2 = req("GET", f"/v1/replay?freeze_id={frz['id']}&limit=1000")
        check("freeze survives restart", len(rep2["events"]) == end_off)

        print("== 6. corrupt a segment -> quarantine + resume position ==")
        stop_server(proc)
        seg_root = os.path.join(data_dir, "segments")
        victim = sorted(os.listdir(seg_root))[0]
        with open(os.path.join(seg_root, victim, "events.log"), "r+b") as fh:
            fh.seek(20)
            b = fh.read(1)
            fh.seek(20)
            fh.write(bytes([b[0] ^ 0xFF]))
        proc = start_server(data_dir)

        segs = req("GET", "/v1/segments")
        bad = [m for m in segs["segments"] if m["id"] == victim][0]
        check("corrupted segment quarantined", bad["status"] == "quarantined")
        check("resume offset reported", bad["last_offset"] + 1 == 5)

        body, code = req_status("GET", f"/v1/segments/{victim}/events")
        check("direct read of quarantined segment -> 410 with resume_offset",
              code == 410 and body.get("resume_offset") == 5)

        got = req("GET", "/v1/devices/dev-A/events?limit=100")
        check("device query isolates gap, keeps serving",
              len(got["gaps"]) == 1 and got["gaps"][0]["resume_offset"] == 5
              and all(e["event"]["seq"] >= 5 for e in got["events"]))

        rep3 = req("GET", f"/v1/replay?freeze_id={frz['id']}&limit=1000")
        check("replay marks gap and continues past it",
              len(rep3["gaps"]) == 1 and rep3["gaps"][0]["resume_offset"] == 5)

        print("== 7. rebuild from retained WAL ==")
        r = req("POST", f"/v1/segments/{victim}/rebuild")
        check("segment rebuilt", r["segment"]["status"] == "sealed")
        got = req("GET", "/v1/devices/dev-A/events?limit=100")
        check("device view fully restored",
              [e["event"]["seq"] for e in got["events"]] == list(range(1, 13)))

        stats = req("GET", "/v1/stats")
        check("stats sane", stats["segments"]["quarantined"] == 0
              and stats["segments"]["sealed"] >= 3)
    finally:
        stop_server(proc)
        shutil.rmtree(data_dir, ignore_errors=True)

    print()
    if failures:
        print(f"SMOKE FAILED: {len(failures)} check(s): {failures}")
        return 1
    print("SMOKE OK: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
