"""Environment-driven configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Config:
    data_dir: str = "./data"
    addr: str = "0.0.0.0:8080"
    segment_max_records: int = 1000        # seal threshold: record count
    segment_max_age_sec: float = 300.0     # seal threshold: oldest-record age
    late_threshold_sec: float = 900.0      # device_ts older than this -> "late"
    wal_retain_segments: int = 8           # sealed segments kept as WAL for rebuild
    fsync: bool = True                     # set EA_FSYNC=0 only for benchmarks
    janitor_interval_sec: float = 1.0
    max_batch: int = 1000
    repair_workers: int = 2               # background repair-job concurrency
    repair_max_attempts: int = 5          # version-conflict / torn-WAL retries
    repair_retry_backoff_sec: float = 0.1
    repair_history: int = 100             # finished jobs retained in the journal

    @classmethod
    def from_env(cls) -> "Config":
        def env(key, default, cast):
            v = os.environ.get(key)
            if v is None or v == "":
                return default
            return cast(v)

        return cls(
            data_dir=env("EA_DATA_DIR", cls.data_dir, str),
            addr=env("EA_ADDR", cls.addr, str),
            segment_max_records=env("EA_SEGMENT_MAX_RECORDS", cls.segment_max_records, int),
            segment_max_age_sec=env("EA_SEGMENT_MAX_AGE_SEC", cls.segment_max_age_sec, float),
            late_threshold_sec=env("EA_LATE_THRESHOLD_SEC", cls.late_threshold_sec, float),
            wal_retain_segments=env("EA_WAL_RETAIN_SEGMENTS", cls.wal_retain_segments, int),
            fsync=env("EA_FSYNC", "1", str).lower() not in ("0", "false", "no"),
            janitor_interval_sec=env("EA_JANITOR_INTERVAL_SEC", cls.janitor_interval_sec, float),
            max_batch=env("EA_MAX_BATCH", cls.max_batch, int),
            repair_workers=env("EA_REPAIR_WORKERS", cls.repair_workers, int),
            repair_max_attempts=env("EA_REPAIR_MAX_ATTEMPTS", cls.repair_max_attempts, int),
            repair_retry_backoff_sec=env("EA_REPAIR_RETRY_BACKOFF_SEC",
                                         cls.repair_retry_backoff_sec, float),
            repair_history=env("EA_REPAIR_HISTORY", cls.repair_history, int),
        )
