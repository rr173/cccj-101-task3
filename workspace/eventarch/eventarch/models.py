"""Event model, timestamp helpers, and ingress validation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional, Tuple

FLAG_KEYS = ("late", "duplicate", "clock_rollback", "seq_conflict")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(s: str) -> datetime:
    """Parse an ISO-8601/RFC3339 timestamp; naive values are treated as UTC."""
    dt = datetime.fromisoformat(s)  # py3.11 understands a trailing "Z"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_ts(dt: datetime) -> str:
    """Canonical UTC rendering; lexicographically sortable."""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def new_flags() -> dict:
    return {k: False for k in FLAG_KEYS}


def validate_event(raw: Any) -> Tuple[Optional[dict], Optional[datetime], Optional[str]]:
    """Validate one inbound event.

    Returns (event_dict, device_ts_datetime, error). On error the first two
    are None. The returned event dict is normalized (UTC device_ts).
    """
    if not isinstance(raw, dict):
        return None, None, "event must be a JSON object"

    device_id = raw.get("device_id")
    if not isinstance(device_id, str) or not device_id or len(device_id) > 256:
        return None, None, "device_id must be a non-empty string (<=256 chars)"

    event_id = raw.get("event_id")
    if not isinstance(event_id, str) or not event_id or len(event_id) > 256:
        return None, None, "event_id must be a non-empty string (<=256 chars)"

    seq = raw.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int):
        return None, None, "seq must be an integer (per-device business sequence)"

    ts = raw.get("device_ts")
    if not isinstance(ts, str):
        return None, None, "device_ts must be an ISO-8601/RFC3339 string"
    try:
        dt = parse_ts(ts)
    except ValueError:
        return None, None, "device_ts is not a valid ISO-8601 timestamp"

    event = {
        "device_id": device_id,
        "event_id": event_id,
        "seq": seq,
        "device_ts": fmt_ts(dt),
        "payload": raw.get("payload"),
    }
    return event, dt, None
