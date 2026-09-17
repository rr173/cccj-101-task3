#!/usr/bin/env bash
# End-to-end smoke test: starts a real server, exercises classification,
# freeze/replay, restart durability, corruption quarantine and rebuild.
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 scripts/smoke.py
