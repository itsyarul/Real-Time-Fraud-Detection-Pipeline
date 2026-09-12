#!/usr/bin/env python3
"""
run_loop.py
-----------
Calls push_metrics.main() every INTERVAL_SECONDS (default 60).
Pure-Python replacement for supercronic — no external binaries needed.
"""

import os
import time
import logging
import traceback

import push_metrics

INTERVAL = int(os.getenv("PUSH_INTERVAL_SECONDS", "60"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [run_loop] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("run_loop")

log.info("Metrics push loop started — interval: %ds", INTERVAL)

while True:
    start = time.monotonic()
    try:
        push_metrics.main()
    except SystemExit:
        # push_metrics calls sys.exit(0/1) — catch it and keep looping
        pass
    except Exception:
        log.error("Unexpected error in push_metrics:\n%s", traceback.format_exc())

    elapsed = time.monotonic() - start
    sleep_for = max(0, INTERVAL - elapsed)
    log.info("Next push in %.1fs", sleep_for)
    time.sleep(sleep_for)
