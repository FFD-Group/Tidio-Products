"""
entrypoint.py - Tidio sync scheduler

Runs inside the container instead of supercronic. Avoids any shell dependency
and keeps everything within the Python runtime that is already present.

Schedule (all times UTC):
  - Incremental sync: top of every even hour except 02:00
      00:00, 04:00, 06:00, 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00, 22:00
  - Full catalog sync: 02:00 daily
"""

import datetime
import logging
import subprocess
import sys
import time

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='ts=%(asctime)s level=%(levelname)s logger=scheduler msg="%(message)s"',
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger("scheduler")

PYTHON = sys.executable          # reuse whichever python is running this script
SCRIPT = "/app/app.py"
FULL_HOUR = 2                    # UTC hour for the daily full sync
INCREMENTAL_HOURS = {0, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22}


def run_sync(full: bool = False) -> None:
    cmd = [PYTHON, SCRIPT] + (["--full"] if full else [])
    label = "full" if full else "incremental"
    logger.info(f"Launching {label} sync: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode == 0:
        logger.info(f"{label} sync finished (exit 0).")
    else:
        logger.error(f"{label} sync exited with code {result.returncode}.")


def seconds_until_next_minute() -> float:
    now = datetime.datetime.utcnow()
    return 60 - now.second - now.microsecond / 1_000_000


def main() -> None:
    logger.info("Scheduler started. Waiting for next scheduled run...")
    last_fired_hour: int | None = None

    while True:
        time.sleep(seconds_until_next_minute())

        now = datetime.datetime.utcnow()

        # Only act on the :00 minute, and guard against double-firing
        if now.minute != 0 or now.hour == last_fired_hour:
            continue

        last_fired_hour = now.hour

        if now.hour == FULL_HOUR:
            run_sync(full=True)
        elif now.hour in INCREMENTAL_HOURS:
            run_sync(full=False)
        # else: off-schedule hour (e.g. 01:00, 03:00) — do nothing


if __name__ == "__main__":
    main()
