"""
Standalone scheduler process. Run alongside the API and worker(s):

    python -m app.scheduler

You can safely run more than one instance of this process for redundancy -
they use a distributed lock (app/services/scheduler_service.py) to elect a
single leader per tick, so promotions/recurring-job materialization/reaping
never happen twice for the same job.

Each tick, the lock holder:
  1. Promotes SCHEDULED jobs to QUEUED once their run_at time arrives.
  2. Materializes new Job rows for RECURRING (cron) schedules.
  3. Reaps jobs stuck in CLAIMED/RUNNING whose worker has gone silent.
"""
import signal
import socket
import sys
import time
import uuid

from app.config import settings
from app.database import SessionLocal
from app.logging_config import logger
from app.services.scheduler_service import (
    process_recurring_jobs,
    promote_ready_scheduled_jobs,
    reap_stale_jobs,
    release_lock,
    try_acquire_lock,
)

INSTANCE_ID = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


def tick() -> None:
    db = SessionLocal()
    try:
        lease = int(settings.SCHEDULER_POLL_INTERVAL_SECONDS * 5) or 5
        if not try_acquire_lock(db, INSTANCE_ID, lease_seconds=lease):
            logger.info(f"instance={INSTANCE_ID} | did not win scheduler lock this tick, standing by")
            return

        promoted = promote_ready_scheduled_jobs(db)
        created = process_recurring_jobs(db)
        reaped = reap_stale_jobs(db)
        if promoted or created or reaped:
            logger.info(f"promoted={promoted} recurring_created={created} reaped_stale={reaped}")
    finally:
        db.close()


def _shutdown(*_args):
    logger.info(f"instance={INSTANCE_ID} | shutdown signal received, releasing lock")
    db = SessionLocal()
    try:
        release_lock(db, INSTANCE_ID)
    finally:
        db.close()
    sys.exit(0)


def main() -> None:
    logger.info(f"instance={INSTANCE_ID} | scheduler started, polling every "
                f"{settings.SCHEDULER_POLL_INTERVAL_SECONDS}s ... Ctrl+C to stop.")
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    while True:
        try:
            tick()
        except Exception as e:  # noqa: BLE001 - keep the scheduler alive across transient errors
            logger.error(f"tick error: {e}")
        time.sleep(settings.SCHEDULER_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
