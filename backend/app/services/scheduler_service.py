"""
Logic run by the standalone scheduler process (see app/scheduler.py).

Responsibilities:
  1. Promote SCHEDULED jobs (delayed/scheduled one-off jobs) to QUEUED once
     their run_at time has arrived.
  2. Walk the ScheduledJob (recurring/cron) table and materialize a new
     concrete Job whenever next_run_at has passed, then advance
     next_run_at using croniter.
  3. Reap stale jobs: requeue jobs stuck in CLAIMED/RUNNING whose worker's
     heartbeat has gone silent past the timeout (fixes the "no stale-job
     reaper" gap noted in docs/DESIGN_DECISIONS.md).
  4. Distributed locking: if more than one scheduler process is running for
     redundancy, only the lock holder performs 1-3 on a given tick, so two
     schedulers can never double-materialize the same recurring job.
"""
from datetime import datetime, timedelta

from croniter import croniter
from sqlalchemy import update
from sqlalchemy.orm import Session

from app import models
from app.config import settings
from app.logging_config import logger


def promote_ready_scheduled_jobs(db: Session) -> int:
    now = datetime.utcnow()
    ready = db.query(models.Job).filter(
        models.Job.status == models.JobStatus.SCHEDULED,
        models.Job.run_at <= now,
    ).all()

    for job in ready:
        job.status = models.JobStatus.QUEUED
        db.add(models.JobLog(job_id=job.id, level="INFO", message="Promoted from SCHEDULED to QUEUED"))

    if ready:
        db.commit()
    return len(ready)


def process_recurring_jobs(db: Session) -> int:
    now = datetime.utcnow()
    due = db.query(models.ScheduledJob).filter(
        models.ScheduledJob.is_active.is_(True),
        models.ScheduledJob.next_run_at <= now,
    ).all()

    created = 0
    for sched in due:
        queue = db.query(models.Queue).filter(models.Queue.id == sched.queue_id).first()
        if queue is None or queue.is_paused:
            # Still advance the cron cursor so we don't spam-create backlog
            # once the queue is unpaused.
            _advance_cron(sched, now)
            continue

        job = models.Job(
            queue_id=sched.queue_id,
            job_type=models.JobType.RECURRING,
            task_name=sched.task_name,
            payload=sched.payload,
            status=models.JobStatus.QUEUED,
            run_at=now,
            cron_expression=sched.cron_expression,
            retry_policy_id=queue.retry_policy_id,
            max_retries=queue.retry_policy.max_retries if queue.retry_policy else 3,
        )
        db.add(job)
        sched.last_run_at = now
        _advance_cron(sched, now)
        created += 1

    if due:
        db.commit()
    return created


def _advance_cron(sched: models.ScheduledJob, now: datetime) -> None:
    itr = croniter(sched.cron_expression, now)
    sched.next_run_at = itr.get_next(datetime)


# --------------------------------------------------------------------------
# Distributed locking (leader election for redundant scheduler instances)
# --------------------------------------------------------------------------

LOCK_ID = "scheduler"


def try_acquire_lock(db: Session, holder_id: str, lease_seconds: int = 10) -> bool:
    """
    Best-effort mutual-exclusion lock so that running multiple scheduler
    processes for redundancy never results in two of them promoting the
    same job or double-materializing the same recurring job on the same
    tick. Uses the identical atomic-UPDATE pattern as job claiming
    (job_service.claim_next_job): whichever instance's UPDATE actually
    matches a row wins the lock for `lease_seconds`. If that instance dies
    or stalls, the lease expires and another instance can take over -
    there is no need for explicit unlock-on-crash handling.
    """
    now = datetime.utcnow()
    expires_at = now + timedelta(seconds=lease_seconds)

    # Ensure the singleton lock row exists.
    existing = db.query(models.SchedulerLock).filter(models.SchedulerLock.id == LOCK_ID).first()
    if existing is None:
        try:
            db.add(models.SchedulerLock(id=LOCK_ID, holder_id=None, expires_at=None))
            db.commit()
        except Exception:
            db.rollback()  # another instance created it first; fine

    result = db.execute(
        update(models.SchedulerLock)
        .where(
            models.SchedulerLock.id == LOCK_ID,
            (models.SchedulerLock.holder_id.is_(None))
            | (models.SchedulerLock.holder_id == holder_id)
            | (models.SchedulerLock.expires_at < now),
        )
        .values(holder_id=holder_id, acquired_at=now, expires_at=expires_at)
    )
    db.commit()
    return result.rowcount > 0


def release_lock(db: Session, holder_id: str) -> None:
    db.execute(
        update(models.SchedulerLock)
        .where(models.SchedulerLock.id == LOCK_ID, models.SchedulerLock.holder_id == holder_id)
        .values(holder_id=None, expires_at=None)
    )
    db.commit()


# --------------------------------------------------------------------------
# Stale job reaper
# --------------------------------------------------------------------------

def reap_stale_jobs(db: Session) -> int:
    """
    Finds jobs stuck in CLAIMED/RUNNING whose claiming worker's heartbeat
    has gone silent for longer than WORKER_HEARTBEAT_TIMEOUT_SECONDS (i.e.
    the worker almost certainly crashed mid-job) and requeues them for
    another attempt. This closes the "at-least-once delivery" gap called
    out explicitly in docs/DESIGN_DECISIONS.md #2 - jobs no longer stay
    stuck forever if their worker dies.
    """
    cutoff = datetime.utcnow() - timedelta(seconds=settings.WORKER_HEARTBEAT_TIMEOUT_SECONDS)

    stuck = db.query(models.Job).join(
        models.Worker, models.Worker.id == models.Job.claimed_by_worker_id
    ).filter(
        models.Job.status.in_([models.JobStatus.CLAIMED, models.JobStatus.RUNNING]),
        models.Worker.last_seen_at < cutoff,
    ).all()

    for job in stuck:
        job.status = models.JobStatus.QUEUED
        job.run_at = datetime.utcnow()
        job.claimed_by_worker_id = None
        job.claimed_at = None
        db.add(models.JobLog(
            job_id=job.id, level="WARNING",
            message=f"Reaped: worker heartbeat went stale mid-execution; requeued for retry",
        ))
        logger.warning(f"job={job.id} | Reaped stale job from dead worker, requeued")

    if stuck:
        db.commit()
    return len(stuck)
