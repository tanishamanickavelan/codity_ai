"""
Core job lifecycle logic shared by the API layer and the worker process.

The most important function here is `claim_next_job`, which implements
atomic job claiming so that two workers polling the same queue at the
same instant can never both start executing the same job.
"""
import hashlib
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import update
from sqlalchemy.orm import Session

from app import models
from app.config import settings
from app.logging_config import logger
from app.services.retry_service import compute_delay_seconds
from app.services.failure_summary import generate_failure_summary


def _log(db: Session, job_id: str, message: str, level: str = "INFO") -> None:
    db.add(models.JobLog(job_id=job_id, level=level, message=message))
    getattr(logger, level.lower(), logger.info)(f"job={job_id} | {message}")


def _shard_for(job_id: str, shard_count: int) -> int:
    """Deterministic shard assignment: hash(job_id) % shard_count."""
    if shard_count <= 1:
        return 0
    digest = hashlib.sha256(job_id.encode()).hexdigest()
    return int(digest, 16) % shard_count


def create_job(
    db: Session,
    queue: models.Queue,
    task_name: str,
    payload: dict,
    job_type: models.JobType = models.JobType.IMMEDIATE,
    priority: int = 0,
    run_at: Optional[datetime] = None,
    cron_expression: Optional[str] = None,
    max_retries: Optional[int] = None,
    idempotency_key: Optional[str] = None,
    batch_id: Optional[str] = None,
    depends_on: Optional[list[str]] = None,
) -> models.Job:
    """
    Create a job in the correct initial lifecycle state:
      - unmet dependencies (depends_on jobs not all COMPLETED) -> BLOCKED,
        regardless of run_at; promoted event-drivenly when the last
        dependency completes (see promote_ready_dependents below)
      - run_at in the future (delayed/scheduled)               -> SCHEDULED
        (the scheduler process promotes it to QUEUED when run_at is reached)
      - run_at now or in the past (immediate)                  -> QUEUED directly

    Idempotency: if an idempotency_key is supplied and a job with the same
    key already exists on this queue, that existing job is returned instead
    of creating a duplicate (enforced at the DB level via a unique
    constraint on (queue_id, idempotency_key) as a second line of defense).

    Sharding: each job is deterministically assigned a shard_id in
    [0, queue.shard_count) so that dedicated worker pools can each own a
    shard instead of every worker contending for every job.
    """
    now = datetime.utcnow()
    effective_run_at = run_at or now

    if idempotency_key:
        existing = db.query(models.Job).filter(
            models.Job.queue_id == queue.id,
            models.Job.idempotency_key == idempotency_key,
        ).first()
        if existing:
            return existing

    unmet_deps: list[str] = []
    if depends_on:
        dep_jobs = db.query(models.Job).filter(models.Job.id.in_(depends_on)).all()
        found_ids = {j.id for j in dep_jobs}
        missing = set(depends_on) - found_ids
        if missing:
            raise ValueError(f"depends_on references unknown job id(s): {missing}")
        unmet_deps = [j.id for j in dep_jobs if j.status != models.JobStatus.COMPLETED]

    if unmet_deps:
        initial_status = models.JobStatus.BLOCKED
    elif effective_run_at > now:
        initial_status = models.JobStatus.SCHEDULED
    else:
        initial_status = models.JobStatus.QUEUED

    job_id = models.gen_uuid()
    job = models.Job(
        id=job_id,
        queue_id=queue.id,
        job_type=job_type,
        task_name=task_name,
        payload=payload,
        status=initial_status,
        priority=priority,
        run_at=effective_run_at,
        cron_expression=cron_expression,
        retry_policy_id=queue.retry_policy_id,
        max_retries=max_retries if max_retries is not None else (
            queue.retry_policy.max_retries if queue.retry_policy else settings.DEFAULT_MAX_RETRIES
        ),
        idempotency_key=idempotency_key,
        batch_id=batch_id,
        shard_id=_shard_for(job_id, queue.shard_count),
    )
    db.add(job)

    if depends_on:
        for dep_id in depends_on:
            db.add(models.JobDependency(job_id=job_id, depends_on_job_id=dep_id))

    db.commit()
    db.refresh(job)
    dep_note = f", depends_on={depends_on}" if depends_on else ""
    _log(db, job.id, f"Job created (type={job_type.value}, status={initial_status.value}, shard={job.shard_id}{dep_note})")
    db.commit()
    return job


def promote_ready_dependents(db: Session, completed_job: models.Job) -> int:
    """
    Event-driven half of the workflow-dependencies feature: called right
    after a job completes. Finds every BLOCKED job that depends on it and,
    for each one, checks whether *all* of its dependencies are now
    COMPLETED - if so, promotes it straight to QUEUED (or SCHEDULED if its
    run_at is still in the future) immediately, rather than waiting for a
    scheduler poll tick to notice.
    """
    edges = db.query(models.JobDependency).filter(
        models.JobDependency.depends_on_job_id == completed_job.id
    ).all()
    promoted = 0
    now = datetime.utcnow()

    for edge in edges:
        dependent = db.query(models.Job).filter(models.Job.id == edge.job_id).first()
        if dependent is None or dependent.status != models.JobStatus.BLOCKED:
            continue

        remaining = db.query(models.JobDependency).join(
            models.Job, models.Job.id == models.JobDependency.depends_on_job_id
        ).filter(
            models.JobDependency.job_id == dependent.id,
            models.Job.status != models.JobStatus.COMPLETED,
        ).count()

        if remaining == 0:
            dependent.status = models.JobStatus.QUEUED if dependent.run_at <= now else models.JobStatus.SCHEDULED
            _log(db, dependent.id, f"All dependencies satisfied by job {completed_job.id}; promoted to {dependent.status.value}")
            promoted += 1

    if promoted:
        db.commit()
    return promoted


def claim_next_job(
    db: Session,
    worker: models.Worker,
    queue_ids: Optional[list[str]] = None,
    shard_id: Optional[int] = None,
) -> Optional[models.Job]:
    """
    Atomically claim the single highest-priority, oldest, ready-to-run job
    across the given queues (or all queues the worker is allowed to poll),
    optionally restricted to one shard (see Queue.shard_count).

    Atomicity strategy:
      - We issue one UPDATE statement that both selects the target row
        (via a correlated subquery) and flips its status in the same
        atomic operation. The database guarantees only one transaction can
        win this UPDATE for a given row, so concurrent workers polling at
        the same moment cannot both claim the same job.
      - On Postgres in production, the equivalent (and preferred, since it
        also skips contention on other in-flight candidate rows) approach
        is `SELECT ... FOR UPDATE SKIP LOCKED` inside a transaction,
        immediately followed by the UPDATE. Both approaches are documented
        in docs/DESIGN_DECISIONS.md.
    """
    now = datetime.utcnow()

    base_query = db.query(models.Job).join(models.Queue).filter(
        models.Job.status == models.JobStatus.QUEUED,
        models.Job.run_at <= now,
        models.Queue.is_paused.is_(False),
    )
    if queue_ids:
        base_query = base_query.filter(models.Job.queue_id.in_(queue_ids))
    if shard_id is not None:
        base_query = base_query.filter(models.Job.shard_id == shard_id)

    # Respect per-queue concurrency limits: don't consider queues that are
    # already at their running-job cap.
    candidate = base_query.order_by(
        models.Job.priority.desc(), models.Job.run_at.asc()
    ).first()

    if candidate is None:
        return None

    # Enforce the queue's concurrency_limit before claiming.
    running_count = db.query(models.Job).filter(
        models.Job.queue_id == candidate.queue_id,
        models.Job.status.in_([models.JobStatus.CLAIMED, models.JobStatus.RUNNING]),
    ).count()
    if running_count >= candidate.queue.concurrency_limit:
        return None

    # Atomic claim: UPDATE ... WHERE id = X AND status = 'queued'.
    # If another transaction claimed it first, rowcount will be 0 and we
    # simply report "nothing claimed" rather than raising - the caller's
    # poll loop will try again next tick.
    result = db.execute(
        update(models.Job)
        .where(models.Job.id == candidate.id, models.Job.status == models.JobStatus.QUEUED)
        .values(
            status=models.JobStatus.CLAIMED,
            claimed_by_worker_id=worker.id,
            claimed_at=now,
            attempt_count=models.Job.attempt_count + 1,
        )
    )
    db.commit()

    if result.rowcount == 0:
        # Lost the race to another worker.
        return None

    db.refresh(candidate)
    _log(db, candidate.id, f"Claimed by worker {worker.name} ({worker.id})")
    db.commit()
    return candidate


def mark_running(db: Session, job: models.Job, worker: models.Worker) -> models.JobExecution:
    job.status = models.JobStatus.RUNNING
    execution = models.JobExecution(
        job_id=job.id,
        worker_id=worker.id,
        attempt_number=job.attempt_count,
        status=models.JobStatus.RUNNING,
        started_at=datetime.utcnow(),
    )
    db.add(execution)
    _log(db, job.id, f"Attempt {job.attempt_count} started on worker {worker.name}")
    db.commit()
    db.refresh(execution)
    return execution


def mark_completed(db: Session, job: models.Job, execution: models.JobExecution, result: dict) -> None:
    now = datetime.utcnow()
    job.status = models.JobStatus.COMPLETED
    job.result = result
    job.completed_at = now

    execution.status = models.JobStatus.COMPLETED
    execution.finished_at = now
    execution.result = result
    execution.duration_ms = int((now - execution.started_at).total_seconds() * 1000)

    _log(db, job.id, "Completed successfully")
    db.commit()

    # Event-driven: immediately check whether this completion unblocks any
    # dependent jobs, rather than waiting for the next scheduler tick.
    promote_ready_dependents(db, job)


def mark_failed(db: Session, job: models.Job, execution: models.JobExecution, error: str) -> None:
    """
    Handle a failed attempt: either schedule a retry with backoff, or move
    the job permanently to the Dead Letter Queue if retries are exhausted.
    """
    now = datetime.utcnow()
    execution.status = models.JobStatus.FAILED
    execution.finished_at = now
    execution.error_message = error
    execution.duration_ms = int((now - execution.started_at).total_seconds() * 1000)

    job.error_message = error

    if job.attempt_count < job.max_retries:
        policy = job.retry_policy or (job.queue.retry_policy if job.queue else None)
        strategy = policy.strategy if policy else models.RetryStrategy.EXPONENTIAL
        base_delay = policy.base_delay_seconds if policy else settings.DEFAULT_RETRY_BASE_DELAY_SECONDS
        max_delay = policy.max_delay_seconds if policy else 3600

        delay = compute_delay_seconds(strategy, job.attempt_count, base_delay, max_delay)

        job.status = models.JobStatus.QUEUED
        job.run_at = now + timedelta(seconds=delay)
        job.claimed_by_worker_id = None
        job.claimed_at = None

        _log(
            db, job.id,
            f"Attempt {job.attempt_count} failed: {error}. "
            f"Retrying in {delay}s (strategy={strategy.value})",
            level="WARNING",
        )
    else:
        job.status = models.JobStatus.DEAD
        summary = generate_failure_summary(job=job, last_error=error, attempt_count=job.attempt_count)
        dlq_entry = models.DeadLetterQueueEntry(
            job_id=job.id,
            reason=f"Exhausted {job.max_retries} retries. Last error: {error}",
            final_payload=job.payload,
            ai_summary=summary,
        )
        db.add(dlq_entry)
        _log(db, job.id, f"Moved to Dead Letter Queue after {job.attempt_count} attempts", level="ERROR")

    db.commit()


def replay_dlq_entry(db: Session, dlq_entry: models.DeadLetterQueueEntry) -> models.Job:
    """Requeue a dead job for another full attempt cycle."""
    job = dlq_entry.job
    job.status = models.JobStatus.QUEUED
    job.attempt_count = 0
    job.run_at = datetime.utcnow()
    job.claimed_by_worker_id = None
    job.error_message = None
    dlq_entry.replayed = True
    _log(db, job.id, "Replayed from Dead Letter Queue")
    db.commit()
    db.refresh(job)
    return job
