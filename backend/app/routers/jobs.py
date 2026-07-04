import uuid
from datetime import datetime

from croniter import croniter
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app import models, schemas
from app.auth import get_current_user, require_role
from app.database import get_db
from app.rate_limit import limiter
from app.services import job_service

router = APIRouter(prefix="/api", tags=["jobs"])


def _get_queue_or_404(db: Session, queue_id: str) -> models.Queue:
    queue = db.query(models.Queue).filter(models.Queue.id == queue_id).first()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    return queue


@router.post("/jobs", response_model=schemas.JobOut, status_code=201)
@limiter.limit("60/minute")
def create_job(
    request: Request,
    payload: schemas.JobCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    queue = _get_queue_or_404(db, payload.queue_id)

    if payload.job_type == models.JobType.RECURRING:
        if not payload.cron_expression:
            raise HTTPException(status_code=400, detail="cron_expression is required for recurring jobs")
        if not croniter.is_valid(payload.cron_expression):
            raise HTTPException(status_code=400, detail="Invalid cron expression")

        sched = models.ScheduledJob(
            queue_id=queue.id,
            task_name=payload.task_name,
            payload=payload.payload,
            cron_expression=payload.cron_expression,
            next_run_at=croniter(payload.cron_expression, datetime.utcnow()).get_next(datetime),
        )
        db.add(sched)
        db.commit()
        # For a RECURRING request we return the *definition* wrapped as a
        # not-yet-materialized job-shaped response for a consistent API.
        raise HTTPException(
            status_code=202,
            detail=f"Recurring schedule created (id={sched.id}); first job will be materialized at {sched.next_run_at.isoformat()}",
        )

    try:
        job = job_service.create_job(
            db,
            queue=queue,
            task_name=payload.task_name,
            payload=payload.payload,
            job_type=payload.job_type,
            priority=payload.priority,
            run_at=payload.run_at,
            max_retries=payload.max_retries,
            idempotency_key=payload.idempotency_key,
            batch_id=payload.batch_id,
            depends_on=payload.depends_on,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return job


@router.post("/jobs/batch", response_model=list[schemas.JobOut], status_code=201)
def create_batch_jobs(
    payload: schemas.BatchJobCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    queue = _get_queue_or_404(db, payload.queue_id)
    batch_id = str(uuid.uuid4())
    jobs = [
        job_service.create_job(
            db,
            queue=queue,
            task_name=payload.task_name,
            payload=p,
            job_type=models.JobType.BATCH,
            priority=payload.priority,
            max_retries=payload.max_retries,
            batch_id=batch_id,
        )
        for p in payload.payloads
    ]
    return jobs


@router.get("/jobs", response_model=list[schemas.JobOut])
def list_jobs(
    queue_id: str | None = None,
    status: models.JobStatus | None = None,
    job_type: models.JobType | None = None,
    batch_id: str | None = None,
    limit: int = Query(default=50, le=200),
    offset: int = 0,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    query = db.query(models.Job)
    if queue_id:
        query = query.filter(models.Job.queue_id == queue_id)
    if status:
        query = query.filter(models.Job.status == status)
    if job_type:
        query = query.filter(models.Job.job_type == job_type)
    if batch_id:
        query = query.filter(models.Job.batch_id == batch_id)

    return query.order_by(models.Job.created_at.desc()).offset(offset).limit(limit).all()


@router.get("/jobs/{job_id}", response_model=schemas.JobOut)
def get_job(job_id: str, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/jobs/{job_id}/executions", response_model=list[schemas.JobExecutionOut])
def get_job_executions(job_id: str, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    return db.query(models.JobExecution).filter(models.JobExecution.job_id == job_id).order_by(
        models.JobExecution.attempt_number.asc()
    ).all()


@router.get("/jobs/{job_id}/logs", response_model=list[schemas.JobLogOut])
def get_job_logs(job_id: str, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    return db.query(models.JobLog).filter(models.JobLog.job_id == job_id).order_by(
        models.JobLog.created_at.asc()
    ).all()


@router.post("/jobs/{job_id}/cancel", response_model=schemas.JobOut)
def cancel_job(job_id: str, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status in (models.JobStatus.COMPLETED, models.JobStatus.RUNNING):
        raise HTTPException(status_code=400, detail=f"Cannot cancel a job in status '{job.status.value}'")
    job.status = models.JobStatus.DEAD
    db.add(models.JobLog(job_id=job.id, level="INFO", message="Job cancelled by user"))
    db.commit()
    db.refresh(job)
    return job


@router.post("/jobs/{job_id}/retry", response_model=schemas.JobOut)
def manual_retry_job(job_id: str, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    """Manually force-requeue a failed/dead job, resetting its attempt count."""
    job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    job.status = models.JobStatus.QUEUED
    job.attempt_count = 0
    job.run_at = datetime.utcnow()
    job.claimed_by_worker_id = None
    job.error_message = None
    db.add(models.JobLog(job_id=job.id, level="INFO", message="Manually retried by user"))
    db.commit()
    db.refresh(job)
    return job


# ---------------- Worker execution callbacks ----------------
# These endpoints are called by worker processes (not end users), so they
# are intentionally not behind get_current_user - in production they'd be
# authenticated with a worker/service token instead. See DESIGN_DECISIONS.md.

@router.post("/jobs/{job_id}/start", response_model=schemas.JobExecutionOut)
def start_job(job_id: str, worker_id: str, db: Session = Depends(get_db)):
    job = db.query(models.Job).filter(models.Job.id == job_id).first()
    worker = db.query(models.Worker).filter(models.Worker.id == worker_id).first()
    if not job or not worker:
        raise HTTPException(status_code=404, detail="Job or worker not found")
    return job_service.mark_running(db, job, worker)


@router.post("/jobs/{job_id}/complete", response_model=schemas.JobOut)
def complete_job(job_id: str, payload: schemas.JobResultIn, db: Session = Depends(get_db)):
    job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    execution = db.query(models.JobExecution).filter(
        models.JobExecution.job_id == job_id
    ).order_by(models.JobExecution.attempt_number.desc()).first()
    if not execution:
        raise HTTPException(status_code=400, detail="No execution in progress for this job")
    job_service.mark_completed(db, job, execution, payload.result)
    db.refresh(job)
    return job


@router.post("/jobs/{job_id}/fail", response_model=schemas.JobOut)
def fail_job(job_id: str, payload: schemas.JobErrorIn, db: Session = Depends(get_db)):
    job = db.query(models.Job).filter(models.Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    execution = db.query(models.JobExecution).filter(
        models.JobExecution.job_id == job_id
    ).order_by(models.JobExecution.attempt_number.desc()).first()
    if not execution:
        raise HTTPException(status_code=400, detail="No execution in progress for this job")
    job_service.mark_failed(db, job, execution, payload.error)
    db.refresh(job)
    return job


# ---------------- Dead Letter Queue ----------------

@router.get("/dead-letter-queue", response_model=list[schemas.DLQEntryOut])
def list_dlq(
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return db.query(models.DeadLetterQueueEntry).order_by(
        models.DeadLetterQueueEntry.moved_at.desc()
    ).offset(offset).limit(limit).all()


@router.post("/dead-letter-queue/{entry_id}/replay", response_model=schemas.JobOut)
def replay_dlq(
    entry_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_role(models.UserRole.ADMIN)),
):
    entry = db.query(models.DeadLetterQueueEntry).filter(models.DeadLetterQueueEntry.id == entry_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="DLQ entry not found")
    return job_service.replay_dlq_entry(db, entry)
