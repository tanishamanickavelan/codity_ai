from datetime import datetime, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app import models, schemas
from app.auth import get_current_user
from app.config import settings
from app.database import get_db

router = APIRouter(prefix="/api", tags=["dashboard"])


def _compute_health(db: Session) -> dict:
    """
    Shared by GET /api/dashboard/health and the /ws/dashboard websocket
    broadcaster (app/routers/websocket.py) so both surfaces always agree.
    """
    now = datetime.utcnow()
    hour_ago = now - timedelta(hours=1)
    offline_cutoff = now - timedelta(seconds=settings.WORKER_HEARTBEAT_TIMEOUT_SECONDS)

    total_workers = db.query(models.Worker).count()
    active_workers = db.query(models.Worker).filter(models.Worker.last_seen_at >= offline_cutoff).count()
    offline_workers = total_workers - active_workers

    total_queues = db.query(models.Queue).count()
    paused_queues = db.query(models.Queue).filter(models.Queue.is_paused.is_(True)).count()

    jobs_queued = db.query(models.Job).filter(models.Job.status == models.JobStatus.QUEUED).count()
    jobs_running = db.query(models.Job).filter(
        models.Job.status.in_([models.JobStatus.RUNNING, models.JobStatus.CLAIMED])
    ).count()
    jobs_completed_last_hour = db.query(models.Job).filter(
        models.Job.status == models.JobStatus.COMPLETED, models.Job.completed_at >= hour_ago
    ).count()
    jobs_failed_last_hour = db.query(models.JobExecution).filter(
        models.JobExecution.status == models.JobStatus.FAILED, models.JobExecution.finished_at >= hour_ago
    ).count()
    dlq_size = db.query(models.DeadLetterQueueEntry).filter(
        models.DeadLetterQueueEntry.replayed.is_(False)
    ).count()

    return {
        "total_workers": total_workers,
        "active_workers": active_workers,
        "offline_workers": offline_workers,
        "total_queues": total_queues,
        "paused_queues": paused_queues,
        "jobs_queued": jobs_queued,
        "jobs_running": jobs_running,
        "jobs_completed_last_hour": jobs_completed_last_hour,
        "jobs_failed_last_hour": jobs_failed_last_hour,
        "dlq_size": dlq_size,
    }


@router.get("/dashboard/health", response_model=schemas.SystemHealth)
def system_health(db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    return schemas.SystemHealth(**_compute_health(db))


@router.get("/dashboard/throughput")
def throughput(hours: int = 6, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    """
    Bucketed completed/failed counts per hour for the last `hours` hours,
    used to render the dashboard's throughput chart.
    """
    now = datetime.utcnow()
    buckets = []
    for i in range(hours - 1, -1, -1):
        bucket_start = (now - timedelta(hours=i)).replace(minute=0, second=0, microsecond=0)
        bucket_end = bucket_start + timedelta(hours=1)
        completed = db.query(models.Job).filter(
            models.Job.status == models.JobStatus.COMPLETED,
            models.Job.completed_at >= bucket_start,
            models.Job.completed_at < bucket_end,
        ).count()
        failed = db.query(models.JobExecution).filter(
            models.JobExecution.status == models.JobStatus.FAILED,
            models.JobExecution.finished_at >= bucket_start,
            models.JobExecution.finished_at < bucket_end,
        ).count()
        buckets.append({
            "hour": bucket_start.strftime("%H:%M"),
            "completed": completed,
            "failed": failed,
        })
    return buckets
