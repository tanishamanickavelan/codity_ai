from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import models, schemas
from app.auth import get_current_user, require_role
from app.config import settings
from app.database import get_db
from app.services import job_service

router = APIRouter(prefix="/api", tags=["workers"])


@router.post("/workers/register", response_model=schemas.WorkerOut, status_code=201)
def register_worker(payload: schemas.WorkerRegister, db: Session = Depends(get_db)):
    """
    No auth required here on purpose: workers authenticate to the queue via
    a project API key in a real deployment (see docs/DESIGN_DECISIONS.md);
    simplified here so the worker process can self-register out of the box.
    """
    worker = models.Worker(name=payload.name, concurrency=payload.concurrency, queues=payload.queues,
                            shard_id=payload.shard_id, status=models.WorkerStatus.IDLE)
    db.add(worker)
    db.commit()
    db.refresh(worker)
    return worker


@router.post("/workers/{worker_id}/heartbeat", response_model=schemas.WorkerOut)
def heartbeat(worker_id: str, payload: schemas.WorkerHeartbeatIn, db: Session = Depends(get_db)):
    worker = db.query(models.Worker).filter(models.Worker.id == worker_id).first()
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")

    now = datetime.utcnow()
    worker.last_seen_at = now
    worker.status = models.WorkerStatus.ACTIVE if payload.active_jobs > 0 else models.WorkerStatus.IDLE

    db.add(models.WorkerHeartbeat(
        worker_id=worker.id, sent_at=now, active_jobs=payload.active_jobs,
        cpu_percent=payload.cpu_percent, memory_mb=payload.memory_mb,
    ))
    db.commit()
    db.refresh(worker)
    return worker


@router.post("/workers/{worker_id}/drain", response_model=schemas.WorkerOut)
def drain_worker(
    worker_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_role(models.UserRole.ADMIN)),
):
    """Signal a worker to stop claiming new jobs and shut down gracefully."""
    worker = db.query(models.Worker).filter(models.Worker.id == worker_id).first()
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
    worker.status = models.WorkerStatus.DRAINING
    db.commit()
    db.refresh(worker)
    return worker


@router.post("/workers/{worker_id}/claim", response_model=schemas.JobOut | None)
def claim_job(worker_id: str, db: Session = Depends(get_db)):
    """Called by the worker process's poll loop to atomically grab the next job."""
    worker = db.query(models.Worker).filter(models.Worker.id == worker_id).first()
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
    if worker.status == models.WorkerStatus.DRAINING:
        return None

    job = job_service.claim_next_job(db, worker, queue_ids=worker.queues or None, shard_id=worker.shard_id)
    return job


@router.get("/workers", response_model=list[schemas.WorkerOut])
def list_workers(db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    workers = db.query(models.Worker).order_by(models.Worker.started_at.desc()).all()
    # Mark stale workers OFFLINE for display purposes (missed heartbeat).
    cutoff = datetime.utcnow() - timedelta(seconds=settings.WORKER_HEARTBEAT_TIMEOUT_SECONDS)
    changed = False
    for w in workers:
        if w.last_seen_at < cutoff and w.status != models.WorkerStatus.OFFLINE:
            w.status = models.WorkerStatus.OFFLINE
            changed = True
    if changed:
        db.commit()
    return workers
