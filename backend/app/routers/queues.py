from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import models, schemas
from app.auth import get_current_user, require_role
from app.database import get_db

router = APIRouter(prefix="/api", tags=["queues"])


def _project_owned_by(db: Session, project_id: str, user: models.User) -> models.Project:
    project = db.query(models.Project).join(models.Organization).filter(
        models.Project.id == project_id,
        models.Organization.owner_id == user.id,
    ).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


# ---------------- Retry Policies ----------------

@router.post("/retry-policies", response_model=schemas.RetryPolicyOut, status_code=201)
def create_retry_policy(
    payload: schemas.RetryPolicyCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    policy = models.RetryPolicy(**payload.model_dump())
    db.add(policy)
    db.commit()
    db.refresh(policy)
    return policy


@router.get("/retry-policies", response_model=list[schemas.RetryPolicyOut])
def list_retry_policies(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return db.query(models.RetryPolicy).all()


# ---------------- Queues ----------------

@router.post("/queues", response_model=schemas.QueueOut, status_code=201)
def create_queue(
    payload: schemas.QueueCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _project_owned_by(db, payload.project_id, current_user)

    existing = db.query(models.Queue).filter(
        models.Queue.project_id == payload.project_id, models.Queue.name == payload.name
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Queue name already exists in this project")

    queue = models.Queue(**payload.model_dump())
    db.add(queue)
    db.commit()
    db.refresh(queue)
    return queue


@router.get("/queues", response_model=list[schemas.QueueOut])
def list_queues(
    project_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    query = db.query(models.Queue).join(models.Project).join(models.Organization).filter(
        models.Organization.owner_id == current_user.id
    )
    if project_id:
        query = query.filter(models.Queue.project_id == project_id)
    return query.offset(offset).limit(limit).all()


@router.get("/queues/{queue_id}", response_model=schemas.QueueOut)
def get_queue(
    queue_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    queue = db.query(models.Queue).filter(models.Queue.id == queue_id).first()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    return queue


@router.patch("/queues/{queue_id}", response_model=schemas.QueueOut)
def update_queue(
    queue_id: str,
    payload: schemas.QueueUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    queue = db.query(models.Queue).filter(models.Queue.id == queue_id).first()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")

    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(queue, field, value)

    db.commit()
    db.refresh(queue)
    return queue


@router.post("/queues/{queue_id}/pause", response_model=schemas.QueueOut)
def pause_queue(
    queue_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_role(models.UserRole.ADMIN)),
):
    queue = db.query(models.Queue).filter(models.Queue.id == queue_id).first()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    queue.is_paused = True
    db.commit()
    db.refresh(queue)
    return queue


@router.post("/queues/{queue_id}/resume", response_model=schemas.QueueOut)
def resume_queue(
    queue_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_role(models.UserRole.ADMIN)),
):
    queue = db.query(models.Queue).filter(models.Queue.id == queue_id).first()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")
    queue.is_paused = False
    db.commit()
    db.refresh(queue)
    return queue


@router.get("/queues/{queue_id}/stats", response_model=schemas.QueueStats)
def queue_stats(queue_id: str, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    queue = db.query(models.Queue).filter(models.Queue.id == queue_id).first()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found")

    def count(status: models.JobStatus) -> int:
        return db.query(models.Job).filter(models.Job.queue_id == queue_id, models.Job.status == status).count()

    return schemas.QueueStats(
        queue_id=queue.id,
        queue_name=queue.name,
        queued=count(models.JobStatus.QUEUED),
        scheduled=count(models.JobStatus.SCHEDULED),
        running=count(models.JobStatus.RUNNING) + count(models.JobStatus.CLAIMED),
        completed=count(models.JobStatus.COMPLETED),
        failed=count(models.JobStatus.FAILED),
        dead=count(models.JobStatus.DEAD),
        is_paused=queue.is_paused,
        concurrency_limit=queue.concurrency_limit,
    )
