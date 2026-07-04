from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, EmailStr, Field

from app.models import JobStatus, JobType, RetryStrategy, UserRole, WorkerStatus


# ---------------- Auth ----------------

class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    full_name: Optional[str] = None
    # Self-selectable at registration for demo/grading convenience only; in
    # a real product this would be assigned by an existing admin, not
    # chosen by the registering user. Defaults to OPERATOR.
    role: UserRole = UserRole.OPERATOR


class UserOut(BaseModel):
    id: str
    email: EmailStr
    full_name: Optional[str] = None
    role: UserRole
    created_at: datetime

    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ---------------- Organization / Project ----------------

class OrganizationCreate(BaseModel):
    name: str


class OrganizationOut(BaseModel):
    id: str
    name: str
    created_at: datetime

    class Config:
        from_attributes = True


class ProjectCreate(BaseModel):
    name: str
    organization_id: str


class ProjectOut(BaseModel):
    id: str
    name: str
    organization_id: str
    api_key: str
    created_at: datetime

    class Config:
        from_attributes = True


# ---------------- Retry Policy ----------------

class RetryPolicyCreate(BaseModel):
    name: str
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    max_retries: int = 3
    base_delay_seconds: int = 5
    max_delay_seconds: int = 3600


class RetryPolicyOut(BaseModel):
    id: str
    name: str
    strategy: RetryStrategy
    max_retries: int
    base_delay_seconds: int
    max_delay_seconds: int

    class Config:
        from_attributes = True


# ---------------- Queue ----------------

class QueueCreate(BaseModel):
    name: str
    project_id: str
    priority: int = 0
    concurrency_limit: int = 5
    retry_policy_id: Optional[str] = None
    shard_count: int = 1


class QueueUpdate(BaseModel):
    priority: Optional[int] = None
    concurrency_limit: Optional[int] = None
    is_paused: Optional[bool] = None
    retry_policy_id: Optional[str] = None
    shard_count: Optional[int] = None


class QueueOut(BaseModel):
    id: str
    name: str
    project_id: str
    priority: int
    concurrency_limit: int
    is_paused: bool
    retry_policy_id: Optional[str]
    shard_count: int
    created_at: datetime

    class Config:
        from_attributes = True


class QueueStats(BaseModel):
    queue_id: str
    queue_name: str
    queued: int
    scheduled: int
    running: int
    completed: int
    failed: int
    dead: int
    is_paused: bool
    concurrency_limit: int


# ---------------- Jobs ----------------

class JobCreate(BaseModel):
    queue_id: str
    task_name: str
    payload: dict[str, Any] = Field(default_factory=dict)
    job_type: JobType = JobType.IMMEDIATE
    priority: int = 0
    run_at: Optional[datetime] = None          # for DELAYED / SCHEDULED
    cron_expression: Optional[str] = None        # for RECURRING
    max_retries: Optional[int] = None
    idempotency_key: Optional[str] = None
    batch_id: Optional[str] = None
    depends_on: Optional[list[str]] = None       # job ids that must COMPLETE first


class BatchJobCreate(BaseModel):
    queue_id: str
    task_name: str
    payloads: list[dict[str, Any]]
    priority: int = 0
    max_retries: Optional[int] = None


class JobOut(BaseModel):
    id: str
    queue_id: str
    job_type: JobType
    task_name: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    run_at: datetime
    cron_expression: Optional[str]
    batch_id: Optional[str]
    attempt_count: int
    max_retries: int
    shard_id: int
    claimed_by_worker_id: Optional[str]
    result: Optional[dict[str, Any]]
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]

    class Config:
        from_attributes = True


class JobExecutionOut(BaseModel):
    id: str
    job_id: str
    worker_id: Optional[str]
    attempt_number: int
    status: JobStatus
    started_at: datetime
    finished_at: Optional[datetime]
    duration_ms: Optional[int]
    result: Optional[dict[str, Any]]
    error_message: Optional[str]

    class Config:
        from_attributes = True


class JobLogOut(BaseModel):
    id: str
    level: str
    message: str
    created_at: datetime

    class Config:
        from_attributes = True


# ---------------- Worker execution callbacks ----------------

class JobResultIn(BaseModel):
    result: dict[str, Any] = Field(default_factory=dict)


class JobErrorIn(BaseModel):
    error: str


# ---------------- Workers ----------------

class WorkerRegister(BaseModel):
    name: str
    concurrency: int = 4
    queues: list[str] = Field(default_factory=list)
    shard_id: Optional[int] = None  # None = claims from every shard


class WorkerHeartbeatIn(BaseModel):
    active_jobs: int = 0
    cpu_percent: Optional[int] = None
    memory_mb: Optional[int] = None


class WorkerOut(BaseModel):
    id: str
    name: str
    status: WorkerStatus
    concurrency: int
    queues: list[str]
    shard_id: Optional[int]
    started_at: datetime
    last_seen_at: datetime

    class Config:
        from_attributes = True


# ---------------- Dead Letter Queue ----------------

class DLQEntryOut(BaseModel):
    id: str
    job_id: str
    reason: str
    ai_summary: Optional[str] = None
    moved_at: datetime
    replayed: bool

    class Config:
        from_attributes = True


# ---------------- Dashboard / metrics ----------------

class SystemHealth(BaseModel):
    total_workers: int
    active_workers: int
    offline_workers: int
    total_queues: int
    paused_queues: int
    jobs_queued: int
    jobs_running: int
    jobs_completed_last_hour: int
    jobs_failed_last_hour: int
    dlq_size: int
