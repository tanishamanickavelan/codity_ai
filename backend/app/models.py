"""
SQLAlchemy models for the Distributed Job Scheduler.

Schema overview (see docs/ER_DIAGRAM.md for the full diagram + rationale):

Organization 1---* Project 1---* Queue 1---* Job 1---* JobExecution
User *---* Organization (via membership, simplified to owner_id here)
Queue 1---1 RetryPolicy (default policy; jobs may override)
Job 1---* JobLog
Job 1---1 DeadLetterQueueEntry (only when permanently failed)
Job 1---1 ScheduledJob (only for recurring/cron jobs)
Worker 1---* WorkerHeartbeat
Worker 1---* JobExecution (a worker executes many job executions)
"""
import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import relationship

from app.database import Base


def gen_uuid() -> str:
    return str(uuid.uuid4())


# --------------------------------------------------------------------------
# Enums
# --------------------------------------------------------------------------

class JobStatus(str, enum.Enum):
    SCHEDULED = "scheduled"   # waiting for its scheduled/delayed run_at time
    BLOCKED = "blocked"       # waiting on unfinished job dependencies
    QUEUED = "queued"         # ready to be claimed by any worker now
    CLAIMED = "claimed"       # a worker has atomically claimed it, not yet running
    RUNNING = "running"       # actively executing on a worker
    COMPLETED = "completed"   # terminal success state
    FAILED = "failed"         # a single attempt failed; may be retried
    RETRYING = "retrying"     # waiting for backoff delay before requeue
    DEAD = "dead"             # exhausted retries -> moved to Dead Letter Queue


class RetryStrategy(str, enum.Enum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class JobType(str, enum.Enum):
    IMMEDIATE = "immediate"
    DELAYED = "delayed"
    SCHEDULED = "scheduled"
    RECURRING = "recurring"
    BATCH = "batch"


class WorkerStatus(str, enum.Enum):
    ACTIVE = "active"
    IDLE = "idle"
    DRAINING = "draining"   # graceful shutdown in progress
    OFFLINE = "offline"     # missed heartbeat timeout


class UserRole(str, enum.Enum):
    """
    Simplified single-tenant RBAC: a global role per user rather than a full
    per-organization membership model. Sufficient to demonstrate the pattern
    (viewer vs operator vs admin permissions) without the complexity of a
    many-to-many membership table - see docs/DESIGN_DECISIONS.md.
    """
    ADMIN = "admin"       # can pause/resume queues, replay DLQ, drain workers
    OPERATOR = "operator"  # can create/manage jobs and queues
    VIEWER = "viewer"     # read-only access


# --------------------------------------------------------------------------
# Users / Organizations / Projects
# --------------------------------------------------------------------------

class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=gen_uuid)
    email = Column(String, unique=True, nullable=False, index=True)
    hashed_password = Column(String, nullable=False)
    full_name = Column(String, nullable=True)
    role = Column(Enum(UserRole), default=UserRole.OPERATOR, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    organizations = relationship("Organization", back_populates="owner")


class Organization(Base):
    __tablename__ = "organizations"

    id = Column(String, primary_key=True, default=gen_uuid)
    name = Column(String, nullable=False)
    owner_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    owner = relationship("User", back_populates="organizations")
    projects = relationship("Project", back_populates="organization", cascade="all, delete-orphan")


class Project(Base):
    __tablename__ = "projects"

    id = Column(String, primary_key=True, default=gen_uuid)
    name = Column(String, nullable=False)
    organization_id = Column(String, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    api_key = Column(String, unique=True, default=gen_uuid, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    organization = relationship("Organization", back_populates="projects")
    queues = relationship("Queue", back_populates="project", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("organization_id", "name", name="uq_project_name_per_org"),
    )


# --------------------------------------------------------------------------
# Queues & Retry Policies
# --------------------------------------------------------------------------

class RetryPolicy(Base):
    __tablename__ = "retry_policies"

    id = Column(String, primary_key=True, default=gen_uuid)
    name = Column(String, nullable=False)
    strategy = Column(Enum(RetryStrategy), default=RetryStrategy.EXPONENTIAL, nullable=False)
    max_retries = Column(Integer, default=3, nullable=False)
    base_delay_seconds = Column(Integer, default=5, nullable=False)
    max_delay_seconds = Column(Integer, default=3600, nullable=False)

    queues = relationship("Queue", back_populates="retry_policy")


class Queue(Base):
    __tablename__ = "queues"

    id = Column(String, primary_key=True, default=gen_uuid)
    name = Column(String, nullable=False)
    project_id = Column(String, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)

    priority = Column(Integer, default=0, nullable=False)  # higher = served first
    concurrency_limit = Column(Integer, default=5, nullable=False)  # max jobs running at once
    is_paused = Column(Boolean, default=False, nullable=False)

    # Sharding: splits this queue's jobs across N logical shards so that
    # dedicated worker pools can each own a shard (e.g. shard 0 handled by
    # workers in region A, shard 1 by region B) instead of all workers
    # contending for every job. shard_count=1 (default) means "no sharding".
    shard_count = Column(Integer, default=1, nullable=False)

    retry_policy_id = Column(String, ForeignKey("retry_policies.id"), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    project = relationship("Project", back_populates="queues")
    retry_policy = relationship("RetryPolicy", back_populates="queues")
    jobs = relationship("Job", back_populates="queue", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("project_id", "name", name="uq_queue_name_per_project"),
        Index("ix_queue_project_priority", "project_id", "priority"),
    )


# --------------------------------------------------------------------------
# Jobs
# --------------------------------------------------------------------------

class Job(Base):
    __tablename__ = "jobs"

    id = Column(String, primary_key=True, default=gen_uuid)
    queue_id = Column(String, ForeignKey("queues.id", ondelete="CASCADE"), nullable=False, index=True)

    job_type = Column(Enum(JobType), default=JobType.IMMEDIATE, nullable=False)
    task_name = Column(String, nullable=False)  # maps to a handler in the task registry
    payload = Column(JSON, default=dict, nullable=False)

    status = Column(Enum(JobStatus), default=JobStatus.QUEUED, nullable=False, index=True)
    priority = Column(Integer, default=0, nullable=False)

    # scheduling
    run_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    cron_expression = Column(String, nullable=True)  # only for RECURRING jobs
    batch_id = Column(String, nullable=True, index=True)  # groups jobs created together

    # retry bookkeeping
    retry_policy_id = Column(String, ForeignKey("retry_policies.id"), nullable=True)
    attempt_count = Column(Integer, default=0, nullable=False)
    max_retries = Column(Integer, default=3, nullable=False)

    # claim bookkeeping (idempotency + atomic claim)
    idempotency_key = Column(String, nullable=True, index=True)
    claimed_by_worker_id = Column(String, ForeignKey("workers.id"), nullable=True)
    claimed_at = Column(DateTime, nullable=True)

    # Sharding: assigned at creation as hash(job.id) % queue.shard_count.
    # A worker registered with a specific --shard only claims jobs whose
    # shard_id matches; a worker with no --shard claims from every shard.
    shard_id = Column(Integer, default=0, nullable=False, index=True)

    result = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)

    queue = relationship("Queue", back_populates="jobs")
    retry_policy = relationship("RetryPolicy")
    executions = relationship("JobExecution", back_populates="job", cascade="all, delete-orphan")
    logs = relationship("JobLog", back_populates="job", cascade="all, delete-orphan")
    dlq_entry = relationship("DeadLetterQueueEntry", back_populates="job", uselist=False,
                              cascade="all, delete-orphan")
    dependencies = relationship("JobDependency", foreign_keys="JobDependency.job_id", cascade="all, delete-orphan")
    dependents = relationship("JobDependency", foreign_keys="JobDependency.depends_on_job_id")

    __table_args__ = (
        # Speeds up the atomic-claim query: "give me the next queued job for
        # this queue, ordered by priority then run_at".
        Index("ix_job_claim_lookup", "queue_id", "status", "priority", "run_at"),
        UniqueConstraint("queue_id", "idempotency_key", name="uq_job_idempotency_per_queue"),
    )


class JobExecution(Base):
    """One row per attempt. Job.attempt_count mirrors len(executions)."""
    __tablename__ = "job_executions"

    id = Column(String, primary_key=True, default=gen_uuid)
    job_id = Column(String, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    worker_id = Column(String, ForeignKey("workers.id"), nullable=True, index=True)

    attempt_number = Column(Integer, nullable=False)
    status = Column(Enum(JobStatus), nullable=False)

    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    finished_at = Column(DateTime, nullable=True)
    duration_ms = Column(Integer, nullable=True)

    result = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)

    job = relationship("Job", back_populates="executions")
    worker = relationship("Worker", back_populates="executions")


class JobLog(Base):
    """Structured, append-only log lines for a job (visible in the dashboard)."""
    __tablename__ = "job_logs"

    id = Column(String, primary_key=True, default=gen_uuid)
    job_id = Column(String, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    level = Column(String, default="INFO", nullable=False)
    message = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    job = relationship("Job", back_populates="logs")


class DeadLetterQueueEntry(Base):
    __tablename__ = "dead_letter_queue"

    id = Column(String, primary_key=True, default=gen_uuid)
    job_id = Column(String, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    reason = Column(Text, nullable=False)
    final_payload = Column(JSON, nullable=True)
    # Human-readable failure summary. Generated by
    # app/services/failure_summary.py - a lightweight heuristic classifier
    # by default, but designed to be swapped for a real LLM call (Claude/
    # OpenAI) with no change to callers - see that module's docstring.
    ai_summary = Column(Text, nullable=True)
    moved_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    replayed = Column(Boolean, default=False, nullable=False)

    job = relationship("Job", back_populates="dlq_entry")


# --------------------------------------------------------------------------
# Workers
# --------------------------------------------------------------------------

class Worker(Base):
    __tablename__ = "workers"

    id = Column(String, primary_key=True, default=gen_uuid)
    name = Column(String, nullable=False)
    status = Column(Enum(WorkerStatus), default=WorkerStatus.IDLE, nullable=False)
    concurrency = Column(Integer, default=4, nullable=False)  # max jobs this worker runs at once
    queues = Column(JSON, default=list, nullable=False)  # list of queue_ids it polls; [] = all
    shard_id = Column(Integer, nullable=True)  # None = claims from every shard
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    heartbeats = relationship("WorkerHeartbeat", back_populates="worker", cascade="all, delete-orphan")
    executions = relationship("JobExecution", back_populates="worker")


class WorkerHeartbeat(Base):
    __tablename__ = "worker_heartbeats"

    id = Column(String, primary_key=True, default=gen_uuid)
    worker_id = Column(String, ForeignKey("workers.id", ondelete="CASCADE"), nullable=False, index=True)
    sent_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    active_jobs = Column(Integer, default=0, nullable=False)
    cpu_percent = Column(Integer, nullable=True)
    memory_mb = Column(Integer, nullable=True)

    worker = relationship("Worker", back_populates="heartbeats")


# --------------------------------------------------------------------------
# Recurring / scheduled job definitions
# --------------------------------------------------------------------------

class ScheduledJob(Base):
    """
    Template for recurring (cron) jobs. The scheduler process periodically
    checks these and materializes a concrete Job row when `next_run_at` is
    reached, then advances `next_run_at` using croniter.
    """
    __tablename__ = "scheduled_jobs"

    id = Column(String, primary_key=True, default=gen_uuid)
    queue_id = Column(String, ForeignKey("queues.id", ondelete="CASCADE"), nullable=False, index=True)
    task_name = Column(String, nullable=False)
    payload = Column(JSON, default=dict, nullable=False)
    cron_expression = Column(String, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    next_run_at = Column(DateTime, nullable=False, index=True)
    last_run_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


# --------------------------------------------------------------------------
# Workflow dependencies (bonus feature)
# --------------------------------------------------------------------------

class JobDependency(Base):
    """
    Directed edge: `job_id` cannot run until `depends_on_job_id` reaches
    COMPLETED. A job with one or more unmet dependencies is created in
    BLOCKED status (see job_service.create_job) rather than QUEUED/SCHEDULED,
    so the atomic claim query never has to know about dependencies at all -
    it only ever sees QUEUED jobs. When a job completes, job_service walks
    its dependents and promotes any whose dependencies are now all satisfied
    (see job_service.promote_ready_dependents) - this is the "event-driven"
    half of the workflow-dependencies + event-driven-execution bonus pair:
    a completion event immediately triggers dependent evaluation instead of
    waiting for the next scheduler poll tick.
    """
    __tablename__ = "job_dependencies"

    id = Column(String, primary_key=True, default=gen_uuid)
    job_id = Column(String, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    depends_on_job_id = Column(String, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint("job_id", "depends_on_job_id", name="uq_job_dependency_edge"),
    )


# --------------------------------------------------------------------------
# Distributed locking (bonus feature)
# --------------------------------------------------------------------------

class SchedulerLock(Base):
    """
    A single-row leader-election lock. If you run more than one scheduler
    process (for redundancy), only the one holding this lock promotes
    scheduled/recurring jobs and reaps stale workers on a given tick -
    otherwise two schedulers could double-materialize the same recurring
    job. Acquired via the same atomic-UPDATE pattern as job claiming (see
    scheduler_service.try_acquire_lock): whoever's UPDATE affects a row
    wins the lock for `lease_seconds`; if that instance dies, the lease
    expires and another scheduler instance can take over.
    """
    __tablename__ = "scheduler_locks"

    id = Column(String, primary_key=True, default="scheduler")  # single well-known row
    holder_id = Column(String, nullable=True)
    acquired_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=True)
