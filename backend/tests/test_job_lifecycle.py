from datetime import datetime, timedelta

from app import models
from app.services import job_service


def _make_queue(db_session, concurrency_limit=5, max_retries=2):
    policy = models.RetryPolicy(
        name="fast-fixed", strategy=models.RetryStrategy.FIXED,
        max_retries=max_retries, base_delay_seconds=0, max_delay_seconds=60,
    )
    db_session.add(policy)
    db_session.commit()

    user = models.User(email="owner@example.com", hashed_password="x")
    db_session.add(user)
    db_session.commit()

    org = models.Organization(name="Acme", owner_id=user.id)
    db_session.add(org)
    db_session.commit()

    project = models.Project(name="Proj", organization_id=org.id)
    db_session.add(project)
    db_session.commit()

    queue = models.Queue(
        name="default", project_id=project.id, concurrency_limit=concurrency_limit,
        retry_policy_id=policy.id,
    )
    db_session.add(queue)
    db_session.commit()
    db_session.refresh(queue)
    return queue


def _make_worker(db_session, name="w1"):
    worker = models.Worker(name=name)
    db_session.add(worker)
    db_session.commit()
    db_session.refresh(worker)
    return worker


def test_immediate_job_starts_queued(db_session):
    queue = _make_queue(db_session)
    job = job_service.create_job(db_session, queue=queue, task_name="noop", payload={})
    assert job.status == models.JobStatus.QUEUED


def test_future_run_at_starts_scheduled(db_session):
    queue = _make_queue(db_session)
    job = job_service.create_job(
        db_session, queue=queue, task_name="noop", payload={},
        run_at=datetime.utcnow() + timedelta(hours=1),
    )
    assert job.status == models.JobStatus.SCHEDULED


def test_claim_is_exclusive_between_two_workers(db_session):
    """The core atomicity guarantee: only one worker can claim a given job."""
    queue = _make_queue(db_session)
    job_service.create_job(db_session, queue=queue, task_name="noop", payload={})

    worker_a = _make_worker(db_session, "worker-a")
    worker_b = _make_worker(db_session, "worker-b")

    claimed_a = job_service.claim_next_job(db_session, worker_a)
    claimed_b = job_service.claim_next_job(db_session, worker_b)

    assert claimed_a is not None
    assert claimed_b is None  # nothing left to claim
    assert claimed_a.claimed_by_worker_id == worker_a.id
    assert claimed_a.status == models.JobStatus.CLAIMED


def test_claim_respects_queue_concurrency_limit(db_session):
    queue = _make_queue(db_session, concurrency_limit=1)
    job_service.create_job(db_session, queue=queue, task_name="noop", payload={})
    job_service.create_job(db_session, queue=queue, task_name="noop", payload={})

    worker = _make_worker(db_session)
    first = job_service.claim_next_job(db_session, worker)
    second = job_service.claim_next_job(db_session, worker)

    assert first is not None
    assert second is None  # blocked by concurrency_limit=1 until first finishes


def test_failed_job_retries_then_moves_to_dlq(db_session):
    queue = _make_queue(db_session, max_retries=2)
    job = job_service.create_job(db_session, queue=queue, task_name="noop", payload={})
    worker = _make_worker(db_session)

    # Attempt 1: fails, should retry (goes back to QUEUED).
    claimed = job_service.claim_next_job(db_session, worker)
    execution = job_service.mark_running(db_session, claimed, worker)
    job_service.mark_failed(db_session, claimed, execution, "boom")
    db_session.refresh(claimed)
    assert claimed.status == models.JobStatus.QUEUED
    assert claimed.attempt_count == 1

    # Force run_at into the past so it's claimable again immediately.
    claimed.run_at = datetime.utcnow()
    db_session.commit()

    # Attempt 2: fails again, exhausts max_retries=2 -> DEAD + DLQ entry.
    claimed2 = job_service.claim_next_job(db_session, worker)
    execution2 = job_service.mark_running(db_session, claimed2, worker)
    job_service.mark_failed(db_session, claimed2, execution2, "boom again")
    db_session.refresh(claimed2)

    assert claimed2.status == models.JobStatus.DEAD
    assert claimed2.attempt_count == 2
    assert claimed2.dlq_entry is not None


def test_completed_job_stores_result(db_session):
    queue = _make_queue(db_session)
    job = job_service.create_job(db_session, queue=queue, task_name="noop", payload={})
    worker = _make_worker(db_session)

    claimed = job_service.claim_next_job(db_session, worker)
    execution = job_service.mark_running(db_session, claimed, worker)
    job_service.mark_completed(db_session, claimed, execution, {"ok": True})
    db_session.refresh(claimed)

    assert claimed.status == models.JobStatus.COMPLETED
    assert claimed.result == {"ok": True}
    assert claimed.completed_at is not None
