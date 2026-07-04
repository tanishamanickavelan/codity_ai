from datetime import datetime, timedelta

from app import models
from app.services import job_service, scheduler_service
from app.services.failure_summary import generate_failure_summary
from tests.test_job_lifecycle import _make_queue, _make_worker


# ---------------- Workflow dependencies + event-driven execution ----------------

def test_job_with_unmet_dependency_starts_blocked(db_session):
    queue = _make_queue(db_session)
    upstream = job_service.create_job(db_session, queue=queue, task_name="a", payload={})
    downstream = job_service.create_job(db_session, queue=queue, task_name="b", payload={}, depends_on=[upstream.id])

    assert downstream.status == models.JobStatus.BLOCKED
    # A BLOCKED job must never be claimable.
    worker = _make_worker(db_session)
    claimed = job_service.claim_next_job(db_session, worker)
    assert claimed.id == upstream.id  # only the upstream job is claimable


def test_completing_dependency_promotes_blocked_job_immediately(db_session):
    queue = _make_queue(db_session)
    upstream = job_service.create_job(db_session, queue=queue, task_name="a", payload={})
    downstream = job_service.create_job(db_session, queue=queue, task_name="b", payload={}, depends_on=[upstream.id])
    worker = _make_worker(db_session)

    claimed = job_service.claim_next_job(db_session, worker)
    execution = job_service.mark_running(db_session, claimed, worker)
    job_service.mark_completed(db_session, claimed, execution, {"ok": True})

    db_session.refresh(downstream)
    assert downstream.status == models.JobStatus.QUEUED  # promoted event-drivenly, not by polling


def test_job_with_multiple_dependencies_waits_for_all(db_session):
    queue = _make_queue(db_session)
    dep1 = job_service.create_job(db_session, queue=queue, task_name="a", payload={})
    dep2 = job_service.create_job(db_session, queue=queue, task_name="b", payload={})
    downstream = job_service.create_job(db_session, queue=queue, task_name="c", payload={}, depends_on=[dep1.id, dep2.id])
    worker = _make_worker(db_session)

    claimed1 = job_service.claim_next_job(db_session, worker)
    exec1 = job_service.mark_running(db_session, claimed1, worker)
    job_service.mark_completed(db_session, claimed1, exec1, {})
    db_session.refresh(downstream)
    assert downstream.status == models.JobStatus.BLOCKED  # dep2 still incomplete

    claimed2 = job_service.claim_next_job(db_session, worker)
    exec2 = job_service.mark_running(db_session, claimed2, worker)
    job_service.mark_completed(db_session, claimed2, exec2, {})
    db_session.refresh(downstream)
    assert downstream.status == models.JobStatus.QUEUED


def test_create_job_rejects_unknown_dependency(db_session):
    queue = _make_queue(db_session)
    try:
        job_service.create_job(db_session, queue=queue, task_name="a", payload={}, depends_on=["does-not-exist"])
        assert False, "expected ValueError"
    except ValueError:
        pass


# ---------------- Queue sharding ----------------

def test_jobs_are_assigned_a_shard_within_range(db_session):
    queue = _make_queue(db_session)
    queue.shard_count = 4
    db_session.commit()

    jobs = [job_service.create_job(db_session, queue=queue, task_name="x", payload={}) for _ in range(20)]
    assert all(0 <= j.shard_id < 4 for j in jobs)


def test_worker_pinned_to_shard_only_claims_that_shard(db_session):
    queue = _make_queue(db_session, concurrency_limit=100)
    queue.shard_count = 2
    db_session.commit()

    jobs = [job_service.create_job(db_session, queue=queue, task_name="x", payload={}) for _ in range(10)]
    worker = _make_worker(db_session)

    claimed_shard_0 = []
    for _ in range(len(jobs)):
        job = job_service.claim_next_job(db_session, worker, shard_id=0)
        if job is None:
            break
        claimed_shard_0.append(job)

    assert all(j.shard_id == 0 for j in claimed_shard_0)
    assert len(claimed_shard_0) == sum(1 for j in jobs if j.shard_id == 0)


# ---------------- Distributed locking ----------------

def test_only_one_instance_can_hold_scheduler_lock(db_session):
    got_a = scheduler_service.try_acquire_lock(db_session, "instance-a", lease_seconds=10)
    got_b = scheduler_service.try_acquire_lock(db_session, "instance-b", lease_seconds=10)

    assert got_a is True
    assert got_b is False  # instance-a's lease hasn't expired yet


def test_lock_can_be_reacquired_after_release(db_session):
    scheduler_service.try_acquire_lock(db_session, "instance-a", lease_seconds=10)
    scheduler_service.release_lock(db_session, "instance-a")
    got_b = scheduler_service.try_acquire_lock(db_session, "instance-b", lease_seconds=10)
    assert got_b is True


# ---------------- Stale job reaper ----------------

def test_reaper_requeues_jobs_from_dead_worker(db_session):
    queue = _make_queue(db_session)
    job = job_service.create_job(db_session, queue=queue, task_name="x", payload={})
    worker = _make_worker(db_session)
    job_service.claim_next_job(db_session, worker)

    # Simulate the worker having gone silent well past the timeout.
    worker.last_seen_at = datetime.utcnow() - timedelta(minutes=10)
    db_session.commit()

    reaped = scheduler_service.reap_stale_jobs(db_session)
    db_session.refresh(job)

    assert reaped == 1
    assert job.status == models.JobStatus.QUEUED
    assert job.claimed_by_worker_id is None


def test_reaper_leaves_jobs_from_healthy_worker_alone(db_session):
    queue = _make_queue(db_session)
    job_service.create_job(db_session, queue=queue, task_name="x", payload={})
    worker = _make_worker(db_session)
    job_service.claim_next_job(db_session, worker)

    reaped = scheduler_service.reap_stale_jobs(db_session)
    assert reaped == 0


# ---------------- AI-style failure summaries ----------------

def test_failure_summary_classifies_timeout():
    class FakeJob:
        task_name = "generate_report"
    summary = generate_failure_summary(FakeJob(), "Request timeout after 30s", attempt_count=3)
    assert "time budget" in summary
    assert "generate_report" in summary


def test_failure_summary_falls_back_for_unknown_errors():
    class FakeJob:
        task_name = "custom_task"
    summary = generate_failure_summary(FakeJob(), "kaboom, totally novel error", attempt_count=1)
    assert "manual investigation" in summary


def test_dead_letter_entry_stores_ai_summary(db_session):
    queue = _make_queue(db_session, max_retries=0)
    job = job_service.create_job(db_session, queue=queue, task_name="x", payload={})
    worker = _make_worker(db_session)
    claimed = job_service.claim_next_job(db_session, worker)
    execution = job_service.mark_running(db_session, claimed, worker)
    job_service.mark_failed(db_session, claimed, execution, "Connection refused")

    db_session.refresh(claimed)
    assert claimed.dlq_entry.ai_summary is not None
    assert "connection" in claimed.dlq_entry.ai_summary.lower()


# ---------------- RBAC ----------------

def test_admin_can_pause_queue_operator_cannot(client):
    client.post("/api/auth/register", json={"email": "admin@x.com", "password": "password123", "role": "admin"})
    client.post("/api/auth/register", json={"email": "op@x.com", "password": "password123", "role": "operator"})
    admin_token = client.post("/api/auth/login", data={"username": "admin@x.com", "password": "password123"}).json()["access_token"]
    op_token = client.post("/api/auth/login", data={"username": "op@x.com", "password": "password123"}).json()["access_token"]

    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    op_headers = {"Authorization": f"Bearer {op_token}"}

    org = client.post("/api/organizations", json={"name": "Acme"}, headers=admin_headers).json()
    project = client.post("/api/projects", json={"name": "P", "organization_id": org["id"]}, headers=admin_headers).json()
    queue = client.post("/api/queues", json={"name": "q", "project_id": project["id"]}, headers=admin_headers).json()

    forbidden = client.post(f"/api/queues/{queue['id']}/pause", headers=op_headers)
    assert forbidden.status_code == 403

    allowed = client.post(f"/api/queues/{queue['id']}/pause", headers=admin_headers)
    assert allowed.status_code == 200
    assert allowed.json()["is_paused"] is True
