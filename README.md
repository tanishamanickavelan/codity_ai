# Distributed Job Scheduler

A production-inspired distributed job scheduling platform: submit jobs to
priority queues, run them across a horizontally-scalable pool of workers,
get automatic retries with backoff, dead-letter handling for permanently
failed jobs, cron-based recurring jobs, and a live ops dashboard.

Built with **FastAPI + SQLAlchemy** (backend/API), a plain **Python worker
process** (execution), and a **vanilla HTML/CSS/JS dashboard** (no build
step required).

```
distributed-job-scheduler/
├── backend/            FastAPI app, worker process, scheduler process, tests
├── frontend/            Static dashboard (open frontend/index.html)
└── docs/                 Architecture, ER diagram, API reference, design notes
```

Read `docs/ARCHITECTURE.md` for how the pieces fit together and
`docs/DESIGN_DECISIONS.md` for the reasoning behind the trickier choices
(atomic job claiming, retry/backoff, at-least-once delivery, etc).

## Quick start

Requires Python 3.11+.

```bash
cd backend
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 1. Start the API (creates job_scheduler.db, a local SQLite file, on first run)
uvicorn app.main:app --reload --port 8000

# 2. In a second terminal: start the scheduler (promotes scheduled/cron jobs)
python -m app.scheduler

# 3. In a third terminal: start one or more workers
python -m app.worker --name worker-1 --concurrency 4
```

Then open `frontend/index.html` directly in a browser (double-click it, or
serve it with `python3 -m http.server 5500` from the `frontend/` folder and
visit `http://localhost:5500`). Register an account in the dashboard — an
organization, project, and empty queue set are created automatically on
first login.

API docs (interactive Swagger UI) are available at
`http://localhost:8000/docs` once the API is running.

## Running the tests

```bash
cd backend
pytest -v
```

16 tests cover: retry backoff math (fixed/linear/exponential + capping),
atomic job claiming under concurrent workers, queue concurrency limits,
the full failure → retry → dead-letter-queue lifecycle, and the main API
flows (auth, project/queue/job creation, worker registration + claim).

## Try it end-to-end

1. Start the API, scheduler, and a worker as above.
2. In the dashboard, create a queue (or use the auto-created "Default
   Project" queue list).
3. Click **+ New job**, pick task `sum_numbers`, payload
   `{"numbers": [1, 2, 3, 4]}`, submit.
4. Watch the **Overview** pipeline and the **Job Explorer** — the job moves
   Queued → Claimed → Running → Completed within a second or two.
5. Try task `flaky_task` (fails ~50% of the time) a few times to see the
   retry/backoff logs in the job drawer, and eventually a job land in the
   **Dead Letter Queue**, where you can **Replay** it.
6. Create a **Recurring** job with cron `*/1 * * * *` to see the scheduler
   materialize a new job every minute.

## Key features implemented

Core:
- **Atomic job claiming** — no two workers can ever pick up the same job
  (see `app/services/job_service.py::claim_next_job` and
  `docs/DESIGN_DECISIONS.md`).
- **Priority queues** with per-queue concurrency limits and pause/resume.
- **Retry policies**: fixed, linear, and exponential backoff, per queue,
  with a configurable max-delay cap.
- **Dead Letter Queue** with manual replay.
- **Delayed, scheduled, and recurring (cron) jobs**, plus batch job
  submission.
- **Idempotency keys** to safely de-duplicate job submissions.
- **Worker heartbeats** and graceful draining on shutdown (Ctrl+C).
- **Full job lifecycle audit trail**: per-attempt `JobExecution` rows and a
  structured job log, both visible in the dashboard drawer.
- **Application-level structured logging** (`app/logging_config.py`) across
  the API, worker, and scheduler processes.
- **Stale job reaper**: jobs stuck in CLAIMED/RUNNING because their worker
  crashed mid-execution are automatically requeued once that worker's
  heartbeat goes silent past the timeout.

Bonus features:
- **Workflow dependencies** — a job can `depends_on` one or more other job
  IDs and stays `BLOCKED` until all of them `COMPLETE`.
- **Event-driven execution** — completing a job immediately checks and
  promotes any dependents whose dependencies are now all satisfied,
  rather than waiting for the next poll tick.
- **Rate limiting** — auth and job-creation endpoints are rate-limited per
  client IP (`app/rate_limit.py`, via slowapi).
- **Distributed locking** — if you run more than one scheduler process for
  redundancy, a lease-based lock (`SchedulerLock`) ensures only one of them
  promotes/materializes/reaps jobs on a given tick.
- **Queue sharding** — a queue can be split into N shards; a worker started
  with `--shard K` only claims jobs from that shard, so dedicated worker
  pools can each own a shard.
- **Role-based access control** — `admin` / `operator` / `viewer` roles;
  pausing queues, replaying the DLQ, and draining workers require `admin`.
- **AI-style failure summaries** — every Dead Letter Queue entry gets a
  human-readable summary of why the job failed and what to do about it
  (`app/services/failure_summary.py`; heuristic by default, designed to be
  swapped for a real LLM call with no other code changes).
- **Live WebSocket updates** — the dashboard's Overview page also connects
  to `/ws/dashboard` for instant health updates, in addition to periodic
  polling.

## Trying the bonus features

**Workflow dependencies:**
```bash
curl -X POST http://localhost:8000/api/jobs -H "Authorization: Bearer $TOKEN" \
  -d '{"queue_id": "...", "task_name": "generate_report", "payload": {}}'
# copy the returned "id", then:
curl -X POST http://localhost:8000/api/jobs -H "Authorization: Bearer $TOKEN" \
  -d '{"queue_id": "...", "task_name": "send_email", "payload": {}, "depends_on": ["<id from above>"]}'
```
The second job starts `blocked` and flips to `queued` the instant the first
one completes.

**Sharded workers:**
```bash
# create a queue with shard_count=2 via the dashboard or API, then:
python -m app.worker --name worker-shard0 --shard 0
python -m app.worker --name worker-shard1 --shard 1
```

**RBAC:** register a `viewer` account in the dashboard and confirm the
Pause/Replay/Drain buttons are hidden and the underlying API calls 403.

**Redundant schedulers:** run `python -m app.scheduler` in two terminals at
once — only one will log `promoted=...` on any given tick; the other logs
"did not win scheduler lock this tick."



## Scaling out

Start more worker processes (same machine or different machines, pointing
`--url` at the API's address) to add capacity horizontally — the atomic
claim guarantees they never collide. See `docs/ARCHITECTURE.md` for how
this maps onto a real multi-machine deployment, including the swap from
SQLite to Postgres for real concurrent write throughput.
