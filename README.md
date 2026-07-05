# Distributed Job Scheduler

A production-inspired distributed job scheduling platform: submit jobs to
priority queues, run them across a horizontally-scalable pool of workers,
get automatic retries with backoff, dead-letter handling for permanently
failed jobs, cron-based recurring jobs, workflow dependencies, and a live
operations dashboard.

Built with **FastAPI + SQLAlchemy** (API), a plain **Python worker
process** (execution), a **standalone scheduler process** (timing/cron/
recovery), and a **vanilla HTML/CSS/JS dashboard** (no build step
required).

```
distributed-job-scheduler/
├── backend/            FastAPI app, worker process, scheduler process, tests
├── frontend/           Static dashboard (open frontend/index.html)
└── docs/               Architecture, ER diagram, API reference, design notes
```

Read `docs/ARCHITECTURE.md` for how the pieces fit together and
`docs/DESIGN_DECISIONS.md` for the reasoning behind the trickier choices
(atomic job claiming, retry/backoff, sharding, distributed locking, RBAC,
and known simplifications).

---

## Features

**Core**
- Atomic job claiming — two workers can never execute the same job
- Priority queues with per-queue concurrency limits, pause/resume
- Retry policies: fixed, linear, exponential backoff, with a max-delay cap
- Dead Letter Queue with manual replay
- Immediate, delayed, scheduled, recurring (cron), and batch jobs
- Idempotency keys to safely de-duplicate submissions
- Worker heartbeats and graceful draining on shutdown (Ctrl+C)
- Full audit trail: per-attempt execution history + structured job logs
- Application-level structured logging across API, worker, and scheduler
- Automatic stale-job recovery if a worker dies mid-execution

**Bonus**
- Workflow dependencies (`depends_on`) with event-driven promotion
- Rate limiting on auth and job-creation endpoints
- Distributed locking for safe multi-instance scheduler redundancy
- Queue sharding with worker-to-shard pinning
- Role-based access control (admin / operator / viewer)
- AI-style Dead Letter Queue failure summaries
- Live WebSocket dashboard updates (in addition to polling)

---

## Quick start (local)

Requires Python 3.11.

### 1. Set up the backend

**macOS / Linux:**
```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Windows (PowerShell):**
```powershell
cd backend
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
> If PowerShell blocks the activation script with an execution-policy
> error, run `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`
> once, then try activating again.

> **If `psycopg2-binary` fails to build on Windows** with a `pg_config
> executable not found` error: this happens when pip can't find a
> precompiled wheel for your Python version and tries to build from
> source. Fix it with:
> ```powershell
> pip install --only-binary :all: psycopg2-binary
> ```
> This dependency is only needed if you're connecting to Postgres (see
> Deployment below) — local development with the default SQLite database
> doesn't need it at all, so you can also just remove that line from
> `requirements.txt` if you're only running locally.

### 2. Run the three backend processes (separate terminals, all from `backend/`)

```bash
# Terminal 1 — the API (creates job_scheduler.db, a local SQLite file, on first run)
uvicorn app.main:app --reload --port 8000

# Terminal 2 — the scheduler (promotes scheduled/cron jobs, reaps stale jobs)
python -m app.scheduler

# Terminal 3 — one or more workers
python -m app.worker --name worker-1 --concurrency 4
```

### 3. Serve the dashboard

Don't open `frontend/index.html` by double-clicking it — browsers block
API calls from `file://` pages. Serve it instead:

```bash
cd frontend
python -m http.server 5500 --bind 127.0.0.1
```

Then open **`http://127.0.0.1:5500`** in your browser (not the `[::]`
address some terminals print — that's an IPv6 placeholder, not a
clickable URL).

Register an account (pick a role — **Admin** gets full access, including
pausing queues and replaying the Dead Letter Queue). An organization,
project, and empty queue set are created automatically on first login.

---

## Running the tests

```bash
cd backend
pytest -v
```

30 tests cover: retry backoff math, atomic job claiming under concurrent
workers, queue concurrency limits, the full failure → retry → dead-letter
lifecycle, workflow dependencies, queue sharding, distributed locking, the
stale-job reaper, RBAC, AI failure summaries, and the core API flows.

---

## Try it end-to-end

1. Start the API, scheduler, and a worker (see Quick Start above).
2. In the dashboard, create a queue (or use the auto-created default).
3. **+ New job** → task `sum_numbers`, payload `{"numbers": [1, 2, 3, 4]}`
   → submit. Watch it move through the pipeline on **Overview** within a
   couple of seconds.
4. Try task `flaky_task` a few times to see retry/backoff behavior in the
   job's log drawer.
5. Submit a job with a task name that doesn't exist (e.g. `does_not_exist`)
   to guarantee it exhausts retries and lands in the **Dead Letter Queue**,
   complete with an AI-generated summary of why it failed — replay it from
   there (Admin role required).
6. Create two jobs where the second has `depends_on` set to the first's
   job ID — it stays `blocked` until the first completes, then promotes
   itself automatically.
7. Create a **Recurring** job with cron `*/1 * * * *` to see the scheduler
   materialize a new job every minute.


---

## Documentation

| Document | Contents |
|---|---|
| `docs/ARCHITECTURE.md` | Process/component diagram, job lifecycle diagram, data flow |
| `docs/ER_DIAGRAM.md` | Full schema with rationale for every design choice |
| `docs/API_DOCUMENTATION.md` | Every endpoint, with example requests |
| `docs/DESIGN_DECISIONS.md` | Trade-offs: atomic claiming, at-least-once delivery, RBAC scope, sharding strategy, and known simplifications |

## Scaling out

Start more worker processes (same machine or different machines, pointing
`--url` at wherever the API lives) to add capacity horizontally — atomic
claiming guarantees they never collide. Pin workers to a shard with
`--shard N` if a queue has `shard_count > 1`. Run more than one scheduler
process for redundancy — a distributed lock ensures only one of them acts
on any given tick.