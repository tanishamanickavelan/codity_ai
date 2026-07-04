# Architecture

## Components

```
                        ┌─────────────────────┐
                        │   Dashboard (SPA)    │
                        │  static HTML/CSS/JS  │
                        └──────────┬───────────┘
                                   │ REST / JSON (JWT auth)
                                   ▼
┌──────────────────────────────────────────────────────────┐
│                     FastAPI application                   │
│  routers: auth, projects, queues, jobs, workers, dashboard │
│  services: job_service, retry_service, scheduler_service   │
└───────────────┬───────────────────────────┬───────────────┘
                │                             │
                ▼                             ▼
        ┌───────────────┐            ┌────────────────┐
        │   Database     │◄──────────►│  Scheduler     │
        │ (SQLite/Postgres)│  polls    │  process       │
        └───────▲────────┘            └────────────────┘
                │  atomic claim (UPDATE ... WHERE status='queued')
                │
        ┌───────┴────────┐   ┌────────────────┐   ┌────────────────┐
        │   Worker #1     │   │   Worker #2     │   │   Worker #N     │
        │ poll/claim/run  │   │ poll/claim/run  │   │ poll/claim/run  │
        └────────────────┘   └────────────────┘   └────────────────┘
```

The API, scheduler, and workers are **separate OS processes** that only
share state through the database — this is what makes the system
horizontally scalable and lets each piece be deployed and scaled
independently (e.g. run 20 workers against one API instance).

## Job lifecycle

```
   create_job()
        │
        ▼
 run_at in future?
   │           │
  yes          no
   │           │
   ▼           ▼
SCHEDULED    QUEUED ◄────────────────┐
   │                                 │  requeue with backoff delay
   │ scheduler promotes              │  (attempt_count < max_retries)
   │ when run_at <= now              │
   └────────────────► QUEUED         │
                         │           │
              worker atomically      │
              claims job             │
                         ▼           │
                     CLAIMED         │
                         │           │
                worker starts it     │
                         ▼           │
                     RUNNING         │
                    /        \       │
              success       failure──┘
                 │              │
                 ▼              ▼ (attempt_count >= max_retries)
             COMPLETED         DEAD  ──► DeadLetterQueueEntry created
                                          (replayable via the dashboard)
```

## Why separate processes instead of one monolith?

- **Independent scaling.** Job execution (CPU/IO heavy, bursty) has very
  different scaling needs than the API (request/response, latency
  sensitive). Splitting them lets you run 1 API instance behind a load
  balancer and 50 worker instances, or vice versa.
- **Fault isolation.** A worker crash (e.g. a task handler segfaults or
  leaks memory) doesn't take down the API or other workers. A crashed
  worker's claimed-but-not-finished jobs are visible in the dashboard as
  stuck in `CLAIMED`/`RUNNING`; a production hardening step (see
  `docs/DESIGN_DECISIONS.md`) would add a reaper that requeues jobs whose
  worker's heartbeat has gone stale.
- **Deployment flexibility.** Workers can run on entirely different
  machines (even different clouds/regions) than the API, as long as they
  can reach it over HTTP.

## Data flow for a single job

1. A client (dashboard or any HTTP caller) `POST /api/jobs` with a
   `queue_id`, `task_name`, and `payload`.
2. The API validates the queue exists, computes the initial status
   (`QUEUED` or `SCHEDULED`), and writes a `Job` row.
3. If `SCHEDULED`, the standalone scheduler process promotes it to
   `QUEUED` once `run_at` has passed (recurring/cron jobs are also
   materialized here, from the `ScheduledJob` template table).
4. A worker's poll loop calls `POST /api/workers/{id}/claim`. The API runs
   the atomic-claim UPDATE (see `docs/DESIGN_DECISIONS.md`) and returns
   the claimed job, or `null` if nothing is claimable.
5. The worker calls `POST /api/jobs/{id}/start` (creates a `JobExecution`
   row, flips the job to `RUNNING`), runs the handler from
   `app/tasks.py`'s registry, then reports back via `.../complete` or
   `.../fail`.
6. On failure, `job_service.mark_failed` either requeues the job with a
   computed backoff delay or, if retries are exhausted, moves it to the
   Dead Letter Queue.
7. Every step appends a `JobLog` row, so the full history of a job is
   visible in the dashboard's job drawer.

## Scaling to Postgres in production

SQLite is the default so the project runs with zero setup, and its
`UPDATE ... WHERE status='queued'` claim is still atomic (SQLite
serializes writes). For real production concurrency, swap
`DATABASE_URL` to a Postgres connection string and change the claim query
in `job_service.claim_next_job` to use
`SELECT ... FOR UPDATE SKIP LOCKED` before the `UPDATE`, which lets many
workers probe for work in parallel without blocking on each other's
row locks. Both approaches are functionally atomic; Postgres's version
scales better under high worker counts. This swap is isolated to one
function, by design.
