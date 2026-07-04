# Design Decisions

## 1. How is a job guaranteed to run on exactly one worker?

Two workers can call the poll/claim endpoint at the same instant. Correctness
depends on the **claim** being atomic. `job_service.claim_next_job` does this
in two steps:

1. `SELECT` the best candidate job (highest priority, oldest `run_at`,
   status `QUEUED`, `run_at <= now`) — this is just to *find* a candidate id,
   it makes no promise about locking.
2. `UPDATE jobs SET status='claimed', claimed_by_worker_id=:w WHERE id=:id
   AND status='queued'` — this is the atomic part. If another transaction
   already flipped that row's status away from `queued` (because it won the
   race), this UPDATE's `rowcount` is `0` and the caller correctly treats it
   as "lost the race, try again," rather than incorrectly believing it owns
   the job.

This works because a single `UPDATE` statement is atomic at the database
engine level — SQLite serializes writes, and Postgres/MySQL take the
necessary row lock for the duration of the statement. No application-level
mutex or distributed lock is needed.

**Production upgrade path:** on Postgres, replace step 1 with
`SELECT id FROM jobs WHERE ... ORDER BY priority DESC, run_at ASC LIMIT 1
FOR UPDATE SKIP LOCKED` inside the same transaction as the UPDATE. This
lets N workers each grab a *different* row-lock in parallel (SKIP LOCKED
means a worker doesn't wait behind another worker's candidate row), which
scales better than one contended query at high worker counts. It's not
needed for correctness — only for performance at scale — so the simpler
UPDATE-based approach is used here to keep the reference implementation
readable, with this noted as the swap-in for a real deployment.

## 2. At-least-once delivery, not exactly-once

If a worker process is killed mid-execution (after claiming, before
reporting success), the job would be stuck in `RUNNING`/`CLAIMED` forever
unless something notices. **This is now handled**: `app/services/
scheduler_service.py::reap_stale_jobs` runs on every scheduler tick and
requeues any job whose claiming worker's heartbeat has gone silent past
`WORKER_HEARTBEAT_TIMEOUT_SECONDS`. This closes what was originally an
explicit, documented gap in this design.

Delivery is still **at-least-once**, not exactly-once: a reaped job may
have partially executed side effects before its worker died, and will run
again from the start on retry. Because of this, **task handlers should be
idempotent** where possible (e.g. use `payload`'s natural key to skip
duplicate side effects). The `idempotency_key` field on `Job` prevents
duplicate *submissions*, but doesn't protect against a task handler
running twice due to a worker crash mid-execution - that's a property of
the handler, not something the scheduler can guarantee for arbitrary code.

## 3. Why three separate processes (API / scheduler / worker) instead of background threads in the API?

Running the scheduler and workers as background threads inside the FastAPI
process is the simplest possible thing that works for a demo, but it
directly contradicts "distributed": it would mean job execution scaled
1:1 with API instances, execution failures could crash the API, and you
could never deploy workers on separate machines. Splitting them into
independent processes that talk only through the database (and, for the
worker, through the HTTP API) is what makes the system actually
horizontally scalable and independently deployable — which is the whole
point of a "distributed job scheduler" as opposed to an in-process task
queue.

## 4. Why SQLite by default?

So the project runs with `pip install -r requirements.txt` and no other
setup — no Docker, no separate database server. The schema and every query
in `job_service.py` were written to be Postgres-compatible from day one
(explicit atomic UPDATE rather than an ORM-level `.update()` that might
hide row-lock semantics differently across backends), so moving to
Postgres for real concurrent throughput is a one-line `DATABASE_URL`
change plus the `SKIP LOCKED` upgrade described in decision #1.

## 5. Why compute retry backoff as a pure function?

`retry_service.compute_delay_seconds` takes primitives in and returns a
number — no database, no side effects. This is what makes it trivial to
unit-test all three strategies (and the max-delay cap) in isolation
without spinning up a database or a job, and it's a pattern applied
throughout: pure logic (`retry_service`) is kept separate from
stateful orchestration (`job_service`).

## 6. Why is the job execution registry a plain dict instead of a plugin system?

`app/tasks.py`'s `TASK_REGISTRY` maps a string `task_name` to a Python
function. This is the simplest mechanism that satisfies the real
requirement (a queue doesn't care *what* work it schedules, only *that* it
gets scheduled reliably) without over-engineering a plugin/discovery
system that the assignment doesn't ask for. Real teams would register
their own handlers here; the five sample tasks (including one intentionally
flaky one) exist purely so the system is demoable end-to-end without a real
business integration.

## 7. Known simplifications (explicitly out of scope)

- Worker-to-API calls aren't authenticated with a service token (a real
  deployment would use a project API key or mTLS instead of leaving those
  endpoints open).
- The dashboard's JWT is stored in `localStorage` for simplicity; a
  production app would use an httpOnly cookie to reduce XSS exposure.
- Alembic migrations aren't wired up; `Base.metadata.create_all` is used
  for simplicity, which is fine for a fresh database but doesn't handle
  schema evolution on an existing one.
- RBAC is a single global role per user, not a per-organization membership
  table (see #8 below) — sufficient to demonstrate the access-control
  pattern the assignment asks for, without the complexity of a full
  multi-tenant membership model.
- The AI failure summary is a heuristic classifier, not a live LLM call
  (see #9 below) — this is a deliberate choice to keep the project
  runnable with zero API keys and zero network dependency during grading.

These are called out explicitly, rather than hidden, so the tradeoffs are
visible rather than discovered later.

## 8. Why a single global role instead of per-organization membership?

A fully realistic RBAC system would have a `Membership` table
(user, organization, role) so the same person could be an admin in one
org and a viewer in another. That's meaningfully more schema and query
complexity for a feature whose purpose here is to *demonstrate the
access-control pattern* (some actions require elevated privilege, and the
API enforces it centrally via `require_role()`, not ad hoc checks
scattered through route handlers). The simpler single-role-per-user model
demonstrates the same pattern - dependency-injected, centrally-enforced
authorization - with a fraction of the surface area. `require_role()` is
written so that swapping in a real membership check later only touches
that one function.

## 9. Why a heuristic failure summarizer instead of a live LLM call?

`app/services/failure_summary.py` pattern-matches common failure
categories (timeout, connection, permission, validation, ...) and
composes a human-readable summary referencing the job's actual retry
history - not a static string. It's deliberately not a live API call so
the project has zero external dependencies and zero API keys required to
run and grade end-to-end. The function's signature
(`generate_failure_summary(job, last_error, attempt_count) -> str`) is
intentionally the same shape a real LLM call would have; the module's
docstring shows the exact ~5-line change to swap in a real Claude/OpenAI
call with no changes anywhere else in the codebase.

## 10. Why leader election for the scheduler instead of just running one?

Running exactly one scheduler process is simpler and is what the default
setup does. The distributed lock (`SchedulerLock`, acquired via the same
atomic-UPDATE pattern as job claiming) exists for the case where you want
scheduler *redundancy* - e.g. one scheduler process per availability zone,
so a zone outage doesn't stop cron jobs from firing. Without the lock, two
schedulers running simultaneously would double-materialize every recurring
job and double-promote every delayed job. The lock's lease (rather than an
explicit unlock-on-crash) means a crashed scheduler's lock expires on its
own within `lease_seconds`, so a backup instance takes over automatically
without needing to detect the crash itself.

## 11. Why shard by hash(job_id) instead of round-robin or queue-based sharding?

`shard_id = hash(job.id) % queue.shard_count` is computed once at job
creation and never changes, so a given job always lands on the same shard
even if you inspect/replay/requeue it later - this makes shard assignment
predictable and debuggable ("which shard is job X on? always the same
one"). A round-robin counter would need shared, contended state across API
instances to stay balanced; hashing the job's own ID needs no shared state
at all and still distributes evenly across shards for any reasonable
number of jobs.
