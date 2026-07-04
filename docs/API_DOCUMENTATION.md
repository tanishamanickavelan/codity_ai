# API Reference

Base URL: `http://localhost:8000`. Interactive Swagger docs at `/docs`
whenever the API is running. All endpoints except auth, worker
registration/heartbeat/claim, and job start/complete/fail require a
`Authorization: Bearer <token>` header.

## Auth

| Method | Path | Description |
|---|---|---|
| POST | `/api/auth/register` | Create a user account (`role`: admin/operator/viewer, default operator) — rate limited 10/minute |
| POST | `/api/auth/login` | Form-encoded `username`/`password`, returns a JWT — rate limited 20/minute |
| GET | `/api/auth/me` | Get the current user's profile and role |

## Organizations & Projects

| Method | Path | Description |
|---|---|---|
| POST | `/api/organizations` | Create an organization |
| GET | `/api/organizations` | List your organizations |
| POST | `/api/projects` | Create a project under an organization |
| GET | `/api/projects?organization_id=` | List projects |

## Retry Policies

| Method | Path | Description |
|---|---|---|
| POST | `/api/retry-policies` | Create a named retry policy (fixed/linear/exponential) |
| GET | `/api/retry-policies` | List retry policies |

## Queues

| Method | Path | Description |
|---|---|---|
| POST | `/api/queues` | Create a queue (`shard_count`: default 1) |
| GET | `/api/queues?project_id=&limit=&offset=` | List/paginate queues |
| GET | `/api/queues/{id}` | Get a queue |
| PATCH | `/api/queues/{id}` | Update priority/concurrency/policy |
| POST | `/api/queues/{id}/pause` | **Admin only.** Pause a queue (stops claiming) |
| POST | `/api/queues/{id}/resume` | **Admin only.** Resume a queue |
| GET | `/api/queues/{id}/stats` | Live counts by status |

## Jobs

| Method | Path | Description |
|---|---|---|
| POST | `/api/jobs` | Create a job (immediate/delayed/scheduled/recurring; supports `depends_on: [job_id, ...]` for workflow dependencies) — rate limited 60/minute |
| POST | `/api/jobs/batch` | Create many jobs sharing one `batch_id` |
| GET | `/api/jobs?queue_id=&status=&job_type=&batch_id=&limit=&offset=` | Filter/paginate jobs |
| GET | `/api/jobs/{id}` | Get a job |
| GET | `/api/jobs/{id}/executions` | Attempt history |
| GET | `/api/jobs/{id}/logs` | Structured log lines |
| POST | `/api/jobs/{id}/cancel` | Cancel a non-running job |
| POST | `/api/jobs/{id}/retry` | Force-requeue a failed/dead job |

**Example — create an immediate job:**
```bash
curl -X POST http://localhost:8000/api/jobs \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{
    "queue_id": "...",
    "task_name": "send_email",
    "payload": {"to": "user@example.com", "subject": "Welcome"}
  }'
```

**Example — create a recurring job:**
```bash
curl -X POST http://localhost:8000/api/jobs \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{
    "queue_id": "...",
    "task_name": "generate_report",
    "payload": {"report_id": "daily-active-users"},
    "job_type": "recurring",
    "cron_expression": "0 * * * *"
  }'
```
This returns HTTP 202 with the `ScheduledJob` id; the first concrete `Job`
row is materialized by the scheduler process at the next cron tick.

**Example — create a job with a dependency (workflow):**
```bash
curl -X POST http://localhost:8000/api/jobs \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{
    "queue_id": "...",
    "task_name": "send_email",
    "payload": {"to": "user@example.com"},
    "depends_on": ["<id of an upstream job>"]
  }'
```
The new job starts in `blocked` status and is promoted to `queued`
automatically the instant every job it depends on reaches `completed`.

## Workers (called by the worker process, not end users)

| Method | Path | Description |
|---|---|---|
| POST | `/api/workers/register` | Register a new worker (`shard_id`: optional, restricts claims to that shard), returns its id |
| POST | `/api/workers/{id}/heartbeat` | Report liveness + active job count |
| POST | `/api/workers/{id}/claim` | Atomically claim the next runnable job (or `null`) |
| POST | `/api/workers/{id}/drain` | **Admin only.** Signal graceful shutdown |
| GET | `/api/workers` | List the worker fleet (auth required) |
| POST | `/api/jobs/{id}/start?worker_id=` | Mark a claimed job as running |
| POST | `/api/jobs/{id}/complete` | Report success + result payload |
| POST | `/api/jobs/{id}/fail` | Report failure (triggers retry or DLQ) |

## Dead Letter Queue

| Method | Path | Description |
|---|---|---|
| GET | `/api/dead-letter-queue?limit=&offset=` | List permanently failed jobs (each includes an `ai_summary` field) |
| POST | `/api/dead-letter-queue/{id}/replay` | **Admin only.** Requeue for another full attempt cycle |

## Dashboard / metrics

| Method | Path | Description |
|---|---|---|
| GET | `/api/dashboard/health` | Worker/queue/job/DLQ counts |
| GET | `/api/dashboard/throughput?hours=8` | Hourly completed/failed counts |

## Live updates

| Protocol | Path | Description |
|---|---|---|
| WebSocket | `/ws/dashboard` | Pushes a fresh `{"type": "health", "data": {...}}` snapshot (same shape as `/api/dashboard/health`) every ~3 seconds while connected |
