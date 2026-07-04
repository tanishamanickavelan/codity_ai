# Entity-Relationship Diagram

```mermaid
erDiagram
    USER ||--o{ ORGANIZATION : owns
    ORGANIZATION ||--o{ PROJECT : contains
    PROJECT ||--o{ QUEUE : contains
    RETRY_POLICY ||--o{ QUEUE : "default policy for"
    QUEUE ||--o{ JOB : contains
    QUEUE ||--o{ SCHEDULED_JOB : "cron template for"
    JOB ||--o{ JOB_EXECUTION : "has attempts"
    JOB ||--o{ JOB_LOG : "has log lines"
    JOB ||--o| DEAD_LETTER_QUEUE : "moved to (if exhausted)"
    JOB ||--o{ JOB_DEPENDENCY : "requires"
    WORKER ||--o{ JOB_EXECUTION : executes
    WORKER ||--o{ WORKER_HEARTBEAT : sends

    USER {
        string id PK
        string email UK
        string hashed_password
        string full_name
        enum role "admin|operator|viewer"
        bool is_active
    }
    ORGANIZATION {
        string id PK
        string name
        string owner_id FK
    }
    PROJECT {
        string id PK
        string name
        string organization_id FK
        string api_key UK
    }
    RETRY_POLICY {
        string id PK
        string name
        enum strategy "fixed|linear|exponential"
        int max_retries
        int base_delay_seconds
        int max_delay_seconds
    }
    QUEUE {
        string id PK
        string name
        string project_id FK
        int priority
        int concurrency_limit
        bool is_paused
        int shard_count
        string retry_policy_id FK
    }
    JOB {
        string id PK
        string queue_id FK
        enum job_type "immediate|delayed|scheduled|recurring|batch"
        string task_name
        json payload
        enum status "scheduled|blocked|queued|claimed|running|completed|failed|retrying|dead"
        int priority
        datetime run_at
        string cron_expression
        string batch_id
        string retry_policy_id FK
        int attempt_count
        int max_retries
        int shard_id
        string idempotency_key
        string claimed_by_worker_id FK
        json result
        text error_message
    }
    JOB_EXECUTION {
        string id PK
        string job_id FK
        string worker_id FK
        int attempt_number
        enum status
        datetime started_at
        datetime finished_at
        int duration_ms
        json result
        text error_message
    }
    JOB_LOG {
        string id PK
        string job_id FK
        string level
        text message
        datetime created_at
    }
    DEAD_LETTER_QUEUE {
        string id PK
        string job_id FK "UK"
        text reason
        text ai_summary
        json final_payload
        bool replayed
    }
    WORKER {
        string id PK
        string name
        enum status "active|idle|draining|offline"
        int concurrency
        json queues
        datetime last_seen_at
    }
    WORKER_HEARTBEAT {
        string id PK
        string worker_id FK
        datetime sent_at
        int active_jobs
    }
    SCHEDULED_JOB {
        string id PK
        string queue_id FK
        string task_name
        json payload
        string cron_expression
        bool is_active
        datetime next_run_at
        datetime last_run_at
    }
    JOB_DEPENDENCY {
        string id PK
        string job_id FK
        string depends_on_job_id FK
    }
    SCHEDULER_LOCK {
        string id PK "always 'scheduler'"
        string holder_id
        datetime acquired_at
        datetime expires_at
    }
```

## Design rationale

- **UUID string primary keys** everywhere instead of auto-increment
  integers, so IDs are safe to generate client-side, don't leak row
  counts, and merge painlessly across a distributed / multi-region setup.
- **`Job` vs `JobExecution` split.** `Job` holds the current/latest state;
  `JobExecution` is an append-only row per attempt. This gives a full
  audit trail (attempt #2 took 340ms and failed with X) without
  overloading the `Job` row with per-attempt columns.
- **`ScheduledJob` vs `Job` (RECURRING type) split.** `ScheduledJob` is a
  reusable *template* ("run this every 5 minutes"); each firing
  materializes a concrete, independent `Job` row. This keeps the job
  table's semantics simple (a `Job` is always one concrete unit of work)
  and makes recurring-job history queryable exactly like any other job.
- **`RetryPolicy` as its own table**, referenced by both `Queue` (as a
  default) and optionally overridable per `Job`, so a team can define a
  handful of named policies ("fast-fixed", "network-exponential") and
  reuse them instead of duplicating retry config on every job.
- **Composite index `(queue_id, status, priority, run_at)`** on `Job` is
  the single most important index in the schema — it's exactly the shape
  of the atomic-claim query's WHERE/ORDER BY clause.
- **Unique constraint `(queue_id, idempotency_key)`** enforces
  de-duplication at the database level as a second line of defense beyond
  the application-level check in `job_service.create_job`.
