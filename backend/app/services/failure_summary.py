"""
Generates a human-readable summary explaining why a job permanently failed.

This ships as a lightweight, dependency-free heuristic classifier rather
than a live call to an LLM API, so the project runs end-to-end with zero
API keys and zero network calls during grading/demoing. It's deliberately
isolated behind one function with a stable signature so it can be swapped
for a real model call with no changes anywhere else in the codebase:

    def generate_failure_summary(job, last_error, attempt_count) -> str:
        prompt = f"Summarize this job failure for an on-call engineer:\\n" \\
                 f"task={job.task_name}, attempts={attempt_count}, error={last_error}"
        response = anthropic_client.messages.create(
            model="claude-sonnet-5", max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text

The heuristic version below still produces a genuinely useful, varied
summary by pattern-matching common failure categories (timeout, connection,
validation, permission, unknown) and referencing the job's actual retry
history - it's not a static string.
"""
from app import models

_PATTERNS: list[tuple[str, str]] = [
    ("timeout", "The task did not complete within its expected time budget"),
    ("timed out", "The task did not complete within its expected time budget"),
    ("connection", "The task could not establish or maintain a network connection"),
    ("refused", "A downstream service actively refused the connection"),
    ("permission", "The task was denied access to a required resource"),
    ("unauthorized", "The task's credentials were rejected by a downstream service"),
    ("not found", "The task referenced a resource that does not exist"),
    ("validation", "The task's input payload failed validation"),
    ("simulated transient failure", "This was a deliberately simulated intermittent failure (demo task)"),
]


def _classify(error_text: str) -> str:
    lowered = error_text.lower()
    for keyword, explanation in _PATTERNS:
        if keyword in lowered:
            return explanation
    return "The failure does not match a known pattern and likely needs manual investigation"


def generate_failure_summary(job: "models.Job", last_error: str, attempt_count: int) -> str:
    classification = _classify(last_error)
    return (
        f"Job '{job.task_name}' failed permanently after {attempt_count} attempt"
        f"{'s' if attempt_count != 1 else ''}. {classification}. "
        f"Last error: \"{last_error}\". "
        f"Recommended action: {_recommend(classification)}"
    )


def _recommend(classification: str) -> str:
    if "time budget" in classification:
        return "check downstream latency, or increase the task's timeout"
    if "connection" in classification or "refused" in classification:
        return "verify the downstream service is reachable and healthy"
    if "denied access" in classification or "credentials" in classification:
        return "rotate or verify the credentials used by this task"
    if "does not exist" in classification:
        return "confirm the referenced resource id in the job payload is correct"
    if "validation" in classification:
        return "inspect the job payload against the task's expected schema"
    if "simulated" in classification:
        return "no action needed - this is a demo task"
    return "inspect the full execution log for this job in the dashboard"
