"""
Task registry: maps a job's `task_name` to a Python callable.

In a real deployment, teams would register their own business-logic
handlers here (send an email, resize an image, generate a report, call a
third-party API, ...). This file ships a handful of illustrative sample
tasks so the scheduler is runnable end-to-end out of the box, including one
(`flaky_task`) that fails intermittently to make retries/backoff/DLQ
observable in the dashboard during a demo.
"""
import random
import time
from typing import Any, Callable


class TaskFailure(Exception):
    """Raised by a task handler to signal a failed attempt."""


def task_send_email(payload: dict[str, Any]) -> dict[str, Any]:
    time.sleep(0.3)
    return {"sent_to": payload.get("to"), "subject": payload.get("subject"), "status": "delivered"}


def task_resize_image(payload: dict[str, Any]) -> dict[str, Any]:
    time.sleep(0.5)
    return {"image": payload.get("url"), "resized_to": payload.get("dimensions", "800x600")}


def task_generate_report(payload: dict[str, Any]) -> dict[str, Any]:
    time.sleep(1.0)
    return {"report_id": payload.get("report_id"), "rows_processed": random.randint(100, 5000)}


def task_sum_numbers(payload: dict[str, Any]) -> dict[str, Any]:
    numbers = payload.get("numbers", [])
    return {"sum": sum(numbers)}


def task_flaky_task(payload: dict[str, Any]) -> dict[str, Any]:
    """Fails ~50% of the time - useful for demonstrating retry/backoff/DLQ."""
    if random.random() < 0.5:
        raise TaskFailure("Simulated transient failure")
    return {"status": "ok after retry logic"}


TASK_REGISTRY: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "send_email": task_send_email,
    "resize_image": task_resize_image,
    "generate_report": task_generate_report,
    "sum_numbers": task_sum_numbers,
    "flaky_task": task_flaky_task,
}


def run_task(task_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    handler = TASK_REGISTRY.get(task_name)
    if handler is None:
        raise TaskFailure(f"No handler registered for task '{task_name}'")
    return handler(payload)
