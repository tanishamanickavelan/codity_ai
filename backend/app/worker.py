"""
Standalone worker process.

Run one or many of these (in separate terminals/containers/machines) to
scale out execution horizontally:

    python -m app.worker --name worker-1 --concurrency 4
    python -m app.worker --name worker-2 --concurrency 8 --queues <queue_id>

Each worker:
  1. Registers itself with the API.
  2. Sends periodic heartbeats on a background thread.
  3. Polls for claimable jobs and executes them concurrently in a thread
     pool (bounded by --concurrency).
  4. Reports success/failure back to the API, which drives retry/backoff/DLQ.
  5. On SIGINT/SIGTERM, stops claiming new jobs, waits for in-flight jobs to
     finish, then exits (graceful shutdown / "draining").
"""
import argparse
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from app.tasks import run_task, TaskFailure


class Worker:
    def __init__(self, base_url: str, name: str, concurrency: int, queues: list[str],
                 poll_interval: float, heartbeat_interval: float, shard_id: int | None = None):
        self.base_url = base_url.rstrip("/")
        self.name = name
        self.concurrency = concurrency
        self.queues = queues
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.shard_id = shard_id

        self.worker_id: str | None = None
        self.executor = ThreadPoolExecutor(max_workers=concurrency)
        self.active_jobs = 0
        self.active_lock = threading.Lock()
        self.shutting_down = threading.Event()

    # -------------------------------------------------------------
    def register(self) -> None:
        resp = requests.post(
            f"{self.base_url}/api/workers/register",
            json={"name": self.name, "concurrency": self.concurrency, "queues": self.queues,
                  "shard_id": self.shard_id},
            timeout=10,
        )
        resp.raise_for_status()
        self.worker_id = resp.json()["id"]
        shard_note = f", shard={self.shard_id}" if self.shard_id is not None else ""
        print(f"[{self.name}] registered as worker_id={self.worker_id}{shard_note}")

    def heartbeat_loop(self) -> None:
        while not self.shutting_down.is_set():
            try:
                with self.active_lock:
                    active = self.active_jobs
                requests.post(
                    f"{self.base_url}/api/workers/{self.worker_id}/heartbeat",
                    json={"active_jobs": active}, timeout=5,
                )
            except requests.RequestException as e:
                print(f"[{self.name}] heartbeat failed: {e}")
            time.sleep(self.heartbeat_interval)

    # -------------------------------------------------------------
    def poll_loop(self) -> None:
        while not self.shutting_down.is_set():
            with self.active_lock:
                if self.active_jobs >= self.concurrency:
                    time.sleep(self.poll_interval)
                    continue
            job = self._claim()
            if job:
                with self.active_lock:
                    self.active_jobs += 1
                self.executor.submit(self._execute, job)
            else:
                time.sleep(self.poll_interval)

    def _claim(self) -> dict | None:
        try:
            resp = requests.post(f"{self.base_url}/api/workers/{self.worker_id}/claim", timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"[{self.name}] claim request failed: {e}")
            return None

    def _execute(self, job: dict) -> None:
        job_id = job["id"]
        try:
            requests.post(
                f"{self.base_url}/api/jobs/{job_id}/start",
                params={"worker_id": self.worker_id}, timeout=10,
            )
            print(f"[{self.name}] running job {job_id} ({job['task_name']})")
            result = run_task(job["task_name"], job["payload"])
            requests.post(f"{self.base_url}/api/jobs/{job_id}/complete", json={"result": result}, timeout=10)
            print(f"[{self.name}] completed job {job_id}")
        except TaskFailure as e:
            requests.post(f"{self.base_url}/api/jobs/{job_id}/fail", json={"error": str(e)}, timeout=10)
            print(f"[{self.name}] job {job_id} failed: {e}")
        except Exception as e:  # noqa: BLE001 - any unexpected exception still needs to be reported
            requests.post(f"{self.base_url}/api/jobs/{job_id}/fail", json={"error": f"Unexpected error: {e}"}, timeout=10)
            print(f"[{self.name}] job {job_id} crashed: {e}")
        finally:
            with self.active_lock:
                self.active_jobs -= 1

    # -------------------------------------------------------------
    def drain_and_shutdown(self, *_args) -> None:
        print(f"\n[{self.name}] shutdown signal received, draining...")
        self.shutting_down.set()
        if self.worker_id:
            try:
                requests.post(f"{self.base_url}/api/workers/{self.worker_id}/drain", timeout=5)
            except requests.RequestException:
                pass
        self.executor.shutdown(wait=True)
        print(f"[{self.name}] all in-flight jobs finished, exiting.")
        sys.exit(0)

    def run(self) -> None:
        self.register()
        signal.signal(signal.SIGINT, self.drain_and_shutdown)
        signal.signal(signal.SIGTERM, self.drain_and_shutdown)

        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        print(f"[{self.name}] polling for jobs (concurrency={self.concurrency}) ... Ctrl+C to stop.")
        self.poll_loop()


def main():
    parser = argparse.ArgumentParser(description="Distributed Job Scheduler worker process")
    parser.add_argument("--url", default="http://localhost:8000", help="Base URL of the API")
    parser.add_argument("--name", default="worker-1", help="Human-readable worker name")
    parser.add_argument("--concurrency", type=int, default=4, help="Max jobs this worker runs at once")
    parser.add_argument("--queues", nargs="*", default=[], help="Queue IDs to poll (default: all queues)")
    parser.add_argument("--shard", type=int, default=None,
                         help="Only claim jobs assigned to this shard (see Queue.shard_count). Omit to claim from every shard.")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--heartbeat-interval", type=float, default=5.0)
    args = parser.parse_args()

    worker = Worker(
        base_url=args.url, name=args.name, concurrency=args.concurrency, queues=args.queues,
        poll_interval=args.poll_interval, heartbeat_interval=args.heartbeat_interval, shard_id=args.shard,
    )
    worker.run()


if __name__ == "__main__":
    main()
