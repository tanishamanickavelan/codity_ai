"""
Application-wide logging configuration.

Fixes the "no application-level logging" gap: previously the only visible
output was FastAPI/uvicorn's access log plus ad-hoc print() calls in the
worker and scheduler. This gives every process (API, worker, scheduler) a
consistent, structured logger that includes timestamps, log level, and the
originating module - call setup_logging() once at process startup.
"""
import logging
import sys


def setup_logging(service_name: str = "job-scheduler") -> logging.Logger:
    logger = logging.getLogger(service_name)
    if logger.handlers:
        return logger  # already configured (e.g. re-imported under uvicorn --reload)

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# Shared logger instance used across app.main, app.worker, app.scheduler,
# and app.services.*
logger = setup_logging()
