"""
Centralized application configuration.

All tunables live here so the rest of the codebase never reads
environment variables directly. Values can be overridden via a
.env file (see .env.example) or real environment variables.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- General ---
    APP_NAME: str = "Distributed Job Scheduler"
    ENV: str = "development"

    # --- Database ---
    # Defaults to a local SQLite file so the project runs with zero setup.
    # Swap to a Postgres URL in production, e.g.:
    # postgresql+psycopg2://user:password@localhost:5432/jobscheduler
    DATABASE_URL: str = "sqlite:///./job_scheduler.db"

    # --- Auth / JWT ---
    JWT_SECRET_KEY: str = "CHANGE_ME_IN_PRODUCTION_super_secret_key"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours

    # --- Worker / Scheduler tuning ---
    WORKER_POLL_INTERVAL_SECONDS: float = 1.0
    WORKER_HEARTBEAT_INTERVAL_SECONDS: float = 5.0
    WORKER_HEARTBEAT_TIMEOUT_SECONDS: int = 30  # worker considered dead after this
    SCHEDULER_POLL_INTERVAL_SECONDS: float = 1.0

    # --- Retry defaults ---
    DEFAULT_MAX_RETRIES: int = 3
    DEFAULT_RETRY_STRATEGY: str = "exponential"  # fixed | linear | exponential
    DEFAULT_RETRY_BASE_DELAY_SECONDS: int = 5

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
