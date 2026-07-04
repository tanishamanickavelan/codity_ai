from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from app.config import settings
from app.database import Base, engine
from app.logging_config import logger
from app.rate_limit import limiter
from app.routers import auth, dashboard, jobs, projects, queues, websocket, workers

# Creates tables if they don't exist. For real schema evolution, use Alembic
# migrations instead (see docs/SETUP.md).
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title=settings.APP_NAME,
    description="A production-inspired distributed job scheduling platform.",
    version="1.0.0",
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # relax for the bundled static dashboard; restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(projects.router)
app.include_router(queues.router)
app.include_router(jobs.router)
app.include_router(workers.router)
app.include_router(dashboard.router)
app.include_router(websocket.router)


@app.on_event("startup")
def on_startup():
    logger.info(f"{settings.APP_NAME} starting up")


@app.get("/api/health")
def health_check():
    return {"status": "ok", "service": settings.APP_NAME}
