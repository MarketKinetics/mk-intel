from fastapi import FastAPI
from backend.config import settings
from backend.celery_app import celery_app
from backend.db.jobs import init_db
from backend.db.demo import init_demo_db
from backend.routers import sessions, pipeline
from backend.routers.demo import router as demo_router, admin_router
from backend.routers.examples import router as examples_router
import redis

app = FastAPI(title=settings.app_name, version=settings.app_version)
app.include_router(sessions.router)
app.include_router(pipeline.router)
app.include_router(demo_router)
app.include_router(admin_router)
app.include_router(examples_router)


@app.on_event("startup")
def startup():
    init_db()
    init_demo_db()


@app.get("/health")
def health():
    try:
        r = redis.from_url(settings.redis_url)
        r.ping()
        redis_status = "ok"
    except Exception as e:
        redis_status = f"error: {e}"

    try:
        inspect = celery_app.control.inspect(timeout=2.0)
        active = inspect.active()
        worker_status = "ok" if active else "no workers"
    except Exception as e:
        worker_status = f"error: {e}"

    return {
        "api":     "ok",
        "redis":   redis_status,
        "worker":  worker_status,
        "version": settings.app_version,
    }
