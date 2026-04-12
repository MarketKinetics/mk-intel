from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.config import settings
from backend.celery_app import celery_app
from backend.db.jobs import init_db
from backend.db.demo import init_demo_db
from backend.routers import sessions, pipeline
from backend.routers.demo import router as demo_router, admin_router
from backend.routers.examples import router as examples_router
from backend.routers.admin import router as mk_admin_router
import redis

app = FastAPI(title=settings.app_name, version=settings.app_version)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(sessions.router)
app.include_router(pipeline.router)
app.include_router(demo_router)
app.include_router(admin_router)
app.include_router(examples_router)
app.include_router(mk_admin_router)


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
        ping = inspect.ping()
        worker_status = "ok" if ping else "no workers"
    except Exception as e:
        worker_status = f"error: {e}"

    return {
        "api":     "ok",
        "redis":   redis_status,
        "worker":  worker_status,
        "version": settings.app_version,
    }
