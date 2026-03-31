from celery import Celery
from dotenv import load_dotenv
load_dotenv()
from backend.config import settings

celery_app = Celery(
    "mk_intel",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["backend.tasks.pipeline"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    broker_connection_retry_on_startup=True,
)
