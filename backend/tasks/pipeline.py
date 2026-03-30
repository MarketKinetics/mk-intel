from backend.celery_app import celery_app


@celery_app.task(bind=True)
def add(self, x: int, y: int) -> int:
    """Toy task — proves Celery wiring works."""
    return x + y
