web: PYTHONPATH=/app uvicorn backend.main:app --host 0.0.0.0 --port $PORT
worker: PYTHONPATH=/app celery -A backend.celery_app worker --loglevel=info
