#!/bin/bash
export PYTHONPATH=/app
celery -A backend.celery_app worker --loglevel=info --concurrency=2 2>&1 &
sleep 3
echo "=== Celery started ==="
uvicorn backend.main:app --host 0.0.0.0 --port 8000
