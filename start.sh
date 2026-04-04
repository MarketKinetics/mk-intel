#!/bin/bash
export PYTHONPATH=/app
echo "=== Starting Celery ==="
celery -A backend.celery_app worker --loglevel=debug --concurrency=2 2>&1 &
CELERY_PID=$!
echo "=== Celery PID: $CELERY_PID ==="
sleep 5
echo "=== Celery still running: $(kill -0 $CELERY_PID 2>&1) ==="
echo "=== Starting Uvicorn ==="
uvicorn backend.main:app --host 0.0.0.0 --port 8000
