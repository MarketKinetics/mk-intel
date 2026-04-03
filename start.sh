#!/bin/bash
celery -A backend.celery_app worker --loglevel=info --concurrency=2 &
uvicorn backend.main:app --host 0.0.0.0 --port 8000
