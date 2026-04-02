FROM python:3.11-slim
WORKDIR /app
COPY backend/requirements.txt backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt
COPY . .
RUN mkdir -p /app/bta_data && \
    cp /app/data/societal_processed/bta_cards/mk_bta_rag_corpus.jsonl /app/bta_data/ && \
    cp /app/data/societal_processed/bta_cards/mk_bta_baseline.parquet /app/bta_data/ && \
    cp /app/data/reference/zcta_enrichment.parquet /app/bta_data/
ENV PYTHONPATH=/app
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
