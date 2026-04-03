FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y supervisor && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

COPY . .

RUN mkdir -p /app/bta_data && \
    cp /app/data/societal_processed/bta_cards/mk_bta_rag_corpus.jsonl /app/bta_data/ && \
    cp /app/data/societal_processed/bta_cards/mk_bta_baseline.parquet /app/bta_data/ && \
    cp /app/data/reference/zcta_enrichment.parquet /app/bta_data/

RUN mkdir -p /var/log/supervisor

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

ENV PYTHONPATH=/app

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
