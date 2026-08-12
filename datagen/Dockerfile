# Runs the SIEM data producers + Flink-less streaming apps (demo/*_streaming_app.py)
# as a single long-lived container — see entrypoint.sh for start order/timing.
#
# Build context is the repo root (see docker-compose.yml) so this can COPY
# siem_producer.py, templates/, schemas/, and demo/ as they actually sit in
# the repo.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY siem_producer.py .
COPY templates/ ./templates/
COPY schemas/ ./schemas/
COPY demo/*.py ./demo/
COPY demo/schemas/ ./demo/schemas/
COPY demo/utils/ ./demo/utils/

COPY datagen/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
