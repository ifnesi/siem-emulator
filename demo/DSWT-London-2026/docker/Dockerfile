# DSWT London 2026 — datagen image that pumps the e-commerce streams into
# Confluent Cloud. Build context is the REPO ROOT so it can COPY siem_producer.py
# and templates/ as they sit in the repo:
#
#   docker build -f demo/DSWT-London-2026/docker/Dockerfile -t dswt-datagen .
#
# (docker-compose.yml sets the context/dockerfile for you.)
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY siem_producer.py .
COPY templates/ ./templates/
# Avro schemas the producers pin via --schema (must match what Terraform
# registers + RTCE-enables). schemas/dswt_*.avsc.
COPY schemas/ ./schemas/

COPY demo/DSWT-London-2026/docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
