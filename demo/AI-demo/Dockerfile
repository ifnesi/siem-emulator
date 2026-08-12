# Shared Python image for the CLI agent (agent.py) and the Web UI
# (webui/app.py) — docker-compose.ai-demo.yml runs the two services from this
# same image with a different `command:`.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core.py mcp_client.py agent.py ./
COPY webui/ ./webui/

# Default: web UI. The ai-cli service overrides this with `python agent.py`.
CMD ["python", "webui/app.py"]
