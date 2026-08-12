# Confluent MCP Server (https://github.com/confluentinc/mcp-confluent),
# installed from npm rather than built from source — no pnpm/build step
# needed inside the image.
FROM node:22-slim

# Pin a specific version so the demo doesn't silently pick up breaking
# changes on rebuild. Bump deliberately if you want a newer server.
ARG MCP_CONFLUENT_VERSION=1.5.0
RUN npm install -g @confluentinc/mcp-confluent@${MCP_CONFLUENT_VERSION}

WORKDIR /app
COPY mcp-config.yaml ./mcp-config.yaml

EXPOSE 8080

ENTRYPOINT ["mcp-confluent", "--config", "/app/mcp-config.yaml"]
