"""
Runs an MCP Streamable-HTTP client + session in a dedicated background thread
with its own asyncio event loop, so synchronous callers (the CLI REPL loop,
Flask's synchronous request handlers) can invoke MCP tools via a simple
blocking call (call_tool).

Talks to the mcp-confluent server over HTTP (see mcp-server/mcp-config.yaml
and the `mcp-confluent` service in docker-compose.ai-demo.yml) instead of
spawning it as a stdio subprocess — the MCP server runs in its own container,
shared by the CLI, the web UI, and (optionally) Claude Code.
"""

import asyncio
import contextlib
import threading

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from core import ALLOWED_TOOLS, MCP_SERVER_URL, mcp_tool_to_bedrock


class MCPWorker:
    def __init__(self, server_url: str = MCP_SERVER_URL, connect_retry_seconds: int = 60):
        self.server_url = server_url
        # mcp-confluent takes a few seconds to boot (load tools, connect to
        # Kafka) after its container is *started* — `depends_on` in
        # docker-compose.ai-demo.yml only guarantees the container exists,
        # not that the HTTP server inside is accepting connections yet. Retry
        # the initial connection instead of crashing on the first
        # ConnectionError, as a second line of defense on top of the
        # mcp-confluent healthcheck.
        self.connect_retry_seconds = connect_retry_seconds
        self.loop = asyncio.new_event_loop()
        self.session: ClientSession | None = None
        self.tools: list = []
        self._ready = threading.Event()
        self._error: Exception | None = None
        self._stop_event: asyncio.Event | None = None
        self._thread = threading.Thread(target=self._run_loop, daemon=True)

    def start(self, timeout: int | None = None):
        self._thread.start()
        if not self._ready.wait(timeout=timeout or self.connect_retry_seconds + 30):
            raise RuntimeError(f"Timed out waiting for MCP server at {self.server_url}")
        if self._error:
            raise self._error

    def stop(self):
        if self._stop_event is not None:
            self.loop.call_soon_threadsafe(self._stop_event.set)

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._main())
        except Exception as exc:  # noqa: BLE001
            self._error = exc
            self._ready.set()

    async def _connect(self) -> tuple[ClientSession, contextlib.AsyncExitStack]:
        """Connect + initialize, retrying transient connection failures.

        Returns the session plus an AsyncExitStack that owns both the
        transport and the session — the caller keeps it open for as long as
        the connection is in use, then closes it on shutdown. Each failed
        attempt tears down whatever it partially opened (streamable_http_client,
        then ClientSession) before retrying, via the stack.pop_all() idiom:
        https://docs.python.org/3/library/contextlib.html#cleaning-up-in-an-__enter__-implementation
        """
        deadline = asyncio.get_event_loop().time() + self.connect_retry_seconds
        delay = 1
        while True:
            stack = contextlib.AsyncExitStack()
            try:
                async with stack:
                    read, write = await stack.enter_async_context(streamable_http_client(self.server_url))
                    session = await stack.enter_async_context(ClientSession(read, write))
                    await session.initialize()
                    # Success — detach the stack so the `async with` above
                    # doesn't tear it down; the caller now owns it.
                    stack = stack.pop_all()
                return session, stack
            except Exception:
                if asyncio.get_event_loop().time() >= deadline:
                    raise
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5)

    async def _main(self):
        session, stack = await self._connect()
        async with stack:
            tools_result = await session.list_tools()
            self.session = session
            self.tools = [t for t in tools_result.tools if t.name in ALLOWED_TOOLS]
            self._ready.set()

            self._stop_event = asyncio.Event()
            await self._stop_event.wait()

    def bedrock_tools(self) -> list[dict]:
        return [mcp_tool_to_bedrock(t) for t in self.tools]

    def call_tool(self, name: str, args: dict, timeout: int = 30):
        fut = asyncio.run_coroutine_threadsafe(self.session.call_tool(name, args), self.loop)
        return fut.result(timeout=timeout)
