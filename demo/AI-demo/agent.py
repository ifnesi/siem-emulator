#!/usr/bin/env python3
"""
SIEM AI Agent — terminal chat backed by AWS Bedrock + the Confluent MCP
Server (mcp-confluent), running over HTTP against this project's local
docker-compose SIEM cluster.

Usage (inside the ai-cli container — see docker-compose.ai-demo.yml):
    docker compose -f docker-compose.yml -f docker-compose.ai-demo.yml \\
        run --rm ai-cli

Or directly:
    python agent.py

Environment:
    AWS_ACCESS_KEY_ID     — AWS credentials for Bedrock
    AWS_SECRET_ACCESS_KEY
    AWS_SESSION_TOKEN     — optional, for temporary credentials
    AWS_REGION            — AWS region (default: us-east-1)
    BEDROCK_MODEL         — Bedrock model id (default: us.anthropic.claude-haiku-4-5-20251001-v1:0)
    MCP_SERVER_URL        — mcp-confluent HTTP endpoint (default: http://mcp-confluent:8080/mcp)
"""

import asyncio
import json
import sys

import boto3
from rich.console import Console
from rich.live import Live
from rich.text import Text

from core import ALLOWED_TOOLS, AWS_REGION, BEDROCK_MODEL, MCP_SERVER_URL, SYSTEM_PROMPT, missing_env_vars
from mcp_client import MCPWorker

console = Console()


# -- Bedrock streaming chat --------------------------------------------------

async def chat(worker: MCPWorker, bedrock, messages: list[dict]) -> None:
    bedrock_tools = worker.bedrock_tools()

    while True:
        response = bedrock.converse_stream(
            modelId=BEDROCK_MODEL,
            system=[{"text": SYSTEM_PROMPT}],
            messages=messages,
            toolConfig={"tools": bedrock_tools} if bedrock_tools else {},
        )

        current_text = ""
        tool_uses = []
        current_tool: dict | None = None
        stop_reason = None

        console.print()
        with Live(Text(""), console=console, refresh_per_second=15) as live:
            for event in response["stream"]:
                if "contentBlockStart" in event:
                    block = event["contentBlockStart"].get("start", {})
                    if "toolUse" in block:
                        current_tool = {
                            "toolUseId": block["toolUse"]["toolUseId"],
                            "name": block["toolUse"]["name"],
                            "input_str": "",
                        }

                elif "contentBlockDelta" in event:
                    delta = event["contentBlockDelta"]["delta"]
                    if "text" in delta:
                        current_text += delta["text"]
                        live.update(Text(current_text))
                    elif "toolUse" in delta and current_tool:
                        current_tool["input_str"] += delta["toolUse"].get("input", "")

                elif "contentBlockStop" in event:
                    if current_tool:
                        try:
                            current_tool["input"] = json.loads(current_tool["input_str"] or "{}")
                        except json.JSONDecodeError:
                            current_tool["input"] = {}
                        tool_uses.append(current_tool)
                        current_tool = None

                elif "messageStop" in event:
                    stop_reason = event["messageStop"].get("stopReason")

        assistant_content = []
        if current_text:
            assistant_content.append({"text": current_text})
        for tu in tool_uses:
            assistant_content.append({
                "toolUse": {
                    "toolUseId": tu["toolUseId"],
                    "name": tu["name"],
                    "input": tu["input"],
                }
            })
        if assistant_content:
            messages.append({"role": "assistant", "content": assistant_content})

        if stop_reason != "tool_use" or not tool_uses:
            break

        tool_results = []
        for tu in tool_uses:
            console.print(
                Text(f"\n  -> tool: {tu['name']}  {json.dumps(tu['input'])}", style="dim italic")
            )
            try:
                result = worker.call_tool(tu["name"], tu["input"])
                result_text = "\n".join(
                    block.text for block in result.content if hasattr(block, "text")
                )
                tool_results.append({
                    "toolUseId": tu["toolUseId"],
                    "content": [{"text": result_text}],
                })
            except Exception as exc:
                console.print(Text(f"  [tool error] {exc}", style="red"))
                tool_results.append({
                    "toolUseId": tu["toolUseId"],
                    "content": [{"text": f"Error: {exc}"}],
                })

        messages.append({
            "role": "user",
            "content": [{"toolResult": tr} for tr in tool_results],
        })


# -- Main REPL ----------------------------------------------------------------

async def main():
    missing = missing_env_vars()
    if missing:
        console.print(
            Text(f"Missing environment variables: {', '.join(missing)}", style="bold red")
        )
        sys.exit(1)

    bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)

    console.print(Text("\n SIEM AI Agent  (type 'exit' or Ctrl-C to quit)\n", style="bold cyan"))
    console.print(Text(f" Model     : {BEDROCK_MODEL}", style="dim"))
    console.print(Text(f" MCP server: {MCP_SERVER_URL}", style="dim"))
    console.print(Text(f" Tools     : {', '.join(sorted(ALLOWED_TOOLS))}\n", style="dim"))

    worker = MCPWorker()
    try:
        worker.start()
    except Exception as exc:
        console.print(Text(f"Failed to connect to MCP server at {MCP_SERVER_URL}: {exc}", style="bold red"))
        sys.exit(1)

    messages: list[dict] = []

    while True:
        try:
            user_input = console.input("[bold blue]You:[/bold blue] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\nBye.")
            break

        if not user_input or user_input.lower() in ("exit", "quit"):
            console.print("Bye.")
            break

        messages.append({"role": "user", "content": [{"text": user_input}]})
        console.print(Text("Agent:", style="bold green"), end=" ")

        try:
            await chat(worker, bedrock, messages)
        except Exception as exc:
            console.print(Text(f"\n[error] {exc}", style="bold red"))
            messages.pop()

        console.print()

    worker.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
