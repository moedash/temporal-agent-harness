"""ToolRegistryWorkflow — routing table for the Durable Tool Call Gateway.

Perpetual workflow mapping 3rd-party external MCP server names to their URL + tool list.
Nexus-native servers never appear here — they register directly against the calling agent's
own registry and bypass the gateway entirely.

Workflow ID:  REGISTRY_WORKFLOW_ID  (singleton per namespace)
Task queue:   "mcp-registry"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from temporalio import activity, workflow
from temporalio.exceptions import ActivityError

REGISTRY_WORKFLOW_ID = "mcp-tool-registry"
REGISTRY_TASK_QUEUE = "mcp-registry"


@activity.defn
async def fetch_external_tools(name: str, url: str) -> list[dict[str, Any]]:
    """Fetch an external server's tool list and prefix each tool `{name}_{tool}`."""
    activity.logger.info("[registry] fetching tools from %s", url)
    activity.heartbeat()

    async with streamable_http_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.list_tools()

    tools = []
    for tool in result.tools:
        prefixed = tool.model_copy(update={"name": f"{name}_{tool.name}"})
        tools.append(prefixed.model_dump())

    activity.logger.info("[registry] fetched %d tool(s) from %s", len(tools), url)
    return tools


@dataclass
class RegistryEntry:
    """Routing entry for one 3rd-party external MCP server."""

    url: str = ""
    tools: list[dict[str, Any]] = field(default_factory=list)


@workflow.defn(sandboxed=False, name="ToolRegistry")
class ToolRegistryWorkflow:
    """Perpetual routing-table workflow for the Durable Tool Call Gateway."""

    def __init__(self) -> None:
        self._entries: dict[str, RegistryEntry] = {}
        self._pending_external: list[tuple[str, str]] = []

    @workflow.run
    async def run(self) -> None:
        while True:
            await workflow.wait_condition(lambda: bool(self._pending_external))
            while self._pending_external:
                name, url = self._pending_external.pop(0)
                try:
                    tools: list[dict[str, Any]] = await workflow.execute_activity(
                        fetch_external_tools,
                        args=[name, url],
                        start_to_close_timeout=timedelta(seconds=60),
                    )
                except ActivityError as exc:
                    workflow.logger.error(
                        "[registry] Failed registering external MCP server %r: could not fetch tools: %s", name, exc
                    )
                    continue
                self._entries[name] = RegistryEntry(url=url, tools=tools)
                tool_names = [t.get("name", "?") for t in tools]
                print(
                    f"[registry] Successfully registered external MCP server {name!r} at {url}  "
                    f"({len(tools)} tools: {tool_names})",
                    flush=True,
                )

    # -- registration ------------------------------------------------------------

    @workflow.signal
    def register_external(self, name: str, url: str) -> None:
        """Queue a 3rd-party MCP server for tool fetching."""
        print(f"[registry] queued external {name!r} -> {url}", flush=True)
        self._pending_external.append((name, url))

    @workflow.signal
    def deregister(self, name: str) -> None:
        """Remove a registration by service name."""
        removed = self._entries.pop(name, None)
        if removed:
            workflow.logger.info("[registry] deregistered %r", name)
        else:
            workflow.logger.debug(
                "[registry] deregister: %r not found (stale signal, ignoring)", name
            )

    @workflow.signal
    def clear_all(self) -> None:
        """Remove all entries."""
        count = len(self._entries)
        self._entries.clear()
        self._pending_external.clear()
        workflow.logger.info("[registry] cleared %d entries", count)

    # -- queries -----------------------------------------------------------------

    @workflow.query
    def find(self, name: str) -> RegistryEntry | None:
        return self._entries.get(name)

    @workflow.query
    def list_tools(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for entry in self._entries.values():
            result.extend(entry.tools)
        return result
