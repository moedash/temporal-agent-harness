"""RegistryServiceHandler — Nexus-facing surface of the Durable Tools Gateway.

Lets any namespace register a 3rd-party (non-Nexus) MCP server and invoke its tools,
durably, via ToolCallWorkflow. Nexus-native servers never touch this — they register and
are called directly by the calling agent workflow.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler
import temporalio.nexus
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from nexusrpc.handler import StartOperationContext
from pydantic import BaseModel
from temporalio import activity, workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy

from .registry import REGISTRY_WORKFLOW_ID, RegistryEntry, ToolRegistryWorkflow
from .generated import (
    CallToolInput,
    CallToolOutput,
    DeregisterInput,
    ListToolsOutput,
    RegisterExternalInput,
    RegistryService,
)

REGISTRY_NEXUS_ENDPOINT = "mcp-registry-endpoint"


class ExternalMCPCallInput(BaseModel):
    """Input for an HTTP call to a 3rd-party MCP server."""

    server_url: str
    tool_name: str
    arguments: dict[str, Any]


@activity.defn
async def mcp_proxy_activity(input: ExternalMCPCallInput) -> str:
    """Call one tool on an external MCP server over Streamable HTTP."""
    activity.logger.info(
        "[proxy-activity] calling %r on %s", input.tool_name, input.server_url
    )
    activity.heartbeat()

    async with streamable_http_client(input.server_url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(input.tool_name, input.arguments)

    activity.logger.info(
        "[proxy-activity] %r completed  is_error=%s", input.tool_name, result.isError
    )
    return _call_tool_result_to_str(result)


def _call_tool_result_to_str(result: Any) -> str:
    if result.isError:
        texts = [c.text for c in (result.content or []) if hasattr(c, "text") and c.text]
        raise RuntimeError(texts[0] if texts else "MCP tool returned an error")
    parts: list[str] = []
    for c in result.content or []:
        if hasattr(c, "text") and c.text:
            parts.append(c.text)
        elif hasattr(c, "data"):
            parts.append(f"[binary data: {len(c.data)} bytes]")
        else:
            parts.append(str(c))
    return "\n".join(parts)


@workflow.defn(name="ToolCall", sandboxed=False)
class ToolCallWorkflow:
    """Durable, per-call wrapper around mcp_proxy_activity — avoids standalone activities,
    which need an experimental server capability known to deadlock the calling workflow."""

    @workflow.run
    async def run(self, input: ExternalMCPCallInput) -> str:
        return await workflow.execute_activity(
            mcp_proxy_activity,
            input,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
            ),
        )


@nexusrpc.handler.service_handler(service=RegistryService)
class RegistryServiceHandler:
    """Signals/queries ToolRegistryWorkflow and dispatches tool calls, on behalf of
    callers from any namespace, via the mcp-registry-endpoint Nexus endpoint."""

    def __init__(self, client: Client) -> None:
        self._client = client

    @nexusrpc.handler.sync_operation
    async def register_external(
        self, ctx: StartOperationContext, input: RegisterExternalInput
    ) -> None:
        """Register a 3rd-party external MCP server with the gateway."""
        handle = self._client.get_workflow_handle(REGISTRY_WORKFLOW_ID)
        await handle.signal(
            ToolRegistryWorkflow.register_external,
            args=[input.name, input.url],
        )

    @nexusrpc.handler.sync_operation
    async def deregister(
        self, ctx: StartOperationContext, input: DeregisterInput
    ) -> None:
        """Remove a service registration from the gateway."""
        handle = self._client.get_workflow_handle(REGISTRY_WORKFLOW_ID)
        await handle.signal(ToolRegistryWorkflow.deregister, args=[input.name])

    @nexusrpc.handler.sync_operation
    async def list_tools(
        self, ctx: StartOperationContext, input: None
    ) -> ListToolsOutput:
        """Return all tool dicts for 3rd-party servers registered with the gateway.

        Doesn't extend authoring.MCPOverNexusServiceHandler — the tool list comes from
        register_external, not from this handler's own operations.
        """
        handle = self._client.get_workflow_handle(REGISTRY_WORKFLOW_ID)
        tools = await handle.query(ToolRegistryWorkflow.list_tools)
        return ListToolsOutput(tools=tools)

    @nexusrpc.handler.sync_operation
    async def call_tool(
        self, ctx: StartOperationContext, input: CallToolInput
    ) -> CallToolOutput:
        """Invoke one tool on a registered 3rd-party MCP server via ToolCallWorkflow.

        Failures are raised as nexusrpc.HandlerError with an explicit type — a bare
        exception defaults to UNKNOWN, which nexusrpc retries forever instead of
        surfacing to the caller.
        """
        name = input.name or ""
        service, _, operation = name.partition("_")
        if not service or not operation:
            raise nexusrpc.HandlerError(
                f"Invalid tool name {name!r}: expected 'service_operation'",
                type=nexusrpc.HandlerErrorType.BAD_REQUEST,
            )

        handle = self._client.get_workflow_handle(REGISTRY_WORKFLOW_ID)
        entry: RegistryEntry | None = await handle.query(ToolRegistryWorkflow.find, service)
        if entry is None:
            raise nexusrpc.HandlerError(
                f"Service {service!r} is not registered with the gateway.",
                type=nexusrpc.HandlerErrorType.NOT_FOUND,
            )

        try:
            call_handle = await self._client.start_workflow(
                ToolCallWorkflow.run,
                ExternalMCPCallInput(
                    server_url=entry.url,
                    tool_name=operation,
                    arguments=input.arguments or {},
                ),
                id=f"mcp-proxy-{uuid.uuid4()}",
                task_queue=temporalio.nexus.info().task_queue,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            )
            result = await call_handle.result()
        except WorkflowFailureError as exc:
            raise nexusrpc.HandlerError(
                str(exc.cause or exc),
                type=nexusrpc.HandlerErrorType.INTERNAL,
                retryable_override=False,
            ) from exc
        return CallToolOutput(result=result)
