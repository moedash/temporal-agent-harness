"""WorkflowTransport: In-process MCP transport backed by Temporal Nexus.

Every tool source is reachable over Nexus, so MCP client <-> server calls can go straight
through a Nexus call instead of a real MCP session.

Usage inside a Temporal workflow::

    transport = WorkflowTransport(registered_servers=registry.servers)
    async with transport.connect_client_session() as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools  = await session.list_tools()
            result = await session.call_tool("tool_name", {"url": "..."})
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import anyio
import anyio.streams.memory
import pydantic
from authoring import CALL_TOOL_OPERATION, LIST_TOOLS_OPERATION
from mcp import types
from mcp.shared.message import SessionMessage
from temporalio import workflow


class WorkflowTransport:
    """In-process MCP transport backed uniformly by Temporal Nexus.

    Args:
        registered_servers: Live ``{name: endpoint}`` map of registered MCP servers/proxies.
                            For now, I'm leaving this map as an externally-mutated map to
                            allow the workflow to implement its own way of registering/deregistering servers.
                            The WorkflowTransport will just read from it when listing and calling tools.
        name: A readable name for this transport instance.
    """

    def __init__(
        self,
        registered_servers: Mapping[str, str],
        name: str = "workflow-transport",
    ) -> None:
        self._registered_servers = registered_servers
        self._name = name
        # Tool prefix -> (registered name, endpoint), rebuilt on every list_tools() call.
        self._tool_routes: dict[str, tuple[str, str]] = {}

    @property
    def name(self) -> str:
        return self._name

    # -- Public API: async context manager ---------------------------------------

    @asynccontextmanager
    async def connect_client_session(
        self,
    ) -> AsyncGenerator[
        tuple[
            anyio.streams.memory.MemoryObjectReceiveStream[SessionMessage],
            anyio.streams.memory.MemoryObjectSendStream[SessionMessage],
        ],
        None,
    ]:
        """
        Open an in-process MCP transport pair compatible with `mcp.ClientSession`, for a caller wanting
        mcp.ClientSession async context manager. Example usage:

            transport = WorkflowTransport(registered_servers=registry.servers)
            async with transport.connect_client_session() as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    tools  = await session.list_tools()
                    result = await session.call_tool("tool_name", {"url": "..."})
        """
        client_write, transport_read = anyio.create_memory_object_stream(0)  # type: ignore[var-annotated]
        transport_write, client_read = anyio.create_memory_object_stream(0)  # type: ignore[var-annotated]

        async def _router() -> None:
            try:
                async for session_message in transport_read:
                    request = session_message.message.root
                    if not isinstance(request, types.JSONRPCRequest):
                        continue  # ignore notifications etc.

                    result: types.Result | types.ErrorData
                    try:
                        match request:
                            case types.JSONRPCRequest(method="initialize"):
                                result = self._initialize(
                                    types.InitializeRequestParams.model_validate(request.params)
                                )
                            case types.JSONRPCRequest(method="tools/list"):
                                result = types.ListToolsResult(tools=await self.list_tools())
                            case types.JSONRPCRequest(method="tools/call"):
                                result = await self._call_tool(
                                    types.CallToolRequestParams.model_validate(request.params)
                                )
                            case _:
                                result = types.ErrorData(
                                    code=types.METHOD_NOT_FOUND,
                                    message=f"Unknown method: {request.method}",
                                )
                    except pydantic.ValidationError as exc:
                        result = types.ErrorData(
                            code=types.INVALID_PARAMS,
                            message=f"Invalid request params: {exc}",
                        )

                    payload = {"jsonrpc": "2.0", "id": request.id}
                    payload["result" if isinstance(result, types.Result) else "error"] = (
                        result.model_dump()
                    )
                    response = types.JSONRPCResponse.model_validate(payload)
                    await transport_write.send(SessionMessage(types.JSONRPCMessage(root=response)))

            except anyio.ClosedResourceError:
                pass
            finally:
                await transport_write.aclose()

        router_task = asyncio.create_task(_router())
        try:
            yield client_read, client_write
        finally:
            await client_write.aclose()
            router_task.cancel()
            try:
                await router_task
            except asyncio.CancelledError:
                # Only swallow our own cancellation of router_task -- re-raise if the outer
                # task itself is being cancelled (e.g. workflow eviction), or eviction hangs.
                current = asyncio.current_task()
                if current is not None and current.cancelling() > 0:
                    raise
            await transport_read.aclose()


    # -- Public API: direct list/call tools --------------------------------------

    async def list_tools(self) -> list[types.Tool]:
        """Fan out across every registered entry, and rebuild the tool-prefix route table
        from whatever answered for each tool."""

        # Helper that uses Nexus to call list_tools() on a name + endpoint.
        async def _fetch(name: str, endpoint: str) -> list[dict[str, Any]]:
            client = workflow.create_nexus_client(service=name, endpoint=endpoint)
            result: Any = await client.execute_operation(LIST_TOOLS_OPERATION, None)
            return result.get("tools", []) if isinstance(result, dict) else []

        # Snapshot so a concurrent registration doesn't change the set mid-fan-out.
        servers = dict(self._registered_servers)
        names = list(servers.keys())
        results = await asyncio.gather(
            *(_fetch(name, endpoint) for name, endpoint in servers.items()),
            return_exceptions=True,
        )

        tool_dicts: list[dict[str, Any]] = []
        new_routes: dict[str, tuple[str, str]] = {}
        for name, result in zip(names, results):
            if isinstance(result, BaseException):
                workflow.logger.warning(
                    "[workflow-transport] list_tools failed for %r: %s", name, result
                )
                continue
            endpoint = servers[name]
            for tool_dict in result:
                service = str(tool_dict.get("name", "")).partition("_")[0]
                new_routes[service] = (name, endpoint)
                tool_dicts.append(tool_dict)
        self._tool_routes = new_routes

        return [types.Tool(**d) for d in tool_dicts]

    async def call_tool(
        self, tool_name: str, arguments: dict[str, Any] | None, meta: dict[str, Any] | None = None
    ) -> types.CallToolResult:
        """Route via the table list_tools built: direct Nexus call if the tool's prefix
        matches its registered name, else the generic call_tool contract (a proxy)."""
        params_kwargs: dict[str, Any] = {"name": tool_name, "arguments": arguments}
        if meta is not None:
            params_kwargs["_meta"] = types.RequestParams.Meta.model_validate(meta)
        return await self._call_tool(types.CallToolRequestParams(**params_kwargs))


    # -- Implementation details --------------------------------------------------

    def _initialize(self, params: types.InitializeRequestParams) -> types.InitializeResult:
        return types.InitializeResult(
            protocolVersion="2024-11-05",
            capabilities=types.ServerCapabilities(tools=types.ToolsCapability()),
            serverInfo=types.Implementation(name=self._name, version="0.1.0"),
        )

    async def _call_tool(self, params: types.CallToolRequestParams) -> types.CallToolResult:
        service, _, operation = params.name.partition("_")
        if not service or not operation:
            return types.CallToolResult(
                content=[types.TextContent(type="text",
                    text=f"Invalid tool name {params.name!r}: expected 'service_operation'")],
                isError=True,
            )
        arguments = params.arguments or {}
        route = self._tool_routes.get(service)
        if route is None:
            return types.CallToolResult(
                content=[types.TextContent(type="text",
                    text=f"Service {service!r} is not a registered Nexus-native server, "
                         f"and no proxy (e.g. a Durable Tools Gateway) is registered to "
                         f"route it.")],
                isError=True,
            )
        registered_name, endpoint = route
        try:
            client = workflow.create_nexus_client(service=registered_name, endpoint=endpoint)
            if registered_name == service:
                # Nexus-native server -- call its own operation directly.
                result: Any = await client.execute_operation(operation, arguments)
                text = (
                    result if isinstance(result, str)
                    else result.model_dump_json(indent=2) if hasattr(result, "model_dump_json")
                    else str(result)
                )
            else:
                # Proxy answering on behalf of another prefix -- use the generic contract.
                # This is intended to be used together with the Durable Tools Gateway.
                # TODO: flesh out code conditions in slightly clearer manner.
                call_result: Any = await client.execute_operation(
                    CALL_TOOL_OPERATION, {"name": params.name, "arguments": arguments}
                )
                text = (
                    call_result.get("result") or ""
                    if isinstance(call_result, dict)
                    else str(call_result)
                )
        except Exception as exc:
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=str(exc))],
                isError=True,
            )

        return types.CallToolResult(
            content=[types.TextContent(type="text", text=text)]
        )
