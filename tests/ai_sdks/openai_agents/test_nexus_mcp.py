# ABOUTME: Tests the Nexus-transport MCP server (nexus_transport_mcp_server /
# _NexusTransportMCPServer / NexusMcpServerRegistry). Eight things need proving, since none
# are obvious from reading the code alone:
#   1. TemporalOpenAIRunner._prepare_workflow_run allowlists it (via _DurableMCPServerMarker)
#      and still rejects an arbitrary MCPServer subclass — a plain unit test, no server.
#   2. WorkflowTransport is uniform: list_tools fans out over the gateway (a REAL
#      ToolRegistryWorkflow + RegistryServiceHandler, reached via a real Nexus Endpoint,
#      registered against the calling workflow under its own real Nexus service name,
#      REGISTRY_SERVICE_NAME) AND a Nexus-native server registered directly against the same
#      workflow's own NexusMcpServerRegistry — both via the exact same "register_mcp_server"
#      signal (nothing else to declare), no separate worker-level gateway config and no
#      central registry involved. WorkflowTransport tells the two apart structurally, from
#      what list_tools returns (see workflow_transport.py's module docstring).
#   3. call_tool routes each tool to the right place: direct Nexus for the registered
#      Nexus-native server, the gateway's own generic call_tool op (-> ToolCallWorkflow, a
#      plain workflow wrapping mcp_proxy_activity -- NOT a standalone activity, which needs
#      an experimental server capability observed to deadlock the caller in real usage) for
#      the 3rd-party one, and a clean is_error result (not a raised exception) for a name
#      neither knows about.
#   4. NexusMcpServerRegistry's registration signal takes effect live, mid-run — the probe
#      workflow blocks on workflow.wait_condition until a registration lands, then the test
#      signals it, exactly as an external Nexus-native server's own worker would.
#   5. OpenAIAgentsPlugin(use_nexus_mcp_transport=True) actually makes the Nexus-transport MCP server
#      opaque: an Agent built with no mcp_servers=[...] at all still gets one, transparently,
#      via TemporalOpenAIRunner._prepare_workflow_run — a fresh instance per Runner.run() call,
#      not one shared instance for the whole workflow.
#   6. A REAL Agent, driven through Runner.run_streamed() (not Runner.run()) against a fake
#      streaming model, actually calls both a 1st-party Nexus-native tool and a gateway-routed
#      3rd-party tool -- an earlier version of _NexusTransportMCPServer drove a real (if
#      entirely in-process/fake) MCP ClientSession, and holding that open while
#      Runner.run_streamed()'s own streaming machinery was also active deadlocked (TMPRL1101)
#      in real usage; now fixed structurally by not having a session at all (see
#      _nexus_mcp.py's module docstring).
#   7. Two tool calls requested in the SAME turn (parallel_tool_calls) are dispatched
#      concurrently by the OpenAI Agents SDK and share one _NexusTransportMCPServer instance
#      -- with no shared session left to race (item 6), this is just two concurrent
#      coroutines, as reliable as calling execute_operation() directly.
#   8. WorkflowTransport's list_tools/call_tool/name work standalone, no agents.mcp import
#      needed — _NexusTransportMCPServer wraps it (composition) to satisfy MCPServer's ABC;
#      list_prompts/get_prompt are that ABC's requirements, not WorkflowTransport's.
#
# _NexusTransportMCPServer no longer has a sandboxing restriction (an earlier version drove a
# real anyio-based MCP session and needed @workflow.defn(sandboxed=False); confirmed live that
# the current, session-less implementation runs fine in a normally-sandboxed workflow), so
# there's no "fails fast when sandboxed" test here anymore.
#
# Run with: uv run --extra nexus-mcp pytest tests/ai_sdks/openai_agents/test_nexus_mcp.py -v

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest

pytest.importorskip("transport")  # requires the `nexus-mcp` extra (Python >=3.13)

import nexusrpc
import nexusrpc.handler
import pytest_asyncio
import temporalio.api.nexus.v1 as nexus_api
import temporalio.api.operatorservice.v1 as operator_api
from agents import Agent
from agents.mcp import MCPServer
from mcp.types import CallToolResult, GetPromptResult, ListPromptsResult
from mcp.types import Tool as MCPTool
from openai.types.responses import ResponseFunctionToolCall
from temporalio import activity
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from authoring import MCPOverNexusServiceHandler, nexus_mcp_tool
from durable_tools_gateway import (
    REGISTRY_SERVICE_NAME,
    REGISTRY_WORKFLOW_ID,
    ExternalMCPCallInput,
    RegistryServiceHandler,
    ToolCallWorkflow,
    ToolRegistryWorkflow,
)
from temporal_agent_harness.ai_sdks.openai_agents import OpenAIAgentsPlugin
from temporal_agent_harness.ai_sdks.openai_agents._model_parameters import ModelActivityParameters
from temporal_agent_harness.ai_sdks.openai_agents._openai_runner import TemporalOpenAIRunner
from temporal_agent_harness.ai_sdks.openai_agents.testing import (
    AgentEnvironment,
    ResponseBuilders,
    TestStreamingModel,
)
from temporal_agent_harness.harness.agent_protocol import AgentConfig
from _nexus_mcp_probe_workflows import (
    AgentToolCallE2EProbe,
    AutoInjectionProbe,
    BareWorkflowTransportProbe,
    GATEWAY_ENDPOINT,
    NexusTransportProbe,
    NEXUS_NATIVE_SERVICE_NAME,
)

# ---------------------------------------------------------------------------
# 1. _prepare_workflow_run allowlist — plain unit test, no Temporal server.
# ---------------------------------------------------------------------------


class _ArbitraryMCPServer(MCPServer):
    """A minimal, non-durable MCPServer stand-in — must be rejected."""

    async def connect(self) -> None: ...

    @property
    def name(self) -> str:
        return "arbitrary"

    async def cleanup(self) -> None: ...

    async def list_tools(self, run_context=None, agent=None) -> list[MCPTool]:
        return []

    async def call_tool(self, tool_name, arguments, meta=None) -> CallToolResult:
        raise NotImplementedError

    async def list_prompts(self) -> ListPromptsResult:
        raise NotImplementedError

    async def get_prompt(self, name, arguments=None) -> GetPromptResult:
        raise NotImplementedError


def test_prepare_workflow_run_allows_nexus_transport_server():
    # Constructs _NexusTransportMCPServer directly rather than via the public
    # nexus_transport_mcp_server() factory: that factory now calls
    # nexus_mcp_server_registry(), which needs workflow.instance() -- fine in production
    # (run()/run_streamed() only ever reach _prepare_workflow_run from inside a real
    # workflow), but this test deliberately calls _prepare_workflow_run directly, with no
    # workflow or Temporal server at all, to keep this one fast and dependency-free.
    from temporal_agent_harness.ai_sdks.openai_agents._nexus_mcp import (
        _NexusTransportMCPServer,
    )

    runner = TemporalOpenAIRunner(
        model_params=ModelActivityParameters(start_to_close_timeout=timedelta(seconds=30))
    )
    agent = Agent(
        name="probe",
        instructions="probe",
        mcp_servers=[_NexusTransportMCPServer({})],
    )
    # Should not raise.
    runner._prepare_workflow_run(agent, {})


def test_prepare_workflow_run_rejects_arbitrary_mcp_server():
    runner = TemporalOpenAIRunner(
        model_params=ModelActivityParameters(start_to_close_timeout=timedelta(seconds=30))
    )
    agent = Agent(
        name="probe",
        instructions="probe",
        mcp_servers=[_ArbitraryMCPServer()],
    )
    with pytest.raises(ValueError, match="may not work durably"):
        runner._prepare_workflow_run(agent, {})


# ---------------------------------------------------------------------------
# 2, 3, 4 & 5. Real gateway + real Nexus-native service + live mid-run registration.
# ---------------------------------------------------------------------------

NEXUS_NATIVE_ENDPOINT = "pytest-nexus-native-endpoint"


@nexusrpc.handler.service_handler(name=NEXUS_NATIVE_SERVICE_NAME)
class ProbeNexusToolsServiceHandler(MCPOverNexusServiceHandler):
    """A tiny Nexus-native MCP server double. Exercises TWO things at once, exactly like a
    real one would: nexus_mcp_tool (no separate Pydantic model / Operation[...] needed --
    see its docstring) and list_tools coming for free from MCPOverNexusServiceHandler,
    derived from ping below."""

    @nexus_mcp_tool
    async def ping(self, text: str) -> str:
        """Echo the input text back."""
        return f"echoed:{text}"


@activity.defn(name="fetch_external_tools")
async def fake_fetch_external_tools(name: str, url: str) -> list[dict]:
    """Stands in for nexus_mcp's real outbound fetch — ToolRegistryWorkflow calls this
    (asynchronously, off its own registration signal) to build an external service's tool
    list. Real nexus_mcp code, unmodified, is what consumes this return value."""
    return [
        {
            "name": f"{name}_echo_ping",
            "description": "Echo back the input.",
            "inputSchema": {
                "type": "object",
                "properties": {"text": {"type": "string"}},
                "required": ["text"],
            },
        }
    ]


@activity.defn(name="mcp_proxy_activity")
async def fake_mcp_proxy_activity(input: ExternalMCPCallInput) -> str:
    # Stands in for a real outbound HTTP call to a 3rd-party MCP server. Runs on the
    # GATEWAY's own worker/task queue -- RegistryServiceHandler.call_tool starts
    # ToolCallWorkflow (a plain workflow wrapping this activity), not the calling
    # (agent-side) workflow.
    return f"echoed:{input.arguments.get('text')}"


async def _wait_for_registration(handle, name: str, *, timeout: float = 20.0) -> None:
    """Poll ToolRegistryWorkflow.find until the async external-tool fetch has landed.

    rpc_timeout is set generously (well above the per-query default) since several tests in
    this file each spin up their own embedded (packaged) test server, and polling can lag
    under load when many run back to back.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if (
            await handle.query(
                ToolRegistryWorkflow.find, name, rpc_timeout=timedelta(seconds=10)
            )
            is not None
        ):
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"{name!r} was not registered within {timeout}s")


@pytest_asyncio.fixture
async def env_and_queue():
    # OpenAIAgentsPlugin is what marks "openai"/"agents"/"mcp" as sandbox passthrough
    # modules (see _temporal_openai_agents.py's workflow_runner hook) — without it, the
    # default sandboxed runner tries to re-execute those packages from scratch and trips
    # on things like openai's httpx/urllib usage. Any real worker using this integration
    # already needs this plugin per the README, so wiring it here matches production setup
    # rather than working around it.
    # register_activities=False: this test never calls Runner.run, so skip constructing
    # ModelActivity (which eagerly builds an OpenAI client and needs an API key).
    env = await WorkflowEnvironment.start_time_skipping(
        plugins=[OpenAIAgentsPlugin(register_activities=False)]
    )

    gateway_task_queue = f"gateway-test-{uuid.uuid4()}"
    nexus_native_task_queue = f"nexus-native-test-{uuid.uuid4()}"
    task_queue = f"nexus-mcp-test-{uuid.uuid4()}"

    # Two real Nexus Endpoints, created the same way `just setup-nexus` does for the real
    # example — just via the operator service directly instead of the `temporal` CLI, since
    # the time-skipping test server supports it natively.
    await env.client.operator_service.create_nexus_endpoint(
        operator_api.CreateNexusEndpointRequest(
            spec=nexus_api.EndpointSpec(
                name=GATEWAY_ENDPOINT,
                target=nexus_api.EndpointTarget(
                    worker=nexus_api.EndpointTarget.Worker(
                        namespace=env.client.namespace, task_queue=gateway_task_queue
                    )
                ),
            )
        )
    )
    await env.client.operator_service.create_nexus_endpoint(
        operator_api.CreateNexusEndpointRequest(
            spec=nexus_api.EndpointSpec(
                name=NEXUS_NATIVE_ENDPOINT,
                target=nexus_api.EndpointTarget(
                    worker=nexus_api.EndpointTarget.Worker(
                        namespace=env.client.namespace, task_queue=nexus_native_task_queue
                    )
                ),
            )
        )
    )

    # A real ToolRegistryWorkflow + RegistryServiceHandler — the Durable Tools Gateway.
    # RegistryServiceHandler.call_tool starts ToolCallWorkflow (a plain workflow wrapping
    # mcp_proxy_activity) on this same task queue -- NOT a standalone activity, which needs
    # an experimental server capability the packaged test server doesn't (and shouldn't need
    # to) support; see registry_service_handler.py's call_tool docstring.
    async with Worker(
        env.client,
        task_queue=gateway_task_queue,
        workflows=[ToolRegistryWorkflow, ToolCallWorkflow],
        activities=[fake_fetch_external_tools, fake_mcp_proxy_activity],
        nexus_service_handlers=[RegistryServiceHandler(env.client)],
    ):
        registry_handle = await env.client.start_workflow(
            ToolRegistryWorkflow.run,
            id=REGISTRY_WORKFLOW_ID,
            task_queue=gateway_task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        await registry_handle.signal(
            ToolRegistryWorkflow.register_external, args=["demo", "http://fake.example/mcp"]
        )
        await _wait_for_registration(registry_handle, "demo")

        # A real Nexus-native MCP server double — reached directly, never touching the
        # gateway/registry above at all.
        async with Worker(
            env.client,
            task_queue=nexus_native_task_queue,
            nexus_service_handlers=[ProbeNexusToolsServiceHandler()],
        ):
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[
                    NexusTransportProbe,
                    AutoInjectionProbe,
                    BareWorkflowTransportProbe,
                ],
            ):
                try:
                    yield env.client, task_queue
                finally:
                    await env.shutdown()


@pytest.mark.asyncio
async def test_list_and_call_tool_round_trip_when_unsandboxed(env_and_queue):
    client, task_queue = env_and_queue
    handle = await client.start_workflow(
        NexusTransportProbe.run,
        "hello",
        id=f"nexus-mcp-probe-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    # Register the gateway itself — the SAME "register_mcp_server" signal every other
    # Nexus-reachable service uses, under its own real Nexus service name (nothing else to
    # declare) — then the Nexus-native service directly against THIS running workflow,
    # exactly as an external Nexus-native server's own worker would send it. The workflow
    # blocks on workflow.wait_condition until the direct registration lands, by which point
    # the gateway's (sent first) is already applied too.
    await handle.signal(
        "register_mcp_server", args=[REGISTRY_SERVICE_NAME, GATEWAY_ENDPOINT]
    )
    await handle.signal(
        "register_mcp_server", args=[NEXUS_NATIVE_SERVICE_NAME, NEXUS_NATIVE_ENDPOINT]
    )
    result = await handle.result()

    # list_tools fans out over the proxy (3rd-party "demo", via the gateway) AND the
    # directly-registered Nexus-native server ("probe-nexus") uniformly.
    assert result["tool_names"] == ["demo_echo_ping", "probe-nexus_ping"]

    # call_tool on the gateway-routed name: RegistryServiceHandler.call_tool starts
    # ToolCallWorkflow (a plain workflow wrapping mcp_proxy_activity), which runs fine
    # against the packaged test server -- no experimental server capability needed.
    assert result["gateway_result_text"] == "echoed:hello"
    assert result["gateway_is_error"] is False

    # call_tool on the Nexus-native name dispatches directly, no gateway involved at all.
    assert result["nexus_native_result_text"] == "echoed:hello"
    assert result["nexus_native_is_error"] is False

    # A name neither the local registry nor the gateway knows about is a clean error result,
    # not a raised exception.
    assert result["missing_is_error"] is True


@pytest.mark.asyncio
async def test_call_tool_without_any_registered_proxy_is_a_clean_error(env_and_queue):
    """There's no separate, worker-level "gateway" concept anymore — WorkflowTransport only
    ever consults whatever's registered live against this workflow. With NO proxy registered
    at all, 1st-party Nexus-native registration still works unconditionally (this is the
    whole point of decoupling the two), and a proxy-routed name (like the 3rd-party
    "demo_echo_ping") gets a clean, non-crashing error instead of trying to reach a
    nonexistent proxy.
    """
    client, task_queue = env_and_queue
    handle = await client.start_workflow(
        NexusTransportProbe.run,
        "hello",
        id=f"nexus-mcp-no-proxy-probe-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    # Only the Nexus-native (direct) service registers — no proxy/gateway at all.
    await handle.signal(
        "register_mcp_server", args=[NEXUS_NATIVE_SERVICE_NAME, NEXUS_NATIVE_ENDPOINT]
    )
    result = await handle.result()

    # list_tools only sees the direct server — nothing to fan out to for a proxy.
    assert result["tool_names"] == ["probe-nexus_ping"]
    # The direct tool still works fine, with zero proxy configured anywhere.
    assert result["nexus_native_result_text"] == "echoed:hello"
    assert result["nexus_native_is_error"] is False
    # "demo_echo_ping" (a 3rd-party/proxy-routed name) is unreachable without a registered
    # proxy — confirm it's OUR clean early error, not some other exception.
    assert result["gateway_is_error"] is True
    assert "no proxy" in (result["gateway_result_text"] or "")


@pytest.mark.asyncio
async def test_workflow_transport_works_standalone_without_agents_sdk(env_and_queue):
    """WorkflowTransport's list_tools/call_tool/name work with no
    _NexusTransportMCPServer, no agents.mcp.MCPServer, no OpenAIAgentsPlugin needed.
    """
    client, task_queue = env_and_queue
    handle = await client.start_workflow(
        BareWorkflowTransportProbe.run,
        "hello",
        id=f"bare-workflow-transport-probe-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    await handle.signal(
        "register_mcp_server", args=[REGISTRY_SERVICE_NAME, GATEWAY_ENDPOINT]
    )
    await handle.signal(
        "register_mcp_server", args=[NEXUS_NATIVE_SERVICE_NAME, NEXUS_NATIVE_ENDPOINT]
    )
    result = await handle.result()

    assert result["name"] == "bare-probe"
    assert result["tool_names"] == ["demo_echo_ping", "probe-nexus_ping"]
    assert result["gateway_is_error"] is False


@pytest.mark.asyncio
async def test_nexus_transport_mcp_server_is_auto_injected(env_and_queue):
    """OpenAIAgentsPlugin(use_nexus_mcp_transport=True) / TemporalOpenAIRunner: an Agent
    constructed with NO mcp_servers=[...] at all still gets a working Nexus-transport MCP
    server, transparently -- a fresh one per Runner.run()-shaped call (one per conversation
    turn). No cleanup needed -- the current _NexusTransportMCPServer holds no session or
    background task at all (see _nexus_mcp.py's module docstring).

    tool_names comes back EMPTY here, and that's correct, not a bug: nothing was ever
    registered against AutoInjectionProbe's own NexusMcpServerRegistry -- the injected
    transport is real and functional, but has nothing to fan out to.
    """
    client, task_queue = env_and_queue
    result = await client.execute_workflow(
        AutoInjectionProbe.run,
        id=f"nexus-mcp-auto-inject-probe-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    assert result["injected_count"] == 1
    assert result["distinct_instances_per_call"] is True
    assert result["tool_names"] == []


# ---------------------------------------------------------------------------
# 7. End-to-end: a REAL Agent, driven through Runner.run(), actually calling both a
#    1st-party Nexus-native tool and a gateway-routed 3rd-party tool in sequence, via
#    Runner.run_streamed() (not Runner.run()) -- matching examples/nexus_hello's own ask()
#    handler exactly, since the live deadlock (TMPRL1101) happened on the streaming path.
#    None of the probes above exercise this: they all call
#    _NexusTransportMCPServer.list_tools/call_tool directly, bypassing Runner (and the
#    OpenAI Agents SDK's own tracing/tool-orchestration/streaming machinery around it)
#    entirely.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agent_runner_calls_both_nexus_native_and_gateway_tools():
    test_model = TestStreamingModel.returning_responses(
        [
            ResponseFunctionToolCall(
                arguments='{"text": "hello"}',
                call_id="call1",
                name="probe-nexus_ping",
                type="function_call",
                id="id1",
                status="completed",
            ),
            ResponseFunctionToolCall(
                arguments='{"text": "hello"}',
                call_id="call2",
                name="demo_echo_ping",
                type="function_call",
                id="id2",
                status="completed",
            ),
            ResponseBuilders.response_output_message("done"),
        ]
    )
    async with AgentEnvironment(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(seconds=30),
            streaming_topic="events",
        ),
        model=test_model,
        use_nexus_mcp_transport=True,
    ) as agent_env:
        env = await WorkflowEnvironment.start_time_skipping(
            plugins=[agent_env.openai_agents]
        )

        gateway_task_queue = f"gateway-test-{uuid.uuid4()}"
        nexus_native_task_queue = f"nexus-native-test-{uuid.uuid4()}"
        task_queue = f"nexus-mcp-agent-e2e-test-{uuid.uuid4()}"

        await env.client.operator_service.create_nexus_endpoint(
            operator_api.CreateNexusEndpointRequest(
                spec=nexus_api.EndpointSpec(
                    name=GATEWAY_ENDPOINT,
                    target=nexus_api.EndpointTarget(
                        worker=nexus_api.EndpointTarget.Worker(
                            namespace=env.client.namespace, task_queue=gateway_task_queue
                        )
                    ),
                )
            )
        )
        await env.client.operator_service.create_nexus_endpoint(
            operator_api.CreateNexusEndpointRequest(
                spec=nexus_api.EndpointSpec(
                    name=NEXUS_NATIVE_ENDPOINT,
                    target=nexus_api.EndpointTarget(
                        worker=nexus_api.EndpointTarget.Worker(
                            namespace=env.client.namespace, task_queue=nexus_native_task_queue
                        )
                    ),
                )
            )
        )

        try:
            async with Worker(
                env.client,
                task_queue=gateway_task_queue,
                workflows=[ToolRegistryWorkflow, ToolCallWorkflow],
                activities=[fake_fetch_external_tools, fake_mcp_proxy_activity],
                nexus_service_handlers=[RegistryServiceHandler(env.client)],
            ):
                registry_handle = await env.client.start_workflow(
                    ToolRegistryWorkflow.run,
                    id=REGISTRY_WORKFLOW_ID,
                    task_queue=gateway_task_queue,
                    id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                )
                await registry_handle.signal(
                    ToolRegistryWorkflow.register_external,
                    args=["demo", "http://fake.example/mcp"],
                )
                await _wait_for_registration(registry_handle, "demo")

                async with Worker(
                    env.client,
                    task_queue=nexus_native_task_queue,
                    nexus_service_handlers=[ProbeNexusToolsServiceHandler()],
                ):
                    async with Worker(
                        env.client,
                        task_queue=task_queue,
                        workflows=[AgentToolCallE2EProbe],
                    ):
                        handle = await env.client.start_workflow(
                            AgentToolCallE2EProbe.run,
                            AgentConfig(),
                            id=f"agent-e2e-probe-{uuid.uuid4()}",
                            task_queue=task_queue,
                        )
                        await handle.signal(
                            "register_mcp_server",
                            args=[NEXUS_NATIVE_SERVICE_NAME, NEXUS_NATIVE_ENDPOINT],
                        )
                        # Real (non-simulated) wall-clock timeout -- if this reproduces the
                        # live TMPRL1101 deadlock, fail the test cleanly instead of hanging
                        # the whole suite forever.
                        result = await asyncio.wait_for(handle.result(), timeout=30)
        finally:
            await env.shutdown()

    assert result["final_output"] == "done"
    assert result["tool_outputs"] == {
        "call1": [{"type": "input_text", "text": "echoed:hello"}],
        "call2": [{"type": "input_text", "text": "echoed:hello"}],
    }


# ---------------------------------------------------------------------------
# 8. Concurrent tool calls in the SAME turn (parallel_tool_calls) must not race
#    _NexusTransportMCPServer's shared session teardown.
# ---------------------------------------------------------------------------
#
# The OpenAI Agents SDK dispatches every function_call in one model turn concurrently
# (asyncio.gather) -- both calls below share the SAME _NexusTransportMCPServer instance (one
# per Runner.run_streamed() call). An earlier version of this class drove a real (if entirely
# in-process/fake) MCP ClientSession -- a persistent, stateful session that had to be
# connected, disconnected, and (once made reference-counted to stop tearing itself down out
# from under a sibling call still using it) still occasionally deadlocked (TMPRL1101) under
# concurrent calls in a way never fully root-caused. _NexusTransportMCPServer no longer drives
# a session at all -- list_tools()/call_tool() call straight into WorkflowTransport's own
# handlers (see _nexus_mcp.py's module docstring) -- so there is no shared session lifecycle
# left to race: this test is what confirms that concurrent calls are now just concurrent
# coroutines, as reliable as calling execute_operation() directly.


@pytest.mark.asyncio
async def test_agent_runner_calls_concurrent_tool_calls_without_racing_cleanup():
    test_model = TestStreamingModel.returning_responses(
        [
            # Both calls requested in the SAME turn -- see ResponseBuilders.stream_events'
            # docstring on why a list (not two separate turns) is what actually exercises
            # the SDK's concurrent dispatch.
            [
                ResponseFunctionToolCall(
                    arguments='{"text": "hello"}',
                    call_id="call1",
                    name="probe-nexus_ping",
                    type="function_call",
                    id="id1",
                    status="completed",
                ),
                ResponseFunctionToolCall(
                    arguments='{"text": "hello"}',
                    call_id="call2",
                    name="demo_echo_ping",
                    type="function_call",
                    id="id2",
                    status="completed",
                ),
            ],
            ResponseBuilders.response_output_message("done"),
        ]
    )
    async with AgentEnvironment(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(seconds=30),
            streaming_topic="events",
        ),
        model=test_model,
        use_nexus_mcp_transport=True,
    ) as agent_env:
        env = await WorkflowEnvironment.start_time_skipping(
            plugins=[agent_env.openai_agents]
        )

        gateway_task_queue = f"gateway-test-{uuid.uuid4()}"
        nexus_native_task_queue = f"nexus-native-test-{uuid.uuid4()}"
        task_queue = f"nexus-mcp-agent-e2e-test-{uuid.uuid4()}"

        await env.client.operator_service.create_nexus_endpoint(
            operator_api.CreateNexusEndpointRequest(
                spec=nexus_api.EndpointSpec(
                    name=GATEWAY_ENDPOINT,
                    target=nexus_api.EndpointTarget(
                        worker=nexus_api.EndpointTarget.Worker(
                            namespace=env.client.namespace, task_queue=gateway_task_queue
                        )
                    ),
                )
            )
        )
        await env.client.operator_service.create_nexus_endpoint(
            operator_api.CreateNexusEndpointRequest(
                spec=nexus_api.EndpointSpec(
                    name=NEXUS_NATIVE_ENDPOINT,
                    target=nexus_api.EndpointTarget(
                        worker=nexus_api.EndpointTarget.Worker(
                            namespace=env.client.namespace, task_queue=nexus_native_task_queue
                        )
                    ),
                )
            )
        )

        try:
            async with Worker(
                env.client,
                task_queue=gateway_task_queue,
                workflows=[ToolRegistryWorkflow, ToolCallWorkflow],
                activities=[fake_fetch_external_tools, fake_mcp_proxy_activity],
                nexus_service_handlers=[RegistryServiceHandler(env.client)],
            ):
                registry_handle = await env.client.start_workflow(
                    ToolRegistryWorkflow.run,
                    id=REGISTRY_WORKFLOW_ID,
                    task_queue=gateway_task_queue,
                    id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                )
                await registry_handle.signal(
                    ToolRegistryWorkflow.register_external,
                    args=["demo", "http://fake.example/mcp"],
                )
                await _wait_for_registration(registry_handle, "demo")

                async with Worker(
                    env.client,
                    task_queue=nexus_native_task_queue,
                    nexus_service_handlers=[ProbeNexusToolsServiceHandler()],
                ):
                    async with Worker(
                        env.client,
                        task_queue=task_queue,
                        workflows=[AgentToolCallE2EProbe],
                    ):
                        handle = await env.client.start_workflow(
                            AgentToolCallE2EProbe.run,
                            AgentConfig(),
                            id=f"agent-e2e-probe-{uuid.uuid4()}",
                            task_queue=task_queue,
                        )
                        await handle.signal(
                            "register_mcp_server",
                            args=[NEXUS_NATIVE_SERVICE_NAME, NEXUS_NATIVE_ENDPOINT],
                        )
                        result = await asyncio.wait_for(handle.result(), timeout=30)
        finally:
            await env.shutdown()

    assert result["final_output"] == "done"
    # The actual assertion this test exists for: BOTH concurrently-dispatched calls must
    # have gotten their OWN real result, not an error from a session a sibling call tore
    # down out from under them.
    assert result["tool_outputs"] == {
        "call1": [{"type": "input_text", "text": "echoed:hello"}],
        "call2": [{"type": "input_text", "text": "echoed:hello"}],
    }
