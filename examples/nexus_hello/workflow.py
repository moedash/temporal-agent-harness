"""A hello-world OpenAI Agents SDK agent whose tools are reached through the Nexus-transport
MCP server. See worker.py - this is enabled via `use_nexus_mcp_transport=true` param when we create
the OpenAIAgentsPlugin that gets passed into this workflow.

For the demo, we provide 2 tools, both demonstrating different ways of integrating with Nexus:

  - demo_get_fun_fact: a 3rd-party (non-Nexus) MCP server, reachable through the Durable
    Tools Gateway. This demonstrates how we can use Nexus to broker calls between the harness
    and an existing MCP server that the user may not have source code to.
    We use Nexus to broker agent <-> gateway (which is a Nexus service), and the demo_get_fun_fact
    MCP server is registered against the gateway. The gateway acts as a proxy and can potentially
    hold auth, creds, etc... and make tool calls to demo_get_fun_fact durable.

  - demo-nexus_get_lucky_number: a Nexus service that acts as an MCP server, which we can register
    against the harness directly. This demonstrates how we can use Nexus to broker calls between
    MCP client <-> server seamlessly when the server is a Nexus service.

When running the demo, look at event history of a single turn to see how each tool is invoked
and how Nexus brokers the transport.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream

from agents import Agent as OpenAIAgent
from agents import Runner, TResponseInputItem

from temporal_agent_harness.harness import agent
from temporal_agent_harness.harness.agent_protocol import (
    AgentConfig,
    TextMessage,
    TextReply,
    ToolApprovalPolicy,
)
from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner

TASK_QUEUE = "nexus-hello"
DEFAULT_MODEL = "gpt-5.1"

SYSTEM_INSTRUCTION = """\
You are a friendly assistant. Answer the user in brief, natural prose.
"""


@workflow.defn(name="NexusHelloAgent")
@agent.defn
class NexusHelloAgentWorkflow:
    """A conversational agent whose tools are reached via the Nexus-transport MCP server -
    wired in entirely by worker.py's OpenAIAgentsPlugin(use_nexus_mcp_transport=True)."""

    @workflow.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            # Hello-world stance: don't gate tool calls (this MCP tool bypasses the harness
            # approval gate entirely regardless - see the openai-agents-nexus-transport
            # integration notes).
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )
        self._conversation: list[TResponseInputItem] = []

    @workflow.run
    async def run(self, _config: AgentConfig) -> None:
        await self._runner.run(self)

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask the agent a question; it may use its Nexus-transport tools to answer."""
        sdk_agent = OpenAIAgent(
            name="NexusHello",
            instructions=SYSTEM_INSTRUCTION,
            model=DEFAULT_MODEL,
            # No mcp_servers=[...] here, we register Nexus-based MCP servers against the
            # harness using Temporal itself (CLI, SDK, etc...). The plugin sets up
            # signal handlers that allow this registration (see README.md for details).
            # If the client wants to use a vanilla MCP server, they
            # can append them here and we will not use Nexus to broker the transport.
        )
        input_items: list[TResponseInputItem] = [
            *self._conversation,
            {"role": "user", "content": message.text},
        ]

        result = Runner.run_streamed(sdk_agent, input=input_items, context=self._runner)
        async for _ in result.stream_events():
            pass

        self._conversation = result.to_input_list()
        return TextReply(text=str(result.final_output))
