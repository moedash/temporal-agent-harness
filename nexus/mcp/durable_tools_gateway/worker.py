"""Durable Tool Call Gateway Temporal worker. This serves:
- the handler for the registry service, where we can register 3rd-party MCP servers
- the durable tool call flow, which wraps tool calls around an activity that does the actual HTTP call to the 3rd-party MCP server
TODO: convert the ToolCallWorkflow to an SAA once we have SDK support for Nexus-invoked SAA.

Usage (from repo root):
    uv run --extra nexus-mcp --group examples python -m durable_tools_gateway.worker
"""

from __future__ import annotations

import asyncio
import logging

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

from temporal_agent_harness.utils.large_payload import with_large_payload_offload

from .registry import (
    REGISTRY_TASK_QUEUE,
    REGISTRY_WORKFLOW_ID,
    ToolRegistryWorkflow,
    fetch_external_tools,
)
from .registry_service_handler import (
    RegistryServiceHandler,
    ToolCallWorkflow,
    mcp_proxy_activity,
)

logger = logging.getLogger(__name__)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    connect_config = ClientConfig.load_client_connect_config()
    client = await Client.connect(
        **connect_config,
        data_converter=await with_large_payload_offload(pydantic_data_converter),
    )

    await client.start_workflow(
        ToolRegistryWorkflow.run,
        id=REGISTRY_WORKFLOW_ID,
        task_queue=REGISTRY_TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )

    worker = Worker(
        client,
        task_queue=REGISTRY_TASK_QUEUE,
        workflows=[ToolRegistryWorkflow, ToolCallWorkflow],
        activities=[mcp_proxy_activity, fetch_external_tools],
        nexus_service_handlers=[RegistryServiceHandler(client)],
    )
    logger.info("Durable Tool Call Gateway ready — task_queue=%r", REGISTRY_TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
