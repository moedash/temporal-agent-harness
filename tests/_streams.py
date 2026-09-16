"""Where the stream-backed tests get their provider, their server, and their turn events.

``STREAMS_PROVIDER`` picks the provider the way it does for a worker. The default,
``workflow_streams``, is today's transport and runs on the time-skipping test server, so the
suite needs nothing installed. ``native`` needs a server built from the stream branch:
``TEMPORAL_ADDRESS`` names it and the tests connect there instead. ``redis`` needs a Redis at
``AI198_REDIS_URL`` and runs on the test server like the default. ``memory`` is the in-process
reference provider.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment

from temporal_agent_harness.harness.agent_protocol import AgentEvent
from temporal_agent_harness.harness.stream_transport import (
    configure_from_env,
    follow_turn_events,
    worker_options,
)

__all__ = ["configure", "turn_events", "worker_options", "workflow_environment"]


def configure() -> str:
    """Name the provider for this test process. Safe to call more than once."""
    return configure_from_env()


async def workflow_environment(**kwargs: Any) -> WorkflowEnvironment:
    """A server the configured provider can run on.

    The native provider's streams live in the server, and no released server carries them, so
    that provider connects to the one ``TEMPORAL_ADDRESS`` names rather than starting the
    time-skipping one.
    """
    if configure() == "native":
        address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
        return WorkflowEnvironment.from_client(await Client.connect(address, **kwargs))
    return await WorkflowEnvironment.start_time_skipping(**kwargs)


async def turn_events(
    client: Client, workflow_id: str, *, after: str = ""
) -> AsyncIterator[AgentEvent]:
    """Yield ``workflow_id``'s turn events from the beginning (or after a cursor), tailing live."""
    records = follow_turn_events(client, workflow_id, after=after)
    try:
        async for record in records:
            yield record.value
    finally:
        await records.aclose()
