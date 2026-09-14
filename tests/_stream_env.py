"""Where the stream-backed tests get a Temporal server.

Server-side streams are not in any Temporal release, and the time-skipping test
server is a released one, so a test whose Workflow publishes to a stream cannot
run against it. ``TEMPORAL_STREAM_TARGET`` names a server built from the
prototype branch; without it these tests keep using the test server, where the
publish command is unrecognized and they fail rather than quietly pass.

This is the cost of the payload living in Temporal. A design that keeps it in an
external store leaves the test server usable, and that is worth weighing.
"""

from __future__ import annotations

import os
from typing import Any

from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment

STREAM_TARGET_ENV = "TEMPORAL_STREAM_TARGET"


def stream_server_target() -> str | None:
    """The prototype server to run against, if one was named."""
    return os.environ.get(STREAM_TARGET_ENV)


async def stream_workflow_environment(**kwargs: Any) -> WorkflowEnvironment:
    """A server that understands the stream commands, when one was named."""
    target = stream_server_target()
    if target:
        return WorkflowEnvironment.from_client(await Client.connect(target, **kwargs))
    return await stream_workflow_environment(**kwargs)
