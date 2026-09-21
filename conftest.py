"""Suite-wide fixtures.

Every test gets its own event loop, and a stream provider that holds connections (Redis, the
native stream service) binds them to the loop it first used. Closing the process's provider
after each test makes the next test build a fresh one on its own loop; the memory provider
starts empty, which each test expects anyway.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from temporal_agent_harness.harness import stream_transport


@pytest.fixture(autouse=True)
async def _stream_provider_per_test() -> AsyncIterator[None]:
    yield
    await stream_transport.close_provider()
