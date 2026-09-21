# ABOUTME: Tests for the batched publisher an activity streams through, on a fake producer.
# They pin what leaving the context guarantees: every queued value reaches the producer.

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS,
    TURN_EVENTS_TOPIC,
    AgentEvent,
)
from temporal_agent_harness.harness.stream_transport import ActivityPublisher


class _SlowProducer:
    """A producer whose append suspends a few times before landing, like a real RPC."""

    def __init__(self) -> None:
        self.appended: list[list[Any]] = []
        self.entered = asyncio.Event()

    async def append(self, *values: Any) -> None:
        self.entered.set()
        for _ in range(3):
            await asyncio.sleep(0)
        self.appended.append(list(values))


async def test_exit_lets_the_batch_in_flight_land():
    producer = _SlowProducer()
    async with ActivityPublisher(producer, batch_interval=timedelta(0)) as publisher:
        publisher.publish("a")
        # The flusher is inside append with ["a"] when the context closes.
        await producer.entered.wait()
    assert producer.appended == [["a"]]


async def test_exit_sends_the_tail_without_waiting_for_the_interval():
    producer = _SlowProducer()
    async with ActivityPublisher(producer, batch_interval=timedelta(hours=1)) as publisher:
        publisher.publish("a")
        publisher.publish("b")
    assert producer.appended == [["a", "b"]]


async def test_publish_after_exit_is_refused():
    producer = _SlowProducer()
    async with ActivityPublisher(producer, batch_interval=timedelta(hours=1)) as publisher:
        pass
    with pytest.raises(RuntimeError):
        publisher.publish("late")
    assert producer.appended == []


def test_turn_events_definition_names_the_wire_topic():
    """The typed definition and the wire string are one topic, decoding to the envelope."""
    assert TURN_EVENTS.name == TURN_EVENTS_TOPIC
    assert TURN_EVENTS.result_type is AgentEvent
