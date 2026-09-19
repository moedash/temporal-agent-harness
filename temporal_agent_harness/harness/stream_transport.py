# ABOUTME: How the harness reaches its streams: the one place a provider is named, the batched
# publisher an activity uses, and the reader every client-side consumer of turn events shares.
# Workflow and activity code publish through ``temporalio.streams`` and never name a store.

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

from temporalio import activity, streams

from temporal_agent_harness.harness.agent_protocol import TURN_EVENTS_TOPIC, AgentEvent

PROVIDER_ENV = "STREAMS_PROVIDER"
DEFAULT_PROVIDER = "workflow_streams"


def configure_from_env() -> str:
    """Name this process's stream provider from ``STREAMS_PROVIDER``.

    ``workflow_streams`` (today's Workflow Streams, nothing to run) is the default. ``redis``
    reads ``AI198_REDIS_URL``; ``native`` needs a Temporal server that carries streams;
    ``memory`` is the in-process reference the conformance tests use. Call it once, before
    building a ``Worker`` or opening a producer or consumer. Returns the name it chose.
    """
    name = os.environ.get(PROVIDER_ENV, DEFAULT_PROVIDER)
    options: dict[str, Any] = {}
    if name == "workflow_streams":
        # The shipped transport polls between deliveries; the UI wants deltas within a
        # few milliseconds of publish.
        options["poll_cooldown"] = timedelta(milliseconds=10)
    streams.configure(provider=name, **options)
    return name


def worker_options() -> dict[str, Any]:
    """What every ``Worker`` in this process passes through, whichever provider is named."""
    return streams.worker_options()


def cursor(token: str) -> streams.Cursor:
    """The cursor a stored token names, or the beginning when the token is empty."""
    return streams.Cursor(token) if token else streams.BEGINNING


async def latest_turn_event(client: Any, workflow_id: str) -> str:
    """The token of ``workflow_id``'s newest turn event, or empty when it has none.

    A client about to send a message reads this first, then follows the stream after it, so
    it sees the turn it started without replaying the agent's history.
    """
    consumer = await streams.consumer(client, workflow_id=workflow_id)
    return (await consumer.latest(topic=TURN_EVENTS_TOPIC)).token


async def follow_turn_events(
    client: Any, workflow_id: str, *, after: str = ""
) -> AsyncIterator[streams.StreamRecord[AgentEvent]]:
    """Yield ``workflow_id``'s turn events after the record ``after`` names, then tail live.

    Only data records come out; a producer's finish marker and the supersession a reader
    synthesizes when an activity is retried are not turn events. Each record carries the
    cursor to store for a later ``after``. Closing this generator closes the subscription,
    which matters on the transport that parks a long poll against the workflow.
    """
    consumer = await streams.consumer(client, workflow_id=workflow_id)
    records = consumer.read(after=cursor(after), topic=TURN_EVENTS_TOPIC, type=AgentEvent)
    try:
        async for record in records:
            if record.kind is streams.RecordKind.DATA:
                yield record
    finally:
        await records.aclose()


class ActivityPublisher:
    """A synchronous ``publish`` over the interface's asynchronous producer.

    Activities publish from places that cannot await, such as an SDK's streaming callback,
    so ``publish`` only queues. A background task appends what has queued every
    ``batch_interval``, and leaving the context appends the tail, so nothing is lost and
    nothing is sent one record at a time.
    """

    def __init__(self, producer: streams.Producer, *, batch_interval: timedelta) -> None:
        self._producer = producer
        self._interval = batch_interval.total_seconds()
        self._pending: list[Any] = []
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        # Closing wakes the flusher rather than cancelling it: a cancel landing inside an
        # append would unwind ``flush`` with the batch it had already taken off ``_pending``.
        self._wake = asyncio.Event()

    def publish(self, value: Any) -> None:
        if self._closed:
            raise RuntimeError("publisher is closed")
        self._pending.append(value)

    async def flush(self) -> None:
        batch, self._pending = self._pending, []
        if batch:
            await self._producer.append(*batch)

    async def _run(self) -> None:
        while not self._closed:
            try:
                await asyncio.wait_for(self._wake.wait(), self._interval)
            except TimeoutError:
                pass
            await self.flush()

    async def __aenter__(self) -> ActivityPublisher:
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._closed = True
        self._wake.set()
        if self._task is not None:
            await self._task
        # The tail goes out even when the activity is failing, so a reader sees what was
        # produced up to the failure.
        await self.flush()


@asynccontextmanager
async def publisher_for_activity(
    topic: str, *, batch_interval: timedelta = timedelta(milliseconds=50)
) -> AsyncIterator[ActivityPublisher]:
    """A batched publisher onto ``topic`` of the stream this activity's workflow publishes.

    The producer carries the activity's own id and attempt, so a retry's records deduplicate
    and a new attempt is reported to readers as a supersession, and the records land next
    to the workflow's own for whoever follows the topic.
    """
    producer = await streams.producer(
        activity.client(), workflow_id=activity.info().workflow_id, topic=topic
    )
    async with ActivityPublisher(producer, batch_interval=batch_interval) as publisher:
        yield publisher
