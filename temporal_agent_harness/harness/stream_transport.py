# ABOUTME: How the harness reaches its streams: the one place a provider is named, the batched
# publisher an activity uses, and the readers every client-side consumer of turn events shares.
# Workflow code publishes through ``workflow.stream_writer`` and never names a store.

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

from temporalio import activity
from temporalio.client import Client
from temporalio.streams import (
    BEGINNING,
    Cursor,
    RecordKind,
    StreamProducer,
    StreamProvider,
    StreamRecord,
)

from temporal_agent_harness.harness.agent_protocol import TURN_EVENTS_TOPIC, AgentEvent

PROVIDER_ENV = "STREAMS_PROVIDER"
DEFAULT_PROVIDER = "workflow_streams"

_process_provider: StreamProvider | None = None


def provider_name() -> str:
    """The provider this process names, read from ``STREAMS_PROVIDER``."""
    return os.environ.get(PROVIDER_ENV, DEFAULT_PROVIDER)


def provider_from_env() -> StreamProvider:
    """The stream provider ``STREAMS_PROVIDER`` names, built once per process.

    ``workflow_streams`` (today's Workflow Streams, nothing to run) is the default. ``redis``
    reads ``AI198_REDIS_URL``; ``native`` needs a Temporal server that carries streams;
    ``memory`` is the in-process reference the conformance tests use. The Nexus front is not
    offered: it needs an endpoint name and an HTTP address the harness has no settings for,
    and it cannot serve a worker.

    A worker passes the result as ``Worker(plugins=[provider])``; everything else opens
    handles from it. The first call builds the provider and later calls return the same
    instance: an activity has no runtime to ask its worker for the provider, and the memory
    provider shares nothing between two instances.
    """
    global _process_provider
    if _process_provider is None:
        _process_provider = _build(provider_name())
    return _process_provider


async def close_provider() -> None:
    """Close the provider this process built, if any; the next call builds a new one.

    For a process that is done, and for a test suite that gives every test its own event
    loop, which a provider holding connections cannot outlive.
    """
    global _process_provider
    provider, _process_provider = _process_provider, None
    if provider is not None:
        await provider.close()


def _build(name: str) -> StreamProvider:
    if name == "workflow_streams":
        from temporalio.streams.providers.workflow_streams import WorkflowStreamsProvider

        # The shipped transport polls between deliveries; the UI wants deltas within a few
        # milliseconds of publish.
        return WorkflowStreamsProvider(poll_cooldown=timedelta(milliseconds=10))
    if name == "redis":
        from temporalio.streams.providers.redis import RedisStreams

        return RedisStreams(url=os.environ.get("AI198_REDIS_URL", "redis://127.0.0.1:6379"))
    if name == "native":
        from temporalio.streams.providers.native import NativeStreams

        return NativeStreams()
    if name == "memory":
        from temporalio.streams.providers.memory import MemoryStreams

        return MemoryStreams()
    raise ValueError(f"{PROVIDER_ENV}={name!r} names no stream provider")


def cursor(token: str) -> Cursor:
    """The cursor a stored token names, or the beginning when the token is empty."""
    return Cursor(token) if token else BEGINNING


async def latest_turn_event(provider: StreamProvider, client: Client, workflow_id: str) -> str:
    """The token of ``workflow_id``'s newest turn event, or empty when it has none.

    A client about to send a message reads this first, then follows the stream after it, so
    it sees the turn it started without replaying the agent's history.
    """
    handle = provider.get_stream_handle(client, workflow_id)
    return (await handle.latest(topic=TURN_EVENTS_TOPIC)).token


def follow_turn_events(
    provider: StreamProvider, client: Client, workflow_id: str, *, after: str = ""
) -> AsyncGenerator[StreamRecord[AgentEvent], None]:
    """Yield ``workflow_id``'s turn events after the record ``after`` names, then tail live.

    Only data records come out; a producer's finish marker and the supersession a reader
    synthesizes when an activity is retried are not turn events. Each record carries the
    cursor to store for a later ``after``. A token another provider minted is refused by this
    call with ``StreamCursorError``, before anything is read. Closing the generator closes the
    subscription, which matters on the transport that parks a long poll against the workflow.
    """
    handle = provider.get_stream_handle(client, workflow_id)
    records = handle.read(topic=TURN_EVENTS_TOPIC, after=cursor(after), result_type=AgentEvent)
    return _data_records(records)


async def _data_records(
    records: AsyncGenerator[StreamRecord[AgentEvent], None],
) -> AsyncGenerator[StreamRecord[AgentEvent], None]:
    try:
        async for record in records:
            if record.kind is RecordKind.DATA:
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

    def __init__(self, producer: StreamProducer, *, batch_interval: timedelta) -> None:
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
    topic: str,
    *,
    provider: StreamProvider | None = None,
    batch_interval: timedelta = timedelta(milliseconds=50),
) -> AsyncIterator[ActivityPublisher]:
    """A batched publisher onto ``topic`` of the stream this activity's workflow publishes.

    The producer carries the activity's own id and attempt, so a retry's records deduplicate
    and a new attempt is reported to readers as a supersession, and the records land next
    to the workflow's own for whoever follows the topic. ``provider`` defaults to the one
    this process built from the environment.
    """
    handle = (provider or provider_from_env()).get_stream_handle(
        activity.client(), activity.info().workflow_id
    )
    producer = handle.producer(topic=topic)
    async with ActivityPublisher(producer, batch_interval=batch_interval) as publisher:
        yield publisher
