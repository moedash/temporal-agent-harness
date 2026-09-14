# Server-side streams fork and branch setup

The harness normally streams agent events through `temporalio.contrib.workflow_streams`, which
batches items into Signals and reads them back with a long-polling Update. This branch runs the
same harness on server-side streams instead: the payload goes to a Temporal-owned log beside
Workflow History, and a reader reads that log.

Nothing about how an agent is written changes. `AgentWorkflowRunner` still takes a
`WorkflowStream`, a Workflow still publishes with a plain call, an Activity still publishes
through a buffered handle, and `AgentClient` still merges a parent with its subagents. The
import moved and the plumbing underneath it is different.

## Repository topology

| Component | Repository | Branch |
|---|---|---|
| Agent harness | `https://github.com/moedash/temporal-agent-harness.git` | `moe/AI-198-server-streams` |
| Temporal Python SDK | `https://github.com/moedash/sdk-python.git` | `moe/AI-198-stream-client` |
| Temporal server | `https://github.com/moedash/temporal.git` | `moe/AI-198-server-side-streams` |

The server is the part that has no released equivalent. Server-side streams are a prototype in
the server itself, so this harness needs a server built from that branch. There is no version of
this that runs against Temporal Cloud or a released binary today.

`pyproject.toml` pins the Python SDK to an exact commit rather than to a branch, so the same
harness revision cannot resolve to a different stream implementation later.

## Run it

Build and start the server from its branch:

```bash
cd <temporal-checkout>
go build -o temporal-server ./cmd/server
./temporal-server --root <config-dir> --config . --env development --allow-no-auth start
temporal operator namespace create --address 127.0.0.1:7233 --namespace default --retention 24h
```

Then, from a checkout of this branch:

```bash
uv sync --group examples
cp .env.example .env.local
chmod 600 .env.local
just app-install
```

Set `OPENAI_API_KEY` and/or `GEMINI_API_KEY` in `.env.local` for the model-backed examples. The
Monty dynamic agent needs neither: its turns are pre-written scripts, so it exercises the whole
stream path without a model.

The tests that drive a Workflow take the server address from `TEMPORAL_STREAM_TARGET`:

```bash
TEMPORAL_STREAM_TARGET=127.0.0.1:7233 uv run pytest tests/harness tests/examples
```

Without it they fall back to the time-skipping test server, where the publish command does not
exist and they fail rather than quietly passing.

The Nexus gateway tests need the dynamic config they would otherwise pass to their own dev
server, so put this in the server's dynamic config file:

```yaml
history.enableChasm: [{value: true}]
history.enableTransitionHistory: [{value: true}]
history.enableCHASMCallbacks: [{value: true}]
history.enableCHASMSignalBacklinks: [{value: true}]
nexusoperation.enableStandalone: [{value: true}]
system.refreshNexusEndpointsMinWait: [{value: "0s"}]
history.enableUpdateCallbacks: [{value: true}]
```

## What changed

**The import.** `temporalio.contrib.workflow_streams` became `temporalio.contrib.server_streams`.
That is the whole change in every example and in most of the library.

**The read-start hint.** `AgentMessageReply.accepted_offset` is gone. The Workflow used to read
its own log head inside the update handler, because the log was Workflow state. It is not any
more, and a Workflow cannot read the server's copy without doing I/O. `AgentClient` snapshots the
tail before submitting instead, which is all the stream-merge needed of it: a position no later
than this turn's `turn_started`.

**The Nexus gateway's `pollMessages`.** It used to start the Workflow Stream's private poll
Update and hand the caller an async completion token, because an Update was the only thing that
could wait for an event. A stream read blocks on the server until something arrives, so the
operation answers directly. Nothing is parked on the agent's Workflow while a caller sits idle.

**A dependency.** The stream service is reached over its own gRPC channel, because sdk-core does
not know that service yet, so the harness now needs `temporalio[grpc]`.

## Two tests changed meaning

Both asserted a limitation that does not exist here, so both were inverted rather than deleted.

`test_attach_after_stopped_subagent_degrades_gracefully` drove a subagent, stopped it, and
reattached. Under Workflow Streams a completed Workflow's stream could not be read, so the merge
had to give up on the child, release its close gate, and emit a `subagent_stream_unavailable`
marker in place of its detail. A stream outlives its Workflow, so the stopped child now replays
in full and no marker is produced. It is now
`test_attach_after_stopped_subagent_still_replays_its_detail`. The degradation path itself is
untouched and still covers a child whose stream really is unreachable.

`test_poll_messages_delivers_via_async_callback` asserted that the Nexus poll went async, which
it had to, because parking an Update was the only way to wait. It is now
`test_poll_messages_delivers_without_parking_an_update` and asserts the opposite.

## What is different at runtime

- **History stops growing with the stream.** A batch of events costs History one fixed-size event
  naming an offset range, whatever the batch holds. Nothing published is in History, so the 50MB
  and 51,200-event ceilings stop being a function of how much an agent says.
- **Readers are not capped at ten.** A reader holds no in-flight Update, so the per-Workflow
  concurrent-update cap no longer bounds how many consumers one agent can have, and an abandoned
  reader parks nothing on the agent.
- **A finished session stays readable.** The stream outlives the Workflow, until its retention
  expires. `stream_merge`'s stall backstop for a stopped subagent still exists, but the case it
  was written for, a completed child whose stream cannot be replayed, no longer arises.
- **A reader is released when the agent ends.** Nothing can be added to a stream inside a closed
  execution, so a subscription tailing a finished agent ends rather than waiting forever.

## What this costs

- **It needs a server build.** The time-skipping test server is a released Temporal, so a Workflow
  that publishes cannot run on it. An external store leaves that path alone. This is the clearest
  practical cost of putting the payload in Temporal, and it is worth weighing against not running
  a second datastore.
- **An Activity's publish is a transition.** A Workflow's own publish rides its Workflow Task and
  costs nothing extra. A publish from anywhere else costs one transition on the agent's execution
  per batch, which is why the Activity-side handle buffers.
- **The payload skips the codec.** Values go through the payload converter, so typed decode works,
  but the codec chain that would encrypt or compress them does not run. The prototype's stream
  client owns its own connection and does not have it.

## Not done

- The UI and the connectors were not touched. They talk to `AgentClient` and the Nexus gateway,
  both of which kept their shapes, but neither has been run on this branch.
- Cassandra is untested, so nothing here says what this costs on it.
- A Workflow terminated rather than completed has not been checked against a live reader.
