# ABOUTME: Fast, no-server unit tests for handler.py's pure helper logic.
#
# Run with: uv run pytest tests/nexus_agent_adapter/test_handler_unit.py -v

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from nexusrpc import HandlerError, HandlerErrorType
from temporalio.streams import StreamCursorError

from temporal_agent_harness.nexus_agent_adapter import handler as handler_mod
from temporal_agent_harness.nexus_agent_adapter.handler import (
    AgentServiceHandler,
    Config,
    _is_workflow_already_completed,
)
from temporal_agent_harness.nexus_agent_adapter.generated import PollMessagesInput


def test_is_workflow_already_completed_true() -> None:
    err = MagicMock()
    err.__str__.return_value = (
        "rpc error: workflow execution already completed for id 'x'"
    )
    assert _is_workflow_already_completed(err) is True


def test_is_workflow_already_completed_case_insensitive() -> None:
    err = MagicMock()
    err.__str__.return_value = "Workflow Execution Already Completed"
    assert _is_workflow_already_completed(err) is True


def test_is_workflow_already_completed_false_for_unrelated_error() -> None:
    err = MagicMock()
    err.__str__.return_value = "deadline exceeded"
    assert _is_workflow_already_completed(err) is False


async def test_poll_messages_answers_bad_request_for_a_foreign_cursor() -> None:
    # The read parses the cursor at the call, so a token this provider did not mint
    # raises before any poll. Outside the guard it leaves the operation as an
    # unhandled exception instead of telling the caller what it got wrong.
    handler = AgentServiceHandler(
        client=MagicMock(),
        config=Config(
            agent_task_queue="tq",
            workflow_name="Agent",
            workflow_id_prefix="agent-session-",
            is_message_queuing_enabled=False,
        ),
    )

    def refuse(_client, _workflow_id, *, after=""):
        raise StreamCursorError(f"cursor {after!r} was not minted by this provider")

    with patch.object(handler_mod, "follow_turn_events", refuse):
        with pytest.raises(HandlerError) as caught:
            await handler.poll_messages(
                MagicMock(),
                PollMessagesInput(session_id="s", cursor="memory:3"),
            )
    assert caught.value.type is HandlerErrorType.BAD_REQUEST
