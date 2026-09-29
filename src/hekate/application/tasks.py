from __future__ import annotations

from hekate.domain.models import (
    CancellationReceipt, InputChange, ResponseRef, RevisionReceipt, TaskReceipt,
    TaskSnapshot, TaskView, UserMessage,
)
from hekate.domain.types import ActorContext, Revision, StopReason, TaskId


async def submit(actor: ActorContext, message: UserMessage, request_key: str) -> TaskReceipt:
    raise NotImplementedError


async def revise(
    actor: ActorContext,
    task_id: TaskId,
    expected_revision: Revision,
    change: InputChange,
) -> TaskSnapshot:
    raise NotImplementedError


async def cancel(actor: ActorContext, task_id: TaskId, reason: StopReason) -> CancellationReceipt:
    raise NotImplementedError


async def complete(
    task_id: TaskId, outcome: str, reason: StopReason, response: ResponseRef
) -> TaskView:
    raise NotImplementedError
