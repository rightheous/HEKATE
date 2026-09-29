from __future__ import annotations

from collections.abc import Sequence

from hekate.domain.errors import FailureClass
from hekate.domain.models import (
    ApprovedAction, HekateProposal, ScheduledDecision, SchemaFailure,
)
from hekate.domain.types import AttemptId, Instant, TaskId


async def decide(task_id: TaskId, proposal: HekateProposal) -> ScheduledDecision:
    raise NotImplementedError


async def schedule_attempt(task_id: TaskId, action: ApprovedAction) -> AttemptId:
    raise NotImplementedError


async def schedule_repair(attempt_id: AttemptId, failure: SchemaFailure) -> AttemptId:
    raise NotImplementedError


async def schedule_retry(attempt_id: AttemptId, failure: FailureClass) -> AttemptId:
    raise NotImplementedError


async def stop_due_tasks(now: Instant, limit: int) -> Sequence[TaskId]:
    raise NotImplementedError
