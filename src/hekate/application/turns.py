from __future__ import annotations

from collections.abc import Sequence

from hekate.domain.models import (
    DecisionReceipt, HekateProposal, RuntimeBinding, TemplateResponse, TurnReceipt,
)
from hekate.domain.types import ResultId, StopReason, TaskId


async def plan(task_id: TaskId) -> TurnReceipt:
    raise NotImplementedError


async def synthesize(task_id: TaskId, result_ids: Sequence[ResultId]) -> TurnReceipt:
    raise NotImplementedError


async def finalize(task_id: TaskId, reason: StopReason) -> TurnReceipt | TemplateResponse:
    raise NotImplementedError


async def handle_proposal(
    binding: RuntimeBinding, proposal: HekateProposal
) -> DecisionReceipt:
    raise NotImplementedError
