from __future__ import annotations

from hekate.domain.models import (
    AgentRecord, CreateObservation, DeleteObservation, RetirementReceipt,
    SpawnProposal,
)
from hekate.domain.types import OperationId, PrincipalId, RegistryId, TaskId


async def ensure_hekate(owner: PrincipalId) -> AgentRecord:
    raise NotImplementedError


async def request_critic(task_id: TaskId, proposal: SpawnProposal) -> AgentRecord:
    raise NotImplementedError


async def create_from_intent(operation_id: OperationId) -> CreateObservation:
    raise NotImplementedError


async def retire(registry_id: RegistryId) -> RetirementReceipt:
    raise NotImplementedError


async def confirm_deletion(operation_id: OperationId) -> DeleteObservation:
    raise NotImplementedError
