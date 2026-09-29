from __future__ import annotations

from hekate.domain.models import (
    OperatorContext, OrphanReport, RecoveryDecision,
    RecoveryReceipt, RecoveryReport, Resolution,
)
from hekate.domain.types import OperationId, RecoveryCaseId


async def reconcile_startup() -> RecoveryReport:
    raise NotImplementedError


async def reconcile_operation(operation_id: OperationId) -> RecoveryDecision:
    raise NotImplementedError


async def reconcile_owned_agents() -> OrphanReport:
    raise NotImplementedError


async def resolve_unknown(
    actor: OperatorContext, case_id: RecoveryCaseId, decision: Resolution
) -> RecoveryReceipt:
    raise NotImplementedError
