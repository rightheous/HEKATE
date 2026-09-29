from __future__ import annotations

from hekate.domain.models import (
    BillableCallIntent, BudgetReservation, CallPermit, GuardBinding,
    ReservationRequest, SettlementReceipt, UsageCompleteness, UsageReceipt,
    UsageRecord,
)
from hekate.domain.types import OperationId, ReservationId
from hekate.ports.store import UnitOfWork


async def reserve(uow: UnitOfWork, request: ReservationRequest) -> BudgetReservation:
    raise NotImplementedError


async def authorize_provider_call(
    binding: GuardBinding, call: BillableCallIntent
) -> CallPermit:
    raise NotImplementedError


async def record_usage(record: UsageRecord) -> UsageReceipt:
    raise NotImplementedError


async def settle(
    operation_id: OperationId, report: UsageCompleteness
) -> SettlementReceipt:
    raise NotImplementedError


async def reconcile_pending(reservation_id: ReservationId) -> SettlementReceipt:
    raise NotImplementedError
