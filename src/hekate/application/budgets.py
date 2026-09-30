from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Mapping, Sequence

from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.errors import Conflict, PolicyDenied, StaleInput, UnknownExecution
from hekate.domain.models import (
    BillableCallIntent,
    BudgetReservation,
    CallObservation,
    CallPermit,
    ExecutionEnvelope,
    GuardBinding,
    NormalizedUsage,
    ReservationRequest,
    SettlementReceipt,
    UsageObservation,
    UsageReceipt,
    UsageRecord,
)
from hekate.domain.types import (
    AccountingCallId,
    AttemptStatus,
    OperationId,
    PermitId,
    ReservationId,
    TaskStatus,
)
from hekate.ports.store import UowFactory


def _json_value(value: object) -> object:
    return json.loads(canonical_json(value))


def _same_binding(stored: object, binding: GuardBinding) -> bool:
    return stored == _json_value(binding)


def _aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


async def _lock_guard(uow, operation, binding: GuardBinding, lease_owner: str):
    if not _same_binding(operation["binding"], binding):
        raise Conflict("runtime binding differs from the admitted operation")
    if operation["state"] == "UNKNOWN" or operation["execution_state"] == "UNKNOWN":
        raise UnknownExecution("operation execution remains unresolved")
    if operation["state"] != "ADMITTED" or operation["execution_state"] == "QUIESCENT":
        raise PolicyDenied("operation no longer admits provider calls")

    scope = await uow.tasks.lock_scope(binding.scope)
    if (scope.principal_id, scope.policy_version, scope.authz_epoch) != (
        binding.principal_id,
        binding.policy_version,
        binding.authz_epoch,
    ):
        raise PolicyDenied("authorization snapshot changed")
    task = await uow.tasks.lock_task(binding.task_id)
    if task.scope != binding.scope or task.input_revision != binding.input_revision:
        raise StaleInput("task scope or input revision changed")
    if task.status != TaskStatus.RUNNING:
        raise PolicyDenied("task is not running")
    now = datetime.now(UTC)
    if task.deadline <= now:
        raise PolicyDenied("task deadline expired")

    attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
    if (
        attempt.task_id != binding.task_id
        or attempt.operation_id != operation["id"]
        or attempt.input_revision != binding.input_revision
        or attempt.agent_registry_id != binding.agent_registry_id
        or attempt.status not in {AttemptStatus.PENDING, AttemptStatus.DISPATCHED, AttemptStatus.RUNNING}
    ):
        raise StaleInput("attempt binding or state changed")
    agent = await uow.agents.lock_registry(binding.agent_registry_id)
    if (
        agent.owner_scope != binding.scope
        or agent.provider_id != binding.provider_agent_id
        or agent.policy_version != binding.policy_version
        or agent.intended_state != "BUSY"
    ):
        raise PolicyDenied("agent binding or state changed")
    await uow.agents.assert_current_lease(binding.agent_registry_id, lease_owner, binding.fence)
    hold = await uow.agents.active_execution_hold(binding.agent_registry_id, lock=True)
    if hold is None or hold["operation_id"] != operation["id"]:
        raise StaleInput("operation execution hold is unavailable")
    if hold["state"] == "UNKNOWN":
        raise UnknownExecution("agent execution remains unresolved")
    return task, attempt, agent


async def reserve(factory: UowFactory, request: ReservationRequest) -> BudgetReservation:
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(request.operation_id)
        scope_id = operation["owner_scope"]
        scope = await uow.tasks.lock_scope(scope_id)
        task = await uow.tasks.lock_task(request.task_id)
        if task.scope != scope.scope or operation["task_id"] != request.task_id:
            raise Conflict("reservation task or scope differs from its operation")
        reservation = await uow.budgets.reserve_operation(request)
        await uow.commit()
        return reservation


async def authorize_provider_call(factory: UowFactory, call: BillableCallIntent) -> CallPermit:
    if not _aware(call.permit_expires_at) or not _aware(call.limits.deadline):
        raise ValueError("call deadlines must be timezone-aware")
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(call.operation_id)
        _, attempt, _ = await _lock_guard(uow, operation, call.binding, call.lease_owner)
        if call.operation_id != operation["id"]:
            raise Conflict("call operation identity changed")
        if call.binding.conversation_id != operation["binding"].get("conversation_id"):
            raise Conflict("call conversation binding changed")
        reservation_id = call.reservation_id
        if reservation_id is None:
            reservation_id = attempt.reservation_id
        if reservation_id is None:
            raise StaleInput("attempt has no reservation")
        reservation = await uow.budgets.get_reservation(reservation_id)
        if reservation is None or reservation["operation_id"] != call.operation_id:
            raise StaleInput("call reservation is unavailable")
        envelope = ExecutionEnvelope.model_validate_json(canonical_json(operation["envelope"]))
        permit = await uow.budgets.allocate_call(
            call,
            reservation_id,
            attempt.id,
            envelope,
            canonical_json_hash(call),
        )
        await uow.commit()
        return permit


async def consume_call_permit(
    factory: UowFactory,
    binding: GuardBinding,
    lease_owner: str,
    permit_id: PermitId,
    accounting_call_id: AccountingCallId,
) -> CallPermit:
    async with factory() as uow:
        descriptor = await uow.budgets.get_call_descriptor(accounting_call_id)
        if descriptor is None:
            raise StaleInput("provider call is unavailable")
        operation = await uow.delivery.lock_operation(OperationId(descriptor["operation_id"]))
        _, _, _ = await _lock_guard(uow, operation, binding, lease_owner)
        if (
            descriptor["task_id"] != binding.task_id
            or descriptor["attempt_id"] != binding.attempt_id
            or descriptor["registry_id"] != binding.agent_registry_id
            or descriptor["owner_scope"] != binding.scope
            or descriptor["permit_id"] != permit_id
            or descriptor["lease_owner"] != lease_owner
            or descriptor["input_revision"] != binding.input_revision
            or descriptor["fence"] != binding.fence
            or descriptor["conversation_id"] != binding.conversation_id
        ):
            raise Conflict("permit is bound to another runtime identity")
        envelope = ExecutionEnvelope.model_validate_json(canonical_json(operation["envelope"]))
        if envelope.deadline <= datetime.now(UTC):
            raise PolicyDenied("operation deadline expired")
        permit = await uow.budgets.consume_call_permit(permit_id, accounting_call_id)
        await uow.commit()
        return permit


async def record_call_observation(factory: UowFactory, observation: CallObservation) -> None:
    if not _aware(observation.observed_at):
        raise ValueError("call observation timestamp must be timezone-aware")
    async with factory() as uow:
        descriptor = await uow.budgets.get_call_descriptor(observation.accounting_call_id)
        if descriptor is None:
            await uow.delivery.append_audit({
                "event_kind": "provider_call.observation_orphan",
                "safe_payload": {"accounting_call_id": str(observation.accounting_call_id), "source": observation.source},
            })
            await uow.commit()
            return
        if (
            descriptor["task_id"] != observation.binding.task_id
            or descriptor["attempt_id"] != observation.binding.attempt_id
            or descriptor["registry_id"] != observation.binding.agent_registry_id
            or descriptor["owner_scope"] != observation.binding.scope
        ):
            await uow.delivery.append_audit({
                "owner_scope": str(descriptor["owner_scope"]),
                "task_id": str(descriptor["task_id"]),
                "attempt_id": str(descriptor["attempt_id"]),
                "operation_id": str(descriptor["operation_id"]),
                "registry_id": str(descriptor["registry_id"]),
                "event_kind": "provider_call.observation_binding_rejected",
                "safe_payload": {"accounting_call_id": str(observation.accounting_call_id), "source": observation.source},
            })
            await uow.commit()
            return
        operation = await uow.delivery.lock_operation(OperationId(descriptor["operation_id"]))
        if not _same_binding(operation["binding"], observation.binding):
            await uow.delivery.append_audit({
                "owner_scope": str(descriptor["owner_scope"]),
                "task_id": str(descriptor["task_id"]),
                "attempt_id": str(descriptor["attempt_id"]),
                "operation_id": str(descriptor["operation_id"]),
                "registry_id": str(descriptor["registry_id"]),
                "event_kind": "provider_call.observation_binding_rejected",
                "safe_payload": {"accounting_call_id": str(observation.accounting_call_id), "source": observation.source},
            })
            await uow.commit()
            return
        await uow.tasks.lock_scope(observation.binding.scope)
        await uow.tasks.lock_task(observation.binding.task_id)
        await uow.tasks.get_attempt(observation.binding.attempt_id, for_update=True)
        agent = await uow.agents.lock_registry(observation.binding.agent_registry_id)
        if agent.provider_id != observation.binding.provider_agent_id or agent.owner_scope != observation.binding.scope:
            raise Conflict("provider call observation registry mismatch")
        await uow.agents.assert_current_lease(
            observation.binding.agent_registry_id,
            observation.lease_owner,
            observation.observer_fence,
        )
        await uow.budgets.record_call_observation(observation)
        await uow.delivery.append_audit({
            "owner_scope": str(observation.binding.scope),
            "task_id": str(observation.binding.task_id),
            "attempt_id": str(observation.binding.attempt_id),
            "operation_id": str(descriptor["operation_id"]),
            "registry_id": str(observation.binding.agent_registry_id),
            "event_kind": f"provider_call.{observation.state.lower()}",
            "safe_payload": {"accounting_call_id": str(observation.accounting_call_id), "source": observation.source},
        })
        await uow.commit()


async def record_usage(factory: UowFactory, record: UsageRecord) -> UsageReceipt:
    if not record.observation_identity or not record.source:
        raise ValueError("usage source and observation identity are required")
    async with factory() as uow:
        descriptor = await uow.budgets.get_call_descriptor(record.accounting_call_id)
        if descriptor is None:
            await uow.delivery.append_audit({
                "event_kind": "usage.observation_orphan",
                "safe_payload": {
                    "accounting_call_id": str(record.accounting_call_id),
                    "source": record.source,
                    "observation_identity": record.observation_identity,
                },
            })
            await uow.commit()
            return UsageReceipt(
                accounting_call_id=record.accounting_call_id,
                observation_id=record.observation_identity,
                duplicate=False,
                conflict=False,
                completeness=record.completeness,
                settlement_state="REJECTED",
                accepted=False,
                reason="unknown accounting call",
            )
        operation_id = OperationId(descriptor["operation_id"])
        operation = await uow.delivery.lock_operation(operation_id)
        binding = record.binding
        if not _same_binding(operation["binding"], binding):
            await uow.delivery.append_audit({
                "owner_scope": str(descriptor["owner_scope"]),
                "task_id": str(descriptor["task_id"]),
                "attempt_id": str(descriptor["attempt_id"]),
                "operation_id": str(operation_id),
                "registry_id": str(descriptor["registry_id"]),
                "event_kind": "usage.binding_rejected",
                "safe_payload": {"accounting_call_id": str(record.accounting_call_id), "source": record.source},
            })
            await uow.commit()
            return UsageReceipt(
                accounting_call_id=record.accounting_call_id,
                observation_id=record.observation_identity,
                duplicate=False,
                conflict=False,
                completeness=record.completeness,
                settlement_state="REJECTED",
                accepted=False,
                reason="binding mismatch",
            )
        await uow.tasks.lock_scope(binding.scope)
        task = await uow.tasks.lock_task(binding.task_id)
        attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        agent = await uow.agents.lock_registry(binding.agent_registry_id)
        if (
            task.scope != binding.scope
            or descriptor["task_id"] != binding.task_id
            or descriptor["attempt_id"] != binding.attempt_id
            or descriptor["registry_id"] != binding.agent_registry_id
            or descriptor["owner_scope"] != binding.scope
            or attempt.task_id != binding.task_id
            or attempt.operation_id != operation_id
            or attempt.agent_registry_id != binding.agent_registry_id
            or agent.owner_scope != binding.scope
            or agent.provider_id != binding.provider_agent_id
        ):
            await uow.delivery.append_audit({
                "owner_scope": str(binding.scope),
                "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id),
                "operation_id": str(operation_id),
                "registry_id": str(binding.agent_registry_id),
                "event_kind": "usage.binding_rejected",
                "safe_payload": {"accounting_call_id": str(record.accounting_call_id), "source": record.source},
            })
            await uow.commit()
            return UsageReceipt(
                accounting_call_id=record.accounting_call_id,
                observation_id=record.observation_identity,
                duplicate=False,
                conflict=False,
                completeness=record.completeness,
                settlement_state="REJECTED",
                accepted=False,
                reason="persisted identity mismatch",
            )
        if record.pricing_version is not None and record.pricing_version != descriptor["pricing_version"]:
            await uow.delivery.append_audit({
                "owner_scope": str(binding.scope),
                "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id),
                "operation_id": str(operation_id),
                "registry_id": str(binding.agent_registry_id),
                "event_kind": "usage.pricing_version_rejected",
                "safe_payload": {"accounting_call_id": str(record.accounting_call_id), "source": record.source},
            })
            await uow.commit()
            return UsageReceipt(
                accounting_call_id=record.accounting_call_id,
                observation_id=record.observation_identity,
                duplicate=False,
                conflict=False,
                completeness=record.completeness,
                settlement_state="REJECTED",
                accepted=False,
                reason="pricing version mismatch",
            )
        if not _aware(record.observed_at):
            raise ValueError("usage timestamp must be timezone-aware")
        observation = UsageObservation(
            accounting_call_id=record.accounting_call_id,
            source=record.source,
            observation_identity=record.observation_identity,
            usage=NormalizedUsage(
                completeness=record.completeness,
                input_tokens=record.input_tokens,
                output_tokens=record.output_tokens,
                total_tokens=record.total_tokens,
                cache_tokens=record.cache_tokens,
                reasoning_tokens=record.reasoning_tokens,
                reported_cost_usd=record.monetary_amount,
            ),
            binding=binding,
            observed_at=record.observed_at,
            provider_call_id=record.provider_call_id,
        )
        try:
            receipt = await uow.budgets.record_usage(observation)
        except (Conflict, StaleInput) as error:
            await uow.delivery.append_audit({
                "owner_scope": str(binding.scope),
                "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id),
                "operation_id": str(operation_id),
                "registry_id": str(binding.agent_registry_id),
                "event_kind": "usage.observation_rejected",
                "safe_payload": {
                    "accounting_call_id": str(record.accounting_call_id),
                    "source": record.source,
                    "reason_class": type(error).__name__,
                },
            })
            await uow.commit()
            return UsageReceipt(
                accounting_call_id=record.accounting_call_id,
                observation_id=record.observation_identity,
                duplicate=False,
                conflict=False,
                completeness=record.completeness,
                settlement_state="REJECTED",
                accepted=False,
                reason=type(error).__name__,
            )
        settlement = await uow.budgets.settle_call(record.accounting_call_id)
        if receipt.conflict:
            await uow.delivery.append_audit({
                "owner_scope": str(binding.scope),
                "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id),
                "operation_id": str(operation_id),
                "registry_id": str(binding.agent_registry_id),
                "event_kind": "usage.observation_conflict",
                "safe_payload": {
                    "accounting_call_id": str(record.accounting_call_id),
                    "source": record.source,
                    "observation_identity": record.observation_identity,
                },
            })
        await uow.commit()
        if settlement.settled:
            return replace(receipt, settlement_state="SETTLED")
        return receipt


async def settle_call(factory: UowFactory, accounting_call_id: AccountingCallId) -> SettlementReceipt:
    async with factory() as uow:
        descriptor = await uow.budgets.get_call_descriptor(accounting_call_id)
        if descriptor is None:
            raise StaleInput("provider call is unavailable")
        operation_id = OperationId(descriptor["operation_id"])
        operation = await uow.delivery.lock_operation(operation_id)
        binding = GuardBinding.model_validate_json(canonical_json(operation["binding"]))
        await uow.tasks.lock_scope(binding.scope)
        await uow.tasks.lock_task(binding.task_id)
        await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        await uow.agents.lock_registry(binding.agent_registry_id)
        receipt = await uow.budgets.settle_call(accounting_call_id)
        await uow.commit()
        return receipt


async def settle(factory: UowFactory, operation_id: OperationId) -> tuple[SettlementReceipt, ...]:
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(operation_id)
        binding = GuardBinding.model_validate_json(canonical_json(operation["binding"]))
        await uow.tasks.lock_scope(binding.scope)
        await uow.tasks.lock_task(binding.task_id)
        await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        await uow.agents.lock_registry(binding.agent_registry_id)
        call_ids = await uow.budgets.call_ids_for_operation(operation_id)
        receipts = tuple(await uow.budgets.settle_call(AccountingCallId(call_id)) for call_id in call_ids)
        await uow.commit()
        return receipts


async def reconcile_pending(factory: UowFactory, reservation_id: ReservationId) -> tuple[SettlementReceipt, ...]:
    async with factory() as uow:
        call_ids = await uow.budgets.call_ids_for_reservation(reservation_id)
        receipts: list[SettlementReceipt] = []
        for call_id in call_ids:
            descriptor = await uow.budgets.get_call_descriptor(AccountingCallId(call_id))
            if descriptor is None:
                continue
            operation = await uow.delivery.lock_operation(OperationId(descriptor["operation_id"]))
            binding = GuardBinding.model_validate_json(canonical_json(operation["binding"]))
            await uow.tasks.lock_scope(binding.scope)
            await uow.tasks.lock_task(binding.task_id)
            await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
            await uow.agents.lock_registry(binding.agent_registry_id)
            receipts.append(await uow.budgets.settle_call(AccountingCallId(call_id)))
        await uow.commit()
        return tuple(receipts)


async def list_pending(factory: UowFactory, limit: int = 100) -> Sequence[Mapping[str, object]]:
    async with factory() as uow:
        return await uow.budgets.list_pending_calls(limit)


async def apply_adjustment(
    factory: UowFactory,
    account_id: str,
    effect_key: str,
    amount: Decimal,
    *,
    reservation_id: ReservationId | None = None,
    accounting_call_id: AccountingCallId | None = None,
) -> bool:
    async with factory() as uow:
        await uow.budgets.lock_accounts([account_id])
        inserted = await uow.budgets.apply_adjustment(
            account_id,
            effect_key,
            amount,
            reservation_id=reservation_id,
            accounting_call_id=accounting_call_id,
        )
        if inserted:
            await uow.delivery.append_audit({
                "event_kind": "budget.adjustment_applied",
                "safe_payload": {"account_id": account_id, "effect_key": effect_key, "amount": str(amount)},
            })
        await uow.commit()
        return inserted
