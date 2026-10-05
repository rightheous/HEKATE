from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import fields, replace
from datetime import UTC, datetime
from decimal import Decimal

from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.errors import Conflict, PolicyDenied, StaleInput, UnknownExecution
from hekate.application.evidence import manifest_references_current
from hekate.domain.models import (
    BillableCallIntent,
    BudgetReservation,
    CallObservation,
    CallPermit,
    ExecutionEnvelope,
    GuardBinding,
    NormalizedUsage,
    ProviderCallPlan,
    ReservationRequest,
    SettlementReceipt,
    UsageObservation,
    UsageReceipt,
    UsageRecord,
)
from hekate.domain.types import (
    AccountingCallId,
    AttemptId,
    AttemptStatus,
    OperationId,
    PermitId,
    PrincipalId,
    ProviderAgentId,
    RegistryId,
    ReservationId,
    ScopeId,
    TaskId,
    TaskStatus,
)
from hekate.ports.store import UowFactory


def _json_value(value: object) -> object:
    return json.loads(canonical_json(value))


def _same_binding(stored: object, binding: GuardBinding) -> bool:
    return stored == _json_value(binding)


def _call_intent_hash(call: BillableCallIntent) -> str:
    value = {field.name: getattr(call, field.name) for field in fields(call)}
    measurement = value.get("measurement")
    if measurement is not None:
        measurement_value = measurement.model_dump(mode="json")
        measurement_value.pop("measured_at", None)
        value["measurement"] = measurement_value
    return canonical_json_hash(value)


_BINDING_KEYS = frozenset(field.name for field in fields(GuardBinding))


def _restore_binding(operation: Mapping[str, object]) -> GuardBinding:
    value = operation.get("binding")
    if not isinstance(value, Mapping) or set(value) != _BINDING_KEYS:
        raise Conflict("persisted operation binding is malformed")
    string_fields = (
        "task_id", "attempt_id", "agent_registry_id", "provider_agent_id",
        "principal_id", "scope", "policy_version",
    )
    if any(type(value[name]) is not str or not value[name] for name in string_fields):
        raise Conflict("persisted operation binding is malformed")
    integer_fields = ("input_revision", "authz_epoch", "fence")
    if any(type(value[name]) is not int for name in integer_fields):
        raise Conflict("persisted operation binding is malformed")
    if value["input_revision"] < 1 or value["authz_epoch"] < 0 or value["fence"] < 1:
        raise Conflict("persisted operation binding is malformed")
    conversation_id = value["conversation_id"]
    if conversation_id is not None and (type(conversation_id) is not str or not conversation_id):
        raise Conflict("persisted operation binding is malformed")
    return GuardBinding(
        task_id=TaskId(value["task_id"]),
        attempt_id=AttemptId(value["attempt_id"]),
        agent_registry_id=RegistryId(value["agent_registry_id"]),
        provider_agent_id=ProviderAgentId(value["provider_agent_id"]),
        principal_id=PrincipalId(value["principal_id"]),
        scope=ScopeId(value["scope"]),
        input_revision=value["input_revision"],
        policy_version=value["policy_version"],
        authz_epoch=value["authz_epoch"],
        fence=value["fence"],
        conversation_id=conversation_id,
    )


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


async def _validate_provider_context(uow, operation_id: OperationId, task, binding: GuardBinding) -> None:
    manifest_row = await uow.knowledge.get_context_manifest(operation_id)
    requires_manifest = task.topic_id is not None or bool(task.evidence_refs)
    if manifest_row is None:
        if requires_manifest:
            raise PolicyDenied("context_manifest_missing")
        return

    manifest = manifest_row.get("manifest")
    if (
        not isinstance(manifest, Mapping)
        or manifest_row.get("task_id") != binding.task_id
        or manifest_row.get("input_revision") != binding.input_revision
        or manifest.get("task_id") != str(binding.task_id)
        or manifest.get("attempt_id") != str(binding.attempt_id)
        or manifest.get("registry_id") != str(binding.agent_registry_id)
        or manifest.get("input_revision") != binding.input_revision
        or manifest.get("topic_id") != (str(task.topic_id) if task.topic_id is not None else None)
        or manifest.get("base_position_version") != task.base_position_version
    ):
        raise PolicyDenied("context_manifest_binding_mismatch")

    evidence = manifest.get("evidence")
    if not isinstance(evidence, list) or any(not isinstance(item, Mapping) for item in evidence):
        raise PolicyDenied("evidence_reference_unavailable")
    selected = tuple(map(str, task.evidence_refs))
    manifest_ids = tuple(item.get("id") for item in evidence)
    if (
        any(type(value) is not str for value in manifest_ids)
        or len(set(manifest_ids)) != len(manifest_ids)
        or set(manifest_ids) != set(selected)
        or len(manifest_ids) != len(selected)
        or not await manifest_references_current(
            uow, binding.scope, binding.authz_epoch, manifest, task.evidence_refs,
        )
    ):
        raise PolicyDenied("evidence_reference_unavailable")


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
        task, attempt, _ = await _lock_guard(uow, operation, call.binding, call.lease_owner)
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
        dispatch = await uow.delivery.get_dispatch_payload(call.operation_id)
        dispatch_input = dispatch.get("payload") if isinstance(dispatch, Mapping) else None
        raw_plan = dispatch_input.get("call_plan") if isinstance(dispatch_input, Mapping) else None
        if call.measurement is None and not call.test_only:
            raise PolicyDenied("provider request has no final-request token measurement")
        if raw_plan is None:
            if not call.test_only:
                raise PolicyDenied("provider call has no admission-bound call plan")
        else:
            plan = ProviderCallPlan.model_validate(raw_plan, strict=True)
            role_limit = {
                "turn": plan.main_turn_calls + plan.retry_calls,
                "compaction": plan.compaction_calls,
            }.get(call.call_kind)
            if role_limit is None or call.model != plan.model or call.price_table.version != plan.pricing_version:
                raise PolicyDenied("provider call differs from its admission-bound call plan")
            if (
                call.limits.max_input_tokens > plan.max_input_tokens
                or call.limits.max_output_tokens > plan.max_output_tokens
            ):
                raise PolicyDenied("provider call exceeds its admission-bound token plan")
            if call.measurement is not None:
                if (
                    plan.profile_digest is None
                    or call.measurement.profile_digest != plan.profile_digest
                    or call.measurement.requested_output_tokens != call.limits.max_output_tokens
                    or call.measurement.measured_input_tokens > min(call.limits.max_input_tokens, plan.max_input_tokens)
                    or call.measurement.verification_state not in {"TEST_CONTRACT_VERIFIED", "PRODUCTION_VERIFIED"}
                ):
                    raise PolicyDenied("provider request measurement differs from its admitted call plan")
            existing_call = await uow.budgets.get_call_descriptor(call.accounting_call_id)
            call_count = await uow.budgets.count_calls_by_kind(call.operation_id, call.call_kind) if existing_call is None else 0
            if existing_call is None and call_count >= role_limit:
                raise PolicyDenied(f"provider call role limit reached ({call_count}/{role_limit})")
        envelope = ExecutionEnvelope.model_validate_json(canonical_json(operation["envelope"]))
        permit = await uow.budgets.allocate_call(
            call,
            reservation_id,
            attempt.id,
            envelope,
            _call_intent_hash(call),
        )
        await _validate_provider_context(uow, call.operation_id, task, call.binding)
        await uow.commit()
        return permit


async def consume_call_permit(
    factory: UowFactory,
    binding: GuardBinding,
    lease_owner: str,
    permit_id: PermitId,
    accounting_call_id: AccountingCallId,
    *,
    expected_request_digest: str | None = None,
    expected_profile_digest: str | None = None,
) -> CallPermit:
    async with factory() as uow:
        descriptor = await uow.budgets.get_call_descriptor(accounting_call_id)
        if descriptor is None:
            raise StaleInput("provider call is unavailable")
        operation = await uow.delivery.lock_operation(OperationId(descriptor["operation_id"]))
        task, _, _ = await _lock_guard(uow, operation, binding, lease_owner)
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
        if expected_request_digest is not None and (
            descriptor.get("request_digest") != expected_request_digest
            or descriptor.get("measurement_status") != "MEASURED"
        ):
            raise Conflict("permit is bound to another measured provider request")
        if expected_profile_digest is not None and descriptor.get("profile_digest") != expected_profile_digest:
            raise Conflict("permit is bound to another immutable execution profile")
        envelope = ExecutionEnvelope.model_validate_json(canonical_json(operation["envelope"]))
        if envelope.deadline <= datetime.now(UTC):
            raise PolicyDenied("operation deadline expired")
        permit = await uow.budgets.consume_call_permit(
            permit_id, accounting_call_id,
            expected_request_digest=expected_request_digest,
            expected_profile_digest=expected_profile_digest,
        )
        await _validate_provider_context(uow, OperationId(descriptor["operation_id"]), task, binding)
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
        binding = _restore_binding(operation)
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
        binding = _restore_binding(operation)
        await uow.tasks.lock_scope(binding.scope)
        await uow.tasks.lock_task(binding.task_id)
        await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        await uow.agents.lock_registry(binding.agent_registry_id)
        call_ids = await uow.budgets.call_ids_for_operation(operation_id)
        receipts: list[SettlementReceipt] = []
        for call_id in call_ids:
            receipts.append(await uow.budgets.settle_call(AccountingCallId(call_id)))
        await uow.commit()
        return tuple(receipts)


async def reconcile_pending(factory: UowFactory, reservation_id: ReservationId) -> tuple[SettlementReceipt, ...]:
    async with factory() as uow:
        call_ids = await uow.budgets.call_ids_for_reservation(reservation_id)
        receipts: list[SettlementReceipt] = []
        for call_id in call_ids:
            descriptor = await uow.budgets.get_call_descriptor(AccountingCallId(call_id))
            if descriptor is None:
                continue
            operation = await uow.delivery.lock_operation(OperationId(descriptor["operation_id"]))
            binding = _restore_binding(operation)
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
