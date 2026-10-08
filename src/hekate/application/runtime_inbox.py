from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import model_validator

from hekate.domain.errors import Conflict, PolicyDenied, StaleInput, UnknownExecution
from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.models import (
    CallObservation,
    ContractModel,
    ExecutionObservation,
    GuardBinding,
    NormalizedUsage,
    UsageObservation,
)
from hekate.domain.types import AccountingCallId, OperationId, ProviderCallId
from hekate.application.budgets import _restore_binding
from hekate.ports.store import UowFactory


class InboxBinding(ContractModel):
    task_id: str
    attempt_id: str
    agent_registry_id: str
    provider_agent_id: str
    input_revision: int
    fence: int
    conversation_id: str

    @classmethod
    def from_binding(cls, binding) -> InboxBinding:
        return cls(
            task_id=str(binding.task_id), attempt_id=str(binding.attempt_id),
            agent_registry_id=str(binding.agent_registry_id), provider_agent_id=str(binding.provider_agent_id),
            input_revision=binding.input_revision, fence=binding.fence,
            conversation_id=binding.conversation_id,
        )

    def matches(self, binding: GuardBinding) -> bool:
        return self == self.from_binding(binding)


class InboxUsage(ContractModel):
    source: Literal["runtime_reported", "provider_reported", "runtime_estimated"]
    completeness: Literal["UNKNOWN", "PARTIAL", "COMPLETE"]
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cache_tokens: int | None = None
    reasoning_tokens: int | None = None
    reported_cost_usd: Decimal | None = None


class RuntimeInboxPayload(ContractModel):
    event_type: Literal["provider_call", "execution", "runtime_usage"]
    operation_id: str
    accounting_call_id: str
    source: str
    observation_identity: str
    binding: InboxBinding
    provider_call_id: str | None = None
    state: Literal["RUNNING", "UNKNOWN", "QUIESCENT"] | None = None
    outcome: Literal["SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"] | None = None
    reason: str | None = None
    lease_owner: str | None = None
    observer_fence: int | None = None
    usage: InboxUsage | None = None

    @model_validator(mode="after")
    def check_event_shape(self):
        if not self.accounting_call_id or not self.observation_identity:
            raise ValueError("runtime inbox identity is required")
        if self.event_type in {"provider_call", "execution"} and (
            self.state is None or not self.lease_owner or self.observer_fence is None
        ):
            raise ValueError("state observations require state and trusted lease identity")
        if self.event_type == "execution" and self.state == "QUIESCENT" and self.outcome is None:
            raise ValueError("terminal execution requires an outcome")
        if self.event_type == "runtime_usage" and self.usage is None:
            raise ValueError("runtime usage event requires usage")
        return self


async def process_runtime_observation(
    factory: UowFactory,
    provider_scope: str,
    stable_event_key: str,
    payload: RuntimeInboxPayload | dict[str, object],
    *,
    processor_owner: str | None = None,
) -> dict[str, object]:
    event = payload if isinstance(payload, RuntimeInboxPayload) else RuntimeInboxPayload.model_validate_json(canonical_json(payload), strict=True)
    values = event.model_dump(mode="json", exclude_none=True)
    if len(canonical_json(values).encode("utf-8")) > 16_384:
        raise ValueError("runtime inbox payload exceeds 16 KiB")
    payload_hash = canonical_json_hash(values)
    if event.event_type == "execution" and event.state == "QUIESCENT":
        async with factory() as uow:
            operation = await uow.delivery.lock_operation(OperationId(event.operation_id))
            binding = _restore_binding(operation)
            if not event.binding.matches(binding):
                raise ValueError("runtime inbox binding differs from the admitted operation")
            await uow.tasks.lock_scope_for_observation(binding.scope)
            # A byte-identical, already-processed terminal fact was authenticated
            # under its original observer fence. Return its durable receipt before
            # checking the current lease so a worker restart cannot turn a safe
            # replay into a stale-fence failure. New or conflicting observations
            # still require the original live fence below.
            processed_inbox_id = await uow.delivery.processed_inbox_id(
                provider_scope, stable_event_key, payload_hash,
            )
            if processed_inbox_id is not None:
                await uow.commit()
                return {
                    "inbox_id": processed_inbox_id,
                    "duplicate": True,
                    "conflict": False,
                    "processed": True,
                }
            # Authenticate the observation's original runtime fence before persisting it.
            await uow.agents.assert_current_lease(binding.agent_registry_id, event.lease_owner, event.observer_fence)
            if processor_owner is not None:
                lease = await uow.agents.get_lease(binding.agent_registry_id)
                if lease is None or lease.owner != processor_owner:
                    raise UnknownExecution("terminal observer does not hold the registry lease")
                await uow.agents.assert_current_lease(binding.agent_registry_id, lease.owner, lease.fence)
            receipt = await uow.delivery.insert_inbox_once(provider_scope, stable_event_key, values, payload_hash)
            await uow.commit()
        if receipt.conflict:
            async with factory() as uow:
                await uow.delivery.reject_inbox(receipt.id, "terminal_payload_conflict")
                await uow.commit()
            return {"inbox_id": receipt.id, "duplicate": receipt.duplicate, "conflict": True, "processed": True}
        return await _apply_terminal_inbox(factory, receipt.id, processor_owner)
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(OperationId(event.operation_id))
        binding = _restore_binding(operation)
        if not event.binding.matches(binding):
            raise ValueError("runtime inbox binding differs from the admitted operation")
        await uow.tasks.lock_scope_for_observation(binding.scope)
        receipt = await uow.delivery.insert_inbox_once(provider_scope, stable_event_key, values, payload_hash)
        if receipt.conflict:
            row = await uow.delivery.lock_inbox(receipt.id)
            if row is None:
                raise RuntimeError("conflicting inbox row disappeared")
            if row["processed_at"] is None:
                if event.usage is not None:
                    descriptor = await uow.budgets.get_call_descriptor(AccountingCallId(event.accounting_call_id))
                    if descriptor is None or descriptor["operation_id"] != event.operation_id:
                        raise ValueError("conflicting inbox usage operation mismatch")
                    usage = event.usage
                    await uow.budgets.record_usage(UsageObservation(
                        accounting_call_id=AccountingCallId(event.accounting_call_id),
                        source=usage.source,
                        observation_identity=event.observation_identity,
                        usage=NormalizedUsage(
                            completeness=usage.completeness,
                            input_tokens=usage.input_tokens,
                            output_tokens=usage.output_tokens,
                            total_tokens=usage.total_tokens,
                            cache_tokens=usage.cache_tokens,
                            reasoning_tokens=usage.reasoning_tokens,
                            reported_cost_usd=usage.reported_cost_usd,
                        ),
                        binding=binding,
                        observed_at=row["received_at"],
                        provider_call_id=ProviderCallId(event.provider_call_id) if event.provider_call_id else None,
                    ))
                    await uow.budgets.settle_call(AccountingCallId(event.accounting_call_id))
                await uow.delivery.mark_inbox_processed(receipt.id)
            await uow.commit()
            return {"inbox_id": receipt.id, "duplicate": receipt.duplicate, "conflict": True, "processed": row["processed_at"] is not None or event.usage is not None}
        row = await uow.delivery.lock_inbox(receipt.id)
        if row is None:
            raise RuntimeError("inbox row disappeared")
        if row["processed_at"] is not None:
            await uow.commit()
            return {"inbox_id": receipt.id, "duplicate": True, "conflict": False, "processed": True}
        stored = RuntimeInboxPayload.model_validate_json(canonical_json(row["payload"]), strict=True)
        observed_at = row["received_at"]
        accounting_call_id = AccountingCallId(stored.accounting_call_id)
        if stored.event_type == "provider_call":
            descriptor = await uow.budgets.get_call_descriptor(accounting_call_id)
            if descriptor is None or descriptor["operation_id"] != stored.operation_id:
                raise ValueError("inbox provider call operation mismatch")
            await uow.budgets.record_call_observation(CallObservation(
                accounting_call_id=accounting_call_id,
                binding=binding,
                state=stored.state,
                source=stored.source,
                observed_at=observed_at,
                lease_owner=stored.lease_owner,
                observer_fence=stored.observer_fence,
                provider_call_id=ProviderCallId(stored.provider_call_id) if stored.provider_call_id else None,
            ))
        elif stored.event_type == "execution":
            from hekate.application.operations import apply_execution_observation

            await apply_execution_observation(uow, ExecutionObservation(
                operation_id=OperationId(stored.operation_id),
                binding=binding,
                lease_owner=stored.lease_owner,
                observer_fence=stored.observer_fence,
                state=stored.state,
                source=stored.source,
                observed_at=observed_at,
                outcome=stored.outcome,
                reason=stored.reason,
            ))
        if stored.usage is not None:
            usage = stored.usage
            await uow.budgets.record_usage(UsageObservation(
                accounting_call_id=accounting_call_id,
                source=usage.source,
                observation_identity=stored.observation_identity,
                usage=NormalizedUsage(
                    completeness=usage.completeness,
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    total_tokens=usage.total_tokens,
                    cache_tokens=usage.cache_tokens,
                    reasoning_tokens=usage.reasoning_tokens,
                    reported_cost_usd=usage.reported_cost_usd,
                ),
                binding=binding,
                observed_at=observed_at,
                provider_call_id=ProviderCallId(stored.provider_call_id) if stored.provider_call_id else None,
            ))
        if stored.event_type == "provider_call" or stored.usage is not None:
            await uow.budgets.settle_call(accounting_call_id)
        await uow.delivery.mark_inbox_processed(receipt.id)
        await uow.commit()
        result = {"inbox_id": receipt.id, "duplicate": receipt.duplicate, "conflict": False, "processed": True}
    if stored.event_type == "provider_call" and stored.state == "QUIESCENT":
        async with factory() as uow:
            pending_ids = await uow.delivery.pending_terminal_inbox(OperationId(stored.operation_id))
            await uow.commit()
        for pending_id in pending_ids:
            await _apply_terminal_inbox(factory, pending_id, stored.lease_owner)
    return result


async def _apply_terminal_inbox(factory: UowFactory, inbox_id: str, processor_owner: str | None) -> dict[str, object]:
    try:
        async with factory() as uow:
            row = await uow.delivery.lock_inbox(inbox_id)
            if row is None:
                raise RuntimeError("terminal inbox row disappeared")
            if row["processed_at"] is not None:
                await uow.commit()
                return {"inbox_id": inbox_id, "duplicate": True, "conflict": False, "processed": True}
            stored = RuntimeInboxPayload.model_validate_json(canonical_json(row["payload"]), strict=True)
            operation = await uow.delivery.lock_operation(OperationId(stored.operation_id))
            binding = _restore_binding(operation)
            if not stored.binding.matches(binding):
                raise Conflict("terminal observation binding differs from admitted operation")
            from hekate.application.operations import apply_execution_observation

            await apply_execution_observation(uow, ExecutionObservation(
                operation_id=OperationId(stored.operation_id),
                binding=binding,
                lease_owner=stored.lease_owner,
                observer_fence=stored.observer_fence,
                state="QUIESCENT",
                source=stored.source,
                observed_at=row["received_at"],
                outcome=stored.outcome,
                reason=stored.reason,
                processor_owner=processor_owner,
            ))
            await uow.delivery.mark_inbox_processed(inbox_id)
            await uow.commit()
            return {"inbox_id": inbox_id, "duplicate": False, "conflict": False, "processed": True}
    except UnknownExecution as error:
        reason = "processor_lease_unavailable" if "processor" in str(error) else "call_termination_unconfirmed"
        async with factory() as uow:
            await uow.delivery.defer_inbox(inbox_id, reason)
            await uow.commit()
        return {"inbox_id": inbox_id, "duplicate": False, "conflict": False, "processed": False, "pending_reason": reason}
    except (Conflict, PolicyDenied, StaleInput, ValueError):
        async with factory() as uow:
            await uow.delivery.reject_inbox(inbox_id, "terminal_observation_rejected")
            await uow.commit()
        return {"inbox_id": inbox_id, "duplicate": False, "conflict": True, "processed": True, "rejected": True}
