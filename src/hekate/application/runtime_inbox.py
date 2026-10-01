from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import model_validator

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
) -> dict[str, object]:
    event = payload if isinstance(payload, RuntimeInboxPayload) else RuntimeInboxPayload.model_validate_json(canonical_json(payload), strict=True)
    values = event.model_dump(mode="json", exclude_none=True)
    payload_hash = canonical_json_hash(values)
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(OperationId(event.operation_id))
        binding = _restore_binding(operation)
        if not event.binding.matches(binding):
            raise ValueError("runtime inbox binding differs from the admitted operation")
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
        return {"inbox_id": receipt.id, "duplicate": receipt.duplicate, "conflict": False, "processed": True}
