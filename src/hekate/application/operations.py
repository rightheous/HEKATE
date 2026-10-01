from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from hekate.domain.errors import Conflict, PolicyDenied, StaleInput, UnknownExecution
from hekate.domain.models import AdmissionReceipt, AdmissionRequest, Attempt, ExecutionObservation, OutboxJob
from hekate.domain.types import AttemptEvent, AttemptId, AttemptStatus, TaskStatus
from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.ports.store import UowFactory


def _json_value(value: object) -> object:
    return json.loads(canonical_json(value))


def _binding(request: AdmissionRequest) -> dict[str, object]:
    return _json_value(request.binding)


def _envelope(request: AdmissionRequest) -> dict[str, object]:
    return _json_value(request.envelope)


def _request_hash(request: AdmissionRequest) -> str:
    return canonical_json_hash({
        "operation_id": request.envelope.operation_id,
        "kind": request.operation_kind,
        "scope": request.binding.scope,
        "binding": request.binding,
        "envelope": request.envelope,
        "reservation": request.reservation,
        "attempt_kind": request.attempt_kind,
        "parent_attempt_id": request.parent_attempt_id,
        "payload": request.payload,
    })


def _validate_request(request: AdmissionRequest) -> None:
    binding = request.binding
    envelope = request.envelope
    reservation = request.reservation
    if envelope.task_id != binding.task_id or envelope.attempt_id != binding.attempt_id:
        raise Conflict("envelope task or attempt binding mismatch")
    if envelope.operation_id != reservation.operation_id:
        raise Conflict("envelope and reservation operation mismatch")
    if envelope.input_revision != binding.input_revision or envelope.fence != binding.fence:
        raise Conflict("envelope revision or fence mismatch")
    if envelope.principal_id != binding.principal_id or envelope.scope != binding.scope:
        raise Conflict("envelope principal or scope mismatch")
    if envelope.reservation_id != reservation.id:
        raise Conflict("envelope reservation mismatch")
    if request.attempt_kind not in {"planning", "critic_review", "synthesis", "schema_repair", "transient_retry", "final_response"}:
        raise PolicyDenied("unsupported attempt kind")
    if not request.operation_kind or not request.lease_owner:
        raise ValueError("operation kind and lease owner are required")
    if reservation.task_id != binding.task_id or reservation.operation_id != envelope.operation_id:
        raise Conflict("reservation task or operation binding mismatch")
    if reservation.purpose != "operation_envelope":
        raise PolicyDenied("operation admission requires an operation-envelope reservation")
    if reservation.pricing_version != envelope.pricing_version:
        raise Conflict("reservation pricing version differs from envelope")
    if not reservation.amount.is_finite() or reservation.amount < 0:
        raise ValueError("reservation amount must be finite and nonnegative")
    if envelope.max_input_tokens < 0 or envelope.max_output_tokens < 0 or envelope.billable_call_slots < 0:
        raise ValueError("execution limits must be nonnegative")
    if envelope.deadline.tzinfo is None or envelope.deadline.utcoffset() is None:
        raise ValueError("execution deadline must be timezone-aware")


async def admit_operation(factory: UowFactory, request: AdmissionRequest) -> AdmissionReceipt:
    _validate_request(request)
    binding = request.binding
    envelope = request.envelope
    request_hash = _request_hash(request)
    async with factory() as uow:
        claim = await uow.delivery.claim_operation(
            envelope.operation_id,
            binding.scope,
            binding.task_id,
            request.operation_kind,
            request_hash,
            _binding(request),
            _envelope(request),
        )
        if claim.state in {"ADMITTED", "COMPLETED", "FAILED"}:
            receipt = claim.receipt
            if (
                receipt is None
                or receipt.state != "ADMITTED"
                or receipt.operation_id != envelope.operation_id
                or receipt.attempt_id != binding.attempt_id
                or receipt.reservation_id != request.reservation.id
            ):
                raise Conflict("operation has no valid admission receipt")
            await uow.commit()
            return AdmissionReceipt(
                operation_id=receipt.operation_id,
                attempt_id=receipt.attempt_id,
                reservation_id=receipt.reservation_id,
                state=receipt.state,
                replayed=True,
            )
        if claim.state == "UNKNOWN":
            raise UnknownExecution("operation execution remains unresolved")
        if claim.state != "CLAIMED":
            raise Conflict("operation claim is incomplete")

        scope = await uow.tasks.lock_scope(binding.scope)
        if (scope.principal_id, scope.policy_version, scope.authz_epoch) != (
            binding.principal_id,
            binding.policy_version,
            binding.authz_epoch,
        ):
            raise PolicyDenied("authorization snapshot changed")
        task = await uow.tasks.lock_task(binding.task_id)
        now = datetime.now(UTC)
        if task.scope != binding.scope or task.input_revision != binding.input_revision:
            raise StaleInput("task scope or input revision changed")
        if task.status in {TaskStatus.STOPPING, TaskStatus.CANCELLED, TaskStatus.COMPLETED, TaskStatus.FAILED}:
            raise PolicyDenied("task cancellation or terminal state blocks dispatch")
        if task.deadline <= now or envelope.deadline > task.deadline or envelope.deadline <= now:
            raise PolicyDenied("task or operation deadline expired")
        if await uow.tasks.attempt_exists(binding.attempt_id):
            raise Conflict("attempt id is already bound")
        agent = await uow.agents.lock_registry(binding.agent_registry_id)
        if agent.owner_scope != binding.scope or agent.provider_id != binding.provider_agent_id:
            raise Conflict("registry and provider-agent binding mismatch")
        if agent.policy_version != binding.policy_version:
            raise PolicyDenied("agent policy changed")
        await uow.agents.assert_current_lease(binding.agent_registry_id, request.lease_owner, binding.fence)
        active = await uow.agents.active_execution_hold(binding.agent_registry_id, lock=True)
        if active is not None:
            raise UnknownExecution("agent has an unresolved execution")
        if agent.intended_state != "READY":
            raise PolicyDenied("agent is not ready under the current policy")

        review_round = task.counters.review_rounds + (1 if request.attempt_kind == "critic_review" else 0)
        attempt = Attempt(
            id=AttemptId(binding.attempt_id),
            task_id=binding.task_id,
            kind=request.attempt_kind,
            parent_attempt_id=request.parent_attempt_id,
            review_round=review_round,
            input_revision=binding.input_revision,
            agent_registry_id=binding.agent_registry_id,
            status=AttemptStatus.PENDING,
            operation_id=envelope.operation_id,
            reservation_id=request.reservation.id,
            deadline=envelope.deadline,
        )
        await uow.budgets.reserve_operation(request.reservation)
        await uow.tasks.insert_attempt(attempt)
        await uow.tasks.apply_admission(task, request.attempt_kind)
        await uow.agents.set_registry_busy(binding.agent_registry_id, binding.attempt_id)
        await uow.agents.create_execution_hold(binding.agent_registry_id, envelope.operation_id)

        receipt = AdmissionReceipt(
            operation_id=envelope.operation_id,
            attempt_id=binding.attempt_id,
            reservation_id=request.reservation.id,
            state="ADMITTED",
            replayed=False,
        )
        job_id = str(uuid5(NAMESPACE_URL, f"hekate:outbox:{envelope.operation_id}:dispatch:0"))
        await uow.delivery.append_outbox(OutboxJob(
            id=job_id,
            operation_id=envelope.operation_id,
            kind="dispatch",
            generation=0,
            payload={
                "binding": _binding(request),
                "envelope": _envelope(request),
                "payload": dict(request.payload),
                "request_hash": request_hash,
            },
            status="PENDING",
        ))
        await uow.delivery.complete_admission(receipt)
        await uow.delivery.append_audit({
            "owner_scope": str(binding.scope),
            "task_id": str(binding.task_id),
            "attempt_id": str(binding.attempt_id),
            "operation_id": str(envelope.operation_id),
            "registry_id": str(binding.agent_registry_id),
            "event_kind": "operation.admitted",
            "safe_payload": {"request_hash": request_hash, "reservation_id": str(request.reservation.id)},
        })
        await uow.commit()
        return receipt


async def record_execution_observation(factory: UowFactory, observation: ExecutionObservation) -> None:
    binding = observation.binding
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(observation.operation_id)
        if operation["binding"] != _json_value(binding):
            raise Conflict("execution observation binding mismatch")
        await uow.tasks.lock_scope(binding.scope)
        await uow.tasks.lock_task(binding.task_id)
        attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        agent = await uow.agents.lock_registry(binding.agent_registry_id)
        if agent.provider_id != binding.provider_agent_id or agent.owner_scope != binding.scope:
            raise Conflict("execution observation registry mismatch")
        await uow.agents.assert_current_lease(binding.agent_registry_id, observation.lease_owner, observation.observer_fence)
        active = await uow.agents.active_execution_hold(binding.agent_registry_id, lock=True)
        if active is None or active["operation_id"] != observation.operation_id:
            raise StaleInput("execution hold is unavailable")
        if observation.state == "RUNNING" and active["state"] == "UNKNOWN":
            raise UnknownExecution("unknown execution cannot be reset to running")
        await uow.delivery.record_execution(observation)
        if observation.state == "RUNNING":
            await uow.tasks.observe_attempt(binding.attempt_id, AttemptEvent.START)
        elif observation.state == "QUIESCENT":
            if attempt.status not in {AttemptStatus.SUCCEEDED, AttemptStatus.FAILED, AttemptStatus.TIMED_OUT, AttemptStatus.CANCELLED}:
                event = {
                    "SUCCEEDED": AttemptEvent.SUCCEED,
                    "FAILED": AttemptEvent.FAIL,
                    "TIMED_OUT": AttemptEvent.TIMEOUT,
                    "CANCELLED": AttemptEvent.CANCEL,
                }[observation.outcome]
                await uow.tasks.observe_attempt(binding.attempt_id, event)
            await uow.agents.set_registry_ready(binding.agent_registry_id)
            if observation.outcome == "CANCELLED":
                await uow.tasks.confirm_cancelled(binding.task_id)
            await uow.budgets.mark_operation_calls_quiescent(observation.operation_id)
            await uow.budgets.release_unallocated_after_quiescence(observation.operation_id)
        await uow.commit()
