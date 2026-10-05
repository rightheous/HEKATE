from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from hekate.application.budgets import _restore_binding
from hekate.application.evidence import manifest_references_current
from hekate.domain.errors import Conflict, EvidenceUnavailable, PolicyDenied, StaleInput, UnknownExecution
from hekate.domain.models import AdmissionReceipt, AdmissionRequest, Attempt, ExecutionObservation, OutboxJob, ProviderCallPlan, RuntimeBinding
from hekate.domain.types import AccountingCallId, AttemptEvent, AttemptId, AttemptStatus, TaskStatus
from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.ports.store import UowFactory
from hekate.ports.runtime import AgentRuntime


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
        "context_manifest": request.context_manifest,
        "workflow_stage": request.workflow_stage,
        "workflow_stage_hash": request.workflow_stage_hash,
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
    if "call_plan" in request.payload:
        plan = ProviderCallPlan.model_validate(request.payload["call_plan"], strict=True)
        if (
            plan.model not in envelope.model_allowlist
            or plan.pricing_version != envelope.pricing_version
            or plan.max_input_tokens > envelope.max_input_tokens
            or plan.max_output_tokens > envelope.max_output_tokens
            or plan.main_turn_calls + plan.compaction_calls + plan.retry_calls > envelope.billable_call_slots
        ):
            raise Conflict("provider call plan exceeds the persisted envelope")
    if envelope.deadline.tzinfo is None or envelope.deadline.utcoffset() is None:
        raise ValueError("execution deadline must be timezone-aware")
    if request.attempt_kind in {"critic_review", "synthesis"} and (
        request.workflow_stage is None or request.workflow_stage_hash is None
    ):
        raise PolicyDenied("Critic and synthesis admissions require a durable workflow step")


async def prepare_runtime_session(
    factory: UowFactory,
    runtime: AgentRuntime,
    binding: RuntimeBinding,
    lease_owner: str,
    output_contract: str | None = None,
) -> tuple[RuntimeBinding, dict[str, object]]:
    async with factory() as uow:
        agent = await uow.agents.lock_registry(binding.agent_registry_id)
        if agent.provider_id != binding.provider_agent_id:
            raise Conflict("session binding differs from registered provider agent")
        await uow.agents.assert_current_lease(binding.agent_registry_id, lease_owner, binding.fence)
        if await uow.agents.active_execution_hold(binding.agent_registry_id, lock=True) is not None:
            raise UnknownExecution("unresolved execution blocks session preparation")
        if agent.intended_state == "BUSY":
            raise UnknownExecution("agent is temporarily busy with another Task")
        if agent.intended_state != "READY":
            raise PolicyDenied("agent is not ready for session preparation")
        await uow.commit()

    prepared, session = (
        await runtime.prepare_session(binding, output_contract=output_contract)
        if output_contract is not None
        else await runtime.prepare_session(binding)
    )
    if (
        prepared.task_id != binding.task_id
        or prepared.attempt_id != binding.attempt_id
        or prepared.agent_registry_id != binding.agent_registry_id
        or prepared.provider_agent_id != binding.provider_agent_id
        or prepared.input_revision != binding.input_revision
        or prepared.fence != binding.fence
        or not prepared.conversation_id
    ):
        raise Conflict("prepared session changed its trusted runtime binding")
    return prepared, dict(session)


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
            request.workflow_stage_hash,
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
        if request.context_manifest is None and (task.topic_id is not None or task.evidence_refs or task.base_position_version):
            raise PolicyDenied("context_manifest_missing")
        if task.status in {TaskStatus.STOPPING, TaskStatus.CANCELLED, TaskStatus.COMPLETED, TaskStatus.FAILED}:
            raise PolicyDenied("task cancellation or terminal state blocks dispatch")
        if task.status == TaskStatus.WAITING and request.workflow_stage is None:
            raise PolicyDenied("waiting Task admits only its approved workflow step")
        if request.workflow_stage is not None:
            workflow = await uow.critic_workflows.authorize_admission(
                task_id=binding.task_id, scope=binding.scope, stage=request.workflow_stage,
                attempt_id=binding.attempt_id, operation_id=envelope.operation_id,
                registry_id=binding.agent_registry_id,
            )
            if (
                workflow.input_revision != binding.input_revision
                or request.parent_attempt_id != workflow.parent_attempt_id
                or request.workflow_stage_hash != workflow.stage_hash(request.workflow_stage)
            ):
                raise PolicyDenied("workflow admission binding changed")
        if task.deadline <= now or envelope.deadline > task.deadline or envelope.deadline <= now:
            raise PolicyDenied("task or operation deadline expired")
        if await uow.tasks.attempt_exists(binding.attempt_id):
            raise Conflict("attempt id is already bound")
        agent = await uow.agents.lock_registry(binding.agent_registry_id)
        if agent.owner_scope != binding.scope or agent.provider_id != binding.provider_agent_id:
            raise Conflict("registry and provider-agent binding mismatch")
        if agent.policy_version != binding.policy_version:
            raise PolicyDenied("agent policy changed")
        if request.workflow_stage == "critic_review" and (
            agent.kind != "critic" or agent.persistence != "ephemeral" or agent.task_id != task.id
        ):
            raise PolicyDenied("review admission requires the Task's ephemeral Critic registry")
        if request.workflow_stage == "synthesis":
            planning_attempt = await uow.tasks.get_attempt(workflow.parent_attempt_id)
            if (
                agent.kind != "hekate" or agent.persistence != "persistent" or agent.task_id is not None
                or agent.registry_id != planning_attempt.agent_registry_id
            ):
                raise PolicyDenied("synthesis must use the persistent HEKATE bound to planning")
        if request.context_manifest is not None:
            if request.context_manifest.get("task_id") != str(binding.task_id) or request.context_manifest.get("input_revision") != binding.input_revision:
                raise Conflict("context manifest differs from the trusted Task binding")
            if not await manifest_references_current(
                uow, binding.scope, binding.authz_epoch, request.context_manifest,
            ):
                raise EvidenceUnavailable("evidence_reference_unavailable")
        await uow.agents.assert_current_lease(binding.agent_registry_id, request.lease_owner, binding.fence)
        active = await uow.agents.active_execution_hold(binding.agent_registry_id, lock=True)
        if active is not None:
            raise UnknownExecution("agent has an unresolved execution")
        if agent.intended_state == "BUSY":
            raise UnknownExecution("agent is temporarily busy with another Task")
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
        if request.workflow_stage == "critic_review":
            await uow.critic_workflows.transition(
                binding.task_id, ("CRITIC_READY",), "REVIEW_ADMITTED",
            )
        elif request.workflow_stage == "synthesis":
            await uow.critic_workflows.transition(
                binding.task_id, ("SYNTHESIS_PENDING",), "SYNTHESIS_ADMITTED",
            )
        await uow.delivery.append_audit({
            "owner_scope": str(binding.scope),
            "task_id": str(binding.task_id),
            "attempt_id": str(binding.attempt_id),
            "operation_id": str(envelope.operation_id),
            "registry_id": str(binding.agent_registry_id),
            "event_kind": "operation.admitted",
            "safe_payload": {"request_hash": request_hash, "reservation_id": str(request.reservation.id)},
        })
        if request.task_preparation_owner is not None:
            await uow.tasks.mark_task_preparation_admitted(
                binding.task_id, binding.input_revision, request.task_preparation_owner,
                binding.conversation_id or "", binding.fence,
            )
        if request.context_manifest is not None:
            await uow.knowledge.save_context_manifest(
                envelope.operation_id, binding.task_id, binding.input_revision,
                request.context_manifest,
            )
        await uow.commit()
        return receipt


async def record_dispatch_send_intent(factory: UowFactory, job: OutboxJob, worker: str) -> None:
    if job.kind != "dispatch" or job.claim_fence is None or job.claim_expires_at is None:
        raise Conflict("outbox dispatch claim is incomplete")
    binding = _restore_binding({"binding": job.payload.get("binding")})
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(job.operation_id)
        if operation["binding"] != _json_value(binding) or operation["request_hash"] != job.payload.get("request_hash"):
            raise Conflict("outbox payload differs from admitted operation")
        if operation["state"] != "ADMITTED" or operation["dispatch_state"] != "INTENT_RECORDED" or operation["execution_state"] != "PENDING":
            raise StaleInput("operation is no longer dispatchable")
        scope = await uow.tasks.lock_scope(binding.scope)
        if (scope.principal_id, scope.policy_version, scope.authz_epoch) != (
            binding.principal_id, binding.policy_version, binding.authz_epoch,
        ):
            raise PolicyDenied("authorization_snapshot_changed")
        task = await uow.tasks.lock_task(binding.task_id)
        if task.scope != binding.scope or task.input_revision != binding.input_revision or task.status != TaskStatus.RUNNING:
            raise StaleInput("task binding or state changed before dispatch")
        if task.deadline <= datetime.now(UTC):
            raise StaleInput("task deadline expired before dispatch")
        attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        if attempt.operation_id != job.operation_id or attempt.status != AttemptStatus.PENDING:
            raise StaleInput("attempt is no longer pending")
        agent = await uow.agents.lock_registry(binding.agent_registry_id)
        if agent.owner_scope != binding.scope or agent.provider_id != binding.provider_agent_id or agent.intended_state != "BUSY":
            raise StaleInput("registry binding changed before dispatch")
        manifest_row = await uow.knowledge.get_context_manifest(job.operation_id)
        if manifest_row is None:
            if task.topic_id is not None or task.evidence_refs or task.base_position_version:
                raise PolicyDenied("context_manifest_missing")
        elif not await manifest_references_current(
            uow, binding.scope, binding.authz_epoch, manifest_row["manifest"],
        ):
            raise PolicyDenied("evidence_reference_unavailable")
        await uow.agents.assert_current_lease(binding.agent_registry_id, worker, binding.fence)
        hold = await uow.agents.active_execution_hold(binding.agent_registry_id, lock=True)
        if hold is None or hold["operation_id"] != job.operation_id or hold["state"] != "PENDING":
            raise UnknownExecution("execution hold is unavailable before dispatch")
        await uow.delivery.record_send_intent(job, worker, job.claim_fence)
        await uow.delivery.append_audit({
            "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
            "attempt_id": str(binding.attempt_id), "operation_id": str(job.operation_id),
            "registry_id": str(binding.agent_registry_id), "event_kind": "outbox.send_intent",
            "safe_payload": {"claim_fence": job.claim_fence},
        })
        await uow.commit()


async def record_dispatch_accepted(factory: UowFactory, job: OutboxJob, worker: str) -> None:
    if job.claim_fence is None:
        raise Conflict("outbox dispatch claim has no fence")
    binding = _restore_binding({"binding": job.payload.get("binding")})
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(job.operation_id)
        if operation["binding"] != _json_value(binding):
            raise Conflict("outbox acceptance binding changed")
        await uow.tasks.lock_scope(binding.scope)
        await uow.tasks.lock_task(binding.task_id)
        attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        await uow.agents.lock_registry(binding.agent_registry_id)
        await uow.agents.assert_current_lease(binding.agent_registry_id, worker, binding.fence)
        if attempt.status == AttemptStatus.PENDING:
            await uow.tasks.observe_attempt(binding.attempt_id, AttemptEvent.DISPATCH)
        elif attempt.status not in {AttemptStatus.DISPATCHED, AttemptStatus.RUNNING, AttemptStatus.SUCCEEDED, AttemptStatus.FAILED, AttemptStatus.TIMED_OUT, AttemptStatus.CANCELLED}:
            raise StaleInput("attempt state changed after bridge acceptance")
        await uow.delivery.mark_dispatch_accepted(job, worker, job.claim_fence)
        await uow.delivery.append_audit({
            "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
            "attempt_id": str(binding.attempt_id), "operation_id": str(job.operation_id),
            "registry_id": str(binding.agent_registry_id), "event_kind": "outbox.sdk_accepted",
            "safe_payload": {"claim_fence": job.claim_fence, "attempt_was": attempt.status.value},
        })
        await uow.commit()


async def apply_execution_observation(uow, observation: ExecutionObservation) -> None:
    binding = observation.binding
    operation = await uow.delivery.lock_operation(observation.operation_id)
    if operation["binding"] != _json_value(binding):
        raise Conflict("execution observation binding mismatch")
    await uow.tasks.lock_scope(binding.scope)
    task_before = await uow.tasks.lock_task(binding.task_id)
    attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
    agent = await uow.agents.lock_registry(binding.agent_registry_id)
    if agent.provider_id != binding.provider_agent_id or agent.owner_scope != binding.scope:
        raise Conflict("execution observation registry mismatch")
    if observation.processor_owner is not None:
        lease = await uow.agents.get_lease(binding.agent_registry_id)
        if lease is None or lease.owner != observation.processor_owner:
            raise UnknownExecution("inbox processor does not hold the registry lease")
        try:
            await uow.agents.assert_current_lease(binding.agent_registry_id, lease.owner, lease.fence)
        except StaleInput as error:
            raise UnknownExecution("inbox processor lease expired") from error
    else:
        await uow.agents.assert_current_lease(binding.agent_registry_id, observation.lease_owner, observation.observer_fence)
    active = await uow.agents.active_execution_hold(binding.agent_registry_id, lock=True)
    if active is None:
        if observation.state == "QUIESCENT":
            await uow.delivery.record_execution(observation)
            return
        raise StaleInput("execution hold is unavailable")
    if active["operation_id"] != observation.operation_id:
        raise StaleInput("execution hold belongs to another operation")
    if observation.state == "RUNNING" and active["state"] == "UNKNOWN":
        raise UnknownExecution("unknown execution cannot be reset to running")
    if observation.state == "QUIESCENT":
        call_ids = await uow.budgets.call_ids_for_operation(observation.operation_id)
        for call_id in call_ids:
            call = await uow.budgets.get_call(AccountingCallId(call_id))
            if call and call["status"] in {"CONSUMED", "DISPATCHED", "RUNNING", "UNKNOWN"}:
                raise UnknownExecution("provider call lacks individual termination evidence")
    await uow.delivery.record_execution(observation)
    if observation.state == "RUNNING":
        if attempt.status == AttemptStatus.DISPATCHED:
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
        await uow.budgets.release_unallocated_after_quiescence(observation.operation_id)
        from hekate.application.tasks import converge_task_execution

        await converge_task_execution(uow, binding.task_id, binding.input_revision)
        if observation.source == "pre_dispatch_policy":
            current_task = await uow.tasks.lock_task(binding.task_id)
            if (
                task_before.input_revision == binding.input_revision
                and task_before.status == TaskStatus.RUNNING
                and task_before.cancel_requested_at is None
                and task_before.deadline > observation.observed_at
                and current_task.input_revision == binding.input_revision
                and current_task.status == TaskStatus.RUNNING
                and current_task.cancel_requested_at is None
            ):
                if not await uow.tasks.fail_policy_task(binding.task_id, binding.input_revision):
                    raise StaleInput("Task changed before policy rejection was finalized")
            else:
                # Cancellation, deadline, or a newer revision owns Task
                # convergence; a stale child dispatch cannot fail it again.
                await uow.delivery.append_audit({
                    "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
                    "attempt_id": str(binding.attempt_id), "operation_id": str(observation.operation_id),
                    "registry_id": str(binding.agent_registry_id),
                    "event_kind": "dispatch.rejected_after_task_change",
                    "safe_payload": {"reason": observation.reason, "current_revision": current_task.input_revision},
                })
            if current_task.status in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}:
                workflow = await uow.critic_workflows.get(binding.task_id, lock=True)
                if workflow is not None and workflow.stage in {
                    "REVIEW_ADMITTED", "SYNTHESIS_PENDING", "SYNTHESIS_ADMITTED", "FAILED",
                }:
                    from hekate.application.lifecycle import _queue_retirement_in_uow

                    try:
                        await _queue_retirement_in_uow(uow, workflow)
                    except (PolicyDenied, UnknownExecution):
                        pass
            await uow.delivery.append_audit({
                "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id), "operation_id": str(observation.operation_id),
                "registry_id": str(binding.agent_registry_id), "event_kind": "task.policy_rejected",
                "safe_payload": {"reason": observation.reason},
            })


async def record_execution_observation(factory: UowFactory, observation: ExecutionObservation) -> None:
    async with factory() as uow:
        await apply_execution_observation(uow, observation)
        await uow.commit()
