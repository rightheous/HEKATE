from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from uuid import NAMESPACE_URL, uuid5

from hekate.application.budgets import _restore_binding
from hekate.domain.contracts import canonical_json_hash
from hekate.domain.errors import Conflict, PolicyDenied, StaleInput, UnknownExecution
from hekate.domain.models import (
    AdmissionReceipt, AgentRecord, Attempt, CreateObservation, CriticWorkflow,
    DeleteObservation, OutboxJob, ReservationRequest, RetirementReceipt,
    SpawnProposal, TaskExecutionConfig, StoredConclusion,
)
from hekate.domain.types import (
    AccountingCallId, ActorContext, AgentState, AttemptId, DeploymentId, DomainId, OperationId, PrincipalId,
    ProviderAgentId, RegistryId, ReservationId, ScopeId, StopReason, TaskId, TaskStatus,
)
from hekate.ports.runtime import AgentRuntime
from hekate.ports.store import UowFactory, UnitOfWork
from hekate.settings import execution_config_snapshot, restore_execution_config


_LOG = logging.getLogger(__name__)
_DELIBERATION_MAINTENANCE_RETRY_SECONDS = 60


def _field(value: object, name: str) -> object:
    return value.get(name) if isinstance(value, Mapping) else getattr(value, name)


def _hekate_creation_hash(scope: ScopeId) -> str:
    return canonical_json_hash({"scope": str(scope), "role": "hekate", "persistence": "persistent"})


def _assert_hekate_create_observation(
    operation: Mapping[str, object], registry: AgentRecord, scope: ScopeId,
) -> ProviderAgentId:
    observation = operation.get("observation")
    if (
        operation.get("state") != "COMPLETED"
        or operation.get("dispatch_state") != "QUIESCENT"
        or operation.get("execution_state") != "QUIESCENT"
        or not isinstance(observation, Mapping)
        or observation.get("registry_id") != str(registry.registry_id)
        or observation.get("owner") != str(scope)
        or observation.get("creation_tag") != str(registry.creation_operation_id)
        or observation.get("role") != "hekate"
        or observation.get("present") is not True
        or observation.get("external_call_started") is not True
        or observation.get("confirmation") not in {"agent.get", "agent.list"}
    ):
        raise Conflict("completed persistent HEKATE create journal does not match its registry")
    provider_id = ProviderAgentId(str(observation.get("provider_agent_id", "")))
    if registry.provider_id != provider_id:
        raise Conflict("persistent HEKATE create journal differs from its provider binding")
    return provider_id


async def _mark_hekate_create_unknown(
    factory: UowFactory, scope: ScopeId, operation_id: OperationId, request_hash: str, reason: str,
) -> None:
    async with factory() as uow:
        await uow.tasks.lock_scope(scope)
        operation = await uow.delivery.claim_lifecycle_operation(
            operation_id, scope, "agent.create", request_hash,
        )
        # A late reconciliation failure must never downgrade a completed create.
        # Keep this protection local to persistent HEKATE; Critic lifecycle rows
        # retain their existing state machine.
        if operation.get("state") == "COMPLETED":
            await uow.commit()
            return
        if (
            operation.get("state") == "UNKNOWN"
            and operation.get("last_error") == reason[:240]
            and isinstance(operation.get("observation"), Mapping)
            and operation["observation"].get("external_call_started") is True
        ):
            await uow.commit()
            return
        await uow.delivery.mark_lifecycle_unknown(operation_id, reason)
        await uow.commit()


def _assert_hekate_runtime_identity(
    value: object, *, scope: ScopeId, creation_id: OperationId, provider_id: ProviderAgentId,
    source: str,
) -> None:
    state = _field(value, "state") if source == "agent.get" else None
    state = getattr(state, "value", state)
    observed_id = str(_field(value, "provider_agent_id"))
    owner = _field(value, "owner")
    creation_tag = _field(value, "creation_tag")
    role = _field(value, "role") if source in {"agent.get", "agent.list"} else "hekate"
    present = (
        state == "PRESENT" if source == "agent.get"
        else _field(value, "present") is True if source == "agent.create"
        else source == "agent.list"
    )
    if (
        observed_id != str(provider_id)
        or owner != str(scope)
        or creation_tag != str(creation_id)
        or role != "hekate"
        or not present
    ):
        raise Conflict("Letta response did not confirm the persistent HEKATE creation identity")


async def ensure_hekate(
    factory: UowFactory,
    runtime: AgentRuntime,
    actor: ActorContext,
    config: TaskExecutionConfig,
) -> AgentRecord:
    creation_id = OperationId(str(uuid5(NAMESPACE_URL, f"hekate:create:{actor.scope}")))
    registry_id = RegistryId(str(uuid5(NAMESPACE_URL, f"hekate:registry:{actor.scope}")))
    request_hash = _hekate_creation_hash(ScopeId(actor.scope))
    should_create = False
    already_completed = False
    async with factory() as uow:
        authorization = await uow.tasks.lock_scope(actor.scope)
        if (authorization.principal_id, authorization.policy_version, authorization.authz_epoch) != (
            actor.principal_id, actor.policy_version, actor.authz_epoch,
        ):
            raise PolicyDenied("authorization snapshot changed")
        operation = await uow.delivery.claim_lifecycle_operation(
            creation_id, ScopeId(actor.scope), "agent.create", request_hash,
        )
        record = await uow.agents.get_persistent_scope(actor.scope, lock=True)
        if record is None:
            if not operation.get("created_now"):
                raise UnknownExecution("persistent HEKATE registry is missing for an existing create journal")
            record = AgentRecord(
                registry_id=registry_id,
                owner_scope=ScopeId(actor.scope),
                kind="hekate",
                persistence="persistent",
                creation_operation_id=creation_id,
                intended_state="CREATING",
                observation="UNKNOWN",
                policy_version=actor.policy_version,
            )
            await uow.agents.insert_intent(record)
            if not await uow.delivery.mark_lifecycle_started(creation_id, "agent.create"):
                raise Conflict("new persistent HEKATE creation intent was already started")
            should_create = True
        elif (
            record.registry_id != registry_id
            or record.owner_scope != actor.scope
            or record.kind != "hekate"
            or record.persistence != "persistent"
            or record.creation_operation_id != creation_id
        ):
            raise Conflict("persistent HEKATE registry differs from its deterministic creation identity")
        elif operation.get("state") == "COMPLETED":
            _assert_hekate_create_observation(operation, record, ScopeId(actor.scope))
            already_completed = True
        await uow.commit()

    confirmation = "agent.get"
    if should_create:
        try:
            created = await runtime.create_agent({
                "owner": str(actor.scope),
                "creation_tag": str(record.creation_operation_id),
                "role": "hekate",
                "persistence": "persistent",
                "registry_id": str(record.registry_id),
                "model": config.letta_model,
                "max_input_tokens": config.max_input_tokens,
                "max_output_tokens": config.max_output_tokens,
            }, record.creation_operation_id)
            provider_id = ProviderAgentId(str(_field(created, "provider_agent_id")))
            _assert_hekate_runtime_identity(
                created, scope=ScopeId(actor.scope), creation_id=creation_id,
                provider_id=provider_id, source="agent.create",
            )
            observed = await runtime.observe_agent(provider_id)
            _assert_hekate_runtime_identity(
                observed, scope=ScopeId(actor.scope), creation_id=creation_id,
                provider_id=provider_id, source="agent.get",
            )
        except Conflict as error:
            await _mark_hekate_create_unknown(
                factory, ScopeId(actor.scope), creation_id, request_hash, type(error).__name__,
            )
            raise
        except Exception as error:
            await _mark_hekate_create_unknown(
                factory, ScopeId(actor.scope), creation_id, request_hash, type(error).__name__,
            )
            raise UnknownExecution("persistent HEKATE create outcome requires owner-tag reconciliation") from error
    elif not already_completed:
        try:
            candidates = await runtime.list_owned_agents(
                DeploymentId(str(actor.scope)), creation_id,
            )
        except Exception as error:
            await _mark_hekate_create_unknown(
                factory, ScopeId(actor.scope), creation_id, request_hash, type(error).__name__,
            )
            raise UnknownExecution("persistent HEKATE create outcome could not be queried") from error
        if len(candidates) != 1:
            reason = "no matching persistent HEKATE agent" if not candidates else "multiple matching persistent HEKATE agents"
            await _mark_hekate_create_unknown(factory, ScopeId(actor.scope), creation_id, request_hash, reason)
            raise UnknownExecution("persistent HEKATE create intent requires unambiguous owner-tag reconciliation")
        candidate = candidates[0]
        try:
            provider_id = ProviderAgentId(str(_field(candidate, "provider_agent_id")))
            _assert_hekate_runtime_identity(
                candidate, scope=ScopeId(actor.scope), creation_id=creation_id,
                provider_id=provider_id, source="agent.list",
            )
            if record.provider_id is not None and record.provider_id != provider_id:
                raise Conflict("persistent HEKATE runtime ID differs from its stored provider binding")
        except Conflict:
            await _mark_hekate_create_unknown(
                factory, ScopeId(actor.scope), creation_id, request_hash, "runtime ownership or provider binding mismatch",
            )
            raise
        confirmation = "agent.list"
    else:
        provider_id = record.provider_id

    if not already_completed:
        async with factory() as uow:
            await uow.tasks.lock_scope(actor.scope)
            operation = await uow.delivery.claim_lifecycle_operation(
                creation_id, ScopeId(actor.scope), "agent.create", request_hash,
            )
            current = await uow.agents.lock_registry(registry_id)
            if (
                current.owner_scope != actor.scope or current.registry_id != registry_id
                or current.kind != "hekate" or current.persistence != "persistent"
                or current.creation_operation_id != creation_id
            ):
                raise Conflict("persistent HEKATE registry changed before create journal completion")
            if operation.get("state") == "COMPLETED":
                _assert_hekate_create_observation(operation, current, ScopeId(actor.scope))
                if current.provider_id != provider_id:
                    raise Conflict("concurrent persistent HEKATE reconciliation selected another provider ID")
            else:
                if operation.get("state") not in {"CLAIMED", "UNKNOWN"}:
                    raise Conflict("persistent HEKATE create journal is not completable")
                if current.provider_id is not None and current.provider_id != provider_id:
                    raise Conflict("persistent HEKATE provider binding changed")
                if current.provider_id is None:
                    await uow.agents.bind_provider(registry_id, provider_id)
                await uow.delivery.complete_lifecycle_operation(creation_id, {
                    "provider_agent_id": str(provider_id), "registry_id": str(registry_id),
                    "owner": str(actor.scope), "creation_tag": str(creation_id),
                    "role": "hekate", "present": True, "confirmation": confirmation,
                    "external_call_started": True,
                })
            await uow.commit()

    async with factory() as uow:
        authorization = await uow.tasks.lock_scope(actor.scope)
        record = await uow.agents.lock_registry(record.registry_id)
        await uow.commit()
        if (authorization.principal_id, authorization.policy_version, authorization.authz_epoch) != (
            actor.principal_id, actor.policy_version, actor.authz_epoch,
        ) or record.owner_scope != actor.scope or record.policy_version != actor.policy_version:
            raise PolicyDenied("persistent HEKATE scope or policy changed")
        if record.provider_id is None:
            raise UnknownExecution("persistent HEKATE provider binding is not confirmed")
        if record.intended_state != "READY":
            raise UnknownExecution("persistent HEKATE creation is confirmed but the agent is not ready")
        return record


def _operation_id(label: str, task_id: TaskId) -> OperationId:
    return OperationId(str(uuid5(NAMESPACE_URL, f"hekate:{label}:{task_id}")))


def _reservation_amount(config: TaskExecutionConfig) -> Decimal:
    return (
        Decimal(config.max_input_tokens) * config.input_usd_per_million
        + Decimal(config.max_output_tokens) * config.output_usd_per_million
    ) * Decimal(1 + config.max_compaction_calls) / Decimal(1_000_000)


class WorkflowStopCode(StrEnum):
    EVIDENCE_UNAVAILABLE = "evidence_unavailable"
    AUTHORIZATION_CHANGED = "authorization_changed"
    POLICY_REJECTED = "policy_rejected"


_WORKFLOW_STOP_TEXT = {
    WorkflowStopCode.EVIDENCE_UNAVAILABLE: "The task stopped because selected evidence is no longer available.",
    WorkflowStopCode.AUTHORIZATION_CHANGED: "The task stopped because its authorization changed.",
    WorkflowStopCode.POLICY_REJECTED: "The task could not continue under the current policy.",
}

_DELIBERATION_POLICY_TEXT = {
    "evidence_unavailable": "The task stopped because selected evidence is no longer available.",
    "authorization_changed": "The task stopped because its authorization changed.",
    "policy_rejected": "The task could not continue under the current policy.",
}


async def _lock_workflow_operations(uow: UnitOfWork, workflow: CriticWorkflow) -> dict[str, Mapping[str, object]]:
    step_rows = await uow.deliberation.list_for_task(workflow.task_id)
    operation_ids = sorted({
        str(workflow.planning_operation_id), str(workflow.create_operation_id),
        str(workflow.review_operation_id), str(workflow.synthesis_operation_id),
        *(str(row["operation_id"]) for row in step_rows),
    })
    rows: dict[str, Mapping[str, object]] = {}
    for value in operation_ids:
        operation_id = OperationId(value)
        rows[value] = await uow.delivery.lock_operation(operation_id)
    return rows


async def _verify_and_seal_workflow_steps(
    uow: UnitOfWork,
    workflow: CriticWorkflow,
    operations: Mapping[str, Mapping[str, object]],
    reason: str,
) -> None:
    deferred: list[tuple[OperationId, str]] = []
    for operation_id, stage in sorted((
        (workflow.review_operation_id, "critic_review"),
        (workflow.synthesis_operation_id, "synthesis"),
    ), key=lambda item: str(item[0])):
        row = operations[str(operation_id)]
        observation = row.get("observation") or {}
        if (
            row["state"] == "CLAIMED" and row["execution_state"] == "PENDING"
            and row["dispatch_state"] == "NOT_STARTED" and row["task_id"] is None
            and row["binding"] == {} and row["envelope"] == {}
            and observation.get("deferred_task_id") == str(workflow.task_id)
            and observation.get("deferred_stage_hash") == workflow.stage_hash(stage)
        ):
            deferred.append((operation_id, stage))
            continue
        if row["execution_state"] != "QUIESCENT" or row["dispatch_state"] not in {"QUIESCENT", "INTENT_RECORDED"}:
            raise UnknownExecution("Critic workflow step has no quiescent execution proof")
        if row["state"] not in {"ADMITTED", "COMPLETED", "FAILED"}:
            raise UnknownExecution("Critic workflow step state is not safely terminal")
        for call_id in await uow.budgets.call_ids_for_operation(operation_id):
            call = await uow.budgets.get_call(AccountingCallId(call_id))
            if call is not None and call["status"] in {"CONSUMED", "DISPATCHED", "RUNNING", "UNKNOWN"}:
                raise UnknownExecution("Critic workflow provider call is not confirmed quiescent")
    for operation_id, stage in deferred:
        await uow.delivery.reject_deferred_operation(
            operation_id, workflow.task_id, workflow.stage_hash(stage), reason,
        )
    for step in await uow.deliberation.list_for_task(workflow.task_id, lock=True):
        operation_id = OperationId(step["operation_id"])
        row = operations[str(operation_id)]
        observation = row.get("observation") or {}
        deferred_step = (
            step["state"] in {"READY", "WAITING_PARENT"}
            and row["state"] == "CLAIMED" and row["execution_state"] == "PENDING"
            and row["dispatch_state"] == "NOT_STARTED" and row["task_id"] is None
            and row["binding"] == {} and row["envelope"] == {}
            and observation.get("deferred_task_id") == str(workflow.task_id)
            and observation.get("deferred_stage_hash") == uow.deliberation.stage_hash(step)
        )
        if deferred_step:
            await uow.delivery.reject_deferred_operation(
                operation_id, workflow.task_id, uow.deliberation.stage_hash(step), reason,
            )
            await uow.deliberation.transition(
                step["id"], (step["state"],), "STOPPED", stop_reason=reason,
            )
            continue
        if row["execution_state"] != "QUIESCENT" or row["dispatch_state"] not in {"QUIESCENT", "INTENT_RECORDED"}:
            raise UnknownExecution("deliberation step has no quiescent execution proof")
        if row["state"] not in {"ADMITTED", "COMPLETED", "FAILED"}:
            raise UnknownExecution("deliberation step state is not safely terminal")
        for call_id in await uow.budgets.call_ids_for_operation(operation_id):
            call = await uow.budgets.get_call(AccountingCallId(call_id))
            if call is not None and call["status"] in {"CONSUMED", "DISPATCHED", "RUNNING", "UNKNOWN"}:
                raise UnknownExecution("deliberation provider call is not confirmed quiescent")


async def reject_critic_workflow_step(
    factory: UowFactory,
    selected: CriticWorkflow,
    expected_stage: str,
    code: WorkflowStopCode,
) -> str:
    """Converge a permanent step rejection without trusting the stale worker actor."""
    async with factory() as uow:
        operations = await _lock_workflow_operations(uow, selected)
        authorization = await uow.tasks.lock_scope(selected.owner_scope)
        task = await uow.tasks.lock_task(selected.task_id)
        workflow = await uow.critic_workflows.get(selected.task_id, lock=True)
        if workflow is None or workflow.stage != expected_stage:
            await uow.commit()
            return "stage_changed"
        if (
            workflow.planning_operation_id != selected.planning_operation_id
            or workflow.create_operation_id != selected.create_operation_id
            or workflow.review_operation_id != selected.review_operation_id
            or workflow.synthesis_operation_id != selected.synthesis_operation_id
        ):
            await uow.commit()
            return "operation_identity_changed"
        if task.scope != workflow.owner_scope:
            await uow.commit()
            return "scope_changed"
        if task.input_revision != workflow.input_revision:
            await uow.critic_workflows.transition(
                task.id, (workflow.stage,), "FAILED",
            )
            await uow.delivery.append_audit({
                "owner_scope": str(workflow.owner_scope), "task_id": str(task.id),
                "operation_id": str(workflow.planning_operation_id),
                "registry_id": str(workflow.critic_registry_id),
                "event_kind": "critic.workflow_superseded",
                "safe_payload": {
                    "workflow_revision": workflow.input_revision,
                    "current_revision": task.input_revision,
                },
            })
            await uow.commit()
            return "superseded"
        if task.cancel_requested_at is not None or task.deadline <= datetime.now(UTC):
            await uow.commit()
            return "task_stop_pending"
        if task.status != TaskStatus.WAITING:
            await uow.commit()
            return "task_not_waiting"
        if await uow.tasks.get_task_response(task.id) is not None:
            await uow.commit()
            return "response_already_final"

        planning = operations[str(workflow.planning_operation_id)]
        planning_binding = _restore_binding(planning)
        if (
            planning_binding.task_id != task.id
            or planning_binding.attempt_id != workflow.parent_attempt_id
            or planning_binding.input_revision != workflow.input_revision
            or planning_binding.scope != workflow.owner_scope
            or planning["state"] != "COMPLETED"
            or planning["execution_state"] != "QUIESCENT"
        ):
            raise UnknownExecution("planning operation is not safely bound and quiescent")
        create = operations[str(workflow.create_operation_id)]
        if create["state"] != "COMPLETED" or create["execution_state"] != "QUIESCENT":
            raise UnknownExecution("Critic creation is not safely confirmed")

        registry = await uow.agents.lock_registry(planning_binding.agent_registry_id)
        if (
            registry.kind != "hekate" or registry.persistence != "persistent"
            or registry.owner_scope != workflow.owner_scope
            or registry.policy_version != planning_binding.policy_version
        ):
            code = WorkflowStopCode.POLICY_REJECTED
        if (
            authorization.principal_id != planning_binding.principal_id
            or authorization.policy_version != planning_binding.policy_version
            or authorization.authz_epoch != planning_binding.authz_epoch
        ):
            code = WorkflowStopCode.AUTHORIZATION_CHANGED

        if code != WorkflowStopCode.AUTHORIZATION_CHANGED and task.evidence_refs:
            accessible = await uow.knowledge.lock_accessible_references(
                workflow.owner_scope, task.evidence_refs, authorization.authz_epoch,
            )
            if len(accessible) != len(set(task.evidence_refs)):
                code = WorkflowStopCode.EVIDENCE_UNAVAILABLE

        planning_result = await uow.knowledge.accepted_turn_result_for_conclusion(
            str(workflow.planning_conclusion_id),
        )
        if planning_result is None or (
            planning_result["task_id"] != task.id
            or planning_result["attempt_id"] != workflow.parent_attempt_id
            or planning_result["operation_id"] != workflow.planning_operation_id
            or planning_result["registry_id"] != planning_binding.agent_registry_id
            or planning_result["input_revision"] != workflow.input_revision
        ):
            raise UnknownExecution("accepted planning result is unavailable for the server response")

        response = {
            "operation_id": str(workflow.planning_operation_id),
            "attempt_id": str(workflow.parent_attempt_id),
            "registry_id": str(planning_binding.agent_registry_id),
            "source_inbox_id": planning_result["inbox_id"],
            "proposal": {
                "action": "abstain", "server_generated": True,
                "failure_code": code.value,
            },
            "response_text": _WORKFLOW_STOP_TEXT[code],
            "outcome": "FAILED", "stop_reason": StopReason.POLICY.value,
        }
        if not await uow.tasks.fail_waiting_task_with_response(
            task.id, workflow.input_revision, response,
        ):
            await uow.commit()
            return "task_changed"

        await uow.critic_workflows.transition(
            task.id, (workflow.stage,), "FAILED",
        )
        await _verify_and_seal_workflow_steps(
            uow, workflow, operations, code.value,
        )
        failed_workflow = workflow.model_copy(update={"stage": "FAILED"})
        critic_registry = await uow.agents.lock_registry(workflow.critic_registry_id)
        if critic_registry.intended_state == "DELETED" and critic_registry.provider_id is None:
            await uow.budgets.release_unallocated_after_quiescence(workflow.review_operation_id)
            await uow.budgets.release_unallocated_after_quiescence(workflow.synthesis_operation_id)
            for step in await uow.deliberation.list_for_task(workflow.task_id):
                await uow.budgets.release_unallocated_after_quiescence(OperationId(step["operation_id"]))
            await uow.critic_workflows.transition(task.id, ("FAILED",), "COMPLETE")
        else:
            await _queue_retirement_in_uow(uow, failed_workflow)
        await uow.delivery.append_audit({
            "owner_scope": str(workflow.owner_scope), "task_id": str(task.id),
            "attempt_id": str(workflow.parent_attempt_id),
            "operation_id": str(workflow.planning_operation_id),
            "registry_id": str(planning_binding.agent_registry_id),
            "event_kind": "critic.workflow_policy_stopped",
            "safe_payload": {
                "failure_code": code.value, "stop_reason": StopReason.POLICY.value,
                "workflow_stage": expected_stage, "input_revision": workflow.input_revision,
                "critic_registry_id": str(workflow.critic_registry_id),
            },
        })
        await uow.commit()
        return "failed_policy"


async def _verify_and_seal_standalone_deliberation_steps(
    uow: UnitOfWork, task_id: TaskId, operations: Mapping[str, Mapping[str, object]], reason: str,
    *, step_rows: Sequence[Mapping[str, object]] | None = None,
) -> tuple[OperationId, ...]:
    """Seal only deferred Phase 5B steps and prove admitted ones are quiescent."""
    step_rows = tuple(step_rows) if step_rows is not None else await uow.deliberation.list_for_task(task_id, lock=True)
    for step in step_rows:
        operation_id = OperationId(step["operation_id"])
        row = operations.get(str(operation_id))
        if row is None:
            raise UnknownExecution("deliberation operation set changed during stop convergence")
        observation = row.get("observation") or {}
        deferred = (
            step["state"] in {"READY", "WAITING_PARENT", "STOPPED"}
            and row["state"] == "CLAIMED" and row["execution_state"] == "PENDING"
            and row["dispatch_state"] == "NOT_STARTED" and row["task_id"] is None
            and row["binding"] == {} and row["envelope"] == {}
            and observation.get("deferred_task_id") == str(task_id)
            and observation.get("deferred_stage_hash") == uow.deliberation.stage_hash(step)
            and observation.get("external_call_started") is not True
        )
        if deferred:
            await uow.delivery.reject_deferred_operation(
                operation_id, task_id, uow.deliberation.stage_hash(step), reason,
            )
            if step["state"] != "STOPPED":
                await uow.deliberation.transition(
                    step["id"], (step["state"],), "STOPPED", stop_reason=reason,
                )
            continue
        if step["state"] in {"STOPPED", "COMPLETE"} and row["state"] == "FAILED" and row["execution_state"] == "QUIESCENT":
            continue
        if row["execution_state"] != "QUIESCENT" or row["dispatch_state"] not in {"QUIESCENT", "INTENT_RECORDED"}:
            raise UnknownExecution("deliberation step has no quiescent execution proof")
        if row["state"] not in {"ADMITTED", "COMPLETED", "FAILED"}:
            raise UnknownExecution("deliberation step state is not safely terminal")
        for call_id in await uow.budgets.call_ids_for_operation(operation_id):
            call = await uow.budgets.get_call(AccountingCallId(call_id))
            if call is not None and call["status"] in {"CONSUMED", "DISPATCHED", "RUNNING", "UNKNOWN"}:
                raise UnknownExecution("deliberation provider call is not confirmed quiescent")
        if step["state"] not in {"STOPPED", "COMPLETE"}:
            await uow.deliberation.transition(
                step["id"], (step["state"],), "STOPPED", stop_reason=reason,
            )
    return tuple(OperationId(row["operation_id"]) for row in step_rows)


async def maintain_deliberation_steps(factory: UowFactory, *, limit: int = 100) -> int:
    """Seal standalone Phase 5B work and advance past deferred candidates."""
    async with factory() as uow:
        candidates = await uow.deliberation.list_maintenance_candidates(limit)
        await uow.commit()

    completed = 0
    seen_tasks: set[str] = set()
    for candidate in candidates:
        task_id = TaskId(str(candidate["task_id"]))
        if str(task_id) in seen_tasks:
            continue
        seen_tasks.add(str(task_id))
        retry_steps: Sequence[Mapping[str, object]] = (candidate,)
        try:
            async with factory() as uow:
                hints = await uow.deliberation.list_for_task(task_id)
                # Lock every related operation in one stable order before scope,
                # Task, workflow, and step rows. Parent results are immutable;
                # their operation identities are included in the same lock set.
                operation_ids = {
                    str(value)
                    for row in hints
                    for value in (row["operation_id"], row["parent_operation_id"])
                }
                for row in hints:
                    candidate_id = (row.get("context") or {}).get(
                        "candidate_conclusion_id", row.get("parent_conclusion_id"),
                    )
                    if candidate_id is None:
                        continue
                    accepted = await uow.knowledge.accepted_turn_result_for_conclusion(str(candidate_id))
                    if accepted is not None:
                        operation_ids.add(str(accepted["operation_id"]))
                operations = {
                    value: await uow.delivery.lock_operation(OperationId(value))
                    for value in sorted(operation_ids)
                }
                await uow.tasks.lock_scope(ScopeId(str(candidate["owner_scope"])))
                task = await uow.tasks.lock_task(task_id)
                workflow = await uow.critic_workflows.get(task_id, lock=True)
                if workflow is not None:
                    await uow.commit()
                    continue
                if task.scope != ScopeId(str(candidate["owner_scope"])):
                    raise UnknownExecution("standalone deliberation Task scope changed")
                steps = await uow.deliberation.list_for_task(task_id, lock=True)
                current_operation_ids = {
                    str(value)
                    for row in steps
                    for value in (row["operation_id"], row["parent_operation_id"])
                }
                if current_operation_ids - set(operations):
                    raise UnknownExecution("standalone deliberation operation set changed during maintenance")

                now = datetime.now(UTC)
                task_stopped = (
                    task.status in {TaskStatus.STOPPING, TaskStatus.CANCELLED, TaskStatus.FAILED, TaskStatus.COMPLETED}
                    or task.cancel_requested_at is not None or task.deadline <= now
                )
                eligible = tuple(
                    row for row in steps
                    if row["state"] in {"READY", "WAITING_PARENT", "ADMITTED", "RESULT_ACCEPTED", "STOPPED"}
                    and (row["input_revision"] < task.input_revision or task_stopped)
                    and row["maintenance_completed_at"] is None
                    and (
                        row["maintenance_retry_after"] is None
                        or row["maintenance_retry_after"] <= now
                    )
                )
                retry_steps = eligible
                if not eligible:
                    await uow.commit()
                    continue

                for step in eligible:
                    parent = operations.get(str(step["parent_operation_id"]))
                    if parent is None or (
                        parent["state"] != "COMPLETED" or parent["execution_state"] != "QUIESCENT"
                    ):
                        raise UnknownExecution("standalone deliberation parent is not durably quiescent")
                    binding = _restore_binding(parent)
                    if (
                        parent["id"] != step["parent_operation_id"]
                        or binding.task_id != task_id
                        or binding.attempt_id != AttemptId(step["parent_attempt_id"])
                        or binding.input_revision != step["input_revision"]
                        or binding.scope != ScopeId(str(step["owner_scope"]))
                    ):
                        raise UnknownExecution("standalone deliberation parent binding changed")
                    candidate_id = (step.get("context") or {}).get(
                        "candidate_conclusion_id", step["parent_conclusion_id"],
                    )
                    accepted = await uow.knowledge.accepted_turn_result_for_conclusion(str(candidate_id))
                    conclusion = await uow.knowledge.get_conclusion(str(candidate_id))
                    candidate_operation = operations.get(str(accepted["operation_id"])) if accepted else None
                    if (
                        accepted is None or conclusion is None or candidate_operation is None
                        or accepted["task_id"] != task_id
                        or accepted["input_revision"] != step["input_revision"]
                        or accepted["attempt_id"] != step["parent_attempt_id"]
                        or accepted["registry_id"] != step["registry_id"]
                        or conclusion["task_id"] != task_id
                        or conclusion["input_revision"] != step["input_revision"]
                        or candidate_operation["state"] != "COMPLETED"
                        or candidate_operation["execution_state"] != "QUIESCENT"
                    ):
                        raise UnknownExecution("standalone deliberation parent result is not durably connected")
                    candidate_binding = _restore_binding(candidate_operation)
                    if (
                        candidate_binding.task_id != task_id
                        or candidate_binding.attempt_id != AttemptId(accepted["attempt_id"])
                        or candidate_binding.input_revision != step["input_revision"]
                        or candidate_binding.agent_registry_id != RegistryId(accepted["registry_id"])
                    ):
                        raise UnknownExecution("standalone deliberation result binding changed")
                    registry = await uow.agents.lock_registry(RegistryId(accepted["registry_id"]))
                    if (
                        registry.kind != "hekate" or registry.persistence != "persistent"
                        or registry.owner_scope != task.scope
                        or registry.provider_id != candidate_binding.provider_agent_id
                    ):
                        raise UnknownExecution("standalone deliberation parent is not its persistent HEKATE")

                if task.cancel_requested_at is not None or task.status == TaskStatus.CANCELLED:
                    task_reason = "task_cancelled"
                elif task.deadline <= now or task.stop_reason == StopReason.DEADLINE.value:
                    task_reason = "task_deadline"
                else:
                    task_reason = "task_terminal"
                reason_groups: dict[str, tuple[Mapping[str, object], ...]] = {
                    "revision_superseded": tuple(row for row in eligible if row["input_revision"] < task.input_revision),
                    task_reason: tuple(row for row in eligible if row["input_revision"] == task.input_revision),
                }
                for reason, group in reason_groups.items():
                    if not group:
                        continue
                    prior_states = {str(row["id"]): row["state"] for row in group}
                    operation_ids_to_release = await _verify_and_seal_standalone_deliberation_steps(
                        uow, task_id, operations, reason, step_rows=group,
                    )
                    for operation_id in operation_ids_to_release:
                        await uow.budgets.release_unallocated_after_quiescence(operation_id)
                    for step in group:
                        if prior_states[str(step["id"])] != "STOPPED" and step["state"] != "COMPLETE":
                            await uow.delivery.append_audit({
                                "owner_scope": str(task.scope), "task_id": str(task_id),
                                "attempt_id": str(step["parent_attempt_id"]),
                                "operation_id": str(step["operation_id"]),
                                "registry_id": str(step["registry_id"]),
                                "event_kind": "deliberation.step_maintenance_stopped",
                                "safe_payload": {
                                    "step_id": str(step["id"]), "reason": reason,
                                    "step_revision": step["input_revision"],
                                    "task_revision": task.input_revision,
                                },
                            })
                        completed += int(await uow.deliberation.mark_maintenance_complete(step))
                await uow.commit()
        except (UnknownExecution, StaleInput) as error:
            _LOG.info("standalone deliberation cleanup remains pending for %s: %s", task_id, str(error)[:200])
            if retry_steps:
                retry_after = datetime.now(UTC) + timedelta(seconds=_DELIBERATION_MAINTENANCE_RETRY_SECONDS)
                try:
                    async with factory() as uow:
                        for step in retry_steps:
                            await uow.deliberation.defer_maintenance_retry(step, retry_after)
                        await uow.commit()
                except Exception as retry_error:
                    _LOG.warning(
                        "could not defer standalone cleanup retry for %s: %s: %s",
                        task_id, type(retry_error).__name__, str(retry_error)[:200],
                    )
    return completed


async def reject_deliberation_step(
    factory: UowFactory,
    selected: Mapping[str, object],
    *,
    failure_code: str,
    stop_reason: StopReason = StopReason.POLICY,
) -> str:
    """Converge a permanently unexecutable READY step after rechecking durable state."""
    if failure_code not in {*_DELIBERATION_POLICY_TEXT, "budget_unavailable", "round_limit", "no_new_work"}:
        raise ValueError("unsupported deliberation stop code")
    if stop_reason not in {StopReason.POLICY, StopReason.BUDGET, StopReason.ROUND_LIMIT, StopReason.NO_NEW_WORK}:
        raise ValueError("unsupported deliberation stop reason")

    task_id = TaskId(str(selected["task_id"]))
    step_id = str(selected["id"])
    async with factory() as uow:
        workflow_hint = await uow.critic_workflows.get(task_id)
        if workflow_hint is not None:
            operations = await _lock_workflow_operations(uow, workflow_hint)
        else:
            step_hints = await uow.deliberation.list_for_task(task_id)
            operation_ids = sorted({
                *(str(row["operation_id"]) for row in step_hints),
                *(str(row["parent_operation_id"]) for row in step_hints),
            })
            operations = {
                value: await uow.delivery.lock_operation(OperationId(value))
                for value in operation_ids
            }
        authorization = await uow.tasks.lock_scope(ScopeId(str(selected["owner_scope"])))
        task = await uow.tasks.lock_task(task_id)
        workflow = await uow.critic_workflows.get(task_id, lock=True)
        step = await uow.deliberation.get(step_id, lock=True)
        if step is None or step["state"] != "READY":
            await uow.commit()
            return "step_changed"
        if (
            step["task_id"] != task_id or step["owner_scope"] != task.scope
            or step["input_revision"] != selected["input_revision"]
            or step["operation_id"] != selected["operation_id"]
            or step["parent_operation_id"] != selected["parent_operation_id"]
        ):
            await uow.commit()
            return "step_identity_changed"
        current_steps = await uow.deliberation.list_for_task(task_id, lock=True)
        if any(str(row["operation_id"]) not in operations for row in current_steps):
            raise UnknownExecution("deliberation steps changed while stop convergence locked operations")

        parent_operation = operations.get(str(step["parent_operation_id"]))
        if parent_operation is None:
            raise UnknownExecution("deliberation parent operation is unavailable")
        parent_binding = _restore_binding(parent_operation)
        if (
            parent_binding.task_id != task_id
            or parent_binding.attempt_id != AttemptId(step["parent_attempt_id"])
            or parent_binding.input_revision != step["input_revision"]
            or parent_binding.scope != ScopeId(str(step["owner_scope"]))
            or parent_operation["id"] != step["parent_operation_id"]
            or parent_operation["state"] != "COMPLETED"
            or parent_operation["execution_state"] != "QUIESCENT"
        ):
            raise UnknownExecution("deliberation parent result is not safely bound and quiescent")

        # A revision that superseded this step is cleanup-only. Never fail or answer for
        # the new revision, but still prove every old operation is safe before releasing holds.
        if task.scope != step["owner_scope"]:
            await uow.commit()
            return "scope_changed"
        if task.input_revision != step["input_revision"]:
            if workflow is not None:
                if workflow.stage not in {"FAILED", "DELETE_PENDING", "COMPLETE"}:
                    await uow.critic_workflows.transition(task_id, (workflow.stage,), "FAILED")
                    workflow = workflow.model_copy(update={"stage": "FAILED"})
                if workflow.stage == "FAILED":
                    await _queue_retirement_in_uow(uow, workflow)
            else:
                operation_ids = await _verify_and_seal_standalone_deliberation_steps(
                    uow, task_id, operations, "revision_superseded",
                )
                for operation_id in operation_ids:
                    await uow.budgets.release_unallocated_after_quiescence(operation_id)
            await uow.delivery.append_audit({
                "owner_scope": str(step["owner_scope"]), "task_id": str(task_id),
                "operation_id": str(step["operation_id"]), "registry_id": str(step["registry_id"]),
                "event_kind": "deliberation.step_superseded",
                "safe_payload": {"workflow_revision": step["input_revision"], "current_revision": task.input_revision},
            })
            await uow.commit()
            return "superseded_cleanup_queued"

        if task.cancel_requested_at is not None or task.deadline <= datetime.now(UTC):
            await uow.commit()
            return "task_stop_pending"
        if task.status != TaskStatus.WAITING:
            await uow.commit()
            return "task_not_waiting"
        if await uow.tasks.get_task_response(task_id) is not None:
            await uow.commit()
            return "response_already_final"

        if (
            authorization.principal_id != parent_binding.principal_id
            or authorization.policy_version != parent_binding.policy_version
            or authorization.authz_epoch != parent_binding.authz_epoch
        ):
            failure_code = "authorization_changed"
            stop_reason = StopReason.POLICY
        elif failure_code == "authorization_changed":
            # The DB still matches the trusted parent binding; only this worker actor is stale.
            await uow.commit()
            return "worker_identity_stale"

        if failure_code != "authorization_changed" and task.evidence_refs:
            accessible = await uow.knowledge.lock_accessible_references(
                step["owner_scope"], task.evidence_refs, authorization.authz_epoch,
            )
            if len(accessible) != len(set(task.evidence_refs)):
                failure_code = "evidence_unavailable"
                stop_reason = StopReason.POLICY

        candidate_id = (step.get("context") or {}).get(
            "candidate_conclusion_id", step["parent_conclusion_id"],
        )
        candidate = await uow.knowledge.get_conclusion(str(candidate_id))
        accepted = await uow.knowledge.accepted_turn_result_for_conclusion(str(candidate_id))
        if candidate is None or accepted is None or (
            accepted["task_id"] != task_id or accepted["input_revision"] != step["input_revision"]
            or accepted["registry_id"] is None
        ):
            raise UnknownExecution("accepted HEKATE candidate is unavailable for stop response")
        candidate_operation = operations.get(str(accepted["operation_id"]))
        if candidate_operation is None:
            raise UnknownExecution("candidate operation was not included in the locked operation set")
        candidate_binding = _restore_binding(candidate_operation)
        candidate_agent = await uow.agents.lock_registry(RegistryId(accepted["registry_id"]))
        if (
            candidate["task_id"] != task_id or candidate["input_revision"] != step["input_revision"]
            or candidate_operation["id"] != accepted["operation_id"]
            or candidate_operation["state"] != "COMPLETED"
            or candidate_operation["execution_state"] != "QUIESCENT"
            or candidate_binding.task_id != task_id
            or candidate_binding.attempt_id != AttemptId(accepted["attempt_id"])
            or candidate_binding.input_revision != step["input_revision"]
            or candidate_binding.agent_registry_id != RegistryId(accepted["registry_id"])
            or candidate_agent.kind != "hekate" or candidate_agent.persistence != "persistent"
            or candidate_agent.owner_scope != task.scope
            or candidate_agent.provider_id != candidate_binding.provider_agent_id
        ):
            raise UnknownExecution("stop response candidate is not an accepted persistent HEKATE result")

        if stop_reason == StopReason.POLICY:
            response_text = _DELIBERATION_POLICY_TEXT.get(
                failure_code, "The task could not continue under the current policy.",
            )
            outcome = "FAILED"
            successful = False
        else:
            capsule = candidate["capsule"]
            assessment = capsule.get("assessment", {}) if isinstance(capsule, Mapping) else {}
            statement = assessment.get("statement", "") if isinstance(assessment, Mapping) else ""
            response_text = (
                f"Provisional assessment: {statement} Further work was not scheduled ({stop_reason.value})."
            ).strip()
            outcome = "PROVISIONAL_ANSWER"
            successful = True
        response = {
            "operation_id": accepted["operation_id"], "attempt_id": accepted["attempt_id"],
            "registry_id": accepted["registry_id"], "source_inbox_id": accepted["inbox_id"],
            "proposal": {"action": "abstain", "server_generated": True, "failure_code": failure_code},
            "response_text": response_text, "outcome": outcome, "stop_reason": stop_reason.value,
        }

        if workflow is not None:
            # The existing retirement path verifies execution and call quiescence,
            # seals deferred work, and releases only unallocated holds.
            if workflow.stage not in {"FAILED", "DELETE_PENDING", "COMPLETE"}:
                await uow.critic_workflows.transition(task_id, (workflow.stage,), "FAILED")
                workflow = workflow.model_copy(update={"stage": "FAILED"})
        else:
            step_operation_ids = await _verify_and_seal_standalone_deliberation_steps(
                uow, task_id, operations, failure_code,
            )

        if not await uow.tasks.resolve_waiting_task_with_response(
            task_id, step["input_revision"], response, successful=successful,
        ):
            # The step/workflow and deferred-operation mutations above are only
            # valid together with the Task response. A failed terminal CAS must
            # discard the entire effect transaction.
            await uow.rollback()
            return "task_changed"

        if workflow is not None:
            if workflow.stage == "FAILED":
                await _queue_retirement_in_uow(uow, workflow)
        else:
            for operation_id in step_operation_ids:
                await uow.budgets.release_unallocated_after_quiescence(operation_id)
        await uow.delivery.append_audit({
            "owner_scope": str(task.scope), "task_id": str(task_id),
            "attempt_id": str(step["parent_attempt_id"]), "operation_id": str(step["operation_id"]),
            "registry_id": str(step["registry_id"]),
            "event_kind": "deliberation.step_stopped",
            "safe_payload": {
                "failure_code": failure_code, "stop_reason": stop_reason.value,
                "step_id": step_id, "input_revision": step["input_revision"],
            },
        })
        await uow.commit()
        return "failed_policy" if stop_reason == StopReason.POLICY else "completed_provisional"


async def request_critic(
    uow: UnitOfWork,
    actor: ActorContext,
    proposal: SpawnProposal,
    planning_operation: Mapping[str, object],
    planning_attempt: Attempt,
    planning_conclusion_id: DomainId,
    hekate_config: TaskExecutionConfig,
    critic_config: TaskExecutionConfig | None,
) -> CriticWorkflow:
    """Approve one concrete planning spawn and durably reserve its child steps."""
    task = await uow.tasks.lock_task(proposal.task_id)
    workflow_hash = canonical_json_hash({
        "scope": str(task.scope), "task_id": str(task.id), "revision": task.input_revision,
        "proposal": proposal, "planning_operation_id": str(planning_attempt.operation_id),
        "planning_conclusion_id": str(planning_conclusion_id),
    })
    if proposal.task_id != task.id or any(not value.strip() for value in (
        proposal.purpose, proposal.target_uncertainty, proposal.expected_decision_impact,
    )):
        raise PolicyDenied("Critic spawn requires a concrete purpose and decision impact")
    authorization = await uow.tasks.lock_scope(actor.scope)
    if (authorization.principal_id, authorization.policy_version, authorization.authz_epoch) != (
        actor.principal_id, actor.policy_version, actor.authz_epoch,
    ):
        raise PolicyDenied("authorization snapshot changed")
    if (
        task.scope != actor.scope or actor.task_id != task.id
        or actor.attempt_id != planning_attempt.id or actor.input_revision != task.input_revision
        or planning_attempt.task_id != task.id
        or planning_operation.get("id") != planning_attempt.operation_id
    ):
        raise PolicyDenied("spawn request does not match its Task and planning binding")
    hek = await uow.agents.lock_registry(planning_attempt.agent_registry_id)
    if (
        hek.kind != "hekate" or hek.persistence != "persistent"
        or hek.owner_scope != task.scope or hek.provider_id is None
        or hek.policy_version != actor.policy_version
        or actor.authenticated_agent_registry_id != hek.registry_id
    ):
        raise PolicyDenied("spawn was not proposed by the authorized persistent HEKATE")
    existing = await uow.critic_workflows.get(task.id, lock=True)
    if existing is not None:
        if existing.spawn_request_hash != workflow_hash:
            raise Conflict("Task already has a different Critic spawn request")
        return existing
    if critic_config is None:
        raise PolicyDenied("Critic profile is disabled or unavailable")
    if task.counters.critic_agents >= 1 or task.counters.review_rounds >= 1:
        raise PolicyDenied("Task Critic or review limit is already used")
    if (
        task.input_revision != planning_attempt.input_revision
        or task.status != TaskStatus.RUNNING or task.cancel_requested_at is not None
        or task.deadline <= datetime.now(UTC) or planning_attempt.kind != "planning"
        or planning_attempt.status.value != "SUCCEEDED"
        or planning_operation.get("state") != "COMPLETED"
        or planning_operation.get("execution_state") != "QUIESCENT"
    ):
        raise PolicyDenied("planning result is not eligible for Critic spawn")

    critic_registry = RegistryId(str(uuid5(NAMESPACE_URL, f"hekate:critic-registry:{task.id}")))
    create_operation = _operation_id("critic-create", task.id)
    review_attempt = AttemptId(str(uuid5(NAMESPACE_URL, f"hekate:critic-attempt:{task.id}:1")))
    review_operation = _operation_id("critic-review", task.id)
    review_reservation = ReservationId(str(uuid5(NAMESPACE_URL, f"hekate:critic-reservation:{task.id}:1")))
    synthesis_attempt = AttemptId(str(uuid5(NAMESPACE_URL, f"hekate:synthesis-attempt:{task.id}:1")))
    synthesis_operation = _operation_id("hekate:synthesis", task.id)
    synthesis_reservation = ReservationId(str(uuid5(NAMESPACE_URL, f"hekate:synthesis-reservation:{task.id}:1")))
    workflow = CriticWorkflow(
        task_id=task.id, owner_scope=task.scope, input_revision=task.input_revision,
        stage="CREATE_PENDING", spawn_request_hash=workflow_hash, proposal=proposal,
        critic_profile=execution_config_snapshot(critic_config),
        hekate_profile=execution_config_snapshot(hekate_config),
        parent_attempt_id=planning_attempt.id, planning_operation_id=planning_attempt.operation_id,
        planning_conclusion_id=planning_conclusion_id, critic_registry_id=critic_registry,
        create_operation_id=create_operation, review_attempt_id=review_attempt,
        review_operation_id=review_operation, review_reservation_id=review_reservation,
        synthesis_attempt_id=synthesis_attempt, synthesis_operation_id=synthesis_operation,
        synthesis_reservation_id=synthesis_reservation,
    )

    await uow.delivery.claim_lifecycle_operation(
        create_operation, task.scope, "critic.create",
        canonical_json_hash({"owner": str(task.scope), "creation_tag": str(create_operation), "role": "critic"}),
    )
    await uow.delivery.claim_deferred_operation(
        review_operation, task.scope, task.id, "critic.review", workflow.stage_hash("critic_review"),
    )
    await uow.delivery.claim_deferred_operation(
        synthesis_operation, task.scope, task.id, "hekate.synthesis", workflow.stage_hash("synthesis"),
    )
    period = (task.created_at or datetime.now(UTC)).astimezone(UTC).strftime("%Y-%m-%d")
    task_account = f"task-budget:{task.id}"
    system_account = f"system-budget:{period}"
    await uow.budgets.reserve_operation(ReservationRequest(
        id=review_reservation, operation_id=review_operation, purpose="operation_envelope",
        amount=_reservation_amount(critic_config), task_id=task.id,
        task_account_id=task_account, system_account_id=system_account,
        pricing_version=critic_config.pricing_version, system_period_id=period,
    ))
    await uow.budgets.reserve_operation(ReservationRequest(
        id=synthesis_reservation, operation_id=synthesis_operation, purpose="operation_envelope",
        amount=_reservation_amount(hekate_config), task_id=task.id,
        task_account_id=task_account, system_account_id=system_account,
        pricing_version=hekate_config.pricing_version, system_period_id=period,
    ))
    await uow.agents.insert_intent(AgentRecord(
        registry_id=critic_registry, owner_scope=task.scope, kind="critic", task_id=task.id,
        persistence="ephemeral", creation_operation_id=create_operation,
        intended_state="CREATING", observation="UNKNOWN", policy_version=actor.policy_version,
    ))
    await uow.critic_workflows.insert(workflow)
    job_id = str(uuid5(NAMESPACE_URL, f"hekate:outbox:{create_operation}:critic_create:0"))
    await uow.delivery.append_outbox(OutboxJob(
        id=job_id, operation_id=create_operation, kind="critic_create", generation=0,
        payload={"task_id": str(task.id), "registry_id": str(critic_registry)}, status="PENDING",
    ))
    if not await uow.tasks.mark_waiting_for_workflow(task.id, task.input_revision):
        raise PolicyDenied("Task changed before Critic workflow approval")
    await uow.delivery.append_audit({
        "owner_scope": str(task.scope), "task_id": str(task.id),
        "attempt_id": str(planning_attempt.id), "operation_id": str(planning_attempt.operation_id),
        "registry_id": str(hek.registry_id), "event_kind": "critic.spawn_approved",
        "safe_payload": {
            "request_hash": workflow_hash, "critic_registry_id": str(critic_registry),
            "review_operation_id": str(review_operation), "synthesis_operation_id": str(synthesis_operation),
            "review_hold": str(_reservation_amount(critic_config)),
            "synthesis_hold": str(_reservation_amount(hekate_config)),
        },
    })
    return workflow


async def _queue_retirement_in_uow(uow: UnitOfWork, workflow: CriticWorkflow) -> RetirementReceipt:
    operations = await _lock_workflow_operations(uow, workflow)
    task = await uow.tasks.lock_task(workflow.task_id)
    if task.scope != workflow.owner_scope:
        raise PolicyDenied("Critic workflow is outside its durable owner scope")
    superseded = task.input_revision != workflow.input_revision
    if task.status not in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED} and not superseded:
        raise PolicyDenied("Critic retirement waits for a terminal or superseded workflow")
    creation = operations[str(workflow.create_operation_id)]
    if creation.get("state") != "COMPLETED" or creation.get("execution_state") != "QUIESCENT":
        raise UnknownExecution("Critic creation must be reconciled before retirement")
    registry = await uow.agents.lock_registry(workflow.critic_registry_id)
    if (
        registry.kind != "critic" or registry.persistence != "ephemeral"
        or registry.task_id != task.id or registry.owner_scope != workflow.owner_scope
        or registry.creation_operation_id != workflow.create_operation_id
    ):
        raise PolicyDenied("only this Task's ephemeral Critic may be retired")
    if (
        registry.intended_state == "DELETED" and registry.provider_id is None
        and (creation.get("observation") or {}).get("cancelled_before_create") is True
    ):
        await _verify_and_seal_workflow_steps(uow, workflow, operations, "critic_cancelled_before_create")
        await uow.budgets.release_unallocated_after_quiescence(workflow.review_operation_id)
        await uow.budgets.release_unallocated_after_quiescence(workflow.synthesis_operation_id)
        for step in await uow.deliberation.list_for_task(workflow.task_id):
            await uow.budgets.release_unallocated_after_quiescence(OperationId(step["operation_id"]))
        await uow.critic_workflows.transition(
            task.id, (workflow.stage,), "COMPLETE",
        )
        return {"registry_id": str(registry.registry_id), "operation_id": str(workflow.create_operation_id), "state": "COMPLETE", "replayed": False}
    active = await uow.agents.active_execution_hold(registry.registry_id, lock=True)
    if active is not None:
        raise UnknownExecution("Critic execution is not quiescent")
    await _verify_and_seal_workflow_steps(uow, workflow, operations, "critic_retirement")
    if workflow.stage == "DELETE_PENDING" and workflow.delete_operation_id:
        return {"registry_id": str(registry.registry_id), "operation_id": str(workflow.delete_operation_id), "state": "DELETE_PENDING", "replayed": True}
    await uow.budgets.release_unallocated_after_quiescence(workflow.review_operation_id)
    await uow.budgets.release_unallocated_after_quiescence(workflow.synthesis_operation_id)
    for step in await uow.deliberation.list_for_task(workflow.task_id):
        await uow.budgets.release_unallocated_after_quiescence(OperationId(step["operation_id"]))
    delete_operation = _operation_id("critic-delete", task.id)
    await uow.delivery.claim_lifecycle_operation(
        delete_operation, workflow.owner_scope, "critic.delete",
        canonical_json_hash({"registry_id": str(registry.registry_id), "creation_operation_id": str(registry.creation_operation_id)}),
    )
    await uow.agents.set_registry_deleting(registry.registry_id)
    await uow.critic_workflows.transition(
        task.id, ("CREATE_PENDING", "CREATE_UNKNOWN", "CRITIC_READY", "REVIEW_ADMITTED", "SYNTHESIS_PENDING", "SYNTHESIS_ADMITTED", "FAILED"), "DELETE_PENDING",
        delete_operation_id=delete_operation,
    )
    job_id = str(uuid5(NAMESPACE_URL, f"hekate:outbox:{delete_operation}:critic_delete:0"))
    await uow.delivery.append_outbox(OutboxJob(
        id=job_id, operation_id=delete_operation, kind="critic_delete", generation=0,
        payload={"task_id": str(task.id), "registry_id": str(registry.registry_id)}, status="PENDING",
    ))
    return {"registry_id": str(registry.registry_id), "operation_id": str(delete_operation), "state": "DELETE_PENDING", "replayed": False}


async def maintain_critic_workflows(factory: UowFactory, *, limit: int = 100) -> int:
    """Retire Critics after Task termination or safely superseded input revisions."""
    stages = (
        "CRITIC_READY", "REVIEW_ADMITTED", "SYNTHESIS_PENDING", "SYNTHESIS_ADMITTED", "FAILED",
    )
    async with factory() as uow:
        workflows = await uow.critic_workflows.list_stages(stages, limit)
        await uow.commit()
    queued = 0
    for selected in workflows:
        async with factory() as uow:
            operations = await _lock_workflow_operations(uow, selected)
            task = await uow.tasks.lock_task(selected.task_id)
            workflow = await uow.critic_workflows.get(selected.task_id, lock=True)
            if workflow is None or workflow.stage not in stages:
                await uow.commit()
                continue
            if (
                workflow.planning_operation_id != selected.planning_operation_id
                or workflow.create_operation_id != selected.create_operation_id
                or workflow.review_operation_id != selected.review_operation_id
                or workflow.synthesis_operation_id != selected.synthesis_operation_id
            ):
                await uow.commit()
                continue
            terminal = task.status in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}
            superseded = task.input_revision != workflow.input_revision
            if not terminal and not superseded:
                stop_requested = task.cancel_requested_at is not None or task.deadline <= datetime.now(UTC)
                if stop_requested:
                    try:
                        await _verify_and_seal_workflow_steps(
                            uow, workflow, operations,
                            task.stop_reason or (StopReason.DEADLINE.value if task.deadline <= datetime.now(UTC) else StopReason.USER_CANCELLED.value),
                        )
                    except UnknownExecution:
                        # An admitted/running or UNKNOWN child remains authoritative
                        # until its ordinary execution observer proves quiescence.
                        await uow.commit()
                        continue
                    await uow.commit()
                    continue
                await uow.commit()
                continue
            if superseded and workflow.stage != "FAILED":
                await uow.critic_workflows.transition(task.id, (workflow.stage,), "FAILED")
                await uow.delivery.append_audit({
                    "owner_scope": str(workflow.owner_scope), "task_id": str(task.id),
                    "operation_id": str(workflow.planning_operation_id),
                    "registry_id": str(workflow.critic_registry_id),
                    "event_kind": "critic.workflow_superseded",
                    "safe_payload": {
                        "workflow_revision": workflow.input_revision,
                        "current_revision": task.input_revision,
                    },
                })
                await uow.commit()
                continue
            if workflow.stage == "FAILED" and not terminal and not superseded:
                await uow.commit()
                try:
                    outcome = await reject_critic_workflow_step(
                        factory, workflow, "FAILED", WorkflowStopCode.POLICY_REJECTED,
                    )
                except (PolicyDenied, UnknownExecution, StaleInput):
                    continue
                if outcome == "failed_policy":
                    queued += 1
                continue
            try:
                await _queue_retirement_in_uow(uow, workflow)
            except (PolicyDenied, UnknownExecution):
                await uow.commit()
                continue
            await uow.commit()
            queued += 1
    return queued


async def retire(factory: UowFactory, registry_id: RegistryId) -> RetirementReceipt:
    async with factory() as uow:
        registry = await uow.agents.get_registry(registry_id)
        if registry is None:
            raise StaleInput("Critic registry is unavailable")
        if registry.kind != "critic" or registry.persistence != "ephemeral" or registry.task_id is None:
            raise PolicyDenied("persistent or unbound agents cannot be retired")
        workflow = await uow.critic_workflows.get(registry.task_id)
        if workflow is None or workflow.critic_registry_id != registry_id:
            raise PolicyDenied("Critic has no matching durable workflow")
        await _lock_workflow_operations(uow, workflow)
        await uow.tasks.lock_task(registry.task_id)
        workflow = await uow.critic_workflows.get(registry.task_id, lock=True)
        current_registry = await uow.agents.lock_registry(registry_id)
        if workflow is None or current_registry.task_id != registry.task_id or workflow.critic_registry_id != registry_id:
            raise StaleInput("Critic binding changed before retirement")
        receipt = await _queue_retirement_in_uow(uow, workflow)
        await uow.commit()
        return receipt


async def create_from_intent(
    factory: UowFactory, runtime: AgentRuntime, job: OutboxJob, worker: str,
) -> CreateObservation:
    operation_id = job.operation_id
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(operation_id)
        if job.claim_fence is None:
            raise Conflict("Critic create outbox claim is incomplete")
        await uow.delivery.assert_job_claim(job, worker, job.claim_fence)
        if operation["kind"] != "critic.create":
            raise PolicyDenied("operation is not a Critic creation intent")
        workflow = await uow.critic_workflows.get_by_create_operation(operation_id, lock=True)
        if workflow is None:
            raise StaleInput("Critic create intent has no workflow")
        authorization = await uow.tasks.lock_scope(workflow.owner_scope)
        planning_operation = await uow.delivery.lock_operation(workflow.planning_operation_id)
        planning_binding = _restore_binding(planning_operation)
        task = await uow.tasks.lock_task(workflow.task_id)
        registry = await uow.agents.lock_registry(workflow.critic_registry_id)
        if task.scope != workflow.owner_scope or registry.kind != "critic" or registry.persistence != "ephemeral" or registry.task_id != task.id:
            raise PolicyDenied("Critic creation binding changed")
        already_started = bool((operation.get("observation") or {}).get("external_call_started"))
        still_authorized = (
            registry.policy_version == authorization.policy_version
            and planning_binding.scope == workflow.owner_scope
            and planning_binding.principal_id == authorization.principal_id
            and planning_binding.policy_version == authorization.policy_version
            and planning_binding.authz_epoch == authorization.authz_epoch
            and planning_binding.agent_registry_id != registry.registry_id
        )
        may_create = (
            workflow.stage in {"CREATE_PENDING", "CREATE_UNKNOWN"}
            and task.input_revision == workflow.input_revision
            and task.status == TaskStatus.WAITING and task.cancel_requested_at is None
            and task.deadline > datetime.now(UTC) and still_authorized
        )
        if not already_started and not may_create:
            await uow.delivery.complete_lifecycle_operation(operation_id, {
                "present": False, "cancelled_before_create": True,
            })
            await uow.agents.mark_registry_deleted(workflow.critic_registry_id)
            next_stage = "COMPLETE" if task.status in {
                TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED,
            } else "FAILED"
            await uow.critic_workflows.transition(
                workflow.task_id, (workflow.stage,), next_stage,
            )
            await uow.budgets.release_unallocated_after_quiescence(workflow.review_operation_id)
            await uow.budgets.release_unallocated_after_quiescence(workflow.synthesis_operation_id)
            for step in await uow.deliberation.list_for_task(workflow.task_id):
                await uow.budgets.release_unallocated_after_quiescence(OperationId(step["operation_id"]))
            if job.claim_fence is None:
                raise Conflict("Critic create outbox claim is incomplete")
            await uow.delivery.ack_job(job, worker, job.claim_fence)
            await uow.delivery.append_audit({
                "owner_scope": str(workflow.owner_scope), "task_id": str(workflow.task_id),
                "operation_id": str(operation_id), "registry_id": str(workflow.critic_registry_id),
                "event_kind": "critic.create_cancelled_before_call",
                "safe_payload": {"task_revision": task.input_revision, "workflow_revision": workflow.input_revision},
            })
            await uow.commit()
            return {"provider_agent_id": None, "owner": str(workflow.owner_scope),
                    "creation_tag": str(workflow.create_operation_id), "present": False}
        if not already_started:
            await uow.delivery.mark_lifecycle_started(operation_id, "critic.create")
        await uow.commit()

    # The profile was fixed and persisted in the spawn-approval transaction. A
    # restart must not silently route this already-approved Critic via new config.
    critic_config = restore_execution_config(workflow.critic_profile)

    owner = DeploymentId(str(workflow.owner_scope))
    creation_tag = str(workflow.create_operation_id)
    try:
        if already_started:
            candidates = await runtime.list_owned_agents(owner, workflow.create_operation_id)
            candidates = tuple(candidate for candidate in candidates if (
                _field(candidate, "owner") == str(owner)
                and _field(candidate, "creation_tag") == creation_tag
                and _field(candidate, "role") == "critic"
            ))
            if len(candidates) != 1:
                raise UnknownExecution("Critic creation outcome is ambiguous; no duplicate create was sent")
            observation = {
                "provider_agent_id": str(_field(candidates[0], "provider_agent_id")),
                "owner": str(owner), "creation_tag": creation_tag, "present": True,
            }
        else:
            created = await runtime.create_agent({
                "owner": str(owner), "creation_tag": creation_tag, "role": "critic",
                "persistence": "ephemeral", "registry_id": str(workflow.critic_registry_id),
                "model": critic_config.letta_model,
                "max_input_tokens": critic_config.max_input_tokens,
                "max_output_tokens": critic_config.max_output_tokens,
            }, workflow.create_operation_id)
            observation = {
                "provider_agent_id": str(_field(created, "provider_agent_id")),
                "owner": str(_field(created, "owner")),
                "creation_tag": str(_field(created, "creation_tag")),
                "present": bool(_field(created, "present")),
            }
            if observation["owner"] != str(owner) or observation["creation_tag"] != creation_tag or not observation["present"]:
                raise Conflict("Letta create response did not confirm the Critic owner and creation tag")
    except PolicyDenied:
        raise
    except Exception as error:
        async with factory() as uow:
            await uow.delivery.mark_lifecycle_unknown(operation_id, type(error).__name__)
            workflow = await uow.critic_workflows.get(workflow.task_id, lock=True)
            if workflow is not None and workflow.stage == "CREATE_PENDING":
                await uow.critic_workflows.transition(workflow.task_id, ("CREATE_PENDING",), "CREATE_UNKNOWN")
            await uow.commit()
        raise UnknownExecution("Critic create outcome requires owner-tag reconciliation") from error

    async with factory() as uow:
        current = await uow.critic_workflows.get(workflow.task_id, lock=True)
        task = await uow.tasks.lock_task(workflow.task_id)
        registry = await uow.agents.lock_registry(workflow.critic_registry_id)
        if current is None or current.create_operation_id != operation_id or registry.owner_scope != workflow.owner_scope:
            raise StaleInput("Critic creation result lost its workflow fence")
        provider_id = ProviderAgentId(str(_field(observation, "provider_agent_id")))
        await uow.agents.bind_provider(workflow.critic_registry_id, provider_id)
        await uow.delivery.complete_lifecycle_operation(operation_id, {
            "provider_agent_id": str(provider_id), "owner": str(owner),
            "creation_tag": creation_tag, "present": True, "external_call_started": True,
        })
        if current.stage in {"CREATE_PENDING", "CREATE_UNKNOWN"}:
            await uow.critic_workflows.transition(workflow.task_id, (current.stage,), "CRITIC_READY")
            current = current.model_copy(update={"stage": "CRITIC_READY"})
        if job.claim_fence is None:
            raise Conflict("Critic create outbox claim is incomplete")
        await uow.delivery.ack_job(job, worker, job.claim_fence)
        await uow.commit()
        return observation


async def confirm_deletion(factory: UowFactory, runtime: AgentRuntime, job: OutboxJob, worker: str) -> DeleteObservation:
    registry_id = RegistryId(str(job.payload["registry_id"]))
    task_id = TaskId(str(job.payload["task_id"]))
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(job.operation_id)
        if job.claim_fence is None:
            raise Conflict("Critic delete outbox claim is incomplete")
        await uow.delivery.assert_job_claim(job, worker, job.claim_fence)
        workflow = await uow.critic_workflows.get(task_id, lock=True)
        task = await uow.tasks.lock_task(task_id)
        registry = await uow.agents.lock_registry(registry_id)
        if (
            operation["kind"] != "critic.delete" or workflow is None
            or workflow.stage != "DELETE_PENDING" or workflow.critic_registry_id != registry_id
            or workflow.delete_operation_id != job.operation_id
            or registry.kind != "critic" or registry.persistence != "ephemeral"
            or registry.owner_scope != workflow.owner_scope or registry.task_id != task_id
            or (
                task.status not in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}
                and task.input_revision == workflow.input_revision
            )
        ):
            raise PolicyDenied("Critic deletion is not authorized by a terminal workflow")
        if await uow.agents.active_execution_hold(registry_id, lock=True) is not None:
            raise UnknownExecution("Critic still has an unresolved execution")
        review = await uow.delivery.lock_operation(workflow.review_operation_id)
        if review.get("execution_state") != "QUIESCENT":
            raise UnknownExecution("Critic review execution is not confirmed quiescent")
        provider_id = registry.provider_id
        await uow.commit()

    try:
        owned = await runtime.list_owned_agents(DeploymentId(str(workflow.owner_scope)), workflow.create_operation_id)
        exact = tuple(candidate for candidate in owned if (
            _field(candidate, "owner") == str(workflow.owner_scope)
            and _field(candidate, "creation_tag") == str(workflow.create_operation_id)
            and _field(candidate, "role") == "critic"
        ))
        if len(exact) > 1:
            raise UnknownExecution("multiple Critic agents match the durable creation tag")
        if exact:
            exact_id = ProviderAgentId(str(_field(exact[0], "provider_agent_id")))
            if provider_id is not None and exact_id != provider_id:
                raise Conflict("owned Critic provider ID differs from registry binding")
            provider_id = exact_id
        if provider_id is None and not exact:
            observation = {"provider_agent_id": None, "present": False, "observed_at": datetime.now(UTC).isoformat()}
        else:
            if provider_id is None:
                raise UnknownExecution("Critic provider binding is unresolved")
            if not exact:
                observed = await runtime.observe_agent(provider_id)
                if observed.state.value == "ABSENT":
                    observation = {"provider_agent_id": str(provider_id), "present": False, "observed_at": observed.observed_at.isoformat()}
                elif (
                    observed.owner != str(workflow.owner_scope)
                    or observed.creation_tag != str(workflow.create_operation_id)
                    or observed.role != "critic"
                ):
                    raise UnknownExecution("provider Critic ownership tags are not confirmed")
                else:
                    deletion = await runtime.delete_agent(provider_id, job.operation_id)
                    observation = dict(deletion)
            else:
                deletion = await runtime.delete_agent(provider_id, job.operation_id)
                observation = dict(deletion)
            if _field(observation, "provider_agent_id") != provider_id or _field(observation, "present"):
                raise UnknownExecution("Letta did not confirm Critic deletion")
    except Exception as error:
        async with factory() as uow:
            await uow.delivery.mark_lifecycle_unknown(job.operation_id, type(error).__name__)
            await uow.commit()
        raise

    async with factory() as uow:
        current = await uow.critic_workflows.get(task_id, lock=True)
        current_registry = await uow.agents.lock_registry(registry_id)
        if current is None or current.stage != "DELETE_PENDING" or current.delete_operation_id != job.operation_id:
            raise StaleInput("Critic delete result lost its workflow fence")
        await uow.agents.mark_registry_deleted(registry_id)
        await uow.delivery.complete_lifecycle_operation(job.operation_id, {
            "provider_agent_id": str(_field(observation, "provider_agent_id")) if _field(observation, "provider_agent_id") else None,
            "present": False, "observed_at": str(_field(observation, "observed_at")),
        })
        await uow.critic_workflows.transition(task_id, ("DELETE_PENDING",), "COMPLETE")
        if job.claim_fence is None:
            raise Conflict("Critic delete outbox claim is incomplete")
        await uow.delivery.ack_job(job, worker, job.claim_fence)
        await uow.delivery.append_audit({
            "owner_scope": str(workflow.owner_scope), "task_id": str(task_id),
            "operation_id": str(job.operation_id), "registry_id": str(registry_id),
            "event_kind": "critic.deleted", "safe_payload": {
                "provider_agent_id": str(_field(observation, "provider_agent_id")) if _field(observation, "provider_agent_id") else None,
            },
        })
        await uow.commit()
    return observation
