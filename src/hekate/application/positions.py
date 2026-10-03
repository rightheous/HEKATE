from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from hekate.application.evidence import manifest_references_current
from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.errors import Conflict, PolicyDenied
from hekate.domain.models import (
    CommitReceipt, ConclusionCapsule, HistoryPage, OutboxJob, PositionCommitRequest,
    PositionTopicView, PositionVersionRecord,
)
from hekate.domain.types import (
    ActorContext, AttemptStatus, DomainId, OperationId, RegistryId, ScopeId, StopReason,
    TaskId, TaskStatus, TopicId,
)
from hekate.ports.store import UowFactory


def _receipt(value: Mapping[str, object], *, replayed: bool = False) -> CommitReceipt:
    return CommitReceipt.model_validate({**value, "replayed": replayed}, strict=True)


def _request_hash(actor: ActorContext, request: PositionCommitRequest) -> str:
    return canonical_json_hash({
        "scope": str(actor.scope),
        "registry_id": str(actor.authenticated_agent_registry_id),
        "request": request.model_dump(mode="json"),
    })


async def _validate_actor_scope(uow, actor: ActorContext) -> None:
    authorization = await uow.tasks.lock_scope(actor.scope)
    if (authorization.principal_id, authorization.policy_version, authorization.authz_epoch) != (
        actor.principal_id, actor.policy_version, actor.authz_epoch,
    ):
        raise PolicyDenied("authorization snapshot changed")


async def read_current(factory: UowFactory, actor: ActorContext, topic_id: TopicId) -> PositionTopicView:
    async with factory() as uow:
        await _validate_actor_scope(uow, actor)
        agent = await uow.agents.get_persistent_scope(actor.scope)
        result = await uow.knowledge.get_current_position(
            actor.scope, topic_id, agent.registry_id if agent else None,
        )
        await uow.commit()
        return result


async def read_history(factory: UowFactory, actor: ActorContext, topic_id: TopicId, cursor) -> HistoryPage:
    if cursor.after_version < 0 or not 1 <= cursor.limit <= 200:
        raise ValueError("history cursor requires after_version >= 0 and limit between 1 and 200")
    async with factory() as uow:
        await _validate_actor_scope(uow, actor)
        agent = await uow.agents.get_persistent_scope(actor.scope)
        items = await uow.knowledge.get_position_history(
            actor.scope, topic_id, cursor.after_version, cursor.limit + 1,
            agent.registry_id if agent else None,
        )
        await uow.commit()
    has_more = len(items) > cursor.limit
    selected = tuple(items[:cursor.limit])
    return HistoryPage(
        items=selected, after_version=cursor.after_version, limit=cursor.limit,
        next_after_version=selected[-1].version if has_more and selected else None,
    )


async def _propose_commit_in_uow(
    uow, actor: ActorContext, request: PositionCommitRequest, *, runtime_operation_id: OperationId,
    operation: Mapping[str, object], binding, attempt, conclusion_id: DomainId,
    conclusion_evidence_used: tuple[object, ...], manifest: Mapping[str, object],
) -> CommitReceipt:
    if actor.authenticated_agent_registry_id is None:
        raise PolicyDenied("Position commit requires a trusted HEKATE registry identity")
    request_hash = _request_hash(actor, request)
    expected_operation = OperationId(f"{runtime_operation_id}:position.commit")
    if request.operation_id != expected_operation:
        raise PolicyDenied("commit_operation_id_mismatch")
    if operation.get("id") != runtime_operation_id or operation.get("task_id") != request.task_id:
        raise PolicyDenied("commit_runtime_operation_mismatch")
    if binding.task_id != request.task_id or binding.attempt_id != attempt.id:
        raise PolicyDenied("commit_runtime_binding_mismatch")
    if binding.agent_registry_id != actor.authenticated_agent_registry_id:
        raise PolicyDenied("commit_registry_binding_mismatch")

    await _validate_actor_scope(uow, actor)
    task = await uow.tasks.lock_task(request.task_id)
    registry = await uow.agents.lock_registry(binding.agent_registry_id)
    if registry.kind != "hekate" or registry.persistence != "persistent" or registry.owner_scope != actor.scope:
        raise PolicyDenied("commit_registry_not_authorized")
    previous = await uow.knowledge.get_commit_receipt(request.operation_id)
    if previous is not None:
        if (previous["scope"], previous["registry_id"], previous["request_hash"]) != (
            actor.scope, actor.authenticated_agent_registry_id, request_hash,
        ):
            raise Conflict("Position commit operation ID is already bound to a different actor or request")
        return _receipt(previous["receipt"], replayed=True)

    if task.scope != actor.scope or task.topic_id is None or request.topic_id != task.topic_id:
        raise PolicyDenied("commit_topic_outside_task")
    if (
        request.input_revision != binding.input_revision or task.input_revision != binding.input_revision
        or request.task_id != binding.task_id
    ):
        raise PolicyDenied("commit_revision_mismatch")
    if request.base_version != task.base_position_version:
        raise PolicyDenied("commit_base_version_mismatch")
    if (
        operation.get("execution_state") != "QUIESCENT"
        or operation.get("state") != "COMPLETED"
        or (operation.get("observation") or {}).get("outcome") != "SUCCEEDED"
        or attempt.status != AttemptStatus.SUCCEEDED
    ):
        raise PolicyDenied("commit_requires_successful_quiescent_execution")
    if task.status != TaskStatus.RUNNING or task.cancel_requested_at is not None:
        raise PolicyDenied("commit_task_not_active")
    if task.deadline <= datetime.now(UTC):
        raise PolicyDenied("commit_task_deadline_elapsed")

    if (
        manifest.get("task_id") != str(task.id)
        or manifest.get("attempt_id") != str(attempt.id)
        or manifest.get("registry_id") != str(registry.registry_id)
        or manifest.get("input_revision") != binding.input_revision
        or manifest.get("topic_id") != str(request.topic_id)
        or manifest.get("base_position_version") != request.base_version
    ):
        raise PolicyDenied("commit_context_manifest_mismatch")
    current_version = await uow.knowledge.lock_topic(actor.scope, request.topic_id)
    allowed_evidence = {item.get("id") for item in manifest.get("evidence", []) if isinstance(item, dict)}
    requested_evidence = set(map(str, request.proposed_position.evidence_refs))
    used_evidence = set(map(str, conclusion_evidence_used))
    allowed_dissent = set(manifest.get("dissent_refs", []))
    requested_dissent = set(map(str, request.proposed_position.dissent_refs))
    if not requested_evidence <= allowed_evidence or not used_evidence <= allowed_evidence:
        raise PolicyDenied("commit_evidence_reference_unavailable")
    if not await manifest_references_current(
        uow, actor.scope, actor.authz_epoch, manifest,
        tuple(request.proposed_position.evidence_refs) + tuple(conclusion_evidence_used),
    ):
        raise PolicyDenied("commit_evidence_reference_unavailable")
    if not requested_dissent <= allowed_dissent or not await uow.knowledge.valid_dissent_refs(
        actor.scope, request.proposed_position.dissent_refs,
    ):
        raise PolicyDenied("commit_dissent_reference_unavailable")

    conflict = current_version != request.base_version
    if conflict:
        current = await uow.knowledge.get_current_position(actor.scope, request.topic_id, registry.registry_id)
        statement = current.current.body.statement if current.current else "(no saved Position)"
        response_text = f"The Position changed to version {current_version} while this Task was running: {statement}"
        receipt = CommitReceipt(
            operation_id=request.operation_id, request_hash=request_hash, scope=actor.scope,
            registry_id=registry.registry_id, topic_id=request.topic_id, version=current_version,
            conflict=True, current_version=current_version, response_text=response_text,
        )
        await uow.knowledge.insert_commit_receipt(
            request.operation_id, request_hash, actor.scope, registry.registry_id,
            receipt.model_dump(mode="json"),
        )
        return receipt

    conclusion = await uow.knowledge.get_conclusion(str(conclusion_id))
    if conclusion is None or conclusion["task_id"] != task.id or conclusion["attempt_id"] != attempt.id or conclusion["registry_id"] != registry.registry_id:
        raise PolicyDenied("commit_source_conclusion_mismatch")
    capsule = ConclusionCapsule.model_validate_json(canonical_json(conclusion["capsule"]), strict=True)
    await uow.knowledge.ensure_dissent(actor.scope, conclusion_id, capsule.objections)
    version = current_version + 1
    record = PositionVersionRecord(
        scope=actor.scope, topic_id=request.topic_id, version=version, base_version=current_version,
        body=request.proposed_position, operation_id=request.operation_id, task_id=task.id,
        input_revision=binding.input_revision, registry_id=registry.registry_id,
        conclusion_id=conclusion_id, reason_for_change=request.reason_for_change,
        created_at=datetime.now(UTC),
    )
    await uow.knowledge.append_position(record)
    if not await uow.knowledge.cas_current(actor.scope, request.topic_id, current_version, version):
        raise Conflict("Position topic changed despite its row lock")
    await uow.knowledge.upsert_projection(actor.scope, request.topic_id, registry.registry_id, version)
    projection_job_id = str(uuid5(NAMESPACE_URL, f"hekate:position-projection:{runtime_operation_id}:{version}"))
    await uow.delivery.append_outbox(OutboxJob(
        id=projection_job_id, operation_id=runtime_operation_id, kind="position_projection",
        generation=version, payload={
            "scope": str(actor.scope), "topic_id": str(request.topic_id),
            "registry_id": str(registry.registry_id), "desired_version": version,
        }, status="PENDING",
    ))
    response_text = f"Position saved for {request.topic_id} as version {version}: {request.proposed_position.statement}"
    receipt = CommitReceipt(
        operation_id=request.operation_id, request_hash=request_hash, scope=actor.scope,
        registry_id=registry.registry_id, topic_id=request.topic_id, version=version,
        response_text=response_text,
    )
    await uow.knowledge.insert_commit_receipt(
        request.operation_id, request_hash, actor.scope, registry.registry_id,
        receipt.model_dump(mode="json"),
    )
    await uow.delivery.append_audit({
        "owner_scope": str(actor.scope), "task_id": str(task.id), "attempt_id": str(attempt.id),
        "operation_id": str(runtime_operation_id), "registry_id": str(registry.registry_id),
        "event_kind": "position.committed",
        "safe_payload": {"topic_id": str(request.topic_id), "version": version, "request_hash": request_hash,
                         "conclusion_id": str(conclusion_id)},
    })
    return receipt


async def propose_commit(
    factory: UowFactory, actor: ActorContext, request: PositionCommitRequest, *,
    runtime_operation_id: OperationId, binding, attempt, conclusion_id: DomainId,
    conclusion_evidence_used: tuple[object, ...], manifest: Mapping[str, object],
) -> CommitReceipt:
    """Standalone wrapper; the result adopter calls _propose_commit_in_uow in its effect transaction."""
    async with factory() as uow:
        receipt = await _propose_commit_in_uow(
            uow, actor, request, runtime_operation_id=runtime_operation_id,
            operation=await uow.delivery.lock_operation(runtime_operation_id),
            binding=binding, attempt=attempt, conclusion_id=conclusion_id,
            conclusion_evidence_used=conclusion_evidence_used, manifest=manifest,
        )
        await uow.commit()
        return receipt
