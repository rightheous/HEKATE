from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

from pydantic import Field

from hekate.application.budgets import _restore_binding
from hekate.application.tasks import complete as complete_task, converge_task_execution
from hekate.application.evidence import manifest_references_current
from hekate.application.positions import _propose_commit_in_uow
from hekate.domain.bridge_contracts import BridgeBusinessResult, BridgeEvent
from hekate.domain.capsules import parse_hekate_turn_output, validate_capsule_binding
from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.errors import Conflict, PolicyDenied
from hekate.domain.models import (
    CommitProposal, ConclusionCapsule, ContractModel, GuardBinding, HekateProposal, HekateTurnOutput,
    PositionCommitRequest, StoredConclusion,
)
from hekate.domain.proposals import parse_hekate_proposal, validate_proposal_shape
from hekate.domain.types import (
    ActorContext, AttemptStatus, DomainId, EvidenceId, OperationId, RegistryId, StopReason, TaskId, TaskStatus,
)
from hekate.application.runtime_inbox import InboxBinding
from hekate.ports.store import UowFactory


class BusinessResultPayload(ContractModel):
    event_type: Literal["business_result"]
    operation_id: str
    observation_identity: str = Field(min_length=1, max_length=256)
    binding: InboxBinding
    business_result: BridgeBusinessResult


def _reason(error: Exception) -> str:
    message = str(error)
    known = {
        "output_hash_mismatch", "output_truncated", "output_missing", "output_not_valid",
        "structured_output_mismatch", "conclusion_binding_mismatch", "conclusion_not_done",
        "evidence_not_provided", "dissent_not_provided", "commit_without_topic",
        "commit_operation_id_mismatch", "commit_proposal_binding_mismatch", "unsupported_action",
        "empty_answer", "empty_reason",
    }
    return message if message in known else "invalid_structured_output"


def validate_turn_output(
    output: HekateTurnOutput, binding: GuardBinding, runtime_operation_id: OperationId,
    manifest: Mapping[str, object],
) -> None:
    try:
        validate_capsule_binding(output.conclusion, binding)
    except ValueError as error:
        raise ValueError("conclusion_binding_mismatch") from error
    if output.conclusion.status != "done":
        raise ValueError("conclusion_not_done")
    validate_proposal_shape(output.proposal)
    if output.proposal.action not in {"answer", "request_information", "abstain", "commit"}:
        raise ValueError("unsupported_action")
    allowed_evidence = {item.get("id") for item in manifest.get("evidence", []) if isinstance(item, dict)}
    if any(str(value) not in allowed_evidence for value in output.conclusion.evidence_used):
        raise ValueError("evidence_not_provided")
    if isinstance(output.proposal, CommitProposal):
        if manifest.get("topic_id") is None:
            raise ValueError("commit_without_topic")
        if output.proposal.operation_id != OperationId(f"{runtime_operation_id}:position.commit"):
            raise ValueError("commit_operation_id_mismatch")
        if (
            output.proposal.task_id != binding.task_id
            or output.proposal.input_revision != binding.input_revision
            or str(output.proposal.topic_id) != manifest.get("topic_id")
            or output.proposal.base_version != manifest.get("base_position_version")
        ):
            raise ValueError("commit_proposal_binding_mismatch")
        if any(str(value) not in allowed_evidence for value in output.proposal.proposed_position.evidence_refs):
            raise ValueError("evidence_not_provided")
        allowed_dissent = set(manifest.get("dissent_refs", []))
        if any(str(value) not in allowed_dissent for value in output.proposal.proposed_position.dissent_refs):
            raise ValueError("dissent_not_provided")


def supported_proposal_response(proposal: HekateProposal) -> tuple[str, str, StopReason]:
    validate_proposal_shape(proposal)
    if proposal.action == "answer":
        return proposal.answer, "PROVISIONAL_ANSWER", StopReason.COMPLETED
    if proposal.action == "request_information":
        return f"Additional information needed: {proposal.reason}", "NEEDS_USER_INPUT", StopReason.NEEDS_USER_INPUT
    if proposal.action == "abstain":
        return proposal.reason or "", "ABSTAINED", StopReason.POLICY
    raise ValueError("unsupported_action")


async def receive_business_result(factory: UowFactory, event: BridgeEvent) -> dict[str, object]:
    if event.event_type != "business_result" or event.business_result is None:
        raise ValueError("bridge event is not a business result")
    payload = {
        "event_type": "business_result",
        "operation_id": event.operation_id,
        "observation_identity": event.event_id,
        "binding": event.binding.model_dump(mode="json"),
        "business_result": event.business_result.model_dump(mode="json", exclude_none=True),
    }
    encoded = canonical_json(payload).encode("utf-8")
    if len(encoded) > 131_072:
        raise ValueError("business result inbox payload exceeds 128 KiB")
    async with factory() as uow:
        operation = await uow.delivery.lock_operation(OperationId(event.operation_id))
        binding = _restore_binding(operation)
        if payload["binding"] != {
            "task_id": str(binding.task_id), "attempt_id": str(binding.attempt_id),
            "agent_registry_id": str(binding.agent_registry_id), "provider_agent_id": str(binding.provider_agent_id),
            "conversation_id": binding.conversation_id, "input_revision": binding.input_revision,
            "fence": binding.fence,
        }:
            raise Conflict("business result binding differs from admitted runtime")
        receipt = await uow.delivery.insert_inbox_once(
            "letta-bridge", event.event_id, payload, canonical_json_hash(payload),
        )
        if receipt.conflict:
            await uow.delivery.reject_inbox(receipt.id, "business_result_conflict")
        await uow.commit()
    if receipt.conflict:
        await _store_rejected_conflict(factory, receipt.id, payload)
        return {"inbox_id": receipt.id, "conflict": True, "processed": True}
    return await process_business_result_inbox(factory, receipt.id)


async def receive_missing_business_result(
    factory: UowFactory, operation_id: OperationId, binding_value: dict[str, object], failure_code: str | None,
) -> dict[str, object]:
    event = BridgeEvent.model_validate({
        "schema_version": "1",
        "event_id": f"{operation_id}:business-result-missing",
        "operation_id": str(operation_id),
        "binding": binding_value,
        "event_type": "business_result",
        "usage": {"completeness": "UNKNOWN"},
        "business_result": {"state": "MISSING", "output_truncated": False, **({"failure_code": failure_code} if failure_code else {})},
    }, strict=True)
    return await receive_business_result(factory, event)


async def _store_rejected_conflict(factory: UowFactory, inbox_id: str, payload: dict[str, object]) -> None:
    async with factory() as uow:
        inbox = await uow.delivery.lock_inbox(inbox_id)
        if inbox is None:
            raise RuntimeError("conflicting business result disappeared")
        operation_id = inbox["payload"]["operation_id"]
        operation = await uow.delivery.lock_operation(OperationId(operation_id))
        trusted = _restore_binding(operation)
        binding = inbox["payload"]["binding"]
        result = inbox["payload"]["business_result"]
        await uow.knowledge.save_turn_result({
            "inbox_id": inbox_id, "task_id": binding["task_id"], "attempt_id": binding["attempt_id"],
            "operation_id": inbox["payload"]["operation_id"], "registry_id": binding["agent_registry_id"],
            "input_revision": binding["input_revision"],
            "output_hash": result.get("output_sha256") or canonical_json_hash(payload),
            "raw_output": result.get("raw_output"), "structured_output": result.get("structured_output"),
            "processing_state": "REJECTED", "rejection_reason": "business_result_conflict",
        })
        await uow.delivery.append_audit({
            "owner_scope": str(trusted.scope),
            "task_id": str(binding["task_id"]), "attempt_id": str(binding["attempt_id"]),
            "operation_id": str(operation_id), "registry_id": str(binding["agent_registry_id"]),
            "event_kind": "business_result.conflict", "safe_payload": {"output_hash": result.get("output_sha256")},
        })
        await uow.commit()


async def process_business_result_inbox(factory: UowFactory, inbox_id: str) -> dict[str, object]:
    async with factory() as uow:
        inbox = await uow.delivery.lock_inbox(inbox_id)
        if inbox is None:
            raise ValueError("business result inbox is unavailable")
        if inbox["processed_at"] is not None:
            result = await uow.knowledge.lock_turn_result(inbox_id)
            await uow.commit()
            return {
                "inbox_id": inbox_id, "duplicate": True,
                "processed": True,
                "pending": bool(result and result["processing_state"] == "WAITING_EXECUTION"),
            }
        payload = BusinessResultPayload.model_validate_json(canonical_json(inbox["payload"]), strict=True)
        operation = await uow.delivery.lock_operation(OperationId(payload.operation_id))
        binding = _restore_binding(operation)
        stored_binding = payload.binding
        expected = {
            "task_id": str(binding.task_id), "attempt_id": str(binding.attempt_id),
            "agent_registry_id": str(binding.agent_registry_id), "provider_agent_id": str(binding.provider_agent_id),
            "conversation_id": binding.conversation_id, "input_revision": binding.input_revision,
            "fence": binding.fence,
        }
        if stored_binding.model_dump(mode="json") != expected:
            raise Conflict("persisted business result binding differs from operation")
        business = payload.business_result
        raw = business.raw_output
        raw_bytes = raw.encode("utf-8") if raw is not None else b""
        digest = business.output_sha256 or hashlib.sha256(raw_bytes).hexdigest()
        result_values = {
            "inbox_id": inbox_id, "task_id": str(binding.task_id), "attempt_id": str(binding.attempt_id),
            "operation_id": payload.operation_id, "registry_id": str(binding.agent_registry_id),
            "input_revision": binding.input_revision, "output_hash": digest,
            "raw_output": raw, "structured_output": business.structured_output,
            "processing_state": "REJECTED", "rejection_reason": None,
        }
        await uow.knowledge.save_turn_result(result_values)
        if business.state != "VALID":
            reason = "output_truncated" if business.output_truncated else "output_missing" if business.state == "MISSING" else "output_not_valid"
            await uow.knowledge.update_turn_result(inbox_id, state="WAITING_EXECUTION", rejection_reason=reason, delay_seconds=1)
            await uow.delivery.mark_inbox_processed(inbox_id)
            await uow.delivery.append_audit({
                "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id), "operation_id": payload.operation_id,
                "registry_id": str(binding.agent_registry_id), "event_kind": "business_result.rejected",
                "safe_payload": {"reason": reason, "output_hash": digest},
            })
            await uow.commit()
            return await apply_turn_result(factory, inbox_id)
        if raw is None or business.output_truncated:
            reason = "output_truncated" if business.output_truncated else "output_missing"
            await uow.knowledge.update_turn_result(inbox_id, state="WAITING_EXECUTION", rejection_reason=reason, delay_seconds=1)
            await uow.delivery.mark_inbox_processed(inbox_id)
            await uow.commit()
            return await apply_turn_result(factory, inbox_id)
        if hashlib.sha256(raw_bytes).hexdigest() != business.output_sha256:
            reason = "output_hash_mismatch"
            await uow.knowledge.update_turn_result(inbox_id, state="WAITING_EXECUTION", rejection_reason=reason, delay_seconds=1)
            await uow.delivery.mark_inbox_processed(inbox_id)
            await uow.commit()
            return await apply_turn_result(factory, inbox_id)
        try:
            output = parse_hekate_turn_output(raw_bytes)
        except Exception as error:
            reason = "invalid_structured_output"
            await uow.knowledge.update_turn_result(inbox_id, state="WAITING_EXECUTION", rejection_reason=reason, delay_seconds=1)
            await uow.delivery.mark_inbox_processed(inbox_id)
            await uow.commit()
            return await apply_turn_result(factory, inbox_id)
        if canonical_json(json.loads(raw)) != canonical_json(business.structured_output):
            reason = "structured_output_mismatch"
        else:
            try:
                manifest_row = await uow.knowledge.get_context_manifest(OperationId(payload.operation_id))
                if manifest_row is None:
                    raise ValueError("context_manifest_missing")
                validate_turn_output(output, binding, OperationId(payload.operation_id), manifest_row["manifest"])
            except Exception as error:
                reason = _reason(error)
            else:
                reason = None

        proposal = output.proposal.model_dump(mode="json")
        conclusion_id = str(uuid5(NAMESPACE_URL, f"hekate:conclusion:{inbox_id}"))
        capsule = output.conclusion
        capsule_matches = (
            capsule.task_id == binding.task_id and capsule.attempt_id == binding.attempt_id
            and capsule.agent_id == binding.agent_registry_id
        )
        if capsule_matches:
            await uow.knowledge.insert_conclusion(StoredConclusion(
                id=DomainId(conclusion_id), attempt_id=binding.attempt_id,
                payload_hash=canonical_json_hash(capsule), capsule=capsule,
                validation_status="VALIDATED" if reason is None else "REJECTED",
                eligible=False,
                provider_provenance={
                    "operation_id": payload.operation_id, "input_revision": binding.input_revision,
                    "inbox_id": inbox_id, "event_id": payload.observation_identity,
                    "output_hash": digest, "provider_agent_id": str(binding.provider_agent_id),
                    "fence": binding.fence,
                },
                rejection_reason=reason,
            ))
            if reason is None:
                await uow.knowledge.ensure_dissent(binding.scope, DomainId(conclusion_id), capsule.objections)
        await uow.knowledge.update_turn_result(
            inbox_id, state="WAITING_EXECUTION",
            proposal=proposal, conclusion_id=conclusion_id if capsule_matches else None,
            rejection_reason=reason, delay_seconds=1,
        )
        await uow.delivery.mark_inbox_processed(inbox_id)
        await uow.commit()
    return await apply_turn_result(factory, inbox_id)


async def apply_turn_result(factory: UowFactory, inbox_id: str) -> dict[str, object]:
    async with factory() as uow:
        result = await uow.knowledge.lock_turn_result(inbox_id)
        if result is None:
            return {"inbox_id": inbox_id, "processed": False, "reason": "result_not_stored"}
        if result["processing_state"] in {"REJECTED", "LATE", "ACCEPTED"}:
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": True, "state": result["processing_state"]}
        operation = await uow.delivery.lock_operation(OperationId(result["operation_id"]))
        binding = _restore_binding(operation)
        current_scope = await uow.tasks.lock_scope(binding.scope)
        task = await uow.tasks.lock_task(binding.task_id)
        attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        registry = await uow.agents.lock_registry(binding.agent_registry_id)
        if operation["execution_state"] != "QUIESCENT":
            await uow.knowledge.update_turn_result(inbox_id, state="WAITING_EXECUTION", delay_seconds=1)
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": False, "pending": True}

        reason = result["rejection_reason"]
        operation_outcome = (operation.get("observation") or {}).get("outcome")
        successful = operation["state"] == "COMPLETED" and operation_outcome == "SUCCEEDED" and attempt.status == AttemptStatus.SUCCEEDED
        proposal = result["proposal"]
        response_text: str
        outcome: str
        stop_reason: str
        proposal_model: HekateProposal | None = None
        manifest_row = await uow.knowledge.get_context_manifest(OperationId(result["operation_id"]))
        manifest = manifest_row["manifest"] if manifest_row is not None else {}
        if successful and reason is None and not isinstance(proposal, dict):
            successful = False
            reason = "proposal_missing"
        if successful and reason is None and isinstance(proposal, dict):
            try:
                proposal_model = parse_hekate_proposal(canonical_json(proposal).encode("utf-8"))
            except (ValueError, KeyError, TypeError) as error:
                successful = False
                reason = _reason(error)
        if successful and reason is None:
            conclusion_row = await uow.knowledge.get_conclusion(result["conclusion_id"]) if result["conclusion_id"] else None
            if manifest_row is None or conclusion_row is None:
                successful, reason = False, "context_manifest_missing"
            else:
                conclusion_capsule = ConclusionCapsule.model_validate_json(canonical_json(conclusion_row["capsule"]), strict=True)
                if not isinstance(proposal_model, CommitProposal) and not await manifest_references_current(
                    uow, binding.scope, binding.authz_epoch, manifest, conclusion_capsule.evidence_used,
                ):
                    successful, reason = False, "evidence_reference_unavailable"
        if successful and reason is None and proposal_model is not None:
            if isinstance(proposal_model, CommitProposal):
                actor = ActorContext(
                    principal_id=binding.principal_id, scope=binding.scope,
                    authenticated_agent_registry_id=binding.agent_registry_id,
                    task_id=binding.task_id, attempt_id=binding.attempt_id,
                    input_revision=binding.input_revision, policy_version=binding.policy_version,
                    authz_epoch=binding.authz_epoch, fence=binding.fence,
                )
                commit_request = PositionCommitRequest(
                    operation_id=proposal_model.operation_id, task_id=proposal_model.task_id,
                    topic_id=proposal_model.topic_id, base_version=proposal_model.base_version,
                    input_revision=proposal_model.input_revision,
                    proposed_position=proposal_model.proposed_position,
                    reason_for_change=proposal_model.reason_for_change,
                )
                try:
                    receipt = await _propose_commit_in_uow(
                        uow, actor, commit_request, runtime_operation_id=OperationId(result["operation_id"]),
                        operation=operation, binding=binding, attempt=attempt,
                        conclusion_id=DomainId(result["conclusion_id"]),
                        conclusion_evidence_used=conclusion_capsule.evidence_used,
                        manifest=manifest,
                    )
                except (PolicyDenied, Conflict) as error:
                    successful, reason = False, str(error) or "commit_policy_rejected"
                else:
                    response_text = receipt.response_text
                    outcome = "NEEDS_USER_INPUT" if receipt.conflict else "POSITION_COMMITTED"
                    stop_reason = StopReason.NEEDS_USER_INPUT.value if receipt.conflict else StopReason.COMPLETED.value
            else:
                response_text, outcome, stop_reason_value = supported_proposal_response(proposal_model)
                stop_reason = stop_reason_value.value
        if not successful or reason is not None:
            safe_reason = reason or "execution_failed"
            response_text = f"HEKATE could not complete this request ({safe_reason})."
            policy_rejection = safe_reason.startswith(("evidence_", "dissent_", "commit_", "authorization_")) or safe_reason in {
                "invalid_structured_output", "output_hash_mismatch", "output_truncated", "output_missing",
                "output_not_valid", "structured_output_mismatch", "conclusion_binding_mismatch",
                "conclusion_not_done", "unsupported_action", "empty_answer", "empty_reason",
                "context_manifest_missing",
            }
            outcome = "FAILED"
            stop_reason = StopReason.POLICY.value if policy_rejection else StopReason.ERROR.value
            successful = False

        stale = (
            task.scope != binding.scope or task.input_revision != binding.input_revision
            or current_scope.principal_id != binding.principal_id
            or current_scope.policy_version != binding.policy_version
            or current_scope.authz_epoch != binding.authz_epoch
            or attempt.operation_id != result["operation_id"]
            or attempt.input_revision != binding.input_revision
            or attempt.agent_registry_id != binding.agent_registry_id
        )
        if stale:
            reason = "task_or_binding_changed"
            await uow.knowledge.update_turn_result(inbox_id, state="LATE", rejection_reason=reason)
            if result["conclusion_id"]:
                await uow.knowledge.set_conclusion_eligible(result["conclusion_id"], False, reason)
            await uow.delivery.append_audit({
                "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id), "operation_id": str(result["operation_id"]),
                "registry_id": str(binding.agent_registry_id), "event_kind": "business_result.late",
                "safe_payload": {"output_hash": result["output_hash"], "rejection_reason": reason},
            })
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": True, "state": "LATE"}
        terminal_reason = None
        if task.cancel_requested_at is not None:
            terminal_reason = "task_cancelled"
        elif task.status == TaskStatus.FAILED and task.stop_reason == StopReason.DEADLINE.value:
            terminal_reason = "deadline_elapsed"
        elif task.status in {TaskStatus.COMPLETED, TaskStatus.CANCELLED, TaskStatus.FAILED}:
            terminal_reason = "task_already_terminal"
        elif task.deadline <= datetime.now(UTC):
            terminal_reason = "deadline_elapsed"
        elif task.status != TaskStatus.RUNNING:
            terminal_reason = "task_not_running"
        if terminal_reason is not None:
            await converge_task_execution(uow, binding.task_id, binding.input_revision)
            reason = terminal_reason
            await uow.knowledge.update_turn_result(inbox_id, state="LATE", rejection_reason=reason)
            if result["conclusion_id"]:
                await uow.knowledge.set_conclusion_eligible(result["conclusion_id"], False, reason)
            await uow.delivery.append_audit({
                "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id), "operation_id": str(result["operation_id"]),
                "registry_id": str(binding.agent_registry_id), "event_kind": "business_result.late",
                "safe_payload": {"output_hash": result["output_hash"], "rejection_reason": reason},
            })
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": True, "state": "LATE"}
        response = {
            "operation_id": result["operation_id"], "attempt_id": result["attempt_id"],
            "registry_id": result["registry_id"], "source_inbox_id": inbox_id,
            "proposal": proposal, "response_text": response_text,
            "outcome": outcome, "stop_reason": stop_reason,
        }
        actor = ActorContext(
            principal_id=binding.principal_id, scope=binding.scope,
            authenticated_agent_registry_id=binding.agent_registry_id, task_id=binding.task_id,
            attempt_id=binding.attempt_id, input_revision=binding.input_revision,
            policy_version=binding.policy_version, authz_epoch=binding.authz_epoch,
            fence=binding.fence,
        )
        adopted = await complete_task(
            uow, actor, binding.task_id, binding.input_revision, response,
            successful=successful,
        )
        if not adopted:
            await uow.rollback()
            reason = "task_changed_before_adoption"
            async with factory() as late_uow:
                await late_uow.knowledge.update_turn_result(inbox_id, state="LATE", rejection_reason=reason)
                if result["conclusion_id"]:
                    await late_uow.knowledge.set_conclusion_eligible(result["conclusion_id"], False, reason)
                await late_uow.delivery.append_audit({
                    "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
                    "attempt_id": str(binding.attempt_id), "operation_id": str(result["operation_id"]),
                    "registry_id": str(binding.agent_registry_id), "event_kind": "business_result.late",
                    "safe_payload": {"output_hash": result["output_hash"], "rejection_reason": reason},
                })
                await late_uow.commit()
            return {"inbox_id": inbox_id, "processed": True, "state": "LATE"}
        await uow.knowledge.update_turn_result(inbox_id, state="ACCEPTED" if successful else "REJECTED", rejection_reason=None if successful else reason or "execution_failed")
        if result["conclusion_id"]:
            await uow.knowledge.set_conclusion_eligible(result["conclusion_id"], successful and reason is None, None if successful and reason is None else reason or "execution_failed")
        await uow.delivery.append_audit({
            "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
            "attempt_id": str(binding.attempt_id), "operation_id": str(result["operation_id"]),
            "registry_id": str(binding.agent_registry_id),
            "event_kind": "task.result_accepted" if successful else "task.result_failed",
            "safe_payload": {"output_hash": result["output_hash"], "outcome": outcome, "stop_reason": stop_reason},
        })
        await uow.commit()
        return {"inbox_id": inbox_id, "processed": True, "state": "ACCEPTED" if successful else "REJECTED"}


async def process_pending_results(factory: UowFactory, limit: int = 100) -> int:
    async with factory() as uow:
        inbox_ids = await uow.tasks.list_pending_turn_results(limit)
        await uow.commit()
    processed = 0
    for inbox_id in inbox_ids:
        result = await apply_turn_result(factory, inbox_id)
        processed += int(result.get("processed", False))
    return processed
