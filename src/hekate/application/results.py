from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

from pydantic import Field

from hekate.application.budgets import _restore_binding
from hekate.application.tasks import complete as complete_task, converge_task_execution
from hekate.application.evidence import manifest_references_current
from hekate.application.positions import _propose_commit_in_uow
from hekate.application.lifecycle import _queue_retirement_in_uow, _reservation_amount, request_critic
from hekate.domain.bridge_contracts import BridgeBusinessResult, BridgeEvent
from hekate.domain.capsules import parse_critic_turn_output, parse_hekate_turn_output, validate_capsule_binding
from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.errors import BudgetDenied, Conflict, PolicyDenied, StaleInput
from hekate.domain.models import (
    AuthorizationSnapshot, CommitProposal, ConclusionCapsule, ContractModel, ContinuationProposal, CriticTurnOutput, GuardBinding,
    HekateProposal, HekateTurnOutput, PositionCommitRequest, ReservationRequest, SpawnProposal,
    StoredConclusion, TaskExecutionConfig,
)
from hekate.domain.proposals import parse_hekate_proposal, validate_proposal_shape
from hekate.domain.types import (
    ActorContext, AttemptId, AttemptStatus, DomainId, EvidenceId, OperationId, RegistryId,
    ReservationId, StopReason, TaskId, TaskStatus,
)
from hekate.application.runtime_inbox import InboxBinding
from hekate.ports.store import UowFactory
from hekate.settings import DeliberationConfig, execution_config_snapshot


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
        "empty_answer", "empty_reason", "invalid_spawn_proposal", "critic_binding_mismatch",
        "critic_workflow_binding_mismatch", "synthesis_workflow_binding_mismatch",
        "unsupported_continue_action", "continuation_fields_required", "continuation_not_allowed",
    }
    return message if message in known else "invalid_structured_output"


def validate_turn_output(
    output: HekateTurnOutput, binding: GuardBinding, runtime_operation_id: OperationId,
    manifest: Mapping[str, object], *, allow_spawn: bool = False, allow_continue: bool = False,
) -> None:
    try:
        validate_capsule_binding(output.conclusion, binding)
    except ValueError as error:
        raise ValueError("conclusion_binding_mismatch") from error
    if output.conclusion.status != "done":
        raise ValueError("conclusion_not_done")
    validate_proposal_shape(output.proposal)
    allowed_actions = {"answer", "request_information", "abstain", "commit"}
    if allow_spawn:
        allowed_actions.add("spawn")
    if allow_continue:
        allowed_actions.add("continue")
    if output.proposal.action not in allowed_actions:
        raise ValueError("unsupported_action")
    if isinstance(output.proposal, SpawnProposal):
        if not allow_spawn or output.proposal.task_id != binding.task_id or any(not value.strip() for value in (
            output.proposal.purpose, output.proposal.target_uncertainty, output.proposal.expected_decision_impact,
        )):
            raise ValueError("invalid_spawn_proposal")
    if isinstance(output.proposal, ContinuationProposal):
        if not allow_continue:
            raise ValueError("continuation_not_allowed")
        if output.proposal.next_action not in {"hekate_reasoning", "critic_review"}:
            raise ValueError("unsupported_continue_action")
        if any(not value.strip() for value in (
            output.proposal.unresolved_issue, output.proposal.expected_information_gain,
            output.proposal.decision_impact,
        )):
            raise ValueError("continuation_fields_required")
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


def validate_critic_turn_output(
    output: CriticTurnOutput, binding: GuardBinding, manifest: Mapping[str, object],
) -> None:
    try:
        validate_capsule_binding(output.conclusion, binding)
    except ValueError as error:
        raise ValueError("conclusion_binding_mismatch") from error
    if output.conclusion.status != "done":
        raise ValueError("conclusion_not_done")
    allowed = {item.get("id") for item in manifest.get("evidence", []) if isinstance(item, dict)}
    if any(str(value) not in allowed for value in output.conclusion.evidence_used):
        raise ValueError("evidence_not_provided")


def supported_proposal_response(proposal: HekateProposal) -> tuple[str, str, StopReason]:
    validate_proposal_shape(proposal)
    if proposal.action == "answer":
        return proposal.answer, "PROVISIONAL_ANSWER", StopReason.COMPLETED
    if proposal.action == "request_information":
        return f"Additional information needed: {proposal.reason}", "NEEDS_USER_INPUT", StopReason.NEEDS_USER_INPUT
    if proposal.action == "abstain":
        return proposal.reason or "", "ABSTAINED", StopReason.POLICY
    raise ValueError("unsupported_action")


async def receive_business_result(
    factory: UowFactory, event: BridgeEvent, *, hekate_config=None, critic_config=None,
    deliberation_config: DeliberationConfig | None = None,
) -> dict[str, object]:
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
    return await process_business_result_inbox(
        factory, receipt.id, hekate_config=hekate_config, critic_config=critic_config,
        deliberation_config=deliberation_config,
    )


async def receive_missing_business_result(
    factory: UowFactory, operation_id: OperationId, binding_value: dict[str, object], failure_code: str | None,
    *, hekate_config=None, critic_config=None, deliberation_config: DeliberationConfig | None = None,
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
    return await receive_business_result(
        factory, event, hekate_config=hekate_config, critic_config=critic_config,
        deliberation_config=deliberation_config,
    )


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


async def process_business_result_inbox(
    factory: UowFactory, inbox_id: str, *, hekate_config=None, critic_config=None,
    deliberation_config: DeliberationConfig | None = None,
) -> dict[str, object]:
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
            return await apply_turn_result(factory, inbox_id, hekate_config=hekate_config, critic_config=critic_config, deliberation_config=deliberation_config)
        if raw is None or business.output_truncated:
            reason = "output_truncated" if business.output_truncated else "output_missing"
            await uow.knowledge.update_turn_result(inbox_id, state="WAITING_EXECUTION", rejection_reason=reason, delay_seconds=1)
            await uow.delivery.mark_inbox_processed(inbox_id)
            await uow.commit()
            return await apply_turn_result(factory, inbox_id, hekate_config=hekate_config, critic_config=critic_config, deliberation_config=deliberation_config)
        if hashlib.sha256(raw_bytes).hexdigest() != business.output_sha256:
            reason = "output_hash_mismatch"
            await uow.knowledge.update_turn_result(inbox_id, state="WAITING_EXECUTION", rejection_reason=reason, delay_seconds=1)
            await uow.delivery.mark_inbox_processed(inbox_id)
            await uow.commit()
            return await apply_turn_result(factory, inbox_id, hekate_config=hekate_config, critic_config=critic_config, deliberation_config=deliberation_config)
        attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        agent = await uow.agents.lock_registry(binding.agent_registry_id)
        deliberation_step = await uow.deliberation.get_by_operation(payload.operation_id, lock=True)
        critic_output: CriticTurnOutput | None = None
        output: HekateTurnOutput | None = None
        try:
            if attempt.kind == "critic_review":
                if operation["kind"] != "critic.review" or agent.kind != "critic" or agent.persistence != "ephemeral":
                    raise ValueError("critic_binding_mismatch")
                if deliberation_step is not None:
                    workflow = await uow.critic_workflows.get(binding.task_id, lock=True)
                    if (
                        deliberation_step["step_kind"] != "critic_review"
                        or deliberation_step["state"] != "ADMITTED"
                        or deliberation_step["attempt_id"] != str(binding.attempt_id)
                        or deliberation_step["registry_id"] != str(binding.agent_registry_id)
                        or workflow is None or workflow.critic_registry_id != binding.agent_registry_id
                    ):
                        raise ValueError("critic_workflow_binding_mismatch")
                critic_output = parse_critic_turn_output(raw_bytes)
            else:
                if attempt.kind not in {"planning", "synthesis", "hekate_reasoning", "synthesis_round2"} or agent.kind != "hekate" or agent.persistence != "persistent":
                    raise ValueError("hekate_binding_mismatch")
                if attempt.kind == "synthesis" and operation["kind"] != "hekate.synthesis":
                    raise ValueError("synthesis_binding_mismatch")
                if attempt.kind == "hekate_reasoning" and operation["kind"] != "hekate.reasoning":
                    raise ValueError("synthesis_binding_mismatch")
                if attempt.kind == "synthesis_round2" and operation["kind"] != "hekate.synthesis":
                    raise ValueError("synthesis_binding_mismatch")
                if attempt.kind in {"hekate_reasoning", "synthesis_round2"} and (
                    deliberation_step is None or deliberation_step["state"] != "ADMITTED"
                    or deliberation_step["attempt_id"] != str(binding.attempt_id)
                    or deliberation_step["registry_id"] != str(binding.agent_registry_id)
                    or deliberation_step["step_kind"] != ("hekate_reasoning" if attempt.kind == "hekate_reasoning" else "synthesis")
                ):
                    raise ValueError("synthesis_workflow_binding_mismatch")
                output = parse_hekate_turn_output(raw_bytes)
        except Exception as error:
            reason = "invalid_structured_output"
            await uow.knowledge.update_turn_result(inbox_id, state="WAITING_EXECUTION", rejection_reason=reason, delay_seconds=1)
            await uow.delivery.mark_inbox_processed(inbox_id)
            await uow.commit()
            return await apply_turn_result(factory, inbox_id, hekate_config=hekate_config, critic_config=critic_config, deliberation_config=deliberation_config)
        if canonical_json(json.loads(raw)) != canonical_json(business.structured_output):
            reason = "structured_output_mismatch"
        else:
            try:
                manifest_row = await uow.knowledge.get_context_manifest(OperationId(payload.operation_id))
                if manifest_row is None:
                    raise ValueError("context_manifest_missing")
                if attempt.kind == "critic_review":
                    if deliberation_step is None:
                        workflow = await uow.critic_workflows.get(binding.task_id, lock=True)
                        if (
                            workflow is None or workflow.stage != "REVIEW_ADMITTED"
                            or workflow.review_attempt_id != binding.attempt_id
                            or workflow.review_operation_id != OperationId(payload.operation_id)
                            or workflow.critic_registry_id != binding.agent_registry_id
                            or critic_output is None
                        ):
                            raise ValueError("critic_workflow_binding_mismatch")
                    elif critic_output is None:
                        raise ValueError("critic_workflow_binding_mismatch")
                    validate_critic_turn_output(critic_output, binding, manifest_row["manifest"])
                else:
                    if output is None:
                        raise ValueError("output_missing")
                    if attempt.kind == "synthesis":
                        workflow = await uow.critic_workflows.get(binding.task_id, lock=True)
                        if (
                            workflow is None or workflow.stage != "SYNTHESIS_ADMITTED"
                            or workflow.synthesis_attempt_id != binding.attempt_id
                            or workflow.synthesis_operation_id != OperationId(payload.operation_id)
                        ):
                            raise ValueError("synthesis_workflow_binding_mismatch")
                    validate_turn_output(
                        output, binding, OperationId(payload.operation_id), manifest_row["manifest"],
                        allow_spawn=attempt.kind == "planning",
                        allow_continue=bool(
                            deliberation_config is not None and deliberation_config.enabled
                            and attempt.kind in {"planning", "synthesis", "hekate_reasoning", "synthesis_round2"}
                        ),
                    )
            except Exception as error:
                reason = _reason(error)
            else:
                reason = None

        proposal = output.proposal.model_dump(mode="json") if output is not None else None
        conclusion_id = str(uuid5(NAMESPACE_URL, f"hekate:conclusion:{inbox_id}"))
        capsule = critic_output.conclusion if critic_output is not None else output.conclusion
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
            if reason is None and critic_output is None:
                await uow.knowledge.ensure_dissent(binding.scope, DomainId(conclusion_id), capsule.objections)
        await uow.knowledge.update_turn_result(
            inbox_id, state="WAITING_EXECUTION",
            proposal=proposal, conclusion_id=conclusion_id if capsule_matches else None,
            rejection_reason=reason, delay_seconds=1,
        )
        await uow.delivery.mark_inbox_processed(inbox_id)
        await uow.commit()
    return await apply_turn_result(factory, inbox_id, hekate_config=hekate_config, critic_config=critic_config, deliberation_config=deliberation_config)


def _late_reason(task, attempt, result, binding, now: datetime) -> str | None:
    if (
        task.scope != binding.scope or task.input_revision != binding.input_revision
        or attempt.operation_id != result["operation_id"]
        or attempt.input_revision != binding.input_revision
        or attempt.agent_registry_id != binding.agent_registry_id
    ):
        return "task_or_binding_changed"
    if task.cancel_requested_at is not None:
        return "task_cancelled"
    if task.status == TaskStatus.FAILED and task.stop_reason == StopReason.DEADLINE.value:
        return "deadline_elapsed"
    if task.status in {TaskStatus.COMPLETED, TaskStatus.CANCELLED, TaskStatus.FAILED}:
        return "task_already_terminal"
    if task.deadline <= now:
        return "deadline_elapsed"
    if task.status != TaskStatus.RUNNING:
        return "task_not_running"
    return None


def _authorization_failure(scope_state, binding) -> str | None:
    snapshot = scope_state.snapshot
    if not scope_state.active:
        return "authorization_scope_revoked"
    if (snapshot.principal_id, snapshot.policy_version, snapshot.authz_epoch) != (
        binding.principal_id, binding.policy_version, binding.authz_epoch,
    ):
        return "authorization_snapshot_changed"
    return None


def _expected_authorization(binding):
    return AuthorizationSnapshot(
        scope=binding.scope,
        principal_id=binding.principal_id,
        policy_version=binding.policy_version,
        authz_epoch=binding.authz_epoch,
    )


def _policy_rejection(reason: str) -> bool:
    return reason.startswith(("evidence_", "dissent_", "commit_", "authorization_", "critic_", "spawn_")) or reason in {
        "invalid_structured_output", "output_hash_mismatch", "output_truncated", "output_missing",
        "output_not_valid", "structured_output_mismatch", "conclusion_binding_mismatch",
        "conclusion_not_done", "unsupported_action", "empty_answer", "empty_reason",
        "context_manifest_missing", "unsupported_continue_action", "continuation_fields_required",
        "continuation_not_allowed", "continuation_parent_missing",
    }


def _failure_stop_reason(reason: str) -> str:
    if reason.startswith("budget_") or "budget" in reason:
        return StopReason.BUDGET.value
    return StopReason.POLICY.value if _policy_rejection(reason) else StopReason.ERROR.value


async def _record_late_after_rollback(
    factory: UowFactory, inbox_id: str, reason: str, clock: Callable[[], datetime],
) -> dict[str, object]:
    async with factory() as uow:
        result = await uow.knowledge.lock_turn_result(inbox_id)
        if result is None:
            return {"inbox_id": inbox_id, "processed": False, "reason": "result_not_stored"}
        if result["processing_state"] in {"REJECTED", "LATE", "ACCEPTED"}:
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": True, "state": result["processing_state"]}
        operation = await uow.delivery.lock_operation(OperationId(result["operation_id"]))
        binding = _restore_binding(operation)
        await uow.tasks.lock_scope_for_observation(binding.scope)
        task = await uow.tasks.lock_task(binding.task_id)
        attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        await uow.agents.lock_registry(binding.agent_registry_id)
        if (
            task.scope == binding.scope and task.input_revision == binding.input_revision
            and attempt.operation_id == result["operation_id"]
            and operation["execution_state"] == "QUIESCENT"
        ):
            resolved = await converge_task_execution(uow, binding.task_id, binding.input_revision, now=clock())
            workflow = await uow.critic_workflows.get(binding.task_id, lock=True)
            if resolved is not None and workflow is not None and workflow.stage in {
                "REVIEW_ADMITTED", "SYNTHESIS_PENDING", "SYNTHESIS_ADMITTED", "FAILED",
            }:
                await _queue_retirement_in_uow(uow, workflow)
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


async def _record_commit_failure(
    factory: UowFactory, inbox_id: str, failure: str, clock: Callable[[], datetime],
) -> dict[str, object]:
    async with factory() as uow:
        result = await uow.knowledge.lock_turn_result(inbox_id)
        if result is None:
            return {"inbox_id": inbox_id, "processed": False, "reason": "result_not_stored"}
        if result["processing_state"] in {"REJECTED", "LATE", "ACCEPTED"}:
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": True, "state": result["processing_state"]}
        operation = await uow.delivery.lock_operation(OperationId(result["operation_id"]))
        binding = _restore_binding(operation)
        scope_state = await uow.tasks.lock_scope_for_observation(binding.scope)
        task = await uow.tasks.lock_task(binding.task_id)
        attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        await uow.agents.lock_registry(binding.agent_registry_id)
        if operation["execution_state"] != "QUIESCENT":
            await uow.knowledge.update_turn_result(inbox_id, state="WAITING_EXECUTION", delay_seconds=1)
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": False, "pending": True}
        terminal_reason = _late_reason(task, attempt, result, binding, clock())
        if terminal_reason is not None:
            await uow.rollback()
            return await _record_late_after_rollback(factory, inbox_id, terminal_reason, clock)
        authorization_failure = _authorization_failure(scope_state, binding)
        reason = authorization_failure or failure or "commit_policy_rejected"
        response = {
            "operation_id": result["operation_id"], "attempt_id": result["attempt_id"],
            "registry_id": result["registry_id"], "source_inbox_id": inbox_id,
            "proposal": result["proposal"],
            "response_text": f"HEKATE could not complete this request ({reason}).",
            "outcome": "FAILED",
            "stop_reason": _failure_stop_reason(reason),
        }
        actor = ActorContext(
            principal_id=binding.principal_id, scope=binding.scope,
            authenticated_agent_registry_id=binding.agent_registry_id, task_id=binding.task_id,
            attempt_id=binding.attempt_id, input_revision=binding.input_revision,
            policy_version=binding.policy_version, authz_epoch=binding.authz_epoch,
            fence=binding.fence,
        )
        try:
            if authorization_failure is not None:
                adopted = await uow.tasks.fail_unaccepted_execution_with_policy_response(
                    binding.scope, _expected_authorization(binding), binding.task_id,
                    binding.input_revision, response, accepted_at=clock(),
                )
            else:
                adopted = await complete_task(
                    uow, actor, binding.task_id, binding.input_revision, response,
                    successful=False, accepted_at=clock(),
                )
        except (Conflict, PolicyDenied, StaleInput):
            await uow.rollback()
            return await _record_late_after_rollback(factory, inbox_id, "task_changed_before_adoption", clock)
        if not adopted:
            await uow.rollback()
            return await _record_late_after_rollback(factory, inbox_id, "task_changed_before_adoption", clock)
        await uow.knowledge.update_turn_result(inbox_id, state="REJECTED", rejection_reason=reason)
        if result["conclusion_id"]:
            await uow.knowledge.set_conclusion_eligible(result["conclusion_id"], False, reason)
        dynamic_step = await uow.deliberation.get_by_operation(result["operation_id"], lock=True)
        if dynamic_step is not None and dynamic_step["state"] in {"ADMITTED", "READY", "WAITING_PARENT"}:
            await uow.deliberation.transition(
                dynamic_step["id"], (dynamic_step["state"],), "STOPPED",
                conclusion_id=result["conclusion_id"], stop_reason=reason,
            )
        workflow = await uow.critic_workflows.get(binding.task_id, lock=True)
        if workflow is not None and workflow.stage in {
            "REVIEW_ADMITTED", "SYNTHESIS_PENDING", "SYNTHESIS_ADMITTED", "FAILED",
        }:
            await _queue_retirement_in_uow(uow, workflow)
        await uow.delivery.append_audit({
            "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
            "attempt_id": str(binding.attempt_id), "operation_id": str(result["operation_id"]),
            "registry_id": str(binding.agent_registry_id), "event_kind": "task.result_failed",
            "safe_payload": {"output_hash": result["output_hash"], "outcome": "FAILED", "stop_reason": response["stop_reason"]},
        })
        await uow.commit()
    return {"inbox_id": inbox_id, "processed": True, "state": "REJECTED"}


async def _approve_continuation_in_uow(
    uow, actor: ActorContext, task, operation: Mapping[str, object], attempt,
    conclusion_id: DomainId, proposal: ContinuationProposal, manifest: Mapping[str, object],
    hekate_config: TaskExecutionConfig | None, critic_config: TaskExecutionConfig | None,
    limits: DeliberationConfig,
) -> Mapping[str, object]:
    """Reserve an approved bounded follow-up and its durable identity in this result transaction."""
    if not limits.enabled or hekate_config is None:
        raise PolicyDenied("bounded_deliberation_disabled")
    binding = _restore_binding(operation)
    scope = await uow.tasks.lock_scope(binding.scope)
    if (scope.principal_id, scope.policy_version, scope.authz_epoch) != (
        binding.principal_id, binding.policy_version, binding.authz_epoch,
    ):
        raise PolicyDenied("authorization_snapshot_changed")
    if (
        actor.scope != binding.scope or actor.authenticated_agent_registry_id != binding.agent_registry_id
        or task.scope != binding.scope or task.input_revision != binding.input_revision
        or task.status != TaskStatus.RUNNING or task.cancel_requested_at is not None
        or task.deadline <= datetime.now(UTC)
        or operation.get("state") != "COMPLETED" or operation.get("execution_state") != "QUIESCENT"
        or attempt.status != AttemptStatus.SUCCEEDED or attempt.operation_id != operation.get("id")
        or attempt.input_revision != task.input_revision or attempt.agent_registry_id != binding.agent_registry_id
        or attempt.kind not in {"planning", "synthesis", "hekate_reasoning", "synthesis_round2"}
    ):
        raise PolicyDenied("continuation_parent_is_not_an_accepted_current_HEKATE_result")
    current_agent = await uow.agents.lock_registry(binding.agent_registry_id)
    if (
        current_agent.kind != "hekate" or current_agent.persistence != "persistent"
        or current_agent.owner_scope != task.scope or current_agent.provider_id != binding.provider_agent_id
    ):
        raise PolicyDenied("continuation_parent_is_not_the_persistent_HEKATE")
    proposal_value = proposal.model_dump(mode="json")
    proposal_hash = canonical_json_hash(proposal_value)
    replay = await uow.deliberation.get_by_parent_proposal(
        task.id, str(attempt.operation_id), proposal_hash,
    )
    if replay is not None:
        if replay["request_hash"] != canonical_json_hash({
            "scope": str(binding.scope), "registry_id": str(binding.agent_registry_id),
            "task_id": str(task.id), "revision": task.input_revision,
            "parent_attempt_id": str(attempt.id), "parent_operation_id": str(attempt.operation_id),
            "parent_conclusion_id": str(conclusion_id), "proposal": proposal_value,
            "manifest": manifest,
        }):
            raise Conflict("continuation proposal identity changed on replay")
        return {
            "approved": True, "replayed": True, "request_hash": replay["request_hash"],
            "step_id": replay["id"], "step_ids": {str(replay["step_slot"]): replay["id"]},
        }

    fingerprint = canonical_json_hash({
        "task_id": str(task.id), "revision": task.input_revision,
        "next_action": proposal.next_action,
        "unresolved_issue": " ".join(proposal.unresolved_issue.split()).casefold(),
        "expected_information_gain": " ".join(proposal.expected_information_gain.split()).casefold(),
        "decision_impact": " ".join(proposal.decision_impact.split()).casefold(),
        "topic_id": manifest.get("topic_id"),
        "base_position_version": manifest.get("base_position_version"),
        "evidence": manifest.get("evidence", []),
    })
    if await uow.deliberation.get_by_work_fingerprint(task.id, fingerprint) is not None:
        return {"approved": False, "stop_reason": StopReason.NO_NEW_WORK.value}

    workflow = await uow.critic_workflows.get(task.id, lock=True)
    critic_registry_id = None
    review_history: list[dict[str, object]] = []
    if workflow is not None:
        review_sources: list[tuple[int, str]] = []
        if workflow.critic_conclusion_id is not None:
            review_sources.append((1, str(workflow.critic_conclusion_id)))
        for prior in await uow.deliberation.list_for_task(task.id):
            if prior["step_kind"] == "critic_review" and prior["conclusion_id"] and prior["state"] in {"RESULT_ACCEPTED", "COMPLETE"}:
                review_sources.append((int(prior["review_round"]), str(prior["conclusion_id"])))
        for review_round, review_id in sorted(set(review_sources)):
            prior_row = await uow.knowledge.get_conclusion(review_id)
            if prior_row is None or not prior_row["eligible"]:
                raise PolicyDenied("accepted Critic review history is unavailable")
            prior_capsule = ConclusionCapsule.model_validate_json(canonical_json(prior_row["capsule"]), strict=True)
            prior_dissent_ids = await uow.knowledge.get_dissent_for_conclusion(task.scope, DomainId(review_id))
            prior_dissent = await uow.knowledge.get_dissent_context(task.scope, tuple(prior_dissent_ids))
            review_history.append({
                "review_round": review_round, "conclusion_id": review_id,
                "conclusion": prior_capsule.model_dump(mode="json"),
                "dissent": [item.model_dump(mode="json") for item in prior_dissent],
            })
    if proposal.next_action == "hekate_reasoning":
        if task.counters.hekate_continuations >= limits.max_hekate_continuations:
            return {"approved": False, "stop_reason": StopReason.ROUND_LIMIT.value}
        if not await uow.tasks.increment_counter_if_below(
            task.id, "hekate_continuations", limits.max_hekate_continuations,
        ):
            return {"approved": False, "stop_reason": StopReason.ROUND_LIMIT.value}
        critic_registry_id = binding.agent_registry_id
        selected_config = hekate_config
        slot = "hekate_reasoning_1"
        plans = [("hekate_reasoning", slot, 0, "READY", None, hekate_config, binding.agent_registry_id)]
    else:
        if critic_config is None or workflow is None or workflow.critic_conclusion_id is None:
            raise PolicyDenied("additional_critic_review_requires_an_accepted_first_review")
        if task.counters.review_rounds < 1:
            raise PolicyDenied("additional_critic_review_requires_the_first_review")
        if task.counters.review_rounds >= limits.max_reviews:
            return {"approved": False, "stop_reason": StopReason.ROUND_LIMIT.value}
        critic = await uow.agents.lock_registry(workflow.critic_registry_id)
        if (
            critic.kind != "critic" or critic.persistence != "ephemeral" or critic.task_id != task.id
            or critic.owner_scope != task.scope or critic.provider_id is None
            or critic.intended_state in {"DELETING", "DELETE_PENDING", "DELETED", "FAILED"}
        ):
            raise PolicyDenied("the existing Task Critic is unavailable for another review")
        if await uow.agents.active_execution_hold(critic.registry_id, lock=True) is not None:
            raise PolicyDenied("the existing Critic has an unresolved execution")
        first_row = await uow.knowledge.get_conclusion(str(workflow.critic_conclusion_id))
        if first_row is None or not first_row["eligible"]:
            raise PolicyDenied("the first Critic Conclusion is not accepted")
        critic_registry_id = critic.registry_id
        plans = [
            ("critic_review", "critic_review_2", 2, "READY", None, critic_config, critic.registry_id),
            ("synthesis", "synthesis_2", 2, "WAITING_PARENT", "critic_review_2", hekate_config, binding.agent_registry_id),
        ]

    request_hash = canonical_json_hash({
        "scope": str(binding.scope), "registry_id": str(binding.agent_registry_id),
        "task_id": str(task.id), "revision": task.input_revision,
        "parent_attempt_id": str(attempt.id), "parent_operation_id": str(attempt.operation_id),
        "parent_conclusion_id": str(conclusion_id), "proposal": proposal_value,
        "manifest": manifest,
    })
    steps_by_slot: dict[str, str] = {}
    step_identities: dict[str, tuple[str, str, str]] = {}
    task_rows = await uow.deliberation.list_for_task(task.id)
    next_order = max((int(row["step_order"]) for row in task_rows), default=0) + 1
    for index, (kind, slot, review_round, state, previous_slot, profile_config, registry_id) in enumerate(plans):
        step_id = str(uuid5(NAMESPACE_URL, f"hekate:deliberation-step:{task.id}:{slot}"))
        operation_id = OperationId(str(uuid5(NAMESPACE_URL, f"hekate:deliberation-operation:{task.id}:{slot}")))
        attempt_id = AttemptId(str(uuid5(NAMESPACE_URL, f"hekate:deliberation-attempt:{task.id}:{slot}")))
        reservation_id = ReservationId(str(uuid5(NAMESPACE_URL, f"hekate:deliberation-reservation:{task.id}:{slot}")))
        operation_kind = "critic.review" if kind == "critic_review" else "hekate.reasoning" if kind == "hekate_reasoning" else "hekate.synthesis"
        step_request_hash = request_hash if index == 0 else canonical_json_hash({"request_hash": request_hash, "slot": slot})
        previous_step_id = steps_by_slot.get(previous_slot) if previous_slot else None
        parent_attempt_id = str(attempt.id)
        parent_operation_id = str(attempt.operation_id)
        parent_conclusion_value = str(conclusion_id)
        if previous_slot is not None:
            parent_attempt_id, parent_operation_id, _ = step_identities[previous_slot]
        context = {
            "unresolved_issue": proposal.unresolved_issue,
            "next_action": proposal.next_action,
            "expected_information_gain": proposal.expected_information_gain,
            "decision_impact": proposal.decision_impact,
            "parent_conclusion_id": str(conclusion_id),
            "candidate_conclusion_id": str(conclusion_id),
            "review_history": review_history,
            "approved_context_fingerprint": fingerprint,
        }
        value = {
            "id": step_id, "task_id": str(task.id), "owner_scope": str(task.scope),
            "input_revision": task.input_revision, "step_order": next_order + index,
            "step_kind": kind, "step_slot": slot, "review_round": review_round,
            "parent_attempt_id": parent_attempt_id, "parent_operation_id": parent_operation_id,
            "parent_conclusion_id": parent_conclusion_value, "previous_step_id": previous_step_id,
            "proposal": proposal_value, "proposal_hash": proposal_hash,
            "request_hash": step_request_hash, "work_fingerprint": fingerprint if index == 0 else None,
            "state": state, "attempt_id": str(attempt_id), "operation_id": str(operation_id),
            "reservation_id": str(reservation_id), "registry_id": str(registry_id),
            "profile": execution_config_snapshot(profile_config), "context": context,
        }
        stage_hash = uow.deliberation.stage_hash(value)
        await uow.delivery.claim_deferred_operation(
            operation_id, task.scope, task.id, operation_kind, stage_hash,
        )
        period = (task.created_at or datetime.now(UTC)).astimezone(UTC).strftime("%Y-%m-%d")
        await uow.budgets.reserve_operation(ReservationRequest(
            id=reservation_id, operation_id=operation_id, purpose="operation_envelope",
            amount=_reservation_amount(profile_config), task_id=task.id,
            task_account_id=f"task-budget:{task.id}", system_account_id=f"system-budget:{period}",
            pricing_version=profile_config.pricing_version, system_period_id=period,
        ))
        await uow.deliberation.insert(value)
        steps_by_slot[slot] = step_id
        step_identities[slot] = (str(attempt_id), str(operation_id), str(conclusion_id))
    if not await uow.tasks.mark_waiting_for_workflow(task.id, task.input_revision):
        raise PolicyDenied("Task changed before bounded continuation approval")
    await uow.delivery.append_audit({
        "owner_scope": str(task.scope), "task_id": str(task.id),
        "attempt_id": str(attempt.id), "operation_id": str(attempt.operation_id),
        "registry_id": str(binding.agent_registry_id), "event_kind": "deliberation.continuation_approved",
        "safe_payload": {"request_hash": request_hash, "next_action": proposal.next_action,
                         "step_ids": steps_by_slot, "work_fingerprint": fingerprint},
    })
    return {
        "approved": True, "replayed": False, "request_hash": request_hash,
        "step_id": steps_by_slot[plans[0][1]], "step_ids": steps_by_slot,
    }


async def _record_continuation_stop(
    factory: UowFactory, inbox_id: str, stop_reason: StopReason,
    clock: Callable[[], datetime],
) -> dict[str, object]:
    """Finish from the accepted assessment without another inference or Position effect."""
    async with factory() as uow:
        result = await uow.knowledge.lock_turn_result(inbox_id)
        if result is None:
            return {"inbox_id": inbox_id, "processed": False, "reason": "result_not_stored"}
        if result["processing_state"] in {"REJECTED", "LATE", "ACCEPTED"}:
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": True, "state": result["processing_state"]}
        operation = await uow.delivery.lock_operation(OperationId(result["operation_id"]))
        binding = _restore_binding(operation)
        scope_state = await uow.tasks.lock_scope_for_observation(binding.scope)
        task = await uow.tasks.lock_task(binding.task_id)
        attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        await uow.agents.lock_registry(binding.agent_registry_id)
        if operation["execution_state"] != "QUIESCENT":
            await uow.knowledge.update_turn_result(inbox_id, state="WAITING_EXECUTION", delay_seconds=1)
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": False, "pending": True}
        late = _late_reason(task, attempt, result, binding, clock())
        if late is not None:
            await uow.rollback()
            return await _record_late_after_rollback(factory, inbox_id, late, clock)
        authorization_failure = _authorization_failure(scope_state, binding)
        if authorization_failure is not None:
            await uow.rollback()
            return await _record_commit_failure(factory, inbox_id, authorization_failure, clock)
        manifest_row = await uow.knowledge.get_context_manifest(OperationId(result["operation_id"]))
        conclusion_row = await uow.knowledge.get_conclusion(result["conclusion_id"]) if result["conclusion_id"] else None
        if manifest_row is None or conclusion_row is None or not await manifest_references_current(
            uow, binding.scope, binding.authz_epoch, manifest_row["manifest"],
        ):
            await uow.rollback()
            return await _record_commit_failure(factory, inbox_id, "evidence_reference_unavailable", clock)
        conclusion = ConclusionCapsule.model_validate_json(canonical_json(conclusion_row["capsule"]), strict=True)
        response = {
            "operation_id": result["operation_id"], "attempt_id": result["attempt_id"],
            "registry_id": result["registry_id"], "source_inbox_id": inbox_id,
            "proposal": result["proposal"],
            "response_text": (
                f"Provisional assessment: {conclusion.assessment.statement} "
                f"Further work was not scheduled ({stop_reason.value})."
            ),
            "outcome": "PROVISIONAL_ANSWER", "stop_reason": stop_reason.value,
        }
        actor = ActorContext(
            principal_id=binding.principal_id, scope=binding.scope,
            authenticated_agent_registry_id=binding.agent_registry_id, task_id=binding.task_id,
            attempt_id=binding.attempt_id, input_revision=binding.input_revision,
            policy_version=binding.policy_version, authz_epoch=binding.authz_epoch,
            fence=binding.fence,
        )
        if not await complete_task(
            uow, actor, binding.task_id, binding.input_revision, response,
            successful=True, accepted_at=clock(),
        ):
            await uow.rollback()
            return await _record_late_after_rollback(factory, inbox_id, "task_changed_before_adoption", clock)
        await uow.knowledge.update_turn_result(inbox_id, state="ACCEPTED", rejection_reason=None)
        await uow.knowledge.set_conclusion_eligible(result["conclusion_id"], True, None)
        step = await uow.deliberation.get_by_operation(result["operation_id"], lock=True)
        if step is not None:
            await uow.deliberation.transition(step["id"], ("ADMITTED",), "COMPLETE", conclusion_id=result["conclusion_id"], stop_reason=stop_reason.value)
        workflow = await uow.critic_workflows.get(binding.task_id, lock=True)
        if workflow is not None and workflow.stage in {
            "REVIEW_ADMITTED", "SYNTHESIS_PENDING", "SYNTHESIS_ADMITTED", "FAILED",
        }:
            await _queue_retirement_in_uow(uow, workflow)
        await uow.delivery.append_audit({
            "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
            "attempt_id": str(binding.attempt_id), "operation_id": result["operation_id"],
            "registry_id": str(binding.agent_registry_id), "event_kind": "deliberation.stopped",
            "safe_payload": {"stop_reason": stop_reason.value, "outcome": "PROVISIONAL_ANSWER"},
        })
        await uow.commit()
        return {"inbox_id": inbox_id, "processed": True, "state": "ACCEPTED", "stop_reason": stop_reason.value}


async def apply_turn_result(
    factory: UowFactory, inbox_id: str, *, clock: Callable[[], datetime] | None = None,
    hekate_config: TaskExecutionConfig | None = None,
    critic_config: TaskExecutionConfig | None = None,
    deliberation_config: DeliberationConfig | None = None,
) -> dict[str, object]:
    current_time = clock or (lambda: datetime.now(UTC))
    async with factory() as uow:
        result = await uow.knowledge.lock_turn_result(inbox_id)
        if result is None:
            return {"inbox_id": inbox_id, "processed": False, "reason": "result_not_stored"}
        if result["processing_state"] in {"REJECTED", "LATE", "ACCEPTED"}:
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": True, "state": result["processing_state"]}
        operation = await uow.delivery.lock_operation(OperationId(result["operation_id"]))
        binding = _restore_binding(operation)
        current_scope = await uow.tasks.lock_scope_for_observation(binding.scope)
        task = await uow.tasks.lock_task(binding.task_id)
        attempt = await uow.tasks.get_attempt(binding.attempt_id, for_update=True)
        await uow.agents.lock_registry(binding.agent_registry_id)
        if operation["execution_state"] != "QUIESCENT":
            await uow.knowledge.update_turn_result(inbox_id, state="WAITING_EXECUTION", delay_seconds=1)
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": False, "pending": True}

        late_reason = _late_reason(task, attempt, result, binding, current_time())
        if late_reason is not None:
            if task.input_revision == binding.input_revision and task.scope == binding.scope:
                await converge_task_execution(uow, binding.task_id, binding.input_revision, now=current_time())
            await uow.knowledge.update_turn_result(inbox_id, state="LATE", rejection_reason=late_reason)
            if result["conclusion_id"]:
                await uow.knowledge.set_conclusion_eligible(result["conclusion_id"], False, late_reason)
            await uow.delivery.append_audit({
                "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id), "operation_id": str(result["operation_id"]),
                "registry_id": str(binding.agent_registry_id), "event_kind": "business_result.late",
                "safe_payload": {"output_hash": result["output_hash"], "rejection_reason": late_reason},
            })
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": True, "state": "LATE"}

        authorization_failure = _authorization_failure(current_scope, binding)
        if authorization_failure is not None:
            await uow.rollback()
            return await _record_commit_failure(factory, inbox_id, authorization_failure, current_time)

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
        conclusion_capsule: ConclusionCapsule | None = None
        critic_review = attempt.kind == "critic_review"
        if successful and reason is None and not critic_review and not isinstance(proposal, dict):
            successful = False
            reason = "proposal_missing"
        if successful and reason is None and not critic_review and isinstance(proposal, dict):
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
        if successful and reason is None and critic_review:
            step = await uow.deliberation.get_by_operation(result["operation_id"], lock=True)
            workflow = await uow.critic_workflows.get(binding.task_id, lock=True)
            if step is not None:
                if (
                    step["step_kind"] != "critic_review" or step["state"] != "ADMITTED"
                    or step["attempt_id"] != str(binding.attempt_id)
                    or step["registry_id"] != str(binding.agent_registry_id)
                    or workflow is None or workflow.critic_registry_id != binding.agent_registry_id
                    or workflow.stage != "SYNTHESIS_ADMITTED" or conclusion_capsule is None
                ):
                    successful, reason = False, "critic_workflow_binding_mismatch"
                else:
                    await uow.knowledge.ensure_dissent(binding.scope, DomainId(result["conclusion_id"]), conclusion_capsule.objections)
                    await uow.knowledge.set_conclusion_eligible(result["conclusion_id"], True, None)
                    await uow.deliberation.mark_result_accepted(step["id"], result["conclusion_id"])
                    downstream = await uow.deliberation.list_for_task(binding.task_id)
                    for child in downstream:
                        if child["previous_step_id"] == step["id"] and child["state"] == "WAITING_PARENT":
                            await uow.deliberation.transition(
                                child["id"], ("WAITING_PARENT",), "READY",
                                parent_conclusion_id=result["conclusion_id"],
                            )
                    if not await uow.tasks.mark_waiting_for_workflow(binding.task_id, binding.input_revision):
                        await uow.rollback()
                        return await _record_late_after_rollback(
                            factory, inbox_id, "task_changed_after_deliberation_review", current_time,
                        )
                    await uow.knowledge.update_turn_result(inbox_id, state="ACCEPTED", rejection_reason=None)
                    await uow.delivery.append_audit({
                        "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
                        "attempt_id": str(binding.attempt_id), "operation_id": str(result["operation_id"]),
                        "registry_id": str(binding.agent_registry_id), "event_kind": "deliberation.review_accepted",
                        "safe_payload": {"conclusion_id": result["conclusion_id"], "review_round": step["review_round"],
                                         "dissent_count": len(conclusion_capsule.objections)},
                    })
                    await uow.commit()
                    return {"inbox_id": inbox_id, "processed": True, "state": "SYNTHESIS_ROUND_PENDING"}
            elif (
                workflow is None or workflow.stage != "REVIEW_ADMITTED"
                or workflow.review_attempt_id != binding.attempt_id
                or workflow.review_operation_id != OperationId(result["operation_id"])
                or workflow.critic_registry_id != binding.agent_registry_id
                or conclusion_capsule is None
            ):
                successful, reason = False, "critic_workflow_binding_mismatch"
            else:
                await uow.knowledge.ensure_dissent(binding.scope, DomainId(result["conclusion_id"]), conclusion_capsule.objections)
                await uow.knowledge.set_conclusion_eligible(result["conclusion_id"], True, None)
                await uow.critic_workflows.transition(
                    binding.task_id, ("REVIEW_ADMITTED",), "SYNTHESIS_PENDING",
                    critic_conclusion_id=DomainId(result["conclusion_id"]),
                )
                if not await uow.tasks.mark_waiting_for_workflow(binding.task_id, binding.input_revision):
                    await uow.rollback()
                    return await _record_late_after_rollback(
                        factory, inbox_id, "task_changed_after_critic_review", current_time,
                    )
                await uow.knowledge.update_turn_result(inbox_id, state="ACCEPTED", rejection_reason=None)
                await uow.delivery.append_audit({
                    "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
                    "attempt_id": str(binding.attempt_id), "operation_id": str(result["operation_id"]),
                    "registry_id": str(binding.agent_registry_id), "event_kind": "critic.result_accepted",
                    "safe_payload": {
                        "output_hash": result["output_hash"],
                        "conclusion_id": result["conclusion_id"],
                        "dissent_count": len(conclusion_capsule.objections),
                    },
                })
                await uow.commit()
                return {"inbox_id": inbox_id, "processed": True, "state": "SYNTHESIS_PENDING"}
        if successful and reason is None and isinstance(proposal_model, SpawnProposal):
            if attempt.kind != "planning" or hekate_config is None or critic_config is None:
                await uow.rollback()
                failure = "critic_profile_unavailable" if critic_config is None else "spawn_not_allowed_for_attempt"
                return await _record_commit_failure(factory, inbox_id, failure, current_time)
            try:
                workflow = await request_critic(
                    uow, ActorContext(
                        principal_id=binding.principal_id, scope=binding.scope,
                        authenticated_agent_registry_id=binding.agent_registry_id,
                        task_id=binding.task_id, attempt_id=binding.attempt_id,
                        input_revision=binding.input_revision, policy_version=binding.policy_version,
                        authz_epoch=binding.authz_epoch, fence=binding.fence,
                    ), proposal_model, operation, attempt,
                    DomainId(result["conclusion_id"]), hekate_config, critic_config,
                )
            except BudgetDenied:
                await uow.rollback()
                return await _record_commit_failure(factory, inbox_id, "budget_critic_and_synthesis_unavailable", current_time)
            except PolicyDenied as error:
                await uow.rollback()
                failure = str(error) or "critic_spawn_policy_rejected"
                return await _record_commit_failure(factory, inbox_id, failure, current_time)
            await uow.knowledge.set_conclusion_eligible(result["conclusion_id"], True, None)
            await uow.knowledge.update_turn_result(inbox_id, state="ACCEPTED", rejection_reason=None)
            await uow.delivery.append_audit({
                "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id), "operation_id": str(result["operation_id"]),
                "registry_id": str(binding.agent_registry_id), "event_kind": "planning.spawn_accepted",
                "safe_payload": {
                    "request_hash": workflow.spawn_request_hash,
                    "critic_registry_id": str(workflow.critic_registry_id),
                    "create_operation_id": str(workflow.create_operation_id),
                },
            })
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": True, "state": "CRITIC_CREATE_PENDING"}

        if successful and reason is None and isinstance(proposal_model, ContinuationProposal):
            if deliberation_config is None or not deliberation_config.enabled:
                await uow.rollback()
                return await _record_commit_failure(
                    factory, inbox_id, "continuation_not_allowed", current_time,
                )
            if conclusion_capsule is None:
                await uow.rollback()
                return await _record_commit_failure(factory, inbox_id, "continuation_parent_missing", current_time)
            actor = ActorContext(
                principal_id=binding.principal_id, scope=binding.scope,
                authenticated_agent_registry_id=binding.agent_registry_id,
                task_id=binding.task_id, attempt_id=binding.attempt_id,
                input_revision=binding.input_revision, policy_version=binding.policy_version,
                authz_epoch=binding.authz_epoch, fence=binding.fence,
            )
            try:
                approval = await _approve_continuation_in_uow(
                    uow, actor, task, operation, attempt, DomainId(result["conclusion_id"]),
                    proposal_model, manifest, hekate_config, critic_config, deliberation_config,
                )
            except BudgetDenied:
                await uow.rollback()
                return await _record_continuation_stop(factory, inbox_id, StopReason.BUDGET, current_time)
            except (PolicyDenied, Conflict, StaleInput) as error:
                await uow.rollback()
                return await _record_commit_failure(
                    factory, inbox_id, str(error) or "continuation_policy_rejected", current_time,
                )
            if not approval.get("approved"):
                stop_reason = StopReason(str(approval["stop_reason"]))
                await uow.rollback()
                return await _record_continuation_stop(factory, inbox_id, stop_reason, current_time)
            await uow.knowledge.set_conclusion_eligible(result["conclusion_id"], True, None)
            await uow.knowledge.update_turn_result(inbox_id, state="ACCEPTED", rejection_reason=None)
            await uow.delivery.append_audit({
                "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id), "operation_id": str(result["operation_id"]),
                "registry_id": str(binding.agent_registry_id), "event_kind": "deliberation.step_approved",
                "safe_payload": {
                    "request_hash": approval.get("request_hash"), "step_id": approval.get("step_id"),
                    "step_ids": approval.get("step_ids", {}), "replayed": approval.get("replayed", False),
                },
            })
            await uow.commit()
            return {"inbox_id": inbox_id, "processed": True, "state": "CONTINUATION_PENDING"}

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
                        manifest=manifest, now=current_time(),
                    )
                except (PolicyDenied, Conflict) as error:
                    failure = str(error) or "commit_policy_rejected"
                    await uow.rollback()
                    return await _record_commit_failure(factory, inbox_id, failure, current_time)
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
            outcome = "FAILED"
            stop_reason = _failure_stop_reason(safe_reason)
            successful = False

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
        try:
            adopted = await complete_task(
                uow, actor, binding.task_id, binding.input_revision, response,
                successful=successful, accepted_at=current_time(),
            )
        except PolicyDenied:
            await uow.rollback()
            return await _record_commit_failure(
                factory, inbox_id, "authorization_snapshot_changed", current_time,
            )
        except (Conflict, StaleInput):
            await uow.rollback()
            return await _record_late_after_rollback(factory, inbox_id, "task_changed_before_adoption", current_time)
        if not adopted:
            await uow.rollback()
            return await _record_late_after_rollback(factory, inbox_id, "task_changed_before_adoption", current_time)
        await uow.knowledge.update_turn_result(inbox_id, state="ACCEPTED" if successful else "REJECTED", rejection_reason=None if successful else reason or "execution_failed")
        if result["conclusion_id"]:
            await uow.knowledge.set_conclusion_eligible(result["conclusion_id"], successful and reason is None, None if successful and reason is None else reason or "execution_failed")
        dynamic_step = await uow.deliberation.get_by_operation(result["operation_id"], lock=True)
        if dynamic_step is not None and dynamic_step["state"] == "ADMITTED":
            if successful:
                await uow.deliberation.transition(
                    dynamic_step["id"], ("ADMITTED",), "COMPLETE",
                    conclusion_id=result["conclusion_id"],
                )
            else:
                await uow.deliberation.transition(
                    dynamic_step["id"], ("ADMITTED",), "STOPPED",
                    conclusion_id=result["conclusion_id"], stop_reason=stop_reason,
                )
        workflow = await uow.critic_workflows.get(binding.task_id, lock=True)
        if workflow is not None and workflow.stage in {
            "REVIEW_ADMITTED", "SYNTHESIS_PENDING", "SYNTHESIS_ADMITTED", "FAILED",
        }:
            await _queue_retirement_in_uow(uow, workflow)
        await uow.delivery.append_audit({
            "owner_scope": str(binding.scope), "task_id": str(binding.task_id),
            "attempt_id": str(binding.attempt_id), "operation_id": str(result["operation_id"]),
            "registry_id": str(binding.agent_registry_id),
            "event_kind": "task.result_accepted" if successful else "task.result_failed",
            "safe_payload": {"output_hash": result["output_hash"], "outcome": outcome, "stop_reason": stop_reason},
        })
        await uow.commit()
        return {"inbox_id": inbox_id, "processed": True, "state": "ACCEPTED" if successful else "REJECTED"}


async def process_pending_results(
    factory: UowFactory, limit: int = 100, *, hekate_config: TaskExecutionConfig | None = None,
    critic_config: TaskExecutionConfig | None = None,
    deliberation_config: DeliberationConfig | None = None,
) -> int:
    async with factory() as uow:
        inbox_ids = await uow.tasks.list_pending_turn_results(limit)
        await uow.commit()
    processed = 0
    for inbox_id in inbox_ids:
        result = await apply_turn_result(
            factory, inbox_id, hekate_config=hekate_config, critic_config=critic_config,
            deliberation_config=deliberation_config,
        )
        processed += int(result.get("processed", False))
    return processed
