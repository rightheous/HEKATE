from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4
from hekate.domain.contracts import canonical_json_hash
from hekate.domain.errors import PolicyDenied, StaleInput
from hekate.domain.models import (
    AccountSnapshot, CancellationReceipt, InputChange, RevisionReceipt, Task,
    TaskExecutionConfig, TaskCounters,
    TaskReceipt, TaskView, UserMessage,
)
from hekate.domain.types import ActorContext, OperationId, ReservationId, Revision, ScopeId, StopReason, TaskId, TaskStatus
from hekate.ports.store import UnitOfWork, UowFactory


MAX_QUESTION_BYTES = 65_536


def _normalized_question(message: UserMessage) -> str:
    value = unicodedata.normalize("NFC", message.text.replace("\r\n", "\n").replace("\r", "\n"))
    if not value.strip():
        raise ValueError("question must not be empty")
    if len(value.encode("utf-8")) > MAX_QUESTION_BYTES:
        raise ValueError(f"question exceeds {MAX_QUESTION_BYTES} UTF-8 bytes")
    return value


def _validate_submission_config(config: TaskExecutionConfig) -> None:
    if not isinstance(config.task_budget_usd, Decimal) or not config.task_budget_usd.is_finite() or config.task_budget_usd <= 0:
        raise ValueError("task budget must be an explicit positive Decimal")
    if not isinstance(config.system_daily_budget_usd, Decimal) or not config.system_daily_budget_usd.is_finite() or config.system_daily_budget_usd <= 0:
        raise ValueError("system daily budget must be an explicit positive Decimal")
    if type(config.deadline_seconds) is not int or not 1 <= config.deadline_seconds <= 86_400:
        raise ValueError("task deadline must be configured between 1 and 86400 seconds")
    if not all((config.profile_id, config.letta_model, config.model, config.pricing_version)):
        raise ValueError("a fixed HEKATE model and pricing profile are required")
    if type(config.max_input_tokens) is not int or config.max_input_tokens < 1:
        raise ValueError("max_input_tokens must be explicitly configured")
    if type(config.max_output_tokens) is not int or config.max_output_tokens < 1:
        raise ValueError("max_output_tokens must be explicitly configured")
    if type(config.max_compaction_calls) is not int or config.max_compaction_calls < 0:
        raise ValueError("max_compaction_calls must be explicitly configured and nonnegative")


async def submit(
    factory: UowFactory,
    actor: ActorContext,
    message: UserMessage,
    request_key: str,
    config: TaskExecutionConfig,
) -> TaskReceipt:
    question = _normalized_question(message)
    _validate_submission_config(config)
    if not isinstance(request_key, str) or not request_key or len(request_key) > 128 or any(not char.isprintable() or char.isspace() for char in request_key):
        raise ValueError("request key must be 1–128 printable non-space characters")
    evidence_refs = tuple(sorted(set(message.evidence_refs)))
    request_hash = canonical_json_hash({
        "question": question,
        "topic_id": str(message.topic_id) if message.topic_id is not None else None,
        "evidence_refs": [str(ref) for ref in evidence_refs],
    })
    now = datetime.now(UTC)
    period_id = now.strftime("%Y-%m-%d")
    async with factory() as uow:
        authorization = await uow.tasks.lock_scope(actor.scope)
        if (authorization.principal_id, authorization.policy_version, authorization.authz_epoch) != (
            actor.principal_id, actor.policy_version, actor.authz_epoch,
        ):
            raise PolicyDenied("authorization snapshot changed")
        submission = await uow.tasks.claim_submission(actor.scope, request_key, request_hash)
        if submission["receipt"] is not None:
            await uow.commit()
            return submission["receipt"]

        task_id = TaskId(str(uuid4()))
        task = Task(
            id=task_id,
            scope=ScopeId(actor.scope),
            question=question,
            input_revision=1,
            constraints_hash=canonical_json_hash({}),
            status=TaskStatus.QUEUED,
            topic_id=message.topic_id,
            evidence_refs=evidence_refs,
            deadline=now + timedelta(seconds=config.deadline_seconds),
            max_provider_calls=config.max_generations_per_task,
            created_at=now,
            counters=TaskCounters(),
        )
        await uow.tasks.insert_task(task, {})
        await uow.budgets.ensure_account(AccountSnapshot(
            id=f"task-budget:{task_id}", scope_kind="TASK", scope_ref=str(task_id), period_id="lifetime",
            limit_amount=config.task_budget_usd, spent_amount=Decimal(0), held_amount=Decimal(0),
        ))
        await uow.budgets.ensure_account(AccountSnapshot(
            id=f"system-budget:{period_id}", scope_kind="SYSTEM", scope_ref="hekate", period_id=period_id,
            limit_amount=config.system_daily_budget_usd, spent_amount=Decimal(0), held_amount=Decimal(0),
        ))
        receipt = {
            "schema_version": "1",
            "task_id": str(task_id),
            "input_revision": 1,
            "state": TaskStatus.QUEUED.value,
            "request_key": request_key,
        }
        await uow.tasks.complete_submission(actor.scope, request_key, task_id, receipt)
        await uow.delivery.append_audit({
            "owner_scope": str(actor.scope), "task_id": str(task_id),
            "event_kind": "task.submitted",
            "safe_payload": {"request_hash": request_hash, "request_key_hash": canonical_json_hash(request_key)},
        })
        await uow.commit()
        return receipt


async def get_task(factory: UowFactory, actor: ActorContext, task_id: TaskId) -> TaskView:
    async with factory() as uow:
        authorization = await uow.tasks.lock_scope(actor.scope)
        if (authorization.principal_id, authorization.policy_version, authorization.authz_epoch) != (
            actor.principal_id, actor.policy_version, actor.authz_epoch,
        ):
            raise PolicyDenied("authorization snapshot changed")
        task = await uow.tasks.lock_task(task_id)
        if task.scope != actor.scope or (actor.task_id is not None and actor.task_id != task_id):
            raise PolicyDenied("task is outside the actor scope")
        response = await uow.tasks.get_task_response(task_id)
        cost = await uow.tasks.get_task_cost_status(task_id)
        deliberation_rows = await uow.deliberation.list_for_task(task_id)
        deliberation_operations = await uow.delivery.get_operation_statuses(tuple(
            OperationId(row["operation_id"]) for row in deliberation_rows
        )) if deliberation_rows else {}
        deliberation_steps = []
        for row in deliberation_rows:
            reservation = await uow.budgets.get_reservation(ReservationId(row["reservation_id"]))
            deliberation_steps.append({
                "step_id": row["id"], "kind": row["step_kind"], "round": row["review_round"],
                "state": row["state"], "attempt_id": row["attempt_id"],
                "operation_id": row["operation_id"],
                "operation": deliberation_operations.get(row["operation_id"]),
                "reservation": ({
                    "state": reservation["status"], "reserved_usd": str(reservation["amount"]),
                    "pricing_version": reservation["pricing_version"],
                } if reservation else None),
                "conclusion_id": row["conclusion_id"], "stop_reason": row["stop_reason"],
            })
        workflow = await uow.critic_workflows.get(task_id)
        critic_review = None
        if workflow is not None:
            operation_ids = [
                workflow.create_operation_id, workflow.review_operation_id,
                workflow.synthesis_operation_id,
            ]
            if workflow.delete_operation_id is not None:
                operation_ids.append(workflow.delete_operation_id)
            operation_rows = await uow.delivery.get_operation_statuses(operation_ids)
            reservations = {}
            for label, reservation_id in (
                ("review", workflow.review_reservation_id),
                ("synthesis", workflow.synthesis_reservation_id),
            ):
                reservation = await uow.budgets.get_reservation(reservation_id)
                reservations[label] = ({
                    "state": reservation["status"],
                    "reserved_usd": str(reservation["amount"]),
                    "pricing_version": reservation["pricing_version"],
                } if reservation else None)
            critic_agent = await uow.agents.lock_registry(workflow.critic_registry_id)
            critic_review = {
                "stage": workflow.stage,
                "critic_registry_id": str(workflow.critic_registry_id),
                "critic_agent_state": critic_agent.intended_state,
                "critic_create": operation_rows.get(str(workflow.create_operation_id)),
                "review": {
                    "attempt_id": str(workflow.review_attempt_id),
                    "operation": operation_rows.get(str(workflow.review_operation_id)),
                    "reservation": reservations["review"],
                },
                "synthesis": {
                    "attempt_id": str(workflow.synthesis_attempt_id),
                    "operation": operation_rows.get(str(workflow.synthesis_operation_id)),
                    "reservation": reservations["synthesis"],
                },
                "conclusion_id": str(workflow.critic_conclusion_id) if workflow.critic_conclusion_id else None,
                "delete_operation": operation_rows.get(str(workflow.delete_operation_id)) if workflow.delete_operation_id else None,
            }
        await uow.commit()
        response_text = response["response_text"] if response else None
        result_status = "accepted" if response and response["outcome"] != "FAILED" else "failed" if task.status == TaskStatus.FAILED else "pending"
        if response is None and task.status == TaskStatus.FAILED:
            response_text = f"HEKATE could not complete this request ({task.stop_reason or 'ERROR'})."
        return {
            "task_id": str(task.id),
            "input_revision": task.input_revision,
            "state": task.status.value,
            "outcome": task.outcome,
            "stop_reason": task.stop_reason,
            "deadline": task.deadline.isoformat(),
            "response": response_text,
            "result_status": result_status,
            "critic_review": critic_review,
            "deliberation": {
                "hekate_continuations": task.counters.hekate_continuations,
                "review_rounds": task.counters.review_rounds,
                "steps": deliberation_steps,
            },
            **cost,
        }


async def revise(
    factory: UowFactory,
    actor: ActorContext,
    task_id: TaskId,
    expected_revision: Revision,
    change: InputChange,
) -> Task:
    if change.expected_revision != expected_revision:
        raise StaleInput("input change revision does not match request")
    async with factory() as uow:
        authorization = await uow.tasks.lock_scope(actor.scope)
        if (
            authorization.principal_id != actor.principal_id
            or authorization.policy_version != actor.policy_version
            or authorization.authz_epoch != actor.authz_epoch
        ):
            raise PolicyDenied("authorization snapshot changed")
        task = await uow.tasks.lock_task(task_id)
        if task.scope != actor.scope or (actor.task_id is not None and actor.task_id != task_id):
            raise PolicyDenied("task is outside the actor scope")
        if actor.input_revision is not None and actor.input_revision != expected_revision:
            raise StaleInput("actor input revision is stale")
        updated = await uow.tasks.revise_input(
            task_id,
            expected_revision,
            change,
            canonical_json_hash(change.constraints),
            accepted_at=datetime.now(UTC),
        )
        await uow.delivery.append_audit({
            "owner_scope": str(actor.scope),
            "task_id": str(task_id),
            "event_kind": "task.input_revised",
            "safe_payload": {"from_revision": expected_revision, "to_revision": updated.input_revision, "constraints_hash": updated.constraints_hash},
        })
        await uow.commit()
        return updated


async def cancel(
    factory: UowFactory,
    actor: ActorContext,
    task_id: TaskId,
    reason: StopReason,
) -> CancellationReceipt:
    async with factory() as uow:
        authorization = await uow.tasks.lock_scope(actor.scope)
        if (
            authorization.principal_id != actor.principal_id
            or authorization.policy_version != actor.policy_version
            or authorization.authz_epoch != actor.authz_epoch
        ):
            raise PolicyDenied("authorization snapshot changed")
        task = await uow.tasks.lock_task(task_id)
        if task.scope != actor.scope or (actor.task_id is not None and actor.task_id != task_id):
            raise PolicyDenied("task is outside the actor scope")
        if actor.input_revision is not None and actor.input_revision != task.input_revision:
            raise StaleInput("actor input revision is stale")
        cancelled = await uow.tasks.request_cancel(task_id, reason)
        resolved = await converge_task_execution(uow, task_id, task.input_revision)
        if task.status not in {TaskStatus.STOPPING, TaskStatus.CANCELLED, TaskStatus.COMPLETED, TaskStatus.FAILED}:
            await uow.delivery.append_audit({
                "owner_scope": str(actor.scope),
                "task_id": str(task_id),
                "event_kind": "task.cancel_requested",
                "safe_payload": {"reason": reason.value, "state": cancelled.status.value},
            })
        await uow.commit()
        return {
            "task_id": str(task_id),
            "state": resolved.status.value if resolved is not None else cancelled.status.value,
            "stop_reason": resolved.stop_reason if resolved is not None else cancelled.stop_reason,
        }


async def converge_task_execution(
    uow: UnitOfWork, task_id: TaskId, revision: Revision, *, now: datetime | None = None,
) -> Task | None:
    operations = await uow.delivery.task_execution_states(task_id)
    if not operations or any(
        operation["execution_state"] != "QUIESCENT" or operation["unconfirmed_calls"]
        for operation in operations
    ):
        return None
    resolved = await uow.tasks.resolve_task_after_execution(task_id, revision, now=now)
    if resolved is not None:
        await uow.delivery.append_audit({
            "owner_scope": str(resolved.scope), "task_id": str(task_id),
            "event_kind": "task.execution_cancelled" if resolved.status == TaskStatus.CANCELLED else "task.execution_deadline_failed",
            "safe_payload": {"reason": resolved.stop_reason, "input_revision": resolved.input_revision},
        })
    return resolved


async def process_pending_execution_tasks(factory: UowFactory, limit: int = 100) -> int:
    async with factory() as uow:
        candidates = await uow.tasks.list_execution_terminal_candidates(limit)
        await uow.commit()
    processed = 0
    for candidate in candidates:
        task_id = TaskId(candidate["id"])
        scope = ScopeId(candidate["owner_scope"])
        async with factory() as uow:
            await uow.tasks.lock_scope_for_observation(scope)
            resolved = await converge_task_execution(uow, task_id, candidate["input_revision"])
            if resolved is not None:
                processed += 1
            await uow.commit()
    return processed


async def complete(
    uow: UnitOfWork,
    actor: ActorContext,
    task_id: TaskId,
    expected_revision: Revision,
    response: Mapping[str, object],
    *,
    successful: bool,
    accepted_at: datetime | None = None,
) -> bool:
    """Persist a scoped response and terminal Task state in the caller's UoW."""
    authorization = await uow.tasks.lock_scope(actor.scope)
    if (authorization.principal_id, authorization.policy_version, authorization.authz_epoch) != (
        actor.principal_id, actor.policy_version, actor.authz_epoch,
    ):
        raise PolicyDenied("authorization snapshot changed")
    task = await uow.tasks.lock_task(task_id)
    if task.scope != actor.scope or (actor.task_id is not None and actor.task_id != task_id):
        raise PolicyDenied("task is outside the actor scope")
    if actor.input_revision is not None and actor.input_revision != expected_revision:
        raise StaleInput("actor input revision is stale")
    if task.input_revision != expected_revision:
        return False
    return await uow.tasks.finalize_task_response(
        task_id, expected_revision, response, successful=successful, accepted_at=accepted_at,
    )
