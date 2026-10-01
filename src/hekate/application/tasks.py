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
from hekate.domain.types import ActorContext, Revision, ScopeId, StopReason, TaskId, TaskStatus
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
    request_hash = canonical_json_hash({"question": question})
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
            deadline=now + timedelta(seconds=config.deadline_seconds),
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
            "state": cancelled.status.value,
            "stop_reason": cancelled.stop_reason,
        }


async def complete(
    uow: UnitOfWork,
    actor: ActorContext,
    task_id: TaskId,
    expected_revision: Revision,
    response: Mapping[str, object],
    *,
    successful: bool,
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
        task_id, expected_revision, response, successful=successful,
    )
