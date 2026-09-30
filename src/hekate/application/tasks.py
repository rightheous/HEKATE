from __future__ import annotations

from datetime import UTC, datetime
from hekate.domain.contracts import canonical_json_hash
from hekate.domain.errors import PolicyDenied, StaleInput
from hekate.domain.models import (
    CancellationReceipt, InputChange, ResponseRef, RevisionReceipt, Task,
    TaskReceipt, TaskView, UserMessage,
)
from hekate.domain.types import ActorContext, Revision, StopReason, TaskId, TaskStatus
from hekate.ports.store import UowFactory


async def submit(actor: ActorContext, message: UserMessage, request_key: str) -> TaskReceipt:
    raise NotImplementedError


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
    task_id: TaskId, outcome: str, reason: StopReason, response: ResponseRef
) -> TaskView:
    raise NotImplementedError
