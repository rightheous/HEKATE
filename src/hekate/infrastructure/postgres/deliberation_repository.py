from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

from sqlalchemy import exists, insert, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from hekate.domain.errors import Conflict, StaleInput
from hekate.domain.types import ScopeId, TaskId

from . import tables
from .common import aware_now, json_value


class PostgresDeliberationRepository:
    """Persistence for the bounded Phase 5B step ledger."""

    def __init__(self, connection: AsyncSession) -> None:
        self.connection = connection

    async def insert(self, value: Mapping[str, object]) -> None:
        await self.connection.execute(insert(tables.deliberation_steps).values(
            **{**dict(value), "proposal": json_value(value["proposal"]),
               "profile": json_value(value["profile"]), "context": json_value(value.get("context", {})),
               "created_at": aware_now(), "updated_at": aware_now()},
        ))

    async def get(self, step_id: str, *, lock: bool = False) -> Mapping[str, object] | None:
        query = select(tables.deliberation_steps).where(tables.deliberation_steps.c.id == step_id)
        if lock:
            query = query.with_for_update()
        row = (await self.connection.execute(query)).mappings().one_or_none()
        return dict(row) if row else None

    async def get_by_operation(self, operation_id: str, *, lock: bool = False) -> Mapping[str, object] | None:
        query = select(tables.deliberation_steps).where(tables.deliberation_steps.c.operation_id == operation_id)
        if lock:
            query = query.with_for_update()
        row = (await self.connection.execute(query)).mappings().one_or_none()
        return dict(row) if row else None

    async def get_by_parent_proposal(
        self, task_id: TaskId, parent_operation_id: str, proposal_hash: str, *, lock: bool = False,
    ) -> Mapping[str, object] | None:
        query = select(tables.deliberation_steps).where(
            tables.deliberation_steps.c.task_id == task_id,
            tables.deliberation_steps.c.parent_operation_id == parent_operation_id,
            tables.deliberation_steps.c.proposal_hash == proposal_hash,
        )
        if lock:
            query = query.with_for_update()
        row = (await self.connection.execute(query)).mappings().one_or_none()
        return dict(row) if row else None

    async def get_by_work_fingerprint(self, task_id: TaskId, fingerprint: str) -> Mapping[str, object] | None:
        row = (await self.connection.execute(select(tables.deliberation_steps).where(
            tables.deliberation_steps.c.task_id == task_id,
            tables.deliberation_steps.c.work_fingerprint == fingerprint,
        ))).mappings().one_or_none()
        return dict(row) if row else None

    async def list_ready(self, scope: ScopeId, limit: int = 20) -> Sequence[Mapping[str, object]]:
        if not 1 <= limit <= 100:
            return ()
        rows = (await self.connection.execute(select(tables.deliberation_steps).where(
            tables.deliberation_steps.c.owner_scope == scope,
            tables.deliberation_steps.c.state == "READY",
        ).order_by(tables.deliberation_steps.c.created_at, tables.deliberation_steps.c.id).limit(limit))).mappings().all()
        return tuple(dict(row) for row in rows)

    async def list_maintenance_candidates(self, limit: int = 100) -> Sequence[Mapping[str, object]]:
        """Find standalone steps whose Task state makes further admission impossible."""
        if not 1 <= limit <= 1_000:
            raise ValueError("deliberation maintenance batch must be between 1 and 1000")
        now = aware_now()
        has_critic_workflow = exists(select(1).where(
            tables.critic_workflows.c.task_id == tables.deliberation_steps.c.task_id,
        ))
        rows = (await self.connection.execute(select(
            *tables.deliberation_steps.c,
            tables.tasks.c.status.label("task_status"),
            tables.tasks.c.input_revision.label("task_input_revision"),
            tables.tasks.c.cancel_requested_at.label("task_cancel_requested_at"),
            tables.tasks.c.deadline.label("task_deadline"),
        ).select_from(tables.deliberation_steps.join(
            tables.tasks, tables.tasks.c.id == tables.deliberation_steps.c.task_id,
        )).where(
            tables.deliberation_steps.c.state.in_(("READY", "WAITING_PARENT", "ADMITTED", "RESULT_ACCEPTED", "STOPPED")),
            tables.deliberation_steps.c.maintenance_completed_at.is_(None),
            or_(
                tables.deliberation_steps.c.maintenance_retry_after.is_(None),
                tables.deliberation_steps.c.maintenance_retry_after <= now,
            ),
            ~has_critic_workflow,
            or_(
                tables.tasks.c.status.in_(("CANCELLED", "FAILED", "COMPLETED")),
                tables.tasks.c.cancel_requested_at.is_not(None),
                tables.tasks.c.deadline <= now,
                tables.deliberation_steps.c.input_revision < tables.tasks.c.input_revision,
            ),
        ).order_by(
            # New, never-deferred work gets a turn before an UNKNOWN whose
            # backoff has expired. When all candidates have been seen, the
            # oldest retry timestamp moves forward as each batch is deferred.
            tables.deliberation_steps.c.maintenance_retry_after.asc().nulls_first(),
            tables.tasks.c.deadline, tables.tasks.c.id,
            tables.deliberation_steps.c.created_at, tables.deliberation_steps.c.id,
        ).limit(limit))).mappings().all()
        return tuple(dict(row) for row in rows)

    async def mark_maintenance_complete(self, step: Mapping[str, object]) -> bool:
        """Record verified cleanup in the same transaction as its effects."""
        result = await self.connection.execute(update(tables.deliberation_steps).where(
            tables.deliberation_steps.c.id == step["id"],
            tables.deliberation_steps.c.task_id == step["task_id"],
            tables.deliberation_steps.c.owner_scope == step["owner_scope"],
            tables.deliberation_steps.c.input_revision == step["input_revision"],
            tables.deliberation_steps.c.operation_id == step["operation_id"],
            tables.deliberation_steps.c.parent_operation_id == step["parent_operation_id"],
            tables.deliberation_steps.c.state == "STOPPED",
            tables.deliberation_steps.c.maintenance_completed_at.is_(None),
        ).values(
            maintenance_completed_at=aware_now(),
            maintenance_retry_after=None,
        ))
        if result.rowcount == 1:
            return True
        current = await self.get(str(step["id"]), lock=True)
        if (
            current is not None
            and current["task_id"] == step["task_id"]
            and current["owner_scope"] == step["owner_scope"]
            and current["input_revision"] == step["input_revision"]
            and current["operation_id"] == step["operation_id"]
            and current["parent_operation_id"] == step["parent_operation_id"]
            and current["maintenance_completed_at"] is not None
        ):
            return False
        raise StaleInput("deliberation maintenance identity changed before completion")

    async def defer_maintenance_retry(
        self, step: Mapping[str, object], retry_after: datetime,
    ) -> bool:
        """Temporarily move an unconfirmed cleanup behind due candidates."""
        result = await self.connection.execute(update(tables.deliberation_steps).where(
            tables.deliberation_steps.c.id == step["id"],
            tables.deliberation_steps.c.task_id == step["task_id"],
            tables.deliberation_steps.c.owner_scope == step["owner_scope"],
            tables.deliberation_steps.c.input_revision == step["input_revision"],
            tables.deliberation_steps.c.operation_id == step["operation_id"],
            tables.deliberation_steps.c.parent_operation_id == step["parent_operation_id"],
            tables.deliberation_steps.c.maintenance_completed_at.is_(None),
            tables.deliberation_steps.c.state.in_(("READY", "WAITING_PARENT", "ADMITTED", "RESULT_ACCEPTED", "STOPPED")),
            or_(
                tables.deliberation_steps.c.maintenance_retry_after.is_(None),
                tables.deliberation_steps.c.maintenance_retry_after < retry_after,
            ),
        ).values(maintenance_retry_after=retry_after))
        return result.rowcount == 1

    async def list_for_task(self, task_id: TaskId, *, lock: bool = False) -> Sequence[Mapping[str, object]]:
        query = select(tables.deliberation_steps).where(
            tables.deliberation_steps.c.task_id == task_id,
        ).order_by(tables.deliberation_steps.c.step_order, tables.deliberation_steps.c.id)
        if lock:
            query = query.with_for_update()
        rows = (await self.connection.execute(query)).mappings().all()
        return tuple(dict(row) for row in rows)

    async def transition(
        self, step_id: str, expected: Sequence[str], state: str, *,
        conclusion_id: str | None = None, parent_conclusion_id: str | None = None,
        stop_reason: str | None = None,
    ) -> None:
        if not expected:
            raise ValueError("expected deliberation states are required")
        values: dict[str, object] = {"state": state, "updated_at": aware_now()}
        if conclusion_id is not None:
            values["conclusion_id"] = conclusion_id
        if parent_conclusion_id is not None:
            values["parent_conclusion_id"] = parent_conclusion_id
        if stop_reason is not None:
            values["stop_reason"] = stop_reason
        result = await self.connection.execute(update(tables.deliberation_steps).where(
            tables.deliberation_steps.c.id == step_id,
            tables.deliberation_steps.c.state.in_(tuple(expected)),
        ).values(**values))
        if result.rowcount != 1:
            raise StaleInput("deliberation step state changed")

    async def set_ready(self, step_id: str, expected: Sequence[str]) -> None:
        await self.transition(step_id, expected, "READY")

    async def mark_admitted(self, step_id: str) -> None:
        await self.transition(step_id, ("READY",), "ADMITTED")

    async def mark_result_accepted(self, step_id: str, conclusion_id: str) -> None:
        await self.transition(step_id, ("ADMITTED",), "RESULT_ACCEPTED", conclusion_id=conclusion_id)

    async def mark_complete(self, step_id: str) -> None:
        await self.transition(step_id, ("RESULT_ACCEPTED",), "COMPLETE")

    @staticmethod
    def stage_hash(row: Mapping[str, object]) -> str:
        from hekate.domain.contracts import canonical_json_hash
        return canonical_json_hash({
            "id": row["id"], "task_id": row["task_id"], "scope": row["owner_scope"],
            "revision": row["input_revision"], "kind": row["step_kind"],
            "slot": row["step_slot"], "parent_operation_id": row["parent_operation_id"],
            "parent_attempt_id": row["parent_attempt_id"],
            # A WAITING_PARENT step receives its parent Conclusion only after the
            # prior result is accepted. Bind the predecessor step identity here;
            # admission separately checks that predecessor's accepted Conclusion.
            "previous_step_id": row.get("previous_step_id"),
            "attempt_id": row["attempt_id"], "operation_id": row["operation_id"],
            "reservation_id": row["reservation_id"], "registry_id": row["registry_id"],
            "request_hash": row["request_hash"],
        })
