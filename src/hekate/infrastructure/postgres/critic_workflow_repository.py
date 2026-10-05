from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from hekate.domain.errors import Conflict, PolicyDenied, StaleInput
from hekate.domain.models import CriticWorkflow, SpawnProposal
from hekate.domain.types import AttemptId, DomainId, OperationId, RegistryId, ReservationId, ScopeId, TaskId

from . import tables
from .common import aware_now, json_value


class PostgresCriticWorkflowRepository:
    def __init__(self, connection: AsyncSession) -> None:
        self.connection = connection

    async def insert(self, workflow: CriticWorkflow) -> None:
        await self.connection.execute(insert(tables.critic_workflows).values(
            task_id=workflow.task_id,
            owner_scope=workflow.owner_scope,
            input_revision=workflow.input_revision,
            stage=workflow.stage,
            spawn_request_hash=workflow.spawn_request_hash,
            proposal=json_value(workflow.proposal),
            critic_profile=json_value(workflow.critic_profile),
            hekate_profile=json_value(workflow.hekate_profile),
            parent_attempt_id=workflow.parent_attempt_id,
            planning_operation_id=workflow.planning_operation_id,
            planning_conclusion_id=workflow.planning_conclusion_id,
            critic_registry_id=workflow.critic_registry_id,
            create_operation_id=workflow.create_operation_id,
            review_attempt_id=workflow.review_attempt_id,
            review_operation_id=workflow.review_operation_id,
            review_reservation_id=workflow.review_reservation_id,
            synthesis_attempt_id=workflow.synthesis_attempt_id,
            synthesis_operation_id=workflow.synthesis_operation_id,
            synthesis_reservation_id=workflow.synthesis_reservation_id,
            critic_conclusion_id=workflow.critic_conclusion_id,
            delete_operation_id=workflow.delete_operation_id,
            updated_at=aware_now(),
        ))

    @staticmethod
    def _workflow(row) -> CriticWorkflow:
        return CriticWorkflow(
            task_id=TaskId(row["task_id"]), owner_scope=ScopeId(row["owner_scope"]),
            input_revision=row["input_revision"], stage=row["stage"],
            spawn_request_hash=row["spawn_request_hash"],
            proposal=SpawnProposal.model_validate(row["proposal"], strict=True),
            critic_profile=dict(row["critic_profile"]),
            hekate_profile=dict(row["hekate_profile"]),
            parent_attempt_id=AttemptId(row["parent_attempt_id"]),
            planning_operation_id=OperationId(row["planning_operation_id"]),
            planning_conclusion_id=DomainId(row["planning_conclusion_id"]),
            critic_registry_id=RegistryId(row["critic_registry_id"]),
            create_operation_id=OperationId(row["create_operation_id"]),
            review_attempt_id=AttemptId(row["review_attempt_id"]),
            review_operation_id=OperationId(row["review_operation_id"]),
            review_reservation_id=ReservationId(row["review_reservation_id"]),
            synthesis_attempt_id=AttemptId(row["synthesis_attempt_id"]),
            synthesis_operation_id=OperationId(row["synthesis_operation_id"]),
            synthesis_reservation_id=ReservationId(row["synthesis_reservation_id"]),
            critic_conclusion_id=DomainId(row["critic_conclusion_id"]) if row["critic_conclusion_id"] else None,
            delete_operation_id=OperationId(row["delete_operation_id"]) if row["delete_operation_id"] else None,
            updated_at=row["updated_at"],
        )

    async def get(self, task_id: TaskId, *, lock: bool = False) -> CriticWorkflow | None:
        query = select(tables.critic_workflows).where(tables.critic_workflows.c.task_id == task_id)
        if lock:
            query = query.with_for_update()
        row = (await self.connection.execute(query)).mappings().one_or_none()
        return self._workflow(row) if row else None

    async def get_by_create_operation(self, operation_id: OperationId, *, lock: bool = False) -> CriticWorkflow | None:
        query = select(tables.critic_workflows).where(
            tables.critic_workflows.c.create_operation_id == operation_id,
        )
        if lock:
            query = query.with_for_update()
        row = (await self.connection.execute(query)).mappings().one_or_none()
        return self._workflow(row) if row else None

    async def list_stages(self, stages: Sequence[str], limit: int = 100) -> Sequence[CriticWorkflow]:
        if not stages or not 1 <= limit <= 1000:
            return ()
        rows = (await self.connection.execute(select(tables.critic_workflows).where(
            tables.critic_workflows.c.stage.in_(tuple(stages)),
        ).order_by(tables.critic_workflows.c.updated_at, tables.critic_workflows.c.task_id).limit(limit))).mappings().all()
        return tuple(self._workflow(row) for row in rows)

    async def transition(
        self, task_id: TaskId, expected: Sequence[str], stage: str, **values: object,
    ) -> CriticWorkflow:
        allowed = {"critic_conclusion_id", "delete_operation_id"}
        if set(values) - allowed:
            raise ValueError("critic workflow transition contains unsupported fields")
        workflow = await self.get(task_id, lock=True)
        if workflow is None or workflow.stage not in expected:
            raise StaleInput("Critic workflow stage changed")
        changes = {**values, "stage": stage, "updated_at": aware_now()}
        result = await self.connection.execute(update(tables.critic_workflows).where(
            tables.critic_workflows.c.task_id == task_id,
            tables.critic_workflows.c.stage == workflow.stage,
        ).values(**changes))
        if result.rowcount != 1:
            raise StaleInput("Critic workflow transition lost its stage fence")
        return workflow.model_copy(update=changes)

    async def authorize_admission(
        self, *, task_id: TaskId, scope: ScopeId, stage: str, attempt_id: AttemptId,
        operation_id: OperationId, registry_id: RegistryId,
    ) -> CriticWorkflow:
        workflow = await self.get(task_id, lock=True)
        expected_stage = "CRITIC_READY" if stage == "critic_review" else "SYNTHESIS_PENDING" if stage == "synthesis" else None
        expected_attempt = workflow.review_attempt_id if workflow and stage == "critic_review" else workflow.synthesis_attempt_id if workflow else None
        expected_operation = workflow.review_operation_id if workflow and stage == "critic_review" else workflow.synthesis_operation_id if workflow else None
        expected_registry = workflow.critic_registry_id if workflow and stage == "critic_review" else None
        if workflow is None or expected_stage is None or workflow.owner_scope != scope:
            raise PolicyDenied("no approved Critic workflow authorizes this admission")
        if (
            workflow.stage != expected_stage or workflow.input_revision < 1
            or workflow.stage in {"FAILED", "COMPLETE"}
            or attempt_id != expected_attempt or operation_id != expected_operation
            or (stage == "critic_review" and registry_id != expected_registry)
        ):
            raise PolicyDenied("operation does not match the current approved workflow step")
        return workflow
