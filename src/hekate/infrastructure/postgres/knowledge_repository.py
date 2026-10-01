from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from hekate.domain.models import ConclusionCapsule, EvidenceRecord, PositionVersionRecord, StoredConclusion
from hekate.domain.types import AttemptId, DomainId, EvidenceId, OperationId, RegistryId, ScopeId, TaskId, TopicId
from hekate.domain.contracts import canonical_json

from . import tables
from .common import aware_now, json_value


class PostgresKnowledgeRepository:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def lock_topic(self, scope: ScopeId, topic_id: TopicId) -> object: raise NotImplementedError
    async def append_position(self, record: PositionVersionRecord) -> None: raise NotImplementedError
    async def cas_current(self, topic_id: TopicId, base: int, new: int) -> bool: raise NotImplementedError
    async def insert_evidence(self, record: EvidenceRecord) -> None: raise NotImplementedError
    async def get_evidence(self, ids: Sequence[EvidenceId], lock: bool = False) -> Sequence[EvidenceRecord]: raise NotImplementedError
    async def lock_accessible_references(self, refs: Sequence[EvidenceId]) -> object: raise NotImplementedError
    async def insert_conclusion(self, record: StoredConclusion) -> None:
        capsule = json_value(record.capsule)
        await self.connection.execute(pg_insert(tables.conclusions).values(
            id=record.id,
            task_id=record.capsule.task_id,
            attempt_id=record.attempt_id,
            operation_id=record.provider_provenance.get("operation_id"),
            registry_id=record.capsule.agent_id,
            input_revision=record.capsule.input_revision or record.provider_provenance["input_revision"],
            payload_hash=record.payload_hash,
            capsule=capsule,
            validation_status=record.validation_status,
            eligible=record.eligible,
            provider_provenance=json_value(record.provider_provenance),
            rejection_reason=record.rejection_reason,
        ).on_conflict_do_nothing(index_elements=[tables.conclusions.c.attempt_id, tables.conclusions.c.payload_hash]))
        row = (await self.connection.execute(select(tables.conclusions.c.capsule).where(
            tables.conclusions.c.attempt_id == record.attempt_id,
            tables.conclusions.c.payload_hash == record.payload_hash,
        ))).scalar_one_or_none()
        if row != capsule:
            raise ValueError("conclusion payload hash is already bound to different data")

    async def save_turn_result(self, values: Mapping[str, object]) -> None:
        await self.connection.execute(pg_insert(tables.turn_results).values(
            **json_value(values),
        ).on_conflict_do_nothing(index_elements=[tables.turn_results.c.inbox_id]))
        row = (await self.connection.execute(select(tables.turn_results).where(
            tables.turn_results.c.inbox_id == values["inbox_id"],
        ))).mappings().one_or_none()
        if row is None or any(row[key] != values[key] for key in (
            "task_id", "attempt_id", "operation_id", "registry_id", "input_revision", "output_hash",
        )):
            raise ValueError("turn result identity is already bound to different data")

    async def lock_turn_result(self, inbox_id: str):
        return (await self.connection.execute(select(tables.turn_results).where(
            tables.turn_results.c.inbox_id == inbox_id,
        ).with_for_update())).mappings().one_or_none()

    async def update_turn_result(self, inbox_id: str, *, state: str, proposal: object = None,
                                 conclusion_id: str | None = None, rejection_reason: str | None = None,
                                 delay_seconds: float = 0) -> None:
        if state not in {"WAITING_EXECUTION", "VALIDATED", "REJECTED", "ACCEPTED", "LATE"}:
            raise ValueError("invalid turn result state")
        values = {
            "processing_state": state,
            "next_attempt_at": aware_now() + timedelta(seconds=max(delay_seconds, 0)),
        }
        if proposal is not None:
            values["proposal"] = json_value(proposal)
        if conclusion_id is not None:
            values["conclusion_id"] = conclusion_id
        if rejection_reason is not None or state == "ACCEPTED":
            values["rejection_reason"] = rejection_reason
        await self.connection.execute(update(tables.turn_results).where(
            tables.turn_results.c.inbox_id == inbox_id,
        ).values(**values))

    async def get_conclusion(self, conclusion_id: str):
        return (await self.connection.execute(select(tables.conclusions).where(
            tables.conclusions.c.id == conclusion_id,
        ))).mappings().one_or_none()

    async def set_conclusion_eligible(self, conclusion_id: str, eligible: bool, reason: str | None) -> None:
        await self.connection.execute(update(tables.conclusions).where(
            tables.conclusions.c.id == conclusion_id,
        ).values(eligible=eligible, rejection_reason=reason))

    async def get_eligible_conclusions(self, task_id: TaskId, revision: int) -> Sequence[StoredConclusion]:
        rows = (await self.connection.execute(select(tables.conclusions).where(
            tables.conclusions.c.task_id == task_id,
            tables.conclusions.c.input_revision == revision,
            tables.conclusions.c.eligible.is_(True),
        ).order_by(tables.conclusions.c.created_at, tables.conclusions.c.id))).mappings().all()
        return tuple(StoredConclusion(
            id=DomainId(row["id"]),
            attempt_id=AttemptId(row["attempt_id"]),
            payload_hash=row["payload_hash"],
            capsule=ConclusionCapsule.model_validate(row["capsule"], strict=True),
            validation_status=row["validation_status"],
            eligible=row["eligible"],
            provider_provenance=row["provider_provenance"],
            rejection_reason=row["rejection_reason"],
        ) for row in rows)
    async def set_projection_watermark(self, target: object, version: int) -> None: raise NotImplementedError
