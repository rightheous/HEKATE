from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from hekate.domain.errors import Conflict
from hekate.domain.models import (
    ConclusionCapsule, DissentExcerpt, EvidenceRecord, Objection, PositionBody, PositionTopicView, PositionView,
    PositionVersionRecord, StoredConclusion,
)
from hekate.domain.types import AttemptId, DomainId, EvidenceId, OperationId, RegistryId, ScopeId, TaskId, TopicId
from hekate.domain.contracts import canonical_json

from . import tables
from .common import aware_now, json_value


class PostgresKnowledgeRepository:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def lock_topic(self, scope: ScopeId, topic_id: TopicId) -> int:
        await self.connection.execute(pg_insert(tables.position_topics).values(
            scope=scope, topic_id=topic_id, current_version=0,
        ).on_conflict_do_nothing(index_elements=[tables.position_topics.c.scope, tables.position_topics.c.topic_id]))
        row = (await self.connection.execute(select(tables.position_topics.c.current_version).where(
            tables.position_topics.c.scope == scope,
            tables.position_topics.c.topic_id == topic_id,
        ).with_for_update())).scalar_one()
        return int(row)

    async def append_position(self, record: PositionVersionRecord) -> None:
        await self.connection.execute(insert(tables.position_versions).values(
            scope=record.scope, topic_id=record.topic_id, version=record.version,
            base_version=record.base_version, body=json_value(record.body),
            operation_id=record.operation_id, task_id=record.task_id,
            input_revision=record.input_revision, registry_id=record.registry_id,
            conclusion_id=record.conclusion_id, reason_for_change=record.reason_for_change,
            created_at=record.created_at,
        ))
        if record.body.evidence_refs:
            await self.connection.execute(insert(tables.position_evidence), [
                {"scope": record.scope, "topic_id": record.topic_id, "version": record.version, "evidence_id": ref}
                for ref in sorted(set(record.body.evidence_refs))
            ])
        if record.body.dissent_refs:
            await self.connection.execute(insert(tables.position_dissent), [
                {"scope": record.scope, "topic_id": record.topic_id, "version": record.version, "dissent_id": ref}
                for ref in sorted(set(record.body.dissent_refs))
            ])

    async def cas_current(self, scope: ScopeId, topic_id: TopicId, base: int, new: int) -> bool:
        result = await self.connection.execute(update(tables.position_topics).where(
            tables.position_topics.c.scope == scope,
            tables.position_topics.c.topic_id == topic_id,
            tables.position_topics.c.current_version == base,
        ).values(current_version=new))
        if result.rowcount != 1:
            raise RuntimeError("Position current pointer changed while its topic row was locked")
        return True

    async def insert_evidence(self, record: EvidenceRecord) -> None:
        values = {
            "id": record.id, "scope": record.scope, "kind": record.kind,
            "source_uri": record.source_uri, "locator": record.locator,
            "retrieved_at": record.retrieved_at, "observed_at": record.observed_at,
            "content_hash": record.content_hash, "derived_from": json_value(record.derived_from),
            "root_source_ids": json_value(record.root_source_ids), "access_scope": record.access_scope,
            "retention_class": record.retention_class, "content_version": record.content_version,
            "availability": record.availability, "access_epoch": record.access_epoch,
            "expiry_at": record.expiry_at, "artifact_ref": record.artifact_ref,
            "registered_at": record.registered_at or aware_now(),
        }
        await self.connection.execute(pg_insert(tables.evidence).values(**values).on_conflict_do_nothing(
            index_elements=[tables.evidence.c.id],
        ))
        row = (await self.connection.execute(select(tables.evidence).where(
            tables.evidence.c.id == record.id,
        ))).mappings().one()
        if any(row[key] != value for key, value in values.items() if key not in {"availability", "registered_at"}):
            raise ValueError("evidence ID is already bound to different content or metadata")
        if row["availability"] not in {"STAGED", "AVAILABLE"}:
            raise ValueError("expired or unavailable Evidence cannot be restored")
        for source_id in sorted(set(record.derived_from)):
            await self.connection.execute(pg_insert(tables.evidence_edges).values(
                derived_id=record.id, source_id=source_id,
            ).on_conflict_do_nothing(index_elements=[tables.evidence_edges.c.derived_id, tables.evidence_edges.c.source_id]))

    async def get_evidence(self, ids: Sequence[EvidenceId], lock: bool = False) -> Sequence[EvidenceRecord]:
        if not ids:
            return ()
        query = select(tables.evidence).where(tables.evidence.c.id.in_(sorted(set(ids)))).order_by(tables.evidence.c.id)
        if lock:
            query = query.with_for_update()
        rows = (await self.connection.execute(query)).mappings().all()
        return tuple(self._evidence(row) for row in rows)

    async def lock_accessible_references(
        self, scope: ScopeId, refs: Sequence[EvidenceId], access_epoch: int, *, now: datetime | None = None,
    ) -> Sequence[EvidenceRecord]:
        if not refs:
            return ()
        query = select(tables.evidence).where(
            tables.evidence.c.id.in_(sorted(set(refs))),
            tables.evidence.c.scope == scope,
            tables.evidence.c.access_epoch == access_epoch,
            tables.evidence.c.availability == "AVAILABLE",
            (tables.evidence.c.expiry_at.is_(None)) | (tables.evidence.c.expiry_at > (now or aware_now())),
        ).order_by(tables.evidence.c.id).with_for_update()
        rows = (await self.connection.execute(query)).mappings().all()
        return tuple(self._evidence(row) for row in rows)

    async def get_current_position(
        self, scope: ScopeId, topic_id: TopicId, registry_id: RegistryId | None = None,
    ) -> PositionTopicView:
        row = (await self.connection.execute(select(tables.position_topics).where(
            tables.position_topics.c.scope == scope, tables.position_topics.c.topic_id == topic_id,
        ))).mappings().one_or_none()
        version = int(row["current_version"]) if row else 0
        current = None
        if version:
            current = await self._position_view(scope, topic_id, version, registry_id)
        return PositionTopicView(scope=scope, topic_id=topic_id, current_version=version, current=current)

    async def get_position_history(
        self, scope: ScopeId, topic_id: TopicId, after_version: int, limit: int,
        registry_id: RegistryId | None = None,
    ) -> tuple[PositionView, ...]:
        versions = (await self.connection.execute(select(tables.position_versions.c.version).where(
            tables.position_versions.c.scope == scope,
            tables.position_versions.c.topic_id == topic_id,
            tables.position_versions.c.version > after_version,
        ).order_by(tables.position_versions.c.version).limit(limit))).scalars().all()
        return tuple([await self._position_view(scope, topic_id, int(version), registry_id) for version in versions])

    async def _position_view(self, scope: ScopeId, topic_id: TopicId, version: int, registry_id: RegistryId | None) -> PositionView:
        row = (await self.connection.execute(select(tables.position_versions).where(
            tables.position_versions.c.scope == scope,
            tables.position_versions.c.topic_id == topic_id,
            tables.position_versions.c.version == version,
        ))).mappings().one()
        evidence_refs = (await self.connection.execute(select(tables.position_evidence.c.evidence_id).where(
            tables.position_evidence.c.scope == scope,
            tables.position_evidence.c.topic_id == topic_id,
            tables.position_evidence.c.version == version,
        ).order_by(tables.position_evidence.c.evidence_id))).scalars().all()
        dissent_refs = (await self.connection.execute(select(tables.position_dissent.c.dissent_id).where(
            tables.position_dissent.c.scope == scope,
            tables.position_dissent.c.topic_id == topic_id,
            tables.position_dissent.c.version == version,
        ).order_by(tables.position_dissent.c.dissent_id))).scalars().all()
        projection = None
        if registry_id is not None:
            projection = (await self.connection.execute(select(tables.memory_projections).where(
                tables.memory_projections.c.scope == scope,
                tables.memory_projections.c.topic_id == topic_id,
                tables.memory_projections.c.target_registry_id == registry_id,
            ))).mappings().one_or_none()
        return PositionView(
            scope=scope, topic_id=topic_id, version=version, base_version=row["base_version"],
            body=PositionBody.model_validate_json(canonical_json(row["body"]), strict=True),
            task_id=TaskId(row["task_id"]), input_revision=row["input_revision"],
            registry_id=RegistryId(row["registry_id"]), conclusion_id=DomainId(row["conclusion_id"]),
            operation_id=OperationId(row["operation_id"]), reason_for_change=row["reason_for_change"],
            created_at=row["created_at"], evidence_refs=tuple(EvidenceId(value) for value in evidence_refs),
            dissent_refs=tuple(DomainId(value) for value in dissent_refs),
            projection_desired_version=projection["desired_version"] if projection else 0,
            projection_applied_version=projection["applied_version"] if projection else 0,
            projection_state=projection["state"] if projection else "NONE",
            projection_pending_reason=projection["pending_reason"] if projection else None,
        )

    async def ensure_dissent(self, scope: ScopeId, conclusion_id: DomainId, objections) -> tuple[DomainId, ...]:
        ids = []
        for objection in objections:
            dissent_id = DomainId(str(uuid5(NAMESPACE_URL, f"hekate:dissent:{conclusion_id}:{objection.id}")))
            body = json_value(objection)
            await self.connection.execute(pg_insert(tables.dissent).values(
                id=dissent_id, scope=scope, conclusion_id=conclusion_id,
                local_objection_id=objection.id, body=body,
            ).on_conflict_do_nothing(index_elements=[tables.dissent.c.conclusion_id, tables.dissent.c.local_objection_id]))
            row = (await self.connection.execute(select(tables.dissent).where(
                tables.dissent.c.conclusion_id == conclusion_id,
                tables.dissent.c.local_objection_id == objection.id,
            ))).mappings().one()
            if row["id"] != dissent_id or row["body"] != body or row["scope"] != scope:
                raise ValueError("dissent identity is already bound to different content")
            ids.append(dissent_id)
        return tuple(ids)

    async def save_context_manifest(self, operation_id: OperationId, task_id: TaskId, revision: int, manifest: Mapping[str, object]) -> None:
        value = json_value(manifest)
        digest = canonical_json(value)
        await self.connection.execute(pg_insert(tables.context_manifests).values(
            operation_id=operation_id, task_id=task_id, input_revision=revision,
            manifest_hash=hashlib.sha256(digest.encode()).hexdigest(), manifest=value,
        ).on_conflict_do_nothing(index_elements=[tables.context_manifests.c.operation_id]))
        row = (await self.connection.execute(select(tables.context_manifests).where(
            tables.context_manifests.c.operation_id == operation_id,
        ))).mappings().one()
        if row["task_id"] != task_id or row["input_revision"] != revision or row["manifest"] != value:
            raise ValueError("runtime operation is already bound to a different context manifest")

    async def get_context_manifest(self, operation_id: OperationId):
        return (await self.connection.execute(select(tables.context_manifests).where(
            tables.context_manifests.c.operation_id == operation_id,
        ))).mappings().one_or_none()

    async def upsert_projection(self, scope: ScopeId, topic_id: TopicId, registry_id: RegistryId, version: int) -> None:
        await self.connection.execute(pg_insert(tables.memory_projections).values(
            scope=scope, topic_id=topic_id, target_registry_id=registry_id,
            desired_version=version, applied_version=0, state="PENDING_UNSUPPORTED",
            pending_reason="memory_projection_not_implemented",
        ).on_conflict_do_update(
            index_elements=[tables.memory_projections.c.scope, tables.memory_projections.c.topic_id, tables.memory_projections.c.target_registry_id],
            set_={"desired_version": version, "state": "PENDING_UNSUPPORTED", "pending_reason": "memory_projection_not_implemented", "updated_at": aware_now()},
        ))

    @staticmethod
    def _evidence(row) -> EvidenceRecord:
        from hekate.domain.types import EvidenceId

        return EvidenceRecord(
            schema_version="1", id=EvidenceId(row["id"]), kind=row["kind"],
            source_uri=row["source_uri"], locator=row["locator"], retrieved_at=row["retrieved_at"],
            observed_at=row["observed_at"], content_hash=row["content_hash"],
            derived_from=tuple(EvidenceId(value) for value in row["derived_from"]),
            access_scope=row["access_scope"], retention_class=row["retention_class"],
            content_version=row["content_version"], availability=row["availability"],
            access_epoch=row["access_epoch"], root_source_ids=tuple(EvidenceId(value) for value in row["root_source_ids"]),
            expiry_at=row["expiry_at"], scope=ScopeId(row["scope"]), artifact_ref=row["artifact_ref"],
            registered_at=row["registered_at"],
        )

    async def stage_artifact(self, ref: str, digest: str, size: int, retention_class: str) -> None:
        await self.connection.execute(pg_insert(tables.artifacts).values(
            artifact_ref=ref, content_hash=digest, byte_size=size,
            retention_class=retention_class, storage_state="STAGED",
        ).on_conflict_do_nothing(index_elements=[tables.artifacts.c.artifact_ref]))
        row = (await self.connection.execute(select(tables.artifacts).where(
            tables.artifacts.c.artifact_ref == ref,
        ).with_for_update())).mappings().one()
        if (row["content_hash"], row["byte_size"]) != (digest, size):
            raise ValueError("archive reference is already bound to different content")
        if row["storage_state"] in {"DELETE_PENDING", "DELETE_FAILED"}:
            raise Conflict("archive artifact deletion is pending; retry Evidence registration after cleanup")
        if row["storage_state"] == "DELETED":
            await self.connection.execute(update(tables.artifacts).where(
                tables.artifacts.c.artifact_ref == ref,
                tables.artifacts.c.storage_state == "DELETED",
            ).values(storage_state="STAGED", retention_class=retention_class, deleted_at=None))

    async def mark_artifact_available(self, ref: str) -> None:
        result = await self.connection.execute(update(tables.artifacts).where(
            tables.artifacts.c.artifact_ref == ref,
            tables.artifacts.c.storage_state.in_(["STAGED", "AVAILABLE"]),
        ).values(storage_state="AVAILABLE"))
        if result.rowcount != 1:
            raise Conflict("archive artifact state changed before it became available")
        await self.connection.execute(update(tables.evidence).where(
            tables.evidence.c.artifact_ref == ref,
            tables.evidence.c.availability == "STAGED",
        ).values(availability="AVAILABLE"))

    async def register_evidence_import(
        self, scope: ScopeId, request_key: str, request_hash: str, evidence_id: EvidenceId,
    ) -> Mapping[str, object]:
        await self.connection.execute(pg_insert(tables.evidence_imports).values(
            scope=scope, request_key=request_key, request_hash=request_hash, evidence_id=evidence_id,
        ).on_conflict_do_nothing(index_elements=[tables.evidence_imports.c.scope, tables.evidence_imports.c.request_key]))
        row = (await self.connection.execute(select(tables.evidence_imports).where(
            tables.evidence_imports.c.scope == scope,
            tables.evidence_imports.c.request_key == request_key,
        ).with_for_update())).mappings().one()
        if row["request_hash"] != request_hash:
            raise ValueError("evidence request key is already bound to different input")
        return row

    async def get_evidence_import(self, scope: ScopeId, request_key: str):
        return (await self.connection.execute(select(tables.evidence_imports).where(
            tables.evidence_imports.c.scope == scope,
            tables.evidence_imports.c.request_key == request_key,
        ).with_for_update())).mappings().one_or_none()

    async def get_artifact(self, ref: str):
        return (await self.connection.execute(select(tables.artifacts).where(
            tables.artifacts.c.artifact_ref == ref,
        ))).mappings().one_or_none()

    async def pending_archive_deletions(self, limit: int = 100):
        return (await self.connection.execute(select(tables.artifacts).where(
            tables.artifacts.c.storage_state.in_(["DELETE_PENDING", "DELETE_FAILED"]),
        ).order_by(tables.artifacts.c.created_at, tables.artifacts.c.artifact_ref).limit(limit))).mappings().all()

    async def archive_delete_is_pending(self, ref: str) -> bool:
        artifact = (await self.connection.execute(select(tables.artifacts).where(
            tables.artifacts.c.artifact_ref == ref,
            tables.artifacts.c.storage_state.in_(["DELETE_PENDING", "DELETE_FAILED"]),
        ).with_for_update())).mappings().one_or_none()
        if artifact is None:
            return False
        active = await self.connection.scalar(select(func.count()).select_from(tables.evidence).where(
            tables.evidence.c.artifact_ref == ref,
            tables.evidence.c.availability.in_(["AVAILABLE", "STAGED"]),
            (tables.evidence.c.expiry_at.is_(None)) | (tables.evidence.c.expiry_at > aware_now()),
        ))
        return not active

    async def expire_evidence(self, now: datetime, limit: int) -> tuple[str, ...]:
        rows = (await self.connection.execute(select(tables.evidence).where(
            tables.evidence.c.availability.in_(["AVAILABLE", "STAGED"]),
            tables.evidence.c.expiry_at.is_not(None),
            tables.evidence.c.expiry_at <= now,
        ).order_by(tables.evidence.c.id).limit(limit).with_for_update(skip_locked=True))).mappings().all()
        refs = set()
        for row in rows:
            await self.connection.execute(update(tables.evidence).where(
                tables.evidence.c.id == row["id"],
            ).values(availability="EXPIRED"))
            ref = row["artifact_ref"]
            if ref:
                refs.add(ref)
        if refs:
            locked_refs = (await self.connection.execute(select(tables.artifacts.c.artifact_ref).where(
                tables.artifacts.c.artifact_ref.in_(sorted(refs)),
            ).order_by(tables.artifacts.c.artifact_ref).with_for_update())).scalars().all()
            delete_refs = []
            for ref in locked_refs:
                active = await self.connection.scalar(select(func.count()).select_from(tables.evidence).where(
                    tables.evidence.c.artifact_ref == ref,
                    tables.evidence.c.availability.in_(["AVAILABLE", "STAGED"]),
                    (tables.evidence.c.expiry_at.is_(None)) | (tables.evidence.c.expiry_at > now),
                ))
                if not active:
                    delete_refs.append(ref)
            if delete_refs:
                await self.connection.execute(update(tables.artifacts).where(
                    tables.artifacts.c.artifact_ref.in_(delete_refs),
                    tables.artifacts.c.storage_state.in_(["STAGED", "AVAILABLE", "DELETE_FAILED"]),
                ).values(storage_state="DELETE_PENDING"))
        return tuple(str(row["id"]) for row in rows)

    async def finish_archive_delete(self, ref: str, *, deleted: bool) -> None:
        await self.connection.execute(update(tables.artifacts).where(
            tables.artifacts.c.artifact_ref == ref,
            tables.artifacts.c.storage_state.in_(["DELETE_PENDING", "DELETE_FAILED"]),
        ).values(storage_state="DELETED" if deleted else "DELETE_FAILED", deleted_at=aware_now() if deleted else None))

    async def get_commit_receipt(self, operation_id: OperationId):
        return (await self.connection.execute(select(tables.position_commit_receipts).where(
            tables.position_commit_receipts.c.operation_id == operation_id,
        ).with_for_update())).mappings().one_or_none()

    async def insert_commit_receipt(
        self, operation_id: OperationId, request_hash: str, scope: ScopeId,
        registry_id: RegistryId, receipt: Mapping[str, object],
    ) -> None:
        await self.connection.execute(insert(tables.position_commit_receipts).values(
            operation_id=operation_id, request_hash=request_hash, scope=scope,
            registry_id=registry_id, receipt=json_value(receipt),
        ))

    async def valid_dissent_refs(self, scope: ScopeId, refs: Sequence[DomainId]) -> bool:
        if not refs:
            return True
        rows = (await self.connection.execute(select(tables.dissent.c.id).where(
            tables.dissent.c.id.in_(sorted(set(refs))), tables.dissent.c.scope == scope,
        ).order_by(tables.dissent.c.id).with_for_update())).scalars().all()
        return set(rows) == set(refs)

    async def get_dissent_for_conclusion(self, scope: ScopeId, conclusion_id: DomainId) -> tuple[DomainId, ...]:
        rows = (await self.connection.execute(select(tables.dissent.c.id).where(
            tables.dissent.c.scope == scope,
            tables.dissent.c.conclusion_id == conclusion_id,
        ).order_by(tables.dissent.c.id))).scalars().all()
        return tuple(DomainId(value) for value in rows)

    async def get_dissent_context(self, scope: ScopeId, refs: Sequence[DomainId]) -> tuple[DissentExcerpt, ...]:
        if not refs:
            return ()
        rows = (await self.connection.execute(select(tables.dissent).where(
            tables.dissent.c.scope == scope,
            tables.dissent.c.id.in_(sorted(set(refs))),
        ).order_by(tables.dissent.c.id))).mappings().all()
        if len(rows) != len(set(refs)):
            raise ValueError("Position refers to unavailable dissent")
        return tuple(DissentExcerpt(id=DomainId(row["id"]), objection=Objection.model_validate(row["body"], strict=True)) for row in rows)

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
            capsule=ConclusionCapsule.model_validate_json(canonical_json(row["capsule"]), strict=True),
            validation_status=row["validation_status"],
            eligible=row["eligible"],
            provider_provenance=row["provider_provenance"],
            rejection_reason=row["rejection_reason"],
        ) for row in rows)
    async def set_projection_watermark(self, target: object, version: int) -> None: raise NotImplementedError
