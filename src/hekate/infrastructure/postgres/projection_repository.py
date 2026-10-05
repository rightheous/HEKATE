from __future__ import annotations

from datetime import timedelta
from typing import Mapping, Sequence

from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from hekate.domain.errors import Conflict, StaleInput
from hekate.domain.models import ProjectionJob
from hekate.domain.types import OperationId, RegistryId, ScopeId, TopicId
from . import tables
from .common import aware_now, json_value


class PostgresProjectionRepository:
    def __init__(self, connection) -> None:
        self.connection = connection

    async def enable_pending(self, limit: int = 100) -> int:
        rows = (await self.connection.execute(select(
            tables.memory_projections.c.scope,
            tables.memory_projections.c.topic_id,
            tables.memory_projections.c.target_registry_id,
        ).where(
            tables.memory_projections.c.state == "PENDING_UNSUPPORTED",
            tables.memory_projections.c.desired_version > tables.memory_projections.c.applied_version,
        ).order_by(
            tables.memory_projections.c.updated_at,
            tables.memory_projections.c.scope,
            tables.memory_projections.c.topic_id,
        ).limit(limit).with_for_update(skip_locked=True))).all()
        for scope, topic_id, registry_id in rows:
            await self.connection.execute(update(tables.memory_projections).where(
                tables.memory_projections.c.scope == scope,
                tables.memory_projections.c.topic_id == topic_id,
                tables.memory_projections.c.target_registry_id == registry_id,
                tables.memory_projections.c.state == "PENDING_UNSUPPORTED",
            ).values(state="PENDING", pending_reason=None, next_retry_at=aware_now(), updated_at=aware_now()))
        return len(rows)

    async def claim_due(self, worker: str, scope: ScopeId, limit: int, lease_seconds: int = 120) -> Sequence[ProjectionJob]:
        now = aware_now()
        rows = (await self.connection.execute(select(tables.memory_projections).where(
            tables.memory_projections.c.desired_version > tables.memory_projections.c.applied_version,
            tables.memory_projections.c.scope == scope,
            tables.memory_projections.c.state.in_(("PENDING", "UNKNOWN", "CLAIMED")),
            or_(tables.memory_projections.c.next_retry_at.is_(None), tables.memory_projections.c.next_retry_at <= now),
            or_(tables.memory_projections.c.claim_expires_at.is_(None), tables.memory_projections.c.claim_expires_at <= now),
        ).order_by(
            tables.memory_projections.c.next_retry_at.nullsfirst(),
            tables.memory_projections.c.updated_at,
            tables.memory_projections.c.scope,
            tables.memory_projections.c.topic_id,
        ).limit(limit).with_for_update(skip_locked=True))).mappings().all()
        jobs: list[ProjectionJob] = []
        for row in rows:
            source = (await self.connection.execute(select(
                tables.position_versions.c.operation_id,
            ).where(
                tables.position_versions.c.scope == row["scope"],
                tables.position_versions.c.topic_id == row["topic_id"],
                tables.position_versions.c.version == row["desired_version"],
            ))).scalar_one_or_none()
            if source is None:
                continue
            commit_suffix = ":position.commit"
            source = str(source)
            if not source.endswith(commit_suffix):
                raise StaleInput("authoritative Position has no runtime commit operation identity")
            # Position versions keep the commit receipt identity, while the
            # projection journal references the parent runtime operation row.
            source = source[:-len(commit_suffix)]
            signal = (await self.connection.execute(select(tables.outbox).where(
                tables.outbox.c.kind == "position_projection",
                tables.outbox.c.status == "PENDING",
                tables.outbox.c.payload["scope"].as_string() == row["scope"],
                tables.outbox.c.payload["topic_id"].as_string() == row["topic_id"],
                tables.outbox.c.payload["registry_id"].as_string() == row["target_registry_id"],
            ).order_by(tables.outbox.c.generation, tables.outbox.c.created_at, tables.outbox.c.id).limit(1))).mappings().one_or_none()
            fence = int(row["claim_fence"]) + 1
            expires_at = now + timedelta(seconds=lease_seconds)
            await self.connection.execute(update(tables.memory_projections).where(
                tables.memory_projections.c.scope == row["scope"],
                tables.memory_projections.c.topic_id == row["topic_id"],
                tables.memory_projections.c.target_registry_id == row["target_registry_id"],
            ).values(
                state="CLAIMED", claim_owner=worker, claim_expires_at=expires_at,
                claim_fence=fence, attempt_count=int(row["attempt_count"]) + 1,
                last_attempt_at=now, updated_at=now,
            ))
            jobs.append(ProjectionJob(
                id=str(signal["id"]) if signal else f"position_projection:{row['scope']}:{row['topic_id']}:{row['target_registry_id']}:{row['desired_version']}",
                original_operation_id=OperationId(str(source)),
                scope=ScopeId(row["scope"]), topic_id=TopicId(row["topic_id"]),
                target_registry_id=RegistryId(row["target_registry_id"]),
                generation=int(row["desired_version"]), worker_id=worker,
                fence=fence, claim_expires_at=expires_at,
            ))
        return tuple(jobs)

    async def _locked_claim(self, job: ProjectionJob):
        row = (await self.connection.execute(select(tables.memory_projections).where(
            tables.memory_projections.c.scope == job.scope,
            tables.memory_projections.c.topic_id == job.topic_id,
            tables.memory_projections.c.target_registry_id == job.target_registry_id,
        ).with_for_update())).mappings().one_or_none()
        if row is None or row["claim_owner"] != job.worker_id or row["claim_fence"] != job.fence or row["claim_expires_at"] <= aware_now():
            raise StaleInput("Position projection claim was superseded")
        return row

    async def start_operation(
        self, job: ProjectionJob, operation_id: OperationId, request_hash: str, payload_digest: str,
        source_version: int, base_applied_version: int,
    ) -> Mapping[str, object]:
        projection = await self._locked_claim(job)
        await self.connection.execute(pg_insert(tables.memory_projection_operations).values(
            id=operation_id, scope=job.scope, topic_id=job.topic_id,
            target_registry_id=job.target_registry_id, source_version=source_version,
            base_applied_version=base_applied_version, format_version=1,
            payload_digest=payload_digest, request_hash=request_hash,
            original_operation_id=job.original_operation_id, state="STARTED",
        ).on_conflict_do_nothing(index_elements=[tables.memory_projection_operations.c.id]))
        operation = (await self.connection.execute(select(tables.memory_projection_operations).where(
            tables.memory_projection_operations.c.id == operation_id,
        ).with_for_update())).mappings().one()
        expected = (job.scope, job.topic_id, job.target_registry_id, source_version, base_applied_version, 1, payload_digest, request_hash, job.original_operation_id)
        actual = tuple(operation[key] for key in (
            "scope", "topic_id", "target_registry_id", "source_version", "base_applied_version", "format_version",
            "payload_digest", "request_hash", "original_operation_id",
        ))
        if actual != expected:
            raise Conflict("Projection operation identity is already bound to a different request")
        if operation["state"] != "COMPLETED":
            await self.connection.execute(update(tables.memory_projection_operations).where(
                tables.memory_projection_operations.c.id == operation_id,
                tables.memory_projection_operations.c.state != "COMPLETED",
            ).values(state="STARTED", failure_reason=None, updated_at=aware_now()))
        await self.connection.execute(update(tables.memory_projections).where(
            tables.memory_projections.c.scope == job.scope,
            tables.memory_projections.c.topic_id == job.topic_id,
            tables.memory_projections.c.target_registry_id == job.target_registry_id,
        ).values(operation_id=operation_id, request_hash=request_hash, payload_digest=payload_digest))
        return dict(operation)

    async def finish(
        self, job: ProjectionJob, operation_id: OperationId, observation: Mapping[str, object],
        *, requested_version: int, requested_digest: str, current_version: int,
        current_digest: str, delay_seconds: int = 2,
    ) -> bool:
        projection = await self._locked_claim(job)
        operation = (await self.connection.execute(select(tables.memory_projection_operations).where(
            tables.memory_projection_operations.c.id == operation_id,
        ).with_for_update())).mappings().one()
        observed_version = int(observation.get("source_version", 0))
        observed_digest = observation.get("payload_digest")
        observed_verified = observation.get("verified") is True
        observed_present = observation.get("present") is True
        observation_json = json_value(observation)
        replayed = operation["state"] in {"COMPLETED", "SUPERSEDED"}

        accepted_version: int | None = None
        operation_state = "DRIFT"
        projection_state = "DRIFT"
        reason = "runtime_memory_does_not_match_authoritative_Position"
        if observed_verified and observed_present and observed_version == current_version and observed_digest == current_digest:
            accepted_version = current_version
            operation_state = "COMPLETED" if requested_version == current_version and requested_digest == current_digest else "SUPERSEDED"
            projection_state = "APPLIED" if int(projection["desired_version"]) <= current_version else "PENDING"
            reason = None if projection_state == "APPLIED" else "newer_Position_version_pending"
        elif observed_verified and observed_present and observed_version == requested_version and observed_digest == requested_digest:
            accepted_version = requested_version
            operation_state = "COMPLETED"
            projection_state = "PENDING"
            reason = "newer_Position_version_pending" if int(projection["desired_version"]) > requested_version else None
        elif observed_verified and observed_version < current_version:
            operation_state = "UNKNOWN"
            projection_state = "UNKNOWN"
            reason = "runtime_memory_is_older_than_current_Position"
        elif observed_verified and observed_version == 0 and current_version > 0:
            operation_state = "UNKNOWN"
            projection_state = "UNKNOWN"
            reason = "runtime_memory_readback_missing_after_write"

        previous_state = operation["state"]
        if not replayed:
            await self.connection.execute(update(tables.memory_projection_operations).where(
                tables.memory_projection_operations.c.id == operation_id,
                tables.memory_projection_operations.c.state.not_in(("COMPLETED", "SUPERSEDED")),
            ).values(
                state=operation_state, observation=observation_json, failure_reason=reason,
                updated_at=aware_now(),
                completed_at=aware_now() if operation_state in {"COMPLETED", "SUPERSEDED"} else None,
            ))
        values = {
            "observed_memory_version": observed_version,
            "observed_payload_digest": observed_digest,
            "state": projection_state,
            "pending_reason": reason,
            "next_retry_at": (aware_now() + timedelta(seconds=delay_seconds)) if projection_state in {"PENDING", "UNKNOWN"} else None,
            "claim_owner": None,
            "claim_expires_at": None,
            "updated_at": aware_now(),
        }
        if accepted_version is not None:
            values["applied_version"] = max(int(projection["applied_version"]), accepted_version)
        await self.connection.execute(update(tables.memory_projections).where(
            tables.memory_projections.c.scope == job.scope,
            tables.memory_projections.c.topic_id == job.topic_id,
            tables.memory_projections.c.target_registry_id == job.target_registry_id,
            tables.memory_projections.c.claim_owner == job.worker_id,
            tables.memory_projections.c.claim_fence == job.fence,
        ).values(**values))
        if accepted_version is not None:
            await self._ack_signals(job, accepted_version)
        return not replayed and previous_state not in {"COMPLETED", "SUPERSEDED"} and operation_state in {"COMPLETED", "SUPERSEDED"}

    async def fail(
        self, job: ProjectionJob, operation_id: OperationId | None, reason: str, *,
        state: str = "UNKNOWN", delay_seconds: int = 10,
    ) -> None:
        projection = await self._locked_claim(job)
        if operation_id is not None:
            await self.connection.execute(update(tables.memory_projection_operations).where(
                tables.memory_projection_operations.c.id == operation_id,
                tables.memory_projection_operations.c.state.not_in(("COMPLETED", "SUPERSEDED")),
            ).values(state=state, failure_reason=reason[:240], updated_at=aware_now()))
        await self._release_claim(
            job, state if state in {"UNKNOWN", "DRIFT", "CONFLICT"} else "PENDING",
            reason[:240],
            aware_now() + timedelta(seconds=delay_seconds) if state not in {"DRIFT", "CONFLICT"} else None,
        )

    async def _release_claim(self, job: ProjectionJob, state: str, reason: str | None, next_retry):
        await self.connection.execute(update(tables.memory_projections).where(
            tables.memory_projections.c.scope == job.scope,
            tables.memory_projections.c.topic_id == job.topic_id,
            tables.memory_projections.c.target_registry_id == job.target_registry_id,
            tables.memory_projections.c.claim_owner == job.worker_id,
            tables.memory_projections.c.claim_fence == job.fence,
        ).values(
            state=state, pending_reason=reason, next_retry_at=next_retry,
            claim_owner=None, claim_expires_at=None, updated_at=aware_now(),
        ))

    async def _ack_signals(self, job: ProjectionJob, through_version: int) -> None:
        await self.connection.execute(update(tables.outbox).where(
            tables.outbox.c.kind == "position_projection",
            tables.outbox.c.status.in_(("PENDING", "CLAIMED")),
            tables.outbox.c.payload["scope"].as_string() == str(job.scope),
            tables.outbox.c.payload["topic_id"].as_string() == str(job.topic_id),
            tables.outbox.c.payload["registry_id"].as_string() == str(job.target_registry_id),
            tables.outbox.c.generation <= through_version,
        ).values(status="ACKED", acked_at=aware_now(), claim_owner=None, claim_expires_at=None))

    async def release_stale_claim(self, job: ProjectionJob, *, reason: str, delay_seconds: int = 5) -> None:
        await self._release_claim(job, "PENDING", reason[:240], aware_now() + timedelta(seconds=delay_seconds))

    async def get_status(self, scope: ScopeId, topic_id: TopicId, registry_id: RegistryId):
        return (await self.connection.execute(select(tables.memory_projections).where(
            tables.memory_projections.c.scope == scope,
            tables.memory_projections.c.topic_id == topic_id,
            tables.memory_projections.c.target_registry_id == registry_id,
        ))).mappings().one_or_none()

    async def operation_counts(self, operation_id: OperationId) -> Mapping[str, int]:
        rows = (await self.connection.execute(select(
            tables.memory_projection_operations.c.state,
        ).where(tables.memory_projection_operations.c.original_operation_id == operation_id))).scalars().all()
        return {state: rows.count(state) for state in set(rows)}
