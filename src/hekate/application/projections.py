from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, uuid5

from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.errors import Conflict, PolicyDenied, StaleInput
from hekate.domain.models import (
    MemoryProjection, MemoryProjectionObservation, ProjectionBinding, ProjectionJob,
    ProjectionReceipt, ProjectionStatus,
)
from hekate.domain.types import ActorContext, OperationId, RegistryId, ScopeId, TopicId
from hekate.ports.runtime import AgentRuntime
from hekate.ports.store import UowFactory

_LOG = logging.getLogger(__name__)
_NAMESPACE = "hekate.position.v1"
_FORMAT_VERSION = 1
_MAX_ENTRY_BYTES = 4_096
_RETRY_SECONDS = 5
_REGISTRY_LEASE_SECONDS = 120


def _bounded_utf8(value: str, limit: int) -> tuple[str, bool]:
    raw = value.encode("utf-8")
    if len(raw) <= limit:
        return value, False
    return raw[:limit].decode("utf-8", errors="ignore") + "… [truncated]", True


def _bounded_values(values, *, count: int, item_bytes: int) -> tuple[list[str], bool]:
    selected = []
    truncated = len(values) > count
    for value in values[:count]:
        text, cut = _bounded_utf8(str(value), item_bytes)
        selected.append(text)
        truncated = truncated or cut
    return selected, truncated


def _payload(view) -> tuple[str, str]:
    body = view.body
    statement, truncated = _bounded_utf8(body.statement, 1_600)
    applicability, cut = _bounded_values(body.applicability, count=5, item_bytes=160)
    truncated = truncated or cut
    assumptions, cut = _bounded_values(body.assumptions, count=5, item_bytes=120)
    truncated = truncated or cut
    basis, cut = _bounded_values(body.confidence.basis, count=5, item_bytes=120)
    truncated = truncated or cut
    missing, cut = _bounded_values(body.confidence.missing_evidence, count=5, item_bytes=120)
    truncated = truncated or cut
    confidence_level, cut = _bounded_utf8(body.confidence.level, 48)
    truncated = truncated or cut
    uncertainty, cut = _bounded_utf8(body.uncertainty or "", 240)
    truncated = truncated or cut
    reason, cut = _bounded_utf8(view.reason_for_change, 320)
    truncated = truncated or cut
    value = {
        "format_version": _FORMAT_VERSION,
        "scope": str(view.scope),
        "topic_id": str(view.topic_id),
        "source_version": view.version,
        "statement": statement,
        "applicability": applicability,
        "confidence": {"level": confidence_level, "basis": basis, "missing_evidence": missing},
        "assumptions": assumptions,
        "uncertainty": uncertainty or None,
        "truncated": truncated,
        "provenance": {
            "task_id": str(view.task_id),
            "input_revision": view.input_revision,
            "registry_id": str(view.registry_id),
            "conclusion_id": str(view.conclusion_id),
            "position_operation_id": str(view.operation_id),
            "reason_for_change": reason,
            "evidence_refs": sorted(map(str, view.evidence_refs))[:12],
            "dissent_refs": sorted(map(str, view.dissent_refs))[:12],
            "created_at": view.created_at.isoformat(),
        },
    }
    payload = canonical_json(value)
    if len(payload.encode("utf-8")) > _MAX_ENTRY_BYTES:
        # Keep the source decision readable, but always make truncation explicit.
        value["statement"], _ = _bounded_utf8(statement, 800)
        value["applicability"] = applicability[:3]
        value["assumptions"] = assumptions[:3]
        value["confidence"]["basis"] = basis[:3]
        value["confidence"]["missing_evidence"] = missing[:3]
        value["truncated"] = True
        payload = canonical_json(value)
    if len(payload.encode("utf-8")) > _MAX_ENTRY_BYTES:
        raise ValueError("Position provenance exceeds the bounded runtime memory entry")
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return payload, digest


def _request_identity(scope, registry_id, provider_id, topic_id, version, applied_version, digest, actor: ActorContext) -> dict[str, object]:
    return {
        "scope": str(scope),
        "registry_id": str(registry_id),
        "provider_agent_id": str(provider_id),
        "principal_id": str(actor.principal_id),
        "policy_version": actor.policy_version,
        "authz_epoch": actor.authz_epoch,
        "namespace": _NAMESPACE,
        "topic_id": str(topic_id),
        "source_version": version,
        "base_applied_version": applied_version,
        "format_version": _FORMAT_VERSION,
        "payload_digest": digest,
    }


def _observe(value: MemoryProjectionObservation | Mapping[str, object]) -> MemoryProjectionObservation:
    observed = value if isinstance(value, MemoryProjectionObservation) else MemoryProjectionObservation.model_validate(value, strict=True)
    if not observed.verified:
        raise ValueError("runtime memory observation was not verified")
    if observed.present:
        if observed.payload is None or observed.payload_digest is None:
            raise ValueError("runtime memory observation omitted its bounded payload")
        digest = hashlib.sha256(observed.payload.encode("utf-8")).hexdigest()
        if digest != observed.payload_digest:
            raise ValueError("runtime memory observation payload digest mismatch")
        if canonical_json(json.loads(observed.payload)) != observed.payload:
            raise ValueError("runtime memory observation payload is not canonical")
    elif observed.source_version != 0 or observed.payload is not None or observed.payload_digest is not None:
        raise ValueError("empty runtime memory observation contains an entry")
    return observed


async def _current_auth(uow, actor: ActorContext) -> None:
    auth = await uow.tasks.lock_scope(actor.scope)
    if (auth.principal_id, auth.policy_version, auth.authz_epoch) != (
        actor.principal_id, actor.policy_version, actor.authz_epoch,
    ):
        raise PolicyDenied("projection_authorization_snapshot_changed")


async def _defer(factory: UowFactory, job: ProjectionJob, reason: str, *, seconds: int = _RETRY_SECONDS, operation_id=None, state: str = "PENDING") -> None:
    async with factory() as uow:
        await uow.projections.fail(job, operation_id, reason, state=state, delay_seconds=seconds)
        await uow.commit()


async def claim_pending_projections(
    factory: UowFactory, actor: ActorContext, worker: str, *, enabled: bool, limit: int = 4,
) -> tuple[ProjectionJob, ...]:
    if not enabled:
        return ()
    async with factory() as uow:
        await uow.projections.enable_pending(limit=max(100, limit))
        jobs = await uow.projections.claim_due(worker, actor.scope, limit)
        await uow.commit()
        return tuple(jobs)


async def _prepare(
    factory: UowFactory, actor: ActorContext, job: ProjectionJob,
) -> tuple[MemoryProjection, object] | None:
    if actor.scope != job.scope:
        raise PolicyDenied("projection_scope_mismatch")
    async with factory() as uow:
        await _current_auth(uow, actor)
        registry = await uow.agents.lock_registry(job.target_registry_id)
        if (
            registry.registry_id != job.target_registry_id or registry.owner_scope != job.scope
            or registry.kind != "hekate" or registry.persistence != "persistent"
            or registry.provider_id is None or registry.policy_version != actor.policy_version
        ):
            await uow.projections.fail(job, None, "persistent_HEKATE_binding_or_policy_mismatch", state="DRIFT", delay_seconds=0)
            await uow.commit()
            return None
        if registry.intended_state != "READY" or registry.observation != "PRESENT":
            await uow.projections.fail(job, None, "persistent_HEKATE_is_not_ready", state="PENDING", delay_seconds=5)
            await uow.commit()
            return None
        hold = await uow.agents.active_execution_hold(registry.registry_id, lock=True)
        if hold is not None:
            reason = "persistent_HEKATE_has_unresolved_execution_" + str(hold["state"]).lower()
            await uow.projections.fail(job, None, reason, state="PENDING", delay_seconds=5)
            await uow.commit()
            return None
        lease = await uow.agents.acquire_lease(registry.registry_id, job.worker_id, _REGISTRY_LEASE_SECONDS)
        if lease is None:
            await uow.projections.fail(job, None, "persistent_HEKATE_lease_busy", state="PENDING", delay_seconds=2)
            await uow.commit()
            return None
        current_version = await uow.knowledge.lock_topic(job.scope, job.topic_id)
        position = await uow.knowledge.get_current_position(job.scope, job.topic_id, registry.registry_id)
        if current_version <= 0 or position.current is None:
            await uow.agents.release_lease(lease)
            await uow.projections.fail(job, None, "authoritative_Position_is_missing", state="DRIFT", delay_seconds=0)
            await uow.commit()
            return None
        payload, payload_digest = _payload(position.current)
        projection_status = await uow.projections.get_status(job.scope, job.topic_id, registry.registry_id)
        if projection_status is None:
            raise StaleInput("Position projection state disappeared")
        base_applied_version = int(projection_status["applied_version"])
        request_value = _request_identity(
            job.scope, registry.registry_id, registry.provider_id, job.topic_id,
            current_version, base_applied_version, payload_digest, actor,
        )
        request_hash = canonical_json_hash(request_value)
        operation_id = OperationId(str(uuid5(NAMESPACE_URL, "hekate:projection:" + request_hash)))
        binding = ProjectionBinding(
            scope=job.scope, registry_id=registry.registry_id, provider_agent_id=registry.provider_id,
            creation_operation_id=registry.creation_operation_id, authz_epoch=actor.authz_epoch,
            policy_version=actor.policy_version, principal_id=actor.principal_id, fence=job.fence,
        )
        projection = MemoryProjection(
            operation_id=operation_id, request_hash=request_hash, binding=binding,
            topic_id=job.topic_id, source_version=current_version,
            base_applied_version=base_applied_version,
            format_version=_FORMAT_VERSION, payload=payload, payload_digest=payload_digest,
        )
        await uow.projections.start_operation(
            job, operation_id, request_hash, payload_digest, current_version, base_applied_version,
        )
        await uow.commit()
        return projection, lease


async def _complete(
    factory: UowFactory, actor: ActorContext, job: ProjectionJob,
    projection: MemoryProjection, observation: MemoryProjectionObservation, lease,
) -> ProjectionReceipt:
    async with factory() as uow:
        await _current_auth(uow, actor)
        registry = await uow.agents.lock_registry(job.target_registry_id)
        if (
            registry.kind != "hekate" or registry.persistence != "persistent"
            or registry.owner_scope != job.scope or registry.provider_id != projection.binding.provider_agent_id
            or registry.creation_operation_id != projection.binding.creation_operation_id
            or registry.policy_version != actor.policy_version
        ):
            raise PolicyDenied("projection_registry_changed_before_confirmation")
        await uow.agents.assert_current_lease(registry.registry_id, job.worker_id, lease.fence)
        if await uow.agents.active_execution_hold(registry.registry_id, lock=True) is not None:
            raise StaleInput("persistent HEKATE acquired an execution before projection confirmation")
        current_version = await uow.knowledge.lock_topic(job.scope, job.topic_id)
        current = await uow.knowledge.get_current_position(job.scope, job.topic_id, registry.registry_id)
        if current.current is None:
            raise StaleInput("authoritative Position disappeared before projection confirmation")
        if observation.topic_id != job.topic_id:
            raise ValueError("runtime memory read-back returned a different topic")
        _, current_digest = _payload(current.current)
        newly_confirmed = await uow.projections.finish(
            job, projection.operation_id, observation.model_dump(mode="json"),
            requested_version=projection.source_version, requested_digest=projection.payload_digest,
            current_version=current_version, current_digest=current_digest,
        )
        if newly_confirmed:
            await uow.delivery.append_audit({
                "owner_scope": str(job.scope),
                # audit_events.operation_id references the runtime operation table;
                # the projection operation is retained as safe audit context.
                "operation_id": str(job.original_operation_id),
                "registry_id": str(job.target_registry_id),
                "event_kind": "position.memory_projection.confirmed",
                "safe_payload": {
                    "projection_operation_id": str(projection.operation_id),
                    "topic_id": str(job.topic_id),
                    "source_version": projection.source_version,
                    "payload_digest": projection.payload_digest,
                    "observed_memory_version": observation.source_version,
                    "memory_revision": observation.memory_revision,
                },
            })
        status = await uow.projections.get_status(job.scope, job.topic_id, job.target_registry_id)
        await uow.commit()
    return ProjectionReceipt(
        operation_id=projection.operation_id, request_hash=projection.request_hash,
        scope=job.scope, topic_id=job.topic_id, registry_id=job.target_registry_id,
        source_version=projection.source_version, payload_digest=projection.payload_digest,
        observed_version=observation.source_version, observed_digest=observation.payload_digest,
        state=status["state"], replayed=not newly_confirmed,
    )


async def project_position(
    factory: UowFactory, runtime: AgentRuntime, actor: ActorContext, job: ProjectionJob,
) -> ProjectionReceipt | None:
    lease = None
    projection = None
    try:
        prepared = await _prepare(factory, actor, job)
        if prepared is None:
            return None
        projection, lease = prepared
        read = _observe(await runtime.read_projected_memory(projection.binding, projection.topic_id, projection.operation_id))
        if read.present and read.source_version == projection.source_version and read.payload_digest == projection.payload_digest:
            observed = read
        elif read.present and read.source_version == projection.source_version and read.payload_digest != projection.payload_digest:
            await _defer(factory, job, "runtime_has_same_version_with_different_digest", seconds=0, operation_id=projection.operation_id, state="DRIFT")
            return ProjectionReceipt(
                operation_id=projection.operation_id, request_hash=projection.request_hash,
                scope=job.scope, topic_id=job.topic_id, registry_id=job.target_registry_id,
                source_version=projection.source_version, payload_digest=projection.payload_digest,
                observed_version=read.source_version, observed_digest=read.payload_digest,
                state="DRIFT", replayed=False,
            )
        elif read.present and read.source_version > projection.source_version:
            observed = read
        else:
            try:
                await runtime.project_memory(projection)
            except Exception as write_error:
                try:
                    observed = _observe(await runtime.read_projected_memory(projection.binding, projection.topic_id, projection.operation_id))
                except Exception:
                    await _defer(factory, job, "runtime_memory_write_outcome_unknown:" + type(write_error).__name__, seconds=10, operation_id=projection.operation_id, state="UNKNOWN")
                    return None
            else:
                try:
                    observed = _observe(await runtime.read_projected_memory(projection.binding, projection.topic_id, projection.operation_id))
                except Exception as read_error:
                    await _defer(
                        factory, job, "runtime_memory_write_readback_unknown:" + type(read_error).__name__,
                        seconds=10, operation_id=projection.operation_id, state="UNKNOWN",
                    )
                    return None
        receipt = await _complete(factory, actor, job, projection, observed, lease)
        return receipt
    except (PolicyDenied, StaleInput) as error:
        try:
            await _defer(factory, job, str(error) or type(error).__name__, seconds=60, operation_id=projection.operation_id if projection else None)
        except StaleInput:
            pass
        return None
    except Exception as error:
        cause = error.__cause__
        database_error = getattr(cause, "orig", cause)
        diagnostic = getattr(database_error, "diag", None)
        _LOG.error(
            "Position memory projection failed for %s/%s: %s (cause=%s sqlstate=%s constraint=%s)",
            job.scope, job.topic_id, type(error).__name__,
            type(cause).__name__ if cause is not None else None,
            getattr(database_error, "sqlstate", getattr(database_error, "pgcode", None)),
            getattr(diagnostic, "constraint_name", None),
        )
        try:
            await _defer(factory, job, type(error).__name__, seconds=10, operation_id=projection.operation_id if projection else None)
        except Exception:
            pass
        return None
    finally:
        if lease is not None:
            try:
                async with factory() as uow:
                    await uow.agents.release_lease(lease)
                    await uow.commit()
            except Exception as error:
                _LOG.warning("Position projection registry lease release failed: %s", type(error).__name__)


async def maintain_position_projections(
    factory: UowFactory, runtime: AgentRuntime, actor: ActorContext, worker: str, *,
    enabled: bool, limit: int = 4,
) -> tuple[ProjectionReceipt, ...]:
    jobs = await claim_pending_projections(factory, actor, worker, enabled=enabled, limit=limit)
    receipts = []
    for job in jobs:
        receipt = await project_position(factory, runtime, actor, job)
        if receipt is not None:
            receipts.append(receipt)
    return tuple(receipts)


async def verify_projection(
    factory: UowFactory, runtime: AgentRuntime, actor: ActorContext, topic_id: TopicId,
) -> ProjectionStatus:
    async with factory() as uow:
        await _current_auth(uow, actor)
        registry = await uow.agents.get_persistent_scope(actor.scope)
        if registry is None or registry.provider_id is None:
            raise PolicyDenied("persistent HEKATE memory target is unavailable")
        status = await uow.projections.get_status(actor.scope, topic_id, registry.registry_id)
        if status is None:
            raise StaleInput("Position projection intent does not exist")
        lease = await uow.agents.acquire_lease(registry.registry_id, "projection-verifier", _REGISTRY_LEASE_SECONDS)
        if lease is None:
            raise StaleInput("persistent HEKATE is busy")
        binding = ProjectionBinding(
            scope=actor.scope, registry_id=registry.registry_id, provider_agent_id=registry.provider_id,
            creation_operation_id=registry.creation_operation_id, authz_epoch=actor.authz_epoch,
            policy_version=actor.policy_version, principal_id=actor.principal_id,
            fence=max(1, int(status["claim_fence"])),
        )
        opid = OperationId(str(status["operation_id"] or "projection-verify:" + str(topic_id)))
        await uow.commit()
    try:
        observation = _observe(await runtime.read_projected_memory(binding, topic_id, opid))
        async with factory() as uow:
            latest = await uow.projections.get_status(actor.scope, topic_id, registry.registry_id)
            current = await uow.knowledge.get_current_position(actor.scope, topic_id, registry.registry_id)
            matches = current.current is not None and observation.present and observation.source_version == current.current.version
            digest = _payload(current.current)[1] if current.current else None
            matches = matches and observation.payload_digest == digest
            await uow.commit()
        return ProjectionStatus(
            scope=actor.scope, topic_id=topic_id, registry_id=registry.registry_id,
            desired_version=int(latest["desired_version"]), applied_version=int(latest["applied_version"]),
            observed_version=observation.source_version if observation.present else 0,
            observed_digest=observation.payload_digest, payload_digest=digest,
            state="APPLIED" if matches else "DRIFT",
            reason=None if matches else "runtime_memory_differs_from_current_PostgreSQL_Position",
            operation_id=OperationId(latest["operation_id"]) if latest["operation_id"] else None,
            next_retry_at=latest["next_retry_at"],
        )
    finally:
        async with factory() as uow:
            await uow.agents.release_lease(lease)
            await uow.commit()
