from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from hekate.domain.contracts import canonical_json_hash
from hekate.domain.errors import Conflict, PolicyDenied
from hekate.domain.models import EvidenceInput, EvidenceRecord, EvidenceView, ReadLimits, ReferenceValidation
from hekate.domain.types import ActorContext, EvidenceId, ScopeId
from hekate.infrastructure import archive
from hekate.ports.store import UowFactory


MAX_EVIDENCE_BYTES = 1_048_576


async def _authorized_scope(uow, actor: ActorContext):
    authorization = await uow.tasks.lock_scope(actor.scope)
    if (authorization.principal_id, authorization.policy_version, authorization.authz_epoch) != (
        actor.principal_id, actor.policy_version, actor.authz_epoch,
    ):
        raise PolicyDenied("authorization snapshot changed")
    return authorization


async def _root_sources(uow, actor: ActorContext, source: EvidenceInput) -> tuple[EvidenceId, ...]:
    parents = tuple(sorted(set(source.derived_from)))
    if source.id in parents:
        raise ValueError("Evidence cannot derive from itself")
    records = await uow.knowledge.lock_accessible_references(actor.scope, parents, actor.authz_epoch)
    if len(records) != len(parents):
        raise PolicyDenied("Evidence reference is unavailable")
    roots = tuple(sorted({root for record in records for root in record.root_source_ids}))
    expected = roots or (source.id,)
    if source.root_source_ids and tuple(sorted(set(source.root_source_ids))) != expected:
        raise ValueError("root_source_ids do not match the stored derivation graph")
    if source.id in expected and parents:
        raise ValueError("Evidence derivation contains a cycle")
    return expected


def _source_record(
    source: EvidenceInput, actor: ActorContext, *, digest: str, artifact_ref: str,
    roots: tuple[EvidenceId, ...], registered_at: datetime,
) -> EvidenceRecord:
    if not source.retention_class.strip() or source.expiry_at is None:
        raise ValueError("local Evidence requires an explicit retention_class and expires_at")
    if source.expiry_at.tzinfo is None or source.expiry_at <= registered_at:
        raise ValueError("Evidence expiry must be an aware future timestamp")
    if source.content_hash and source.content_hash != digest:
        raise ValueError("provided content hash does not match the imported bytes")
    version = f"sha256:{digest}"
    if source.content_version is not None and source.content_version != version:
        raise ValueError("provided content_version does not match the imported bytes")
    return EvidenceRecord(
        schema_version="1", id=source.id, scope=actor.scope, kind=source.kind,
        source_uri=source.source_uri, locator=source.locator,
        retrieved_at=source.retrieved_at, observed_at=source.observed_at,
        content_hash=digest, derived_from=tuple(sorted(set(source.derived_from))),
        access_scope=f"scope:{actor.scope}", retention_class=source.retention_class,
        content_version=version, availability="STAGED", access_epoch=actor.authz_epoch,
        root_source_ids=roots, expiry_at=source.expiry_at, artifact_ref=artifact_ref,
        registered_at=registered_at,
    )


def _same_registration(existing: EvidenceRecord, candidate: EvidenceRecord) -> bool:
    return all(getattr(existing, name) == getattr(candidate, name) for name in (
        "scope", "kind", "source_uri", "locator", "retrieved_at", "observed_at",
        "content_hash", "derived_from", "access_scope", "retention_class",
        "content_version", "access_epoch", "root_source_ids", "expiry_at", "artifact_ref",
    ))


async def register(
    factory: UowFactory, actor: ActorContext, source: EvidenceInput, content: bytes,
    archive_root: Path, *, request_key: str | None = None,
) -> EvidenceRecord:
    if request_key is not None and (
        not request_key or len(request_key) > 128
        or any(not char.isprintable() or char.isspace() for char in request_key)
    ):
        raise ValueError("evidence request key must be 1-128 printable non-space characters")
    if len(content) > MAX_EVIDENCE_BYTES:
        raise ValueError(f"Evidence exceeds {MAX_EVIDENCE_BYTES} bytes")
    text = content.decode("utf-8", "strict")
    if not isinstance(text, str):
        raise ValueError("Evidence file must be UTF-8 text")
    digest = hashlib.sha256(content).hexdigest()
    artifact_ref = f"sha256:{digest}"
    request_hash = canonical_json_hash({
        "content_hash": digest,
        "kind": source.kind,
        "source_uri": source.source_uri,
        "locator": source.locator,
        "observed_at": source.observed_at.isoformat() if source.observed_at else None,
        "derived_from": sorted(map(str, source.derived_from)),
        "retention_class": source.retention_class,
        "expires_at": source.expiry_at.isoformat() if source.expiry_at else None,
    })
    now = datetime.now(UTC)
    record: EvidenceRecord | None = None
    async with factory() as uow:
        await _authorized_scope(uow, actor)
        if request_key is not None:
            prior = await uow.knowledge.get_evidence_import(actor.scope, request_key)
            if prior is not None:
                if prior["request_hash"] != request_hash:
                    raise Conflict("evidence request key is already bound to different content")
                rows = await uow.knowledge.get_evidence([EvidenceId(prior["evidence_id"])])
                if len(rows) != 1:
                    raise RuntimeError("Evidence import receipt points to missing metadata")
                record = rows[0]
                await uow.commit()
        if record is None:
            existing = await uow.knowledge.get_evidence([source.id], lock=True)
            roots = await _root_sources(uow, actor, source)
            candidate = _source_record(
                source, actor, digest=digest, artifact_ref=artifact_ref,
                roots=roots, registered_at=now,
            )
            if existing:
                if len(existing) != 1 or not _same_registration(existing[0], candidate):
                    raise Conflict("Evidence ID is already bound to different registration content")
                if existing[0].availability not in {"STAGED", "AVAILABLE"}:
                    raise PolicyDenied("expired or unavailable Evidence cannot be restored")
                record = existing[0]
            else:
                await uow.knowledge.stage_artifact(artifact_ref, digest, len(content), source.retention_class)
                await uow.knowledge.insert_evidence(candidate)
                record = candidate
            if request_key is not None:
                prior = await uow.knowledge.register_evidence_import(actor.scope, request_key, request_hash, record.id)
                if prior["evidence_id"] != record.id:
                    raise Conflict("evidence request key is already bound to another Evidence ID")
            await uow.commit()

    if record.availability != "STAGED":
        return record
    stored_ref = await asyncio.to_thread(archive.put, archive_root, content)
    if stored_ref != record.artifact_ref or not archive.verify_hash(content, digest):
        raise IOError("archived Evidence digest differs from registered content")
    async with factory() as uow:
        await _authorized_scope(uow, actor)
        rows = await uow.knowledge.get_evidence([record.id], lock=True)
        if len(rows) != 1 or rows[0].content_hash != digest or rows[0].access_epoch != actor.authz_epoch:
            raise PolicyDenied("Evidence registration changed during archive write")
        await uow.knowledge.mark_artifact_available(stored_ref)
        updated = await uow.knowledge.get_evidence([record.id])
        await uow.commit()
    return updated[0]


async def read_scoped(
    factory: UowFactory, actor: ActorContext, evidence_id: EvidenceId, limits: ReadLimits,
    archive_root: Path,
) -> EvidenceView:
    if not 0 <= limits.max_bytes <= MAX_EVIDENCE_BYTES:
        raise ValueError("Evidence read limit must be between 0 and 1 MiB")
    async with factory() as uow:
        await _authorized_scope(uow, actor)
        rows = await uow.knowledge.get_evidence([evidence_id])
        if len(rows) != 1 or rows[0].scope != actor.scope:
            raise PolicyDenied("Evidence is unavailable")
        record = rows[0]
        if record.access_epoch != actor.authz_epoch or record.availability != "AVAILABLE" or (
            record.expiry_at is not None and record.expiry_at <= datetime.now(UTC)
        ):
            raise PolicyDenied("Evidence is unavailable")
        artifact = await uow.knowledge.get_artifact(record.artifact_ref) if record.artifact_ref else None
        await uow.commit()
    content: bytes | None = None
    truncated = False
    if artifact is not None and artifact["storage_state"] == "AVAILABLE":
        raw = await asyncio.to_thread(archive.read, archive_root, record.artifact_ref, limits.max_bytes + 1)
        truncated = len(raw) > limits.max_bytes
        content = raw[:limits.max_bytes].decode("utf-8", "ignore")
    async with factory() as uow:
        await _authorized_scope(uow, actor)
        current = await uow.knowledge.get_evidence([evidence_id], lock=True)
        if len(current) != 1 or current[0] != record or (
            record.expiry_at is not None and record.expiry_at <= datetime.now(UTC)
        ):
            raise PolicyDenied("Evidence changed while it was being read")
        await uow.commit()
    return EvidenceView(
        id=record.id, kind=record.kind, source_uri=record.source_uri, locator=record.locator,
        retrieved_at=record.retrieved_at, observed_at=record.observed_at,
        content_hash=record.content_hash, derived_from=record.derived_from,
        root_source_ids=record.root_source_ids, scope=record.scope,
        content_version=record.content_version, access_epoch=record.access_epoch,
        availability=record.availability, retention_class=record.retention_class,
        expiry_at=record.expiry_at, content=content, truncated=truncated,
        readable=content is not None, revalidatable=record.source_uri.startswith(("https://", "http://")),
    )


async def read_many_scoped(
    factory: UowFactory, actor: ActorContext, ids: Sequence[EvidenceId],
    archive_root: Path, *, max_total_bytes: int = 32_768,
) -> tuple[EvidenceView, ...]:
    chosen = tuple(sorted(set(ids)))
    if max_total_bytes < 0 or max_total_bytes > 32_768:
        raise ValueError("Task Evidence excerpt limit must be between 0 and 32 KiB")
    async with factory() as uow:
        await _authorized_scope(uow, actor)
        records = await uow.knowledge.get_evidence(chosen)
        if len(records) != len(chosen) or any(
            row.scope != actor.scope or row.access_epoch != actor.authz_epoch
            or row.availability != "AVAILABLE"
            or (row.expiry_at is not None and row.expiry_at <= datetime.now(UTC))
            for row in records
        ):
            raise PolicyDenied("Evidence reference is unavailable")
        artifacts = {row.artifact_ref: await uow.knowledge.get_artifact(row.artifact_ref) for row in records if row.artifact_ref}
        await uow.commit()
    if any(
        row.artifact_ref is None or not artifacts.get(row.artifact_ref)
        or artifacts[row.artifact_ref]["storage_state"] != "AVAILABLE"
        for row in records
    ):
        raise PolicyDenied("Evidence archive content is unavailable")
    views = []
    remaining = max_total_bytes
    for row in records:
        content = None
        truncated = False
        artifact = artifacts.get(row.artifact_ref) if row.artifact_ref else None
        if artifact is not None and artifact["storage_state"] == "AVAILABLE":
            available = max(0, remaining)
            raw = await asyncio.to_thread(archive.read, archive_root, row.artifact_ref, available + 1)
            truncated = len(raw) > available or artifact["byte_size"] > len(raw)
            excerpt = raw[:available]
            remaining -= len(excerpt)
            content = excerpt.decode("utf-8", "ignore")
        views.append(EvidenceView(
            id=row.id, kind=row.kind, source_uri=row.source_uri, locator=row.locator,
            retrieved_at=row.retrieved_at, observed_at=row.observed_at,
            content_hash=row.content_hash, derived_from=row.derived_from,
            root_source_ids=row.root_source_ids, scope=row.scope,
            content_version=row.content_version, access_epoch=row.access_epoch,
            availability=row.availability, retention_class=row.retention_class,
            expiry_at=row.expiry_at, content=content, truncated=truncated,
            readable=bool(artifact and artifact["storage_state"] == "AVAILABLE"),
            revalidatable=row.source_uri.startswith(("http://", "https://")),
        ))
    async with factory() as uow:
        await _authorized_scope(uow, actor)
        current = await uow.knowledge.lock_accessible_references(actor.scope, chosen, actor.authz_epoch)
        if len(current) != len(records) or any(
            before.id != after.id or before.content_version != after.content_version or before.access_epoch != after.access_epoch
            for before, after in zip(records, current, strict=True)
        ):
            raise PolicyDenied("Evidence reference changed while preparing the Task")
        await uow.commit()
    return tuple(views)


async def resolve_references(
    factory: UowFactory, actor: ActorContext, ids: Sequence[EvidenceId],
) -> ReferenceValidation:
    chosen = tuple(sorted(set(ids)))
    async with factory() as uow:
        await _authorized_scope(uow, actor)
        rows = await uow.knowledge.lock_accessible_references(actor.scope, chosen, actor.authz_epoch)
        artifacts = {row.artifact_ref: await uow.knowledge.get_artifact(row.artifact_ref) for row in rows if row.artifact_ref}
        await uow.commit()
    if len(rows) != len(chosen):
        return ReferenceValidation(valid=False, reason="reference_unavailable")
    return ReferenceValidation(valid=True, references=tuple(EvidenceView(
        id=row.id, kind=row.kind, source_uri=row.source_uri, locator=row.locator,
        retrieved_at=row.retrieved_at, observed_at=row.observed_at,
        content_hash=row.content_hash, derived_from=row.derived_from,
        root_source_ids=row.root_source_ids, scope=row.scope,
        content_version=row.content_version, access_epoch=row.access_epoch,
        availability=row.availability, retention_class=row.retention_class,
        expiry_at=row.expiry_at, content=None, truncated=False,
        readable=bool(artifacts.get(row.artifact_ref) and artifacts[row.artifact_ref]["storage_state"] == "AVAILABLE"),
        revalidatable=row.source_uri.startswith(("https://", "http://")),
    ) for row in rows))


async def manifest_references_current(uow, scope: ScopeId, access_epoch: int, manifest, referenced_ids=()) -> bool:
    evidence = manifest.get("evidence", []) if isinstance(manifest, dict) else []
    if not isinstance(evidence, list):
        return False
    by_id = {item.get("id"): item for item in evidence if isinstance(item, dict)}
    if len(by_id) != len(evidence) or any(str(value) not in by_id for value in referenced_ids):
        return False
    ids = tuple(EvidenceId(value) for value in by_id)
    current = await uow.knowledge.lock_accessible_references(scope, ids, access_epoch)
    if len(current) != len(ids):
        return False
    for record in current:
        prior = by_id.get(str(record.id))
        if prior is None or prior.get("content_version") != record.content_version or prior.get("access_epoch") != record.access_epoch:
            return False
    return True


async def expire(
    factory: UowFactory, archive_root: Path, now: datetime, limit: int,
) -> dict[str, object]:
    if limit < 1:
        raise ValueError("expiry batch limit must be positive")
    async with factory() as uow:
        expired = await uow.knowledge.expire_evidence(now, limit)
        await uow.commit()
    async with factory() as uow:
        pending = await uow.knowledge.pending_archive_deletions(limit)
        await uow.commit()
    deleted, failed = [], []
    for item in pending:
        try:
            await asyncio.to_thread(archive.delete, archive_root, item["artifact_ref"])
        except (OSError, ValueError):
            async with factory() as uow:
                await uow.knowledge.finish_archive_delete(item["artifact_ref"], deleted=False)
                await uow.commit()
            failed.append(item["artifact_ref"])
        else:
            async with factory() as uow:
                await uow.knowledge.finish_archive_delete(item["artifact_ref"], deleted=True)
                await uow.commit()
            deleted.append(item["artifact_ref"])
    return {"expired_evidence": len(expired), "deleted_artifacts": deleted, "failed_artifacts": failed}


async def find_exact_reuse(*args, **kwargs):
    raise NotImplementedError("Conclusion reuse is outside Phase 4")
