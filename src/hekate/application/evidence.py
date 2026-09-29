from __future__ import annotations

from collections.abc import Sequence

from hekate.domain.models import (
    ArtifactRef, EvidenceInput, EvidenceRecord, EvidenceView, ReadLimits,
    ReferenceValidation, RetentionReport, ReuseInputs, StoredConclusion,
)
from hekate.domain.types import ActorContext, EvidenceId, Instant


async def register(
    actor: ActorContext, source: EvidenceInput, artifact: ArtifactRef | None
) -> EvidenceRecord:
    raise NotImplementedError


async def read_scoped(
    actor: ActorContext, evidence_id: EvidenceId, limits: ReadLimits
) -> EvidenceView:
    raise NotImplementedError


async def resolve_references(
    actor: ActorContext, ids: Sequence[EvidenceId]
) -> ReferenceValidation:
    raise NotImplementedError


async def expire(now: Instant, limit: int) -> RetentionReport:
    raise NotImplementedError


async def find_exact_reuse(
    actor: ActorContext, inputs: ReuseInputs
) -> StoredConclusion | None:
    raise NotImplementedError
