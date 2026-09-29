from __future__ import annotations

from typing import Protocol

from hekate.domain.models import ArtifactRef, AuditEvent, DeletionReceipt
from hekate.domain.types import Instant


class ArchiveStore(Protocol):
    async def put(self, content: bytes, metadata: object) -> ArtifactRef: ...
    async def read(self, ref: ArtifactRef, limit: int) -> bytes: ...
    async def delete(self, ref: ArtifactRef) -> DeletionReceipt: ...


class Clock(Protocol):
    def now(self) -> Instant: ...
    def monotonic(self) -> float: ...


class AuditSink(Protocol):
    async def emit(self, event: AuditEvent) -> None: ...
