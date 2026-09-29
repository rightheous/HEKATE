from __future__ import annotations

from hekate.domain.models import ArtifactRef, DeletionReceipt


async def put(content: bytes, metadata: object) -> ArtifactRef:
    raise NotImplementedError


async def read(ref: ArtifactRef, limit: int) -> bytes:
    raise NotImplementedError


async def delete(ref: ArtifactRef) -> DeletionReceipt:
    raise NotImplementedError


def verify_hash(content: bytes, digest: str) -> bool:
    raise NotImplementedError
