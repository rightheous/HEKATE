from __future__ import annotations

from hekate.domain.models import (
    CommitReceipt, Page, PositionCommitRequest, PositionView, VersionCursor,
)
from hekate.domain.types import ActorContext, TopicId


async def propose_commit(
    actor: ActorContext, request: PositionCommitRequest
) -> CommitReceipt:
    raise NotImplementedError


async def read_current(actor: ActorContext, topic_id: TopicId) -> PositionView:
    raise NotImplementedError


async def read_history(
    actor: ActorContext, topic_id: TopicId, cursor: VersionCursor
) -> Page[PositionView]:
    raise NotImplementedError
