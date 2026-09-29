from __future__ import annotations

from collections.abc import Sequence

from hekate.domain.models import EvidenceRecord, PositionVersionRecord, StoredConclusion
from hekate.domain.types import EvidenceId, ScopeId, TaskId, TopicId


class PostgresKnowledgeRepository:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def lock_topic(self, scope: ScopeId, topic_id: TopicId) -> object: raise NotImplementedError
    async def append_position(self, record: PositionVersionRecord) -> None: raise NotImplementedError
    async def cas_current(self, topic_id: TopicId, base: int, new: int) -> bool: raise NotImplementedError
    async def insert_evidence(self, record: EvidenceRecord) -> None: raise NotImplementedError
    async def get_evidence(self, ids: Sequence[EvidenceId], lock: bool = False) -> Sequence[EvidenceRecord]: raise NotImplementedError
    async def lock_accessible_references(self, refs: Sequence[EvidenceId]) -> object: raise NotImplementedError
    async def insert_conclusion(self, record: StoredConclusion) -> None: raise NotImplementedError
    async def get_eligible_conclusions(self, task_id: TaskId, revision: int) -> Sequence[StoredConclusion]: raise NotImplementedError
    async def set_projection_watermark(self, target: object, version: int) -> None: raise NotImplementedError
