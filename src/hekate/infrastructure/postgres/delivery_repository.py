from __future__ import annotations

from collections.abc import Sequence

from hekate.domain.models import (
    AuditEvent, Job, Operation, OperationClaim, Page, PublicEvent,
)
from hekate.domain.types import OperationId, TaskId


class PostgresDeliveryRepository:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def claim_operation(self, operation_id: OperationId, request_hash: str) -> OperationClaim: raise NotImplementedError
    async def lock_operation(self, operation_id: OperationId, request_hash: str) -> Operation: raise NotImplementedError
    async def store_commit_receipt(self, operation_id: OperationId, request_hash: str, receipt: object) -> None: raise NotImplementedError
    async def update_operation(self, operation_id: OperationId, expected: object, observation: object) -> None: raise NotImplementedError
    async def append_outbox(self, job: Job) -> None: raise NotImplementedError
    async def claim_jobs(self, worker: str, limit: int, lease: float) -> Sequence[Job]: raise NotImplementedError
    async def ack_job(self, job: Job, fence: int) -> None: raise NotImplementedError
    async def reschedule_job(self, job: Job, error: str, delay: float) -> None: raise NotImplementedError
    async def insert_inbox_once(self, event: object) -> object: raise NotImplementedError
    async def append_audit(self, event: AuditEvent) -> None: raise NotImplementedError
    async def read_public_events(self, task_id: TaskId, after: object) -> Page[PublicEvent]: raise NotImplementedError
