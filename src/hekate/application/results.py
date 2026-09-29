from __future__ import annotations

from hekate.domain.models import (
    AuditRef, IngestReceipt, RejectionReason, ResultDisposition, RuntimeEvent,
    ValidationReport,
)
from hekate.domain.types import InboxId


async def ingest(event: RuntimeEvent) -> IngestReceipt:
    raise NotImplementedError


async def validate_result(inbox_id: InboxId) -> ValidationReport:
    raise NotImplementedError


async def accept_result(
    inbox_id: InboxId, report: ValidationReport
) -> ResultDisposition:
    raise NotImplementedError


async def record_late_result(
    inbox_id: InboxId, reason: RejectionReason
) -> AuditRef:
    raise NotImplementedError
