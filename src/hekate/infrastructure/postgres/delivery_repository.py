from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta

from sqlalchemy import exists, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.errors import Conflict, StaleInput, UnknownExecution
from hekate.domain.models import (
    AdmissionReceipt,
    ExecutionObservation,
    InboxReceipt,
    OperationClaim,
    OutboxJob,
)
from hekate.domain.types import AttemptId, OperationId, ReservationId, ScopeId, TaskId

from . import tables
from .common import aware_now, json_value, new_key


class PostgresDeliveryRepository:
    def __init__(self, connection: AsyncSession) -> None:
        self.connection = connection

    async def claim_lifecycle_operation(self, operation_id: OperationId, owner_scope: ScopeId, kind: str, request_hash: str):
        now = aware_now()
        inserted = await self.connection.execute(pg_insert(tables.operations).values(
            id=operation_id,
            owner_scope=owner_scope,
            task_id=None,
            kind=kind,
            request_hash=request_hash,
            state="CLAIMED",
            dispatch_state="NOT_STARTED",
            execution_state="PENDING",
            binding={},
            envelope={},
            observation={},
            created_at=now,
            updated_at=now,
        ).on_conflict_do_nothing(index_elements=[tables.operations.c.id]).returning(tables.operations.c.id))
        created_now = inserted.scalar_one_or_none() is not None
        row = (await self.connection.execute(select(tables.operations).where(
            tables.operations.c.id == operation_id,
        ).with_for_update())).mappings().one_or_none()
        if row is None or row["owner_scope"] != owner_scope or row["kind"] != kind or row["request_hash"] != request_hash:
            raise Conflict("lifecycle operation identity changed")
        result = dict(row)
        result["created_now"] = created_now
        return result

    async def mark_lifecycle_started(self, operation_id: OperationId, kind: str) -> bool:
        row = (await self.connection.execute(select(tables.operations).where(
            tables.operations.c.id == operation_id,
        ).with_for_update())).mappings().one_or_none()
        if row is None or row["kind"] != kind:
            raise Conflict("lifecycle operation binding changed")
        observation = dict(row["observation"] or {})
        if observation.get("external_call_started") is True:
            return False
        observation["external_call_started"] = True
        await self.connection.execute(update(tables.operations).where(
            tables.operations.c.id == operation_id,
        ).values(observation=observation, updated_at=aware_now()))
        return True

    async def complete_lifecycle_operation(self, operation_id: OperationId, observation: Mapping[str, object]) -> None:
        result = await self.connection.execute(update(tables.operations).where(
            tables.operations.c.id == operation_id,
            tables.operations.c.state.in_(["CLAIMED", "UNKNOWN", "COMPLETED"]),
        ).values(
            state="COMPLETED", dispatch_state="QUIESCENT", execution_state="QUIESCENT",
            observation=json_value(observation), last_error=None, updated_at=aware_now(),
        ))
        if result.rowcount != 1:
            raise StaleInput("lifecycle operation cannot be confirmed")

    async def mark_lifecycle_unknown(self, operation_id: OperationId, reason: str) -> None:
        row = (await self.connection.execute(select(tables.operations).where(
            tables.operations.c.id == operation_id,
        ).with_for_update())).mappings().one_or_none()
        if row is None:
            raise StaleInput("lifecycle operation is unavailable")
        observation = dict(row["observation"] or {})
        observation["external_call_started"] = True
        await self.connection.execute(update(tables.operations).where(
            tables.operations.c.id == operation_id,
        ).values(state="UNKNOWN", dispatch_state="UNKNOWN", execution_state="UNKNOWN",
                 observation=observation, last_error=reason[:240], updated_at=aware_now()))

    async def claim_operation(
        self,
        operation_id: OperationId,
        owner_scope: ScopeId,
        task_id: TaskId,
        kind: str,
        request_hash: str,
        binding: Mapping[str, object],
        envelope: Mapping[str, object],
        workflow_stage_hash: str | None = None,
    ) -> OperationClaim:
        now = aware_now()
        await self.connection.execute(pg_insert(tables.operations).values(
            id=operation_id,
            owner_scope=owner_scope,
            task_id=task_id,
            kind=kind,
            request_hash=request_hash,
            state="CLAIMED",
            dispatch_state="NOT_STARTED",
            execution_state="PENDING",
            binding=json_value(binding),
            envelope=json_value(envelope),
            observation={},
            created_at=now,
            updated_at=now,
        ).on_conflict_do_nothing(index_elements=[tables.operations.c.id]))
        row = (await self.connection.execute(select(tables.operations).where(
            tables.operations.c.id == operation_id,
        ).with_for_update())).mappings().one_or_none()
        if row is None:
            raise Conflict("operation claim could not be acquired")
        deferred_hash = (row["observation"] or {}).get("deferred_stage_hash")
        deferred_task_id = (row["observation"] or {}).get("deferred_task_id")
        if (
            workflow_stage_hash is not None
            and deferred_hash == workflow_stage_hash
            and deferred_task_id == str(task_id)
            and row["state"] == "CLAIMED"
            and row["binding"] == {} and row["envelope"] == {}
            and row["task_id"] in {None, task_id} and row["kind"] == kind
        ):
            await self.connection.execute(update(tables.operations).where(
                tables.operations.c.id == operation_id,
                tables.operations.c.state == "CLAIMED",
                tables.operations.c.binding == {},
                tables.operations.c.envelope == {},
            ).values(
                task_id=task_id,
                request_hash=request_hash,
                binding=json_value(binding),
                envelope=json_value(envelope),
                observation={"workflow_stage_hash": workflow_stage_hash},
                updated_at=now,
            ))
            row = (await self.connection.execute(select(tables.operations).where(
                tables.operations.c.id == operation_id,
            ).with_for_update())).mappings().one()
        if (
            row["owner_scope"] != owner_scope
            or row["task_id"] != task_id
            or row["request_hash"] != request_hash
            or row["binding"] != json_value(binding)
            or row["envelope"] != json_value(envelope)
        ):
            raise Conflict("operation id is already bound to a different request")
        receipt = self._receipt(row["receipt"]) if row["receipt"] else None
        return OperationClaim(
            operation_id=OperationId(row["id"]),
            owner_scope=ScopeId(row["owner_scope"]),
            request_hash=row["request_hash"],
            state=row["state"],
            receipt=receipt,
        )

    async def claim_deferred_operation(
        self, operation_id: OperationId, owner_scope: ScopeId, task_id: TaskId,
        kind: str, stage_hash: str,
    ) -> None:
        now = aware_now()
        await self.connection.execute(pg_insert(tables.operations).values(
            id=operation_id, owner_scope=owner_scope, task_id=None, kind=kind,
            request_hash=stage_hash, state="CLAIMED", dispatch_state="NOT_STARTED",
            execution_state="PENDING", binding={}, envelope={}, receipt=None,
            observation={"deferred_stage_hash": stage_hash, "deferred_task_id": str(task_id)}, created_at=now, updated_at=now,
        ).on_conflict_do_nothing(index_elements=[tables.operations.c.id]))
        row = (await self.connection.execute(select(tables.operations).where(
            tables.operations.c.id == operation_id,
        ).with_for_update())).mappings().one_or_none()
        if row is None or (
            row["owner_scope"] != owner_scope or row["task_id"] is not None
            or row["kind"] != kind or row["observation"].get("deferred_stage_hash") != stage_hash
            or row["observation"].get("deferred_task_id") != str(task_id)
            or row["binding"] != {} or row["envelope"] != {}
        ):
            raise Conflict("deferred operation identity changed")

    async def reject_deferred_operation(
        self, operation_id: OperationId, task_id: TaskId, stage_hash: str, reason: str,
    ) -> bool:
        """Seal a workflow step only while its durable claim proves no admission occurred."""
        row = (await self.connection.execute(select(tables.operations).where(
            tables.operations.c.id == operation_id,
        ).with_for_update())).mappings().one_or_none()
        if row is None:
            raise StaleInput("deferred workflow operation is unavailable")
        observation = dict(row["observation"] or {})
        if (
            row["state"] == "FAILED"
            and observation.get("rejected_before_admission") == reason
            and observation.get("deferred_task_id") == str(task_id)
            and observation.get("deferred_stage_hash") == stage_hash
        ):
            return False
        if (
            row["state"] != "CLAIMED" or row["execution_state"] != "PENDING"
            or row["dispatch_state"] != "NOT_STARTED" or row["task_id"] is not None
            or row["binding"] != {} or row["envelope"] != {}
            or observation.get("deferred_task_id") != str(task_id)
            or observation.get("deferred_stage_hash") != stage_hash
            or observation.get("external_call_started") is True
        ):
            raise UnknownExecution("workflow operation is no longer an unadmitted deferred step")
        observation["rejected_before_admission"] = reason
        await self.connection.execute(update(tables.operations).where(
            tables.operations.c.id == operation_id,
            tables.operations.c.state == "CLAIMED",
            tables.operations.c.execution_state == "PENDING",
            tables.operations.c.dispatch_state == "NOT_STARTED",
        ).values(
            state="FAILED", dispatch_state="QUIESCENT", execution_state="QUIESCENT",
            observation=json_value(observation), last_error=reason[:240], updated_at=aware_now(),
        ))
        return True

    @staticmethod
    def _receipt(value: Mapping[str, object]) -> AdmissionReceipt:
        return AdmissionReceipt(
            operation_id=OperationId(str(value["operation_id"])),
            attempt_id=AttemptId(str(value["attempt_id"])),
            reservation_id=ReservationId(str(value["reservation_id"])),
            state=str(value["state"]),
            replayed=True,
        )

    async def complete_admission(self, receipt: AdmissionReceipt) -> None:
        stored = {
            "operation_id": str(receipt.operation_id),
            "attempt_id": str(receipt.attempt_id),
            "reservation_id": str(receipt.reservation_id),
            "state": receipt.state,
        }
        result = await self.connection.execute(update(tables.operations).where(
            tables.operations.c.id == receipt.operation_id,
            tables.operations.c.state == "CLAIMED",
        ).values(
            state="ADMITTED",
            dispatch_state="INTENT_RECORDED",
            execution_state="PENDING",
            receipt=stored,
            updated_at=aware_now(),
        ))
        if result.rowcount != 1:
            raise Conflict("operation claim changed during admission")

    async def lock_operation(self, operation_id: OperationId, request_hash: str | None = None):
        query = select(tables.operations).where(tables.operations.c.id == operation_id).with_for_update()
        row = (await self.connection.execute(query)).mappings().one_or_none()
        if row is None:
            raise StaleInput("operation is unavailable")
        if request_hash is not None and row["request_hash"] != request_hash:
            raise Conflict("operation request hash mismatch")
        return row

    async def get_operation_statuses(self, operation_ids: Sequence[OperationId]) -> Mapping[str, Mapping[str, object]]:
        ids = tuple(sorted(set(operation_ids)))
        if not ids:
            return {}
        rows = (await self.connection.execute(select(
            tables.operations.c.id, tables.operations.c.kind, tables.operations.c.state,
            tables.operations.c.dispatch_state, tables.operations.c.execution_state,
            tables.operations.c.last_error,
        ).where(tables.operations.c.id.in_(ids)).order_by(tables.operations.c.id))).mappings().all()
        return {row["id"]: dict(row) for row in rows}

    async def lock_task_operations(self, task_id: TaskId):
        return (await self.connection.execute(select(tables.operations).where(
            tables.operations.c.task_id == task_id,
        ).order_by(tables.operations.c.id).with_for_update())).mappings().all()

    async def task_execution_states(self, task_id: TaskId):
        return (await self.connection.execute(select(
            tables.operations.c.execution_state,
            func.count(tables.provider_calls.c.accounting_call_id).filter(
                tables.provider_calls.c.status.not_in(["QUIESCENT", "EXPIRED", "REVOKED"]),
            ).label("unconfirmed_calls"),
        ).select_from(tables.operations.outerjoin(
            tables.provider_calls, tables.provider_calls.c.operation_id == tables.operations.c.id,
        )).where(
            tables.operations.c.task_id == task_id,
        ).group_by(
            tables.operations.c.id, tables.operations.c.execution_state,
        ).order_by(tables.operations.c.id))).mappings().all()

    async def append_outbox(self, job: OutboxJob) -> None:
        await self.connection.execute(insert(tables.outbox).values(
            id=job.id,
            operation_id=job.operation_id,
            kind=job.kind,
            generation=job.generation,
            payload=json_value(job.payload),
            status=job.status,
        ))

    async def claim_jobs(self, worker: str, limit: int, lease: float) -> Sequence[OutboxJob]:
        if limit < 1 or lease <= 0:
            raise ValueError("job limit and lease must be positive")
        now = aware_now()
        valid_dispatch = exists(select(1).where(
            tables.agent_execution_holds.c.operation_id == tables.outbox.c.operation_id,
            tables.agent_execution_holds.c.quiescent_at.is_(None),
            tables.agent_execution_holds.c.state == "PENDING",
        ))
        eligible_operation = exists(select(1).select_from(
            tables.operations.join(tables.tasks, tables.tasks.c.id == tables.operations.c.task_id)
        ).where(
            tables.operations.c.id == tables.outbox.c.operation_id,
            tables.operations.c.state == "ADMITTED",
            tables.operations.c.dispatch_state == "INTENT_RECORDED",
            tables.operations.c.execution_state == "PENDING",
            tables.tasks.c.status.in_(["QUEUED", "RUNNING", "WAITING"]),
        ))
        rows = (await self.connection.execute(select(tables.outbox).where(
            tables.outbox.c.kind == "dispatch",
            (tables.outbox.c.status == "PENDING") |
            ((tables.outbox.c.status == "CLAIMED") & (tables.outbox.c.claim_expires_at <= now)),
            tables.outbox.c.available_at <= now,
            tables.outbox.c.send_intent_at.is_(None),
            eligible_operation,
            valid_dispatch,
        ).order_by(tables.outbox.c.available_at, tables.outbox.c.id).limit(limit).with_for_update(skip_locked=True))).mappings().all()
        jobs = []
        for row in rows:
            fence = (row["claim_fence"] or 0) + 1
            await self.connection.execute(update(tables.outbox).where(tables.outbox.c.id == row["id"]).values(
                status="CLAIMED",
                claim_owner=worker,
                claim_fence=fence,
                claim_expires_at=now + timedelta(seconds=lease),
            ))
            jobs.append(OutboxJob(
                id=row["id"],
                operation_id=OperationId(row["operation_id"]),
                kind=row["kind"],
                generation=row["generation"],
                payload=row["payload"],
                status="CLAIMED",
                claim_owner=worker,
                claim_fence=fence,
                claim_expires_at=now + timedelta(seconds=lease),
            ))
        return jobs

    async def claim_lifecycle_jobs(self, worker: str, limit: int, lease: float) -> Sequence[OutboxJob]:
        if limit < 1 or lease <= 0:
            raise ValueError("job limit and lease must be positive")
        now = aware_now()
        rows = (await self.connection.execute(select(tables.outbox).where(
            tables.outbox.c.kind.in_(["critic_create", "critic_delete"]),
            ((tables.outbox.c.status == "PENDING") | (
                (tables.outbox.c.status == "CLAIMED") & (tables.outbox.c.claim_expires_at <= now)
            )),
            tables.outbox.c.available_at <= now,
            tables.outbox.c.send_intent_at.is_(None),
        ).order_by(tables.outbox.c.available_at, tables.outbox.c.id).limit(limit).with_for_update(skip_locked=True))).mappings().all()
        jobs = []
        for row in rows:
            fence = (row["claim_fence"] or 0) + 1
            await self.connection.execute(update(tables.outbox).where(tables.outbox.c.id == row["id"]).values(
                status="CLAIMED", claim_owner=worker, claim_fence=fence,
                claim_expires_at=now + timedelta(seconds=lease),
            ))
            jobs.append(OutboxJob(
                id=row["id"], operation_id=OperationId(row["operation_id"]), kind=row["kind"],
                generation=row["generation"], payload=row["payload"], status="CLAIMED",
                claim_owner=worker, claim_fence=fence, claim_expires_at=now + timedelta(seconds=lease),
            ))
        return jobs

    async def get_dispatch_payload(self, operation_id: OperationId):
        return (await self.connection.execute(select(tables.outbox.c.payload).where(
            tables.outbox.c.operation_id == operation_id,
            tables.outbox.c.kind == "dispatch",
        ).order_by(tables.outbox.c.generation).limit(1))).scalar_one_or_none()

    async def record_send_intent(self, job: OutboxJob, worker: str, fence: int) -> None:
        now = aware_now()
        result = await self.connection.execute(update(tables.outbox).where(
            tables.outbox.c.id == job.id,
            tables.outbox.c.status == "CLAIMED",
            tables.outbox.c.claim_owner == worker,
            tables.outbox.c.claim_fence == fence,
            tables.outbox.c.claim_expires_at > now,
            tables.outbox.c.send_intent_at.is_(None),
        ).values(send_intent_at=now, send_intent_owner=worker, send_intent_fence=fence))
        if result.rowcount != 1:
            raise StaleInput("outbox claim cannot create send intent")
        changed = await self.connection.execute(update(tables.operations).where(
            tables.operations.c.id == job.operation_id,
            tables.operations.c.dispatch_state == "INTENT_RECORDED",
            tables.operations.c.execution_state == "PENDING",
        ).values(dispatch_state="SEND_INTENT", updated_at=now))
        if changed.rowcount != 1:
            raise StaleInput("operation is no longer dispatchable")

    async def mark_dispatch_accepted(self, job: OutboxJob, worker: str, fence: int) -> None:
        now = aware_now()
        result = await self.connection.execute(update(tables.outbox).where(
            tables.outbox.c.id == job.id,
            tables.outbox.c.status == "CLAIMED",
            tables.outbox.c.claim_owner == worker,
            tables.outbox.c.claim_fence == fence,
            tables.outbox.c.claim_expires_at > now,
            tables.outbox.c.send_intent_owner == worker,
            tables.outbox.c.send_intent_fence == fence,
        ).values(status="ACKED", acked_at=now))
        if result.rowcount != 1:
            raise StaleInput("outbox claim is stale")
        changed = await self.connection.execute(update(tables.operations).where(
            tables.operations.c.id == job.operation_id,
            tables.operations.c.dispatch_state == "SEND_INTENT",
            tables.operations.c.execution_state == "PENDING",
        ).values(dispatch_state="DISPATCHED", updated_at=now))
        if changed.rowcount != 1:
            state = (await self.connection.execute(select(tables.operations.c.dispatch_state).where(
                tables.operations.c.id == job.operation_id,
            ))).scalar_one_or_none()
            if state not in {"DISPATCHED", "UNKNOWN", "QUIESCENT"}:
                raise StaleInput("operation dispatch state changed")

    async def ack_job(self, job: OutboxJob, worker: str, fence: int) -> None:
        result = await self.connection.execute(update(tables.outbox).where(
            tables.outbox.c.id == job.id,
            tables.outbox.c.status == "CLAIMED",
            tables.outbox.c.claim_owner == worker,
            tables.outbox.c.claim_fence == fence,
            tables.outbox.c.claim_expires_at > aware_now(),
            tables.outbox.c.send_intent_at.is_(None),
        ).values(status="ACKED", acked_at=aware_now()))
        if result.rowcount != 1:
            raise StaleInput("outbox claim is stale")

    async def assert_job_claim(self, job: OutboxJob, worker: str, fence: int) -> None:
        row = (await self.connection.execute(select(tables.outbox).where(
            tables.outbox.c.id == job.id,
            tables.outbox.c.status == "CLAIMED",
            tables.outbox.c.claim_owner == worker,
            tables.outbox.c.claim_fence == fence,
            tables.outbox.c.claim_expires_at > aware_now(),
            tables.outbox.c.send_intent_at.is_(None),
        ).with_for_update())).mappings().one_or_none()
        if row is None or row["operation_id"] != job.operation_id or row["kind"] != job.kind:
            raise StaleInput("lifecycle outbox claim is stale")

    async def reschedule_job(self, job: OutboxJob, worker: str, fence: int, error: str, delay: float) -> None:
        if delay < 0:
            raise ValueError("delay must be nonnegative")
        result = await self.connection.execute(update(tables.outbox).where(
            tables.outbox.c.id == job.id,
            tables.outbox.c.status == "CLAIMED",
            tables.outbox.c.claim_owner == worker,
            tables.outbox.c.claim_fence == fence,
            tables.outbox.c.claim_expires_at > aware_now(),
        ).values(
            status="PENDING",
            available_at=aware_now() + timedelta(seconds=delay),
            claim_owner=None,
            claim_expires_at=None,
        ))
        if result.rowcount != 1:
            raise StaleInput("outbox claim is stale")
        await self.append_audit({
            "event_kind": "outbox.rescheduled",
            "operation_id": str(job.operation_id),
            "safe_payload": {"delay_seconds": delay},
        })

    async def insert_inbox_once(
        self,
        provider_scope: str,
        stable_event_key: str,
        payload: Mapping[str, object],
        payload_hash: str,
    ) -> InboxReceipt:
        allowed = {
            "accounting_call_id", "operation_id", "provider_call_id", "source", "observation_identity",
            "event_type", "binding", "usage", "state", "outcome", "reason",
            "lease_owner", "observer_fence", "observed_at", "business_result",
        }
        if set(payload) - allowed:
            raise ValueError("inbox payload contains fields outside the accounting allowlist")
        if canonical_json_hash(payload) != payload_hash:
            raise ValueError("inbox payload hash does not match canonical content")
        binding = payload.get("binding")
        if binding is not None and (
            not isinstance(binding, Mapping)
            or set(binding) - {"task_id", "attempt_id", "agent_registry_id", "provider_agent_id", "conversation_id", "input_revision", "fence"}
        ):
            raise ValueError("inbox binding contains fields outside the runtime identity allowlist")
        usage = payload.get("usage")
        if usage is not None and (
            not isinstance(usage, Mapping)
            or set(usage) - {"completeness", "input_tokens", "output_tokens", "cache_tokens", "reasoning_tokens", "total_tokens", "reported_cost_usd", "cost_usd", "source"}
        ):
            raise ValueError("inbox usage contains fields outside the accounting allowlist")
        business_result = payload.get("business_result")
        if business_result is not None and (
            payload.get("event_type") != "business_result"
            or not isinstance(business_result, Mapping)
            or set(business_result) - {"state", "raw_output", "output_sha256", "output_truncated", "structured_output", "failure_code"}
        ):
            raise ValueError("inbox business result contains fields outside its contract")
        await self.connection.execute(text(
            "SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"
        ), {"identity": canonical_json((provider_scope, stable_event_key))})
        inbox_id = new_key()
        inserted = await self.connection.execute(pg_insert(tables.inbox).values(
            id=inbox_id,
            provider_scope=provider_scope,
            stable_event_key=stable_event_key,
            payload_hash=payload_hash,
            payload=json_value(payload),
        ).on_conflict_do_nothing(index_elements=[
            tables.inbox.c.provider_scope,
            tables.inbox.c.stable_event_key,
            tables.inbox.c.payload_hash,
        ]).returning(tables.inbox.c.id))
        inserted_id = inserted.scalar_one_or_none()
        if inserted_id is not None:
            different = (await self.connection.execute(select(tables.inbox.c.id).where(
                tables.inbox.c.provider_scope == provider_scope,
                tables.inbox.c.stable_event_key == stable_event_key,
                tables.inbox.c.payload_hash != payload_hash,
            ).limit(1))).first() is not None
            if different:
                await self.append_audit({
                    "event_kind": "inbox.payload_conflict",
                    "safe_payload": {"provider_scope": provider_scope, "stable_event_key": stable_event_key},
                })
            return InboxReceipt(id=inserted_id, duplicate=False, conflict=different)
        existing = (await self.connection.execute(select(tables.inbox.c.id).where(
            tables.inbox.c.provider_scope == provider_scope,
            tables.inbox.c.stable_event_key == stable_event_key,
            tables.inbox.c.payload_hash == payload_hash,
        ))).scalar_one()
        different = (await self.connection.execute(select(tables.inbox.c.id).where(
            tables.inbox.c.provider_scope == provider_scope,
            tables.inbox.c.stable_event_key == stable_event_key,
            tables.inbox.c.payload_hash != payload_hash,
        ).limit(1))).first() is not None
        return InboxReceipt(id=existing, duplicate=True, conflict=different)

    async def processed_inbox_id(
        self, provider_scope: str, stable_event_key: str, payload_hash: str,
    ) -> str | None:
        return (await self.connection.execute(select(tables.inbox.c.id).where(
            tables.inbox.c.provider_scope == provider_scope,
            tables.inbox.c.stable_event_key == stable_event_key,
            tables.inbox.c.payload_hash == payload_hash,
            tables.inbox.c.processed_at.is_not(None),
        ).limit(1))).scalar_one_or_none()

    async def lock_inbox(self, inbox_id: str):
        return (await self.connection.execute(select(tables.inbox).where(
            tables.inbox.c.id == inbox_id,
        ).with_for_update())).mappings().one_or_none()

    async def mark_inbox_processed(self, inbox_id: str) -> None:
        result = await self.connection.execute(update(tables.inbox).where(
            tables.inbox.c.id == inbox_id,
            tables.inbox.c.processed_at.is_(None),
        ).values(processed_at=aware_now(), pending_reason=None, rejection_reason=None))
        if result.rowcount not in {0, 1}:
            raise Conflict("inbox processing state is invalid")

    async def defer_inbox(self, inbox_id: str, reason: str, delay: float = 1.0) -> None:
        if reason not in {"call_termination_unconfirmed", "processor_lease_unavailable"}:
            raise ValueError("unsupported inbox defer reason")
        await self.connection.execute(update(tables.inbox).where(
            tables.inbox.c.id == inbox_id,
            tables.inbox.c.processed_at.is_(None),
        ).values(next_attempt_at=aware_now() + timedelta(seconds=delay), pending_reason=reason))

    async def reject_inbox(self, inbox_id: str, reason: str) -> None:
        if reason not in {"terminal_payload_conflict", "terminal_observation_rejected", "business_result_conflict", "business_result_rejected"}:
            raise ValueError("unsupported inbox rejection reason")
        await self.connection.execute(update(tables.inbox).where(
            tables.inbox.c.id == inbox_id,
            tables.inbox.c.processed_at.is_(None),
        ).values(processed_at=aware_now(), pending_reason=None, rejection_reason=reason))

    async def pending_inbox(self, limit: int = 100, owner_scope: ScopeId | None = None):
        if not 1 <= limit <= 1_000:
            raise ValueError("inbox limit must be between 1 and 1000")
        query = select(
            tables.inbox.c.id,
            tables.inbox.c.provider_scope,
            tables.inbox.c.stable_event_key,
            tables.inbox.c.payload,
        ).where(
            tables.inbox.c.processed_at.is_(None),
            tables.inbox.c.next_attempt_at <= aware_now(),
        )
        if owner_scope is not None:
            query = query.join(
                tables.operations,
                tables.operations.c.id == tables.inbox.c.payload["operation_id"].astext,
            ).where(tables.operations.c.owner_scope == owner_scope)
        return (await self.connection.execute(query.order_by(
            tables.inbox.c.next_attempt_at, tables.inbox.c.received_at, tables.inbox.c.id,
        ).limit(limit))).mappings().all()

    async def pending_terminal_inbox(self, operation_id: OperationId):
        return (await self.connection.execute(select(
            tables.inbox.c.id,
        ).where(
            tables.inbox.c.processed_at.is_(None),
            tables.inbox.c.payload["event_type"].astext == "execution",
            tables.inbox.c.payload["state"].astext == "QUIESCENT",
            tables.inbox.c.payload["operation_id"].astext == str(operation_id),
        ).order_by(tables.inbox.c.received_at, tables.inbox.c.id))).scalars().all()

    async def append_audit(self, event: Mapping[str, object]) -> None:
        await self.connection.execute(insert(tables.audit_events).values(
            id=new_key(),
            owner_scope=event.get("owner_scope"),
            task_id=event.get("task_id"),
            attempt_id=event.get("attempt_id"),
            operation_id=event.get("operation_id"),
            registry_id=event.get("registry_id"),
            event_kind=str(event["event_kind"]),
            safe_payload=json_value(event.get("safe_payload", {})),
        ))

    async def record_execution(self, observation: ExecutionObservation) -> None:
        row = await self.lock_operation(observation.operation_id)
        binding = json_value(observation.binding)
        if row["binding"] != binding:
            raise Conflict("execution observation binding mismatch")
        hold = (await self.connection.execute(select(tables.agent_execution_holds).where(
            tables.agent_execution_holds.c.operation_id == observation.operation_id,
        ).with_for_update())).mappings().one_or_none()
        if hold is None:
            raise StaleInput("execution hold is unavailable")
        if hold["registry_id"] != observation.binding.agent_registry_id:
            raise Conflict("execution hold registry binding mismatch")
        safe_reason = observation.reason if observation.reason in {
            "transport_disconnect", "provider_timeout", "provider_failed", "user_cancelled",
            "lease_lost", "bridge_terminal", "provider_stopped", "not_dispatched",
            "evidence_reference_unavailable", "authorization_snapshot_changed", "context_manifest_missing",
        } else None
        if hold["quiescent_at"] is not None:
            if observation.state == "QUIESCENT" and row["execution_state"] == "QUIESCENT":
                prior = row["observation"] or {}
                if prior.get("outcome") != observation.outcome or prior.get("source") != observation.source:
                    raise Conflict("terminal execution observation changed")
                return
            raise Conflict("quiescent execution cannot be reopened")
        if observation.state == "QUIESCENT":
            if observation.source not in {"bridge_terminal", "provider_stopped", "pre_dispatch_cancel", "pre_dispatch_policy"}:
                raise Conflict("untrusted quiescence source")
            if observation.outcome not in {"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"}:
                raise ValueError("quiescent execution requires a terminal outcome")
            if observation.source == "pre_dispatch_cancel":
                task_state = (await self.connection.execute(select(tables.tasks.c.status).where(
                    tables.tasks.c.id == row["task_id"],
                ))).scalar_one_or_none()
                claimed = (await self.connection.execute(select(tables.outbox.c.id).where(
                    tables.outbox.c.operation_id == observation.operation_id,
                    tables.outbox.c.status != "PENDING",
                ).limit(1))).first() is not None
                consumed = (await self.connection.execute(select(tables.call_permits.c.permit_id).select_from(
                    tables.call_permits.join(
                        tables.provider_calls,
                        tables.provider_calls.c.accounting_call_id == tables.call_permits.c.accounting_call_id,
                    )
                ).where(
                    tables.provider_calls.c.operation_id == observation.operation_id,
                    tables.call_permits.c.state == "CONSUMED",
                ).limit(1))).first() is not None
                if task_state not in {"STOPPING", "CANCELLED"} or claimed or consumed:
                    raise Conflict("pre-dispatch cancellation is not proven")
            await self.connection.execute(update(tables.agent_execution_holds).where(
                tables.agent_execution_holds.c.operation_id == observation.operation_id,
                tables.agent_execution_holds.c.quiescent_at.is_(None),
            ).values(state="QUIESCENT", quiescent_at=observation.observed_at, reason=safe_reason))
            await self.connection.execute(update(tables.operations).where(
                tables.operations.c.id == observation.operation_id,
            ).values(
                state="COMPLETED" if observation.outcome == "SUCCEEDED" else "FAILED",
                dispatch_state="QUIESCENT",
                execution_state="QUIESCENT",
                observation={"source": observation.source, "outcome": observation.outcome, "reason": safe_reason},
                updated_at=observation.observed_at,
            ))
            await self.connection.execute(update(tables.recovery_cases).where(
                tables.recovery_cases.c.operation_id == observation.operation_id,
            ).values(resolution="terminal_execution_observed", updated_at=observation.observed_at))
        elif observation.state in {"RUNNING", "UNKNOWN"}:
            if observation.source not in {"bridge_send", "bridge_running", "bridge_disconnect", "provider_response", "bridge_result", "provider_stopped"}:
                raise Conflict("untrusted execution observation source")
            if hold["state"] == "UNKNOWN" and observation.state == "RUNNING":
                raise UnknownExecution("unknown execution cannot be reset to running")
            await self.connection.execute(update(tables.agent_execution_holds).where(
                tables.agent_execution_holds.c.operation_id == observation.operation_id,
                tables.agent_execution_holds.c.quiescent_at.is_(None),
            ).values(state=observation.state, reason=safe_reason))
            await self.connection.execute(update(tables.operations).where(
                tables.operations.c.id == observation.operation_id,
            ).values(
                state="UNKNOWN" if observation.state == "UNKNOWN" else row["state"],
                dispatch_state="UNKNOWN" if observation.state == "UNKNOWN" else "DISPATCHED",
                execution_state=observation.state,
                observation={"source": observation.source, "reason": safe_reason},
                updated_at=observation.observed_at,
            ))
            if observation.state == "UNKNOWN":
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                await self.connection.execute(pg_insert(tables.recovery_cases).values(
                    operation_id=observation.operation_id,
                    reason=safe_reason or "unknown_execution",
                    observations={"latest_source": observation.source, "latest_state": "UNKNOWN"},
                    updated_at=observation.observed_at,
                ).on_conflict_do_update(
                    index_elements=[tables.recovery_cases.c.operation_id],
                    set_={"reason": safe_reason or "unknown_execution", "observations": {"latest_source": observation.source, "latest_state": "UNKNOWN"}, "updated_at": observation.observed_at},
                ))
        else:
            raise ValueError("unsupported execution observation state")
        await self.append_audit({
            "owner_scope": str(observation.binding.scope),
            "task_id": str(observation.binding.task_id),
            "attempt_id": str(observation.binding.attempt_id),
            "operation_id": str(observation.operation_id),
            "registry_id": str(observation.binding.agent_registry_id),
            "event_kind": f"execution.{observation.state.lower()}",
            "safe_payload": {"source": observation.source, "reason": safe_reason},
        })

    async def read_public_events(self, task_id: TaskId, after: object):
        raise NotImplementedError
