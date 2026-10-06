from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone

from sqlalchemy import exists, insert, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from hekate.domain.errors import Conflict, PolicyDenied, StaleInput
from hekate.domain.models import (
    Attempt,
    AuthorizationSnapshot,
    AuthorizationScopeState,
    InputChange,
    Task,
    TaskCounters,
)
from hekate.domain.types import (
    AttemptId,
    AttemptStatus,
    AttemptEvent,
    EvidenceId,
    OperationId,
    PrincipalId,
    RegistryId,
    ScopeId,
    TaskEvent,
    TaskId,
    TaskStatus,
    StopReason,
    TopicId,
)
from hekate.domain.transitions import transition_attempt, transition_task

from . import tables
from .common import aware_now, json_value


class PostgresTaskRepository:
    def __init__(self, connection: AsyncSession) -> None:
        self.connection = connection

    async def insert_scope(self, snapshot: AuthorizationSnapshot) -> None:
        await self.connection.execute(insert(tables.authorization_scopes).values(
            id=snapshot.scope,
            principal_id=snapshot.principal_id,
            policy_version=snapshot.policy_version,
            authz_epoch=snapshot.authz_epoch,
        ))

    async def ensure_local_scope(self, snapshot: AuthorizationSnapshot) -> bool:
        """Create the configured local scope once; never overwrite or reactivate it."""
        inserted = await self.connection.execute(pg_insert(tables.authorization_scopes).values(
            id=snapshot.scope,
            principal_id=snapshot.principal_id,
            policy_version=snapshot.policy_version,
            authz_epoch=snapshot.authz_epoch,
            active=True,
        ).on_conflict_do_nothing(index_elements=[tables.authorization_scopes.c.id]).returning(
            tables.authorization_scopes.c.id,
        ))
        created = inserted.scalar_one_or_none() is not None
        row = (await self.connection.execute(select(tables.authorization_scopes).where(
            tables.authorization_scopes.c.id == snapshot.scope,
        ).with_for_update())).mappings().one_or_none()
        if row is None or not row["active"]:
            raise PolicyDenied("local authorization scope is missing or revoked")
        if (row["principal_id"], row["policy_version"], row["authz_epoch"]) != (
            snapshot.principal_id, snapshot.policy_version, snapshot.authz_epoch,
        ):
            raise Conflict("local authorization scope already has a different principal or policy epoch")
        return created

    async def claim_submission(self, scope: ScopeId, request_key: str, request_hash: str):
        await self.connection.execute(pg_insert(tables.task_submissions).values(
            owner_scope=scope,
            request_key=request_key,
            request_hash=request_hash,
        ).on_conflict_do_nothing(index_elements=[tables.task_submissions.c.owner_scope, tables.task_submissions.c.request_key]))
        row = (await self.connection.execute(select(tables.task_submissions).where(
            tables.task_submissions.c.owner_scope == scope,
            tables.task_submissions.c.request_key == request_key,
        ).with_for_update())).mappings().one_or_none()
        if row is None:
            raise Conflict("task submission receipt could not be acquired")
        if row["request_hash"] != request_hash:
            raise Conflict("request key is already bound to a different question")
        return row

    async def complete_submission(self, scope: ScopeId, request_key: str, task_id: TaskId, receipt: Mapping[str, object]) -> None:
        result = await self.connection.execute(update(tables.task_submissions).where(
            tables.task_submissions.c.owner_scope == scope,
            tables.task_submissions.c.request_key == request_key,
            tables.task_submissions.c.task_id.is_(None),
        ).values(task_id=task_id, receipt=json_value(receipt)))
        if result.rowcount not in {0, 1}:
            raise Conflict("task submission receipt changed")

    async def lock_scope(self, scope: ScopeId) -> AuthorizationSnapshot:
        state = await self.lock_scope_for_observation(scope)
        if not state.active:
            raise PolicyDenied("authorization scope is revoked")
        return state.snapshot

    async def lock_scope_for_observation(self, scope: ScopeId) -> AuthorizationScopeState:
        """Lock an existing scope while processing facts for admitted work.

        Unlike ``lock_scope``, this does not authorize new work and therefore
        returns inactive scopes to the caller as explicit state.
        """
        row = (await self.connection.execute(
            select(tables.authorization_scopes).where(tables.authorization_scopes.c.id == scope).with_for_update()
        )).mappings().one_or_none()
        if row is None:
            raise PolicyDenied("authorization scope is unavailable")
        return AuthorizationScopeState(
            snapshot=AuthorizationSnapshot(
                scope=ScopeId(row["id"]),
                principal_id=PrincipalId(row["principal_id"]),
                policy_version=row["policy_version"],
                authz_epoch=row["authz_epoch"],
            ),
            active=bool(row["active"]),
        )

    async def insert_task(self, task: Task, constraints: Mapping[str, object] | None = None) -> None:
        counters = task.counters
        await self.connection.execute(insert(tables.tasks).values(
            id=task.id,
            owner_scope=task.scope,
            question=task.question,
            input_revision=task.input_revision,
            constraints_hash=task.constraints_hash,
            status=task.status.value,
            topic_id=task.topic_id,
            evidence_refs=json_value(task.evidence_refs),
            base_position_version=task.base_position_version,
            deadline=task.deadline,
            **({"created_at": task.created_at} if task.created_at is not None else {}),
            critic_agents=counters.critic_agents,
            review_rounds=counters.review_rounds,
            hekate_continuations=counters.hekate_continuations,
            schema_repairs=counters.schema_repairs,
            transient_retries=counters.transient_retries,
            tool_calls=counters.tool_calls,
            provider_calls=counters.provider_calls,
            outcome=task.outcome,
            stop_reason=task.stop_reason,
        ))
        await self.connection.execute(insert(tables.task_inputs).values(
            task_id=task.id,
            revision=task.input_revision,
            question=task.question,
            constraints=json_value(constraints or {}),
            constraints_hash=task.constraints_hash,
            topic_id=task.topic_id,
            evidence_refs=json_value(task.evidence_refs),
        ))

    async def lock_task(self, task_id: TaskId) -> Task:
        row = (await self.connection.execute(
            select(tables.tasks).where(tables.tasks.c.id == task_id).with_for_update()
        )).mappings().one_or_none()
        if row is None:
            raise StaleInput("task is unavailable")
        return self._task(row)

    async def list_queued(self, limit: int = 20):
        if not 1 <= limit <= 100:
            raise ValueError("queued task batch must be between 1 and 100")
        rows = (await self.connection.execute(select(tables.tasks).where(
            tables.tasks.c.status == TaskStatus.QUEUED.value,
        ).order_by(tables.tasks.c.created_at, tables.tasks.c.id).with_for_update(skip_locked=True).limit(limit))).mappings().all()
        return tuple(self._task(row) for row in rows)

    async def claim_task_preparation(
        self, task_id: TaskId, revision: int, operation_id: str, attempt_id: str,
        reservation_id: str, owner: str, ttl_seconds: int,
    ):
        if ttl_seconds < 1:
            raise ValueError("task preparation claim TTL must be positive")
        now = datetime.now(timezone.utc)
        await self.connection.execute(pg_insert(tables.task_preparations).values(
            task_id=task_id, input_revision=revision, operation_id=operation_id,
            attempt_id=attempt_id, reservation_id=reservation_id,
            claim_owner=owner, claim_expires_at=now + timedelta(seconds=ttl_seconds),
        ).on_conflict_do_nothing(index_elements=[tables.task_preparations.c.task_id, tables.task_preparations.c.input_revision]))
        row = (await self.connection.execute(select(tables.task_preparations).where(
            tables.task_preparations.c.task_id == task_id,
            tables.task_preparations.c.input_revision == revision,
        ).with_for_update())).mappings().one_or_none()
        if row is None or (row["operation_id"], row["attempt_id"], row["reservation_id"]) != (
            operation_id, attempt_id, reservation_id,
        ):
            raise Conflict("task preparation identity changed")
        now = datetime.now(timezone.utc)
        if row["state"] == "ADMITTED":
            return row
        if row["claim_expires_at"] is not None and row["claim_expires_at"] > now and row["claim_owner"] != owner:
            return row
        await self.connection.execute(update(tables.task_preparations).where(
            tables.task_preparations.c.task_id == task_id,
            tables.task_preparations.c.input_revision == revision,
        ).values(claim_owner=owner, claim_expires_at=now + timedelta(seconds=ttl_seconds)))
        return (await self.connection.execute(select(tables.task_preparations).where(
            tables.task_preparations.c.task_id == task_id,
            tables.task_preparations.c.input_revision == revision,
        ))).mappings().one()

    async def mark_task_preparation_admitted(
        self, task_id: TaskId, revision: int, owner: str, conversation_id: str, fence: int,
    ) -> None:
        result = await self.connection.execute(update(tables.task_preparations).where(
            tables.task_preparations.c.task_id == task_id,
            tables.task_preparations.c.input_revision == revision,
            tables.task_preparations.c.claim_owner == owner,
            tables.task_preparations.c.state == "PREPARING",
        ).values(
            state="ADMITTED", conversation_id=conversation_id, fence=fence,
            claim_owner=None, claim_expires_at=None,
        ))
        if result.rowcount != 1:
            raise Conflict("task preparation claim changed before admission")

    async def release_task_preparation(self, task_id: TaskId, revision: int, owner: str) -> None:
        await self.connection.execute(update(tables.task_preparations).where(
            tables.task_preparations.c.task_id == task_id,
            tables.task_preparations.c.input_revision == revision,
            tables.task_preparations.c.claim_owner == owner,
            tables.task_preparations.c.state == "PREPARING",
        ).values(claim_owner=None, claim_expires_at=None))

    async def get_task_input(self, task_id: TaskId, revision: int):
        return (await self.connection.execute(select(tables.task_inputs).where(
            tables.task_inputs.c.task_id == task_id,
            tables.task_inputs.c.revision == revision,
        ))).mappings().one_or_none()

    async def fail_expired_queued(self, now: datetime) -> int:
        result = await self.connection.execute(update(tables.tasks).where(
            tables.tasks.c.status == TaskStatus.QUEUED.value,
            tables.tasks.c.deadline <= now,
        ).values(status=TaskStatus.FAILED.value, outcome="FAILED", stop_reason=StopReason.DEADLINE.value))
        return int(result.rowcount or 0)

    async def fail_queued_task(self, task_id: TaskId, reason: StopReason) -> bool:
        result = await self.connection.execute(update(tables.tasks).where(
            tables.tasks.c.id == task_id,
            tables.tasks.c.status == TaskStatus.QUEUED.value,
        ).values(
            status=TaskStatus.FAILED.value,
            outcome="FAILED",
            stop_reason=reason.value,
        ))
        return result.rowcount == 1

    async def list_execution_terminal_candidates(self, limit: int = 100):
        if not 1 <= limit <= 1_000:
            raise ValueError("terminal candidate batch must be between 1 and 1000")
        now = aware_now()
        rows = (await self.connection.execute(select(
            tables.tasks.c.id, tables.tasks.c.owner_scope, tables.tasks.c.input_revision,
        ).where(
            tables.tasks.c.status.in_([TaskStatus.RUNNING.value, TaskStatus.WAITING.value, TaskStatus.STOPPING.value]),
            or_(tables.tasks.c.cancel_requested_at.is_not(None), tables.tasks.c.deadline <= now),
            exists(select(1).where(tables.operations.c.task_id == tables.tasks.c.id)),
        ).order_by(tables.tasks.c.deadline, tables.tasks.c.id).limit(limit))).mappings().all()
        return tuple(rows)

    async def get_task_response(self, task_id: TaskId):
        return (await self.connection.execute(select(tables.task_responses).where(
            tables.task_responses.c.task_id == task_id,
        ))).mappings().one_or_none()

    async def get_task_cost_status(self, task_id: TaskId):
        rows = (await self.connection.execute(select(
            tables.operations.c.id.label("operation_id"),
            tables.budget_reservations.c.status.label("reservation_state"),
            tables.provider_calls.c.status.label("call_state"),
            tables.usage_projections.c.settlement_state,
        ).select_from(
            tables.operations.outerjoin(
                tables.budget_reservations,
                tables.budget_reservations.c.operation_id == tables.operations.c.id,
            ).outerjoin(
                tables.provider_calls, tables.provider_calls.c.operation_id == tables.operations.c.id,
            ).outerjoin(
                tables.usage_projections,
                tables.usage_projections.c.accounting_call_id == tables.provider_calls.c.accounting_call_id,
            )
        ).where(or_(
            tables.operations.c.task_id == task_id,
            exists(select(1).where(
                tables.deliberation_steps.c.task_id == task_id,
                tables.deliberation_steps.c.operation_id == tables.operations.c.id,
            )),
            exists(select(1).where(
                tables.critic_workflows.c.task_id == task_id,
                tables.critic_workflows.c.create_operation_id == tables.operations.c.id,
            )),
            exists(select(1).where(
                tables.critic_workflows.c.task_id == task_id,
                tables.critic_workflows.c.review_operation_id == tables.operations.c.id,
            )),
            exists(select(1).where(
                tables.critic_workflows.c.task_id == task_id,
                tables.critic_workflows.c.synthesis_operation_id == tables.operations.c.id,
            )),
            exists(select(1).where(
                tables.critic_workflows.c.task_id == task_id,
                tables.critic_workflows.c.delete_operation_id == tables.operations.c.id,
            )),
        )))).mappings().all()
        operations = {row["operation_id"]: row["reservation_state"] for row in rows}
        billable = [row for row in rows if row["call_state"] not in {None, "EXPIRED", "REVOKED"}]
        pending_calls = sum(
            row["call_state"] != "QUIESCENT" or row["settlement_state"] != "SETTLED"
            for row in billable
        )
        pending_reservations = sum(
            state in {"RESERVED", "PENDING_SETTLEMENT"} for state in operations.values()
        )
        pending = pending_calls > 0 or pending_reservations > 0
        return {
            "cost_status": "PENDING_SETTLEMENT" if pending else "SETTLED" if billable or operations else "NOT_DISPATCHED",
            "pending_provider_calls": pending_calls,
            "pending_reservations": pending_reservations,
        }

    async def finalize_task_response(
        self, task_id: TaskId, revision: int, response: Mapping[str, object], *,
        successful: bool, accepted_at: datetime | None = None,
    ) -> bool:
        task = await self.lock_task(task_id)
        existing = await self.get_task_response(task_id)
        if existing is not None:
            return existing["source_inbox_id"] == response["source_inbox_id"]
        if task.input_revision != revision or task.status != TaskStatus.RUNNING:
            return False
        values = {
            "status": TaskStatus.COMPLETED.value if successful else TaskStatus.FAILED.value,
            "outcome": response["outcome"],
            "stop_reason": response["stop_reason"],
        }
        query = update(tables.tasks).where(
            tables.tasks.c.id == task_id,
            tables.tasks.c.input_revision == revision,
            tables.tasks.c.status == TaskStatus.RUNNING.value,
            tables.tasks.c.cancel_requested_at.is_(None),
            tables.tasks.c.deadline > (accepted_at or datetime.now(timezone.utc)),
        )
        changed = await self.connection.execute(query.values(**values))
        if changed.rowcount != 1:
            return False
        await self.connection.execute(insert(tables.task_responses).values(
            task_id=task_id,
            input_revision=revision,
            operation_id=response["operation_id"],
            attempt_id=response["attempt_id"],
            registry_id=response["registry_id"],
            source_inbox_id=response["source_inbox_id"],
            proposal=json_value(response["proposal"]),
            response_text=response["response_text"],
            outcome=response["outcome"],
            stop_reason=response["stop_reason"],
        ))
        return True

    async def fail_unaccepted_execution_with_policy_response(
        self, scope: ScopeId, expected_authorization: AuthorizationSnapshot,
        task_id: TaskId, revision: int, response: Mapping[str, object], *, accepted_at: datetime,
    ) -> bool:
        """Persist a POLICY failure for a completed execution whose authority changed.

        This narrow path records the result of an already admitted and quiescent
        operation. It cannot accept a model answer and refuses use while the
        original authorization snapshot remains active and current.
        """
        if response.get("stop_reason") != StopReason.POLICY.value or response.get("outcome") != "FAILED":
            raise ValueError("revoked-execution response must be a POLICY failure")
        if expected_authorization.scope != scope:
            raise Conflict("revoked-execution authorization scope mismatch")
        try:
            response_operation_id = OperationId(str(response["operation_id"]))
            response_attempt_id = AttemptId(str(response["attempt_id"]))
            response_registry_id = RegistryId(str(response["registry_id"]))
        except (KeyError, TypeError, ValueError) as error:
            raise Conflict("revoked-execution response has no trusted binding identity") from error
        authorization = (await self.connection.execute(
            select(tables.authorization_scopes).where(
                tables.authorization_scopes.c.id == scope,
            ).with_for_update()
        )).mappings().one_or_none()
        if authorization is None:
            raise PolicyDenied("authorization scope is unavailable")
        still_current = bool(authorization["active"]) and (
            authorization["principal_id"], authorization["policy_version"], authorization["authz_epoch"],
        ) == (
            str(expected_authorization.principal_id), expected_authorization.policy_version,
            expected_authorization.authz_epoch,
        )
        if still_current:
            raise PolicyDenied("authorization snapshot is still current")

        task = await self.lock_task(task_id)
        if task.scope != scope:
            raise PolicyDenied("task is outside the revoked execution scope")
        attempt = await self.get_attempt(response_attempt_id, for_update=True)
        operation = (await self.connection.execute(select(tables.operations).where(
            tables.operations.c.id == response_operation_id,
        ))).mappings().one_or_none()
        trusted_binding = operation["binding"] if operation is not None else None
        if (
            attempt.task_id != task_id
            or attempt.input_revision != revision
            or attempt.operation_id != response_operation_id
            or attempt.agent_registry_id != response_registry_id
            or operation is None
            or operation["owner_scope"] != scope
            or operation["task_id"] != task_id
            or operation["execution_state"] != "QUIESCENT"
            or not isinstance(trusted_binding, Mapping)
            or trusted_binding.get("scope") != str(scope)
            or trusted_binding.get("task_id") != str(task_id)
            or trusted_binding.get("attempt_id") != str(response_attempt_id)
            or trusted_binding.get("agent_registry_id") != str(response_registry_id)
            or trusted_binding.get("input_revision") != revision
            or trusted_binding.get("principal_id") != str(expected_authorization.principal_id)
            or trusted_binding.get("policy_version") != expected_authorization.policy_version
            or trusted_binding.get("authz_epoch") != expected_authorization.authz_epoch
        ):
            raise Conflict("revoked-execution response differs from a quiescent immutable binding")
        existing = await self.get_task_response(task_id)
        if existing is not None:
            return (
                existing["input_revision"] == revision
                and existing["operation_id"] == response["operation_id"]
                and existing["attempt_id"] == response["attempt_id"]
                and existing["registry_id"] == response["registry_id"]
                and existing["source_inbox_id"] == response["source_inbox_id"]
                and existing["proposal"] == json_value(response["proposal"])
                and existing["response_text"] == response["response_text"]
                and existing["stop_reason"] == response["stop_reason"]
                and existing["outcome"] == response["outcome"]
            )
        if (
            task.input_revision != revision
            or task.status not in {TaskStatus.RUNNING, TaskStatus.WAITING}
            or task.cancel_requested_at is not None or task.deadline <= accepted_at
        ):
            return False
        changed = await self.connection.execute(update(tables.tasks).where(
            tables.tasks.c.id == task_id,
            tables.tasks.c.owner_scope == scope,
            tables.tasks.c.input_revision == revision,
            tables.tasks.c.status == task.status.value,
            tables.tasks.c.cancel_requested_at.is_(None),
            tables.tasks.c.deadline > accepted_at,
        ).values(
            status=TaskStatus.FAILED.value,
            outcome="FAILED",
            stop_reason=StopReason.POLICY.value,
        ))
        if changed.rowcount != 1:
            return False
        await self.connection.execute(insert(tables.task_responses).values(
            task_id=task_id,
            input_revision=revision,
            operation_id=response["operation_id"],
            attempt_id=response["attempt_id"],
            registry_id=response["registry_id"],
            source_inbox_id=response["source_inbox_id"],
            proposal=json_value(response["proposal"]),
            response_text=response["response_text"],
            outcome="FAILED",
            stop_reason=StopReason.POLICY.value,
        ))
        return True

    async def resolve_task_after_execution(
        self, task_id: TaskId, revision: int, *, now: datetime | None = None,
    ) -> Task | None:
        task = await self.lock_task(task_id)
        if task.input_revision != revision or task.status not in {
            TaskStatus.RUNNING, TaskStatus.WAITING, TaskStatus.STOPPING,
        } or await self.get_task_response(task_id) is not None:
            return None
        if task.cancel_requested_at is not None:
            if task.status != TaskStatus.STOPPING:
                return None
            status, outcome = transition_task(task.status, TaskEvent.CANCEL), "CANCELLED"
            stop_reason = task.stop_reason or StopReason.USER_CANCELLED.value
        elif task.deadline <= (now or aware_now()):
            status, outcome = transition_task(task.status, TaskEvent.FAIL), "FAILED"
            stop_reason = StopReason.DEADLINE.value
        else:
            return None
        changed = await self.connection.execute(update(tables.tasks).where(
            tables.tasks.c.id == task_id,
            tables.tasks.c.input_revision == revision,
            tables.tasks.c.status == task.status.value,
        ).values(status=status.value, outcome=outcome, stop_reason=stop_reason))
        if changed.rowcount != 1:
            return None
        return task.model_copy(update={"status": status, "outcome": outcome, "stop_reason": stop_reason})

    async def list_pending_turn_results(self, limit: int = 100):
        if not 1 <= limit <= 1_000:
            raise ValueError("result batch must be between 1 and 1000")
        return (await self.connection.execute(select(tables.turn_results.c.inbox_id).where(
            tables.turn_results.c.processing_state.in_(["WAITING_EXECUTION", "VALIDATED"]),
            tables.turn_results.c.next_attempt_at <= datetime.now(timezone.utc),
        ).order_by(tables.turn_results.c.created_at, tables.turn_results.c.inbox_id).limit(limit))).scalars().all()

    @staticmethod
    def _task(row) -> Task:
        return Task(
            id=TaskId(row["id"]),
            scope=ScopeId(row["owner_scope"]),
            question=row["question"],
            input_revision=row["input_revision"],
            constraints_hash=row["constraints_hash"],
            status=TaskStatus(row["status"]),
            topic_id=TopicId(row["topic_id"]) if row["topic_id"] else None,
            evidence_refs=tuple(EvidenceId(value) for value in row["evidence_refs"]),
            base_position_version=row["base_position_version"],
            deadline=row["deadline"],
            created_at=row["created_at"],
            cancel_requested_at=row["cancel_requested_at"],
            counters=TaskCounters(
                critic_agents=row["critic_agents"],
                review_rounds=row["review_rounds"],
                hekate_continuations=row["hekate_continuations"],
                schema_repairs=row["schema_repairs"],
                transient_retries=row["transient_retries"],
                tool_calls=row["tool_calls"],
                provider_calls=row["provider_calls"],
            ),
            outcome=row["outcome"],
            stop_reason=row["stop_reason"],
        )

    async def get_attempt(self, attempt_id: AttemptId, for_update: bool = False) -> Attempt:
        query = select(tables.attempts).where(tables.attempts.c.id == attempt_id)
        if for_update:
            query = query.with_for_update()
        row = (await self.connection.execute(query)).mappings().one_or_none()
        if row is None:
            raise StaleInput("attempt is unavailable")
        return Attempt(
            id=AttemptId(row["id"]),
            task_id=TaskId(row["task_id"]),
            kind=row["kind"],
            parent_attempt_id=AttemptId(row["parent_attempt_id"]) if row["parent_attempt_id"] else None,
            review_round=row["review_round"],
            input_revision=row["input_revision"],
            agent_registry_id=RegistryId(row["agent_registry_id"]),
            status=AttemptStatus(row["status"]),
            operation_id=row["operation_id"],
            reservation_id=row["reservation_id"],
            deadline=row["deadline"],
        )

    async def attempt_exists(self, attempt_id: AttemptId) -> bool:
        return (await self.connection.execute(select(tables.attempts.c.id).where(
            tables.attempts.c.id == attempt_id,
        ))).first() is not None

    async def insert_attempt(self, attempt: Attempt) -> None:
        await self.connection.execute(insert(tables.attempts).values(
            id=attempt.id,
            task_id=attempt.task_id,
            kind=attempt.kind,
            parent_attempt_id=attempt.parent_attempt_id,
            review_round=attempt.review_round,
            input_revision=attempt.input_revision,
            agent_registry_id=attempt.agent_registry_id,
            status=attempt.status.value,
            operation_id=attempt.operation_id,
            reservation_id=attempt.reservation_id,
            deadline=attempt.deadline,
        ))

    async def set_attempt_reservation(self, attempt_id: AttemptId, reservation_id) -> None:
        result = await self.connection.execute(update(tables.attempts).where(
            tables.attempts.c.id == attempt_id,
            tables.attempts.c.reservation_id.is_(None),
        ).values(reservation_id=reservation_id))
        if result.rowcount != 1:
            raise Conflict("attempt reservation binding changed")

    async def observe_attempt(self, attempt_id: AttemptId, event: AttemptEvent) -> Attempt:
        attempt = await self.get_attempt(attempt_id, for_update=True)
        if attempt.status in {AttemptStatus.SUCCEEDED, AttemptStatus.FAILED, AttemptStatus.TIMED_OUT, AttemptStatus.CANCELLED}:
            return attempt
        if attempt.status == AttemptStatus.PENDING and event in {AttemptEvent.START, AttemptEvent.SUCCEED}:
            status = transition_attempt(attempt.status, AttemptEvent.DISPATCH)
            if event == AttemptEvent.SUCCEED:
                status = transition_attempt(status, event)
            else:
                status = transition_attempt(status, event)
        else:
            status = transition_attempt(attempt.status, event)
        await self.connection.execute(update(tables.attempts).where(
            tables.attempts.c.id == attempt_id,
        ).values(status=status.value))
        return attempt.model_copy(update={"status": status})

    async def apply_admission(self, task: Task, attempt_kind: str) -> None:
        status = task.status
        if status == TaskStatus.QUEUED:
            status = transition_task(status, TaskEvent.START)
        elif status == TaskStatus.WAITING:
            status = transition_task(status, TaskEvent.RESUME)
        elif status in {TaskStatus.RUNNING}:
            pass
        else:
            raise PolicyDenied("task does not admit new attempts")
        increments: dict[str, int] = {}
        if attempt_kind == "critic_review":
            if task.counters.review_rounds >= 2:
                raise PolicyDenied("task review-round cap reached")
            increments["review_rounds"] = 1
        elif attempt_kind == "schema_repair":
            if task.counters.schema_repairs >= 1:
                raise PolicyDenied("task schema-repair cap reached")
            increments["schema_repairs"] = 1
        elif attempt_kind == "transient_retry":
            if task.counters.transient_retries >= 1:
                raise PolicyDenied("task transient-retry cap reached")
            increments["transient_retries"] = 1
        values: dict[str, object] = {"status": status.value}
        values.update({name: getattr(tables.tasks.c, name) + count for name, count in increments.items()})
        await self.connection.execute(update(tables.tasks).where(
            tables.tasks.c.id == task.id,
        ).values(**values))

    async def mark_waiting_for_workflow(self, task_id: TaskId, revision: int) -> bool:
        task = await self.lock_task(task_id)
        if task.input_revision != revision or task.cancel_requested_at is not None:
            return False
        if task.status == TaskStatus.WAITING:
            return True
        if task.status != TaskStatus.RUNNING or task.deadline <= aware_now():
            return False
        changed = await self.connection.execute(update(tables.tasks).where(
            tables.tasks.c.id == task_id,
            tables.tasks.c.input_revision == revision,
            tables.tasks.c.status == TaskStatus.RUNNING.value,
            tables.tasks.c.cancel_requested_at.is_(None),
            tables.tasks.c.deadline > aware_now(),
        ).values(status=TaskStatus.WAITING.value))
        return changed.rowcount == 1

    async def revise_input(
        self,
        task_id: TaskId,
        expected_revision: int,
        change: InputChange,
        constraints_hash: str,
        *,
        accepted_at: datetime | None = None,
    ) -> Task:
        task = await self.lock_task(task_id)
        if task.input_revision != expected_revision:
            raise StaleInput("input revision changed")
        if task.status in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.STOPPING}:
            raise PolicyDenied("terminal or stopping task cannot be revised")
        next_revision = expected_revision + 1
        await self.connection.execute(insert(tables.task_inputs).values(
            task_id=task_id,
            revision=next_revision,
            question=change.text,
            constraints=json_value(change.constraints),
            constraints_hash=constraints_hash,
            topic_id=task.topic_id,
            evidence_refs=json_value(task.evidence_refs),
            accepted_at=accepted_at or aware_now(),
        ))
        await self.connection.execute(update(tables.tasks).where(tables.tasks.c.id == task_id).values(
            question=change.text,
            input_revision=next_revision,
            constraints_hash=constraints_hash,
            topic_id=task.topic_id,
            evidence_refs=json_value(task.evidence_refs),
        ))
        return task.model_copy(update={"question": change.text, "input_revision": next_revision, "constraints_hash": constraints_hash})

    async def set_base_position_version(self, task_id: TaskId, revision: int, version: int) -> bool:
        result = await self.connection.execute(update(tables.tasks).where(
            tables.tasks.c.id == task_id,
            tables.tasks.c.input_revision == revision,
            tables.tasks.c.status == TaskStatus.QUEUED.value,
        ).values(base_position_version=version))
        return result.rowcount == 1

    async def fail_policy_task(self, task_id: TaskId, revision: int) -> bool:
        result = await self.connection.execute(update(tables.tasks).where(
            tables.tasks.c.id == task_id,
            tables.tasks.c.input_revision == revision,
            tables.tasks.c.status == TaskStatus.RUNNING.value,
            tables.tasks.c.cancel_requested_at.is_(None),
            tables.tasks.c.deadline > aware_now(),
        ).values(status=TaskStatus.FAILED.value, outcome="FAILED", stop_reason=StopReason.POLICY.value))
        return result.rowcount == 1

    async def fail_waiting_task_with_response(
        self, task_id: TaskId, revision: int, response: Mapping[str, object],
    ) -> bool:
        """Fail the current workflow revision and persist its server response atomically."""
        if response.get("stop_reason") != StopReason.POLICY.value:
            raise ValueError("policy failure response must carry POLICY stop reason")
        return await self.resolve_waiting_task_with_response(
            task_id, revision, response, successful=False,
        )

    async def resolve_waiting_task_with_response(
        self, task_id: TaskId, revision: int, response: Mapping[str, object], *, successful: bool,
    ) -> bool:
        """Finish a WAITING Task with a server response in the caller's transaction."""
        if response.get("stop_reason") not in {item.value for item in StopReason}:
            raise ValueError("waiting response has an unsupported stop reason")
        expected_outcome = str(response["outcome"])
        task = await self.lock_task(task_id)
        existing = await self.get_task_response(task_id)
        if existing is not None:
            return (
                existing["input_revision"] == revision
                and existing["operation_id"] == response["operation_id"]
                and existing["attempt_id"] == response["attempt_id"]
                and existing["registry_id"] == response["registry_id"]
                and existing["source_inbox_id"] == response["source_inbox_id"]
                and existing["proposal"] == json_value(response["proposal"])
                and existing["response_text"] == response["response_text"]
                and existing["stop_reason"] == response["stop_reason"]
                and existing["outcome"] == expected_outcome
            )
        now = aware_now()
        if (
            task.input_revision != revision or task.status != TaskStatus.WAITING
            or task.cancel_requested_at is not None or task.deadline <= now
        ):
            return False
        terminal = transition_task(
            task.status, TaskEvent.COMPLETE if successful else TaskEvent.FAIL,
        )
        changed = await self.connection.execute(update(tables.tasks).where(
            tables.tasks.c.id == task_id,
            tables.tasks.c.input_revision == revision,
            tables.tasks.c.status == TaskStatus.WAITING.value,
            tables.tasks.c.cancel_requested_at.is_(None),
            tables.tasks.c.deadline > now,
        ).values(
            status=terminal.value, outcome=expected_outcome,
            stop_reason=response["stop_reason"],
        ))
        if changed.rowcount != 1:
            return False
        await self.connection.execute(insert(tables.task_responses).values(
            task_id=task_id,
            input_revision=revision,
            operation_id=response["operation_id"],
            attempt_id=response["attempt_id"],
            registry_id=response["registry_id"],
            source_inbox_id=response["source_inbox_id"],
            proposal=json_value(response["proposal"]),
            response_text=response["response_text"],
            outcome=expected_outcome,
            stop_reason=response["stop_reason"],
        ))
        return True

    async def request_cancel(self, task_id: TaskId, reason: StopReason) -> Task:
        task = await self.lock_task(task_id)
        if task.status in {TaskStatus.STOPPING, TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}:
            return task
        next_status = transition_task(task.status, TaskEvent.CANCEL)
        now = aware_now()
        await self.connection.execute(update(tables.tasks).where(tables.tasks.c.id == task_id).values(
            status=next_status.value,
            cancel_requested_at=now,
            **({"outcome": "CANCELLED"} if next_status == TaskStatus.CANCELLED else {}),
            stop_reason=reason.value,
        ))
        return task.model_copy(update={
            "status": next_status, "cancel_requested_at": now,
            "outcome": "CANCELLED" if next_status == TaskStatus.CANCELLED else task.outcome,
            "stop_reason": reason.value,
        })

    async def increment_counter_if_below(self, task_id: TaskId, counter: str, cap: int) -> bool:
        allowed = {"critic_agents", "review_rounds", "hekate_continuations", "schema_repairs", "transient_retries", "tool_calls", "provider_calls"}
        if counter not in allowed:
            raise ValueError("unknown task counter")
        result = await self.connection.execute(update(tables.tasks).where(
            tables.tasks.c.id == task_id,
            getattr(tables.tasks.c, counter) < cap,
        ).values({counter: getattr(tables.tasks.c, counter) + 1}))
        return result.rowcount == 1

    async def increment_provider_calls(self, task_id: TaskId) -> None:
        await self.connection.execute(update(tables.tasks).where(tables.tasks.c.id == task_id).values(
            provider_calls=tables.tasks.c.provider_calls + 1,
        ))
