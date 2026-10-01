from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from hekate.domain.budget_math import price_usage
from hekate.domain.contracts import canonical_json_hash
from hekate.domain.errors import BudgetDenied, Conflict, PolicyDenied, StaleInput, UnknownExecution
from hekate.domain.models import (
    Account,
    AccountSnapshot,
    BillableCallIntent,
    BudgetReservation,
    CallObservation,
    CallPermit,
    NormalizedUsage,
    OutboxJob,
    PriceTable,
    ReservationRequest,
    SettlementReceipt,
    UsageObservation,
    UsageReceipt,
)
from hekate.domain.types import AccountingCallId, AttemptId, OperationId, PermitId, ReservationId, TaskId

from . import tables
from .common import aware_now, json_value, new_key, require_money


class PostgresBudgetRepository:
    def __init__(self, connection: AsyncSession) -> None:
        self.connection = connection

    async def create_account(self, account: AccountSnapshot) -> None:
        for amount in (account.limit_amount, account.spent_amount, account.held_amount):
            require_money(amount)
        await self.connection.execute(insert(tables.budget_accounts).values(
            id=account.id,
            scope_kind=account.scope_kind,
            scope_ref=account.scope_ref,
            period_id=account.period_id,
            limit_amount=account.limit_amount,
            spent_amount=account.spent_amount,
            held_amount=account.held_amount,
        ))

    async def lock_accounts(self, account_ids: Sequence[str]) -> Sequence[Account]:
        ids = sorted(set(account_ids))
        if not ids:
            return ()
        rows = (await self.connection.execute(select(tables.budget_accounts).where(
            tables.budget_accounts.c.id.in_(ids),
        ).order_by(tables.budget_accounts.c.id).with_for_update())).mappings().all()
        if len(rows) != len(ids):
            raise BudgetDenied("budget account is unavailable")
        return tuple(Account(
            id=row["id"],
            scope_kind=row["scope_kind"],
            scope_ref=row["scope_ref"],
            period_id=row["period_id"],
            limit_amount=row["limit_amount"],
            spent_amount=row["spent_amount"],
            held_amount=row["held_amount"],
        ) for row in rows)

    async def reserve_operation(self, request: ReservationRequest) -> BudgetReservation:
        amount = require_money(request.amount)
        accounts = await self.lock_accounts([request.task_account_id, request.system_account_id])
        if len(accounts) != 2:
            raise BudgetDenied("task and system accounts must be distinct")
        by_id = {account.id: account for account in accounts}
        task_account = by_id[request.task_account_id]
        system_account = by_id[request.system_account_id]
        if task_account.scope_kind != "TASK" or task_account.scope_ref == "":
            raise BudgetDenied("task budget account binding is invalid")
        if task_account.scope_ref != str(request.task_id):
            raise BudgetDenied("task budget account belongs to another task")
        if system_account.scope_kind != "SYSTEM" or system_account.period_id != request.system_period_id:
            raise BudgetDenied("system budget period is invalid")
        existing = (await self.connection.execute(select(tables.budget_reservations).where(
            tables.budget_reservations.c.operation_id == request.operation_id,
            tables.budget_reservations.c.purpose == request.purpose,
        ).with_for_update())).mappings().one_or_none()
        if existing is not None:
            account_ids = (await self.connection.execute(select(tables.reservation_accounts.c.account_id).where(
                tables.reservation_accounts.c.reservation_id == existing["id"],
            ).order_by(tables.reservation_accounts.c.account_id))).scalars().all()
            if (
                existing["id"] != request.id
                or existing["amount"] != amount
                or existing["pricing_version"] != request.pricing_version
                or existing["system_period_id"] != request.system_period_id
                or tuple(account_ids) != tuple(sorted((request.task_account_id, request.system_account_id)))
            ):
                raise Conflict("operation reservation request changed")
            return self._reservation(existing)
        if any(account.available < amount for account in accounts):
            raise BudgetDenied("task or system budget is insufficient")
        await self.connection.execute(insert(tables.budget_reservations).values(
            id=request.id,
            operation_id=request.operation_id,
            purpose=request.purpose,
            amount=amount,
            status="RESERVED",
            pricing_version=request.pricing_version,
            system_period_id=request.system_period_id,
        ))
        for account in accounts:
            await self.connection.execute(update(tables.budget_accounts).where(
                tables.budget_accounts.c.id == account.id,
            ).values(held_amount=tables.budget_accounts.c.held_amount + amount))
            await self.connection.execute(insert(tables.reservation_accounts).values(
                reservation_id=request.id,
                account_id=account.id,
                held_amount=amount,
            ))
            await self._ledger(
                account.id,
                f"reserve:{request.id}",
                "HOLD",
                amount,
                held_delta=amount,
                reservation_id=request.id,
            )
        return BudgetReservation(
            id=request.id,
            operation_id=request.operation_id,
            purpose=request.purpose,
            reserved_amount=amount,
            settled_amount=None,
            status="RESERVED",
            pricing_version=request.pricing_version,
        )

    @staticmethod
    def _reservation(row) -> BudgetReservation:
        return BudgetReservation(
            id=ReservationId(row["id"]),
            operation_id=OperationId(row["operation_id"]),
            purpose=row["purpose"],
            reserved_amount=row["amount"],
            settled_amount=None,
            status=row["status"],
            pricing_version=row["pricing_version"],
        )

    async def get_reservation(self, reservation_id: ReservationId, *, lock: bool = False):
        query = select(tables.budget_reservations).where(tables.budget_reservations.c.id == reservation_id)
        if lock:
            query = query.with_for_update()
        return (await self.connection.execute(query)).mappings().one_or_none()

    async def allocate_call(
        self,
        intent: BillableCallIntent,
        reservation_id: ReservationId,
        attempt_id: AttemptId,
        envelope,
        intent_hash: str,
    ) -> CallPermit:
        account_ids = (await self.connection.execute(select(tables.reservation_accounts.c.account_id).where(
            tables.reservation_accounts.c.reservation_id == reservation_id,
        ).order_by(tables.reservation_accounts.c.account_id))).scalars().all()
        if not account_ids:
            raise StaleInput("reservation accounts are unavailable")
        await self.lock_accounts(account_ids)
        reservation = await self.get_reservation(reservation_id, lock=True)
        if reservation is None or reservation["operation_id"] != intent.operation_id:
            raise StaleInput("operation reservation is unavailable")
        if reservation["status"] not in {"RESERVED", "PENDING_SETTLEMENT"}:
            raise BudgetDenied("operation reservation is no longer open")
        unresolved_usage_issue = (await self.connection.execute(select(
            tables.provider_calls.c.accounting_call_id,
        ).select_from(tables.provider_calls.join(
            tables.usage_projections,
            tables.usage_projections.c.accounting_call_id == tables.provider_calls.c.accounting_call_id,
        )).where(
            tables.provider_calls.c.reservation_id == reservation_id,
            tables.usage_projections.c.has_conflict.is_(True) | tables.usage_projections.c.overrun.is_(True),
        ).limit(1))).first() is not None
        if unresolved_usage_issue:
            raise BudgetDenied("usage conflict or overrun blocks new call allocation")
        if reservation["pricing_version"] != intent.price_table.version:
            raise PolicyDenied("pricing version changed")
        if intent.model not in envelope.model_allowlist:
            raise PolicyDenied("model is outside the persisted allowlist")
        if intent.binding.task_id != envelope.task_id or intent.binding.attempt_id != envelope.attempt_id or intent.binding.input_revision != envelope.input_revision or intent.binding.fence != envelope.fence:
            raise Conflict("call binding differs from the persisted envelope")
        if intent.limits.max_input_tokens > envelope.max_input_tokens or intent.limits.max_output_tokens > envelope.max_output_tokens:
            raise PolicyDenied("call token limits exceed the persisted envelope")
        if intent.limits.deadline > envelope.deadline or intent.permit_expires_at > envelope.deadline or intent.permit_expires_at <= aware_now():
            raise PolicyDenied("call deadline or permit expiry is invalid")
        if intent.price_table.synthetic != intent.test_only:
            raise PolicyDenied("synthetic profiles are restricted to test-only permits")
        if not intent.test_only and not (
            intent.price_table.model_profile_verified
            and intent.price_table.pricing_verified
            and intent.price_table.tokenizer_verified
        ):
            raise PolicyDenied("model, tokenizer, or pricing profile is unverified")
        if intent.price_table.model != intent.model:
            raise PolicyDenied("price table model binding changed")
        require_money(intent.price_table.input_usd_per_million)
        require_money(intent.price_table.output_usd_per_million)
        estimate = price_usage(NormalizedUsage(
            completeness="COMPLETE",
            input_tokens=intent.limits.max_input_tokens,
            output_tokens=intent.limits.max_output_tokens,
            total_tokens=intent.limits.max_input_tokens + intent.limits.max_output_tokens,
        ), intent.price_table).amount
        allocation = require_money(intent.allocation_amount)
        if allocation < estimate:
            raise BudgetDenied("call allocation is below its frozen token-cost bound")
        accounts = (await self.connection.execute(select(tables.reservation_accounts).where(
            tables.reservation_accounts.c.reservation_id == reservation_id,
        ).order_by(tables.reservation_accounts.c.account_id).with_for_update())).mappings().all()
        if len(accounts) != 2:
            raise BudgetDenied("reservation does not hold both budget accounts")
        by_id = {row["id"]: row for row in (await self.connection.execute(select(tables.budget_accounts).where(
            tables.budget_accounts.c.id.in_([item["account_id"] for item in accounts]),
        ))).mappings().all()}
        if any(by_id[item["account_id"]]["spent_amount"] + by_id[item["account_id"]]["held_amount"] > by_id[item["account_id"]]["limit_amount"] for item in accounts):
            raise BudgetDenied("budget account is overrun")
        existing_rows = (await self.connection.execute(select(tables.provider_calls).where(
            (tables.provider_calls.c.accounting_call_id == intent.accounting_call_id)
            | ((tables.provider_calls.c.operation_id == intent.operation_id) & (tables.provider_calls.c.slot_key == intent.slot_key)),
        ).with_for_update())).mappings().all()
        if len(existing_rows) > 1:
            raise Conflict("accounting call id and slot are bound to different calls")
        existing = existing_rows[0] if existing_rows else None
        if existing is not None:
            if existing["intent_hash"] != intent_hash:
                raise Conflict("accounting call or slot is bound to another intent")
            if existing["status"] != "ALLOCATED":
                raise UnknownExecution("call permit was already consumed or used")
            permit = (await self.connection.execute(select(tables.call_permits).where(
                tables.call_permits.c.permit_id == existing["permit_id"],
            ).with_for_update())).mappings().one()
            if permit["state"] != "ISSUED" or permit["expires_at"] <= aware_now():
                raise UnknownExecution("call permit is no longer forwardable")
            return self._permit(existing, permit, consumed=False)
        now = aware_now()
        existing_count = (await self.connection.execute(select(func.count()).select_from(tables.provider_calls).where(
            tables.provider_calls.c.operation_id == intent.operation_id,
            (tables.provider_calls.c.status.in_(["CONSUMED", "DISPATCHED", "RUNNING", "UNKNOWN", "QUIESCENT"]))
            | ((tables.provider_calls.c.status == "ALLOCATED") & (tables.provider_calls.c.expires_at > now)),
        ))).scalar_one()
        if existing_count >= envelope.billable_call_slots:
            raise BudgetDenied("operation provider-call cap reached")
        allocated = (await self.connection.execute(select(func.coalesce(func.sum(tables.provider_calls.c.allocation_amount), 0)).where(
            tables.provider_calls.c.reservation_id == reservation_id,
            (tables.provider_calls.c.status.in_(["CONSUMED", "DISPATCHED", "RUNNING", "UNKNOWN", "QUIESCENT"]))
            | ((tables.provider_calls.c.status == "ALLOCATED") & (tables.provider_calls.c.expires_at > now)),
        ))).scalar_one()
        if Decimal(allocated) + allocation > reservation["amount"]:
            raise BudgetDenied("operation reservation has insufficient unallocated amount")
        if any(account["held_amount"] < allocation for account in accounts):
            raise BudgetDenied("reservation hold cannot cover the new call allocation")
        if reservation["purpose"] == "final_response" and intent.call_kind != "final_response":
            raise PolicyDenied("final-response reservation is isolated to final-response calls")
        if reservation["purpose"] != "final_response" and intent.call_kind == "final_response":
            raise PolicyDenied("final-response call requires a final-response reservation")
        await self.connection.execute(insert(tables.provider_calls).values(
            accounting_call_id=intent.accounting_call_id,
            permit_id=intent.permit_id,
            operation_id=intent.operation_id,
            attempt_id=attempt_id,
            reservation_id=reservation_id,
            registry_id=intent.binding.agent_registry_id,
            call_kind=intent.call_kind,
            slot_key=intent.slot_key,
            intent_hash=intent_hash,
            model=intent.model,
            status="ALLOCATED",
            max_input_tokens=intent.limits.max_input_tokens,
            max_output_tokens=intent.limits.max_output_tokens,
            allocation_amount=allocation,
            pricing_version=intent.price_table.version,
            input_usd_per_million=intent.price_table.input_usd_per_million,
            output_usd_per_million=intent.price_table.output_usd_per_million,
            model_profile_verified=intent.price_table.model_profile_verified,
            pricing_verified=intent.price_table.pricing_verified,
            tokenizer_verified=intent.price_table.tokenizer_verified,
            test_only=intent.test_only,
            input_revision=intent.binding.input_revision,
            fence=intent.binding.fence,
            conversation_id=intent.binding.conversation_id,
            lease_owner=intent.lease_owner,
            expires_at=intent.permit_expires_at,
            created_at=now,
        ))
        await self.connection.execute(insert(tables.call_permits).values(
            permit_id=intent.permit_id,
            accounting_call_id=intent.accounting_call_id,
            state="ISSUED",
            expires_at=intent.permit_expires_at,
        ))
        for account in accounts:
            await self.connection.execute(insert(tables.call_allocations).values(
                accounting_call_id=intent.accounting_call_id,
                reservation_id=reservation_id,
                account_id=account["account_id"],
                amount=allocation,
            ))
        await self.connection.execute(update(tables.tasks).where(
            tables.tasks.c.id == intent.binding.task_id,
        ).values(provider_calls=tables.tasks.c.provider_calls + 1))
        return CallPermit(
            permit_id=intent.permit_id,
            accounting_call_id=intent.accounting_call_id,
            operation_id=intent.operation_id,
            model=intent.model,
            max_input_tokens=intent.limits.max_input_tokens,
            max_output_tokens=intent.limits.max_output_tokens,
            expires_at=intent.permit_expires_at,
            consumed=False,
            test_only=intent.test_only,
        )

    @staticmethod
    def _permit(call, permit, consumed: bool) -> CallPermit:
        return CallPermit(
            permit_id=PermitId(call["permit_id"]),
            accounting_call_id=AccountingCallId(call["accounting_call_id"]),
            operation_id=OperationId(call["operation_id"]),
            model=call["model"],
            max_input_tokens=call["max_input_tokens"],
            max_output_tokens=call["max_output_tokens"],
            expires_at=permit["expires_at"],
            consumed=consumed,
            test_only=call["test_only"],
        )

    async def get_call_descriptor(self, accounting_call_id: AccountingCallId):
        return (await self.connection.execute(select(
            tables.provider_calls.c.accounting_call_id,
            tables.provider_calls.c.operation_id,
            tables.provider_calls.c.attempt_id,
            tables.provider_calls.c.registry_id,
            tables.operations.c.task_id,
            tables.provider_calls.c.reservation_id,
            tables.operations.c.owner_scope,
            tables.operations.c.binding,
            tables.operations.c.envelope,
            tables.operations.c.state.label("operation_state"),
            tables.provider_calls.c.status,
            tables.provider_calls.c.permit_id,
            tables.provider_calls.c.lease_owner,
            tables.provider_calls.c.input_revision,
            tables.provider_calls.c.fence,
            tables.provider_calls.c.conversation_id,
            tables.provider_calls.c.expires_at,
            tables.provider_calls.c.pricing_version,
        ).select_from(tables.provider_calls.join(
            tables.operations, tables.operations.c.id == tables.provider_calls.c.operation_id,
        )).where(tables.provider_calls.c.accounting_call_id == accounting_call_id))).mappings().one_or_none()

    async def consume_call_permit(self, permit_id: PermitId, accounting_call_id: AccountingCallId) -> CallPermit:
        accounts = await self.lock_call_accounts(accounting_call_id)
        if any(account.spent_amount + account.held_amount > account.limit_amount for account in accounts):
            raise BudgetDenied("budget account is overrun")
        call = (await self.connection.execute(select(tables.provider_calls).where(
            tables.provider_calls.c.accounting_call_id == accounting_call_id,
        ).with_for_update())).mappings().one_or_none()
        permit = (await self.connection.execute(select(tables.call_permits).where(
            tables.call_permits.c.permit_id == permit_id,
        ).with_for_update())).mappings().one_or_none()
        if call is None or permit is None or call["permit_id"] != permit_id:
            raise StaleInput("call permit is unavailable")
        if permit["state"] != "ISSUED" or call["status"] != "ALLOCATED":
            raise UnknownExecution("call permit is single-use and is not forwardable")
        if permit["expires_at"] <= aware_now():
            raise BudgetDenied("call permit expired")
        await self.connection.execute(update(tables.call_permits).where(
            tables.call_permits.c.permit_id == permit_id,
            tables.call_permits.c.state == "ISSUED",
        ).values(state="CONSUMED", consumed_at=aware_now()))
        await self.connection.execute(update(tables.provider_calls).where(
            tables.provider_calls.c.accounting_call_id == accounting_call_id,
            tables.provider_calls.c.status == "ALLOCATED",
        ).values(status="CONSUMED", consumed_at=aware_now()))
        return self._permit(call, {**permit, "expires_at": permit["expires_at"]}, consumed=True)

    async def get_call(self, accounting_call_id: AccountingCallId, *, lock: bool = False):
        query = select(tables.provider_calls).where(tables.provider_calls.c.accounting_call_id == accounting_call_id)
        if lock:
            query = query.with_for_update()
        return (await self.connection.execute(query)).mappings().one_or_none()

    async def record_call_observation(self, observation: CallObservation) -> None:
        await self.lock_call_accounts(observation.accounting_call_id)
        call = await self.get_call(observation.accounting_call_id, lock=True)
        if call is None:
            raise StaleInput("provider call is unavailable")
        op = (await self.connection.execute(select(tables.operations.c.binding).where(
            tables.operations.c.id == call["operation_id"],
        ))).scalar_one()
        if op != json_value(observation.binding):
            raise Conflict("provider-call observation binding mismatch")
        if observation.source not in {"bridge_send", "bridge_running", "bridge_disconnect", "provider_response", "bridge_result", "provider_stopped"}:
            raise Conflict("untrusted provider-call observation source")
        if observation.state == "QUIESCENT":
            if observation.source not in {"provider_response", "bridge_result", "provider_stopped"}:
                raise Conflict("untrusted provider-call quiescence source")
            if call["status"] not in {"CONSUMED", "DISPATCHED", "RUNNING", "UNKNOWN"}:
                raise Conflict("provider-call quiescence requires a consumed call")
            new_state = "QUIESCENT"
        elif observation.state == "RUNNING":
            if call["status"] not in {"CONSUMED", "DISPATCHED", "RUNNING"}:
                raise Conflict("provider call cannot resume from its observed state")
            new_state = "RUNNING"
        elif observation.state == "UNKNOWN":
            if call["status"] not in {"CONSUMED", "DISPATCHED", "RUNNING", "UNKNOWN"}:
                raise Conflict("unknown outcome requires a consumed provider call")
            new_state = "UNKNOWN"
        else:
            raise ValueError("unsupported provider-call observation state")
        if observation.provider_call_id and call["provider_call_id"] and str(observation.provider_call_id) != call["provider_call_id"]:
            raise Conflict("provider call id changed")
        if call["status"] == "QUIESCENT":
            if observation.state == "QUIESCENT":
                return
            raise Conflict("quiescent provider call cannot be reopened")
        await self.connection.execute(update(tables.provider_calls).where(
            tables.provider_calls.c.accounting_call_id == observation.accounting_call_id,
        ).values(
            status=new_state,
            dispatched_at=observation.observed_at if new_state in {"RUNNING", "UNKNOWN"} else call["dispatched_at"],
            provider_call_id=observation.provider_call_id or call["provider_call_id"],
        ))
        if new_state == "UNKNOWN":
            await self.connection.execute(update(tables.operations).where(
                tables.operations.c.id == call["operation_id"],
            ).values(
                state="UNKNOWN",
                dispatch_state="UNKNOWN",
                execution_state="UNKNOWN",
                updated_at=observation.observed_at,
            ))
            await self.connection.execute(update(tables.agent_execution_holds).where(
                tables.agent_execution_holds.c.operation_id == call["operation_id"],
                tables.agent_execution_holds.c.quiescent_at.is_(None),
            ).values(state="UNKNOWN", reason="provider call outcome unknown"))
            await self.connection.execute(pg_insert(tables.recovery_cases).values(
                operation_id=call["operation_id"],
                reason="provider_call_outcome_unknown",
                observations={"accounting_call_id": str(observation.accounting_call_id), "source": observation.source},
                updated_at=observation.observed_at,
            ).on_conflict_do_update(
                index_elements=[tables.recovery_cases.c.operation_id],
                set_={
                    "reason": "provider_call_outcome_unknown",
                    "observations": {"accounting_call_id": str(observation.accounting_call_id), "source": observation.source},
                    "updated_at": observation.observed_at,
                },
            ))
        elif new_state == "RUNNING":
            await self.connection.execute(update(tables.operations).where(
                tables.operations.c.id == call["operation_id"],
                tables.operations.c.execution_state != "UNKNOWN",
            ).values(dispatch_state="DISPATCHED", execution_state="RUNNING", updated_at=observation.observed_at))
            await self.connection.execute(update(tables.agent_execution_holds).where(
                tables.agent_execution_holds.c.operation_id == call["operation_id"],
                tables.agent_execution_holds.c.quiescent_at.is_(None),
                tables.agent_execution_holds.c.state != "UNKNOWN",
            ).values(state="RUNNING"))

    async def record_usage(self, observation: UsageObservation) -> UsageReceipt:
        await self.lock_call_accounts(observation.accounting_call_id)
        call = await self.get_call(observation.accounting_call_id, lock=True)
        if call is None:
            raise StaleInput("unknown accounting call id")
        if call["status"] in {"ALLOCATED", "EXPIRED", "REVOKED"}:
            raise Conflict("usage cannot be attributed before a consumed provider call")
        persisted_binding = (await self.connection.execute(select(tables.operations.c.binding).where(
            tables.operations.c.id == call["operation_id"],
        ))).scalar_one()
        if persisted_binding != json_value(observation.binding):
            raise Conflict("usage observation binding mismatch")
        usage = observation.usage
        if usage.completeness not in {"UNKNOWN", "PARTIAL", "COMPLETE"}:
            raise ValueError("invalid usage completeness")
        if observation.source not in {"runtime_reported", "provider_reported", "runtime_estimated"}:
            raise ValueError("unsupported usage source")
        for count in (usage.input_tokens, usage.output_tokens, usage.cache_tokens, usage.reasoning_tokens, usage.total_tokens):
            if count is not None and (not isinstance(count, int) or isinstance(count, bool) or count < 0):
                raise ValueError("usage token counts must be nonnegative integers")
        if usage.reported_cost_usd is not None:
            require_money(usage.reported_cost_usd)
        if usage.completeness == "COMPLETE" and any(value is None for value in (usage.input_tokens, usage.output_tokens, usage.total_tokens)):
            raise ValueError("complete usage requires input, output, and total token counts")
        values = {
            "completeness": usage.completeness,
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "cache_tokens": usage.cache_tokens,
            "reasoning_tokens": usage.reasoning_tokens,
            "total_tokens": usage.total_tokens,
            "reported_cost_usd": str(usage.reported_cost_usd) if usage.reported_cost_usd is not None else None,
        }
        payload_hash = canonical_json_hash(values)
        existing = (await self.connection.execute(select(tables.usage_observations).where(
            tables.usage_observations.c.accounting_call_id == observation.accounting_call_id,
            tables.usage_observations.c.source == observation.source,
            tables.usage_observations.c.payload_hash == payload_hash,
        ).with_for_update())).mappings().one_or_none()
        duplicate = existing is not None
        source_conflict = (await self.connection.execute(select(tables.usage_observations.c.id).where(
            tables.usage_observations.c.accounting_call_id == observation.accounting_call_id,
            tables.usage_observations.c.source == observation.source,
            tables.usage_observations.c.observation_identity == observation.observation_identity,
            tables.usage_observations.c.payload_hash != payload_hash,
        ).limit(1))).first() is not None
        if existing is None:
            observation_id = new_key()
            await self.connection.execute(insert(tables.usage_observations).values(
                id=observation_id,
                accounting_call_id=observation.accounting_call_id,
                provider_call_id=observation.provider_call_id,
                source=observation.source,
                observation_identity=observation.observation_identity,
                payload_hash=payload_hash,
                observed_at=observation.observed_at,
                **values,
            ))
        else:
            observation_id = existing["id"]
        projection = (await self.connection.execute(select(tables.usage_projections).where(
            tables.usage_projections.c.accounting_call_id == observation.accounting_call_id,
        ).with_for_update())).mappings().one_or_none()
        fields = ("input_tokens", "output_tokens", "cache_tokens", "reasoning_tokens", "total_tokens", "reported_cost_usd")
        conflicts = source_conflict
        merged: dict[str, object] = {}
        for field in fields:
            old = projection[field] if projection else None
            new = usage.reported_cost_usd if field == "reported_cost_usd" else getattr(usage, field)
            if old is not None and new is not None and old != new:
                conflicts = True
            merged[field] = old if old is not None else new
        if observation.provider_call_id and call["provider_call_id"] and str(observation.provider_call_id) != call["provider_call_id"]:
            conflicts = True
        elif observation.provider_call_id and call["provider_call_id"] is None:
            await self.connection.execute(update(tables.provider_calls).where(
                tables.provider_calls.c.accounting_call_id == observation.accounting_call_id,
            ).values(provider_call_id=str(observation.provider_call_id)))
        if projection and projection["has_conflict"]:
            conflicts = True
        has_values = any(merged[field] is not None for field in fields)
        complete = merged["input_tokens"] is not None and merged["output_tokens"] is not None and merged["total_tokens"] is not None
        completeness = "COMPLETE" if complete else "PARTIAL" if has_values else "UNKNOWN"
        reported_source = projection["reported_cost_source"] if projection else None
        if reported_source is None and usage.reported_cost_usd is not None and merged["reported_cost_usd"] == usage.reported_cost_usd:
            reported_source = observation.source
        settlement_state = "CONFLICT" if conflicts else projection["settlement_state"] if projection else "PENDING"
        projection_values = {
            "completeness": completeness,
            "has_conflict": conflicts,
            "settlement_state": settlement_state,
            **merged,
            "reported_cost_source": reported_source,
            "updated_at": aware_now(),
        }
        if projection is None:
            await self.connection.execute(insert(tables.usage_projections).values(
                accounting_call_id=observation.accounting_call_id,
                **projection_values,
            ))
        else:
            await self.connection.execute(update(tables.usage_projections).where(
                tables.usage_projections.c.accounting_call_id == observation.accounting_call_id,
            ).values(**projection_values))
        if conflicts:
            await self.connection.execute(update(tables.budget_reservations).where(
                tables.budget_reservations.c.id == call["reservation_id"],
            ).values(status="PENDING_SETTLEMENT"))
        return UsageReceipt(
            accounting_call_id=observation.accounting_call_id,
            observation_id=observation_id,
            duplicate=duplicate,
            conflict=conflicts,
            completeness=completeness,
            settlement_state=settlement_state,
        )

    async def mark_operation_calls_quiescent(self, operation_id: OperationId) -> None:
        call_ids = select(tables.provider_calls.c.accounting_call_id).where(
            tables.provider_calls.c.operation_id == operation_id,
            tables.provider_calls.c.status.in_(["CONSUMED", "DISPATCHED", "RUNNING", "UNKNOWN"]),
        )
        account_ids = (await self.connection.execute(select(tables.call_allocations.c.account_id).where(
            tables.call_allocations.c.accounting_call_id.in_(call_ids),
        ).order_by(tables.call_allocations.c.account_id))).scalars().all()
        if account_ids:
            await self.lock_accounts(account_ids)
        await self.connection.execute(update(tables.provider_calls).where(
            tables.provider_calls.c.operation_id == operation_id,
            tables.provider_calls.c.status.in_(["CONSUMED", "DISPATCHED", "RUNNING", "UNKNOWN"]),
        ).values(status="QUIESCENT"))

    async def lock_call_accounts(self, accounting_call_id: AccountingCallId) -> Sequence[Account]:
        account_ids = (await self.connection.execute(select(tables.call_allocations.c.account_id).where(
            tables.call_allocations.c.accounting_call_id == accounting_call_id,
        ))).scalars().all()
        if not account_ids:
            raise StaleInput("call allocation is unavailable")
        return await self.lock_accounts(account_ids)

    async def settle_call(self, accounting_call_id: AccountingCallId) -> SettlementReceipt:
        accounts = await self.lock_call_accounts(accounting_call_id)
        call = await self.get_call(accounting_call_id, lock=True)
        if call is None:
            raise StaleInput("provider call is unavailable")
        projection = (await self.connection.execute(select(tables.usage_projections).where(
            tables.usage_projections.c.accounting_call_id == accounting_call_id,
        ).with_for_update())).mappings().one_or_none()
        if projection and projection["settlement_state"] == "SETTLED":
            return await self._settlement_receipt(call, accounts, True, None, projection["evaluated_cost_usd"], projection["overrun"])
        reason = None
        cost: Decimal | None = None
        if call["status"] != "QUIESCENT":
            reason = "provider call execution is not confirmed quiescent"
        elif projection is None or projection["completeness"] == "UNKNOWN":
            reason = "usage is unknown"
        elif projection["has_conflict"]:
            reason = "usage conflict is unresolved"
        elif projection["reported_cost_usd"] is not None and projection["reported_cost_source"] == "provider_reported":
            cost = projection["reported_cost_usd"]
        elif projection["completeness"] == "COMPLETE" and (
            call["test_only"] or (call["model_profile_verified"] and call["pricing_verified"] and call["tokenizer_verified"])
        ):
            assessment = price_usage(NormalizedUsage(
                completeness=projection["completeness"],
                input_tokens=projection["input_tokens"],
                output_tokens=projection["output_tokens"],
                cache_tokens=projection["cache_tokens"],
                reasoning_tokens=projection["reasoning_tokens"],
                total_tokens=projection["total_tokens"],
            ), PriceTable(
                model=call["model"],
                version=call["pricing_version"],
                input_usd_per_million=call["input_usd_per_million"],
                output_usd_per_million=call["output_usd_per_million"],
                model_profile_verified=call["model_profile_verified"],
                pricing_verified=call["pricing_verified"],
                tokenizer_verified=call["tokenizer_verified"],
                synthetic=call["test_only"],
            ))
            cost = assessment.amount
        else:
            reason = "pricing or usage semantics are incomplete"
        if cost is None:
            await self.connection.execute(update(tables.usage_projections).where(
                tables.usage_projections.c.accounting_call_id == accounting_call_id,
            ).values(settlement_state="CONFLICT" if projection and projection["has_conflict"] else "PENDING"))
            await self.connection.execute(update(tables.budget_reservations).where(
                tables.budget_reservations.c.id == call["reservation_id"],
            ).values(status="PENDING_SETTLEMENT"))
            return await self._settlement_receipt(call, accounts, False, reason, None, False)
        require_money(cost)
        if projection is None:
            raise StaleInput("usage projection disappeared")
        allocations = (await self.connection.execute(select(tables.call_allocations).where(
            tables.call_allocations.c.accounting_call_id == accounting_call_id,
        ).order_by(tables.call_allocations.c.account_id).with_for_update())).mappings().all()
        overrun = cost > call["allocation_amount"]
        for allocation in allocations:
            account = next(item for item in accounts if item.id == allocation["account_id"])
            remaining = (await self.connection.execute(select(tables.reservation_accounts.c.held_amount).where(
                tables.reservation_accounts.c.reservation_id == call["reservation_id"],
                tables.reservation_accounts.c.account_id == account.id,
            ).with_for_update())).scalar_one()
            release = min(remaining, allocation["amount"])
            await self.connection.execute(update(tables.reservation_accounts).where(
                tables.reservation_accounts.c.reservation_id == call["reservation_id"],
                tables.reservation_accounts.c.account_id == account.id,
            ).values(held_amount=remaining - release))
            await self.connection.execute(update(tables.budget_accounts).where(
                tables.budget_accounts.c.id == account.id,
            ).values(
                held_amount=tables.budget_accounts.c.held_amount - release,
                spent_amount=tables.budget_accounts.c.spent_amount + cost,
            ))
            await self._ledger(
                account.id,
                f"settle:{accounting_call_id}",
                "SETTLE",
                cost,
                held_delta=-release,
                spent_delta=cost,
                reservation_id=call["reservation_id"],
                accounting_call_id=accounting_call_id,
            )
        await self.connection.execute(update(tables.usage_projections).where(
            tables.usage_projections.c.accounting_call_id == accounting_call_id,
        ).values(settlement_state="SETTLED", evaluated_cost_usd=cost, overrun=overrun, updated_at=aware_now()))
        await self._refresh_reservation_status(call["reservation_id"])
        return await self._settlement_receipt(call, accounts, True, None, cost, overrun)

    async def _refresh_reservation_status(self, reservation_id: ReservationId) -> None:
        pending = (await self.connection.execute(select(func.count()).select_from(
            tables.provider_calls.outerjoin(
                tables.usage_projections,
                tables.usage_projections.c.accounting_call_id == tables.provider_calls.c.accounting_call_id,
            )
        ).where(
            tables.provider_calls.c.reservation_id == reservation_id,
            tables.provider_calls.c.status.not_in(["EXPIRED", "REVOKED"]),
            (tables.provider_calls.c.status != "QUIESCENT")
            | (func.coalesce(tables.usage_projections.c.settlement_state, "PENDING") != "SETTLED"),
        ))).scalar_one()
        held = (await self.connection.execute(select(func.coalesce(
            func.sum(tables.reservation_accounts.c.held_amount), 0,
        )).where(tables.reservation_accounts.c.reservation_id == reservation_id))).scalar_one()
        status = "PENDING_SETTLEMENT" if pending or held > 0 else "SETTLED"
        await self.connection.execute(update(tables.budget_reservations).where(
            tables.budget_reservations.c.id == reservation_id,
        ).values(status=status, settled_at=None if status == "PENDING_SETTLEMENT" else aware_now()))

    async def _settlement_receipt(self, call, accounts, settled, reason, cost, overrun) -> SettlementReceipt:
        current = (await self.connection.execute(select(tables.budget_accounts).where(
            tables.budget_accounts.c.id.in_([account.id for account in accounts]),
        ))).mappings().all()
        values = {row["scope_kind"]: row for row in current}
        task = values.get("TASK")
        system = values.get("SYSTEM")
        return SettlementReceipt(
            accounting_call_id=AccountingCallId(call["accounting_call_id"]),
            settled=settled,
            pending_reason=reason,
            actual_cost=cost,
            task_spent=task["spent_amount"] if task else Decimal(0),
            task_held=task["held_amount"] if task else Decimal(0),
            system_spent=system["spent_amount"] if system else Decimal(0),
            system_held=system["held_amount"] if system else Decimal(0),
            overrun=overrun,
        )

    async def release_unallocated_after_quiescence(self, operation_id: OperationId) -> None:
        reservation_ids = (await self.connection.execute(select(tables.budget_reservations.c.id).where(
            tables.budget_reservations.c.operation_id == operation_id,
        ).order_by(tables.budget_reservations.c.id))).scalars().all()
        account_ids = (await self.connection.execute(select(tables.reservation_accounts.c.account_id).where(
            tables.reservation_accounts.c.reservation_id.in_(reservation_ids),
        ).order_by(tables.reservation_accounts.c.account_id))).scalars().all() if reservation_ids else []
        if account_ids:
            await self.lock_accounts(account_ids)
        reservations = (await self.connection.execute(select(tables.budget_reservations).where(
            tables.budget_reservations.c.id.in_(reservation_ids),
        ).order_by(tables.budget_reservations.c.id).with_for_update())).mappings().all()
        await self.connection.execute(update(tables.call_permits).where(
            tables.call_permits.c.state == "ISSUED",
            tables.call_permits.c.accounting_call_id.in_(select(tables.provider_calls.c.accounting_call_id).where(
                tables.provider_calls.c.operation_id == operation_id,
            )),
        ).values(state="REVOKED", revoked_at=aware_now()))
        await self.connection.execute(update(tables.provider_calls).where(
            tables.provider_calls.c.operation_id == operation_id,
            tables.provider_calls.c.status == "ALLOCATED",
        ).values(status="REVOKED"))
        for reservation in reservations:
            account_rows = (await self.connection.execute(select(tables.reservation_accounts).where(
                tables.reservation_accounts.c.reservation_id == reservation["id"],
            ).order_by(tables.reservation_accounts.c.account_id).with_for_update())).mappings().all()
            for reservation_account in account_rows:
                account_id = reservation_account["account_id"]
                active_allocations = (await self.connection.execute(select(func.coalesce(func.sum(tables.call_allocations.c.amount), 0)).select_from(
                    tables.call_allocations.join(
                        tables.provider_calls,
                        tables.provider_calls.c.accounting_call_id == tables.call_allocations.c.accounting_call_id,
                    ).outerjoin(
                        tables.usage_projections,
                        tables.usage_projections.c.accounting_call_id == tables.provider_calls.c.accounting_call_id,
                    )
                ).where(
                    tables.provider_calls.c.reservation_id == reservation["id"],
                    tables.call_allocations.c.account_id == account_id,
                    tables.provider_calls.c.status.not_in(["EXPIRED", "REVOKED"]),
                    func.coalesce(tables.usage_projections.c.settlement_state, "PENDING") != "SETTLED",
                    tables.usage_projections.c.evaluated_cost_usd.is_(None),
                ))).scalar_one()
                release = reservation_account["held_amount"] - Decimal(active_allocations)
                if release < 0:
                    raise Conflict("pending child allocations exceed the reservation hold")
                if release == 0:
                    continue
                await self.connection.execute(update(tables.reservation_accounts).where(
                    tables.reservation_accounts.c.reservation_id == reservation["id"],
                    tables.reservation_accounts.c.account_id == account_id,
                ).values(held_amount=reservation_account["held_amount"] - release))
                await self.connection.execute(update(tables.budget_accounts).where(
                    tables.budget_accounts.c.id == account_id,
                ).values(held_amount=tables.budget_accounts.c.held_amount - release))
                await self._ledger(
                    account_id,
                    f"unallocated-release:{reservation['id']}",
                    "RELEASE",
                    release,
                    held_delta=-release,
                    reservation_id=reservation["id"],
                )
            pending = (await self.connection.execute(select(func.count()).select_from(
                tables.provider_calls.outerjoin(
                    tables.usage_projections,
                    tables.usage_projections.c.accounting_call_id == tables.provider_calls.c.accounting_call_id,
                )
            ).where(
                tables.provider_calls.c.reservation_id == reservation["id"],
                tables.provider_calls.c.status.not_in(["EXPIRED", "REVOKED"]),
                (tables.provider_calls.c.status.in_(["CONSUMED", "DISPATCHED", "RUNNING", "UNKNOWN"]))
                | (func.coalesce(tables.usage_projections.c.settlement_state, "PENDING") != "SETTLED"),
            ))).scalar_one()
            outstanding_hold = (await self.connection.execute(select(func.coalesce(func.sum(tables.reservation_accounts.c.held_amount), 0)).where(
                tables.reservation_accounts.c.reservation_id == reservation["id"],
            ))).scalar_one()
            completed_calls = (await self.connection.execute(select(func.count()).select_from(tables.provider_calls).where(
                tables.provider_calls.c.reservation_id == reservation["id"],
                tables.provider_calls.c.status.not_in(["EXPIRED", "REVOKED"]),
            ))).scalar_one()
            await self.connection.execute(update(tables.budget_reservations).where(
                tables.budget_reservations.c.id == reservation["id"],
            ).values(
                status="PENDING_SETTLEMENT" if pending or outstanding_hold > 0 else "SETTLED" if completed_calls else "RELEASED",
                settled_at=None if pending or outstanding_hold > 0 else aware_now(),
            ))

    async def apply_adjustment(
        self,
        account_id: str,
        effect_key: str,
        amount: Decimal,
        *,
        reservation_id: ReservationId | None = None,
        accounting_call_id: AccountingCallId | None = None,
    ) -> bool:
        if not isinstance(amount, Decimal) or not amount.is_finite():
            raise ValueError("adjustment must be a finite Decimal")
        account = (await self.connection.execute(select(tables.budget_accounts).where(
            tables.budget_accounts.c.id == account_id,
        ).with_for_update())).mappings().one_or_none()
        if account is None:
            raise StaleInput("budget account is unavailable")
        existing = (await self.connection.execute(select(tables.budget_ledger).where(
            tables.budget_ledger.c.account_id == account_id,
            tables.budget_ledger.c.effect_key == effect_key,
        ))).mappings().one_or_none()
        if existing is not None:
            if existing["amount"] != amount:
                raise Conflict("adjustment effect key reused with another amount")
            return False
        if account["spent_amount"] + amount < 0:
            raise Conflict("adjustment would make spent amount negative")
        await self.connection.execute(update(tables.budget_accounts).where(
            tables.budget_accounts.c.id == account_id,
        ).values(spent_amount=tables.budget_accounts.c.spent_amount + amount))
        await self._ledger(
            account_id,
            effect_key,
            "ADJUSTMENT",
            amount,
            spent_delta=amount,
            reservation_id=reservation_id,
            accounting_call_id=accounting_call_id,
        )
        return True

    async def _ledger(
        self,
        account_id: str,
        effect_key: str,
        effect_type: str,
        amount: Decimal,
        *,
        held_delta: Decimal = Decimal(0),
        spent_delta: Decimal = Decimal(0),
        reservation_id: str | None = None,
        accounting_call_id: str | None = None,
    ) -> None:
        await self.connection.execute(insert(tables.budget_ledger).values(
            id=new_key(),
            account_id=account_id,
            reservation_id=reservation_id,
            accounting_call_id=accounting_call_id,
            effect_key=effect_key,
            effect_type=effect_type,
            amount=amount,
            held_delta=held_delta,
            spent_delta=spent_delta,
        ))

    async def list_pending_calls(self, limit: int = 100):
        if limit < 1:
            raise ValueError("limit must be positive")
        return (await self.connection.execute(select(
            tables.provider_calls.c.accounting_call_id,
            tables.provider_calls.c.operation_id,
            tables.provider_calls.c.status,
            tables.usage_projections.c.completeness,
            tables.usage_projections.c.settlement_state,
            tables.usage_projections.c.has_conflict,
        ).outerjoin(
            tables.usage_projections,
            tables.usage_projections.c.accounting_call_id == tables.provider_calls.c.accounting_call_id,
        ).where(
            tables.provider_calls.c.status != "EXPIRED",
            tables.provider_calls.c.status != "REVOKED",
            (tables.provider_calls.c.status != "QUIESCENT")
            | (tables.usage_projections.c.settlement_state.is_(None))
            | (tables.usage_projections.c.settlement_state != "SETTLED"),
        ).order_by(tables.provider_calls.c.created_at, tables.provider_calls.c.accounting_call_id).limit(limit))).mappings().all()

    async def call_ids_for_operation(self, operation_id: OperationId):
        return (await self.connection.execute(select(tables.provider_calls.c.accounting_call_id).where(
            tables.provider_calls.c.operation_id == operation_id,
        ).order_by(tables.provider_calls.c.created_at, tables.provider_calls.c.accounting_call_id))).scalars().all()

    async def call_ids_for_reservation(self, reservation_id: ReservationId):
        return (await self.connection.execute(select(tables.provider_calls.c.accounting_call_id).where(
            tables.provider_calls.c.reservation_id == reservation_id,
        ).order_by(tables.provider_calls.c.created_at, tables.provider_calls.c.accounting_call_id))).scalars().all()
