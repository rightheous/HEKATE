from __future__ import annotations

import asyncio
import os
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

from sqlalchemy import text
from sqlalchemy.engine import make_url

from hekate.application.budgets import (
    apply_adjustment,
    authorize_provider_call,
    consume_call_permit,
    reconcile_pending,
    record_call_observation,
    record_usage,
    reserve,
    settle,
    settle_call,
)
from hekate.application.operations import admit_operation, record_execution_observation
from hekate.application.tasks import cancel, revise
from hekate.domain.contracts import canonical_json_hash
from hekate.domain.errors import BudgetDenied, Conflict, PolicyDenied, StorageUnavailable, StaleInput, UnknownExecution
from hekate.domain.models import (
    AccountSnapshot,
    AdmissionRequest,
    AgentRecord,
    AuthorizationSnapshot,
    BillableCallIntent,
    CallObservation,
    ExecutionEnvelope,
    ExecutionObservation,
    GuardBinding,
    InputChange,
    NormalizedUsage,
    PriceTable,
    ReservationRequest,
    RuntimeLimits,
    SettlementReceipt,
    Task,
    UsageObservation,
    UsageRecord,
)
from hekate.domain.types import (
    AccountingCallId,
    ActorContext,
    AttemptId,
    OperationId,
    PermitId,
    PrincipalId,
    ProviderAgentId,
    ProviderCallId,
    RegistryId,
    ReservationId,
    ScopeId,
    StopReason,
    TaskId,
    TaskStatus,
)
from hekate.infrastructure.postgres import tables
from hekate.infrastructure.postgres.database import check_database, create_engine, create_uow_factory
from hekate.infrastructure.postgres.delivery_repository import PostgresDeliveryRepository


DATABASE_URL = os.environ.get("HEKATE_TEST_DATABASE_URL")


class Phase2PostgresTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not DATABASE_URL:
            self.skipTest("set HEKATE_TEST_DATABASE_URL to a disposable PostgreSQL database")
        parsed = make_url(DATABASE_URL)
        if parsed.database != "hekate_phase2_test" or parsed.host not in {"127.0.0.1", "localhost"}:
            raise RuntimeError("tests require the dedicated local hekate_phase2_test database")
        self.database_url = DATABASE_URL
        self.engine = create_engine(DATABASE_URL)
        self.factory = create_uow_factory(self.engine)
        names = ", ".join(f'"{name}"' for name in tables.metadata().tables)
        async with self.engine.begin() as connection:
            await connection.execute(text(f"TRUNCATE TABLE {names} CASCADE"))

    async def asyncTearDown(self) -> None:
        if hasattr(self, "engine"):
            await self.engine.dispose()

    async def _scalar(self, statement: str, **parameters):
        async with self.engine.connect() as connection:
            return await connection.scalar(text(statement), parameters)

    async def _seed(self, task_name: str, *, task_limit: Decimal = Decimal("20"), system_limit: Decimal = Decimal("20"), create_system: bool = True):
        now = datetime.now(UTC)
        task_id = TaskId(task_name)
        scope = ScopeId(f"scope:{task_name}")
        principal = PrincipalId(f"principal:{task_name}")
        registry_id = RegistryId(f"registry:{task_name}")
        provider_id = ProviderAgentId(f"provider:{task_name}")
        task_account_id = f"task-budget:{task_name}"
        system_account_id = "system-budget:2026-09"
        policy = "test-policy-v1"
        task = Task(
            id=task_id,
            scope=scope,
            question="phase two test task",
            input_revision=1,
            constraints_hash="a" * 64,
            status=TaskStatus.QUEUED,
            deadline=now + timedelta(minutes=30),
        )
        async with self.factory() as uow:
            await uow.tasks.insert_scope(AuthorizationSnapshot(scope, principal, policy, 1))
            await uow.tasks.insert_task(task, {"test": True})
            await uow.delivery.claim_operation(
                OperationId(f"agent-intent:{task_name}"),
                scope,
                task_id,
                "agent.create",
                "b" * 64,
                {},
                {},
            )
            await uow.agents.insert_intent(AgentRecord(
                registry_id=registry_id,
                owner_scope=scope,
                kind="hekate",
                creation_operation_id=OperationId(f"agent-intent:{task_name}"),
                provider_id=provider_id,
                intended_state="READY",
                observation="PRESENT",
                policy_version=policy,
            ))
            await uow.budgets.create_account(AccountSnapshot(
                id=task_account_id,
                scope_kind="TASK",
                scope_ref=str(task_id),
                period_id="task-lifetime",
                limit_amount=task_limit,
                spent_amount=Decimal(0),
                held_amount=Decimal(0),
            ))
            if create_system:
                await uow.budgets.create_account(AccountSnapshot(
                    id=system_account_id,
                    scope_kind="SYSTEM",
                    scope_ref="hekate",
                    period_id="2026-09",
                    limit_amount=system_limit,
                    spent_amount=Decimal(0),
                    held_amount=Decimal(0),
                ))
            await uow.commit()
        async with self.factory() as uow:
            lease = await uow.agents.acquire_lease(registry_id, f"worker:{task_name}", 1800)
            self.assertIsNotNone(lease)
            await uow.commit()
        return {
            "task_id": task_id,
            "scope": scope,
            "principal": principal,
            "registry_id": registry_id,
            "provider_id": provider_id,
            "task_account_id": task_account_id,
            "system_account_id": system_account_id,
            "policy": policy,
            "task_deadline": task.deadline,
            "lease": lease,
        }

    def _request(
        self,
        context,
        *,
        operation_id: OperationId | None = None,
        attempt_id: AttemptId | None = None,
        reservation_id: ReservationId | None = None,
        amount: Decimal = Decimal("8"),
        slots: int = 2,
        binding: GuardBinding | None = None,
        lease=None,
        payload=None,
    ) -> AdmissionRequest:
        lease = lease or context["lease"]
        operation_id = operation_id or OperationId(f"operation:{context['task_id']}")
        attempt_id = attempt_id or AttemptId(f"attempt:{context['task_id']}")
        reservation_id = reservation_id or ReservationId(f"reservation:{operation_id}")
        binding = binding or GuardBinding(
            task_id=context["task_id"],
            attempt_id=attempt_id,
            agent_registry_id=context["registry_id"],
            provider_agent_id=context["provider_id"],
            principal_id=context["principal"],
            scope=context["scope"],
            input_revision=1,
            policy_version=context["policy"],
            authz_epoch=1,
            fence=lease.fence,
            conversation_id=f"conversation:{context['task_id']}",
        )
        deadline = min(context["task_deadline"], datetime.now(UTC) + timedelta(minutes=20))
        reservation = ReservationRequest(
            id=reservation_id,
            operation_id=operation_id,
            purpose="operation_envelope",
            amount=amount,
            task_id=context["task_id"],
            task_account_id=context["task_account_id"],
            system_account_id=context["system_account_id"],
            pricing_version="test-price-v1",
            system_period_id="2026-09",
        )
        envelope = ExecutionEnvelope(
            task_id=context["task_id"],
            attempt_id=attempt_id,
            operation_id=operation_id,
            principal_id=context["principal"],
            scope=context["scope"],
            input_revision=1,
            model_allowlist=("test-model",),
            pricing_version="test-price-v1",
            deadline=deadline,
            max_input_tokens=10,
            max_output_tokens=10,
            billable_call_slots=slots,
            max_tool_calls=4,
            fence=binding.fence,
            reservation_id=reservation_id,
        )
        return AdmissionRequest(
            binding=binding,
            reservation=reservation,
            envelope=envelope,
            attempt_kind="planning",
            parent_attempt_id=None,
            operation_kind="turn",
            payload=payload or {"prompt": "synthetic"},
            lease_owner=lease.owner,
        )

    def _call(self, request: AdmissionRequest, slot: str, *, allocation: Decimal = Decimal("4")) -> BillableCallIntent:
        return BillableCallIntent(
            accounting_call_id=AccountingCallId(f"accounting:{slot}"),
            permit_id=PermitId(f"permit:{slot}"),
            operation_id=request.envelope.operation_id,
            call_kind="turn",
            slot_key=slot,
            binding=request.binding,
            model="test-model",
            allocation_amount=allocation,
            limits=RuntimeLimits(10, 10, 1, request.envelope.deadline),
            price_table=PriceTable(
                model="test-model",
                version="test-price-v1",
                input_usd_per_million=Decimal("1"),
                output_usd_per_million=Decimal("1"),
                synthetic=True,
            ),
            permit_expires_at=min(request.envelope.deadline, datetime.now(UTC) + timedelta(minutes=5)),
            lease_owner=request.lease_owner,
            test_only=True,
        )

    async def _admit(self, context, **kwargs):
        request = self._request(context, **kwargs)
        receipt = await admit_operation(self.factory, request)
        return request, receipt

    async def test_ta_public_settlement_entrypoints_keep_complete_and_pending_distinct(self):
        context = await self._seed("settlement-entrypoints")
        request, _ = await self._admit(context)
        call_a = self._call(request, "settlement-entry-a")
        call_b = self._call(request, "settlement-entry-b")
        for call in (call_a, call_b):
            permit = await authorize_provider_call(self.factory, call)
            await consume_call_permit(
                self.factory, request.binding, request.lease_owner, permit.permit_id, permit.accounting_call_id,
            )
            await record_call_observation(self.factory, CallObservation(
                accounting_call_id=call.accounting_call_id,
                binding=request.binding,
                state="QUIESCENT",
                source="provider_response",
                observed_at=datetime.now(UTC),
                lease_owner=request.lease_owner,
                observer_fence=context["lease"].fence,
            ))

        usage_a = UsageRecord(
            accounting_call_id=call_a.accounting_call_id,
            binding=request.binding,
            observation_identity="settlement-complete-a",
            source="provider_reported",
            observed_at=datetime.now(UTC),
            input_tokens=1,
            output_tokens=1,
            total_tokens=2,
            completeness="COMPLETE",
            pricing_version="test-price-v1",
            monetary_amount=Decimal("3"),
        )
        usage_b = UsageRecord(
            accounting_call_id=call_b.accounting_call_id,
            binding=request.binding,
            observation_identity="settlement-partial-b",
            source="runtime_reported",
            observed_at=datetime.now(UTC),
            input_tokens=1,
            completeness="PARTIAL",
            pricing_version="test-price-v1",
        )
        self.assertEqual((await record_usage(self.factory, usage_a)).settlement_state, "SETTLED")
        pending_usage = await record_usage(self.factory, usage_b)
        self.assertEqual(pending_usage.settlement_state, "PENDING")

        direct = await settle_call(self.factory, call_a.accounting_call_id)
        operation_receipts = await settle(self.factory, request.envelope.operation_id)
        reservation_receipts = await reconcile_pending(self.factory, request.reservation.id)
        self.assertIsInstance(direct, SettlementReceipt)
        self.assertTrue(direct.settled)
        self.assertEqual(direct.actual_cost, Decimal("3"))
        for receipts in (operation_receipts, reservation_receipts):
            self.assertEqual(len(receipts), 2)
            self.assertTrue(all(isinstance(item, SettlementReceipt) for item in receipts))
            by_id = {receipt.accounting_call_id: receipt for receipt in receipts}
            self.assertTrue(by_id[call_a.accounting_call_id].settled)
            self.assertEqual(by_id[call_a.accounting_call_id].actual_cost, Decimal("3"))
            self.assertFalse(by_id[call_b.accounting_call_id].settled)
            self.assertIsNone(by_id[call_b.accounting_call_id].actual_cost)
            self.assertIsNotNone(by_id[call_b.accounting_call_id].pending_reason)
        self.assertEqual(await self._scalar("SELECT count(*) FROM budget_ledger WHERE effect_type='SETTLE'"), 2)
        self.assertEqual(await self._scalar(
            "SELECT spent_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"],
        ), Decimal("3"))
        self.assertEqual(await self._scalar(
            "SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"],
        ), Decimal("4"))
        self.assertEqual(await self._scalar(
            "SELECT spent_amount FROM budget_accounts WHERE id=:id", id=context["system_account_id"],
        ), Decimal("3"))
        self.assertEqual(await self._scalar(
            "SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["system_account_id"],
        ), Decimal("4"))

        empty_context = await self._seed("settlement-empty", create_system=False)
        empty_request, _ = await self._admit(empty_context)
        self.assertEqual(await settle(self.factory, empty_request.envelope.operation_id), ())
        self.assertEqual(await reconcile_pending(self.factory, empty_request.reservation.id), ())

    async def test_ta_malformed_persisted_binding_fails_all_settlement_entrypoints(self):
        for index, corruption in enumerate(("missing", "unexpected")):
            context = await self._seed(f"settlement-binding-{index}", create_system=index == 0)
            request, _ = await self._admit(context, amount=Decimal("4"), slots=1)
            call = self._call(request, f"binding-call-{index}")
            permit = await authorize_provider_call(self.factory, call)
            await consume_call_permit(
                self.factory, request.binding, request.lease_owner, permit.permit_id, permit.accounting_call_id,
            )
            await record_call_observation(self.factory, CallObservation(
                accounting_call_id=call.accounting_call_id,
                binding=request.binding,
                state="QUIESCENT",
                source="provider_response",
                observed_at=datetime.now(UTC),
                lease_owner=request.lease_owner,
                observer_fence=context["lease"].fence,
            ))
            async with self.factory() as uow:
                await uow.budgets.record_usage(UsageObservation(
                    accounting_call_id=call.accounting_call_id,
                    source="provider_reported",
                    observation_identity=f"binding-usage-{index}",
                    usage=NormalizedUsage(
                        completeness="COMPLETE",
                        input_tokens=1,
                        output_tokens=1,
                        total_tokens=2,
                        reported_cost_usd=Decimal("3"),
                    ),
                    binding=request.binding,
                    observed_at=datetime.now(UTC),
                ))
                await uow.commit()
            async with self.engine.begin() as connection:
                if corruption == "missing":
                    await connection.execute(text(
                        "UPDATE operations SET binding = binding - 'policy_version' WHERE id=:id"
                    ), {"id": str(request.envelope.operation_id)})
                else:
                    await connection.execute(text(
                        "UPDATE operations SET binding = binding || CAST(:extra AS jsonb) WHERE id=:id"
                    ), {"id": str(request.envelope.operation_id), "extra": '{"unexpected":true}'})

            for action in (
                settle_call(self.factory, call.accounting_call_id),
                settle(self.factory, request.envelope.operation_id),
                reconcile_pending(self.factory, request.reservation.id),
            ):
                with self.assertRaises(Conflict):
                    await action
            self.assertEqual(await self._scalar(
                "SELECT settlement_state FROM usage_projections WHERE accounting_call_id=:id",
                id=str(call.accounting_call_id),
            ), "PENDING")
            self.assertEqual(await self._scalar("SELECT count(*) FROM budget_ledger WHERE effect_type='SETTLE'"), 0)
            self.assertEqual(await self._scalar(
                "SELECT spent_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"],
            ), Decimal("0"))
            self.assertEqual(await self._scalar(
                "SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"],
            ), Decimal("4"))

    async def test_t_terminal_admission_replay_is_side_effect_free(self):
        async def snapshot(context, request):
            async with self.engine.connect() as connection:
                row = (await connection.execute(text("""
                    SELECT
                      (SELECT state || ':' || dispatch_state || ':' || execution_state || ':' || receipt::text
                         FROM operations WHERE id=:operation_id),
                      (SELECT status || ':' || critic_agents || ':' || review_rounds || ':' || schema_repairs || ':' || transient_retries || ':' || tool_calls || ':' || provider_calls
                         FROM tasks WHERE id=:task_id),
                      (SELECT status FROM attempts WHERE id=:attempt_id),
                      (SELECT count(*) FROM attempts WHERE task_id=:task_id),
                      (SELECT intended_state || ':' || coalesce(active_attempt_id, '')
                         FROM agent_registry WHERE id=:registry_id),
                      (SELECT owner_worker || ':' || fence || ':' || expires_at::text
                         FROM agent_leases WHERE registry_id=:registry_id),
                      (SELECT state || ':' || coalesce(quiescent_at::text, '')
                         FROM agent_execution_holds WHERE operation_id=:operation_id),
                      (SELECT count(*) FROM agent_execution_holds WHERE operation_id=:operation_id),
                      (SELECT status FROM budget_reservations WHERE id=:reservation_id),
                      (SELECT count(*) FROM budget_reservations WHERE operation_id=:operation_id),
                      (SELECT coalesce(sum(held_amount), 0) FROM reservation_accounts WHERE reservation_id=:reservation_id),
                      (SELECT coalesce(sum(a.spent_amount), 0) FROM reservation_accounts ra JOIN budget_accounts a ON a.id=ra.account_id WHERE ra.reservation_id=:reservation_id),
                      (SELECT coalesce(sum(a.held_amount), 0) FROM reservation_accounts ra JOIN budget_accounts a ON a.id=ra.account_id WHERE ra.reservation_id=:reservation_id),
                      (SELECT count(*) FROM outbox WHERE operation_id=:operation_id),
                      (SELECT count(*) FROM budget_ledger WHERE reservation_id=:reservation_id),
                      (SELECT count(*) FROM provider_calls WHERE operation_id=:operation_id),
                      (SELECT count(*) FROM call_permits p JOIN provider_calls c USING (accounting_call_id) WHERE c.operation_id=:operation_id)
                """), {
                    "operation_id": str(request.envelope.operation_id),
                    "task_id": str(context["task_id"]),
                    "attempt_id": str(request.binding.attempt_id),
                    "registry_id": str(context["registry_id"]),
                    "reservation_id": str(request.reservation.id),
                })).one()
            return tuple(row)

        for index, outcome in enumerate(("SUCCEEDED", "FAILED")):
            context = await self._seed(f"terminal-replay-{index}", create_system=index == 0)
            request, initial = await self._admit(context)
            self.assertFalse(initial.replayed)
            await record_execution_observation(self.factory, ExecutionObservation(
                operation_id=request.envelope.operation_id,
                binding=request.binding,
                lease_owner=request.lease_owner,
                observer_fence=context["lease"].fence,
                state="QUIESCENT",
                source="bridge_terminal",
                observed_at=datetime.now(UTC),
                outcome=outcome,
            ))
            terminal_snapshot = await snapshot(context, request)
            operation_state = await self._scalar(
                "SELECT state FROM operations WHERE id=:id", id=str(request.envelope.operation_id),
            )
            self.assertEqual(operation_state, "COMPLETED" if outcome == "SUCCEEDED" else "FAILED")

            replay = await admit_operation(self.factory, request)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.state, "ADMITTED")
            self.assertEqual(replay.operation_id, initial.operation_id)
            self.assertEqual(terminal_snapshot, await snapshot(context, request))
            changed_scope = ScopeId(f"different-scope-{index}")
            changed_requests = (
                replace(request, payload={"prompt": "changed"}),
                replace(
                    request,
                    binding=replace(request.binding, fence=request.binding.fence + 1),
                    envelope=request.envelope.model_copy(update={"fence": request.envelope.fence + 1}),
                ),
                replace(
                    request,
                    binding=replace(request.binding, scope=changed_scope),
                    envelope=request.envelope.model_copy(update={"scope": changed_scope}),
                ),
                replace(
                    request,
                    envelope=request.envelope.model_copy(update={"max_tool_calls": request.envelope.max_tool_calls + 1}),
                ),
            )
            for changed_request in changed_requests:
                with self.assertRaises(Conflict):
                    await admit_operation(self.factory, changed_request)
            self.assertEqual(terminal_snapshot, await snapshot(context, request))

            if outcome == "SUCCEEDED":
                async with self.engine.begin() as connection:
                    await connection.execute(text(
                        "UPDATE operations SET receipt=NULL WHERE id=:id"
                    ), {"id": str(request.envelope.operation_id)})
                missing_receipt_snapshot = await snapshot(context, request)
                with self.assertRaises(Conflict):
                    await admit_operation(self.factory, request)
                self.assertEqual(missing_receipt_snapshot, await snapshot(context, request))

    async def test_t1_migration_uow_rollback_and_admission_atomicity(self):
        health = await check_database(self.engine)
        self.assertTrue(health.available)
        self.assertTrue((health.postgres_version or "").startswith("PostgreSQL 16"))
        self.assertEqual(health.migration_head, "0003_runtime_dispatch")

        async with self.factory() as uow:
            await uow.tasks.insert_scope(AuthorizationSnapshot(ScopeId("rolled-back"), PrincipalId("p"), "policy", 1))
        self.assertEqual(await self._scalar("SELECT count(*) FROM authorization_scopes WHERE id='rolled-back'"), 0)

        context = await self._seed("rollback-task")
        request = self._request(context)

        async def fail_outbox(_repository, _job):
            raise RuntimeError("injected before outbox append")

        with patch.object(PostgresDeliveryRepository, "append_outbox", fail_outbox):
            with self.assertRaises(RuntimeError):
                await admit_operation(self.factory, request)
        self.assertEqual(await self._scalar("SELECT count(*) FROM operations WHERE id=:id", id=str(request.envelope.operation_id)), 0)
        self.assertEqual(await self._scalar("SELECT count(*) FROM attempts WHERE id=:id", id=str(request.binding.attempt_id)), 0)
        self.assertEqual(await self._scalar("SELECT count(*) FROM budget_reservations WHERE id=:id", id=str(request.reservation.id)), 0)
        self.assertEqual(await self._scalar("SELECT count(*) FROM outbox"), 0)
        self.assertEqual(await self._scalar("SELECT count(*) FROM budget_ledger"), 0)
        task_state = await self._scalar(
            "SELECT status || ':' || critic_agents || ':' || review_rounds || ':' || schema_repairs || ':' || transient_retries || ':' || tool_calls || ':' || provider_calls FROM tasks WHERE id=:id",
            id=str(context["task_id"]),
        )
        self.assertEqual(task_state, "QUEUED:0:0:0:0:0:0")
        self.assertEqual(await self._scalar("SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]), Decimal(0))

    async def test_t2_operation_replay_and_request_conflict(self):
        context = await self._seed("replay-task")
        request = self._request(context)
        first = await admit_operation(self.factory, request)
        replay = await admit_operation(self.factory, request)
        self.assertFalse(first.replayed)
        self.assertTrue(replay.replayed)
        self.assertEqual(first.operation_id, replay.operation_id)
        with self.assertRaises(Conflict):
            await admit_operation(self.factory, replace(request, payload={"prompt": "different"}))
        self.assertEqual(await self._scalar("SELECT count(*) FROM attempts WHERE task_id=:id", id=str(context["task_id"])), 1)
        self.assertEqual(await self._scalar("SELECT count(*) FROM budget_reservations"), 1)
        self.assertEqual(await self._scalar("SELECT count(*) FROM outbox"), 1)
        self.assertEqual(await self._scalar("SELECT count(*) FROM budget_ledger"), 2)
        self.assertEqual(await self._scalar("SELECT provider_calls FROM tasks WHERE id=:id", id=str(context["task_id"])), 0)

    async def test_task_revision_is_immutable_and_stale_revision_is_rejected(self):
        context = await self._seed("revision-task")
        request, _ = await self._admit(context)
        call = self._call(request, "revision-slot")
        permit = await authorize_provider_call(self.factory, call)
        actor = ActorContext(
            principal_id=context["principal"],
            scope=context["scope"],
            authenticated_agent_registry_id=None,
            task_id=context["task_id"],
            attempt_id=None,
            input_revision=1,
            policy_version=context["policy"],
            authz_epoch=1,
            fence=context["lease"].fence,
        )
        updated = await revise(
            self.factory,
            actor,
            context["task_id"],
            1,
            InputChange(text="revised question", expected_revision=1, constraints={"region": "test"}),
        )
        self.assertEqual((updated.input_revision, updated.question), (2, "revised question"))
        self.assertEqual(await self._scalar(
            "SELECT count(*) FROM task_inputs WHERE task_id=:id AND revision IN (1,2)", id=str(context["task_id"]),
        ), 2)
        with self.assertRaises(StaleInput):
            await consume_call_permit(
                self.factory, request.binding, request.lease_owner, permit.permit_id, permit.accounting_call_id,
            )
        with self.assertRaises(StaleInput):
            await revise(
                self.factory,
                actor,
                context["task_id"],
                1,
                InputChange(text="stale question", expected_revision=1),
            )
        self.assertEqual(await self._scalar(
            "SELECT question FROM tasks WHERE id=:id", id=str(context["task_id"]),
        ), "revised question")

    async def test_t2_outbox_claim_fence_and_inbox_identity_conflict(self):
        context = await self._seed("delivery-task")
        request, _ = await self._admit(context)
        async with self.factory() as uow:
            first_jobs = await uow.delivery.claim_jobs("worker-a", 1, 60)
            await uow.commit()
        self.assertEqual(len(first_jobs), 1)
        first_job = first_jobs[0]
        async with self.engine.begin() as connection:
            await connection.execute(text("UPDATE outbox SET claim_expires_at=:expired WHERE id=:id"), {
                "expired": datetime.now(UTC) - timedelta(seconds=1),
                "id": first_job.id,
            })
        async with self.factory() as uow:
            second_jobs = await uow.delivery.claim_jobs("worker-b", 1, 60)
            await uow.commit()
        self.assertEqual(len(second_jobs), 1)
        second_job = second_jobs[0]
        self.assertGreater(second_job.claim_fence, first_job.claim_fence)
        with self.assertRaises(StaleInput):
            async with self.factory() as uow:
                await uow.delivery.ack_job(first_job, "worker-a", first_job.claim_fence)
                await uow.commit()
        async with self.factory() as uow:
            await uow.delivery.ack_job(second_job, "worker-b", second_job.claim_fence)
            await uow.commit()
        self.assertEqual(await self._scalar("SELECT status FROM outbox WHERE id=:id", id=first_job.id), "ACKED")

        payload_a = {
            "accounting_call_id": "accounting:inbox-test",
            "source": "runtime_reported",
            "observation_identity": "event-1",
            "usage": {"completeness": "PARTIAL", "input_tokens": 1},
        }
        payload_b = {
            **payload_a,
            "usage": {"completeness": "PARTIAL", "input_tokens": 2},
        }
        barrier = asyncio.Barrier(2)

        async def receive(payload):
            await barrier.wait()
            async with self.factory() as uow:
                receipt = await uow.delivery.insert_inbox_once(
                    "provider-scope", "stable-event-1", payload, canonical_json_hash(payload),
                )
                await uow.commit()
                return receipt

        first_receipt, second_receipt = await asyncio.gather(receive(payload_a), receive(payload_b))
        self.assertEqual(sum(receipt.conflict for receipt in (first_receipt, second_receipt)), 1)
        async with self.factory() as uow:
            replay = await uow.delivery.insert_inbox_once(
                "provider-scope", "stable-event-1", payload_a, canonical_json_hash(payload_a),
            )
            await uow.commit()
        self.assertTrue(replay.duplicate)
        self.assertTrue(replay.conflict)
        self.assertEqual(await self._scalar("SELECT count(*) FROM inbox"), 2)
        self.assertEqual(await self._scalar("SELECT count(*) FROM audit_events WHERE event_kind='inbox.payload_conflict'"), 1)

    async def test_t3_concurrent_system_reservation_has_no_partial_hold(self):
        context_a = await self._seed("concurrent-a", task_limit=Decimal("20"), system_limit=Decimal("10"))
        context_b = await self._seed("concurrent-b", task_limit=Decimal("20"), create_system=False)
        request_a = self._request(context_a, amount=Decimal("6"))
        request_b = self._request(context_b, amount=Decimal("6"))
        barrier = asyncio.Barrier(2)

        async def admit_after_barrier(request):
            await barrier.wait()
            try:
                return await admit_operation(self.factory, request)
            except Exception as error:
                return error

        result_a, result_b = await asyncio.gather(admit_after_barrier(request_a), admit_after_barrier(request_b))
        results = (result_a, result_b)
        self.assertEqual(sum(not isinstance(value, Exception) for value in results), 1)
        self.assertEqual(sum(isinstance(value, BudgetDenied) for value in results), 1)
        self.assertEqual(await self._scalar("SELECT held_amount FROM budget_accounts WHERE id=:id", id=context_a["system_account_id"]), Decimal("6"))
        held_a = await self._scalar("SELECT held_amount FROM budget_accounts WHERE id=:id", id=context_a["task_account_id"])
        held_b = await self._scalar("SELECT held_amount FROM budget_accounts WHERE id=:id", id=context_b["task_account_id"])
        self.assertEqual(sorted((held_a, held_b)), [Decimal("0"), Decimal("6")])
        self.assertEqual(await self._scalar("SELECT count(*) FROM attempts"), 1)

    async def test_t4_single_use_multiple_same_kind_slots_and_cancel(self):
        context = await self._seed("permit-task")
        request, _ = await self._admit(context)
        call_a = self._call(request, "slot-a")
        issued = await authorize_provider_call(self.factory, call_a)
        issued_again = await authorize_provider_call(self.factory, call_a)
        self.assertEqual(issued.permit_id, issued_again.permit_id)
        self.assertFalse(issued.consumed)

        async def consume_once():
            try:
                return await consume_call_permit(
                    self.factory, request.binding, request.lease_owner, issued.permit_id, issued.accounting_call_id,
                )
            except Exception as error:
                return error

        one, two = await asyncio.gather(consume_once(), consume_once())
        outcomes = (one, two)
        self.assertEqual(sum(getattr(value, "consumed", False) for value in outcomes), 1)
        self.assertEqual(sum(isinstance(value, UnknownExecution) for value in outcomes), 1)

        call_b = self._call(request, "slot-b")
        permit_b = await authorize_provider_call(self.factory, call_b)
        with self.assertRaises(BudgetDenied):
            await authorize_provider_call(self.factory, self._call(request, "slot-c"))

        actor = ActorContext(
            principal_id=context["principal"],
            scope=context["scope"],
            authenticated_agent_registry_id=None,
            task_id=context["task_id"],
            attempt_id=request.binding.attempt_id,
            input_revision=1,
            policy_version=context["policy"],
            authz_epoch=1,
            fence=context["lease"].fence,
        )
        result = await cancel(self.factory, actor, context["task_id"], StopReason.USER_CANCELLED)
        self.assertEqual(result["state"], "STOPPING")
        with self.assertRaises(PolicyDenied):
            await consume_call_permit(
                self.factory, request.binding, request.lease_owner, permit_b.permit_id, permit_b.accounting_call_id,
            )
        self.assertEqual(await self._scalar("SELECT count(*) FROM call_permits WHERE state='CONSUMED'"), 1)

    async def test_t4_expiry_and_new_fence_reject_old_permit(self):
        context = await self._seed("fence-task")
        request, _ = await self._admit(context)
        call_a = self._call(request, "fence-slot-a")
        call_b = self._call(request, "fence-slot-b")
        permit_a = await authorize_provider_call(self.factory, call_a)
        permit_b = await authorize_provider_call(self.factory, call_b)
        async with self.engine.begin() as connection:
            await connection.execute(text("UPDATE call_permits SET expires_at=:expired WHERE permit_id=:id"), {
                "expired": datetime.now(UTC) - timedelta(seconds=1),
                "id": str(permit_b.permit_id),
            })
        with self.assertRaises(BudgetDenied):
            await consume_call_permit(
                self.factory, request.binding, request.lease_owner, permit_b.permit_id, permit_b.accounting_call_id,
            )

        async with self.factory() as uow:
            await uow.agents.release_lease(context["lease"])
            new_lease = await uow.agents.acquire_lease(context["registry_id"], "new-worker", 1800)
            await uow.commit()
        self.assertGreater(new_lease.fence, context["lease"].fence)
        with self.assertRaises(StaleInput):
            await consume_call_permit(
                self.factory, request.binding, request.lease_owner, permit_a.permit_id, permit_a.accounting_call_id,
            )

    async def test_t4_final_response_reservation_is_separate(self):
        context = await self._seed("final-task")
        request, _ = await self._admit(context)
        final_reservation = ReservationRequest(
            id=ReservationId("reservation:final-response"),
            operation_id=request.envelope.operation_id,
            purpose="final_response",
            amount=Decimal("4"),
            task_id=context["task_id"],
            task_account_id=context["task_account_id"],
            system_account_id=context["system_account_id"],
            pricing_version="test-price-v1",
            system_period_id="2026-09",
        )
        await reserve(self.factory, final_reservation)
        wrong_kind = replace(self._call(request, "final-slot"), reservation_id=final_reservation.id)
        with self.assertRaises(PolicyDenied):
            await authorize_provider_call(self.factory, wrong_kind)
        final_call = replace(wrong_kind, call_kind="final_response")
        permit = await authorize_provider_call(self.factory, final_call)
        self.assertEqual(permit.accounting_call_id, final_call.accounting_call_id)
        self.assertEqual(await self._scalar("SELECT count(*) FROM budget_reservations WHERE operation_id=:id", id=str(request.envelope.operation_id)), 2)
        self.assertEqual(await self._scalar("SELECT reservation_id FROM provider_calls WHERE accounting_call_id=:id", id=str(permit.accounting_call_id)), str(final_reservation.id))

    async def test_tb_new_call_after_first_call_settlement_uses_remaining_hold(self):
        context = await self._seed("sequential-call-task")
        request, _ = await self._admit(context, amount=Decimal("8"), slots=2)
        call_a = self._call(request, "sequential-a", allocation=Decimal("4"))
        permit_a = await authorize_provider_call(self.factory, call_a)
        await consume_call_permit(
            self.factory, request.binding, request.lease_owner, permit_a.permit_id, permit_a.accounting_call_id,
        )
        await record_call_observation(self.factory, CallObservation(
            accounting_call_id=call_a.accounting_call_id,
            binding=request.binding,
            state="QUIESCENT",
            source="provider_response",
            observed_at=datetime.now(UTC),
            lease_owner=request.lease_owner,
            observer_fence=context["lease"].fence,
        ))
        receipt_a = await record_usage(self.factory, UsageRecord(
            accounting_call_id=call_a.accounting_call_id,
            binding=request.binding,
            observation_identity="sequential-usage-a",
            source="provider_reported",
            observed_at=datetime.now(UTC),
            input_tokens=1,
            output_tokens=1,
            total_tokens=2,
            completeness="COMPLETE",
            pricing_version="test-price-v1",
            monetary_amount=Decimal("3"),
        ))
        self.assertEqual(receipt_a.settlement_state, "SETTLED")
        self.assertEqual(await self._scalar(
            "SELECT spent_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"],
        ), Decimal("3"))
        self.assertEqual(await self._scalar(
            "SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"],
        ), Decimal("4"))
        self.assertEqual(await self._scalar(
            "SELECT spent_amount FROM budget_accounts WHERE id=:id", id=context["system_account_id"],
        ), Decimal("3"))
        self.assertEqual(await self._scalar(
            "SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["system_account_id"],
        ), Decimal("4"))
        self.assertEqual(await self._scalar(
            "SELECT status FROM budget_reservations WHERE id=:id", id=str(request.reservation.id),
        ), "PENDING_SETTLEMENT")

        call_b = self._call(request, "sequential-b", allocation=Decimal("4"))
        permit_b = await authorize_provider_call(self.factory, call_b)
        self.assertNotEqual(call_a.accounting_call_id, call_b.accounting_call_id)
        self.assertNotEqual(permit_a.permit_id, permit_b.permit_id)
        self.assertEqual(await self._scalar(
            "SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"],
        ), Decimal("4"))
        await consume_call_permit(
            self.factory, request.binding, request.lease_owner, permit_b.permit_id, permit_b.accounting_call_id,
        )
        await record_call_observation(self.factory, CallObservation(
            accounting_call_id=call_b.accounting_call_id,
            binding=request.binding,
            state="QUIESCENT",
            source="provider_response",
            observed_at=datetime.now(UTC),
            lease_owner=request.lease_owner,
            observer_fence=context["lease"].fence,
        ))
        receipt_b = await record_usage(self.factory, UsageRecord(
            accounting_call_id=call_b.accounting_call_id,
            binding=request.binding,
            observation_identity="sequential-usage-b",
            source="provider_reported",
            observed_at=datetime.now(UTC),
            input_tokens=1,
            output_tokens=1,
            total_tokens=2,
            completeness="COMPLETE",
            pricing_version="test-price-v1",
            monetary_amount=Decimal("2"),
        ))
        self.assertEqual(receipt_b.settlement_state, "SETTLED")
        await record_execution_observation(self.factory, ExecutionObservation(
            operation_id=request.envelope.operation_id,
            binding=request.binding,
            lease_owner=request.lease_owner,
            observer_fence=context["lease"].fence,
            state="QUIESCENT",
            source="bridge_terminal",
            observed_at=datetime.now(UTC),
            outcome="SUCCEEDED",
        ))
        for account_id in (context["task_account_id"], context["system_account_id"]):
            self.assertEqual(await self._scalar(
                "SELECT spent_amount FROM budget_accounts WHERE id=:id", id=account_id,
            ), Decimal("5"))
            self.assertEqual(await self._scalar(
                "SELECT held_amount FROM budget_accounts WHERE id=:id", id=account_id,
            ), Decimal("0"))
        self.assertEqual(await self._scalar("SELECT count(*) FROM budget_ledger WHERE effect_type='SETTLE'"), 4)
        self.assertEqual(await self._scalar(
            "SELECT count(*) FROM budget_ledger WHERE accounting_call_id=:id", id=str(call_a.accounting_call_id),
        ), 2)
        self.assertEqual(await self._scalar(
            "SELECT count(*) FROM budget_ledger WHERE accounting_call_id=:id", id=str(call_b.accounting_call_id),
        ), 2)

    async def test_tb_billing_pending_allows_next_call_but_execution_unknown_blocks(self):
        context = await self._seed("billing-pending-task")
        request, _ = await self._admit(context, amount=Decimal("12"), slots=3)
        call_a = self._call(request, "billing-pending-a", allocation=Decimal("4"))
        permit_a = await authorize_provider_call(self.factory, call_a)
        await consume_call_permit(
            self.factory, request.binding, request.lease_owner, permit_a.permit_id, permit_a.accounting_call_id,
        )
        await record_call_observation(self.factory, CallObservation(
            accounting_call_id=call_a.accounting_call_id,
            binding=request.binding,
            state="QUIESCENT",
            source="provider_response",
            observed_at=datetime.now(UTC),
            lease_owner=request.lease_owner,
            observer_fence=context["lease"].fence,
        ))
        pending = await record_usage(self.factory, UsageRecord(
            accounting_call_id=call_a.accounting_call_id,
            binding=request.binding,
            observation_identity="billing-pending-a-partial",
            source="runtime_reported",
            observed_at=datetime.now(UTC),
            input_tokens=1,
            completeness="PARTIAL",
            pricing_version="test-price-v1",
        ))
        self.assertEqual(pending.settlement_state, "PENDING")
        self.assertEqual(await self._scalar(
            "SELECT status FROM budget_reservations WHERE id=:id", id=str(request.reservation.id),
        ), "PENDING_SETTLEMENT")
        self.assertEqual(await self._scalar(
            "SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"],
        ), Decimal("12"))

        call_b = self._call(request, "billing-pending-b", allocation=Decimal("4"))
        permit_b = await authorize_provider_call(self.factory, call_b)
        await consume_call_permit(
            self.factory, request.binding, request.lease_owner, permit_b.permit_id, permit_b.accounting_call_id,
        )
        self.assertEqual(await self._scalar(
            "SELECT sum(allocation_amount) FROM provider_calls WHERE reservation_id=:id",
            id=str(request.reservation.id),
        ), Decimal("8"))
        self.assertEqual(await self._scalar(
            "SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"],
        ), Decimal("12"))

        pending_call = self._call(request, "billing-pending-issued-before-unknown", allocation=Decimal("4"))
        pending_permit = await authorize_provider_call(self.factory, pending_call)
        self.assertEqual(await self._scalar(
            "SELECT sum(allocation_amount) FROM provider_calls WHERE reservation_id=:id",
            id=str(request.reservation.id),
        ), Decimal("12"))
        await record_execution_observation(self.factory, ExecutionObservation(
            operation_id=request.envelope.operation_id,
            binding=request.binding,
            lease_owner=request.lease_owner,
            observer_fence=context["lease"].fence,
            state="UNKNOWN",
            source="bridge_disconnect",
            observed_at=datetime.now(UTC),
            reason="transport_disconnect",
        ))
        before = (
            await self._scalar("SELECT count(*) FROM provider_calls WHERE operation_id=:id", id=str(request.envelope.operation_id)),
            await self._scalar("SELECT count(*) FROM call_permits"),
            await self._scalar("SELECT count(*) FROM budget_ledger"),
            await self._scalar("SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]),
            await self._scalar("SELECT sum(allocation_amount) FROM provider_calls WHERE operation_id=:id", id=str(request.envelope.operation_id)),
        )
        with self.assertRaises(UnknownExecution):
            await consume_call_permit(
                self.factory, request.binding, request.lease_owner,
                pending_permit.permit_id, pending_permit.accounting_call_id,
            )
        with self.assertRaises(UnknownExecution):
            await authorize_provider_call(self.factory, self._call(request, "billing-pending-c", allocation=Decimal("4")))
        after = (
            await self._scalar("SELECT count(*) FROM provider_calls WHERE operation_id=:id", id=str(request.envelope.operation_id)),
            await self._scalar("SELECT count(*) FROM call_permits"),
            await self._scalar("SELECT count(*) FROM budget_ledger"),
            await self._scalar("SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]),
            await self._scalar("SELECT sum(allocation_amount) FROM provider_calls WHERE operation_id=:id", id=str(request.envelope.operation_id)),
        )
        self.assertEqual(after, before)

    async def test_tb_usage_conflict_keeps_hold_and_blocks_new_allocation(self):
        context = await self._seed("allocation-conflict-task")
        request, _ = await self._admit(context, amount=Decimal("8"), slots=2)
        call_a = self._call(request, "allocation-conflict-a", allocation=Decimal("4"))
        permit_a = await authorize_provider_call(self.factory, call_a)
        await consume_call_permit(
            self.factory, request.binding, request.lease_owner, permit_a.permit_id, permit_a.accounting_call_id,
        )
        await record_call_observation(self.factory, CallObservation(
            accounting_call_id=call_a.accounting_call_id,
            binding=request.binding,
            state="QUIESCENT",
            source="provider_response",
            observed_at=datetime.now(UTC),
            lease_owner=request.lease_owner,
            observer_fence=context["lease"].fence,
        ))
        first = UsageRecord(
            accounting_call_id=call_a.accounting_call_id,
            binding=request.binding,
            observation_identity="allocation-conflict-same-event",
            source="runtime_reported",
            observed_at=datetime.now(UTC),
            input_tokens=1,
            completeness="PARTIAL",
        )
        await record_usage(self.factory, first)
        conflict = await record_usage(self.factory, first.model_copy(update={
            "input_tokens": 2,
            "observed_at": datetime.now(UTC) + timedelta(microseconds=1),
        }))
        self.assertTrue(conflict.conflict)
        with self.assertRaises(BudgetDenied):
            await authorize_provider_call(self.factory, self._call(request, "allocation-conflict-b", allocation=Decimal("4")))
        self.assertEqual(await self._scalar(
            "SELECT status FROM budget_reservations WHERE id=:id", id=str(request.reservation.id),
        ), "PENDING_SETTLEMENT")
        self.assertTrue(await self._scalar(
            "SELECT has_conflict FROM usage_projections WHERE accounting_call_id=:id", id=str(call_a.accounting_call_id),
        ))
        self.assertEqual(await self._scalar("SELECT count(*) FROM provider_calls"), 1)
        self.assertEqual(await self._scalar("SELECT count(*) FROM usage_observations"), 2)
        self.assertEqual(await self._scalar("SELECT count(*) FROM budget_ledger WHERE effect_type='SETTLE'"), 0)
        for account_id in (context["task_account_id"], context["system_account_id"]):
            self.assertEqual(await self._scalar(
                "SELECT spent_amount FROM budget_accounts WHERE id=:id", id=account_id,
            ), Decimal("0"))
            self.assertEqual(await self._scalar(
                "SELECT held_amount FROM budget_accounts WHERE id=:id", id=account_id,
            ), Decimal("8"))

    async def test_tb_overrun_blocks_new_call_allocation(self):
        context = await self._seed("allocation-overrun-task")
        request, _ = await self._admit(context, amount=Decimal("12"), slots=3)
        call_a = self._call(request, "allocation-overrun-a", allocation=Decimal("4"))
        permit_a = await authorize_provider_call(self.factory, call_a)
        await consume_call_permit(
            self.factory, request.binding, request.lease_owner, permit_a.permit_id, permit_a.accounting_call_id,
        )
        await record_call_observation(self.factory, CallObservation(
            accounting_call_id=call_a.accounting_call_id,
            binding=request.binding,
            state="QUIESCENT",
            source="provider_response",
            observed_at=datetime.now(UTC),
            lease_owner=request.lease_owner,
            observer_fence=context["lease"].fence,
        ))
        settled = await record_usage(self.factory, UsageRecord(
            accounting_call_id=call_a.accounting_call_id,
            binding=request.binding,
            observation_identity="allocation-overrun-a-usage",
            source="provider_reported",
            observed_at=datetime.now(UTC),
            input_tokens=1,
            output_tokens=1,
            total_tokens=2,
            completeness="COMPLETE",
            pricing_version="test-price-v1",
            monetary_amount=Decimal("5"),
        ))
        self.assertEqual(settled.settlement_state, "SETTLED")
        self.assertTrue(await self._scalar(
            "SELECT overrun FROM usage_projections WHERE accounting_call_id=:id",
            id=str(call_a.accounting_call_id),
        ))
        self.assertEqual(await self._scalar(
            "SELECT status FROM budget_reservations WHERE id=:id", id=str(request.reservation.id),
        ), "PENDING_SETTLEMENT")
        before = (
            await self._scalar("SELECT count(*) FROM provider_calls WHERE operation_id=:id", id=str(request.envelope.operation_id)),
            await self._scalar("SELECT count(*) FROM call_permits"),
            await self._scalar("SELECT count(*) FROM budget_ledger"),
            await self._scalar("SELECT spent_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]),
            await self._scalar("SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]),
        )
        with self.assertRaises(BudgetDenied):
            await authorize_provider_call(self.factory, self._call(request, "allocation-overrun-b", allocation=Decimal("4")))
        after = (
            await self._scalar("SELECT count(*) FROM provider_calls WHERE operation_id=:id", id=str(request.envelope.operation_id)),
            await self._scalar("SELECT count(*) FROM call_permits"),
            await self._scalar("SELECT count(*) FROM budget_ledger"),
            await self._scalar("SELECT spent_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]),
            await self._scalar("SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]),
        )
        self.assertEqual(after, before)

    async def test_t5_unknown_hold_survives_reconnect_and_new_fence(self):
        context = await self._seed("unknown-task")
        request, _ = await self._admit(context)
        await record_execution_observation(self.factory, ExecutionObservation(
            operation_id=request.envelope.operation_id,
            binding=request.binding,
            lease_owner=request.lease_owner,
            observer_fence=context["lease"].fence,
            state="UNKNOWN",
            source="bridge_disconnect",
            observed_at=datetime.now(UTC),
            reason="transport_disconnect",
        ))
        await self.engine.dispose()
        self.engine = create_engine(self.database_url)
        self.factory = create_uow_factory(self.engine)
        health = await check_database(self.engine)
        self.assertTrue(health.available)
        async with self.factory() as uow:
            hold = await uow.agents.active_execution_hold(context["registry_id"])
            self.assertIsNotNone(hold)
            self.assertEqual(hold["state"], "UNKNOWN")
            await uow.agents.release_lease(context["lease"])
            new_lease = await uow.agents.acquire_lease(context["registry_id"], "reconnected-worker", 1800)
            await uow.commit()
        new_binding = replace(
            request.binding,
            attempt_id=AttemptId("attempt:unknown-task:retry"),
            fence=new_lease.fence,
            conversation_id="new-conversation",
        )
        blocked = self._request(
            context,
            operation_id=OperationId("operation:unknown-task:retry"),
            attempt_id=new_binding.attempt_id,
            reservation_id=ReservationId("reservation:unknown-task:retry"),
            binding=new_binding,
            lease=new_lease,
        )
        with self.assertRaises(UnknownExecution):
            await admit_operation(self.factory, blocked)
        with self.assertRaises(UnknownExecution):
            await admit_operation(self.factory, request)
        self.assertEqual(await self._scalar("SELECT count(*) FROM attempts"), 1)
        self.assertEqual(await self._scalar("SELECT count(*) FROM budget_reservations"), 1)
        self.assertEqual(await self._scalar("SELECT count(*) FROM recovery_cases WHERE operation_id=:id", id=str(request.envelope.operation_id)), 1)

    async def test_t6_usage_completion_overrun_pending_conflict_and_adjustment(self):
        context = await self._seed("usage-task")
        request, _ = await self._admit(context)
        call_a = self._call(request, "usage-a")
        call_b = self._call(request, "usage-b")
        permit_a = await authorize_provider_call(self.factory, call_a)
        permit_b = await authorize_provider_call(self.factory, call_b)
        await consume_call_permit(self.factory, request.binding, request.lease_owner, permit_a.permit_id, permit_a.accounting_call_id)
        await consume_call_permit(self.factory, request.binding, request.lease_owner, permit_b.permit_id, permit_b.accounting_call_id)

        for call, provider_id in ((call_a, "provider-call-a"), (call_b, "provider-call-b")):
            await record_call_observation(self.factory, CallObservation(
                accounting_call_id=call.accounting_call_id,
                binding=request.binding,
                state="QUIESCENT",
                source="provider_response",
                observed_at=datetime.now(UTC),
                lease_owner=request.lease_owner,
                observer_fence=context["lease"].fence,
                provider_call_id=ProviderCallId(provider_id),
            ))
        await record_execution_observation(self.factory, ExecutionObservation(
            operation_id=request.envelope.operation_id,
            binding=request.binding,
            lease_owner=request.lease_owner,
            observer_fence=context["lease"].fence,
            state="QUIESCENT",
            source="bridge_terminal",
            observed_at=datetime.now(UTC),
            outcome="SUCCEEDED",
        ))

        partial = UsageRecord(
            accounting_call_id=call_a.accounting_call_id,
            binding=request.binding,
            observation_identity="usage-a-partial",
            source="runtime_reported",
            observed_at=datetime.now(UTC),
            provider_call_id=ProviderCallId("provider-call-a"),
            input_tokens=1,
            completeness="PARTIAL",
            pricing_version="test-price-v1",
        )
        self.assertTrue((await record_usage(self.factory, partial)).accepted)
        duplicate = await record_usage(self.factory, partial)
        self.assertTrue(duplicate.duplicate)
        self.assertEqual(await self._scalar("SELECT count(*) FROM usage_observations WHERE accounting_call_id=:id", id=str(call_a.accounting_call_id)), 1)

        usage_a = UsageRecord(
            accounting_call_id=call_a.accounting_call_id,
            binding=request.binding,
            observation_identity="usage-a-complete",
            source="provider_reported",
            observed_at=datetime.now(UTC),
            provider_call_id=ProviderCallId("provider-call-a"),
            input_tokens=1,
            output_tokens=1,
            total_tokens=2,
            completeness="COMPLETE",
            pricing_version="test-price-v1",
            monetary_amount=Decimal("5"),
        )
        await record_usage(self.factory, usage_a)
        task_spent = await self._scalar("SELECT spent_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"])
        task_held = await self._scalar("SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"])
        self.assertEqual((task_spent, task_held), (Decimal("5"), Decimal("4")))
        self.assertTrue(await self._scalar("SELECT overrun FROM usage_projections WHERE accounting_call_id=:id", id=str(call_a.accounting_call_id)))

        usage_b = UsageRecord(
            accounting_call_id=call_b.accounting_call_id,
            binding=request.binding,
            observation_identity="usage-b-complete",
            source="provider_reported",
            observed_at=datetime.now(UTC),
            provider_call_id=ProviderCallId("provider-call-b"),
            input_tokens=1,
            output_tokens=1,
            total_tokens=2,
            completeness="COMPLETE",
            pricing_version="test-price-v1",
            monetary_amount=Decimal("2"),
        )
        await record_usage(self.factory, usage_b)
        repeated_b = await record_usage(self.factory, usage_b)
        self.assertTrue(repeated_b.duplicate)
        self.assertEqual(await self._scalar("SELECT spent_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]), Decimal("7"))
        self.assertEqual(await self._scalar("SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]), Decimal("0"))
        self.assertEqual(await self._scalar("SELECT status FROM budget_reservations WHERE id=:id", id=str(request.reservation.id)), "SETTLED")
        self.assertEqual(await self._scalar("SELECT count(*) FROM budget_ledger WHERE effect_type='SETTLE'"), 4)

        contradictory = usage_a.model_copy(update={"input_tokens": 2, "total_tokens": 3, "monetary_amount": Decimal("6")})
        conflict = await record_usage(self.factory, contradictory)
        self.assertTrue(conflict.conflict)
        self.assertEqual(await self._scalar("SELECT input_tokens FROM usage_projections WHERE accounting_call_id=:id", id=str(call_a.accounting_call_id)), 1)
        self.assertEqual(await self._scalar("SELECT reported_cost_usd FROM usage_projections WHERE accounting_call_id=:id", id=str(call_a.accounting_call_id)), Decimal("5"))
        self.assertEqual(await self._scalar("SELECT count(*) FROM usage_observations WHERE accounting_call_id=:id", id=str(call_a.accounting_call_id)), 3)
        self.assertEqual(await self._scalar("SELECT count(*) FROM budget_ledger WHERE effect_type='SETTLE'"), 4)
        self.assertEqual(await self._scalar("SELECT status FROM budget_reservations WHERE id=:id", id=str(request.reservation.id)), "PENDING_SETTLEMENT")

        self.assertTrue(await apply_adjustment(
            self.factory, context["task_account_id"], "invoice-adjustment-1", Decimal("0.25"),
            reservation_id=request.reservation.id, accounting_call_id=call_a.accounting_call_id,
        ))
        self.assertFalse(await apply_adjustment(
            self.factory, context["task_account_id"], "invoice-adjustment-1", Decimal("0.25"),
            reservation_id=request.reservation.id, accounting_call_id=call_a.accounting_call_id,
        ))
        self.assertEqual(await self._scalar("SELECT spent_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]), Decimal("7.25"))

    async def test_t6_conflicting_usage_keeps_pending_hold(self):
        context = await self._seed("conflict-task")
        request, _ = await self._admit(context, amount=Decimal("4"), slots=1)
        call = self._call(request, "conflict-call")
        permit = await authorize_provider_call(self.factory, call)
        await consume_call_permit(self.factory, request.binding, request.lease_owner, permit.permit_id, permit.accounting_call_id)
        await record_call_observation(self.factory, CallObservation(
            accounting_call_id=call.accounting_call_id,
            binding=request.binding,
            state="QUIESCENT",
            source="provider_response",
            observed_at=datetime.now(UTC),
            lease_owner=request.lease_owner,
            observer_fence=context["lease"].fence,
        ))
        await record_execution_observation(self.factory, ExecutionObservation(
            operation_id=request.envelope.operation_id,
            binding=request.binding,
            lease_owner=request.lease_owner,
            observer_fence=context["lease"].fence,
            state="QUIESCENT",
            source="bridge_terminal",
            observed_at=datetime.now(UTC),
            outcome="SUCCEEDED",
        ))
        first = UsageRecord(
            accounting_call_id=call.accounting_call_id,
            binding=request.binding,
            observation_identity="same-runtime-event",
            source="runtime_reported",
            observed_at=datetime.now(UTC),
            input_tokens=1,
            completeness="PARTIAL",
        )
        contradictory = first.model_copy(update={"input_tokens": 2, "observed_at": datetime.now(UTC) + timedelta(microseconds=1)})
        await record_usage(self.factory, first)
        receipt = await record_usage(self.factory, contradictory)
        self.assertTrue(receipt.conflict)
        self.assertEqual(await self._scalar("SELECT count(*) FROM usage_observations WHERE accounting_call_id=:id", id=str(call.accounting_call_id)), 2)
        self.assertEqual(await self._scalar("SELECT spent_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]), Decimal("0"))
        self.assertEqual(await self._scalar("SELECT held_amount FROM budget_accounts WHERE id=:id", id=context["task_account_id"]), Decimal("4"))
        self.assertEqual(await self._scalar("SELECT settlement_state FROM usage_projections WHERE accounting_call_id=:id", id=str(call.accounting_call_id)), "CONFLICT")

    async def test_t7_storage_failure_never_returns_permit(self):
        context = await self._seed("storage-task")
        request, _ = await self._admit(context)
        call = self._call(request, "storage-call")
        broken_url = make_url(self.database_url).set(port=1).render_as_string(hide_password=False)
        broken_engine = create_engine(broken_url)
        try:
            with self.assertRaises(StorageUnavailable):
                await authorize_provider_call(create_uow_factory(broken_engine), call)
        finally:
            await broken_engine.dispose()
        self.assertEqual(await self._scalar("SELECT count(*) FROM provider_calls"), 0)


if __name__ == "__main__":
    unittest.main()
