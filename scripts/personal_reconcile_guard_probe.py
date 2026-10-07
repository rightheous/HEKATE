#!/usr/bin/env python3
"""Focused reconcile --apply boundary check on a fresh PostgreSQL and pinned Letta.

Stored bridge results are synthetic fixtures. The pinned App Server is used only
for agent identity/create calls; the worker is never started and no inference is
sent to any provider.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import create_engine as create_sync_engine, text

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import phase6e_local_cli_probe as p6e  # noqa: E402
import phase6f_reviewed_qwen_probe as p6f  # noqa: E402
from hekate.application.budgets import (  # noqa: E402
    authorize_provider_call, consume_call_permit,
)
from hekate.application.lifecycle import (  # noqa: E402
    _reservation_amount, create_from_intent, ensure_hekate,
)
from hekate.application.operations import (  # noqa: E402
    admit_operation, record_dispatch_accepted, record_dispatch_send_intent,
    record_execution_observation,
)
from hekate.application.results import process_pending_results  # noqa: E402
from hekate.application.runtime_inbox import InboxBinding, RuntimeInboxPayload  # noqa: E402
from hekate.application.tasks import cancel, submit  # noqa: E402
from hekate.bootstrap import build_container, close_container  # noqa: E402
from hekate.domain.contracts import canonical_json_hash  # noqa: E402
from hekate.domain.models import (  # noqa: E402
    AdmissionRequest, AgentRecord, BillableCallIntent, ExecutionEnvelope,
    ExecutionObservation, GuardBinding, PriceTable, ReservationRequest,
    RuntimeLimits, UserMessage,
)
from hekate.domain.types import (  # noqa: E402
    AccountingCallId, AttemptId, OperationId, PermitId, ProviderAgentId, TaskId,
    ProviderCallId, RegistryId, ReservationId, StopReason,
)
from hekate.infrastructure.postgres.database import (  # noqa: E402
    create_engine, create_uow_factory,
)
from hekate.settings import (  # noqa: E402
    configured_critic_execution, configured_deliberation,
    configured_local_actor, configured_task_execution, load_settings, restore_execution_config,
)
from hekate.worker.service import process_inbox_row  # noqa: E402

ARTIFACTS = ROOT / "integration/runtime/artifacts"


def now_utc() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def code_fingerprint() -> tuple[str, list[dict[str, str]]]:
    names = (
        "src/hekate/__main__.py", "src/hekate/application/lifecycle.py",
        "src/hekate/application/results.py", "src/hekate/worker/service.py",
        "docs/personal-use.md", "scripts/personal_reconcile_guard_probe.py",
    )
    digest = hashlib.sha256()
    files = []
    for name in names:
        body = (ROOT / name).read_bytes()
        checksum = hashlib.sha256(body).hexdigest()
        digest.update(name.encode() + b"\0" + bytes.fromhex(checksum))
        files.append({"path": name, "sha256": checksum})
    return digest.hexdigest(), files


def _sql(engine, query: str, params: dict[str, object] | None = None) -> list[dict[str, object]]:
    with engine.connect() as connection:
        rows = connection.execute(text(query), params or {}).mappings().all()
    return [dict(row) for row in rows]


def _snapshot(engine, scope: str, task_id: str) -> dict[str, object]:
    task = _sql(engine, """
        SELECT status, input_revision, critic_agents, review_rounds, hekate_continuations,
               outcome, stop_reason
        FROM tasks WHERE id=:task AND owner_scope=:scope
    """, {"task": task_id, "scope": scope})[0]
    return {
        "task": task,
        "critic_registry_count": _sql(engine, "SELECT count(*) AS n FROM agent_registry WHERE owner_scope=:scope AND task_id=:task", {"scope": scope, "task": task_id})[0]["n"],
        "operation_count": _sql(engine, """
            SELECT count(*) AS n FROM operations WHERE owner_scope=:scope AND (
                task_id=:task OR observation->>'deferred_task_id'=:task
            )
        """, {"scope": scope, "task": task_id})[0]["n"],
        "reservation_count": _sql(engine, """
            SELECT count(*) AS n FROM budget_reservations r WHERE r.operation_id IN (
                SELECT id FROM operations WHERE owner_scope=:scope AND (
                    task_id=:task OR observation->>'deferred_task_id'=:task
                )
            )
        """, {"scope": scope, "task": task_id})[0]["n"],
        "outbox_count": _sql(engine, """
            SELECT count(*) AS n FROM outbox b JOIN operations o ON o.id=b.operation_id
            WHERE o.owner_scope=:scope AND (o.task_id=:task OR o.observation->>'deferred_task_id'=:task)
        """, {"scope": scope, "task": task_id})[0]["n"],
        "workflow": _sql(engine, "SELECT stage FROM critic_workflows WHERE owner_scope=:scope AND task_id=:task", {"scope": scope, "task": task_id}),
        "deliberation_steps": _sql(engine, "SELECT step_slot,state FROM deliberation_steps WHERE owner_scope=:scope AND task_id=:task ORDER BY step_slot", {"scope": scope, "task": task_id}),
        "result": _sql(engine, "SELECT inbox_id,processing_state,rejection_reason,conclusion_id FROM turn_results WHERE task_id=:task", {"task": task_id}),
        "response_count": _sql(engine, "SELECT count(*) AS n FROM task_responses WHERE task_id=:task", {"task": task_id})[0]["n"],
        "dissent_count": _sql(engine, "SELECT count(*) AS n FROM dissent WHERE scope=:scope AND conclusion_id IN (SELECT id FROM conclusions WHERE task_id=:task)", {"scope": scope, "task": task_id})[0]["n"],
    }


def _conclusion(task_id: str, attempt_id: str, registry_id: str, *, objection: bool = False) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": "1", "task_id": task_id, "attempt_id": attempt_id,
        "agent_id": registry_id, "status": "done", "input_revision": 1,
        "assessment": {
            "statement": "Synthetic stored result for reconciliation boundary verification.",
            "confidence": {"level": "medium", "basis": ["fixture contract"], "missing_evidence": []},
        },
        "evidence_used": [], "objections": [], "assumptions": [], "unresolved": [],
        "recommended_next_step": {"type": "none"},
        "position_recommendation": {"action": "maintain", "summary": "No Position change in this fixture."},
    }
    if objection:
        value["objections"] = [{
            "id": "O1", "severity": "medium", "claim": "A synthetic assumption needs validation.",
            "condition": "if it is used outside this fixture", "suggested_validation": "check the source data",
        }]
    return value


def _output(task_id: str, attempt_id: str, registry_id: str, action: str) -> dict[str, object]:
    conclusion = _conclusion(task_id, attempt_id, registry_id)
    if action == "spawn":
        proposal = {
            "schema_version": "1", "action": "spawn", "role": "critic",
            "purpose": "Check one uncertainty in the synthetic decision.",
            "target_uncertainty": "The fixture does not establish the assumption.",
            "expected_decision_impact": "The answer may need a qualification.", "task_id": task_id,
        }
    elif action == "continue":
        proposal = {
            "schema_version": "1", "action": "continue", "next_action": "hekate_reasoning",
            "unresolved_issue": "One bounded assumption remains.",
            "expected_information_gain": "Compare the known input values.",
            "decision_impact": "The final wording may change.",
        }
    else:
        proposal = {"schema_version": "1", "action": "answer", "answer": "Stored final synthetic answer."}
    return {"schema_version": "1", "proposal": proposal, "conclusion": conclusion}


async def _submit_task(factory, actor, task_config, request_key: str, question: str):
    receipt = await submit(factory, actor, UserMessage(text=question), request_key, task_config)
    return str(receipt["task_id"])


async def _admit_execution(
    factory, actor, agent, worker: str, task_id: str, task_config, *,
    attempt_kind: str = "planning", operation_kind: str = "turn",
    workflow_stage: str | None = None, workflow_stage_hash: str | None = None,
    parent_attempt_id: AttemptId | None = None, reservation_id: ReservationId | None = None,
    reservation_amount: Decimal = Decimal("0.01"), pricing_version: str | None = None,
    billable_call_slots: int = 0,
):
    registry_id = RegistryId(str(agent.registry_id))
    async with factory() as uow:
        task = await uow.tasks.lock_task(task_id)
        lease = await uow.agents.acquire_lease(registry_id, worker, 1800)
        if lease is None:
            raise AssertionError("fixture could not acquire the bound registry lease")
        await uow.commit()
    attempt_id = AttemptId(str(uuid4())) if attempt_kind != "critic_review" else AttemptId(str(parent_attempt_id or ""))
    # A Critic workflow supplies its stable review attempt and operation IDs.
    if attempt_kind == "critic_review":
        raise AssertionError("Critic review admission must use _admit_critic_review")
    operation_id = OperationId(f"reconcile-probe:{task_id}:{uuid4()}")
    reservation_id = reservation_id or ReservationId(f"reservation:{operation_id}")
    pricing_version = pricing_version or task_config.pricing_version
    deadline = min(task.deadline, datetime.now(UTC) + timedelta(minutes=10))
    binding = GuardBinding(
        task_id=task.id, attempt_id=attempt_id, agent_registry_id=registry_id,
        provider_agent_id=agent.provider_id, principal_id=actor.principal_id,
        scope=actor.scope, input_revision=task.input_revision,
        policy_version=actor.policy_version, authz_epoch=actor.authz_epoch,
        fence=lease.fence, conversation_id=f"fixture-conversation:{operation_id}",
    )
    amount = reservation_amount
    reservation = ReservationRequest(
        id=reservation_id, operation_id=operation_id, purpose="operation_envelope",
        amount=amount, task_id=task.id,
        task_account_id=f"task-budget:{task.id}",
        system_account_id=f"system-budget:{(task.created_at or datetime.now(UTC)).astimezone(UTC):%Y-%m-%d}",
        pricing_version=pricing_version,
        system_period_id=(task.created_at or datetime.now(UTC)).astimezone(UTC).strftime("%Y-%m-%d"),
    )
    envelope = ExecutionEnvelope(
        task_id=task.id, attempt_id=attempt_id, operation_id=operation_id,
        principal_id=actor.principal_id, scope=actor.scope,
        input_revision=task.input_revision, model_allowlist=(task_config.model,),
        pricing_version=pricing_version, deadline=deadline,
        max_input_tokens=10, max_output_tokens=10,
        billable_call_slots=billable_call_slots, max_tool_calls=0,
        fence=lease.fence, reservation_id=reservation_id,
    )
    manifest = {
        "task_id": str(task.id), "attempt_id": str(attempt_id),
        "registry_id": str(registry_id), "input_revision": task.input_revision,
        "topic_id": str(task.topic_id) if task.topic_id else None,
        "base_position_version": task.base_position_version,
        "evidence": [], "dissent_refs": [],
    }
    request = AdmissionRequest(
        binding=binding, reservation=reservation, envelope=envelope,
        attempt_kind=attempt_kind, parent_attempt_id=parent_attempt_id,
        operation_kind=operation_kind, payload={"prompt": "stored synthetic bridge result fixture"},
        lease_owner=worker, context_manifest=manifest,
        workflow_stage=workflow_stage, workflow_stage_hash=workflow_stage_hash,
    )
    await admit_operation(factory, request)
    async with factory() as uow:
        jobs = await uow.delivery.claim_jobs(worker, 100, 60)
        job = next((value for value in jobs if value.operation_id == operation_id), None)
        if job is None:
            raise AssertionError("admitted operation has no dispatch intent")
        await uow.commit()
    await record_dispatch_send_intent(factory, job, worker)
    await record_dispatch_accepted(factory, job, worker)
    return request


async def _admit_critic_review(factory, actor, worker: str, task_config, workflow):
    critic = None
    async with factory() as uow:
        critic = await uow.agents.lock_registry(workflow.critic_registry_id)
        lease = await uow.agents.acquire_lease(critic.registry_id, worker, 1800)
        task = await uow.tasks.lock_task(workflow.task_id)
        if lease is None:
            raise AssertionError("fixture could not acquire the created Critic lease")
        profile = restore_execution_config(workflow.critic_profile)
        amount = _reservation_amount(profile)
        period = (task.created_at or datetime.now(UTC)).astimezone(UTC).strftime("%Y-%m-%d")
        reservation = ReservationRequest(
            id=workflow.review_reservation_id, operation_id=workflow.review_operation_id,
            purpose="operation_envelope", amount=amount, task_id=task.id,
            task_account_id=f"task-budget:{task.id}", system_account_id=f"system-budget:{period}",
            pricing_version=profile.pricing_version, system_period_id=period,
        )
        envelope = ExecutionEnvelope(
            task_id=task.id, attempt_id=workflow.review_attempt_id,
            operation_id=workflow.review_operation_id, principal_id=actor.principal_id,
            scope=actor.scope, input_revision=workflow.input_revision,
            model_allowlist=(profile.model,), pricing_version=profile.pricing_version,
            deadline=min(task.deadline, datetime.now(UTC) + timedelta(minutes=10)),
            max_input_tokens=profile.max_input_tokens, max_output_tokens=profile.max_output_tokens,
            billable_call_slots=0, max_tool_calls=0, fence=lease.fence,
            reservation_id=workflow.review_reservation_id,
        )
        binding = GuardBinding(
            task_id=task.id, attempt_id=workflow.review_attempt_id,
            agent_registry_id=critic.registry_id, provider_agent_id=critic.provider_id,
            principal_id=actor.principal_id, scope=actor.scope,
            input_revision=workflow.input_revision, policy_version=actor.policy_version,
            authz_epoch=actor.authz_epoch, fence=lease.fence,
            conversation_id=f"fixture-conversation:{workflow.review_operation_id}",
        )
        manifest = {
            "task_id": str(task.id), "attempt_id": str(workflow.review_attempt_id),
            "registry_id": str(critic.registry_id), "input_revision": workflow.input_revision,
            "topic_id": str(task.topic_id) if task.topic_id else None,
            "base_position_version": task.base_position_version,
            "evidence": [], "dissent_refs": [],
        }
        request = AdmissionRequest(
            binding=binding, reservation=reservation, envelope=envelope,
            attempt_kind="critic_review", parent_attempt_id=workflow.parent_attempt_id,
            operation_kind="critic.review", payload={"prompt": "stored synthetic Critic result fixture"},
            lease_owner=worker, context_manifest=manifest,
            workflow_stage="critic_review", workflow_stage_hash=workflow.stage_hash("critic_review"),
        )
        await uow.commit()
    await admit_operation(factory, request)
    async with factory() as uow:
        jobs = await uow.delivery.claim_jobs(worker, 100, 60)
        job = next((value for value in jobs if value.operation_id == workflow.review_operation_id), None)
        if job is None:
            raise AssertionError("Critic review admission has no dispatch intent")
        await uow.commit()
    await record_dispatch_send_intent(factory, job, worker)
    await record_dispatch_accepted(factory, job, worker)
    return request


async def _finish(request, worker: str, *, usage: bool = False) -> str | None:
    call_id = None
    if usage:
        call_id = AccountingCallId(f"reconcile-usage:{uuid4()}")
        provider_call_id = ProviderCallId(f"synthetic-call:{uuid4()}")
        call = BillableCallIntent(
            accounting_call_id=call_id, permit_id=PermitId(f"permit:{uuid4()}"),
            operation_id=request.envelope.operation_id, call_kind="turn", slot_key="main:0",
            binding=request.binding, model=request.envelope.model_allowlist[0],
            allocation_amount=Decimal("0.001"),
            limits=RuntimeLimits(10, 10, 1, request.envelope.deadline),
            price_table=PriceTable(
                model=request.envelope.model_allowlist[0], version="local-pricing-v1",
                input_usd_per_million=Decimal("1"), output_usd_per_million=Decimal("1"),
                synthetic=True,
            ),
            permit_expires_at=min(request.envelope.deadline, datetime.now(UTC) + timedelta(minutes=2)),
            lease_owner=request.lease_owner, test_only=True,
            reservation_id=request.reservation.id,
        )
        permit = await authorize_provider_call(_FACTORY, call)
        await consume_call_permit(_FACTORY, request.binding, request.lease_owner, permit.permit_id, call_id)
        _USAGE_EVENT["provider_call_id"] = str(provider_call_id)
        await _store_runtime_observation(_FACTORY, RuntimeInboxPayload(
            event_type="provider_call", operation_id=str(request.envelope.operation_id),
            accounting_call_id=str(call_id), source="provider_response",
            observation_identity=f"provider-call-{call_id}",
            binding=InboxBinding.from_binding(request.binding), provider_call_id=str(provider_call_id),
            state="QUIESCENT", lease_owner=request.lease_owner,
            observer_fence=request.binding.fence,
        ))
    await _store_runtime_observation(_FACTORY, RuntimeInboxPayload(
        event_type="execution", operation_id=str(request.envelope.operation_id),
        accounting_call_id=str(call_id or f"terminal:{request.envelope.operation_id}"),
        source="bridge_terminal", observation_identity=f"execution-{request.envelope.operation_id}",
        binding=InboxBinding.from_binding(request.binding), state="QUIESCENT", outcome="SUCCEEDED",
        lease_owner=request.lease_owner, observer_fence=request.binding.fence,
    ))
    return str(call_id) if call_id else None


_FACTORY = None
_USAGE_EVENT: dict[str, str] = {}


async def _store_runtime_observation(factory, payload: RuntimeInboxPayload) -> None:
    values = payload.model_dump(mode="json", exclude_none=True)
    event_key = f"runtime:{payload.operation_id}:{payload.observation_identity}"
    async with factory() as uow:
        receipt = await uow.delivery.insert_inbox_once(
            "letta-bridge", event_key, values, canonical_json_hash(values),
        )
        if receipt.conflict or receipt.duplicate:
            raise AssertionError("runtime observation fixture identity was not unique")
        await uow.commit()


async def _store_result(factory, request, result: dict[str, object], event_id: str) -> str:
    from hekate.application.runtime_inbox import InboxBinding

    raw = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    payload = {
        "event_type": "business_result", "operation_id": str(request.envelope.operation_id),
        "observation_identity": event_id,
        "binding": InboxBinding.from_binding(request.binding).model_dump(mode="json"),
        "business_result": {
            "state": "VALID", "raw_output": raw,
            "output_sha256": hashlib.sha256(raw.encode()).hexdigest(),
            "output_truncated": False, "structured_output": result,
        },
    }
    async with factory() as uow:
        receipt = await uow.delivery.insert_inbox_once(
            "letta-bridge", event_id, payload, canonical_json_hash(payload),
        )
        if receipt.conflict or receipt.duplicate:
            raise AssertionError("business-result fixture identity was not unique")
        await uow.commit()
    return receipt.id


async def _store_usage(factory, request, call_id: str, event_id: str) -> str:
    from hekate.application.runtime_inbox import InboxBinding

    payload = {
        "event_type": "runtime_usage", "operation_id": str(request.envelope.operation_id),
        "accounting_call_id": call_id, "source": "bridge_event",
        "observation_identity": event_id,
        "binding": InboxBinding.from_binding(request.binding).model_dump(mode="json"),
        "provider_call_id": _USAGE_EVENT["provider_call_id"],
        "usage": {
            "source": "provider_reported", "completeness": "COMPLETE",
            "input_tokens": 10, "output_tokens": 2, "total_tokens": 12,
        },
    }
    async with factory() as uow:
        receipt = await uow.delivery.insert_inbox_once(
            "letta-bridge", event_id, payload, canonical_json_hash(payload),
        )
        if receipt.conflict or receipt.duplicate:
            raise AssertionError("usage fixture identity was not unique")
        await uow.commit()
    return receipt.id


async def _set_result_due(factory, inbox_ids: list[str]) -> None:
    async with factory() as uow:
        if inbox_ids:
            await uow.session.execute(text(
                "UPDATE turn_results SET next_attempt_at=now() WHERE inbox_id = ANY(:ids)"
            ), {"ids": inbox_ids})
        await uow.commit()


async def _seed_unknown(factory, actor, agent, worker: str, task_config, run_id: str) -> dict[str, object]:
    from hekate.application.operations import record_execution_observation

    task_id = await _submit_task(factory, actor, task_config, f"unknown-{run_id}", "Preserve synthetic UNKNOWN fixture.")
    request = await _admit_execution(factory, actor, agent, worker, task_id, task_config)
    await record_execution_observation(factory, ExecutionObservation(
        operation_id=request.envelope.operation_id, binding=request.binding,
        lease_owner=worker, observer_fence=request.binding.fence,
        state="UNKNOWN", source="bridge_disconnect", observed_at=datetime.now(UTC),
        reason="transport_disconnect",
    ))
    return {"task_id": task_id, "operation_id": str(request.envelope.operation_id)}


async def run_probe() -> int:
    run_id = "personal-reconcile-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    artifact = ARTIFACTS / f"{run_id}.json"
    run_root = Path.home() / ".local/share/hekate/personal-reconcile" / run_id
    report: dict[str, object] = {
        "schema_version": "1", "probe": "personal-reconcile-apply-boundary",
        "run_id": run_id, "started_at": now_utc(), "branch": None, "head": None,
        "status": "FAILED", "actual_model_generations": 0, "model_load_requests": 0,
        "external_provider_calls": 0, "fake_provider_requests": 0,
        "production_dispatch": "BLOCKED", "submission_candidate_files": [],
    }
    engine = None
    sync_engine = None
    container = None
    app_server_name = f"hekate-reconcile-let-ta-{run_id[-8:]}"
    db_container = None
    db_volume = None
    try:
        report["branch"] = subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip()
        report["head"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        report["baseline_head"] = "6173b0232f35402a00a412278d0302a947696135"
        report["existing_untracked_artifacts_preserved"] = True
        node_bin = os.environ.get("HEKATE_NODE_BIN") or shutil.which("node") or ""
        if not node_bin or subprocess.check_output([node_bin, "--version"], text=True).strip() != "v22.19.0":
            raise RuntimeError("the pinned Node 22.19.0 runtime is required")
        report["node"] = {"path": node_bin, "version": "v22.19.0"}
        image = os.environ.get("HEKATE_LETTA_IMAGE", p6e.DEFAULT_IMAGE)
        report["pinned_runtime"] = p6e.verify_image(image)
        db_base, db_port, db_container, db_volume = p6f.configure_postgres(run_id)
        db_url = p6f._db_url(db_base, "hekate_p6f_fake")
        run_root.mkdir(parents=True, exist_ok=False, mode=0o700)
        gateway_port, letta_port = p6e.free_port(), p6e.free_port()
        config_dir, state_dir, _allowance, scope, worker, bundle = p6f.prepare_config(
            run_root, run_id, "fake", db_url, gateway_port, letta_port, personal_local=True,
        )
        env = p6f._env(bundle)
        init, _ = p6e.cli(env, "init-local")
        report["init_local"] = init
        if init.get("scope_created") is not True:
            raise AssertionError("isolated PostgreSQL scope did not initialize")

        # No gateway is started. The isolated CLI/bridge environment uses test
        # mode and an unreachable upstream so an accidental dispatch fails closed.
        local = p6f.yaml.safe_load((config_dir / "local.yaml").read_text(encoding="utf-8"))
        local["gateway"]["upstream_base_url"] = "http://127.0.0.1:1"
        local["gateway"]["upstream_api_key"] = "no-dispatch-in-this-probe"
        p6f.write_yaml(config_dir / "local.yaml", local)
        env["HEKATE_RUNTIME_MODE"] = "test"
        env["HEKATE_NODE_BIN"] = node_bin
        report["provider_isolation"] = {
            "runtime_mode": "test", "gateway_started": False,
            "unreachable_upstream": local["gateway"]["upstream_base_url"],
            "local_model_profile_loaded": "configuration only; no model weights or provider session",
        }
        policy_path = config_dir / "policy.yaml"
        policy = p6f.yaml.safe_load(policy_path.read_text(encoding="utf-8"))
        policy["critic"].update({
            "enabled": True, "max_agents_per_task": 1, "max_review_rounds": 2,
            "max_syntheses_per_task": 2,
        })
        policy["deliberation"].update({
            "enabled": True, "max_critic_agents": 1, "max_review_rounds": 2,
            "max_hekate_continuations": 1, "max_syntheses_per_task": 2,
        })
        p6f.write_yaml(policy_path, policy)
        sync_engine = create_sync_engine(db_url)
        report["database"] = p6e.db_meta(sync_engine)
        report["database_isolation"] = {"container": db_container, "volume": db_volume, "database": "hekate_p6f_fake"}
        p6e.start_app_server(image, app_server_name, state_dir / "letta", bundle["app_token"],
                             int(local["gateway"]["port"]), bundle["gateway_token"], letta_port)
        settings = load_settings(env, config_dir)
        container = await build_container(settings)
        factory = container.uow_factory
        global _FACTORY
        _FACTORY = factory
        actor = configured_local_actor(settings)
        task_config = configured_task_execution(settings)
        critic_config = configured_critic_execution(settings)
        deliberation = configured_deliberation(settings)
        agent = await ensure_hekate(factory, container.runtime, actor, task_config)
        report["runtime_agent"] = {
            "registry_id": str(agent.registry_id), "provider_agent_id": str(agent.provider_id),
            "identity_create_confirmed": True, "inference_calls": 0,
        }

        # Planning spawn is parsed and stored by the real reconcile CLI, but its
        # child workflow is only created later by the normal Worker result path.
        spawn_task = await _submit_task(factory, actor, task_config, f"spawn-{run_id}", "Review one bounded uncertainty.")
        spawn_request = await _admit_execution(factory, actor, agent, worker, spawn_task, task_config)
        await _finish(spawn_request, worker)
        spawn_result = _output(spawn_task, str(spawn_request.binding.attempt_id), str(agent.registry_id), "spawn")
        spawn_inbox = await _store_result(factory, spawn_request, spawn_result, f"spawn-result-{run_id}")
        spawn_before = _snapshot(sync_engine, scope, spawn_task)
        first_apply, _ = p6e.cli(env, "reconcile", "--apply", "--limit", "100")
        spawn_after = _snapshot(sync_engine, scope, spawn_task)
        second_apply, _ = p6e.cli(env, "reconcile", "--apply", "--limit", "100")
        spawn_replayed = _snapshot(sync_engine, scope, spawn_task)
        spawn_defer = [item for item in first_apply.get("apply", {}).get("deferred_followup_results", []) if item.get("task_id") == spawn_task]
        if not spawn_defer or spawn_defer[0].get("reason") != "followup_inference_requires_worker":
            raise AssertionError("reconcile did not report the valid planning SpawnProposal as deferred")
        if spawn_after["workflow"] or spawn_after["critic_registry_count"] != 0 or spawn_after["operation_count"] != spawn_before["operation_count"] or spawn_after["reservation_count"] != spawn_before["reservation_count"] or spawn_after["outbox_count"] != spawn_before["outbox_count"]:
            raise AssertionError("reconcile created planning-spawn child workflow effects")
        if spawn_after["task"]["critic_agents"] != 0 or spawn_after["task"]["review_rounds"] != 0:
            raise AssertionError("reconcile consumed a Critic or review counter")
        await _set_result_due(factory, [spawn_inbox])
        worker_spawn = await process_pending_results(
            factory, 100, hekate_config=task_config, critic_config=critic_config,
            deliberation_config=deliberation, owner_scope=actor.scope,
        )
        spawn_worker = _snapshot(sync_engine, scope, spawn_task)
        if worker_spawn != 1 or len(spawn_worker["workflow"]) != 1 or spawn_worker["workflow"][0]["stage"] != "CREATE_PENDING":
            raise AssertionError("normal Worker did not approve the same stored SpawnProposal once")

        # Use the real pinned App Server lifecycle to create the ephemeral Critic;
        # no session turn is prepared and no inference is dispatched.
        async def create_critic_job():
            async with factory() as uow:
                jobs = await uow.delivery.claim_lifecycle_jobs(worker, 20, 60)
                job = next((item for item in jobs if item.kind == "critic_create"), None)
                if job is None:
                    raise AssertionError("approved spawn has no Critic creation intent")
                await uow.commit()
            return await create_from_intent(factory, container.runtime, job, worker)

        critic_create_observation = await create_critic_job()
        async def get_workflow():
            async with factory() as uow:
                value = await uow.critic_workflows.get(spawn_task)
                await uow.commit()
                return value
        workflow = await get_workflow()
        if workflow is None or workflow.stage != "CRITIC_READY" or not critic_create_observation.get("present"):
            raise AssertionError("pinned runtime Critic creation did not bind a ready registry")
        review_request = await _admit_critic_review(factory, actor, worker, task_config, workflow)
        await _finish(review_request, worker)
        critic_output = {"schema_version": "1", "conclusion": _conclusion(
            spawn_task, str(review_request.binding.attempt_id), str(workflow.critic_registry_id), objection=True,
        )}
        critic_inbox = await _store_result(
            factory, review_request, critic_output, f"critic-result-{run_id}",
        )
        critic_before = _snapshot(sync_engine, scope, spawn_task)
        critic_apply, _ = p6e.cli(env, "reconcile", "--apply", "--limit", "100")
        critic_after = _snapshot(sync_engine, scope, spawn_task)
        critic_defer = [item for item in critic_apply.get("apply", {}).get("deferred_followup_results", []) if item.get("inbox_id") == critic_inbox]
        report["critic_debug"] = {
            "before": critic_before, "after": critic_after,
            "apply": critic_apply.get("apply"), "reported_deferred": critic_defer,
        }
        critic_result_after = next(item for item in critic_after["result"] if item["inbox_id"] == critic_inbox)
        if not critic_defer or critic_after["workflow"][0]["stage"] != "REVIEW_ADMITTED" or critic_after["dissent_count"] != critic_before["dissent_count"] or critic_result_after["processing_state"] != "WAITING_EXECUTION":
            raise AssertionError("reconcile accepted or activated the Critic result instead of deferring it")
        await _set_result_due(factory, [critic_inbox])
        worker_critic = await process_pending_results(
            factory, 100, hekate_config=task_config, critic_config=critic_config,
            deliberation_config=deliberation, owner_scope=actor.scope,
        )
        critic_worker = _snapshot(sync_engine, scope, spawn_task)
        if worker_critic != 1 or critic_worker["workflow"][0]["stage"] != "SYNTHESIS_PENDING" or critic_worker["dissent_count"] != 1:
            raise AssertionError("normal Worker did not accept the same Critic result and persist dissent")

        # Valid enabled continuation is likewise held without consuming counters
        # or creating step/reservation/outbox rows.
        continuation_task = await _submit_task(factory, actor, task_config, f"continue-{run_id}", "Continue one bounded synthetic check.")
        continuation_request = await _admit_execution(factory, actor, agent, worker, continuation_task, task_config)
        await _finish(continuation_request, worker)
        continuation_result = _output(
            continuation_task, str(continuation_request.binding.attempt_id), str(agent.registry_id), "continue",
        )
        continuation_inbox = await _store_result(factory, continuation_request, continuation_result, f"continuation-result-{run_id}")
        continuation_before = _snapshot(sync_engine, scope, continuation_task)
        continuation_apply, _ = p6e.cli(env, "reconcile", "--apply", "--limit", "100")
        continuation_after = _snapshot(sync_engine, scope, continuation_task)
        continuation_defer = [item for item in continuation_apply.get("apply", {}).get("deferred_followup_results", []) if item.get("inbox_id") == continuation_inbox]
        if not continuation_defer or continuation_after["operation_count"] != continuation_before["operation_count"] or continuation_after["reservation_count"] != continuation_before["reservation_count"] or continuation_after["outbox_count"] != continuation_before["outbox_count"] or continuation_after["deliberation_steps"]:
            raise AssertionError("reconcile activated a valid continuation proposal")
        await _set_result_due(factory, [continuation_inbox])
        worker_continuation = await process_pending_results(
            factory, 100, hekate_config=task_config, critic_config=critic_config,
            deliberation_config=deliberation, owner_scope=actor.scope,
        )
        continuation_worker = _snapshot(sync_engine, scope, continuation_task)
        if worker_continuation != 1 or continuation_worker["task"]["hekate_continuations"] != 1 or not continuation_worker["deliberation_steps"]:
            raise AssertionError("normal Worker did not approve the same valid continuation exactly once")

        # A canceled Task with a syntactically valid SpawnProposal follows the
        # normal late-result path; it is not held forever as a valid follow-up.
        canceled_task = await _submit_task(factory, actor, task_config, f"cancel-{run_id}", "Do not continue after cancellation.")
        canceled_request = await _admit_execution(factory, actor, agent, worker, canceled_task, task_config)
        await _finish(canceled_request, worker)
        canceled_result = _output(
            canceled_task, str(canceled_request.binding.attempt_id), str(agent.registry_id), "spawn",
        )
        canceled_inbox = await _store_result(factory, canceled_request, canceled_result, f"canceled-result-{run_id}")
        canceled_before = _snapshot(sync_engine, scope, canceled_task)
        cancel_receipt = await cancel(factory, actor, TaskId(canceled_task), StopReason.USER_CANCELLED)
        canceled_apply, _ = p6e.cli(env, "reconcile", "--apply", "--limit", "100")
        canceled_after = _snapshot(sync_engine, scope, canceled_task)
        canceled_result_after = next(item for item in canceled_after["result"] if item["inbox_id"] == canceled_inbox)
        canceled_deferred = [
            item for item in canceled_apply.get("apply", {}).get("deferred_followup_results", [])
            if item.get("inbox_id") == canceled_inbox
        ]
        if (
            cancel_receipt["state"] not in {"STOPPING", "CANCELLED"}
            or canceled_after["task"]["status"] != "CANCELLED"
            or canceled_result_after["processing_state"] != "LATE"
            or canceled_deferred or canceled_after["workflow"]
            or canceled_after["critic_registry_count"] != 0
            or canceled_after["reservation_count"] != canceled_before["reservation_count"]
        ):
            raise AssertionError("canceled follow-up result was deferred or created Critic effects")

        # A final answer and saved synthetic usage event are processed by the same
        # reconcile batch despite earlier follow-up deferrals.
        answer_task = await _submit_task(factory, actor, task_config, f"answer-{run_id}", "Complete from a stored answer.")
        answer_request = await _admit_execution(
            factory, actor, agent, worker, answer_task, task_config,
            reservation_amount=Decimal("0.01"), pricing_version="local-pricing-v1",
            billable_call_slots=1,
        )
        call_id = await _finish(answer_request, worker, usage=True)
        answer_result = _output(answer_task, str(answer_request.binding.attempt_id), str(agent.registry_id), "answer")
        answer_inbox = await _store_result(factory, answer_request, answer_result, f"answer-result-{run_id}")
        usage_inbox = await _store_usage(factory, answer_request, str(call_id), f"usage-result-{run_id}")
        answer_before = _snapshot(sync_engine, scope, answer_task)
        answer_apply, _ = p6e.cli(env, "reconcile", "--apply", "--limit", "100")
        answer_after = _snapshot(sync_engine, scope, answer_task)
        financial_after = _sql(sync_engine, """
            SELECT p.accounting_call_id,p.status,p.measurement_status,u.completeness,u.settlement_state,
                   u.evaluated_cost_usd::text AS evaluated_cost_usd
            FROM provider_calls p JOIN operations o ON o.id=p.operation_id
            LEFT JOIN usage_projections u USING(accounting_call_id)
            WHERE o.task_id=:task ORDER BY p.accounting_call_id
        """, {"task": answer_task})
        if answer_after["task"]["status"] != "COMPLETED" or answer_after["response_count"] != 1 or answer_after["result"][0]["processing_state"] != "ACCEPTED":
            raise AssertionError("reconcile did not accept a final stored answer")
        if len(financial_after) != 1 or financial_after[0]["settlement_state"] != "SETTLED" or financial_after[0]["status"] != "QUIESCENT":
            raise AssertionError("reconcile did not settle saved quiescent usage")
        settlement_rows_after = len(_sql(
            sync_engine,
            "SELECT * FROM budget_ledger WHERE effect_type='SETTLE' AND accounting_call_id=:id",
            {"id": call_id},
        ))
        await _set_result_due(factory, [spawn_inbox, critic_inbox, continuation_inbox])
        repeated, _ = p6e.cli(env, "reconcile", "--apply", "--limit", "100")
        answer_replayed = _snapshot(sync_engine, scope, answer_task)
        financial_replayed = _sql(sync_engine, """
            SELECT p.accounting_call_id,p.status,p.measurement_status,u.completeness,u.settlement_state,
                   u.evaluated_cost_usd::text AS evaluated_cost_usd
            FROM provider_calls p JOIN operations o ON o.id=p.operation_id
            LEFT JOIN usage_projections u USING(accounting_call_id)
            WHERE o.task_id=:task ORDER BY p.accounting_call_id
        """, {"task": answer_task})
        settlement_rows_replayed = len(_sql(
            sync_engine,
            "SELECT * FROM budget_ledger WHERE effect_type='SETTLE' AND accounting_call_id=:id",
            {"id": call_id},
        ))
        if (
            answer_replayed != answer_after or financial_replayed != financial_after
            or settlement_rows_replayed != settlement_rows_after
        ):
            raise AssertionError("reconcile replay changed the final response, call, or settlement ledger")

        unknown = await _seed_unknown(factory, actor, agent, worker, task_config, run_id)
        unknown_before = _sql(sync_engine, """
            SELECT o.state,o.execution_state,h.state AS hold_state,h.quiescent_at
            FROM operations o JOIN agent_execution_holds h ON h.operation_id=o.id
            WHERE o.id=:operation
        """, {"operation": unknown["operation_id"]})[0]
        unknown_apply, _ = p6e.cli(env, "reconcile", "--apply", "--limit", "100")
        unknown_after = _sql(sync_engine, """
            SELECT o.state,o.execution_state,h.state AS hold_state,h.quiescent_at
            FROM operations o JOIN agent_execution_holds h ON h.operation_id=o.id
            WHERE o.id=:operation
        """, {"operation": unknown["operation_id"]})[0]
        if unknown_before != unknown_after or unknown_after["state"] != "UNKNOWN" or unknown_after["hold_state"] != "UNKNOWN" or unknown_after["quiescent_at"] is not None:
            raise AssertionError("reconcile changed a synthetic UNKNOWN execution or hold")

        report.update({
            "spawn_reconciliation": {
                "task_id": spawn_task, "operation_id": str(spawn_request.envelope.operation_id),
                "inbox_id": spawn_inbox, "before": spawn_before, "after_first_apply": spawn_after,
                "after_replay": spawn_replayed, "reported_deferred": spawn_defer,
                "second_apply": second_apply.get("apply"), "worker_result_count": worker_spawn,
                "worker_state": spawn_worker,
            },
            "critic_reconciliation": {
                "inbox_id": critic_inbox, "operation_id": str(review_request.envelope.operation_id),
                "runtime_create_observation": {
                    "provider_agent_id": str(critic_create_observation["provider_agent_id"]),
                    "present": critic_create_observation["present"],
                    "runtime_create_only_no_inference": True,
                },
                "before": critic_before, "after_reconcile": critic_after,
                "reported_deferred": critic_defer,
                "apply_inbox_outcomes": critic_apply.get("apply", {}).get("inbox", []),
                "worker_result_count": worker_critic,
                "after_worker": critic_worker,
            },
            "continuation_reconciliation": {
                "task_id": continuation_task, "inbox_id": continuation_inbox,
                "before": continuation_before, "after_reconcile": continuation_after,
                "reported_deferred": continuation_defer,
                "worker_result_count": worker_continuation,
                "after_worker": continuation_worker,
            },
            "canceled_followup_result": {
                "task_id": canceled_task, "inbox_id": canceled_inbox,
                "cancel_receipt": cancel_receipt, "before": canceled_before,
                "after": canceled_after,
                "reported_deferred": canceled_deferred,
                "result_state": canceled_result_after["processing_state"],
                "reason": canceled_apply.get("apply", {}).get("turn_result_outcomes"),
            },
            "final_answer_and_usage": {
                "task_id": answer_task, "operation_id": str(answer_request.envelope.operation_id),
                "answer_inbox_id": answer_inbox, "usage_inbox_id": usage_inbox,
                "before": answer_before, "after": answer_after,
                "usage_after": financial_after, "replay_apply": repeated.get("apply"),
                "usage_after_replay": financial_replayed,
                "response_once": answer_after["response_count"] == 1,
                "settlement_rows_after_apply": settlement_rows_after,
                "settlement_rows_after_replay": settlement_rows_replayed,
                "settlement_once": settlement_rows_after == settlement_rows_replayed == 2,
                "inbox_outcomes": answer_apply.get("apply", {}).get("inbox", []),
            },
            "unknown_preservation": {
                **unknown, "before": unknown_before, "after": unknown_after,
                "apply": unknown_apply.get("apply"),
            },
            "calls_and_inference": {
                "synthetic_accounting_fixture_rows": len(financial_after),
                "fake_provider_requests": 0, "actual_model_generations": 0,
                "model_load_requests": 0, "external_provider_calls": 0,
                "pinned_runtime_used_for": ["persistent HEKATE identity", "Critic agent.create only"],
            },
            "commands": [
                f"HEKATE_NODE_BIN={node_bin} uv run --locked python scripts/personal_reconcile_guard_probe.py",
                "python -m hekate init-local",
                "python -m hekate reconcile --apply --limit 100",
                "process_pending_results(... mode default WORKER)",
            ],
            "cli_invocations": p6e.CLI_OBSERVATIONS,
            "unexecuted": [
                "No model inference or provider gateway request was performed.",
                "No separate production Worker process was started; normal result handling was invoked through its application entry point.",
                "No schema or migration changes were made.",
            ],
            "projection_status": "PENDING_UNSUPPORTED was not modified by this probe",
            "production_dispatch": "BLOCKED",
            "status": "PASS",
        })
        report.pop("critic_debug", None)
    except BaseException as error:
        report["status"] = "FAILED"
        report["error"] = {"type": type(error).__name__, "message": str(error)[:1200]}
    finally:
        if container is not None:
            await close_container(container)
        if app_server_name:
            p6e.stop_app_server(app_server_name)
        if sync_engine is not None:
            sync_engine.dispose()
        if db_container:
            p6f.run(["docker", "stop", "--time", "10", db_container], timeout=20)
        fingerprint, files = code_fingerprint()
        report["code_fingerprint_sha256"] = fingerprint
        report["fingerprinted_files"] = files
        report["artifact_path"] = str(artifact.relative_to(ROOT))
        report["run_root"] = str(run_root)
        report["finished_at"] = now_utc()
        report["submission_candidate_files"] = [
            "src/hekate/application/lifecycle.py", "src/hekate/application/results.py",
            "docs/personal-use.md", "scripts/personal_reconcile_guard_probe.py",
            str(artifact.relative_to(ROOT)),
        ]
        artifact.parent.mkdir(parents=True, exist_ok=True)
        temporary = artifact.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        os.chmod(temporary, 0o600)
        os.replace(temporary, artifact)
        print(json.dumps({"status": report["status"], "artifact": str(artifact), "fingerprint": fingerprint}, ensure_ascii=False))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_probe()))
