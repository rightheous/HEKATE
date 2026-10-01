from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import tempfile
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import make_url
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import phase3_runtime_probe as p3

from hekate.application import results as result_application
from hekate.application import tasks as task_application
from hekate.application.operations import record_dispatch_send_intent
from hekate.application.runtime_inbox import InboxBinding, RuntimeInboxPayload, process_runtime_observation
from hekate.application.turns import prepare_queued_tasks
from hekate.domain.contracts import canonical_json_hash
from hekate.domain.errors import Conflict, PolicyDenied
from hekate.domain.models import AuthorizationSnapshot, InputChange, RuntimeBinding, TaskExecutionConfig, UserMessage
from hekate.domain.types import (
    ActorContext, AttemptId, PrincipalId, ProviderAgentId, RegistryId, ScopeId, StopReason,
    TaskId, TaskStatus,
)
from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.infrastructure.letta.bridge_protocol import BridgeClient
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.settings import configured_local_actor, configured_task_execution, load_settings
from hekate.worker.service import dispatch_job, run_worker


def _output_from_request(request: dict[str, object]) -> str:
    def strings(value: object):
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for item in value.values():
                yield from strings(item)
        elif isinstance(value, list):
            for item in value:
                yield from strings(item)

    prompt = next((item for item in strings(request.get("messages")) if "Task Capsule JSON:\n" in item), None)
    if prompt is None:
        return "Fake-only context compression."
    try:
        capsule, _ = json.JSONDecoder().raw_decode(prompt.split("Task Capsule JSON:\n", 1)[1].lstrip())
    except json.JSONDecodeError:
        return "Fake-only context compression."
    match = re.search(r"registry ID; this value is not an authorization credential: ([^\s.]+)", prompt)
    if match is None:
        return "Fake-only context compression."
    registry_id = match.group(1)
    objective = capsule["objective"]
    answer = f"Fake-only response: {objective}"
    output = {
        "schema_version": "1",
        "proposal": {"schema_version": "1", "action": "answer", "answer": answer},
        "conclusion": {
            "schema_version": "1",
            "task_id": capsule["task_id"],
            "attempt_id": capsule["attempt_id"],
            "agent_id": registry_id,
            "status": "done",
            "assessment": {
                "statement": "The isolated fake completed this one-turn task.",
                "confidence": {"level": "high", "basis": ["synthetic integration fixture"]},
            },
            "evidence_used": [],
            "objections": [],
            "assumptions": [],
            "unresolved": [],
            "recommended_next_step": {"type": "none"},
            "position_recommendation": {"action": "maintain", "summary": "No position change."},
        },
    }
    tools = request.get("tools")
    if isinstance(tools, list) and any(
        isinstance(tool, dict) and isinstance(tool.get("function"), dict)
        and tool["function"].get("name") == "StructuredOutput"
        for tool in tools
    ):
        return output
    return json.dumps(output, ensure_ascii=False, separators=(",", ":"))


async def _dispatch_to_saved_result(factory, runtime, actor, config, worker, container, task_id: TaskId) -> dict[str, object]:
    leases = await prepare_queued_tasks(factory, runtime, actor, config, worker)
    job = await p3.claim_one(factory, worker)
    if len(leases) != 1 or job is None:
        raise AssertionError("scenario did not create one ordinary admitted outbox operation")
    try:
        await dispatch_job(container, job, worker)
    finally:
        async with factory() as uow:
            await uow.agents.release_lease(leases[0])
            await uow.commit()
    async with container.database.connect() as connection:
        row = (await connection.execute(text("""
            SELECT o.execution_state, r.processing_state, r.inbox_id
            FROM task_preparations p
            JOIN operations o ON o.id=p.operation_id
            LEFT JOIN turn_results r ON r.operation_id=o.id
            WHERE p.task_id=:task AND p.input_revision=1
        """), {"task": str(task_id)})).mappings().one()
    return dict(row)


async def _cli(env: dict[str, str], *args: str, stdin: bytes = b"") -> dict[str, object]:
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "hekate", *args,
        cwd=ROOT, env=env, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate(stdin)
    if process.returncode != 0:
        raise RuntimeError(f"CLI {args[0]} exited with status {process.returncode}: {stderr.decode(errors='replace')[-300:]}")
    value = json.loads(stdout.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("CLI returned a non-object response")
    return value


async def _wait_for_state(factory, actor: ActorContext, task_id: TaskId, states: set[str], timeout: float = 120) -> dict[str, object]:
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        value = await task_application.get_task(factory, actor, task_id)
        if value["state"] in states:
            return value
        await asyncio.sleep(0.2)
    raise TimeoutError("Task did not reach a terminal state before the probe deadline")


async def _run(database_url: str, node: str, image: str, archive: Path, artifact: Path, run_id: str) -> dict[str, object]:
    branch = p3.p1.run(["git", "branch", "--show-current"])
    report: dict[str, object] = {
        "schema_version": "1", "probe": "phase3b-single-hekate", "run_id": run_id,
        "executed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "base_commit": "6eb11d128cd697d93ef746b0e69d9e3d1dbfce22",
        "branch": branch, "working_tree_dirty_at_probe": bool(p3.p1.run(["git", "status", "--porcelain"])),
        "real_provider_calls": 0, "results": {}, "overall_status": "blocked",
        "limitations": [
            "The provider route is an isolated synthetic loopback fake; no production provider was configured.",
            "Production dispatch remains closed because no verified real model, full-request tokenizer, pricing, or production route was configured.",
            "G7 same-execution resume and G8 exact full-request tokenization remain unresolved.",
            "Successful contract validation is not a claim of factual correctness; answers are provisional.",
        ],
    }
    engine = bridge = sandbox = fake = gateway_server = gateway_task = gateway_app = None
    temporary = tempfile.TemporaryDirectory(prefix="hekate-phase3b-")
    state_path = Path(temporary.name)
    try:
        await p3._probe_database_url(database_url)
        runtime_lock = p3._locked_runtime(image, node, archive)
        report["runtime"] = runtime_lock
        p3.p1.build_bridge(node)
        engine = create_engine(database_url)
        factory = create_uow_factory(engine)
        await p3._truncate(factory)
        async with engine.connect() as connection:
            migration_head = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
            postgres_version = await connection.scalar(text("SHOW server_version"))
        if migration_head != "0005_phase3b":
            raise ValueError("Phase 3B migration head is not current")
        report["database"] = {"postgres_version": postgres_version, "migration_head": migration_head}

        run_scope = f"phase3b:{run_id}"
        principal_id = f"principal:{run_id}"
        policy_version = "phase3b-fake-policy-v1"
        async with factory() as uow:
            await uow.tasks.insert_scope(AuthorizationSnapshot(
                scope=ScopeId(run_scope), principal_id=PrincipalId(principal_id),
                policy_version=policy_version, authz_epoch=1,
            ))
            await uow.tasks.insert_scope(AuthorizationSnapshot(
                scope=ScopeId(f"phase3b-other:{run_id}"), principal_id=PrincipalId(f"other:{run_id}"),
                policy_version=policy_version, authz_epoch=1,
            ))
            await uow.commit()

        config_dir = state_path / "config"
        config_dir.mkdir()
        (config_dir / "local.yaml").write_text(yaml.safe_dump({"identity": {
            "principal_id": principal_id, "scope_id": run_scope,
            "policy_version": policy_version, "authz_epoch": 1,
        }}), encoding="utf-8")
        (config_dir / "policy.yaml").write_text(yaml.safe_dump({"limits": {
            "task_budget_usd": "1.00", "system_daily_budget_usd": "10.00", "task_deadline_seconds": 240,
        }}), encoding="utf-8")
        (config_dir / "models.yaml").write_text(yaml.safe_dump({"hekate": {
            "profile_id": "phase3b-fake-v1", "model": f"openai-compatible/{p3.FAKE_MODEL}",
            "provider_model": p3.FAKE_MODEL, "max_input_tokens": 32768, "max_output_tokens": 2048,
            "max_compaction_calls": 1,
        }}), encoding="utf-8")
        (config_dir / "pricing.yaml").write_text(yaml.safe_dump({"version": "phase3b-synthetic-v1", "prices": {
            p3.FAKE_MODEL: {"input_usd_per_million": "1", "output_usd_per_million": "2"},
        }}), encoding="utf-8")

        fake = p3.FakeProvider()
        fake.set_response_factory(_output_from_request)
        fake.start()
        sandbox = p3.Phase3Sandbox(state_path, run_id, image)
        sandbox.start_network()
        gateway_port = p3.reserve_port(sandbox.gateway_address)
        private_token = __import__("secrets").token_urlsafe(40)
        profile = p3.ProviderGatewayProfile(
            profile_id="phase3b-fake-v1",
            price_table=p3.PriceTable(
                model=p3.FAKE_MODEL, version="phase3b-synthetic-v1",
                input_usd_per_million=__import__("decimal").Decimal("1"),
                output_usd_per_million=__import__("decimal").Decimal("2"), synthetic=True,
            ),
            upstream_base_url=f"http://127.0.0.1:{fake.port}",
            upstream_api_key="isolated-fake-only", max_input_tokens=32768,
            max_output_tokens=2048, test_only=True,
        )
        gateway_app = p3.create_provider_gateway(factory, profile, private_token, allow_test_profile=True)
        gateway_server, gateway_task = await p3.start_gateway_server(gateway_app, sandbox.gateway_address, gateway_port)
        sandbox.start_pinned_app_server(gateway_port, private_token)
        letta_url = f"ws://{sandbox.container_address}:{p3.p1.APP_PORT}"
        env = os.environ.copy()
        env.update({
            "HEKATE_DATABASE_URL": database_url, "HEKATE_NODE_BIN": node,
            "HEKATE_BRIDGE_ENTRY": str(ROOT / "bridge/letta/dist/main.js"),
            "HEKATE_LETTA_URL": letta_url, "HEKATE_LETTA_TOKEN": private_token,
            "HEKATE_WORKER_ID": "phase3b-single-worker", "HEKATE_RUNTIME_MODE": "test",
            "HEKATE_CONFIG_DIR": str(config_dir),
        })
        node_path = str(Path(node).parent)
        env["PATH"] = f"{node_path}:{env.get('PATH', '')}"
        settings = load_settings(env, config_dir)
        actor = configured_local_actor(settings)
        execution_config = configured_task_execution(settings)
        bridge = BridgeClient(node, ROOT / "bridge/letta/dist/main.js", env={
            "HEKATE_LETTA_URL": letta_url, "HEKATE_LETTA_TOKEN": private_token,
            "HEKATE_REQUIRE_PROVIDER_BINDING": "1",
        })
        runtime = LettaRuntimeAdapter(bridge)
        session_prepare_count = {"count": 0}
        original_prepare_session = runtime.prepare_session

        async def count_session_prepare(binding, output_contract=None):
            session_prepare_count["count"] += 1
            return await original_prepare_session(binding, output_contract)

        runtime.prepare_session = count_session_prepare
        await runtime.verify_compatibility()
        from hekate.bootstrap import Container
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)

        stop = asyncio.Event()
        worker_task = asyncio.create_task(run_worker(container, stop))
        request_key = f"phase3b-{run_id}-first"
        duplicate_cli = await asyncio.gather(
            _cli(env, "ask", "--request-key", request_key, "--wait-seconds", "0", stdin=b"Explain the first fake-only task."),
            _cli(env, "ask", "--request-key", request_key, "--wait-seconds", "0", stdin=b"Explain the first fake-only task."),
        )
        first_ids = {item["receipt"]["task_id"] for item in duplicate_cli}
        if len(first_ids) != 1:
            raise AssertionError("concurrent same-key submissions created multiple Tasks")
        first_id = TaskId(next(iter(first_ids)))
        first_view = await _wait_for_state(factory, actor, first_id, {"COMPLETED", "FAILED"})
        first_provider_count = fake.count()
        replay = await _cli(env, "ask", "--request-key", request_key, "--wait-seconds", "0", stdin=b"Explain the first fake-only task.")
        replay_provider_count = fake.count()
        different_question_conflict = False
        try:
            await task_application.submit(
                factory, actor, UserMessage(text="different question"), request_key, execution_config,
            )
        except Conflict:
            different_question_conflict = True
        second = await _cli(env, "ask", "--request-key", f"phase3b-{run_id}-second", "--wait-seconds", "120", stdin=b"Explain the second fake-only task.")
        second_id = TaskId(str(second["receipt"]["task_id"]))
        second_view = second["task"]
        second_cli_view = await _cli(env, "task", str(second_id))

        other_actor = ActorContext(
            principal_id=PrincipalId(f"other:{run_id}"), scope=ScopeId(f"phase3b-other:{run_id}"),
            authenticated_agent_registry_id=None, task_id=None, attempt_id=None, input_revision=None,
            policy_version=policy_version, authz_epoch=1, fence=0,
        )
        cross_scope_denied = False
        try:
            await task_application.get_task(factory, other_actor, first_id)
        except PolicyDenied:
            cross_scope_denied = True

        async with engine.connect() as connection:
            reuse = (await connection.execute(text("""
                SELECT count(DISTINCT a.id) AS registries,
                       count(DISTINCT a.provider_agent_id) AS providers,
                       count(DISTINCT t.operation_id) AS operations,
                       count(DISTINCT t.attempt_id) AS attempts,
                       count(DISTINCT t.source_inbox_id) AS responses
                FROM task_responses t
                JOIN agent_registry a ON a.id=t.registry_id
                WHERE t.task_id IN (:first, :second)
            """), {"first": str(first_id), "second": str(second_id)})).mappings().one()
            summary = (await connection.execute(text("""
                SELECT (SELECT count(*) FROM tasks WHERE id IN (:first, :second)) AS tasks,
                       (SELECT count(*) FROM task_submissions WHERE owner_scope=:scope) AS submissions,
                       (SELECT count(*) FROM turn_results WHERE task_id IN (:first, :second) AND processing_state='ACCEPTED') AS accepted_results,
                       (SELECT count(*) FROM conclusions WHERE task_id IN (:first, :second) AND eligible) AS eligible_conclusions,
                       (SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.task_id IN (:first, :second)) AS provider_calls,
                       (SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.task_id IN (:first, :second) AND p.call_kind='turn') AS main_provider_calls,
                       (SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.task_id IN (:first, :second) AND p.call_kind='compaction') AS compaction_provider_calls,
                       (SELECT count(*) FROM call_permits p JOIN provider_calls c USING (accounting_call_id) JOIN operations o ON o.id=c.operation_id WHERE o.task_id IN (:first, :second) AND p.state='CONSUMED') AS consumed_permits
            """), {"first": str(first_id), "second": str(second_id), "scope": run_scope})).mappings().one()
            identity_chain = (await connection.execute(text("""
                SELECT r.task_id, r.input_revision, r.attempt_id, r.operation_id, r.registry_id,
                       a.provider_agent_id, r.source_inbox_id AS response_ref, r.outcome, r.stop_reason,
                       tr.output_hash, tr.processing_state, tr.conclusion_id,
                       c.payload_hash AS conclusion_hash, c.validation_status, c.eligible,
                       p.accounting_call_id, p.status AS call_state, cp.permit_id, cp.state AS permit_state,
                       u.completeness, u.settlement_state, u.evaluated_cost_usd
                FROM task_responses r
                JOIN agent_registry a ON a.id=r.registry_id
                JOIN turn_results tr ON tr.inbox_id=r.source_inbox_id
                LEFT JOIN conclusions c ON c.id=tr.conclusion_id
                LEFT JOIN provider_calls p ON p.operation_id=r.operation_id
                LEFT JOIN call_permits cp ON cp.accounting_call_id=p.accounting_call_id
                LEFT JOIN usage_projections u ON u.accounting_call_id=p.accounting_call_id
                WHERE r.task_id IN (:first, :second)
                ORDER BY r.task_id, p.accounting_call_id
            """), {"first": str(first_id), "second": str(second_id)})).mappings().all()
            request_hashes = (await connection.execute(text("""
                SELECT task_id, request_hash FROM task_submissions WHERE task_id IN (:first, :second)
                ORDER BY task_id
            """), {"first": str(first_id), "second": str(second_id)})).mappings().all()
        first_answer = first_view.get("response")
        second_answer = second_cli_view.get("response")
        report["results"]["R2_single_persistent_hekate"] = {
            "task_ids": [str(first_id), str(second_id)], "registry_provider_counts": dict(reuse),
            "database_counts": dict(summary), "accepted_result_chain": [dict(row) for row in identity_chain],
            "submission_hashes": [dict(row) for row in request_hashes],
            "request_key_hashes": [canonical_json_hash(key) for key in (request_key, f"phase3b-{run_id}-second")],
            "first_task_state": first_view["state"],
            "second_task_state": second_view["state"], "accepted_responses_read_from_cli_and_db": bool(first_answer and second_answer),
            "task_cost_statuses": [first_view["cost_status"], second_cli_view["cost_status"]],
            "one_main_call_each": summary["main_provider_calls"] == 2,
            "passed": first_view["state"] == "COMPLETED" and second_cli_view["state"] == "COMPLETED"
            and first_answer and second_answer and reuse["registries"] == 1 and reuse["providers"] == 1
            and reuse["operations"] == 2 and reuse["attempts"] == 2 and reuse["responses"] == 2
            and summary["main_provider_calls"] == 2 and summary["provider_calls"] == summary["consumed_permits"]
            and summary["provider_calls"] == summary["main_provider_calls"] + summary["compaction_provider_calls"]
            and summary["compaction_provider_calls"] <= 2
            and first_view["cost_status"] == second_cli_view["cost_status"] == "SETTLED"
            and summary["accepted_results"] == 2 and summary["eligible_conclusions"] == 2,
        }
        report["results"]["R3_submission_idempotency_and_scope"] = {
            "concurrent_same_key_same_task": len(first_ids) == 1,
            "completed_replay_same_task": replay["receipt"]["task_id"] == str(first_id),
            "completed_replay_provider_call_count_unchanged": replay_provider_count == first_provider_count,
            "same_key_different_question_conflict": different_question_conflict,
            "other_scope_lookup_denied": cross_scope_denied,
            "task_submission_count_for_scope": summary["submissions"],
            "passed": len(first_ids) == 1 and replay["receipt"]["task_id"] == str(first_id)
            and replay_provider_count == first_provider_count and different_question_conflict and cross_scope_denied
            and summary["submissions"] == 2,
        }

        stop.set()
        await asyncio.wait_for(worker_task, 10)

        async def late_result_scenario(mode: str) -> dict[str, object]:
            ask = await _cli(
                env, "ask", "--request-key", f"phase3b-{run_id}-{mode}", "--wait-seconds", "0",
                stdin=f"Save a result before {mode}.".encode(),
            )
            task_id = TaskId(str(ask["receipt"]["task_id"]))
            pending = await _dispatch_to_saved_result(
                factory, runtime, actor, execution_config, settings.worker_id, container, task_id,
            )
            if pending["execution_state"] != "QUIESCENT" or pending["processing_state"] != "WAITING_EXECUTION":
                raise AssertionError(f"{mode} scenario did not leave a saved result awaiting acceptance")
            provider_count_before_accept = fake.count()
            if mode == "cancel":
                await task_application.cancel(factory, actor, task_id, StopReason.USER_CANCELLED)
            else:
                await task_application.revise(
                    factory, actor, task_id, 1,
                    InputChange(text="Revised after the original output was saved.", expected_revision=1),
                )
            applied = await result_application.apply_turn_result(factory, str(pending["inbox_id"]))
            view = await task_application.get_task(factory, actor, task_id)
            async with engine.connect() as connection:
                result_state = await connection.scalar(text(
                    "SELECT processing_state FROM turn_results WHERE inbox_id=:inbox"
                ), {"inbox": pending["inbox_id"]})
                response_count = await connection.scalar(text(
                    "SELECT count(*) FROM task_responses WHERE task_id=:task"
                ), {"task": str(task_id)})
            return {
                "task_id": str(task_id), "saved_state_before_mutation": pending["processing_state"],
                "result_state_after_mutation": result_state, "task_state": view["state"],
                "input_revision": view["input_revision"], "response_count": response_count,
                "provider_calls_unchanged": fake.count() == provider_count_before_accept,
                "application_result": applied,
                "passed": result_state == "LATE" and response_count == 0
                and fake.count() == provider_count_before_accept
                and ((mode == "cancel" and view["state"] == "STOPPING")
                     or (mode == "revise" and view["input_revision"] == 2)),
            }

        report["results"]["R5_cancel_and_revision_gate"] = {
            "cancel": await late_result_scenario("cancel"),
            "revision": await late_result_scenario("revise"),
        }
        report["results"]["R5_cancel_and_revision_gate"]["passed"] = all(
            report["results"]["R5_cancel_and_revision_gate"][name]["passed"]
            for name in ("cancel", "revision")
        )

        recovery_ask = await _cli(env, "ask", "--request-key", f"phase3b-{run_id}-recovery", "--wait-seconds", "0", stdin=b"Persist before restart, then finish from the saved result.")
        recovery_id = TaskId(str(recovery_ask["receipt"]["task_id"]))
        pending_recovery = await _dispatch_to_saved_result(
            factory, runtime, actor, execution_config, settings.worker_id, container, recovery_id,
        )
        end = asyncio.get_running_loop().time() + 30
        while asyncio.get_running_loop().time() < end:
            async with engine.connect() as connection:
                state = await connection.scalar(text("SELECT execution_state FROM operations WHERE task_id=:task"), {"task": str(recovery_id)})
                stored_result = await connection.scalar(text("SELECT processing_state FROM turn_results WHERE inbox_id=:inbox"), {"inbox": pending_recovery["inbox_id"]})
            if state == "QUIESCENT" and stored_result == "WAITING_EXECUTION":
                break
            await asyncio.sleep(0.1)
        if state != "QUIESCENT" or stored_result != "WAITING_EXECUTION":
            raise AssertionError("saved result was not left pending after execution became terminal")
        provider_count_after_dispatch = fake.count()

        recovery_bridge = BridgeClient(node, ROOT / "bridge/letta/dist/main.js", env={
            "HEKATE_LETTA_URL": letta_url, "HEKATE_LETTA_TOKEN": private_token,
            "HEKATE_REQUIRE_PROVIDER_BINDING": "1",
        })
        recovery_runtime = LettaRuntimeAdapter(recovery_bridge)
        recovery_container = Container(settings=settings, runtime=recovery_runtime, uow_factory=factory, database=engine)
        recovery_stop = asyncio.Event()
        recovery_worker = asyncio.create_task(run_worker(recovery_container, recovery_stop))
        recovery_view = await _wait_for_state(factory, actor, recovery_id, {"COMPLETED", "FAILED"})
        recovery_stop.set()
        await asyncio.wait_for(recovery_worker, 10)
        await recovery_bridge.close()
        report["results"]["R6_saved_result_restart"] = {
            "task_id": str(recovery_id), "stored_state_before_restart": stored_result,
            "execution_state_before_restart": state, "task_state_after_restart": recovery_view["state"],
            "provider_calls_after_dispatch": provider_count_after_dispatch,
            "provider_calls_after_reprocess": fake.count(),
            "passed": stored_result == "WAITING_EXECUTION" and state == "QUIESCENT"
            and recovery_view["state"] == "COMPLETED" and fake.count() == provider_count_after_dispatch,
        }

        budget_config = replace(execution_config, task_budget_usd=Decimal("0.000001"))
        budget_receipt = await task_application.submit(
            factory, actor, UserMessage(text="This budget is too small to reserve a turn."),
            f"phase3b-{run_id}-budget", budget_config,
        )
        budget_task = TaskId(str(budget_receipt["task_id"]))
        provider_count_before_budget = fake.count()
        budget_leases = await prepare_queued_tasks(factory, runtime, actor, budget_config, settings.worker_id)
        budget_view = await task_application.get_task(factory, actor, budget_task)
        report["results"]["R7_budget_pre_dispatch"] = {
            "task_id": str(budget_task), "task_state": budget_view["state"],
            "stop_reason": budget_view["stop_reason"], "prepared_leases": len(budget_leases),
            "provider_calls_before": provider_count_before_budget, "provider_calls_after": fake.count(),
            "passed": budget_view["state"] == "FAILED" and budget_view["stop_reason"] == "BUDGET"
            and not budget_leases and fake.count() == provider_count_before_budget,
        }

        deadline_config = replace(execution_config, deadline_seconds=1)
        deadline_receipt = await task_application.submit(
            factory, actor, UserMessage(text="This Task should expire before preparation."),
            f"phase3b-{run_id}-deadline", deadline_config,
        )
        deadline_task = TaskId(str(deadline_receipt["task_id"]))
        await asyncio.sleep(1.05)
        provider_count_before_deadline = fake.count()
        session_prepares_before_deadline = session_prepare_count["count"]
        deadline_leases = await prepare_queued_tasks(factory, runtime, actor, deadline_config, settings.worker_id)
        deadline_view = await task_application.get_task(factory, actor, deadline_task)
        report["results"]["R7_deadline_pre_dispatch"] = {
            "task_id": str(deadline_task), "task_state": deadline_view["state"],
            "stop_reason": deadline_view["stop_reason"], "prepared_leases": len(deadline_leases),
            "session_prepares_before": session_prepares_before_deadline,
            "session_prepares_after": session_prepare_count["count"],
            "provider_calls_before": provider_count_before_deadline, "provider_calls_after": fake.count(),
            "passed": deadline_view["state"] == "FAILED" and deadline_view["stop_reason"] == "DEADLINE"
            and not deadline_leases and session_prepare_count["count"] == session_prepares_before_deadline
            and fake.count() == provider_count_before_deadline,
        }

        unknown_receipt = await task_application.submit(
            factory, actor, UserMessage(text="Create an operation with an injected unknown send outcome."),
            f"phase3b-{run_id}-unknown-source", execution_config,
        )
        unknown_task = TaskId(str(unknown_receipt["task_id"]))
        unknown_leases = await prepare_queued_tasks(factory, runtime, actor, execution_config, settings.worker_id)
        unknown_job = await p3.claim_one(factory, settings.worker_id)
        if len(unknown_leases) != 1 or unknown_job is None:
            raise AssertionError("UNKNOWN hold setup did not admit its source operation")
        await record_dispatch_send_intent(factory, unknown_job, settings.worker_id)
        raw_binding = unknown_job.payload["binding"]
        runtime_binding = RuntimeBinding(
            task_id=TaskId(str(raw_binding["task_id"])), attempt_id=AttemptId(str(raw_binding["attempt_id"])),
            agent_registry_id=RegistryId(str(raw_binding["agent_registry_id"])),
            provider_agent_id=ProviderAgentId(str(raw_binding["provider_agent_id"])),
            conversation_id=str(raw_binding["conversation_id"]), input_revision=int(raw_binding["input_revision"]),
            fence=int(raw_binding["fence"]),
        )
        unknown_identity = f"execution:{unknown_job.operation_id}:synthetic-disconnect"
        await process_runtime_observation(factory, "letta-bridge", unknown_identity, RuntimeInboxPayload(
            event_type="execution", operation_id=str(unknown_job.operation_id),
            accounting_call_id=f"execution:{unknown_job.operation_id}", source="bridge_disconnect",
            observation_identity=unknown_identity, binding=InboxBinding.from_binding(runtime_binding),
            state="UNKNOWN", reason="synthetic disconnect after durable send intent",
            lease_owner=settings.worker_id, observer_fence=runtime_binding.fence,
        ))
        async with factory() as uow:
            await uow.agents.release_lease(unknown_leases[0])
            await uow.commit()
        async with engine.connect() as connection:
            unknown_hold_state = await connection.scalar(text(
                "SELECT state FROM agent_execution_holds WHERE operation_id=:operation AND quiescent_at IS NULL"
            ), {"operation": str(unknown_job.operation_id)})
        unknown_task_view = await task_application.get_task(factory, actor, unknown_task)

        blocked_receipt = await task_application.submit(
            factory, actor, UserMessage(text="This must wait behind the unresolved HEKATE execution."),
            f"phase3b-{run_id}-unknown-blocked", execution_config,
        )
        blocked_task = TaskId(str(blocked_receipt["task_id"]))
        provider_count_before_hold = fake.count()
        session_prepares_before_hold = session_prepare_count["count"]
        blocked_leases = await prepare_queued_tasks(factory, runtime, actor, execution_config, settings.worker_id)
        blocked_view = await task_application.get_task(factory, actor, blocked_task)
        async with engine.connect() as connection:
            blocked_counts = (await connection.execute(text("""
                SELECT (SELECT count(*) FROM attempts WHERE task_id=:task) AS attempts,
                       (SELECT count(*) FROM operations WHERE task_id=:task) AS operations
            """), {"task": str(blocked_task)})).mappings().one()
        report["results"]["R7_unknown_hold_admission"] = {
            "source_task_id": str(unknown_task), "blocked_task_id": str(blocked_task),
            "unknown_hold_state": unknown_hold_state, "blocked_task_state": blocked_view["state"],
            "source_task_cost_status": unknown_task_view["cost_status"],
            "source_pending_reservations": unknown_task_view["pending_reservations"],
            "blocked_task_attempts_and_operations": dict(blocked_counts),
            "session_prepares_before": session_prepares_before_hold,
            "session_prepares_after": session_prepare_count["count"],
            "provider_calls_before": provider_count_before_hold, "provider_calls_after": fake.count(),
            "passed": unknown_hold_state == "UNKNOWN" and unknown_task_view["cost_status"] == "PENDING_SETTLEMENT"
            and unknown_task_view["pending_reservations"] == 1 and blocked_view["state"] == "QUEUED"
            and not blocked_leases and blocked_counts["attempts"] == 0 and blocked_counts["operations"] == 0
            and session_prepare_count["count"] == session_prepares_before_hold
            and fake.count() == provider_count_before_hold,
        }

        report["provider"] = {
            "mode": "isolated_loopback_fake_only", "real_provider_calls": 0,
            "forwarded_requests": fake.count(),
            "request_metadata": [
                {key: request.get(key) for key in ("sequence", "response_id", "model", "stream", "max_tokens", "max_completion_tokens", "behavior")}
                for request in fake.requests
            ],
        }
        report["overall_status"] = "pass" if all(
            value.get("passed") is True for value in report["results"].values() if isinstance(value, dict)
        ) else "blocked"
    except Exception as error:
        report["failure"] = {"type": type(error).__name__, "message": str(error)[:300]}
        if gateway_app is not None:
            report["gateway_metrics"] = dict(gateway_app.state.metrics)
        if fake is not None:
            report["fake_provider_requests"] = [
                {key: request.get(key) for key in ("sequence", "model", "behavior")}
                for request in fake.requests
            ]
        report["overall_status"] = "blocked"
    finally:
        if 'worker_task' in locals() and not worker_task.done():
            stop.set()
            await asyncio.gather(worker_task, return_exceptions=True)
        if 'recovery_worker' in locals() and not recovery_worker.done():
            recovery_stop.set()
            await asyncio.gather(recovery_worker, return_exceptions=True)
        if bridge is not None:
            await bridge.close()
        if gateway_server is not None:
            gateway_server.should_exit = True
            if gateway_task is not None:
                await asyncio.gather(gateway_task, return_exceptions=True)
        if fake is not None:
            fake.release_block.set()
            fake.stop()
        if sandbox is not None:
            sandbox.stop()
            try:
                sandbox.clear_state()
            except Exception as error:
                report["cleanup_error"] = type(error).__name__
        if engine is not None:
            try:
                async with engine.connect() as connection:
                    report["database_counts_after_run"] = {
                        "tasks": await connection.scalar(text("SELECT count(*) FROM tasks")),
                        "agents": await connection.scalar(text("SELECT count(*) FROM agent_registry")),
                        "conclusions": await connection.scalar(text("SELECT count(*) FROM conclusions")),
                        "responses": await connection.scalar(text("SELECT count(*) FROM task_responses")),
                        "provider_calls": await connection.scalar(text("SELECT count(*) FROM provider_calls")),
                        "consumed_permits": await connection.scalar(text("SELECT count(*) FROM call_permits WHERE state='CONSUMED'")),
                        "pending_inbox": await connection.scalar(text("SELECT count(*) FROM inbox WHERE processed_at IS NULL")),
                        "unknown_holds": await connection.scalar(text("SELECT count(*) FROM agent_execution_holds WHERE state='UNKNOWN' AND quiescent_at IS NULL")),
                    }
                    report["budget_summary"] = {
                        "synthetic": True,
                        "totals_by_account_kind": [dict(row) for row in (await connection.execute(text("""
                            SELECT scope_kind, count(*) AS accounts,
                                   coalesce(sum(limit_amount), 0) AS limit_amount,
                                   coalesce(sum(spent_amount), 0) AS spent_amount,
                                   coalesce(sum(held_amount), 0) AS held_amount
                            FROM budget_accounts GROUP BY scope_kind ORDER BY scope_kind
                        """))).mappings().all()],
                        "ledger_effect_counts": [dict(row) for row in (await connection.execute(text("""
                            SELECT effect_type, count(*) AS count FROM budget_ledger
                            GROUP BY effect_type ORDER BY effect_type
                        """))).mappings().all()],
                        "note": "TASK and SYSTEM accounts are separate ledger scopes; their totals are not additive.",
                    }
                    report["unresolved_provider_calls"] = [dict(row) for row in (await connection.execute(text("""
                        SELECT p.status AS call_state, cp.state AS permit_state,
                               u.completeness, u.settlement_state, count(*) AS count
                        FROM provider_calls p
                        JOIN call_permits cp ON cp.accounting_call_id=p.accounting_call_id
                        LEFT JOIN usage_projections u ON u.accounting_call_id=p.accounting_call_id
                        WHERE p.status <> 'QUIESCENT' OR u.settlement_state IS DISTINCT FROM 'SETTLED'
                        GROUP BY p.status, cp.state, u.completeness, u.settlement_state
                        ORDER BY p.status, cp.state, u.completeness, u.settlement_state
                    """))).mappings().all()]
                    report["turn_result_states"] = [dict(row) for row in (await connection.execute(text("""
                        SELECT processing_state, rejection_reason, count(*) AS count FROM turn_results
                        GROUP BY processing_state, rejection_reason ORDER BY processing_state, rejection_reason
                    """))).mappings().all()]
            except Exception as error:
                report["measurement_error"] = type(error).__name__
            await engine.dispose()
        temporary.cleanup()
        artifact.parent.mkdir(parents=True, exist_ok=True)
        report["executed_at_finished"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        report["artifact"] = artifact.relative_to(ROOT).as_posix()
        artifact.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.environ.get("HEKATE_TEST_DATABASE_URL", ""))
    parser.add_argument("--node-bin", default=os.environ.get("HEKATE_NODE_BIN", ""))
    parser.add_argument("--node-archive", type=Path, default=Path(os.environ.get("HEKATE_NODE_ARCHIVE", "/tmp/hekate-node-v22.19.0-linux-x64.tar.xz")))
    parser.add_argument("--image", default=f"hekate/letta-code-p1:{p3.p1.LOCK['app_server']['source_commit'][:8]}-{p3.p1.LOCK['patches'][0]['sha256'][:8]}")
    parser.add_argument("--artifact", type=Path)
    args = parser.parse_args()
    if not args.database_url or not args.node_bin:
        parser.error("set HEKATE_TEST_DATABASE_URL and HEKATE_NODE_BIN")
    os.environ["PATH"] = f"{Path(args.node_bin).resolve().parent}{os.pathsep}{os.environ.get('PATH', '')}"
    parsed = make_url(args.database_url)
    if parsed.database != "hekate_phase3_test" or parsed.host not in {"127.0.0.1", "localhost"}:
        parser.error("probe requires the dedicated loopback hekate_phase3_test database")
    run_id = f"p3b-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    artifact = args.artifact or ROOT / "integration/runtime/artifacts" / f"{run_id}.json"
    cfg = p3.Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", args.database_url.replace("%", "%%"))
    os.environ["HEKATE_DATABASE_URL"] = args.database_url
    p3.command.upgrade(cfg, "head")
    report = asyncio.run(_run(args.database_url, args.node_bin, args.image, args.node_archive, artifact, run_id))
    print(json.dumps({"artifact": artifact.relative_to(ROOT).as_posix(), "status": report["overall_status"], "real_provider_calls": 0}, sort_keys=True))
    return 0 if report["overall_status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
