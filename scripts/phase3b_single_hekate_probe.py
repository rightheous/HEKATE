from __future__ import annotations

import argparse
import asyncio
import hashlib
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
from hekate.application.turns import TURN_OUTPUT_POLICY, prepare_queued_tasks
from hekate.domain.capsules import export_schemas
from hekate.domain.bridge_contracts import MAX_BRIDGE_FRAME_BYTES
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
from hekate.worker import service as worker_service


def _output_from_request(
    request: dict[str, object], observations: list[dict[str, object]] | None = None,
) -> str:
    def strings(value: object):
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for item in value.values():
                yield from strings(item)
        elif isinstance(value, list):
            for item in value:
                yield from strings(item)

    request_text = tuple(strings(request.get("messages")))
    prompt = next((item for item in request_text if "Task Capsule JSON:\n" in item), None)
    if prompt is None:
        if any("HEKATE server output contract" in item for item in request_text):
            raise ValueError("HEKATE turn request is missing its Task Capsule")
        return "Fake-only context compression."
    try:
        schema_text = prompt.split(
            "HEKATE server output contract (generated from the strict Python HekateTurnOutput model):\n", 1,
        )[1].split("\n\nServer output policy:\n", 1)[0]
        schema = json.loads(schema_text)
        expected_schema = export_schemas()["hekate-turn-output.v1.schema.json"]
        request_schema_hash = canonical_json_hash(schema)
        server_schema_hash = canonical_json_hash(expected_schema)
        if request_schema_hash != server_schema_hash:
            raise ValueError("provider request schema differs from generated HekateTurnOutput schema")
        required = set(schema.get("required", []))
        if not {"schema_version", "proposal", "conclusion"} <= required:
            raise ValueError("provider request schema omits required outer fields")
        resolved_refs: set[str] = set()

        def resolve_refs(value: object) -> None:
            if isinstance(value, dict):
                ref = value.get("$ref")
                if isinstance(ref, str):
                    if not ref.startswith("#/"):
                        raise ValueError("provider request schema has a non-local reference")
                    target: object = schema
                    for part in ref[2:].split("/"):
                        target = target[part]
                    if not isinstance(target, dict):
                        raise ValueError("provider request schema reference is not a definition")
                    resolved_refs.add(ref)
                for child in value.values():
                    resolve_refs(child)
            elif isinstance(value, list):
                for child in value:
                    resolve_refs(child)

        resolve_refs(schema)
        if not resolved_refs or not isinstance(schema.get("$defs"), dict):
            raise ValueError("provider request schema omits nested definitions")
        capsule, _ = json.JSONDecoder().raw_decode(prompt.split("Task Capsule JSON:\n", 1)[1].lstrip())
    except (IndexError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("provider request is missing generated output schema or Task Capsule") from error
    if TURN_OUTPUT_POLICY not in prompt:
        raise ValueError("provider request is missing server output policy")
    supported_actions = ["answer", "request_information", "abstain", "commit"]
    if any(action not in TURN_OUTPUT_POLICY for action in supported_actions):
        raise ValueError("provider request policy omits a supported proposal action")
    match = re.search(
        r"Trusted runtime binding: task_id=([^;]+); attempt_id=([^;]+); agent_registry_id=([^;]+); input_revision=(\d+)\.",
        prompt,
    )
    if match is None:
        raise ValueError("provider request is missing trusted Task, attempt, and RegistryId context")
    task_id, attempt_id, registry_id, revision = match.groups()
    if (task_id, attempt_id, int(revision)) != (
        capsule.get("task_id"), capsule.get("attempt_id"), capsule.get("input_revision"),
    ):
        raise ValueError("trusted runtime binding differs from Task Capsule")
    if "provider_agent_id=" in prompt:
        raise ValueError("provider ID must not replace the trusted RegistryId")
    if observations is not None:
        observations.append({
            "request_hash": hashlib.sha256(json.dumps(
                request, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")).hexdigest(),
            "prompt_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "provider_request_schema_hash": request_schema_hash,
            "server_schema_hash": server_schema_hash,
            "schema_hash_matches_server": request_schema_hash == server_schema_hash,
            "server_policy_hash": hashlib.sha256(TURN_OUTPUT_POLICY.encode("utf-8")).hexdigest(),
            "supported_actions": supported_actions,
            "required_outer_fields": sorted(required),
            "resolved_local_refs": sorted(resolved_refs),
            "definition_count": len(schema["$defs"]),
            "task_id": task_id,
            "attempt_id": attempt_id,
            "agent_registry_id": registry_id,
            "input_revision": int(revision),
            "server_policy_present": True,
            "unicode_prompt_characters": len(prompt),
            "unicode_prompt_bytes": len(prompt.encode("utf-8")),
        })
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


async def _dispatch_to_saved_result(
    factory, runtime, actor, config, worker, container, task_id: TaskId, *, defer_terminal: bool = False,
) -> dict[str, object]:
    leases = await prepare_queued_tasks(factory, runtime, actor, config, worker)
    job = await p3.claim_one(factory, worker)
    if len(leases) != 1 or job is None:
        raise AssertionError("scenario did not create one ordinary admitted outbox operation")
    deferred_observation: list[tuple[tuple[object, ...], dict[str, object]]] = []
    original_observation = worker_service.process_runtime_observation
    if defer_terminal:
        async def hold_terminal(*args, **kwargs):
            payload = args[3]
            if payload.event_type == "execution" and payload.state == "QUIESCENT":
                deferred_observation.append((args, kwargs))
                return {"processed": False, "pending": True}
            return await original_observation(*args, **kwargs)

        worker_service.process_runtime_observation = hold_terminal
    try:
        await dispatch_job(container, job, worker)
    finally:
        worker_service.process_runtime_observation = original_observation
        if not defer_terminal:
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
    result = dict(row)
    if defer_terminal:
        if len(deferred_observation) != 1:
            raise AssertionError("dispatch did not produce exactly one deferred terminal observation")
        result["_deferred_terminal_observation"] = deferred_observation[0]
    return result


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


async def _accounting_effects(engine, task_id: TaskId) -> dict[str, int]:
    async with engine.connect() as connection:
        row = (await connection.execute(text("""
            SELECT (SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.task_id=:task) AS provider_calls,
                   (SELECT count(*) FROM call_permits cp JOIN provider_calls p USING (accounting_call_id)
                     JOIN operations o ON o.id=p.operation_id WHERE o.task_id=:task) AS permits,
                   (SELECT count(*) FROM call_permits cp JOIN provider_calls p USING (accounting_call_id)
                     JOIN operations o ON o.id=p.operation_id WHERE o.task_id=:task AND cp.state='CONSUMED') AS consumed_permits,
                   (SELECT count(*) FROM budget_ledger l JOIN budget_reservations r ON r.id=l.reservation_id
                     JOIN operations o ON o.id=r.operation_id WHERE o.task_id=:task) AS ledger_entries
        """), {"task": str(task_id)})).mappings().one()
    return {key: int(row[key]) for key in row.keys()}


def _effect_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {key: after[key] - before[key] for key in before}


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
    start_commit = p3.p1.run(["git", "rev-parse", "HEAD"])
    working_tree_changes = p3.p1.run(["git", "status", "--porcelain"]).splitlines()
    report: dict[str, object] = {
        "schema_version": "1", "probe": "phase3b-single-hekate", "run_id": run_id,
        "executed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "base_commit": start_commit, "start_commit_sha": start_commit,
        "branch": branch, "working_tree_dirty_at_probe": bool(working_tree_changes),
        "working_tree_change_count_at_probe": len(working_tree_changes),
        "real_provider_calls": 0, "results": {}, "overall_status": "blocked",
        "limitations": [
            "The provider route is an isolated synthetic loopback fake; no production provider was configured.",
            "Production dispatch remains closed because no verified real model, full-request tokenizer, pricing, or production route was configured.",
            "G7 same-execution resume and G8 exact full-request tokenization remain unresolved.",
            "Successful contract validation is not a claim of factual correctness; answers are provisional.",
            "HekateTurnOutput schema and policy are carried in the forwarded turn message; SDK-native structured output is not used or claimed.",
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
        if migration_head != "0007_phase5a_critic_workflows":
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
        turn_contract_observations: list[dict[str, object]] = []
        fake.set_response_factory(lambda request: _output_from_request(request, turn_contract_observations))
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
        task_ids_for_contract = {str(first_id), str(second_id)}
        accepted_contracts = {
            task_id: next((item for item in turn_contract_observations if item["task_id"] == task_id), None)
            for task_id in task_ids_for_contract
        }
        server_output_schema_hash = canonical_json_hash(export_schemas()["hekate-turn-output.v1.schema.json"])
        server_policy_hash = hashlib.sha256(TURN_OUTPUT_POLICY.encode("utf-8")).hexdigest()
        actual_request_hashes = {request["request_hash"] for request in fake.requests}
        report["results"]["R2_single_persistent_hekate"] = {
            "task_ids": [str(first_id), str(second_id)], "registry_provider_counts": dict(reuse),
            "database_counts": dict(summary), "accepted_result_chain": [dict(row) for row in identity_chain],
            "submission_hashes": [dict(row) for row in request_hashes],
            "request_key_hashes": [canonical_json_hash(key) for key in (request_key, f"phase3b-{run_id}-second")],
            "first_task_state": first_view["state"],
            "second_task_state": second_view["state"], "accepted_responses_read_from_cli_and_db": bool(first_answer and second_answer),
            "task_cost_statuses": [first_view["cost_status"], second_cli_view["cost_status"]],
            "input_output_token_caps": {
                "max_input_tokens": execution_config.max_input_tokens,
                "max_output_tokens": execution_config.max_output_tokens,
                "compaction_call_cap": execution_config.max_compaction_calls,
            },
            "one_main_call_each": summary["main_provider_calls"] == 2,
            "provider_request_contracts": accepted_contracts,
            "provider_request_hashes_match_fake_upstream_capture": all(
                item is not None and item["request_hash"] in actual_request_hashes
                for item in accepted_contracts.values()
            ),
            "output_schema_hash": server_output_schema_hash,
            "two_task_requests_included_generated_schema_and_policy": all(accepted_contracts.values())
            and all(
                item["provider_request_schema_hash"] == server_output_schema_hash
                and item["server_schema_hash"] == server_output_schema_hash
                and item["schema_hash_matches_server"]
                and item["server_policy_hash"] == server_policy_hash
                and item["supported_actions"] == ["answer", "request_information", "abstain", "commit"]
                and item["server_policy_present"]
                and {"schema_version", "proposal", "conclusion"} <= set(item["required_outer_fields"])
                and bool(item["resolved_local_refs"])
                and item["task_id"] == accepted_task_id
                for accepted_task_id, item in accepted_contracts.items()
            ),
            "passed": first_view["state"] == "COMPLETED" and second_cli_view["state"] == "COMPLETED"
            and first_answer and second_answer and reuse["registries"] == 1 and reuse["providers"] == 1
            and reuse["operations"] == 2 and reuse["attempts"] == 2 and reuse["responses"] == 2
            and summary["main_provider_calls"] == 2 and summary["provider_calls"] == summary["consumed_permits"]
            and summary["provider_calls"] == summary["main_provider_calls"] + summary["compaction_provider_calls"]
            and summary["compaction_provider_calls"] == 0
            and first_view["cost_status"] == second_cli_view["cost_status"] == "SETTLED"
            and summary["accepted_results"] == 2 and summary["eligible_conclusions"] == 2,
        }
        report["results"]["R2_single_persistent_hekate"]["passed"] = (
            report["results"]["R2_single_persistent_hekate"]["passed"]
            and report["results"]["R2_single_persistent_hekate"]["two_task_requests_included_generated_schema_and_policy"]
            and report["results"]["R2_single_persistent_hekate"]["provider_request_hashes_match_fake_upstream_capture"]
        )
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

        oversized_receipt = await task_application.submit(
            factory, actor, UserMessage(text="x" * 65_536),
            f"phase3b-{run_id}-oversized-frame", execution_config,
        )
        oversized_task = TaskId(str(oversized_receipt["task_id"]))
        calls_before_size_rejection = fake.count()
        oversized_leases = await prepare_queued_tasks(factory, runtime, actor, execution_config, settings.worker_id)
        oversized_view = await task_application.get_task(factory, actor, oversized_task)
        size_rejected = oversized_view["state"] == "FAILED" and oversized_view["stop_reason"] == "ERROR"
        async with engine.connect() as connection:
            size_counts = (await connection.execute(text("""
                SELECT (SELECT count(*) FROM operations WHERE task_id=:task) AS operations,
                       (SELECT count(*) FROM provider_calls WHERE operation_id IN
                         (SELECT id FROM operations WHERE task_id=:task)) AS provider_calls,
                       (SELECT count(*) FROM call_permits WHERE accounting_call_id IN
                         (SELECT accounting_call_id FROM provider_calls WHERE operation_id IN
                           (SELECT id FROM operations WHERE task_id=:task))) AS permits
            """), {"task": str(oversized_task)})).mappings().one()
        report["results"]["C1_oversized_turn_rejected_before_admission"] = {
            "task_id": str(oversized_task), "question_utf8_bytes": 65_536,
            "question_utf8_byte_limit": 65_536,
            "final_message_character_limit": 65_536,
            "bridge_frame_byte_limit_including_newline": MAX_BRIDGE_FRAME_BYTES,
            "message_character_cap_rejected": size_rejected,
            "task_state": oversized_view["state"], "stop_reason": oversized_view["stop_reason"],
            "cost_status": oversized_view["cost_status"],
            "prepared_leases": len(oversized_leases),
            "operations_provider_calls_and_permits": dict(size_counts),
            "provider_calls_before": calls_before_size_rejection, "provider_calls_after": fake.count(),
            "passed": size_rejected and size_counts["operations"] == 0
            and size_counts["provider_calls"] == 0 and size_counts["permits"] == 0
            and not oversized_leases and fake.count() == calls_before_size_rejection,
        }

        async def late_result_scenario(mode: str) -> dict[str, object]:
            ask = await _cli(
                env, "ask", "--request-key", f"phase3b-{run_id}-{mode}", "--wait-seconds", "0",
                stdin=f"Save a result before {mode}.".encode(),
            )
            task_id = TaskId(str(ask["receipt"]["task_id"]))
            pending = await _dispatch_to_saved_result(
                factory, runtime, actor, execution_config, settings.worker_id, container, task_id,
                defer_terminal=mode == "cancel_before_terminal",
            )
            expected_execution = "RUNNING" if mode == "cancel_before_terminal" else "QUIESCENT"
            if pending["execution_state"] != expected_execution or pending["processing_state"] != "WAITING_EXECUTION":
                raise AssertionError(f"{mode} scenario did not leave a saved result awaiting acceptance")
            provider_count_before_accept = fake.count()
            effects_before_cancel = await _accounting_effects(engine, task_id)
            effects_before_acceptance = effects_before_cancel
            effects_after_cancel = effects_before_cancel
            cancel_view = None
            cancel_replay = None
            task_view_before_cancel = None
            task_view_after_cancel = None
            before_terminal_state = None
            sequence = ["task_submitted", "turn_dispatched_and_business_result_saved"]
            sequence.append("execution_terminal_pending" if mode == "cancel_before_terminal" else "execution_terminal_confirmed")
            if mode in {"cancel", "cancel_before_terminal"}:
                task_view_before_cancel = await task_application.get_task(factory, actor, task_id)
                cancel_view = await task_application.cancel(factory, actor, task_id, StopReason.USER_CANCELLED)
                before_terminal_state = cancel_view["state"]
                task_view_after_cancel = await task_application.get_task(factory, actor, task_id)
                effects_after_cancel = await _accounting_effects(engine, task_id)
                sequence.append("cancel_requested")
                if mode == "cancel_before_terminal":
                    terminal_args, terminal_kwargs = pending["_deferred_terminal_observation"]
                    terminal_payload = terminal_args[3]
                    async with factory() as uow:
                        lease = await uow.agents.acquire_lease(
                            RegistryId(terminal_payload.binding.agent_registry_id), settings.worker_id, 45,
                        )
                        await uow.commit()
                    if lease is None:
                        raise AssertionError("could not reacquire registry lease for deferred terminal evidence")
                    try:
                        await process_runtime_observation(*terminal_args, **terminal_kwargs)
                    finally:
                        async with factory() as uow:
                            await uow.agents.release_lease(lease)
                            await uow.commit()
                    sequence.append("execution_terminal_confirmed")
                    effects_before_acceptance = await _accounting_effects(engine, task_id)
                cancel_replay = await task_application.cancel(factory, actor, task_id, StopReason.USER_CANCELLED)
                sequence.append("cancel_replayed")
            else:
                await task_application.revise(
                    factory, actor, task_id, 1,
                    InputChange(text="Revised after the original output was saved.", expected_revision=1),
                )
                sequence.extend(("input_revision_changed",))
            async with engine.connect() as connection:
                execution_state_after_terminal = await connection.scalar(text(
                    "SELECT execution_state FROM operations WHERE task_id=:task"
                ), {"task": str(task_id)})
            applied = await result_application.apply_turn_result(factory, str(pending["inbox_id"]))
            sequence.append("business_result_applied")
            view = await task_application.get_task(factory, actor, task_id)
            async with engine.connect() as connection:
                result_row = (await connection.execute(text(
                    "SELECT processing_state, rejection_reason, conclusion_id, output_hash FROM turn_results WHERE inbox_id=:inbox"
                ), {"inbox": pending["inbox_id"]})).mappings().one()
                result_state = result_row["processing_state"]
                response_count = await connection.scalar(text(
                    "SELECT count(*) FROM task_responses WHERE task_id=:task"
                ), {"task": str(task_id)})
                conclusion_eligible = await connection.scalar(text(
                    "SELECT eligible FROM conclusions WHERE id=:id"
                ), {"id": result_row["conclusion_id"]})
                terminal_audits = await connection.scalar(text(
                    "SELECT count(*) FROM audit_events WHERE task_id=:task AND event_kind='task.execution_cancelled'"
                ), {"task": str(task_id)})
                cancel_audits = await connection.scalar(text(
                    "SELECT count(*) FROM audit_events WHERE task_id=:task AND event_kind='task.cancel_requested'"
                ), {"task": str(task_id)})
                late_audit = (await connection.execute(text(
                    "SELECT safe_payload FROM audit_events WHERE task_id=:task AND event_kind='business_result.late' ORDER BY id LIMIT 1"
                ), {"task": str(task_id)})).scalar_one_or_none()
                audit_rows = (await connection.execute(text(
                    "SELECT event_kind, count(*) AS count FROM audit_events WHERE task_id=:task "
                    "AND event_kind IN ('task.cancel_requested','task.execution_cancelled','business_result.late') "
                    "GROUP BY event_kind"
                ), {"task": str(task_id)})).mappings().all()
            audit_counts_before_replay = {row["event_kind"]: int(row["count"]) for row in audit_rows}
            pending_execution_replay = await task_application.process_pending_execution_tasks(factory)
            pending_result_replay = await result_application.process_pending_results(factory)
            sequence.append("pending_terminal_scans_replayed_noop")
            replay = await result_application.apply_turn_result(factory, str(pending["inbox_id"]))
            sequence.append("business_result_replayed")
            effects_after_replay = await _accounting_effects(engine, task_id)
            provider_calls_after_replay = fake.count()
            async with engine.connect() as connection:
                audit_rows = (await connection.execute(text(
                    "SELECT event_kind, count(*) AS count FROM audit_events WHERE task_id=:task "
                    "AND event_kind IN ('task.cancel_requested','task.execution_cancelled','business_result.late') "
                    "GROUP BY event_kind"
                ), {"task": str(task_id)})).mappings().all()
            audit_counts_after_replay = {row["event_kind"]: int(row["count"]) for row in audit_rows}
            return {
                "sequence": sequence,
                "task_id": str(task_id), "saved_state_before_mutation": pending["processing_state"],
                "execution_state_before_cancel": expected_execution,
                "execution_state_after_terminal": execution_state_after_terminal,
                "task_state_immediately_after_cancel": before_terminal_state,
                "result_state_after_mutation": result_state, "result_rejection_reason": result_row["rejection_reason"],
                "task_state": view["state"], "task_outcome": view.get("outcome"),
                "task_stop_reason": view.get("stop_reason"), "conclusion_eligible": conclusion_eligible,
                "terminal_audits": terminal_audits, "cancel_audits": cancel_audits,
                "cancel_replay": cancel_replay,
                "cost_status_before_cancel": task_view_before_cancel["cost_status"] if task_view_before_cancel else None,
                "cost_status_after_cancel": task_view_after_cancel["cost_status"] if task_view_after_cancel else None,
                "pending_provider_calls_before_cancel": task_view_before_cancel["pending_provider_calls"] if task_view_before_cancel else None,
                "pending_provider_calls_after_cancel": task_view_after_cancel["pending_provider_calls"] if task_view_after_cancel else None,
                "pending_reservations_before_cancel": task_view_before_cancel["pending_reservations"] if task_view_before_cancel else None,
                "pending_reservations_after_cancel": task_view_after_cancel["pending_reservations"] if task_view_after_cancel else None,
                "accounting_before_cancel": effects_before_cancel,
                "accounting_after_cancel": effects_after_cancel,
                "accounting_delta_from_cancel": _effect_delta(effects_before_cancel, effects_after_cancel),
                "accounting_before_acceptance": effects_before_acceptance,
                "accounting_after_result_replay": effects_after_replay,
                "accounting_delta_from_result_replay": _effect_delta(effects_before_acceptance, effects_after_replay),
                "result_replay": replay,
                "pending_execution_replay_count": pending_execution_replay,
                "pending_result_replay_count": pending_result_replay,
                "late_audit_has_output_hash": isinstance(late_audit, dict) and late_audit.get("output_hash") == result_row["output_hash"],
                "audit_counts_before_replay": audit_counts_before_replay,
                "audit_counts_after_replay": audit_counts_after_replay,
                "input_revision": view["input_revision"], "response_count": response_count,
                "provider_requests_before_acceptance": provider_count_before_accept,
                "provider_requests_after_replay": provider_calls_after_replay,
                "provider_request_delta_after_replay": provider_calls_after_replay - provider_count_before_accept,
                "application_result": applied,
                "passed": result_state == "LATE" and response_count == 0
                and conclusion_eligible is False
                and terminal_audits == (0 if mode == "revise" else 1)
                and cancel_audits == (0 if mode == "revise" else 1)
                and execution_state_after_terminal == "QUIESCENT"
                and (mode == "revise" or (
                    task_view_before_cancel["cost_status"] == task_view_after_cancel["cost_status"]
                    and task_view_before_cancel["pending_provider_calls"] == task_view_after_cancel["pending_provider_calls"]
                    and task_view_before_cancel["pending_reservations"] == task_view_after_cancel["pending_reservations"]
                ))
                and effects_before_cancel == effects_after_cancel
                and effects_before_acceptance == effects_after_replay
                and pending_execution_replay == 0 and pending_result_replay == 0
                and audit_counts_before_replay == audit_counts_after_replay
                and late_audit is not None and late_audit.get("output_hash") == result_row["output_hash"]
                and replay.get("state") == "LATE"
                and provider_calls_after_replay == provider_count_before_accept
                and (mode == "revise" or cancel_replay.get("state") == "CANCELLED")
                and ((mode in {"cancel", "cancel_before_terminal"}
                      and view["state"] == "CANCELLED" and view.get("outcome") == "CANCELLED"
                      and view.get("stop_reason") == "USER_CANCELLED"
                      and (mode != "cancel" or before_terminal_state == "CANCELLED")
                      and (mode != "cancel_before_terminal" or before_terminal_state == "STOPPING"))
                     or (mode == "revise" and view["input_revision"] == 2)),
            }

        report["results"]["R5_cancel_and_revision_gate"] = {
            "cancel": await late_result_scenario("cancel"),
            "cancel_before_terminal": await late_result_scenario("cancel_before_terminal"),
            "revision": await late_result_scenario("revise"),
        }
        report["results"]["R5_cancel_and_revision_gate"]["passed"] = all(
            report["results"]["R5_cancel_and_revision_gate"][name]["passed"]
            for name in ("cancel", "cancel_before_terminal", "revision")
        )

        recovery_ask = await _cli(env, "ask", "--request-key", f"phase3b-{run_id}-recovery", "--wait-seconds", "0", stdin=b"Persist before restart, then finish from the saved result.")
        recovery_id = TaskId(str(recovery_ask["receipt"]["task_id"]))
        pending_recovery = await _dispatch_to_saved_result(
            factory, runtime, actor, execution_config, settings.worker_id, container, recovery_id,
        )
        async with engine.begin() as connection:
            await connection.execute(text(
                "UPDATE tasks SET deadline=now() - interval '1 second' WHERE id=:task"
            ), {"task": str(recovery_id)})
            await connection.execute(text(
                "UPDATE turn_results SET next_attempt_at=now() WHERE inbox_id=:inbox"
            ), {"inbox": pending_recovery["inbox_id"]})
        async with engine.connect() as connection:
            state = await connection.scalar(text("SELECT execution_state FROM operations WHERE task_id=:task"), {"task": str(recovery_id)})
            stored_result = await connection.scalar(text("SELECT processing_state FROM turn_results WHERE inbox_id=:inbox"), {"inbox": pending_recovery["inbox_id"]})
        if state != "QUIESCENT" or stored_result != "WAITING_EXECUTION":
            raise AssertionError("saved result was not left pending after execution became terminal")
        task_view_before_restart = await task_application.get_task(factory, actor, recovery_id)
        provider_count_after_dispatch = fake.count()
        accounting_before_restart = await _accounting_effects(engine, recovery_id)

        recovery_bridge = BridgeClient(node, ROOT / "bridge/letta/dist/main.js", env={
            "HEKATE_LETTA_URL": letta_url, "HEKATE_LETTA_TOKEN": private_token,
            "HEKATE_REQUIRE_PROVIDER_BINDING": "1",
        })
        recovery_runtime = LettaRuntimeAdapter(recovery_bridge)
        recovery_container = Container(settings=settings, runtime=recovery_runtime, uow_factory=factory, database=engine)
        recovery_stop = asyncio.Event()
        recovered_late_result = asyncio.Event()
        original_pending_results = worker_service.process_pending_results

        async def signal_recovered_result(*args, **kwargs):
            processed = await original_pending_results(*args, **kwargs)
            async with engine.connect() as connection:
                recovered_state = await connection.scalar(text(
                    "SELECT processing_state FROM turn_results WHERE inbox_id=:inbox"
                ), {"inbox": pending_recovery["inbox_id"]})
            if recovered_state == "LATE":
                recovered_late_result.set()
            return processed

        worker_service.process_pending_results = signal_recovered_result
        recovery_worker = asyncio.create_task(run_worker(recovery_container, recovery_stop))
        try:
            await asyncio.wait_for(recovered_late_result.wait(), 30)
        finally:
            worker_service.process_pending_results = original_pending_results
            recovery_stop.set()
            await asyncio.wait_for(recovery_worker, 10)
            await recovery_bridge.close()
        recovery_view = await task_application.get_task(factory, actor, recovery_id)
        async with engine.connect() as connection:
            recovery_result = (await connection.execute(text(
                "SELECT processing_state, rejection_reason, conclusion_id, output_hash, operation_id, attempt_id, "
                "input_revision, registry_id "
                "FROM turn_results WHERE inbox_id=:inbox"
            ), {"inbox": pending_recovery["inbox_id"]})).mappings().one()
            raw_output = await connection.scalar(text(
                "SELECT payload #>> '{business_result,raw_output}' FROM inbox WHERE id=:inbox"
            ), {"inbox": pending_recovery["inbox_id"]})
            late_audit_payload = (await connection.execute(text(
                "SELECT safe_payload FROM audit_events WHERE task_id=:task AND event_kind='business_result.late' "
                "ORDER BY id DESC LIMIT 1"
            ), {"task": str(recovery_id)})).scalar_one_or_none()
            recovery_response_count = await connection.scalar(text(
                "SELECT count(*) FROM task_responses WHERE task_id=:task"
            ), {"task": str(recovery_id)})
            recovery_conclusion_eligible = await connection.scalar(text(
                "SELECT eligible FROM conclusions WHERE id=:id"
            ), {"id": recovery_result["conclusion_id"]})
            audit_rows = (await connection.execute(text(
                "SELECT event_kind, count(*) AS count FROM audit_events WHERE task_id=:task "
                "AND event_kind IN ('task.execution_deadline_failed','business_result.late','task.result_accepted') "
                "GROUP BY event_kind"
            ), {"task": str(recovery_id)})).mappings().all()
        audit_counts_before_replay = {row["event_kind"]: int(row["count"]) for row in audit_rows}
        accounting_after_restart = await _accounting_effects(engine, recovery_id)
        provider_calls_after_restart = fake.count()
        replay = await result_application.apply_turn_result(factory, str(pending_recovery["inbox_id"]))
        pending_execution_replay = await task_application.process_pending_execution_tasks(factory)
        pending_result_replay = await result_application.process_pending_results(factory)
        recovery_view_after_replay = await task_application.get_task(factory, actor, recovery_id)
        accounting_after_replay = await _accounting_effects(engine, recovery_id)
        provider_calls_after_replay = fake.count()
        async with engine.connect() as connection:
            replay_result = (await connection.execute(text(
                "SELECT processing_state, rejection_reason, output_hash FROM turn_results WHERE inbox_id=:inbox"
            ), {"inbox": pending_recovery["inbox_id"]})).mappings().one()
            response_count_after_replay = await connection.scalar(text(
                "SELECT count(*) FROM task_responses WHERE task_id=:task"
            ), {"task": str(recovery_id)})
            conclusion_eligible_after_replay = await connection.scalar(text(
                "SELECT eligible FROM conclusions WHERE id=:id"
            ), {"id": recovery_result["conclusion_id"]})
            replay_audit_rows = (await connection.execute(text(
                "SELECT event_kind, count(*) AS count FROM audit_events WHERE task_id=:task "
                "AND event_kind IN ('task.execution_deadline_failed','business_result.late','task.result_accepted') "
                "GROUP BY event_kind"
            ), {"task": str(recovery_id)})).mappings().all()
        audit_counts_after_replay = {row["event_kind"]: int(row["count"]) for row in replay_audit_rows}
        raw_output_hash_matches = isinstance(raw_output, str) and hashlib.sha256(
            raw_output.encode("utf-8")
        ).hexdigest() == recovery_result["output_hash"]
        late_audit_matches_result = isinstance(late_audit_payload, dict) and (
            late_audit_payload.get("output_hash") == recovery_result["output_hash"]
            and late_audit_payload.get("rejection_reason") == "deadline_elapsed"
        )
        c3_sequence = [
            "task_submitted", "provider_turn_dispatched_and_business_result_saved",
            "operation_execution_quiescent", "deadline_expired_by_test_injection",
            "new_worker_started", "pending_execution_and_result_processed",
            "late_result_replayed", "pending_scans_replayed_noop",
        ]
        report["results"]["C3_deadline_after_execution_restart"] = {
            "sequence": c3_sequence,
            "task_id": str(recovery_id), "stored_state_before_restart": stored_result,
            "execution_state_before_restart": state, "task_state_after_restart": recovery_view["state"],
            "task_state_after_replay": recovery_view_after_replay["state"],
            "task_outcome": recovery_view["outcome"], "task_stop_reason": recovery_view["stop_reason"],
            "result_state": recovery_result["processing_state"],
            "result_rejection_reason": recovery_result["rejection_reason"],
            "result_state_after_replay": replay_result["processing_state"],
            "result_rejection_reason_after_replay": replay_result["rejection_reason"],
            "result_provenance": {
                "inbox_id": str(pending_recovery["inbox_id"]),
                "operation_id": str(recovery_result["operation_id"]),
                "attempt_id": str(recovery_result["attempt_id"]),
                "registry_id": str(recovery_result["registry_id"]),
                "input_revision": recovery_result["input_revision"],
                "output_hash": recovery_result["output_hash"],
                "raw_output_hash_matches": raw_output_hash_matches,
                "late_audit_matches_output_hash_and_reason": late_audit_matches_result,
            },
            "conclusion_eligible": recovery_conclusion_eligible,
            "conclusion_eligible_after_replay": conclusion_eligible_after_replay,
            "response_count": recovery_response_count,
            "response_count_after_replay": response_count_after_replay,
            "cost_state_before_restart": {
                key: task_view_before_restart[key]
                for key in ("cost_status", "pending_provider_calls", "pending_reservations")
            },
            "cost_state_after_restart": {
                key: recovery_view[key]
                for key in ("cost_status", "pending_provider_calls", "pending_reservations")
            },
            "cost_state_after_replay": {
                key: recovery_view_after_replay[key]
                for key in ("cost_status", "pending_provider_calls", "pending_reservations")
            },
            "accounting_before_restart": dict(accounting_before_restart),
            "accounting_after_restart": dict(accounting_after_restart),
            "accounting_after_replay": dict(accounting_after_replay),
            "accounting_delta_after_restart": _effect_delta(dict(accounting_before_restart), dict(accounting_after_restart)),
            "accounting_delta_after_replay": _effect_delta(dict(accounting_after_restart), dict(accounting_after_replay)),
            "provider_calls_after_dispatch": provider_count_after_dispatch,
            "provider_calls_after_restart": provider_calls_after_restart,
            "provider_calls_after_replay": provider_calls_after_replay,
            "provider_call_delta_after_restart": provider_calls_after_restart - provider_count_after_dispatch,
            "provider_call_delta_after_replay": provider_calls_after_replay - provider_calls_after_restart,
            "result_replay": replay,
            "pending_execution_replay_count": pending_execution_replay,
            "pending_result_replay_count": pending_result_replay,
            "audit_counts_before_replay": audit_counts_before_replay,
            "audit_counts_after_replay": audit_counts_after_replay,
            "passed": stored_result == "WAITING_EXECUTION" and state == "QUIESCENT"
            and recovery_view["state"] == "FAILED" and recovery_view["outcome"] == "FAILED"
            and recovery_view["stop_reason"] == "DEADLINE"
            and recovery_result["processing_state"] == "LATE"
            and recovery_result["rejection_reason"] == "deadline_elapsed"
            and replay_result["processing_state"] == "LATE"
            and replay_result["rejection_reason"] == "deadline_elapsed"
            and recovery_conclusion_eligible is False and conclusion_eligible_after_replay is False
            and recovery_response_count == 0 and response_count_after_replay == 0
            and dict(accounting_before_restart) == dict(accounting_after_restart)
            and dict(accounting_after_restart) == dict(accounting_after_replay)
            and audit_counts_before_replay == audit_counts_after_replay
            and raw_output_hash_matches and late_audit_matches_result
            and recovery_view_after_replay["state"] == "FAILED"
            and all(
                task_view_before_restart[key] == recovery_view[key] == recovery_view_after_replay[key]
                for key in ("cost_status", "pending_provider_calls", "pending_reservations")
            )
            and pending_execution_replay == 0 and pending_result_replay == 0
            and replay.get("state") == "LATE"
            and provider_calls_after_restart == provider_count_after_dispatch
            and provider_calls_after_replay == provider_calls_after_restart,
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
        provider_calls_before_unknown_cancel = fake.count()
        unknown_accounting_before_cancel = await _accounting_effects(engine, unknown_task)
        unknown_cancel_view = await task_application.cancel(
            factory, actor, unknown_task, StopReason.USER_CANCELLED,
        )
        unknown_task_after_cancel = await task_application.get_task(factory, actor, unknown_task)
        unknown_accounting_after_cancel = await _accounting_effects(engine, unknown_task)
        async with engine.connect() as connection:
            unknown_execution_after_cancel = await connection.scalar(text(
                "SELECT execution_state FROM operations WHERE id=:operation"
            ), {"operation": str(unknown_job.operation_id)})
            unknown_hold_after_cancel = await connection.scalar(text(
                "SELECT state FROM agent_execution_holds WHERE operation_id=:operation AND quiescent_at IS NULL"
            ), {"operation": str(unknown_job.operation_id)})

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
            "unknown_execution_state_after_cancel": unknown_execution_after_cancel,
            "unknown_hold_state_after_cancel": unknown_hold_after_cancel,
            "unknown_task_state_after_cancel": unknown_task_after_cancel["state"],
            "unknown_task_outcome_after_cancel": unknown_task_after_cancel["outcome"],
            "unknown_task_stop_reason_after_cancel": unknown_task_after_cancel["stop_reason"],
            "unknown_cancel_receipt": dict(unknown_cancel_view),
            "source_task_cost_status": unknown_task_view["cost_status"],
            "source_pending_reservations": unknown_task_view["pending_reservations"],
            "source_cost_status_after_cancel": unknown_task_after_cancel["cost_status"],
            "source_pending_reservations_after_cancel": unknown_task_after_cancel["pending_reservations"],
            "accounting_before_cancel": unknown_accounting_before_cancel,
            "accounting_after_cancel": unknown_accounting_after_cancel,
            "accounting_delta_from_cancel": _effect_delta(unknown_accounting_before_cancel, unknown_accounting_after_cancel),
            "provider_calls_before_cancel": provider_calls_before_unknown_cancel,
            "provider_calls_after_cancel": fake.count(),
            "blocked_task_attempts_and_operations": dict(blocked_counts),
            "session_prepares_before": session_prepares_before_hold,
            "session_prepares_after": session_prepare_count["count"],
            "provider_calls_before": provider_count_before_hold, "provider_calls_after": fake.count(),
            "passed": unknown_hold_state == "UNKNOWN"
            and unknown_execution_after_cancel == "UNKNOWN" and unknown_hold_after_cancel == "UNKNOWN"
            and unknown_task_after_cancel["state"] == "STOPPING"
            and unknown_task_after_cancel["outcome"] is None
            and unknown_task_after_cancel["stop_reason"] == "USER_CANCELLED"
            and unknown_task_view["cost_status"] == "PENDING_SETTLEMENT"
            and unknown_task_view["pending_reservations"] == 1 and blocked_view["state"] == "QUEUED"
            and unknown_task_after_cancel["cost_status"] == "PENDING_SETTLEMENT"
            and unknown_task_after_cancel["pending_reservations"] == 1
            and unknown_accounting_before_cancel == unknown_accounting_after_cancel
            and fake.count() == provider_calls_before_unknown_cancel
            and not blocked_leases and blocked_counts["attempts"] == 0 and blocked_counts["operations"] == 0
            and session_prepare_count["count"] == session_prepares_before_hold
            and fake.count() == provider_count_before_hold,
        }

        report["provider"] = {
            "mode": "isolated_loopback_fake_only", "real_provider_calls": 0,
            "forwarded_requests": fake.count(),
            "turn_contract_observations": turn_contract_observations,
            "request_metadata": [
                {key: request.get(key) for key in ("sequence", "response_id", "request_hash", "model", "stream", "max_tokens", "max_completion_tokens", "behavior")}
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
