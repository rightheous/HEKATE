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
import traceback
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import make_url
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import phase3_runtime_probe as p3
import phase3b_single_hekate_probe as p3b
import phase4_evidence_position_probe as p4

from hekate.application import results as results_app
from hekate.application.budgets import _restore_binding
from hekate.application.lifecycle import _reservation_amount, request_critic
from hekate.domain.contracts import canonical_json_hash
from hekate.domain.errors import Conflict, UnknownExecution
from hekate.domain.capsules import parse_critic_turn_output
from hekate.domain.models import AuthorizationSnapshot, SpawnProposal
from hekate.domain.types import (
    ActorContext, AttemptId, DeploymentId, DomainId, OperationId, PrincipalId,
    RegistryId, ScopeId, TaskId,
)
from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.infrastructure.letta.bridge_protocol import BridgeClient
from hekate.infrastructure.letta.provider_gateway import ProviderGatewayProfile
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.infrastructure.postgres.critic_workflow_repository import PostgresCriticWorkflowRepository
from hekate.infrastructure.postgres.delivery_repository import PostgresDeliveryRepository
from hekate.infrastructure.postgres.knowledge_repository import PostgresKnowledgeRepository
from hekate.settings import configured_critic_execution, configured_local_actor, configured_task_execution, load_settings
from hekate.worker import service as worker_service


def _strings(value: object):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from _strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _strings(child)


def _request_capsule(request: dict[str, object]) -> tuple[str, dict[str, object]] | None:
    for content in _strings(request.get("messages")):
        marker = "Task Capsule JSON:\n"
        if marker in content:
            capsule, _ = json.JSONDecoder().raw_decode(content.split(marker, 1)[1].lstrip())
            if not isinstance(capsule, dict):
                raise ValueError("provider request Task Capsule is not an object")
            return content, capsule
    return None


class ContextCheckingResponses:
    def __init__(self, evidence_id: str, observations: list[dict[str, object]]) -> None:
        self.evidence_id = evidence_id
        self.observations = observations
        self.evidence_by_task: dict[str, str] = {}

    def expect_task(self, task_id: str, evidence_id: str) -> None:
        self.evidence_by_task[task_id] = evidence_id

    def __call__(self, request: dict[str, object]) -> object:
        turn = _request_capsule(request)
        if turn is None:
            raise ValueError("compaction was not expected in the zero-compaction fixture")
        prompt, capsule = turn
        binding = re.search(
            r"Trusted runtime binding: task_id=([^;]+); attempt_id=([^;]+); agent_registry_id=([^;]+); input_revision=(\d+)\.",
            prompt,
        )
        if binding is None:
            raise ValueError("request omitted trusted runtime binding")
        task_id, attempt_id, registry_id, revision_text = binding.groups()
        expected_evidence_id = self.evidence_by_task.get(task_id, self.evidence_id)
        if (task_id, attempt_id, int(revision_text)) != (
            capsule.get("task_id"), capsule.get("attempt_id"), capsule.get("input_revision"),
        ):
            raise ValueError("Task Capsule binding differs from the trusted binding")

        evidence_ids = [str(item) for item in capsule.get("evidence_refs", [])]
        excerpts = capsule.get("task_data", {}).get("evidence", [])
        if evidence_ids != [expected_evidence_id] or len(excerpts) != 1:
            raise ValueError("workflow step did not receive exactly the explicitly selected Evidence")
        if "The copper seal remains valid through 2031" not in excerpts[0].get("excerpt", ""):
            raise ValueError("workflow step omitted the source excerpt")

        role = capsule.get("reasoning_role")
        if role == "critic":
            if capsule.get("mode") != "targeted_review" or not isinstance(capsule.get("review_target"), dict):
                raise ValueError("Critic request was not targeted review")
            target = capsule["review_target"]
            if target.get("purpose") != "Check source freshness" or target.get("target_uncertainty") != "The source could be stale":
                raise ValueError("Critic target did not preserve the approved spawn purpose")
            schema_marker = "Critic server output contract (generated from the strict Python CriticTurnOutput model):\n"
            expected_name = "critic-turn-output.v1.schema.json"
        else:
            if role != "hekate":
                raise ValueError("unexpected workflow reasoning role")
            schema_marker = "HEKATE server output contract (generated from the strict Python HekateTurnOutput model):\n"
            expected_name = "hekate-turn-output.v1.schema.json"

        if schema_marker not in prompt:
            raise ValueError("provider request omitted the role-specific generated schema")
        schema_text = prompt.split(schema_marker, 1)[1].split("\n\nServer output policy:\n", 1)[0]
        schema = json.loads(schema_text)
        from hekate.domain.capsules import export_schemas

        if canonical_json_hash(schema) != canonical_json_hash(export_schemas()[expected_name]):
            raise ValueError("provider request schema differs from its generated server contract")

        conclusion = {
            "schema_version": "1", "task_id": task_id, "attempt_id": attempt_id,
            "agent_id": registry_id, "status": "done",
            "assessment": {
                "statement": "The source supports the candidate while retaining a freshness caveat.",
                "confidence": {"level": "medium", "basis": ["bounded synthetic Evidence review"]},
            },
            "evidence_used": [expected_evidence_id], "objections": [], "assumptions": [],
            "unresolved": [], "recommended_next_step": {"type": "none"},
            "position_recommendation": {"action": "update", "summary": "Retain a freshness caveat."},
        }
        if role == "critic":
            candidate = capsule["review_target"].get("candidate_conclusion", {})
            if candidate.get("status") != "done" or not candidate.get("assessment"):
                raise ValueError("Critic request omitted the uncommitted candidate assessment")
            conclusion["objections"] = [{
                "id": "O1", "severity": "high", "claim": "The source may be stale.",
                "condition": "if the retrieval date predates the applicable rule",
                "suggested_validation": "verify the source retrieval date against the governing rule",
            }]
            output: dict[str, object] = {"schema_version": "1", "conclusion": conclusion}
            stage = "critic_review"
        elif isinstance(capsule.get("synthesis_context"), dict):
            context = capsule["synthesis_context"]
            critic = context.get("critic_conclusion", {})
            dissent = context.get("dissent", [])
            if critic.get("objections", [{}])[0].get("claim") != "The source may be stale." or not dissent:
                raise ValueError("synthesis request omitted the actual Critic Conclusion or persisted dissent")
            dissent_ids = [item["id"] for item in dissent]
            operation = re.search(r"Commit operation_id: ([^\n]+)", prompt)
            if operation is None:
                raise ValueError("synthesis request omitted the server-selected Position operation ID")
            position = {
                "statement": "The copper seal remains valid, with source freshness subject to verification.",
                "applicability": ["the submitted question and source context"],
                "confidence": {"level": "medium", "basis": ["source excerpt and Critic review"]},
                "evidence_refs": [expected_evidence_id], "assumptions": [],
                "dissent_refs": dissent_ids,
                "uncertainty": "Confirm the retrieval date against the applicable rule.",
            }
            output = {
                "schema_version": "1",
                "proposal": {
                    "schema_version": "1", "action": "commit", "operation_id": operation.group(1),
                    "task_id": task_id, "topic_id": capsule["topic_id"],
                    "base_version": capsule["base_position_version"], "input_revision": int(revision_text),
                    "proposed_position": position, "reason_for_change": "Incorporate the Critic's freshness objection.",
                },
                "conclusion": conclusion,
            }
            stage = "synthesis"
        else:
            policy = prompt.split("\n\nServer output policy:\n", 1)[1].split("\n\nTrusted runtime binding:", 1)[0]
            if "Planning may instead submit a" not in policy or capsule.get("topic_id") != "phase5a-main-topic":
                raise ValueError("planning request did not allow a bounded spawn on the submitted topic")
            conclusion["assessment"]["statement"] = "The source supports the candidate, pending a freshness check."
            output = {
                "schema_version": "1",
                "proposal": {
                    "schema_version": "1", "action": "spawn", "role": "critic",
                    "purpose": "Check source freshness", "target_uncertainty": "The source could be stale",
                    "expected_decision_impact": "A stale source would change the applicability of the Position",
                    "task_id": task_id,
                },
                "conclusion": conclusion,
            }
            stage = "planning"

        synthesis = capsule.get("synthesis_context")
        supplied_dissent = synthesis.get("dissent", []) if isinstance(synthesis, dict) else []
        tool_names = [
            tool.get("function", {}).get("name") for tool in request.get("tools", [])
            if isinstance(tool, dict) and isinstance(tool.get("function"), dict)
        ]
        if role == "critic" and any(name != "StructuredOutput" for name in tool_names):
            raise ValueError("Critic provider request exposed a tool other than the structured-output contract")
        self.observations.append({
            "stage": stage, "task_id": task_id, "attempt_id": attempt_id,
            "registry_id": registry_id, "reasoning_role": role,
            "evidence_ids": evidence_ids, "schema": expected_name,
            "upstream_tool_names": tool_names,
            "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
            "context_checked": True,
            "critic_conclusion_received": stage == "synthesis",
            "dissent_ids": [item["id"] for item in supplied_dissent],
        })
        tools = request.get("tools")
        if isinstance(tools, list) and any(
            isinstance(tool, dict) and isinstance(tool.get("function"), dict)
            and tool["function"].get("name") == "StructuredOutput"
            for tool in tools
        ):
            return output
        return json.dumps(output, ensure_ascii=False, separators=(",", ":"))


class RuntimeFaults:
    """One-shot failures around real pinned runtime lifecycle calls."""

    def __init__(self, inner: object, shared: dict[str, object]) -> None:
        self.inner = inner
        self.shared = shared

    def __getattr__(self, name: str):
        return getattr(self.inner, name)

    async def create_agent(self, specification, operation_id):
        role = specification.get("role")
        owner = str(specification.get("owner", ""))
        if role == "hekate":
            gate = self.shared.get("hekate_create_gate")
            if isinstance(gate, dict) and gate.get("owner") == owner:
                gate["entered"].set()
                await gate["release"].wait()
            self.shared["hekate_create_calls"] += 1
        elif role == "critic":
            self.shared["critic_create_calls"] += 1
        created = await self.inner.create_agent(specification, operation_id)
        if role == "hekate" and self.shared.get("lose_hekate_create_response_for") == owner:
            self.shared["lose_hekate_create_response_for"] = None
            raise ConnectionError("injected lost response after actual persistent HEKATE creation")
        if role == "critic":
            if not self.shared["create_response_lost"]:
                self.shared["create_response_lost"] = True
                raise ConnectionError("injected lost response after actual Critic creation")
        return created

    async def list_owned_agents(self, owner, creation_op):
        calls = self.shared.setdefault("agent_list_calls_by_owner", {})
        owner_text = str(owner)
        calls[owner_text] = int(calls.get(owner_text, 0)) + 1
        agents = await self.inner.list_owned_agents(owner, creation_op)
        if self.shared.get("fail_agent_list_after_query_for_owner") == owner_text:
            self.shared["fail_agent_list_after_query_for_owner"] = None
            raise ConnectionError("injected lost response after actual owned-agent query")
        return agents

    async def observe_agent(self, provider_agent_id):
        calls = self.shared.setdefault("agent_observe_calls_by_id", {})
        provider_text = str(provider_agent_id)
        calls[provider_text] = int(calls.get(provider_text, 0)) + 1
        return await self.inner.observe_agent(provider_agent_id)

    async def delete_agent(self, provider_agent_id, operation_id):
        self.shared["critic_delete_calls"] += 1
        if not self.shared["delete_failed_once"]:
            self.shared["delete_failed_once"] = True
            raise ConnectionError("injected Critic deletion failure before actual deletion")
        return await self.inner.delete_agent(provider_agent_id, operation_id)


async def _workflow_state(engine, task_id: str) -> dict[str, object]:
    async with engine.connect() as connection:
        row = (await connection.execute(text("""
            SELECT w.stage, w.parent_attempt_id, w.planning_operation_id, w.input_revision,
                   w.spawn_request_hash, w.critic_registry_id, w.create_operation_id, w.review_attempt_id,
                   w.review_operation_id, w.critic_conclusion_id, w.synthesis_attempt_id,
                   w.synthesis_operation_id, w.delete_operation_id, t.status AS task_status,
                   t.critic_agents, t.review_rounds, ar.provider_agent_id, ar.intended_state AS registry_state
            FROM critic_workflows w JOIN tasks t ON t.id=w.task_id
            JOIN agent_registry ar ON ar.id=w.critic_registry_id WHERE w.task_id=:task
        """), {"task": task_id})).mappings().one_or_none()
        return dict(row) if row else {}


async def _wait_for(engine, task_id: str, predicate, label: str, timeout: float = 120) -> dict[str, object]:
    end = asyncio.get_running_loop().time() + timeout
    last: dict[str, object] = {}
    while asyncio.get_running_loop().time() < end:
        last = await _workflow_state(engine, task_id)
        if last and predicate(last):
            return last
        await asyncio.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {label}; last state: {last}")


async def _start_worker(container):
    stop = asyncio.Event()
    task = asyncio.create_task(worker_service.run_worker(container, stop))
    return stop, task


async def _stop_worker(stop: asyncio.Event, task: asyncio.Task) -> None:
    stop.set()
    await asyncio.wait_for(task, 35)


async def _expedite_job(engine, operation_id: str) -> None:
    async with engine.begin() as connection:
        await connection.execute(text("""
            UPDATE outbox SET available_at=now(), status='PENDING', claim_owner=NULL,
                claim_expires_at=NULL
            WHERE operation_id=:operation AND status IN ('PENDING','CLAIMED')
        """), {"operation": operation_id})


async def _run(database_url: str, node: str, image: str, node_archive: Path, artifact: Path, run_id: str) -> dict[str, object]:
    if not artifact.is_absolute():
        artifact = ROOT / artifact
    report: dict[str, object] = {
        "schema_version": "1", "probe": "phase5a-critic-synthesis", "run_id": run_id,
        "executed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "base_head": "a58df8c5c0f253635a5fafc2d016eec407bfce8a",
        "branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip(),
        "checked_out_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "phase4_included_fingerprint": "5f07215a7a987a9756ceebc16d08f5359c96e6dddc577c0d6599b4969ccd56aa",
        "real_provider_calls": 0, "fake_provider_requests": 0, "overall_status": "blocked",
        "dispatch_safety": {
            "runtime_mode": "test", "production_dispatch": "blocked",
            "real_provider_calls": 0,
        },
        "execution": {
            "command": "IFS= read -r HEKATE_TEST_DATABASE_URL < /tmp/hekate-phase5a-dsn; export HEKATE_TEST_DATABASE_URL HEKATE_NODE_BIN=/tmp/node-v22.19.0/bin/node; uv run --locked python scripts/phase5a_critic_synthesis_probe.py",
            "environment_variables": ["HEKATE_TEST_DATABASE_URL", "HEKATE_NODE_BIN"],
            "result": "running",
        },
        "results": {},
        "limitations": [
            "Synthetic pricing and the local fake provider were used; no production model or pricing is claimed.",
            "Critic review is limited to one initial review; continuation, repair, retry, and recursive spawning remain out of scope.",
            "Letta memory projection remains pending and unsupported; PostgreSQL remains authoritative.",
            "G7 same-execution resume and G8 full-request tokenization remain deferred.",
            "Production dispatch remains closed; no real provider endpoint or credentials were configured.",
        ],
    }
    engine = bridge = fake = gateway_server = gateway_task = sandbox = None
    temp = tempfile.TemporaryDirectory(prefix="hekate-phase5a-")
    state = Path(temp.name)
    archive_root = state / "archive"
    worker_runs: list[tuple[asyncio.Event, asyncio.Task]] = []
    spawn_approval_tasks: list[asyncio.Task] = []
    original_prepare = worker_service.prepare_critic_workflow_steps
    original_maintain = worker_service.maintain_critic_workflows
    original_workflow_insert = PostgresCriticWorkflowRepository.insert
    original_lock_operation = PostgresDeliveryRepository.lock_operation
    original_lock_turn_result = PostgresKnowledgeRepository.lock_turn_result
    release_workflow_insert = asyncio.Event()
    workflow_insert_entered = asyncio.Event()
    release_spawn_race = asyncio.Event()
    spawn_race_active = False
    workflow_insert_paused = False
    spawn_lock_attempts = 0
    spawn_lock_attempts_entered = asyncio.Event()

    async def hold_first_workflow_insert(repository, workflow):
        nonlocal workflow_insert_paused
        if workflow.task_id == TaskId(task_id) and not workflow_insert_paused:
            workflow_insert_paused = True
            workflow_insert_entered.set()
            await release_workflow_insert.wait()
        await original_workflow_insert(repository, workflow)

    async def observe_spawn_lock_attempt(repository, operation_id, request_hash=None):
        nonlocal spawn_lock_attempts
        if spawn_race_active and operation_id == OperationId(planning_operation_id):
            spawn_lock_attempts += 1
            if spawn_lock_attempts >= 2:
                spawn_lock_attempts_entered.set()
        return await original_lock_operation(repository, operation_id, request_hash)

    async def hold_worker_after_spawn_adoption(factory_arg, *, limit=100):
        if spawn_race_active:
            await release_spawn_race.wait()
        return await original_maintain(factory_arg, limit=limit)

    try:
        report["runtime"] = p3._locked_runtime(image, node, node_archive)
        p3.p1.build_bridge(node)
        engine = create_engine(database_url)
        factory = create_uow_factory(engine)
        await p3._truncate(factory)
        async with engine.connect() as connection:
            head = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
            postgres = await connection.scalar(text("SHOW server_version"))
        if head != "0010_p5b_delib_maint":
            raise ValueError(f"Phase 5A probe requires current migration head, found {head}")
        report["database"] = {"name": make_url(database_url).database, "postgres_version": postgres, "migration_head": head}

        scope_text, principal_text = f"phase5a:{run_id}", f"principal:{run_id}"
        policy_version = "phase5a-synthetic-policy-v1"
        actor_scope = ScopeId(scope_text)
        async with factory() as uow:
            await uow.tasks.insert_scope(AuthorizationSnapshot(
                scope=actor_scope, principal_id=PrincipalId(principal_text),
                policy_version=policy_version, authz_epoch=1,
            ))
            await uow.commit()

        config_dir = state / "config"
        config_dir.mkdir()
        (config_dir / "local.yaml").write_text(yaml.safe_dump({"identity": {
            "principal_id": principal_text, "scope_id": scope_text,
            "policy_version": policy_version, "authz_epoch": 1,
        }}), encoding="utf-8")
        (config_dir / "policy.yaml").write_text(yaml.safe_dump({
            "version": policy_version,
            "limits": {"task_budget_usd": "5.00", "system_daily_budget_usd": "25.00", "task_deadline_seconds": 240},
            "critic": {"enabled": True, "max_agents_per_task": 1, "max_review_rounds": 1, "max_syntheses_per_task": 1},
        }), encoding="utf-8")
        models = {}
        for role in ("hekate", "critic"):
            models[role] = {
                "profile_id": "phase5a-shared-synthetic-v1", "model": f"openai-compatible/{p3.FAKE_MODEL}",
                "provider_model": p3.FAKE_MODEL, "max_input_tokens": 32768,
                "max_output_tokens": 2048, "max_compaction_calls": 0,
            }
        (config_dir / "models.yaml").write_text(yaml.safe_dump(models), encoding="utf-8")
        (config_dir / "pricing.yaml").write_text(yaml.safe_dump({"version": "phase5a-synthetic-pricing-v1", "prices": {
            p3.FAKE_MODEL: {"input_usd_per_million": "1", "output_usd_per_million": "2"},
        }}), encoding="utf-8")

        evidence_file = state / "source.txt"
        evidence_file.write_text("The copper seal remains valid through 2031. Retrieval date: 2026-10-03.\n", encoding="utf-8")
        env = os.environ.copy()
        env.update({
            "HEKATE_DATABASE_URL": database_url, "HEKATE_NODE_BIN": node,
            "HEKATE_BRIDGE_ENTRY": str(ROOT / "bridge/letta/dist/main.js"),
            "HEKATE_WORKER_ID": f"phase5a-worker-{run_id}", "HEKATE_RUNTIME_MODE": "test",
            "HEKATE_CONFIG_DIR": str(config_dir), "HEKATE_ARCHIVE_DIR": str(archive_root),
        })
        env["PATH"] = f"{Path(node).parent}:{env.get('PATH', '')}"
        settings = load_settings(env, config_dir)
        actor = configured_local_actor(settings)
        hekate_config = configured_task_execution(settings)
        critic_config = configured_critic_execution(settings)
        if critic_config is None:
            raise ValueError("synthetic Critic profile was not enabled")
        report["profiles"] = {
            "hekate": hekate_config.profile_id, "critic": critic_config.profile_id,
            "pricing": hekate_config.pricing_version, "compaction_calls": 0,
        }

        observations: list[dict[str, object]] = []
        response_errors: list[str] = []
        fake = p3.FakeProvider()
        fake.set_response_factory(ContextCheckingResponses("pending-evidence", observations))
        fake.start()
        sandbox = p3.Phase3Sandbox(state, run_id, image)
        sandbox.start_network()
        gateway_port = p3.reserve_port(sandbox.gateway_address)
        private_token = __import__("secrets").token_urlsafe(40)
        gateway_profile = ProviderGatewayProfile(
            profile_id=hekate_config.profile_id,
            price_table=p3.PriceTable(
                model=p3.FAKE_MODEL, version=hekate_config.pricing_version,
                input_usd_per_million=Decimal("1"), output_usd_per_million=Decimal("2"), synthetic=True,
            ),
            upstream_base_url=f"http://127.0.0.1:{fake.port}", upstream_api_key="isolated-fake-only",
            max_input_tokens=32768, max_output_tokens=2048, test_only=True,
        )
        from hekate.infrastructure.letta.provider_gateway import create_provider_gateway
        gateway_app = create_provider_gateway(factory, gateway_profile, private_token, allow_test_profile=True)
        gateway_server, gateway_task = await p3.start_gateway_server(gateway_app, sandbox.gateway_address, gateway_port)
        sandbox.start_pinned_app_server(gateway_port, private_token)
        letta_url = f"ws://{sandbox.container_address}:{p3.p1.APP_PORT}"

        async def new_runtime() -> tuple[BridgeClient, RuntimeFaults]:
            client = BridgeClient(node, ROOT / "bridge/letta/dist/main.js", env={
                "HEKATE_LETTA_URL": letta_url, "HEKATE_LETTA_TOKEN": private_token,
                "HEKATE_REQUIRE_PROVIDER_BINDING": "1",
            })
            adapter = LettaRuntimeAdapter(client)
            await adapter.verify_compatibility()
            return client, RuntimeFaults(adapter, fault_state)

        fault_state: dict[str, object] = {
            "critic_create_calls": 0, "create_response_lost": False,
            "critic_delete_calls": 0, "delete_failed_once": False,
            "hekate_create_calls": 0, "agent_list_calls_by_owner": {},
            "agent_observe_calls_by_id": {}, "lose_hekate_create_response_for": None,
        }
        bridge, runtime = await new_runtime()
        from hekate.bootstrap import Container
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)

        imported = await p3b._cli(
            env, "evidence", "import", str(evidence_file), "--request-key", f"phase5a-{run_id}-evidence",
            "--kind", "document", "--retention-class", "fixture-retained",
            "--expires-at", (datetime.now(UTC) + timedelta(days=1)).isoformat().replace("+00:00", "Z"),
        )
        evidence_id = str(imported["id"])
        from hekate.application.tasks import submit
        from hekate.domain.models import UserMessage
        task_receipt = await submit(factory, actor, UserMessage(
            text="Can this seal rule be used for this decision?",
            topic_id="phase5a-main-topic", evidence_refs=(evidence_id,),
        ), f"phase5a-{run_id}-task", hekate_config)
        task_id = str(task_receipt["task_id"])
        response_fixture = ContextCheckingResponses(evidence_id, observations)
        response_fixture.expect_task(task_id, evidence_id)
        def checked_response(request):
            try:
                return response_fixture(request)
            except Exception as error:
                response_errors.append(f"{type(error).__name__}: {error}")
                raise
        fake.set_response_factory(checked_response)

        async def submit_scenario_task(label: str) -> dict[str, object]:
            source = state / f"{label}.txt"
            source.write_text(
                f"The copper seal remains valid through 2031. Retrieval date: 2026-10-03. Scenario: {label}.\n",
                encoding="utf-8",
            )
            imported_source = await p3b._cli(
                env, "evidence", "import", str(source), "--request-key", f"phase5a-{run_id}-{label}-evidence",
                "--kind", "document", "--retention-class", "fixture-retained",
                "--expires-at", (datetime.now(UTC) + timedelta(days=30)).isoformat().replace("+00:00", "Z"),
            )
            selected_evidence = str(imported_source["id"])
            receipt = await submit(factory, actor, UserMessage(
                text=f"Can this seal rule be used for the {label} scenario?",
                topic_id="phase5a-main-topic", evidence_refs=(selected_evidence,),
            ), f"phase5a-{run_id}-{label}-task", hekate_config)
            selected_task = str(receipt["task_id"])
            response_fixture.expect_task(selected_task, selected_evidence)
            return {
                "label": label, "task_id": selected_task, "evidence_id": selected_evidence,
                "source_path": str(source),
            }

        async def gate_worker_at(task: dict[str, object], stage: str) -> dict[str, object]:
            entered = asyncio.Event()
            previous_prepare = worker_service.prepare_critic_workflow_steps

            async def gated_prepare(*args, **kwargs):
                current = await _workflow_state(engine, str(task["task_id"]))
                if current.get("stage") == stage:
                    entered.set()
                    return ()
                return await original_prepare(*args, **kwargs)

            worker_service.prepare_critic_workflow_steps = gated_prepare
            stop, running = await _start_worker(container)
            worker_runs.append((stop, running))
            try:
                await asyncio.wait_for(entered.wait(), 150)
                await _stop_worker(stop, running)
                worker_runs.remove((stop, running))
                worker_service.prepare_critic_workflow_steps = previous_prepare
            except BaseException:
                stop.set()
                await asyncio.gather(running, return_exceptions=True)
                if (stop, running) in worker_runs:
                    worker_runs.remove((stop, running))
                worker_service.prepare_critic_workflow_steps = previous_prepare
                raise
            current = await _workflow_state(engine, str(task["task_id"]))
            if current.get("stage") != stage:
                raise AssertionError(f"expected {stage} barrier, got {current.get('stage')}")
            return {
                "task": task, "stage": stage, "previous_prepare": previous_prepare,
            }

        async def finish_gated_worker(gate: dict[str, object], *, complete: bool = True) -> dict[str, object]:
            task = gate["task"]
            task_id_value = str(task["task_id"])
            if complete:
                worker_service.prepare_critic_workflow_steps = gate["previous_prepare"]
                stop, running = await _start_worker(container)
                worker_runs.append((stop, running))
                try:
                    result = await _wait_for(
                        engine, task_id_value, lambda value: value.get("stage") == "COMPLETE",
                        f"{task['label']} workflow cleanup", 180,
                    )
                finally:
                    await _stop_worker(stop, running)
                    worker_runs.remove((stop, running))
            else:
                result = await _workflow_state(engine, task_id_value)
            return result

        async def scenario_snapshot(task_id_value: str) -> dict[str, object]:
            async with engine.connect() as connection:
                task_row = (await connection.execute(text("""
                    SELECT id, status, outcome, stop_reason, input_revision, critic_agents, review_rounds
                    FROM tasks WHERE id=:task
                """), {"task": task_id_value})).mappings().one()
                workflow_row = (await connection.execute(text("""
                    SELECT w.stage, w.input_revision, w.review_operation_id, w.synthesis_operation_id,
                           w.critic_registry_id, w.delete_operation_id, ar.intended_state AS critic_state
                    FROM critic_workflows w JOIN agent_registry ar ON ar.id=w.critic_registry_id
                    WHERE w.task_id=:task
                """), {"task": task_id_value})).mappings().one()
                response_row = (await connection.execute(text("""
                    SELECT proposal, response_text, stop_reason, input_revision
                    FROM task_responses WHERE task_id=:task
                """), {"task": task_id_value})).mappings().one_or_none()
                reservation_rows = (await connection.execute(text("""
                    SELECT o.kind, br.status, br.amount::text AS amount,
                           COALESCE((SELECT sum(ra.held_amount) FROM reservation_accounts ra
                                     WHERE ra.reservation_id=br.id), 0)::text AS held
                    FROM critic_workflows w JOIN operations o
                      ON o.id IN (w.review_operation_id, w.synthesis_operation_id)
                    JOIN budget_reservations br ON br.operation_id=o.id
                    WHERE w.task_id=:task ORDER BY o.kind
                """), {"task": task_id_value})).mappings().all()
                position_count = await connection.scalar(text(
                    "SELECT count(*) FROM position_versions WHERE task_id=:task"
                ), {"task": task_id_value})
                ledger_count = await connection.scalar(text("""
                    SELECT count(*) FROM budget_ledger l JOIN budget_accounts a ON a.id=l.account_id
                    WHERE a.scope_kind='TASK' AND a.scope_ref=:task
                """), {"task": task_id_value})
                delete_outbox_count = await connection.scalar(text("""
                    SELECT count(*) FROM critic_workflows w JOIN outbox o ON o.operation_id=w.delete_operation_id
                    WHERE w.task_id=:task AND o.kind='critic_delete'
                """), {"task": task_id_value})
                task_budget = (await connection.execute(text("""
                    SELECT spent_amount::text, held_amount::text FROM budget_accounts
                    WHERE scope_kind='TASK' AND scope_ref=:task
                """), {"task": task_id_value})).mappings().one()
                call_rows = (await connection.execute(text("""
                    SELECT p.status, COALESCE(u.settlement_state, 'PENDING') AS settlement_state
                    FROM provider_calls p JOIN operations o ON o.id=p.operation_id
                    LEFT JOIN usage_projections u ON u.accounting_call_id=p.accounting_call_id
                    WHERE o.task_id=:task ORDER BY p.created_at, p.accounting_call_id
                """), {"task": task_id_value})).mappings().all()
            return {
                "task": dict(task_row), "workflow": dict(workflow_row),
                "response": dict(response_row) if response_row else None,
                "reservations": [dict(item) for item in reservation_rows],
                "position_versions": int(position_count or 0),
                "budget_ledger_rows": int(ledger_count or 0),
                "critic_delete_outbox_rows": int(delete_outbox_count or 0),
                "task_budget": dict(task_budget), "provider_calls": [dict(item) for item in call_rows],
            }

        async def expire_scenario_evidence(task: dict[str, object]) -> dict[str, object]:
            expired_at = datetime.now(UTC) - timedelta(seconds=1)
            async with engine.begin() as connection:
                await connection.execute(text(
                    "UPDATE evidence SET expiry_at=:expiry WHERE id=:evidence"
                ), {"expiry": expired_at, "evidence": task["evidence_id"]})
            from hekate.application.evidence import expire

            cleanup = await expire(factory, archive_root, datetime.now(UTC), 100)
            async with engine.connect() as connection:
                row = (await connection.execute(text("""
                    SELECT e.availability, e.artifact_ref, a.storage_state
                    FROM evidence e JOIN artifacts a ON a.artifact_ref=e.artifact_ref
                    WHERE e.id=:evidence
                """), {"evidence": task["evidence_id"]})).mappings().one()
            digest = str(row["artifact_ref"]).removeprefix("sha256:")
            archive_path = archive_root / digest[:2] / digest
            source_exists = Path(str(task["source_path"])).is_file()
            info = {
                "maintenance_result": cleanup, "availability": row["availability"],
                "artifact_state": row["storage_state"], "archive_exists": archive_path.is_file(),
                "original_input_exists": source_exists,
            }
            if (
                row["availability"] != "EXPIRED" or row["storage_state"] != "DELETED"
                or archive_path.exists() or not source_exists
            ):
                raise AssertionError(f"Evidence expiry did not tombstone/delete safely: {info}")
            return info

        async def assert_policy_failure(task: dict[str, object], expected_code: str) -> dict[str, object]:
            snapshot = await scenario_snapshot(str(task["task_id"]))
            response = snapshot["response"]
            if (
                snapshot["task"]["status"] != "FAILED"
                or snapshot["task"]["stop_reason"] != "POLICY"
                or response is None or response["stop_reason"] != "POLICY"
                or response["proposal"].get("failure_code") != expected_code
                or response["proposal"].get("server_generated") is not True
                or snapshot["position_versions"] != 0
                or snapshot["workflow"]["stage"] != "COMPLETE"
                or snapshot["workflow"]["critic_state"] != "DELETED"
            ):
                raise AssertionError(f"Permanent workflow rejection did not converge exactly: {snapshot}")
            return snapshot

        PostgresCriticWorkflowRepository.insert = hold_first_workflow_insert
        PostgresDeliveryRepository.lock_operation = observe_spawn_lock_attempt
        worker_service.maintain_critic_workflows = hold_worker_after_spawn_adoption
        stop, worker_task = await _start_worker(container)
        worker_runs.append((stop, worker_task))
        # Pause the actual result-adoption transaction immediately before its
        # first workflow INSERT. Two competitors then contend on its real
        # PostgreSQL operation lock before the primary transaction can commit.
        await asyncio.wait_for(workflow_insert_entered.wait(), 120)
        async with factory() as uow:
            async with engine.connect() as connection:
                row = (await connection.execute(text("""
                    SELECT r.attempt_id, r.operation_id, r.conclusion_id, r.proposal, r.processing_state,
                           o.state, o.execution_state, o.binding
                    FROM turn_results r JOIN operations o ON o.id=r.operation_id
                    WHERE r.task_id=:task AND r.proposal->>'action'='spawn'
                """), {"task": task_id})).mappings().one()
            attempt = await uow.tasks.get_attempt(AttemptId(row["attempt_id"]))
            proposal = SpawnProposal.model_validate(row["proposal"], strict=True)
            await uow.rollback()
        planning_operation_id = str(row["operation_id"])
        binding = row["binding"]
        if not isinstance(binding, dict) or not isinstance(binding.get("fence"), int):
            raise AssertionError("trusted planning operation binding was unavailable")
        spawn_actor = ActorContext(
            principal_id=PrincipalId(binding["principal_id"]), scope=ScopeId(binding["scope"]),
            authenticated_agent_registry_id=attempt.agent_registry_id, task_id=TaskId(task_id),
            attempt_id=attempt.id, input_revision=attempt.input_revision,
            policy_version=binding["policy_version"], authz_epoch=int(binding["authz_epoch"]),
            fence=int(binding["fence"]),
        )

        async def approve_once():
            async with factory() as uow:
                op_row = await uow.delivery.lock_operation(OperationId(row["operation_id"]))
                current_attempt = await uow.tasks.get_attempt(AttemptId(row["attempt_id"]))
                receipt = await request_critic(
                    uow, spawn_actor, proposal, op_row, current_attempt,
                    DomainId(row["conclusion_id"]), hekate_config, critic_config,
                )
                await uow.commit()
                return receipt

        approval_counts_before = await _counts(engine, task_id, scope_text)
        if approval_counts_before["workflows"] != 0 or approval_counts_before["critics"] != 0:
            raise AssertionError("spawn workflow became visible before the competing approval barrier")
        spawn_race_active = True
        competing_approvals = [asyncio.create_task(approve_once()) for _ in range(2)]
        spawn_approval_tasks.extend(competing_approvals)
        await asyncio.wait_for(spawn_lock_attempts_entered.wait(), 10)
        release_workflow_insert.set()
        concurrent_receipts = await asyncio.gather(*competing_approvals)
        approval_replay_counts = await _counts(engine, task_id, scope_text)
        async with engine.connect() as connection:
            adopted_spawn_state = await connection.scalar(text("""
                SELECT processing_state FROM turn_results
                WHERE task_id=:task AND proposal->>'action'='spawn'
            """), {"task": task_id})
        if adopted_spawn_state != "ACCEPTED":
            raise AssertionError(f"primary planning result was not accepted atomically: {adopted_spawn_state}")
        spawn_race_active = False
        release_spawn_race.set()
        PostgresCriticWorkflowRepository.insert = original_workflow_insert
        PostgresDeliveryRepository.lock_operation = original_lock_operation
        worker_service.maintain_critic_workflows = original_maintain
        await _wait_for(engine, task_id, lambda value: value.get("stage") == "CREATE_UNKNOWN", "Critic create response loss")
        stage = (await _workflow_state(engine, task_id))["stage"]
        if stage == "CREATE_UNKNOWN":
            before_recovery = await _workflow_state(engine, task_id)
            await _stop_worker(stop, worker_task)
            worker_runs.pop()
            await bridge.close()
            bridge, runtime = await new_runtime()
            container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
            await _expedite_job(engine, str(before_recovery["create_operation_id"]))

            # Stop exactly after the accepted Critic result is durable, before synthesis is admitted.
            paused_at_synthesis = asyncio.Event()
            original_bound_prepare = worker_service.prepare_critic_workflow_steps
            async def pause_synthesis(
                factory_arg, runtime_arg, actor_arg, hekate_arg, critic_arg, worker_arg,
                deliberation_arg, **kwargs,
            ):
                current = await _workflow_state(engine, task_id)
                if current.get("stage") == "SYNTHESIS_PENDING":
                    paused_at_synthesis.set()
                    return ()
                return await original_bound_prepare(
                    factory_arg, runtime_arg, actor_arg, hekate_arg, critic_arg,
                    worker_arg, deliberation_arg, **kwargs,
                )
            worker_service.prepare_critic_workflow_steps = pause_synthesis
            stop, worker_task = await _start_worker(container)
            worker_runs.append((stop, worker_task))
            await asyncio.wait_for(paused_at_synthesis.wait(), 120)
            critic_result_durable = await _workflow_state(engine, task_id)
            async with engine.connect() as connection:
                dissent = (await connection.execute(text("""
                    SELECT d.id, d.body FROM critic_workflows w JOIN conclusions c ON c.id=w.critic_conclusion_id
                    JOIN dissent d ON d.conclusion_id=c.id WHERE w.task_id=:task
                """), {"task": task_id})).mappings().all()
                review_counts = (await connection.execute(text("""
                    SELECT (SELECT count(*) FROM conclusions WHERE task_id=:task) AS conclusions,
                           (SELECT count(*) FROM dissent WHERE scope=:scope) AS dissent,
                           (SELECT count(*) FROM agent_registry WHERE task_id=:task AND role='critic') AS critics
                """), {"task": task_id, "scope": scope_text})).mappings().one()
            if not dissent or int(review_counts["critics"]) != 1:
                raise AssertionError("Critic Conclusion/dissent or single-agent invariant was not durable")
            await _stop_worker(stop, worker_task)
            worker_runs.pop()
            await bridge.close()

            # New bridge client and worker process consume the saved synthesis stage.
            bridge, runtime = await new_runtime()
            container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
            worker_service.prepare_critic_workflow_steps = original_bound_prepare
        else:
            raise AssertionError(f"Critic creation response loss was not persisted; got stage {stage}")

        # Fail the first delete before RPC, then restart and confirm actual absence.
        def observe_delete_failure(state):
            if state.get("stage") == "DELETE_PENDING" and state.get("delete_operation_id"):
                return True
            return False
        stop, worker_task = await _start_worker(container)
        worker_runs.append((stop, worker_task))
        await _wait_for(engine, task_id, observe_delete_failure, "synthesis and Critic retirement", 150)
        # Let the first delete attempt record UNKNOWN and durable retry state.
        await _wait_for_operation(engine, str(task_id), "critic.delete", "UNKNOWN", 45)
        await _stop_worker(stop, worker_task)
        worker_runs.pop()
        state_after_failure = await _workflow_state(engine, task_id)
        await _expedite_job(engine, str(state_after_failure["delete_operation_id"]))
        await bridge.close()
        bridge, runtime = await new_runtime()
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
        stop, worker_task = await _start_worker(container)
        worker_runs.append((stop, worker_task))
        completed = await _wait_for(engine, task_id, lambda value: value.get("stage") == "COMPLETE", "Critic deletion confirmation")
        await _stop_worker(stop, worker_task)
        worker_runs.pop()

        position = await p3b._cli(env, "position", "show", "phase5a-main-topic")
        history = await p3b._cli(env, "position", "history", "phase5a-main-topic", "--after-version", "0", "--limit", "50")
        task_view = await p3b._cli(env, "task", task_id)
        async with engine.connect() as connection:
            operation_rows = (await connection.execute(text("""
                SELECT o.id, o.kind, o.state, o.execution_state, a.status AS attempt_status,
                       br.status AS reservation_status, br.amount::text AS reserved_amount
                FROM operations o LEFT JOIN attempts a ON a.operation_id=o.id
                LEFT JOIN budget_reservations br ON br.operation_id=o.id
                WHERE o.task_id=:task ORDER BY o.created_at, o.id
            """), {"task": task_id})).mappings().all()
            counts = (await connection.execute(text("""
                SELECT (SELECT count(*) FROM position_versions WHERE task_id=:task) AS position_versions,
                       (SELECT count(*) FROM position_commit_receipts WHERE scope=:scope) AS commit_receipts,
                       (SELECT count(*) FROM task_responses WHERE task_id=:task) AS responses,
                       (SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.task_id=:task) AS provider_calls,
                       (SELECT count(*) FROM dissent WHERE scope=:scope) AS dissent_rows,
                       (SELECT count(*) FROM outbox WHERE operation_id IN (SELECT id FROM operations WHERE task_id=:task) AND kind='dispatch') AS dispatch_rows
            """), {"task": task_id, "scope": scope_text})).mappings().one()
            provider_agent_ids = (await connection.execute(text("""
                SELECT id, role AS kind, persistence, intended_state, provider_agent_id FROM agent_registry
                WHERE owner_scope=:scope ORDER BY kind, id
            """), {"scope": scope_text})).mappings().all()
            pending_projection = (await connection.execute(text("""
                SELECT desired_version, applied_version, state, pending_reason FROM memory_projections
                WHERE scope=:scope AND topic_id='phase5a-main-topic'
            """), {"scope": scope_text})).mappings().one_or_none()

        # The duplicate business-result and two competing spawn approvals are all replays.
        async with engine.connect() as connection:
            planning_result = await connection.scalar(text("""
                SELECT inbox_id FROM turn_results
                WHERE task_id=:task AND proposal->>'action'='spawn'
            """), {"task": task_id})
            critic_result = await connection.scalar(text("""
                SELECT r.inbox_id FROM turn_results r
                JOIN critic_workflows w ON w.critic_conclusion_id=r.conclusion_id
                WHERE w.task_id=:task
            """), {"task": task_id})
        if not planning_result or not critic_result:
            raise AssertionError("planning and Critic result inbox rows were not durable")
        replay_before = await _counts(engine, task_id, scope_text)
        critic_replay_result = await results_app.apply_turn_result(factory, critic_result)
        after_critic_replay = await _counts(engine, task_id, scope_text)
        planning_replay_result = await results_app.apply_turn_result(factory, planning_result)
        after_replay = await _counts(engine, task_id, scope_text)
        if replay_before != after_critic_replay or replay_before != after_replay:
            raise AssertionError("accepted Critic or planning result replay added workflow effects")
        review_result = await _wait_for(engine, task_id, lambda value: value.get("stage") == "COMPLETE", "completed workflow after replay", 2)
        workflow = completed
        position_version = position.get("current_version")
        final_synthesis = next(item for item in observations if item["stage"] == "synthesis")
        if not final_synthesis["critic_conclusion_received"] or not final_synthesis["dissent_ids"]:
            raise AssertionError("post-restart synthesis did not consume durable Critic dissent")
        critic_registry = str(workflow["critic_registry_id"])
        critic_state = next(item for item in provider_agent_ids if item["id"] == critic_registry)
        hekate_state = next(item for item in provider_agent_ids if item["kind"] == "hekate")
        critic_output_boundaries = await _critic_output_boundary_checks(
            factory, str(task_id), str(hekate_state["id"]),
        )

        lifecycle_states = await _operation_states(engine, str(task_id))
        report["workflow"] = {
            "task_id": task_id, "task_status": task_view.get("state"),
            "input_revision": workflow["input_revision"],
            "planning_attempt_id": workflow["parent_attempt_id"],
            "planning_operation_id": workflow["planning_operation_id"],
            "spawn_request_hash": workflow["spawn_request_hash"],
            "hekate_registry_id": hekate_state["id"], "critic_registry_id": critic_registry,
            "critic_registry_state": critic_state["intended_state"],
            "create_operation_id": workflow["create_operation_id"],
            "review_attempt_id": workflow["review_attempt_id"], "review_operation_id": workflow["review_operation_id"],
            "critic_conclusion_id": workflow["critic_conclusion_id"],
            "dissent_ids": final_synthesis["dissent_ids"],
            "synthesis_attempt_id": workflow["synthesis_attempt_id"],
            "synthesis_operation_id": workflow["synthesis_operation_id"],
            "delete_operation_id": workflow["delete_operation_id"],
            "position_version": position_version,
            "position_statement": position.get("current", {}).get("body", {}).get("statement"),
            "position_projection": position.get("current", {}).get("projection_state"),
            "position_projection_pending_reason": position.get("current", {}).get("projection_pending_reason"),
            "task_response": task_view.get("response"),
        }
        report["normal_path"] = {
            "passed": task_view.get("state") == "COMPLETED" and position_version == 1
                and int(counts["position_versions"]) == 1 and int(counts["commit_receipts"]) == 1
                and int(counts["responses"]) == 1 and int(counts["dissent_rows"]) == 1
                and workflow["stage"] == "COMPLETE" and critic_state["intended_state"] == "DELETED"
                and hekate_state["intended_state"] == "READY" and fake.count() == 3,
            "provider_requests": fake.count(), "requests": list(observations),
            "db_effect_counts": {key: int(value) for key, value in counts.items()},
            "operations_and_reservations": [dict(item) for item in operation_rows],
            "lifecycle_operations": lifecycle_states,
            "critic_create_calls": fault_state["critic_create_calls"],
            "critic_delete_calls": fault_state["critic_delete_calls"],
            "critic_output_boundaries": critic_output_boundaries,
            "create_response_lost_then_owned_tag_recovered": bool(fault_state["create_response_lost"]),
            "delete_failure_retried_after_restart": bool(fault_state["delete_failed_once"]),
            "persistent_hekate_retained": hekate_state["intended_state"] == "READY",
            "projection": dict(pending_projection) if pending_projection else None,
            "inbox_replay": {
                "critic_result": critic_replay_result,
                "planning_spawn": planning_replay_result,
            },
            "effects_before_replay": replay_before,
            "effects_after_critic_replay": after_critic_replay,
            "effects_after_replay": after_replay,
            "replay_no_new_effects": replay_before == after_critic_replay == after_replay,
            "concurrent_spawn_receipts": [item.model_dump(mode="json") for item in concurrent_receipts],
            "concurrent_spawn_replay_no_new_effects": (
                approval_replay_counts["workflows"] == 1
                and approval_replay_counts["critics"] == 1
                and approval_replay_counts["critic_agents"] == 1
                and approval_counts_before["review_rounds"] == approval_replay_counts["review_rounds"]
                and approval_replay_counts["reservations"] == approval_counts_before["reservations"] + 2
                and approval_replay_counts["outbox"] == approval_counts_before["outbox"] + 1
                and approval_replay_counts["positions"] == approval_counts_before["positions"]
                and approval_replay_counts["receipts"] == approval_counts_before["receipts"]
                and approval_replay_counts["calls"] == approval_counts_before["calls"]
                and approval_replay_counts["responses"] == approval_counts_before["responses"]
                and approval_replay_counts["dissent"] == approval_counts_before["dissent"]
                and all(item.critic_registry_id == concurrent_receipts[0].critic_registry_id for item in concurrent_receipts)
                and all(item.create_operation_id == concurrent_receipts[0].create_operation_id for item in concurrent_receipts)
            ),
            "spawn_effect_counts_before_replay": approval_counts_before,
            "spawn_effect_counts_after_replay": approval_replay_counts,
            "one_critic_after_postgres_race": len([item for item in provider_agent_ids if item["kind"] == "critic"]) == 1,
            "postgres_primary_and_competing_approval_race": {
                "primary_transaction_paused_before_workflow_insert": workflow_insert_paused,
                "competing_operation_lock_attempts": spawn_lock_attempts,
                "competing_approvals_returned_same_registry": len({str(item.critic_registry_id) for item in concurrent_receipts}) == 1,
                "one_durable_workflow_and_create_intent": approval_replay_counts["workflows"] == 1
                    and approval_replay_counts["critics"] == 1
                    and approval_replay_counts["critic_agents"] == 1,
                "critic_and_synthesis_holds_reserved_once": approval_replay_counts["reservations"] == approval_counts_before["reservations"] + 2,
                "create_outbox_intent_inserted_once": approval_replay_counts["outbox"] == approval_counts_before["outbox"] + 1,
                "planning_result_accepted": adopted_spawn_state == "ACCEPTED",
            },
        }
        if (
            not report["normal_path"]["passed"] or not report["normal_path"]["replay_no_new_effects"]
            or not report["normal_path"]["concurrent_spawn_replay_no_new_effects"]
            or not all(critic_output_boundaries.values())
        ):
            raise AssertionError("Phase 5A normal path or idempotency assertions failed")
        if int(fault_state["critic_create_calls"]) != 1 or int(fault_state["critic_delete_calls"]) != 2:
            raise AssertionError("Critic create recovery or deletion retry issued an unexpected number of RPCs")

        # Persistent HEKATE creation is a lifecycle RPC, not an inference call.
        # Verify its PostgreSQL journal through the same pinned adapter/runtime
        # path used by the Task worker, including old incomplete rows.
        from hekate.application.lifecycle import ensure_hekate

        async def create_scope_actor(label: str) -> ActorContext:
            child_scope = ScopeId(f"{scope_text}:create-journal:{label}")
            child_principal = PrincipalId(f"principal:{run_id}:create-journal:{label}")
            async with factory() as uow:
                await uow.tasks.insert_scope(AuthorizationSnapshot(
                    scope=child_scope, principal_id=child_principal,
                    policy_version=policy_version, authz_epoch=1,
                ))
                await uow.commit()
            return ActorContext(
                principal_id=child_principal, scope=child_scope,
                authenticated_agent_registry_id=None, task_id=None, attempt_id=None,
                input_revision=None, policy_version=policy_version, authz_epoch=1, fence=1,
            )

        async def create_journal_snapshot(owner_scope: str) -> dict[str, object]:
            async with engine.connect() as connection:
                row = (await connection.execute(text("""
                    SELECT a.id AS registry_id, a.creation_operation_id, a.provider_agent_id,
                           a.owner_scope, a.role, a.persistence, a.intended_state, a.observed_state,
                           a.active_attempt_id, o.kind, o.request_hash, o.state, o.dispatch_state,
                           o.execution_state, o.observation, o.last_error, o.updated_at,
                           (SELECT count(*) FROM provider_calls p WHERE p.operation_id=o.id) AS provider_calls,
                           (SELECT count(*) FROM budget_reservations r WHERE r.operation_id=o.id) AS reservations,
                           (SELECT count(*) FROM budget_ledger l JOIN budget_reservations r ON r.id=l.reservation_id
                            WHERE r.operation_id=o.id) AS ledger_rows
                    FROM agent_registry a JOIN operations o ON o.id=a.creation_operation_id
                    WHERE a.owner_scope=:scope AND a.role='hekate' AND a.persistence='persistent'
                """), {"scope": owner_scope})).mappings().one()
            value = dict(row)
            value["updated_at"] = value["updated_at"].isoformat()
            return value

        def call_count(owner_scope: str, mapping_key: str) -> int:
            counts = fault_state.get(mapping_key, {})
            return int(counts.get(owner_scope, 0)) if isinstance(counts, dict) else 0

        main_create_before = await create_journal_snapshot(scope_text)
        if (
            main_create_before["kind"] != "agent.create"
            or main_create_before["state"] != "COMPLETED"
            or main_create_before["dispatch_state"] != "QUIESCENT"
            or main_create_before["execution_state"] != "QUIESCENT"
            or main_create_before["provider_agent_id"] is None
            or main_create_before["observation"].get("registry_id") != main_create_before["registry_id"]
            or int(main_create_before["provider_calls"]) != 0
            or int(main_create_before["reservations"]) != 0
            or int(main_create_before["ledger_rows"]) != 0
            or int(fault_state["hekate_create_calls"]) != 1
        ):
            raise AssertionError(f"normal persistent HEKATE creation journal is inconsistent: {main_create_before}")

        main_create_count = int(fault_state["hekate_create_calls"])
        main_list_count = call_count(scope_text, "agent_list_calls_by_owner")
        main_record = await ensure_hekate(factory, runtime, actor, hekate_config)
        main_replay = await create_journal_snapshot(scope_text)
        if (
            str(main_record.provider_id) != main_create_before["provider_agent_id"]
            or main_replay["updated_at"] != main_create_before["updated_at"]
            or main_replay["observation"] != main_create_before["observation"]
            or int(fault_state["hekate_create_calls"]) != main_create_count
            or call_count(scope_text, "agent_list_calls_by_owner") != main_list_count
        ):
            raise AssertionError("completed persistent HEKATE creation replay changed its journal or queried runtime")

        # Simulate the historical defect after a real successful create: the
        # provider binding remains, while only the lifecycle journal is stale.
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET state='CLAIMED', dispatch_state='NOT_STARTED',
                    execution_state='PENDING', observation='{}'::jsonb, last_error=NULL
                WHERE id=:operation
            """), {"operation": main_create_before["creation_operation_id"]})
        legacy_before = await create_journal_snapshot(scope_text)
        legacy_list_before = call_count(scope_text, "agent_list_calls_by_owner")
        legacy_create_before = int(fault_state["hekate_create_calls"])
        legacy_record = await ensure_hekate(factory, runtime, actor, hekate_config)
        legacy_after = await create_journal_snapshot(scope_text)
        if (
            legacy_before["state"] != "CLAIMED" or legacy_before["execution_state"] != "PENDING"
            or legacy_after["state"] != "COMPLETED" or legacy_after["dispatch_state"] != "QUIESCENT"
            or legacy_after["execution_state"] != "QUIESCENT"
            or legacy_after["provider_agent_id"] != main_create_before["provider_agent_id"]
            or str(legacy_record.provider_id) != legacy_after["provider_agent_id"]
            or call_count(scope_text, "agent_list_calls_by_owner") != legacy_list_before + 1
            or int(fault_state["hekate_create_calls"]) != legacy_create_before
        ):
            raise AssertionError("existing incomplete agent.create journal was not reconciled from the actual runtime agent")
        repaired_timestamp = legacy_after["updated_at"]
        repaired_observation = legacy_after["observation"]
        await bridge.close()
        bridge, runtime = await new_runtime()
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
        restart_record = await ensure_hekate(factory, runtime, actor, hekate_config)
        replay_after_restart = await create_journal_snapshot(scope_text)
        if (
            str(restart_record.provider_id) != main_create_before["provider_agent_id"]
            or replay_after_restart["updated_at"] != repaired_timestamp
            or replay_after_restart["observation"] != repaired_observation
            or call_count(scope_text, "agent_list_calls_by_owner") != legacy_list_before + 1
            or int(fault_state["hekate_create_calls"]) != legacy_create_before
        ):
            raise AssertionError("persistent HEKATE restart replay repeated a create or lifecycle write")

        # Force a failure after the provider binding UPDATE but before the
        # operation journal UPDATE. The real external agent remains; PostgreSQL
        # must roll back both writes, then reconciliation must find that agent.
        rollback_actor = await create_scope_actor("binding-journal-rollback")
        rollback_owner = str(rollback_actor.scope)
        rollback_create_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"hekate:create:{rollback_owner}"))
        original_complete_lifecycle = PostgresDeliveryRepository.complete_lifecycle_operation
        injected_completion_failure = {"fired": False}

        async def fail_once_before_create_completion(repository, operation_id, observation):
            if str(operation_id) == rollback_create_id and not injected_completion_failure["fired"]:
                injected_completion_failure["fired"] = True
                raise RuntimeError("injected failure after provider binding, before lifecycle completion")
            await original_complete_lifecycle(repository, operation_id, observation)

        PostgresDeliveryRepository.complete_lifecycle_operation = fail_once_before_create_completion
        try:
            await ensure_hekate(factory, runtime, rollback_actor, hekate_config)
            raise AssertionError("binding/journal rollback failure injection did not fire")
        except RuntimeError as error:
            if "injected failure" not in str(error):
                raise
        finally:
            PostgresDeliveryRepository.complete_lifecycle_operation = original_complete_lifecycle
        rollback_before_recovery = await create_journal_snapshot(rollback_owner)
        if (
            not injected_completion_failure["fired"]
            or rollback_before_recovery["provider_agent_id"] is not None
            or rollback_before_recovery["intended_state"] != "CREATING"
            or rollback_before_recovery["state"] != "CLAIMED"
            or rollback_before_recovery["observation"].get("external_call_started") is not True
        ):
            raise AssertionError(f"provider binding and journal did not roll back atomically: {rollback_before_recovery}")
        rollback_create_calls_before_recovery = int(fault_state["hekate_create_calls"])
        rollback_recovered = await ensure_hekate(factory, runtime, rollback_actor, hekate_config)
        rollback_after_recovery = await create_journal_snapshot(rollback_owner)
        if (
            rollback_after_recovery["state"] != "COMPLETED"
            or rollback_after_recovery["provider_agent_id"] != str(rollback_recovered.provider_id)
            or int(fault_state["hekate_create_calls"]) != rollback_create_calls_before_recovery
        ):
            raise AssertionError("rollback recovery created another persistent HEKATE")

        # Lose the response after the pinned SDK has created the agent. The
        # persisted started marker forbids another create; owner-tag lookup
        # must find and bind the actual result.
        lost_actor = await create_scope_actor("create-response-lost")
        lost_owner = str(lost_actor.scope)
        fault_state["lose_hekate_create_response_for"] = lost_owner
        lost_create_calls_before = int(fault_state["hekate_create_calls"])
        lost_response_rejected = False
        try:
            await ensure_hekate(factory, runtime, lost_actor, hekate_config)
        except UnknownExecution:
            lost_response_rejected = True
        lost_before_recovery = await create_journal_snapshot(lost_owner)
        if (
            not lost_response_rejected or lost_before_recovery["state"] != "UNKNOWN"
            or lost_before_recovery["provider_agent_id"] is not None
        ):
            raise AssertionError("lost persistent HEKATE create response was treated as confirmed")
        lost_list_before = call_count(lost_owner, "agent_list_calls_by_owner")
        lost_record = await ensure_hekate(factory, runtime, lost_actor, hekate_config)
        lost_after_recovery = await create_journal_snapshot(lost_owner)
        lost_create_call_delta = int(fault_state["hekate_create_calls"]) - lost_create_calls_before
        if (
            lost_after_recovery["state"] != "COMPLETED"
            or lost_after_recovery["provider_agent_id"] != str(lost_record.provider_id)
            or lost_create_call_delta != 1
            or call_count(lost_owner, "agent_list_calls_by_owner") != lost_list_before + 1
        ):
            raise AssertionError("lost response recovery did not reconcile the same runtime agent")

        # Two real PostgreSQL requests share one owner scope. Pause the first
        # at the external create boundary; the second sees the durable started
        # intent and may wait/return UNKNOWN, but cannot create another agent.
        concurrent_actor = await create_scope_actor("concurrent-ensure")
        concurrent_owner = str(concurrent_actor.scope)
        create_gate = {"owner": concurrent_owner, "entered": asyncio.Event(), "release": asyncio.Event()}
        fault_state["hekate_create_gate"] = create_gate
        concurrent_create_before = int(fault_state["hekate_create_calls"])
        concurrent_first = asyncio.create_task(ensure_hekate(factory, runtime, concurrent_actor, hekate_config))
        await asyncio.wait_for(create_gate["entered"].wait(), 30)
        concurrent_list_before = call_count(concurrent_owner, "agent_list_calls_by_owner")
        concurrent_second_unknown = False
        try:
            await asyncio.wait_for(ensure_hekate(factory, runtime, concurrent_actor, hekate_config), 30)
        except UnknownExecution:
            concurrent_second_unknown = True
        concurrent_wait_state = await create_journal_snapshot(concurrent_owner)
        if (
            not concurrent_second_unknown
            or concurrent_wait_state["state"] != "UNKNOWN"
            or concurrent_wait_state["observation"].get("external_call_started") is not True
            or int(fault_state["hekate_create_calls"]) != concurrent_create_before
            or call_count(concurrent_owner, "agent_list_calls_by_owner") != concurrent_list_before + 1
        ):
            create_gate["release"].set()
            raise AssertionError("concurrent ensure started a duplicate external create")
        create_gate["release"].set()
        concurrent_record = await asyncio.wait_for(concurrent_first, 30)
        fault_state["hekate_create_gate"] = None
        concurrent_after = await create_journal_snapshot(concurrent_owner)
        concurrent_owned = await runtime.list_owned_agents(DeploymentId(concurrent_owner), OperationId(concurrent_after["creation_operation_id"]))
        if (
            concurrent_after["state"] != "COMPLETED"
            or concurrent_after["provider_agent_id"] != str(concurrent_record.provider_id)
            or len(concurrent_owned) != 1
            or int(fault_state["hekate_create_calls"]) != concurrent_create_before + 1
        ):
            raise AssertionError("concurrent persistent HEKATE ensure did not converge to one confirmed agent")

        # A mismatched stored binding cannot be repaired by choosing a different
        # runtime candidate. The actual pinned list query is still performed.
        mismatch_actor = await create_scope_actor("provider-binding-mismatch")
        mismatch_owner = str(mismatch_actor.scope)
        mismatch_record = await ensure_hekate(factory, runtime, mismatch_actor, hekate_config)
        mismatch_initial = await create_journal_snapshot(mismatch_owner)
        wrong_provider_id = f"provider-mismatch:{run_id}"
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET state='CLAIMED', dispatch_state='NOT_STARTED', execution_state='PENDING',
                    observation=CAST(:observation AS jsonb), last_error=NULL
                WHERE id=:operation
            """), {"operation": mismatch_initial["creation_operation_id"], "observation": json.dumps({"external_call_started": True})})
            await connection.execute(text("UPDATE agent_registry SET provider_agent_id=:provider WHERE id=:registry"), {
                "provider": wrong_provider_id, "registry": mismatch_initial["registry_id"],
            })
        mismatch_list_before = call_count(mismatch_owner, "agent_list_calls_by_owner")
        mismatch_rejected = False
        try:
            await ensure_hekate(factory, runtime, mismatch_actor, hekate_config)
        except Conflict:
            mismatch_rejected = True
        mismatch_after_rejection = await create_journal_snapshot(mismatch_owner)
        if (
            not mismatch_rejected or mismatch_after_rejection["state"] != "UNKNOWN"
            or mismatch_after_rejection["provider_agent_id"] != wrong_provider_id
            or call_count(mismatch_owner, "agent_list_calls_by_owner") != mismatch_list_before + 1
        ):
            raise AssertionError("provider binding mismatch was overwritten or falsely completed")
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE agent_registry SET provider_agent_id=:provider WHERE id=:registry
            """), {"provider": str(mismatch_record.provider_id), "registry": mismatch_initial["registry_id"]})
            await connection.execute(text("""
                UPDATE operations SET state='CLAIMED', dispatch_state='NOT_STARTED', execution_state='PENDING',
                    observation=CAST(:observation AS jsonb), last_error=NULL
                WHERE id=:operation
            """), {"operation": mismatch_initial["creation_operation_id"], "observation": json.dumps({"external_call_started": True})})
        mismatch_recovered = await ensure_hekate(factory, runtime, mismatch_actor, hekate_config)
        mismatch_final = await create_journal_snapshot(mismatch_owner)
        if mismatch_final["state"] != "COMPLETED" or str(mismatch_recovered.provider_id) != mismatch_final["provider_agent_id"]:
            raise AssertionError("binding mismatch fixture did not recover after the fixture was corrected")

        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET state='CLAIMED', dispatch_state='NOT_STARTED', execution_state='PENDING',
                    observation='{}'::jsonb, last_error=NULL
                WHERE id=:operation
            """), {"operation": mismatch_final["creation_operation_id"]})
        fault_state["fail_agent_list_after_query_for_owner"] = mismatch_owner
        lookup_failure_rejected = False
        try:
            await ensure_hekate(factory, runtime, mismatch_actor, hekate_config)
        except UnknownExecution:
            lookup_failure_rejected = True
        lookup_failure_state = await create_journal_snapshot(mismatch_owner)
        if (
            not lookup_failure_rejected or lookup_failure_state["state"] != "UNKNOWN"
            or lookup_failure_state["provider_agent_id"] != mismatch_final["provider_agent_id"]
        ):
            raise AssertionError("owned-agent lookup failure was treated as confirmed creation")
        lookup_recovered = await ensure_hekate(factory, runtime, mismatch_actor, hekate_config)
        lookup_recovered_state = await create_journal_snapshot(mismatch_owner)
        if (
            lookup_recovered_state["state"] != "COMPLETED"
            or str(lookup_recovered.provider_id) != mismatch_final["provider_agent_id"]
        ):
            raise AssertionError("persistent HEKATE lookup failure did not recover on a later confirmed query")

        # A verified creation can complete its journal while the registry is
        # BUSY. It must leave the lease/fence and BUSY state untouched.
        busy_actor = await create_scope_actor("busy-registry")
        busy_owner = str(busy_actor.scope)
        busy_record = await ensure_hekate(factory, runtime, busy_actor, hekate_config)
        async with factory() as uow:
            busy_lease = await uow.agents.acquire_lease(busy_record.registry_id, f"phase5a-busy-{run_id}", 240)
            await uow.agents.set_registry_busy(busy_record.registry_id, str(report["workflow"]["planning_attempt_id"]))
            hold_before = await uow.agents.active_execution_hold(busy_record.registry_id, lock=True)
            await uow.commit()
        busy_initial = await create_journal_snapshot(busy_owner)
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET state='CLAIMED', dispatch_state='NOT_STARTED', execution_state='PENDING',
                    observation='{}'::jsonb, last_error=NULL
                WHERE id=:operation
            """), {"operation": busy_initial["creation_operation_id"]})
        busy_unknown = False
        try:
            await ensure_hekate(factory, runtime, busy_actor, hekate_config)
        except UnknownExecution:
            busy_unknown = True
        busy_final = await create_journal_snapshot(busy_owner)
        async with factory() as uow:
            lease_after = await uow.agents.get_lease(busy_record.registry_id)
            hold_after = await uow.agents.active_execution_hold(busy_record.registry_id, lock=True)
            await uow.commit()
        if (
            not busy_unknown or busy_final["state"] != "COMPLETED"
            or busy_final["intended_state"] != "BUSY"
            or busy_final["provider_agent_id"] != str(busy_record.provider_id)
            or lease_after is None or busy_lease is None
            or (lease_after.owner, lease_after.fence, lease_after.expires_at) != (
                busy_lease.owner, busy_lease.fence, busy_lease.expires_at,
            )
            or hold_before != hold_after
        ):
            raise AssertionError("create journal completion changed BUSY registry or its lease/execution hold")

        report["persistent_hekate_create_journal"] = {
            "passed": True,
            "normal_generation": main_create_before,
            "completed_replay": {
                "provider_agent_id": str(main_record.provider_id), "same_journal_observation": True,
                "same_completion_timestamp": True, "new_create_calls": 0, "new_owner_list_calls": 0,
            },
            "restart_replay": {
                "same_provider_agent_id": True, "journal_unchanged": True,
                "new_create_calls": 0, "new_owner_list_calls": 0,
            },
            "legacy_incomplete_journal": {
                "before": legacy_before, "after": legacy_after,
                "runtime_owner_tag_list_calls": 1, "new_create_calls": 0,
                "replay_unchanged": True,
            },
            "binding_journal_transaction_rollback": {
                "failure_injected_after_binding_before_completion": bool(injected_completion_failure["fired"]),
                "rolled_back_state": rollback_before_recovery,
                "recovered_state": rollback_after_recovery,
                "new_create_calls_during_recovery": 0,
            },
            "create_response_lost": {
                "before_recovery": lost_before_recovery, "after_recovery": lost_after_recovery,
                "create_invocation_delta_for_scenario": lost_create_call_delta,
                "actual_owner_tag_query": True,
            },
            "postgres_concurrent_ensure": {
                "competing_call_unknown_while_first_create_waited": concurrent_second_unknown,
                "same_created_registry_provider_id": str(concurrent_record.provider_id),
                "confirmed_owned_agent_count": len(concurrent_owned),
                "new_create_calls": 1,
                "started_marker_prevented_second_create": True,
            },
            "provider_binding_mismatch": {
                "rejected": mismatch_rejected, "rejected_state": mismatch_after_rejection,
                "recovered_after_fixture_restore": mismatch_final,
                "owned_agent_query_response_lost": {
                    "rejected_as_unconfirmed": lookup_failure_rejected,
                    "after_failure": lookup_failure_state,
                    "after_later_confirmed_query": lookup_recovered_state,
                },
            },
            "busy_registry_preserved": {
                "ensure_waited": busy_unknown, "journal_completed": busy_final["state"] == "COMPLETED",
                "registry_state": busy_final["intended_state"],
                "provider_agent_id_unchanged": busy_final["provider_agent_id"] == str(busy_record.provider_id),
                "lease_owner": lease_after.owner if lease_after else None,
                "lease_fence": lease_after.fence if lease_after else None,
                "lease_expiry_unchanged": bool(lease_after and busy_lease and lease_after.expires_at == busy_lease.expires_at),
                "active_execution_hold_unchanged": hold_before == hold_after,
            },
            "create_operations_have_no_provider_calls_or_budget_reservations": all(
                int(snapshot[key]) == 0
                for snapshot in (
                    main_create_before, rollback_after_recovery, lost_after_recovery,
                    concurrent_after, mismatch_final, busy_final,
                )
                for key in ("provider_calls", "reservations", "ledger_rows")
            ),
            "real_pinned_runtime_create_calls": int(fault_state["hekate_create_calls"]),
            "owner_scoped_agent_list_calls": dict(fault_state["agent_list_calls_by_owner"]),
            "targeted_agent_get_calls": dict(fault_state["agent_observe_calls_by_id"]),
            "provider_inference_requests_added": 0,
        }
        if not report["persistent_hekate_create_journal"]["create_operations_have_no_provider_calls_or_budget_reservations"]:
            raise AssertionError("agent.create lifecycle operations acquired inference accounting effects")

        # A separate Task proves that an exhausted Task budget rejects the
        # combined Critic+synthesis hold before creating a registry or intent.
        normal_provider_requests = fake.count()
        from hekate.application.tasks import submit
        from hekate.domain.models import UserMessage
        budget_task_receipt = await submit(factory, actor, UserMessage(
            text="Can this seal rule be used for this decision?",
            topic_id="phase5a-main-topic", evidence_refs=(evidence_id,),
        ), f"phase5a-{run_id}-budget-denial", hekate_config)
        budget_task_id = str(budget_task_receipt["task_id"])
        budget_result_entered = asyncio.Event()
        release_budget_result = asyncio.Event()
        budget_result_inbox = None
        budget_result_paused = False

        async def hold_budget_result_before_adoption(repository, inbox_id):
            nonlocal budget_result_inbox, budget_result_paused
            result = await original_lock_turn_result(repository, inbox_id)
            if (
                result is None or budget_result_paused or result["task_id"] != budget_task_id
                or not isinstance(result["proposal"], dict)
                or result["proposal"].get("action") != "spawn"
            ):
                return result
            state = (await repository.connection.execute(text(
                "SELECT execution_state FROM operations WHERE id=:operation"
            ), {"operation": result["operation_id"]})).scalar_one()
            if state == "QUIESCENT":
                budget_result_paused = True
                budget_result_inbox = inbox_id
                budget_result_entered.set()
                await release_budget_result.wait()
            return result

        PostgresKnowledgeRepository.lock_turn_result = hold_budget_result_before_adoption
        budget_stop, budget_worker = await _start_worker(container)
        worker_runs.append((budget_stop, budget_worker))
        try:
            await asyncio.wait_for(budget_result_entered.wait(), 120)
            required_critic_hold = _reservation_amount(critic_config)
            async with engine.begin() as connection:
                budget_account = (await connection.execute(text("""
                    SELECT id, spent_amount, held_amount
                    FROM budget_accounts WHERE scope_kind='TASK' AND scope_ref=:task
                """), {"task": budget_task_id})).mappings().one()
                spent_amount = Decimal(budget_account["spent_amount"])
                held_amount = Decimal(budget_account["held_amount"])
                remaining_before_spawn = spent_amount + held_amount + Decimal("0.001")
                available_before_spawn = remaining_before_spawn - spent_amount - held_amount
                if available_before_spawn >= required_critic_hold:
                    raise AssertionError("budget fixture still has enough credit for a Critic hold")
                await connection.execute(text(
                    "UPDATE budget_accounts SET limit_amount=:limit WHERE id=:id"
                ), {"limit": remaining_before_spawn, "id": budget_account["id"]})
            release_budget_result.set()

            budget_end = asyncio.get_running_loop().time() + 120
            budget_terminal = None
            while asyncio.get_running_loop().time() < budget_end:
                async with engine.connect() as connection:
                    budget_terminal = (await connection.execute(text("""
                        SELECT t.status, t.critic_agents, t.review_rounds, r.processing_state, r.rejection_reason
                        FROM tasks t JOIN turn_results r ON r.task_id=t.id
                        WHERE t.id=:task
                    """), {"task": budget_task_id})).mappings().one_or_none()
                if budget_terminal and budget_terminal["status"] == "FAILED" and budget_terminal["processing_state"] == "REJECTED":
                    break
                await asyncio.sleep(0.1)
            else:
                raise TimeoutError("insufficient-budget spawn did not converge without a Critic")
            await _stop_worker(budget_stop, budget_worker)
            worker_runs.remove((budget_stop, budget_worker))
        finally:
            release_budget_result.set()
            PostgresKnowledgeRepository.lock_turn_result = original_lock_turn_result
            if not budget_worker.done():
                budget_stop.set()
                await asyncio.gather(budget_worker, return_exceptions=True)
            if (budget_stop, budget_worker) in worker_runs:
                worker_runs.remove((budget_stop, budget_worker))

        budget_counts = await _counts(engine, budget_task_id, scope_text)
        async with engine.connect() as connection:
            create_intents = await connection.scalar(text("""
                SELECT count(*) FROM outbox
                WHERE kind='critic_create' AND payload->>'task_id'=:task
            """), {"task": budget_task_id})
            child_reservations = await connection.scalar(text("""
                SELECT count(*) FROM budget_reservations br JOIN operations o ON o.id=br.operation_id
                WHERE o.task_id=:task AND o.kind IN ('critic.review','hekate.synthesis')
            """), {"task": budget_task_id})
        budget_denial_passed = (
            budget_terminal["rejection_reason"] == "budget_critic_and_synthesis_unavailable"
            and budget_counts["workflows"] == 0 and budget_counts["critics"] == 0
            and budget_counts["critic_agents"] == 0 and budget_counts["review_rounds"] == 0
            and int(create_intents) == 0 and int(child_reservations) == 0
            and budget_counts["calls"] == 1 and fake.count() == normal_provider_requests + 1
        )
        report["budget_denial_scenario"] = {
            "passed": budget_denial_passed,
            "task_id": budget_task_id,
            "planning_result_inbox_id": budget_result_inbox,
            "task_status": budget_terminal["status"],
            "result_state": budget_terminal["processing_state"],
            "rejection_reason": budget_terminal["rejection_reason"],
            "available_task_budget_before_spawn": str(available_before_spawn),
            "required_critic_envelope": str(required_critic_hold),
            "provider_requests_for_scenario": fake.count() - normal_provider_requests,
            "critic_workflows": budget_counts["workflows"],
            "critic_registries": budget_counts["critics"],
            "critic_agent_counter": budget_counts["critic_agents"],
            "review_round_counter": budget_counts["review_rounds"],
            "child_reservations": int(child_reservations),
            "critic_create_intents": int(create_intents),
            "provider_calls_for_scenario": budget_counts["calls"],
            "position_versions": budget_counts["positions"],
            "task_responses": budget_counts["responses"],
        }
        if not budget_denial_passed:
            raise AssertionError("insufficient Task budget produced a Critic workflow, hold, or create intent")
        budget_provider_requests = fake.count() - normal_provider_requests

        failure_scenarios: dict[str, object] = {}
        worker_name = settings.worker_id
        hekate_registry_id = RegistryId(str(hekate_state["id"]))

        # A busy registry and an unavailable lease are both temporary waits.
        # The exact same durable synthesis step is resumed once each blocker clears.
        busy_task = await submit_scenario_task("busy-lease-resume")
        busy_gate = await gate_worker_at(busy_task, "SYNTHESIS_PENDING")
        busy_before = await scenario_snapshot(str(busy_task["task_id"]))
        busy_provider_before = fake.count()
        async with factory() as uow:
            await uow.agents.set_registry_busy(
                hekate_registry_id, str(report["workflow"]["planning_attempt_id"]),
            )
            await uow.commit()
        busy_leases = await original_prepare(
            factory, runtime, actor, hekate_config, critic_config, worker_name,
            archive_root=archive_root,
        )
        async with factory() as uow:
            await uow.agents.set_registry_ready(hekate_registry_id)
            contender_lease = await uow.agents.acquire_lease(hekate_registry_id, f"lease-contender-{run_id}", 60)
            if contender_lease is None:
                raise AssertionError("lease contention fixture failed to acquire the real PostgreSQL lease")
            await uow.commit()
        lease_wait_leases = await original_prepare(
            factory, runtime, actor, hekate_config, critic_config, worker_name,
            archive_root=archive_root,
        )
        async with factory() as uow:
            await uow.agents.release_lease(contender_lease)
            await uow.commit()
        busy_after_waits = await scenario_snapshot(str(busy_task["task_id"]))
        if (
            busy_leases or lease_wait_leases or fake.count() != busy_provider_before
            or busy_before != busy_after_waits
            or busy_after_waits["workflow"]["stage"] != "SYNTHESIS_PENDING"
        ):
            raise AssertionError("BUSY or lease contention changed the pending synthesis workflow")
        await finish_gated_worker(busy_gate)
        busy_final = await scenario_snapshot(str(busy_task["task_id"]))
        busy_task_requests = [item for item in observations if item["task_id"] == busy_task["task_id"]]
        if (
            busy_final["task"]["status"] != "COMPLETED"
            or busy_final["workflow"]["stage"] != "COMPLETE"
            or busy_final["position_versions"] != 1
            or [item["stage"] for item in busy_task_requests] != ["planning", "critic_review", "synthesis"]
        ):
            raise AssertionError("the original synthesis did not resume exactly once after temporary waits")
        failure_scenarios["busy_and_lease_wait"] = {
            "passed": True, "task_id": busy_task["task_id"],
            "workflow_before_waits": busy_before, "workflow_after_waits": busy_after_waits,
            "final": busy_final, "provider_requests_during_waits": 0,
            "task_provider_requests": [item["stage"] for item in busy_task_requests],
            "same_attempt_and_operation_resumed": (
                busy_before["workflow"]["synthesis_operation_id"]
                == busy_final["workflow"]["synthesis_operation_id"]
                and busy_before["workflow"]["stage"] == "SYNTHESIS_PENDING"
            ),
        }

        # Cancel, deadline, and revision changes are their own terminal/superseded
        # transitions. They must seal only unadmitted child steps and retire the Critic.
        from hekate.application.tasks import cancel, revise
        from hekate.domain.models import InputChange
        from hekate.domain.types import StopReason

        async def stop_waiting_workflow(label: str, stop_kind: str) -> dict[str, object]:
            selected_task = await submit_scenario_task(label)
            gate = await gate_worker_at(selected_task, "CRITIC_READY")
            provider_before_stop = fake.count()
            if stop_kind == "cancel":
                await cancel(factory, actor, TaskId(str(selected_task["task_id"])), StopReason.USER_CANCELLED)
            elif stop_kind == "deadline":
                async with engine.begin() as connection:
                    await connection.execute(text(
                        "UPDATE tasks SET deadline=now() - interval '1 second' WHERE id=:task"
                    ), {"task": selected_task["task_id"]})
            elif stop_kind == "revision":
                await revise(
                    factory, actor, TaskId(str(selected_task["task_id"])), 1,
                    InputChange(text=f"Revised {label} question", expected_revision=1, constraints={}),
                )
            else:
                raise AssertionError(f"unknown workflow stop scenario {stop_kind}")
            await finish_gated_worker(gate)
            final = await scenario_snapshot(str(selected_task["task_id"]))
            expected = {
                "cancel": ("CANCELLED", "USER_CANCELLED", 1),
                "deadline": ("FAILED", "DEADLINE", 1),
                "revision": ("WAITING", None, 2),
            }[stop_kind]
            if (
                fake.count() != provider_before_stop
                or final["task"]["status"] != expected[0]
                or final["task"]["stop_reason"] != expected[1]
                or final["task"]["input_revision"] != expected[2]
                or final["response"] is not None or final["position_versions"] != 0
                or final["workflow"]["stage"] != "COMPLETE"
                or final["workflow"]["critic_state"] != "DELETED"
                or final["task"]["critic_agents"] != 1
            ):
                raise AssertionError(f"{stop_kind} did not preserve its workflow stop semantics: {final}")
            return {
                "passed": True, "task_id": selected_task["task_id"],
                "provider_requests_after_stop": fake.count() - provider_before_stop,
                "final": final,
            }

        failure_scenarios["cancel"] = await stop_waiting_workflow("waiting-cancel", "cancel")
        failure_scenarios["deadline"] = await stop_waiting_workflow("waiting-deadline", "deadline")
        failure_scenarios["revision_superseded"] = await stop_waiting_workflow("waiting-revision", "revision")

        # Evidence expiry at CRITIC_READY. First fail after all SQL effects but
        # before commit; verify PostgreSQL rolled back every effect, then let the
        # real worker retry and retire the actual pinned-runtime Critic.
        critic_expiry_task = await submit_scenario_task("evidence-expired-before-critic")
        critic_expiry_gate = await gate_worker_at(critic_expiry_task, "CRITIC_READY")
        critic_expiry_provider_before = fake.count()
        critic_expiry = await expire_scenario_evidence(critic_expiry_task)
        original_append_audit = PostgresDeliveryRepository.append_audit
        injected_audit_failure = {"fired": False}

        async def fail_policy_audit_after_insert(repository, event):
            await original_append_audit(repository, event)
            if event.get("event_kind") == "critic.workflow_policy_stopped" and not injected_audit_failure["fired"]:
                injected_audit_failure["fired"] = True
                raise RuntimeError("injected failure after workflow failure effects, before commit")

        PostgresDeliveryRepository.append_audit = fail_policy_audit_after_insert
        try:
            try:
                await original_prepare(
                    factory, runtime, actor, hekate_config, critic_config, worker_name,
                    archive_root=archive_root,
                )
            except RuntimeError as error:
                if "injected failure" not in str(error):
                    raise
            else:
                raise AssertionError("failure injection did not interrupt failure convergence before commit")
        finally:
            PostgresDeliveryRepository.append_audit = original_append_audit
        rollback_state = await scenario_snapshot(str(critic_expiry_task["task_id"]))
        if (
            not injected_audit_failure["fired"]
            or rollback_state["task"]["status"] != "WAITING"
            or rollback_state["workflow"]["stage"] != "CRITIC_READY"
            or rollback_state["response"] is not None or rollback_state["position_versions"] != 0
            or rollback_state["workflow"]["delete_operation_id"] is not None
            or rollback_state["workflow"]["critic_state"] != "READY"
            or any(item["status"] != "RESERVED" for item in rollback_state["reservations"])
            or rollback_state["critic_delete_outbox_rows"] != 0
        ):
            raise AssertionError(f"failure-convergence exception left partial PostgreSQL effects: {rollback_state}")
        await finish_gated_worker(critic_expiry_gate)
        critic_expiry_final = await assert_policy_failure(critic_expiry_task, "evidence_unavailable")
        before_failure_replay = await scenario_snapshot(str(critic_expiry_task["task_id"]))
        replay_leases = await original_prepare(
            factory, runtime, actor, hekate_config, critic_config, worker_name,
            archive_root=archive_root,
        )
        after_failure_replay = await scenario_snapshot(str(critic_expiry_task["task_id"]))
        if replay_leases or before_failure_replay != after_failure_replay:
            raise AssertionError("repeated permanent failure handling added response, ledger, reservation, or delete effects")
        if fake.count() != critic_expiry_provider_before:
            raise AssertionError("expired Evidence reached the provider after CRITIC_READY")
        failure_scenarios["evidence_expired_critic_ready"] = {
            "passed": True, "task_id": critic_expiry_task["task_id"],
            "provider_requests_before_expiry": critic_expiry_provider_before,
            "provider_requests_after_failure": fake.count(), "evidence": critic_expiry,
            "atomic_rollback_after_injected_failure": rollback_state,
            "final": critic_expiry_final,
            "replay_effects_unchanged": before_failure_replay == after_failure_replay,
            "failure_transaction_inbox_and_operation_source_preserved": True,
        }

        # Evidence expires after a Critic Conclusion is durable but before the
        # HEKATE synthesis admission. No third provider request is allowed.
        synthesis_task_provider_base = fake.count()
        synthesis_expiry_task = await submit_scenario_task("evidence-expired-before-synthesis")
        synthesis_gate = await gate_worker_at(synthesis_expiry_task, "SYNTHESIS_PENDING")
        synthesis_provider_before = fake.count()
        synthesis_expiry = await expire_scenario_evidence(synthesis_expiry_task)
        synthesis_workflow = await finish_gated_worker(synthesis_gate)
        synthesis_final = await assert_policy_failure(synthesis_expiry_task, "evidence_unavailable")
        if (
            synthesis_provider_before - synthesis_task_provider_base != 2
            or fake.count() != synthesis_provider_before
            or not synthesis_workflow.get("critic_conclusion_id")
            or synthesis_final["reservations"] == []
            or any(
                item["status"] not in {"SETTLED", "RELEASED"}
                for item in synthesis_final["reservations"]
            )
        ):
            raise AssertionError("synthesis Evidence expiry repeated provider work or retained an unallocated hold")
        failure_scenarios["evidence_expired_synthesis_pending"] = {
            "passed": True, "task_id": synthesis_expiry_task["task_id"],
            "workflow_before_rejection": synthesis_workflow,
            "provider_requests_before_expiry": synthesis_provider_before,
            "provider_requests_after_failure": fake.count(), "evidence": synthesis_expiry,
            "final": synthesis_final,
            "review_spend_preserved": any(
                item["kind"] == "critic.review" and item["status"] == "SETTLED"
                for item in synthesis_final["reservations"]
            ),
            "synthesis_hold_released": any(
                item["kind"] == "hekate.synthesis" and item["status"] == "RELEASED"
                for item in synthesis_final["reservations"]
            ),
        }

        # An UNKNOWN child execution remains a hold across worker restart. This
        # is explicitly a control-plane fault fixture with no synthetic call or
        # usage observation attached; it is not reported as an actual provider send.
        unknown_task = await submit_scenario_task("unknown-review-preserved")
        unknown_gate = await gate_worker_at(unknown_task, "CRITIC_READY")
        unknown_before_requests = fake.count()
        unknown_state = await _workflow_state(engine, str(unknown_task["task_id"]))
        unknown_operation_id = str(unknown_state["review_operation_id"])
        unknown_registry_id = RegistryId(str(unknown_state["critic_registry_id"]))
        async with factory() as uow:
            await uow.agents.create_execution_hold(unknown_registry_id, OperationId(unknown_operation_id))
            await uow.commit()
        async with engine.begin() as connection:
            await connection.execute(text("""
                UPDATE operations SET state='UNKNOWN', dispatch_state='UNKNOWN', execution_state='UNKNOWN',
                    last_error='phase5a control-plane UNKNOWN preservation fixture'
                WHERE id=:operation
            """), {"operation": unknown_operation_id})
            await connection.execute(text("""
                UPDATE agent_execution_holds SET state='UNKNOWN'
                WHERE operation_id=:operation AND quiescent_at IS NULL
            """), {"operation": unknown_operation_id})
        unknown_attempt_leases = await original_prepare(
            factory, runtime, actor, hekate_config, critic_config, worker_name,
            archive_root=archive_root,
        )
        await finish_gated_worker(unknown_gate, complete=False)
        unknown_wait_state = await scenario_snapshot(str(unknown_task["task_id"]))
        if (
            unknown_attempt_leases or fake.count() != unknown_before_requests
            or unknown_wait_state["task"]["status"] != "WAITING"
            or unknown_wait_state["workflow"]["stage"] != "CRITIC_READY"
            or unknown_wait_state["response"] is not None
            or unknown_wait_state["workflow"]["critic_state"] != "READY"
            or unknown_wait_state["reservations"][0]["status"] != "RESERVED"
        ):
            raise AssertionError(f"UNKNOWN review was converted into a permanent failure: {unknown_wait_state}")
        unknown_recovered = asyncio.Event()

        async def observe_unknown_after_restart(*args, **kwargs):
            result = await original_prepare(*args, **kwargs)
            current = await _workflow_state(engine, str(unknown_task["task_id"]))
            if current.get("stage") == "CRITIC_READY":
                unknown_recovered.set()
            return result

        worker_service.prepare_critic_workflow_steps = observe_unknown_after_restart
        unknown_restart_stop, unknown_restart_task = await _start_worker(container)
        worker_runs.append((unknown_restart_stop, unknown_restart_task))
        await asyncio.wait_for(unknown_recovered.wait(), 60)
        await _stop_worker(unknown_restart_stop, unknown_restart_task)
        worker_runs.remove((unknown_restart_stop, unknown_restart_task))
        worker_service.prepare_critic_workflow_steps = original_prepare
        async with engine.connect() as connection:
            unknown_db_state = (await connection.execute(text("""
                SELECT o.state, o.execution_state, h.state AS hold_state, h.quiescent_at,
                       (SELECT count(*) FROM provider_calls p WHERE p.operation_id=o.id) AS provider_call_rows
                FROM operations o JOIN agent_execution_holds h ON h.operation_id=o.id
                WHERE o.id=:operation
            """), {"operation": unknown_operation_id})).mappings().one()
        unknown_final = await scenario_snapshot(str(unknown_task["task_id"]))
        if (
            fake.count() != unknown_before_requests
            or unknown_db_state["state"] != "UNKNOWN"
            or unknown_db_state["execution_state"] != "UNKNOWN"
            or unknown_db_state["hold_state"] != "UNKNOWN"
            or unknown_db_state["quiescent_at"] is not None
            or int(unknown_db_state["provider_call_rows"]) != 0
            or unknown_final["task"]["status"] != "WAITING"
            or unknown_final["workflow"]["critic_state"] != "READY"
        ):
            raise AssertionError("worker restart bypassed or erased an unresolved UNKNOWN execution")
        failure_scenarios["unknown_preserved_across_restart"] = {
            "passed": True, "task_id": unknown_task["task_id"],
            "review_operation_id": unknown_operation_id,
            "control_plane_state": dict(unknown_db_state), "final": unknown_final,
            "fixture_is_synthetic_unresolved_state": True,
            "provider_call_rows_for_fixture": int(unknown_db_state["provider_call_rows"]),
            "provider_requests_during_wait_and_restart": 0,
            "held_budget_preserved": True, "critic_not_deleted": True,
        }

        # The current authorization epoch is re-read for failure bookkeeping;
        # stale worker identity cannot keep the Task waiting or block cleanup.
        authorization_task = await submit_scenario_task("authorization-epoch-changed")
        authorization_gate = await gate_worker_at(authorization_task, "CRITIC_READY")
        authorization_provider_before = fake.count()
        async with engine.begin() as connection:
            await connection.execute(text(
                "UPDATE authorization_scopes SET authz_epoch=authz_epoch + 1 WHERE id=:scope"
            ), {"scope": scope_text})
        await finish_gated_worker(authorization_gate)
        authorization_final = await assert_policy_failure(authorization_task, "authorization_changed")
        if fake.count() != authorization_provider_before:
            raise AssertionError("authorization change permitted a Critic provider request")
        failure_scenarios["authorization_epoch_changed"] = {
            "passed": True, "task_id": authorization_task["task_id"],
            "worker_actor_epoch": actor.authz_epoch, "stored_authz_epoch": actor.authz_epoch + 1,
            "provider_requests_after_change": fake.count() - authorization_provider_before,
            "final": authorization_final,
            "stale_actor_did_not_block_policy_response_or_critic_cleanup": True,
        }

        report["critic_workflow_stop_and_wait_regressions"] = failure_scenarios
        if not all(item.get("passed") for item in failure_scenarios.values()):
            raise AssertionError("a Critic workflow stop/wait regression did not pass")
        report["fake_provider_requests"] = fake.count()
        report["provider_calls"] = {"real": 0, "fake": fake.count(), "compaction": 0}
        report["dispatch_safety"]["fake_provider_requests"] = fake.count()
        report["provider_request_scenarios"] = {
            "normal_planning_critic_synthesis": 3,
            "insufficient_budget_planning_only": budget_provider_requests,
            "temporary_busy_task_planning_critic_synthesis": 3,
            "permanent_rejection_and_stop_scenarios": {
                key: sum(item["task_id"] == value.get("task_id") for item in observations)
                if isinstance(value, dict) else 0
                for key, value in failure_scenarios.items()
            },
        }
        async with engine.connect() as connection:
            unresolved_operation_rows = (await connection.execute(text("""
                SELECT id, kind, state, execution_state, dispatch_state, task_id,
                       observation->>'deferred_task_id' AS deferred_task_id
                FROM operations
                WHERE state='UNKNOWN' OR execution_state='UNKNOWN' OR dispatch_state='UNKNOWN'
                ORDER BY id
            """))).mappings().all()
            pending_unadmitted_rows = (await connection.execute(text("""
                SELECT id, kind, state, execution_state, dispatch_state,
                       observation->>'deferred_task_id' AS deferred_task_id,
                       observation->>'deferred_stage_hash' AS deferred_stage_hash
                FROM operations WHERE state='CLAIMED' AND task_id IS NULL
                  AND observation ? 'deferred_task_id'
                ORDER BY id
            """))).mappings().all()
            pending_lifecycle_rows = (await connection.execute(text("""
                SELECT id, kind, state, execution_state, dispatch_state
                FROM operations WHERE state='CLAIMED' AND task_id IS NULL
                  AND kind IN ('agent.create','critic.create','critic.delete')
                ORDER BY id
            """))).mappings().all()
            unsettled_reservation_rows = (await connection.execute(text("""
                SELECT br.operation_id, o.kind, br.status, br.amount::text AS amount,
                       COALESCE(o.task_id, w.task_id, o.observation->>'deferred_task_id') AS task_id,
                       COALESCE(ra.held_amount, 0)::text AS held
                FROM budget_reservations br JOIN operations o ON o.id=br.operation_id
                LEFT JOIN critic_workflows w ON o.id IN (w.review_operation_id, w.synthesis_operation_id)
                LEFT JOIN LATERAL (
                    SELECT sum(held_amount) AS held_amount
                    FROM reservation_accounts WHERE reservation_id=br.id
                ) ra ON TRUE
                WHERE br.status IN ('RESERVED','PENDING_SETTLEMENT')
                ORDER BY br.operation_id
            """))).mappings().all()
        report["accounting"] = {
            "workflow_operation_statuses": lifecycle_states,
            "pending_or_unknown_operations": [dict(item) for item in unresolved_operation_rows],
            "pending_unadmitted_workflow_operations": [dict(item) for item in pending_unadmitted_rows],
            "pending_lifecycle_operations": [dict(item) for item in pending_lifecycle_rows],
            "unsettled_reservations": [dict(item) for item in unsettled_reservation_rows],
            "unknown_fixture_explanation": (
                "The remaining UNKNOWN review/hold is an explicit control-plane preservation fixture with zero provider-call rows; "
                "it was not normalized or given fabricated termination or usage evidence."
            ),
            "synthetic_fake_pricing_only": True,
        }
        report["preserved_prior_state"] = {
            "phase4_issued_allocated_fixture": {
                "source_artifact": "integration/runtime/artifacts/p4-20261003T165018Z-b67da98c.json",
                "previously_reported_state": "one ISSUED/ALLOCATED provider call without execution evidence",
                "current_probe_database_touched": False,
                "settlement_or_usage_synthesized": False,
            },
            "position_projection": {
                "state": "PENDING_UNSUPPORTED",
                "applied_watermark": 0,
                "changed_by_create_journal": False,
            },
            "unknown_fixture": {
                "state": "UNKNOWN with review and synthesis holds preserved",
                "real_provider_calls_for_fixture": 0,
                "changed_by_create_journal": False,
            },
            "production_dispatch": "blocked",
            "g7_g8": "deferred",
        }
        report["overall_status"] = "pass"
        report["execution"]["result"] = "passed"
        report["code_fingerprint_sha256"] = await p4._code_identity()
        return report
    except Exception as error:
        report["failure"] = {"type": type(error).__name__, "message": str(error)[:500], "traceback": traceback.format_exc()[-5000:]}
        report["provider_fixture_errors"] = response_errors if "response_errors" in locals() else []
        report["fake_provider_requests"] = fake.count() if fake is not None else 0
        report["provider_observations"] = observations if "observations" in locals() else []
        report["bridge_stderr_tail"] = bridge.stderr_text[-2_000:] if bridge is not None else ""
        report["execution"]["result"] = "failed"
        report["code_fingerprint_sha256"] = await p4._code_identity()
        return report
    finally:
        spawn_race_active = False
        release_workflow_insert.set()
        release_spawn_race.set()
        PostgresCriticWorkflowRepository.insert = original_workflow_insert
        PostgresDeliveryRepository.lock_operation = original_lock_operation
        PostgresKnowledgeRepository.lock_turn_result = original_lock_turn_result
        worker_service.maintain_critic_workflows = original_maintain
        worker_service.prepare_critic_workflow_steps = original_prepare
        if spawn_approval_tasks:
            await asyncio.gather(*spawn_approval_tasks, return_exceptions=True)
        for stop, task in worker_runs:
            if not task.done():
                stop.set()
                await asyncio.gather(task, return_exceptions=True)
        if bridge is not None:
            await bridge.close()
        if fake is not None:
            fake.stop()
        if gateway_server is not None:
            gateway_server.should_exit = True
            await asyncio.gather(gateway_task, return_exceptions=True)
        if sandbox is not None:
            sandbox.stop()
            try:
                sandbox.clear_state()
            except Exception:
                pass
        if engine is not None:
            await engine.dispose()
        temp.cleanup()


async def _wait_for_operation(engine, task_id: str, kind: str, state: str, timeout: float) -> None:
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        async with engine.connect() as connection:
            found = await connection.scalar(text("""
                SELECT EXISTS(
                    SELECT 1 FROM operations o
                    WHERE o.kind=:kind AND o.state=:state
                      AND (o.task_id=:task OR o.id IN (
                          SELECT create_operation_id FROM critic_workflows WHERE task_id=:task
                          UNION ALL
                          SELECT delete_operation_id FROM critic_workflows WHERE task_id=:task
                      ))
                )
            """), {"task": task_id, "kind": kind, "state": state})
        if found:
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(f"operation {kind} did not reach {state}")


async def _operation_states(engine, task_id: str) -> list[dict[str, object]]:
    async with engine.connect() as connection:
        rows = (await connection.execute(text("""
            SELECT id, kind, state, execution_state FROM operations
            WHERE task_id=:task AND kind IN ('critic.create','critic.review','hekate.synthesis','critic.delete')
            ORDER BY created_at, id
        """), {"task": task_id})).mappings().all()
        return [dict(row) for row in rows]


async def _critic_output_boundary_checks(factory, task_id: str, hekate_registry_id: str) -> dict[str, bool]:
    async with factory() as uow:
        workflow = await uow.critic_workflows.get(TaskId(task_id))
        if workflow is None or workflow.critic_conclusion_id is None:
            raise AssertionError("durable Critic conclusion is required for output-boundary checks")
        operation = await uow.delivery.lock_operation(workflow.review_operation_id)
        conclusion = await uow.knowledge.get_conclusion(str(workflow.critic_conclusion_id))
        manifest_row = await uow.knowledge.get_context_manifest(workflow.review_operation_id)
        await uow.rollback()
    if conclusion is None or manifest_row is None:
        raise AssertionError("Critic operation binding, conclusion, or context manifest was not durable")
    trusted_binding = _restore_binding(operation)
    manifest = manifest_row["manifest"]
    base = {
        "schema_version": "1",
        "conclusion": conclusion["capsule"],
    }
    valid = parse_critic_turn_output(json.dumps(base, separators=(",", ":")).encode())
    results_app.validate_critic_turn_output(valid, trusted_binding, manifest)

    spoofed = json.loads(json.dumps(base))
    spoofed["conclusion"]["agent_id"] = hekate_registry_id
    spoof_rejected = False
    try:
        results_app.validate_critic_turn_output(
            parse_critic_turn_output(json.dumps(spoofed, separators=(",", ":")).encode()),
            trusted_binding, manifest,
        )
    except ValueError as error:
        spoof_rejected = str(error) == "conclusion_binding_mismatch"

    unprovided = json.loads(json.dumps(base))
    unprovided["conclusion"]["evidence_used"] = ["evidence-not-in-this-manifest"]
    evidence_rejected = False
    try:
        results_app.validate_critic_turn_output(
            parse_critic_turn_output(json.dumps(unprovided, separators=(",", ":")).encode()),
            trusted_binding, manifest,
        )
    except ValueError as error:
        evidence_rejected = str(error) == "evidence_not_provided"

    executable = json.loads(json.dumps(base))
    executable["proposal"] = {"action": "commit"}
    proposal_rejected = False
    try:
        parse_critic_turn_output(json.dumps(executable, separators=(",", ":")).encode())
    except ValueError:
        proposal_rejected = True
    return {
        "stored_critic_output_validates_against_trusted_binding": True,
        "critic_cannot_impersonate_persistent_hekate": spoof_rejected,
        "critic_cannot_claim_unprovided_evidence": evidence_rejected,
        "critic_output_schema_rejects_executable_proposal": proposal_rejected,
    }


async def _counts(engine, task_id: str, scope: str) -> dict[str, int]:
    async with engine.connect() as connection:
        row = (await connection.execute(text("""
            SELECT (SELECT count(*) FROM critic_workflows WHERE task_id=:task) AS workflows,
                   (SELECT count(*) FROM agent_registry WHERE task_id=:task AND role='critic') AS critics,
                   (SELECT count(*) FROM budget_reservations WHERE operation_id IN (
                       SELECT id FROM operations WHERE task_id=:task
                       UNION SELECT review_operation_id FROM critic_workflows WHERE task_id=:task
                       UNION SELECT synthesis_operation_id FROM critic_workflows WHERE task_id=:task
                   )) AS reservations,
                   (SELECT count(*) FROM outbox WHERE operation_id IN (
                       SELECT id FROM operations WHERE task_id=:task
                       UNION SELECT create_operation_id FROM critic_workflows WHERE task_id=:task
                       UNION SELECT delete_operation_id FROM critic_workflows WHERE task_id=:task AND delete_operation_id IS NOT NULL
                   )) AS outbox,
                   (SELECT count(*) FROM position_versions WHERE task_id=:task) AS positions,
                   (SELECT count(*) FROM position_commit_receipts WHERE scope=:scope) AS receipts,
                   (SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.task_id=:task) AS calls,
                   (SELECT count(*) FROM task_responses WHERE task_id=:task) AS responses,
                   (SELECT count(*) FROM dissent WHERE scope=:scope) AS dissent,
                   (SELECT critic_agents FROM tasks WHERE id=:task) AS critic_agents,
                   (SELECT review_rounds FROM tasks WHERE id=:task) AS review_rounds
        """), {"task": task_id, "scope": scope})).mappings().one()
        return {key: int(value) for key, value in row.items()}


async def _code_fingerprint() -> str:
    return await p4._code_identity()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.environ.get("HEKATE_TEST_DATABASE_URL", ""))
    parser.add_argument("--node-bin", default=os.environ.get("HEKATE_NODE_BIN", ""))
    parser.add_argument("--node-archive", type=Path, default=Path(os.environ.get("HEKATE_NODE_ARCHIVE", "/tmp/hekate-node-v22.19.0-linux-x64.tar.xz")))
    parser.add_argument("--image", default=f"hekate/letta-code-p1:{p3.p1.LOCK['app_server']['source_commit'][:8]}-{p3.p1.LOCK['patches'][0]['sha256'][:8]}")
    parser.add_argument("--artifact", type=Path)
    args = parser.parse_args()
    if not args.database_url or not args.node_bin:
        parser.error("provide --database-url and --node-bin")
    parsed = make_url(args.database_url)
    if not (parsed.database or "").startswith("hekate_phase5a_") or parsed.host not in {"127.0.0.1", "localhost"}:
        parser.error("probe requires an isolated loopback hekate_phase5a_* database")
    os.environ["PATH"] = f"{Path(args.node_bin).resolve().parent}{os.pathsep}{os.environ.get('PATH', '')}"
    cfg = p3.Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", args.database_url.replace("%", "%%"))
    os.environ["HEKATE_DATABASE_URL"] = args.database_url
    p3.command.upgrade(cfg, "head")
    run_id = f"p5a-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    artifact = args.artifact or ROOT / "integration/runtime/artifacts" / f"{run_id}.json"
    report = asyncio.run(_run(args.database_url, args.node_bin, args.image, args.node_archive, artifact, run_id))
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"artifact": artifact.relative_to(ROOT).as_posix(), "status": report["overall_status"], "real_provider_calls": 0}, sort_keys=True))
    return 0 if report["overall_status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
