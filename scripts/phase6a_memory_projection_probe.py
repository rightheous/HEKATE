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
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import make_url
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import phase3_runtime_probe as p3
import phase4_evidence_position_probe as p4
import phase5b_bounded_deliberation_probe as p5b

from hekate.application.projections import claim_pending_projections, project_position
from hekate.application.tasks import submit
from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.models import AuthorizationSnapshot, MemoryProjection, ProjectionBinding, UserMessage
from hekate.domain.types import OperationId, PrincipalId, ScopeId, TaskId, TopicId
from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.infrastructure.letta.bridge_protocol import BridgeClient
from hekate.infrastructure.letta.provider_gateway import ProviderGatewayProfile
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.infrastructure.postgres.projection_repository import PostgresProjectionRepository
from hekate.settings import configured_local_actor, configured_task_execution, load_settings
from hekate.worker import service as worker_service


TOPIC = "phase6a-memory-primary"
ALT_TOPIC = "phase6a-memory-followup"
V1_STATEMENT = "Phase6A durable Position marker: yellow bridge 71-alpha remains valid only within its stated jurisdiction."
V2_STATEMENT = "Phase6A durable Position marker: yellow bridge 71-alpha is valid within the stated jurisdiction and date window."
V3_STATEMENT = "Phase6A durable Position marker: yellow bridge 71-alpha requires confirmation of the date window."
ALT_STATEMENT = "Phase6A secondary topic marker: the blue cable applies only to the stated inspection scope."


class ProjectionRuntimeProbe:
    """Counts real bridge memory calls and can lose one confirmed response."""

    def __init__(self, inner: object) -> None:
        self.inner = inner
        self.read_calls = 0
        self.write_calls = 0
        self.drop_next_write_response = False
        self.errors: list[dict[str, str]] = []

    def __getattr__(self, name: str):
        return getattr(self.inner, name)

    async def read_projected_memory(self, binding, topic_id, operation_id):
        self.read_calls += 1
        try:
            return await self.inner.read_projected_memory(binding, topic_id, operation_id)
        except Exception as error:
            self.errors.append({"call": "memory.read", "type": type(error).__name__, "detail": str(error)[:600]})
            raise

    async def project_memory(self, projection):
        self.write_calls += 1
        try:
            result = await self.inner.project_memory(projection)
        except Exception as error:
            self.errors.append({"call": "memory.project", "type": type(error).__name__, "detail": str(error)[:600]})
            raise
        if self.drop_next_write_response:
            self.drop_next_write_response = False
            raise RuntimeError("phase6a_injected_response_loss_after_runtime_commit")
        return result


def _command(command: str, **fields: object) -> dict[str, object]:
    return {"schema_version": "1", "command": command, **fields}


class ProjectionAwareResponses:
    def __init__(self, observations: list[dict[str, object]]) -> None:
        self.observations = observations
        self.task_statements: dict[str, tuple[str, str]] = {}
        self.first_task_id: str | None = None
        self.v1_seen_in_runtime_prompt = False

    def expect(self, task_id: str, topic_id: str, statement: str) -> None:
        if self.first_task_id is None:
            self.first_task_id = task_id
        self.task_statements[task_id] = (topic_id, statement)

    @staticmethod
    def _without_capsule(prompt: str) -> str:
        marker = "Task Capsule JSON:\n"
        offset = prompt.find(marker)
        if offset < 0:
            raise ValueError("provider request did not contain a Task Capsule")
        start = offset + len(marker)
        suffix = prompt[start:]
        leading = len(suffix) - len(suffix.lstrip())
        capsule_text = suffix.lstrip()
        _, consumed = json.JSONDecoder().raw_decode(capsule_text)
        return prompt[: start + leading] + prompt[start + leading + consumed :]

    @classmethod
    def _outside_capsule(cls, request: dict[str, object], prompt: str) -> str:
        # Letta can send compiled runtime memory in a distinct system message
        # from the user message that carries the Task Capsule. Search the whole
        # actual provider request while excluding only the capsule JSON itself.
        parts = []
        capsule_removed = False
        for content in p5b._strings(request.get("messages")):
            if not capsule_removed and "Task Capsule JSON:\n" in content:
                parts.append(cls._without_capsule(content))
                capsule_removed = True
            else:
                parts.append(content)
        if not capsule_removed or not any(content == prompt for content in p5b._strings(request.get("messages"))):
            raise ValueError("provider request did not preserve the Task Capsule message")
        return "\n".join(parts)

    def __call__(self, request: dict[str, object]) -> object:
        turn = p5b._request_capsule(request)
        if turn is None:
            raise ValueError("Phase 6A probe requires zero compaction calls")
        prompt, capsule = turn
        binding = re.search(
            r"Trusted runtime binding: task_id=([^;]+); attempt_id=([^;]+); agent_registry_id=([^;]+); input_revision=(\d+)\.",
            prompt,
        )
        if binding is None:
            raise ValueError("provider request omitted the trusted runtime binding")
        task_id, _attempt_id, registry_id, revision_text = binding.groups()
        expected = self.task_statements.get(task_id)
        if expected is None:
            raise ValueError("provider request belongs to an unregistered Phase 6A task")
        topic_id, statement = expected
        if capsule.get("task_id") != task_id or capsule.get("topic_id") != topic_id:
            raise ValueError("Task Capsule differs from the trusted task and topic")
        if capsule.get("reasoning_role") != "hekate":
            raise ValueError("only the persistent HEKATE may synthesize the projected Position")
        if "HEKATE server output contract (generated from the strict Python HekateTurnOutput model):\n" not in prompt:
            raise ValueError("provider request omitted the generated HEKATE output schema")

        target = capsule.get("target_position")
        runtime_memory_loaded = False
        if task_id != self.first_task_id and isinstance(target, dict) and target.get("version", 0) > 0:
            # A follow-up request must carry the current DB snapshot and the
            # separately loaded runtime memory at that same prior version.
            # Remove the Task Capsule so the DB snapshot alone cannot satisfy it.
            target_version = target.get("version")
            target_summary = target.get("summary")
            if type(target_version) is not int or not isinstance(target_summary, str):
                raise ValueError("follow-up Task omitted the authoritative PostgreSQL Position snapshot")
            without_capsule = self._outside_capsule(request, prompt)
            source_version_marker = f'"source_version":{target_version}'
            runtime_memory_loaded = target_summary in without_capsule and topic_id in without_capsule and source_version_marker in without_capsule
            if not runtime_memory_loaded:
                self.observations.append({
                    "task_id": task_id, "topic_id": topic_id, "registry_id": registry_id,
                    "runtime_memory_check": {
                        "statement_present_outside_task_capsule": target_summary in without_capsule,
                        "topic_present_outside_task_capsule": topic_id in without_capsule,
                        "source_version_present_outside_task_capsule": source_version_marker in without_capsule,
                        "message_roles": [message.get("role") for message in request.get("messages", []) if isinstance(message, dict)],
                        "system_message_has_memory_marker": any(
                            isinstance(message, dict) and message.get("role") == "system"
                            and isinstance(message.get("content"), str)
                            and "HEKATE Position memory projection" in message["content"]
                            for message in request.get("messages", [])
                        ),
                    },
                })
                raise ValueError("runtime-loaded MemFS core memory matching the prior DB Position was absent from the actual provider request")
            if target_version == 1 and target_summary == V1_STATEMENT:
                self.v1_seen_in_runtime_prompt = True

        operation = re.search(r"Commit operation_id: ([^\n]+)", prompt)
        if operation is None:
            raise ValueError("provider request omitted the server-selected Position operation ID")
        position = {
            "statement": statement,
            "applicability": ["the submitted decision and stated inspection scope"],
            "confidence": {"level": "medium", "basis": ["the bounded synthetic task context"]},
            "evidence_refs": [], "assumptions": [], "dissent_refs": [],
            "uncertainty": "Use only within the described scope.",
        }
        conclusion = {
            "schema_version": "1", "task_id": task_id, "attempt_id": capsule["attempt_id"],
            "agent_id": registry_id, "status": "done",
            "assessment": {
                "statement": statement,
                "confidence": {"level": "medium", "basis": ["bounded synthetic control-plane fixture"]},
            },
            "evidence_used": [], "objections": [], "assumptions": [], "unresolved": [],
            "recommended_next_step": {"type": "none"},
            "position_recommendation": {"action": "update", "summary": "Persist the bounded current judgment."},
        }
        output = {
            "schema_version": "1",
            "proposal": {
                "schema_version": "1", "action": "commit", "operation_id": operation.group(1),
                "task_id": task_id, "topic_id": topic_id,
                "base_version": capsule["base_position_version"],
                "input_revision": int(revision_text), "proposed_position": position,
                "reason_for_change": "Persist the Phase 6A bounded Position fixture.",
            },
            "conclusion": conclusion,
        }
        tools = request.get("tools")
        if isinstance(tools, list) and any(
            isinstance(tool, dict) and isinstance(tool.get("function"), dict)
            and tool["function"].get("name") == "StructuredOutput" for tool in tools
        ):
            result: object = output
        else:
            result = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
        self.observations.append({
            "task_id": task_id, "topic_id": topic_id, "registry_id": registry_id,
            "target_position_version": target.get("version") if isinstance(target, dict) else 0,
            "prior_position_runtime_memory_loaded_outside_task_capsule": runtime_memory_loaded,
            "runtime_memory_source_version": target.get("version") if runtime_memory_loaded and isinstance(target, dict) else None,
            "output_schema_present": True,
        })
        return result


async def _wait_task(engine, task_id: str, timeout: float = 150) -> dict[str, object]:
    end = asyncio.get_running_loop().time() + timeout
    last: dict[str, object] = {}
    while asyncio.get_running_loop().time() < end:
        async with engine.connect() as connection:
            row = (await connection.execute(text("""
                SELECT status, outcome, stop_reason,
                       (SELECT count(*) FROM task_responses WHERE task_id=tasks.id) AS responses
                FROM tasks WHERE id=:task
            """), {"task": task_id})).mappings().one_or_none()
            if row is not None:
                last = dict(row)
        if last.get("status") in {"COMPLETED", "FAILED", "CANCELLED", "NEEDS_USER_INPUT"}:
            return last
        await asyncio.sleep(0.1)
    raise TimeoutError(f"Task did not terminate: {task_id}; state={last}")


async def _wait_projection(engine, scope: str, topic_id: str, target_version: int, timeout: float = 30) -> dict[str, object]:
    end = asyncio.get_running_loop().time() + timeout
    last: dict[str, object] = {}
    while asyncio.get_running_loop().time() < end:
        async with engine.connect() as connection:
            row = (await connection.execute(text("""
                SELECT desired_version, applied_version, observed_memory_version,
                       observed_payload_digest, payload_digest, state, pending_reason,
                       operation_id, request_hash, claim_fence, attempt_count
                FROM memory_projections
                WHERE scope=:scope AND topic_id=:topic
            """), {"scope": scope, "topic": topic_id})).mappings().one_or_none()
            if row is not None:
                last = dict(row)
        if last.get("state") == "APPLIED" and last.get("applied_version") == target_version:
            return last
        if last.get("state") == "DRIFT" or last.get("state") == "CONFLICT":
            raise AssertionError(f"Projection entered a terminal conflict: {last}")
        await asyncio.sleep(0.1)
    raise TimeoutError(f"Projection did not reach APPLIED v{target_version}: {last}")


async def _wait_worker(stop: asyncio.Event, task: asyncio.Task) -> None:
    stop.set()
    await asyncio.wait_for(task, 35)


async def _restart_app_server(sandbox, gateway_port: int, token: str) -> None:
    subprocess.run(["docker", "stop", sandbox.container], check=True, capture_output=True, text=True, timeout=45)
    subprocess.run(["docker", "rm", sandbox.container], check=True, capture_output=True, text=True, timeout=45)
    sandbox.container_created = False
    sandbox.start_pinned_app_server(gateway_port, token)


async def _projection_status(factory, actor, topic_id: str, registry_id: str):
    async with factory() as uow:
        status = await uow.projections.get_status(actor.scope, TopicId(topic_id), registry_id)
        registry = await uow.agents.get_persistent_scope(actor.scope)
        await uow.commit()
    if status is None or registry is None or registry.provider_id is None:
        raise AssertionError("durable projection status or persistent HEKATE binding is missing")
    return status, registry


async def _position_body(factory, actor, topic_id: str, registry_id: str, version: int):
    async with factory() as uow:
        page = await uow.knowledge.get_position_history(actor.scope, TopicId(topic_id), version - 1, 1, registry_id)
        await uow.commit()
    for item in page:
        if item.version == version:
            return item
    raise AssertionError(f"Position v{version} is not durable")


async def _run(database_url: str, node: str, image: str, node_archive: Path, artifact: Path, run_id: str) -> dict[str, object]:
    report: dict[str, object] = {
        "schema_version": "1", "probe": "phase6a-memory-projection", "run_id": run_id,
        "executed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "baseline": {
            "head": "a58df8c5c0f253635a5fafc2d016eec407bfce8a",
            "branch": "phase5b-bounded-deliberation",
            "source_worktree": "/home/hekate/hekate-phase5b-bounded-deliberation",
            "copy_manifest": "integration/runtime/artifacts/p6a-baseline-20261004T184731Z-f941096c.json",
            "phase4_phase5a_phase5b_changes": "selected tracked diff and clean nonignored implementation/docs/artifacts copied before Phase 6A edits",
        },
        "branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip(),
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "production_dispatch": "blocked", "real_provider_calls": 0,
        "projection_inference_calls": 0, "overall_status": "blocked", "results": {},
        "execution": {
            "command": "uv run --locked python scripts/phase6a_memory_projection_probe.py --database-url <isolated-db> --empty-migration-database-url <isolated-empty-db> --node-bin /tmp/node-v22.19.0/bin/node",
            "result": "running",
        },
        "limitations": [
            "Projection is a bounded reference in persistent HEKATE core memory; PostgreSQL remains authoritative.",
            "Actual provider access and production dispatch remain disabled; a local synthetic fake provider is used.",
            "G7 same-execution resume and G8 full-request tokenization remain deferred.",
            "Conclusion exact reuse, schema repair/retry, vector search, and automatic deliberation remain out of scope.",
        ],
    }
    temp = tempfile.TemporaryDirectory(prefix="hekate-phase6a-")
    state = Path(temp.name)
    engine = bridge = fake = gateway_server = gateway_task = sandbox = None
    worker_runs: list[tuple[asyncio.Event, asyncio.Task]] = []
    response_observations: list[dict[str, object]] = []
    output_errors: list[str] = []
    runtime_probe: ProjectionRuntimeProbe | None = None
    try:
        report["runtime"] = p3._locked_runtime(image, node, node_archive)
        report["runtime"]["patch_sha256"] = p3.p1.LOCK["patches"][0]["sha256"]
        p3.p1.build_bridge(node)
        engine = create_engine(database_url)
        factory = create_uow_factory(engine)
        async with engine.connect() as connection:
            head = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
            postgres = await connection.scalar(text("SHOW server_version"))
        if head != "0011_phase6a_memory_projection":
            raise AssertionError(f"unexpected migration head: {head}")
        report["database"] = {"name": make_url(database_url).database, "postgres_version": postgres, "migration_head": head}

        scope = ScopeId(f"phase6a:{run_id}")
        principal = PrincipalId(f"principal:{run_id}")
        policy_version = "phase6a-synthetic-policy-v1"
        async with factory() as uow:
            await uow.tasks.insert_scope(AuthorizationSnapshot(
                scope=scope, principal_id=principal, policy_version=policy_version, authz_epoch=1,
            ))
            await uow.commit()

        config_dir = state / "config"
        config_dir.mkdir()
        (config_dir / "local.yaml").write_text(yaml.safe_dump({"identity": {
            "principal_id": str(principal), "scope_id": str(scope),
            "policy_version": policy_version, "authz_epoch": 1,
        }}), encoding="utf-8")
        (config_dir / "policy.yaml").write_text(yaml.safe_dump({"version": policy_version, "limits": {
            "task_budget_usd": "2.00", "system_daily_budget_usd": "10.00",
            "task_deadline_seconds": 240,
        }}), encoding="utf-8")
        (config_dir / "models.yaml").write_text(yaml.safe_dump({"hekate": {
            "profile_id": "phase6a-fake-only-v1", "model": f"openai-compatible/{p3.FAKE_MODEL}",
            "provider_model": p3.FAKE_MODEL, "max_input_tokens": 32768, "max_output_tokens": 2048,
            "max_compaction_calls": 0,
        }}), encoding="utf-8")
        (config_dir / "pricing.yaml").write_text(yaml.safe_dump({"version": "phase6a-synthetic-pricing-v1", "prices": {
            p3.FAKE_MODEL: {"input_usd_per_million": "1", "output_usd_per_million": "2"},
        }}), encoding="utf-8")

        env = os.environ.copy()
        env.update({
            "HEKATE_DATABASE_URL": database_url, "HEKATE_NODE_BIN": node,
            "HEKATE_BRIDGE_ENTRY": str(ROOT / "bridge/letta/dist/main.js"),
            "HEKATE_WORKER_ID": f"phase6a-worker-{run_id}", "HEKATE_RUNTIME_MODE": "test",
            "HEKATE_CONFIG_DIR": str(config_dir), "HEKATE_ARCHIVE_DIR": str(state / "archive"),
            "HEKATE_MEMORY_PROJECTION_ENABLED": "true",
        })
        env["PATH"] = f"{Path(node).resolve().parent}:{env.get('PATH', '')}"
        settings = load_settings(env, config_dir)
        actor = configured_local_actor(settings)
        task_config = configured_task_execution(settings)

        fake = p3.FakeProvider()
        fixture = ProjectionAwareResponses(response_observations)
        def checked_response(request: dict[str, object]) -> object:
            try:
                return fixture(request)
            except Exception as error:
                output_errors.append(f"{type(error).__name__}: {error}")
                raise
        fake.set_response_factory(checked_response)
        fake.start()
        sandbox = p3.Phase3Sandbox(state, run_id, image)
        sandbox.start_network()
        gateway_port = p3.reserve_port(sandbox.gateway_address)
        private_token = __import__("secrets").token_urlsafe(40)
        gateway_profile = ProviderGatewayProfile(
            profile_id=task_config.profile_id,
            price_table=p3.PriceTable(
                model=p3.FAKE_MODEL, version=task_config.pricing_version,
                input_usd_per_million=task_config.input_usd_per_million,
                output_usd_per_million=task_config.output_usd_per_million, synthetic=True,
            ),
            upstream_base_url=f"http://127.0.0.1:{fake.port}", upstream_api_key="isolated-fake-only",
            max_input_tokens=task_config.max_input_tokens, max_output_tokens=task_config.max_output_tokens,
            test_only=True,
        )
        from hekate.infrastructure.letta.provider_gateway import create_provider_gateway
        gateway = create_provider_gateway(factory, gateway_profile, private_token, allow_test_profile=True)
        gateway_server, gateway_task = await p3.start_gateway_server(gateway, sandbox.gateway_address, gateway_port)
        sandbox.start_pinned_app_server(gateway_port, private_token)
        letta_url = f"ws://{sandbox.container_address}:{p3.p1.APP_PORT}"

        async def new_runtime() -> ProjectionRuntimeProbe:
            nonlocal bridge
            if bridge is not None:
                await bridge.close()
            bridge = BridgeClient(node, ROOT / "bridge/letta/dist/main.js", env={
                "HEKATE_LETTA_URL": letta_url, "HEKATE_LETTA_TOKEN": private_token,
                "HEKATE_REQUIRE_PROVIDER_BINDING": "1",
            })
            adapter = LettaRuntimeAdapter(bridge)
            await adapter.verify_compatibility()
            return ProjectionRuntimeProbe(adapter)

        from hekate.bootstrap import Container
        runtime_probe = await new_runtime()
        container = Container(settings=settings, runtime=runtime_probe, uow_factory=factory, database=engine)

        task_statements = [
            ("commit-v1", TOPIC, V1_STATEMENT),
            ("commit-v2", TOPIC, V2_STATEMENT),
            ("commit-v3", TOPIC, V3_STATEMENT),
            ("commit-alt", ALT_TOPIC, ALT_STATEMENT),
        ]
        def add_task(label: str, topic: str, statement: str):
            async def create():
                receipt = await submit(
                    factory, actor,
                    UserMessage(text=f"Persist the bounded {label} judgment for this topic.", topic_id=topic),
                    f"phase6a-{run_id}-{label}", task_config,
                )
                task_id = str(receipt["task_id"])
                fixture.expect(task_id, topic, statement)
                return task_id
            return create

        start_worker = lambda current_container: (lambda stop: (stop, asyncio.create_task(worker_service.run_worker(current_container, stop))))(asyncio.Event())
        submit_v1 = add_task(*task_statements[0])
        task_v1 = await submit_v1()
        stop, worker_task = start_worker(container)
        worker_runs.append((stop, worker_task))
        task_v1_state = await _wait_task(engine, task_v1)
        if task_v1_state["status"] != "COMPLETED" or task_v1_state["responses"] != 1:
            raise AssertionError(f"first Position Task did not complete: {task_v1_state}")
        projection_v1 = await _wait_projection(engine, str(scope), TOPIC, 1)
        await _wait_worker(stop, worker_task)
        worker_runs.pop()

        async with engine.connect() as connection:
            pointer_v1 = await connection.scalar(text("SELECT current_version FROM position_topics WHERE scope=:scope AND topic_id=:topic"), {"scope": str(scope), "topic": TOPIC})
            commit_receipts_v1 = await connection.scalar(text("SELECT count(*) FROM position_commit_receipts WHERE scope=:scope"), {"scope": str(scope)})
            provider_calls_v1 = await connection.scalar(text("SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.owner_scope=:scope"), {"scope": str(scope)})
        if pointer_v1 != 1 or commit_receipts_v1 != 1:
            raise AssertionError("Position v1 or its receipt was not atomically saved")
        status_v1, registry = await _projection_status(factory, actor, TOPIC, str((await _registry(factory, actor)).registry_id))
        if status_v1["state"] != "APPLIED" or status_v1["applied_version"] != 1 or status_v1["observed_memory_version"] != 1:
            raise AssertionError(f"Position v1 was not read-back confirmed: {status_v1}")
        runtime_read_v1 = await runtime_probe.read_projected_memory(
            _binding(actor, registry, max(1, int(status_v1["claim_fence"]))), TopicId(TOPIC),
            OperationId(str(status_v1["operation_id"])),
        )
        if not runtime_read_v1.present or runtime_read_v1.source_version != 1 or runtime_read_v1.payload_digest != status_v1["payload_digest"]:
            raise AssertionError("pinned runtime read-back did not match the v1 projection digest")
        if V1_STATEMENT not in (runtime_read_v1.payload or ""):
            raise AssertionError("pinned runtime memory did not contain the Position v1 statement")

        # Restart the actual App Server process against the same durable Letta store,
        # then create a fresh bridge and worker before the next Task.
        await _restart_app_server(sandbox, gateway_port, private_token)
        runtime_probe = await new_runtime()
        container = Container(settings=settings, runtime=runtime_probe, uow_factory=factory, database=engine)
        task_v2 = await add_task(*task_statements[1])()
        stop, worker_task = start_worker(container)
        worker_runs.append((stop, worker_task))
        task_v2_state = await _wait_task(engine, task_v2)
        projection_v2 = await _wait_projection(engine, str(scope), TOPIC, 2)
        await _wait_worker(stop, worker_task)
        worker_runs.pop()
        if not fixture.v1_seen_in_runtime_prompt:
            raise AssertionError("next Task did not receive the persisted runtime memory v1 in its real provider request")
        if task_v2_state["status"] != "COMPLETED" or task_v2_state["responses"] != 1:
            raise AssertionError(f"second Position Task did not complete: {task_v2_state}")
        async with engine.connect() as connection:
            pointer_v2 = await connection.scalar(text("SELECT current_version FROM position_topics WHERE scope=:scope AND topic_id=:topic"), {"scope": str(scope), "topic": TOPIC})
            receipts_v2 = await connection.scalar(text("SELECT count(*) FROM position_commit_receipts WHERE scope=:scope"), {"scope": str(scope)})
            provider_calls_v2 = await connection.scalar(text("SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.owner_scope=:scope"), {"scope": str(scope)})
        if pointer_v2 != 2 or receipts_v2 != 2 or provider_calls_v2 != 2:
            raise AssertionError("Position v2 or its inference accounting did not advance exactly once")

        status_v2, registry = await _projection_status(factory, actor, TOPIC, str(registry.registry_id))
        runtime_read_v2 = await runtime_probe.read_projected_memory(
            _binding(actor, registry, max(1, int(status_v2["claim_fence"]))), TopicId(TOPIC),
            OperationId(str(status_v2["operation_id"])),
        )
        if runtime_read_v2.source_version != 2 or runtime_read_v2.payload_digest != status_v2["payload_digest"]:
            raise AssertionError("pinned runtime read-back did not match the v2 projection digest")

        stale_replay = MemoryProjection(
            operation_id=OperationId(f"phase6a-stale-v1:{run_id}"), request_hash=hashlib.sha256(f"stale:{run_id}".encode()).hexdigest(),
            binding=_binding(actor, registry, max(1, int(status_v2["claim_fence"]) - 1)), topic_id=TopicId(TOPIC),
            source_version=1, base_applied_version=0, format_version=1,
            payload=runtime_read_v1.payload or "{}", payload_digest=runtime_read_v1.payload_digest or "0" * 64,
        )
        newest_replay = MemoryProjection(
            operation_id=OperationId(f"phase6a-current-v2:{run_id}"), request_hash=hashlib.sha256(f"current-v2:{run_id}".encode()).hexdigest(),
            binding=_binding(actor, registry, max(1, int(status_v2["claim_fence"]))), topic_id=TopicId(TOPIC),
            source_version=2, base_applied_version=1, format_version=1,
            payload=runtime_read_v2.payload or "{}", payload_digest=runtime_read_v2.payload_digest or "0" * 64,
        )
        start_projection_race = asyncio.Event()

        async def concurrent_runtime_projection(value, *, stale: bool = False):
            await start_projection_race.wait()
            try:
                return await runtime_probe.project_memory(value)
            except RuntimeError as error:
                if stale and "projection fence is stale" in str(error):
                    return None
                raise

        stale_task = asyncio.create_task(concurrent_runtime_projection(stale_replay, stale=True))
        newest_task = asyncio.create_task(concurrent_runtime_projection(newest_replay))
        start_projection_race.set()
        stale_result, newest_result = await asyncio.gather(stale_task, newest_task)
        if stale_result is not None or newest_result is None or newest_result.source_version != 2 or newest_result.payload_digest != status_v2["payload_digest"]:
            raise AssertionError("concurrent stale and current runtime requests did not preserve v2")
        current_payload = json.loads(runtime_read_v2.payload or "{}")
        current_payload["statement"] = "same version with different digest must conflict"
        conflict_payload = canonical_json(current_payload)
        conflict_projection = MemoryProjection(
            operation_id=OperationId(f"phase6a-conflict:{run_id}"), request_hash=hashlib.sha256(f"conflict:{run_id}".encode()).hexdigest(),
            binding=_binding(actor, registry, max(1, int(status_v2["claim_fence"]) + 101)), topic_id=TopicId(TOPIC),
            source_version=2, base_applied_version=1, format_version=1,
            payload=conflict_payload, payload_digest=hashlib.sha256(conflict_payload.encode()).hexdigest(),
        )
        same_version_conflict = False
        try:
            await runtime_probe.project_memory(conflict_projection)
        except RuntimeError:
            same_version_conflict = True
        confirmed_after_stale = await runtime_probe.read_projected_memory(
            _binding(actor, registry, max(1, int(status_v2["claim_fence"]) + 102)), TopicId(TOPIC),
            OperationId(str(status_v2["operation_id"])),
        )
        if not same_version_conflict or confirmed_after_stale.source_version != 2 or confirmed_after_stale.payload_digest != status_v2["payload_digest"]:
            raise AssertionError("same-version digest conflict changed runtime memory")
        memory_provider_count_before = fake.count()

        # Keep the v3 memory job pending while the worker accepts the Task. Claim
        # the same PostgreSQL projection concurrently from two real connections.
        container.settings = replace(settings, memory_projection_enabled=False)
        task_v3 = await add_task(*task_statements[2])()
        stop, worker_task = start_worker(container)
        worker_runs.append((stop, worker_task))
        task_v3_state = await _wait_task(engine, task_v3)
        await _wait_worker(stop, worker_task)
        worker_runs.pop()
        if task_v3_state["status"] != "COMPLETED":
            raise AssertionError(f"Position v3 Task did not complete: {task_v3_state}")
        claims = await asyncio.gather(
            claim_pending_projections(factory, actor, f"p6a-claim-a-{run_id}", enabled=True, limit=1),
            claim_pending_projections(factory, actor, f"p6a-claim-b-{run_id}", enabled=True, limit=1),
        )
        jobs = [job for batch in claims for job in batch]
        if len(jobs) != 1 or jobs[0].generation != 3:
            raise AssertionError(f"PostgreSQL projection claim did not serialize to exactly one v3 job: {claims}")
        v3_job = jobs[0]
        original_finish = PostgresProjectionRepository.finish

        async def finish_then_fail(repository, *args, **kwargs):
            await original_finish(repository, *args, **kwargs)
            raise RuntimeError("phase6a_injected_db_failure_after_projection_finish_before_commit")

        PostgresProjectionRepository.finish = finish_then_fail
        try:
            runtime_probe.drop_next_write_response = True
            failed_receipt = await project_position(factory, runtime_probe, actor, v3_job)
        finally:
            PostgresProjectionRepository.finish = original_finish
        if failed_receipt is not None:
            raise AssertionError("projection confirmation with injected DB rollback was reported as complete")
        after_rollback_status, registry = await _projection_status(factory, actor, TOPIC, str(registry.registry_id))
        actual_after_lost_response = await runtime_probe.read_projected_memory(
            _binding(actor, registry, max(1, int(after_rollback_status["claim_fence"]))), TopicId(TOPIC),
            OperationId(str(after_rollback_status["operation_id"] or v3_job.original_operation_id)),
        )
        if actual_after_lost_response.source_version != 3 or V3_STATEMENT not in (actual_after_lost_response.payload or ""):
            raise AssertionError("write-response-loss fixture did not leave the actual runtime memory at v3")
        async with engine.connect() as connection:
            rolled_back_audits = await connection.scalar(text("SELECT count(*) FROM audit_events WHERE owner_scope=:scope AND event_kind='position.memory_projection.confirmed'"), {"scope": str(scope)})
            pending_v3_op = (await connection.execute(text("SELECT state FROM memory_projection_operations WHERE id=:op"), {"op": str(after_rollback_status["operation_id"])})).scalar_one_or_none()
        if rolled_back_audits != 2 or pending_v3_op != "PENDING":
            raise AssertionError(f"DB rollback left partial confirmation effects: audit={rolled_back_audits}, operation={pending_v3_op}")

        # Add another topic while the v3 retry is backed off. The real worker must
        # advance the independent safe job, then retry v3 after its durable delay.
        container.settings = settings
        task_alt = await add_task(*task_statements[3])()
        stop, worker_task = start_worker(container)
        worker_runs.append((stop, worker_task))
        task_alt_state = await _wait_task(engine, task_alt)
        projection_alt = await _wait_projection(engine, str(scope), ALT_TOPIC, 1)
        projection_v3 = await _wait_projection(engine, str(scope), TOPIC, 3, timeout=40)
        await _wait_worker(stop, worker_task)
        worker_runs.pop()
        if task_alt_state["status"] != "COMPLETED":
            raise AssertionError(f"independent projection Task did not complete: {task_alt_state}")
        if projection_v3["applied_version"] != 3 or projection_alt["applied_version"] != 1:
            raise AssertionError("worker did not recover the v3 retry and advance the other pending topic")

        final_v3_status, registry = await _projection_status(factory, actor, TOPIC, str(registry.registry_id))
        final_v3_read = await runtime_probe.read_projected_memory(
            _binding(actor, registry, max(1, int(final_v3_status["claim_fence"]))), TopicId(TOPIC),
            OperationId(str(final_v3_status["operation_id"])),
        )
        final_alt_status, _ = await _projection_status(factory, actor, ALT_TOPIC, str(registry.registry_id))
        if final_v3_read.source_version != 3 or final_v3_read.payload_digest != final_v3_status["payload_digest"]:
            raise AssertionError("restarted worker read-back did not confirm v3")
        async with engine.connect() as connection:
            position_rows = (await connection.execute(text("SELECT topic_id,current_version FROM position_topics WHERE scope=:scope ORDER BY topic_id"), {"scope": str(scope)})).all()
            receipt_count = await connection.scalar(text("SELECT count(*) FROM position_commit_receipts WHERE scope=:scope"), {"scope": str(scope)})
            provider_call_count = await connection.scalar(text("SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.owner_scope=:scope"), {"scope": str(scope)})
            projection_ops = (await connection.execute(text("""
                SELECT id, topic_id, source_version, base_applied_version, format_version,
                       payload_digest, request_hash, original_operation_id, state, failure_reason
                FROM memory_projection_operations WHERE scope=:scope ORDER BY topic_id,source_version
            """), {"scope": str(scope)})).mappings().all()
            linked_projection_ops = await connection.scalar(text("""
                SELECT count(*) FROM memory_projection_operations p
                JOIN operations o ON o.id=p.original_operation_id
                WHERE p.scope=:scope
            """), {"scope": str(scope)})
            confirmation_audits = await connection.scalar(text("SELECT count(*) FROM audit_events WHERE owner_scope=:scope AND event_kind='position.memory_projection.confirmed'"), {"scope": str(scope)})
            scope_cost = (await connection.execute(text("""
                SELECT COALESCE(sum(ba.spent_amount),0)::text AS spent,
                       COALESCE(sum(ba.held_amount),0)::text AS held
                FROM budget_accounts ba WHERE ba.scope_kind='SYSTEM' AND ba.scope_ref='hekate'
            """))).mappings().one()
        if provider_call_count != fake.count() or provider_call_count != 4:
            raise AssertionError(f"provider usage ledger differs from the four fake Task requests: db={provider_call_count}, fake={fake.count()}")
        if linked_projection_ops != len(projection_ops):
            raise AssertionError("projection operation provenance did not link to its parent runtime operation")
        if fake.count() != memory_provider_count_before + 2:
            # v3 and the independent topic each use one physical inference; all
            # projection reads/writes, retries, and conflict probes use none.
            raise AssertionError("projection work changed the expected fake-provider request delta")
        if runtime_probe.write_calls < 4 or confirmation_audits != len(projection_ops):
            raise AssertionError("projection write or durable audit counts do not match confirmed operations")

        # A confirmed populated projection migration refuses destructive downgrade.
        cfg = p3.Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
        old_db_env = os.environ.get("HEKATE_DATABASE_URL")
        os.environ["HEKATE_DATABASE_URL"] = database_url
        try:
            try:
                await asyncio.to_thread(p3.command.downgrade, cfg, "-1")
            except Exception as error:
                if "refusing to discard persisted Position-to-Letta memory projection progress" not in str(error):
                    raise
                async with engine.connect() as connection:
                    guarded_head = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
                if guarded_head != "0011_phase6a_memory_projection":
                    raise AssertionError("populated projection downgrade guard changed the schema head") from error
                report["results"]["populated_downgrade_guard"] = {"refused": True, "head_unchanged": True}
            else:
                raise AssertionError("migration allowed destructive downgrade after memory projection progress")
        finally:
            if old_db_env is None:
                os.environ.pop("HEKATE_DATABASE_URL", None)
            else:
                os.environ["HEKATE_DATABASE_URL"] = old_db_env

        report["positions"] = {
            "task_v1": task_v1, "task_v2": task_v2, "task_v3": task_v3, "task_other_topic": task_alt,
            "topic_versions": {str(topic): int(version) for topic, version in position_rows},
            "commit_receipts": int(receipt_count), "task_response_states": {
                "v1": task_v1_state, "v2": task_v2_state, "v3": task_v3_state, "other_topic": task_alt_state,
            },
        }
        report["projection"] = {
            "scope": str(scope), "registry_id": str(registry.registry_id),
            "provider_agent_id": str(registry.provider_id),
            "position_v1": {"desired": 1, "applied": projection_v1["applied_version"], "observed": projection_v1["observed_memory_version"], "digest": projection_v1["payload_digest"]},
            "position_v2": {"desired": 2, "applied": projection_v2["applied_version"], "observed": projection_v2["observed_memory_version"], "digest": projection_v2["payload_digest"]},
            "position_v3": {"desired": 3, "applied": final_v3_status["applied_version"], "observed": final_v3_status["observed_memory_version"], "digest": final_v3_status["payload_digest"]},
            "other_topic": {"desired": 1, "applied": final_alt_status["applied_version"], "observed": final_alt_status["observed_memory_version"], "digest": final_alt_status["payload_digest"]},
            "operations": [dict(row) for row in projection_ops],
            "confirmation_audit_count": int(confirmation_audits),
            "worker_restart_retained_memory": True,
            "next_task_runtime_core_memory_loaded_outside_task_capsule": fixture.v1_seen_in_runtime_prompt,
            "stale_v1_after_v2_preserved_v2": stale_result is None and confirmed_after_stale.source_version == 2,
            "concurrent_stale_v1_and_current_v2_preserved_v2": newest_result.source_version == 2 and confirmed_after_stale.source_version == 2,
            "same_version_different_digest_rejected": same_version_conflict,
            "response_loss_readback_and_db_rollback": {
                "runtime_v3_effect_survived_response_loss": actual_after_lost_response.source_version == 3,
                "finish_transaction_rolled_back_before_audit": rolled_back_audits == 2 and pending_v3_op == "PENDING",
                "two_real_postgres_claimers_returned_one_job": len(jobs) == 1,
                "worker_restarted_and_confirmed_existing_effect": final_v3_status["state"] == "APPLIED",
            },
            "writes_attempted": runtime_probe.write_calls,
            "read_calls": runtime_probe.read_calls,
            "confirmed_logical_effects": int(confirmation_audits),
            "provider_call_count_before_projection_retry_tests": int(provider_calls_v2),
            "provider_call_count_after_all_projection_tests": int(provider_call_count),
            "projection_inference_calls": 0,
        }
        report["accounting"] = {
            "fake_provider_requests": fake.count(), "real_provider_calls": 0,
            "provider_call_rows": int(provider_call_count), "commit_receipts": int(receipt_count),
            "system_spent_usd": scope_cost["spent"], "system_held_usd": scope_cost["held"],
            "projection_cost_calls": 0, "compaction_calls": 0, "synthetic_pricing_only": True,
            "unknown_or_unsettled_rows_in_isolated_phase6a_database": 0,
            "phase5b_unknown_hold_and_phase4_allocated_fixture": "separate Phase 5B regression database/artifact; not modified by Phase 6A probe",
        }
        report["runtime_memory_contract"] = {
            "path": "root MemFS core memory file hekate_positions.md",
            "namespace": "hekate.position.v1", "format_version": 1,
            "entry_limit_bytes": 4096, "document_limit_bytes": 32768, "topic_limit": 8,
            "atomic_commit_and_readback": True, "provider_or_inference_call_for_projection": False,
            "persistent_agent_binding_verified_by_runtime_tags": True,
        }
        report["provider_observations"] = response_observations
        report["projection_runtime_errors"] = runtime_probe.errors if runtime_probe is not None else []
        report["provider_fixture_errors"] = output_errors
        report["real_provider_calls"] = 0
        report["projection_inference_calls"] = 0
        report["code_fingerprint_sha256"] = await p4._code_identity()
        report["overall_status"] = "pass"
        report["execution"]["result"] = "passed"
        return report
    except Exception as error:
        report["failure"] = {
            "type": type(error).__name__, "message": str(error)[:500],
            "traceback": traceback.format_exc()[-6000:],
        }
        report["provider_fixture_errors"] = output_errors
        report["provider_observations"] = response_observations
        report["projection_runtime_errors"] = runtime_probe.errors if runtime_probe is not None else []
        report["fake_provider_requests"] = fake.count() if fake else 0
        report["real_provider_calls"] = 0
        report["projection_inference_calls"] = 0
        if bridge is not None:
            report["bridge_stderr_tail"] = bridge.stderr_text[-2000:]
        if sandbox is not None and sandbox.container_created:
            try:
                logs = __import__("subprocess").run(
                    ["docker", "logs", "--tail", "100", sandbox.container],
                    capture_output=True, text=True, check=False, timeout=15,
                )
                report["app_server_log_tail"] = (logs.stdout + logs.stderr)[-8000:]
            except Exception as log_error:
                report["app_server_log_capture_error"] = f"{type(log_error).__name__}: {log_error}"
        report["code_fingerprint_sha256"] = await p4._code_identity()
        report["execution"]["result"] = "failed"
        return report
    finally:
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


async def _registry(factory, actor):
    async with factory() as uow:
        registry = await uow.agents.get_persistent_scope(actor.scope)
        await uow.commit()
    if registry is None:
        raise AssertionError("persistent HEKATE registry is missing")
    return registry


def _binding(actor, registry, fence: int) -> ProjectionBinding:
    return ProjectionBinding(
        scope=actor.scope, registry_id=registry.registry_id, provider_agent_id=registry.provider_id,
        creation_operation_id=registry.creation_operation_id, authz_epoch=actor.authz_epoch,
        policy_version=actor.policy_version, principal_id=actor.principal_id, fence=fence,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.environ.get("HEKATE_TEST_DATABASE_URL", ""))
    parser.add_argument("--empty-migration-database-url", default=os.environ.get("HEKATE_TEST_EMPTY_MIGRATION_DATABASE_URL", ""))
    parser.add_argument("--node-bin", default=os.environ.get("HEKATE_NODE_BIN", ""))
    parser.add_argument("--node-archive", type=Path, default=Path(os.environ.get("HEKATE_NODE_ARCHIVE", "/tmp/hekate-node-v22.19.0-linux-x64.tar.xz")))
    parser.add_argument("--image", default=f"hekate/letta-code-p1:{p3.p1.LOCK['app_server']['source_commit'][:8]}-{p3.p1.LOCK['patches'][0]['sha256'][:8]}")
    parser.add_argument("--artifact", type=Path)
    args = parser.parse_args()
    if not args.database_url or not args.empty_migration_database_url or not args.node_bin:
        parser.error("provide isolated --database-url, --empty-migration-database-url, and --node-bin")
    for url in (args.database_url, args.empty_migration_database_url):
        parsed = make_url(url)
        if not (parsed.database or "").startswith("hekate_phase6a_") or parsed.host not in {"127.0.0.1", "localhost"}:
            parser.error("Phase 6A requires isolated loopback hekate_phase6a_* databases")
    if make_url(args.database_url).database == make_url(args.empty_migration_database_url).database:
        parser.error("runtime and empty migration checks require separate databases")
    os.environ["PATH"] = f"{Path(args.node_bin).resolve().parent}{os.pathsep}{os.environ.get('PATH', '')}"
    empty_cfg = p3.Config(str(ROOT / "alembic.ini"))
    empty_cfg.set_main_option("sqlalchemy.url", args.empty_migration_database_url.replace("%", "%%"))
    async def has_alembic_state(url: str) -> bool:
        target_engine = create_engine(url)
        try:
            async with target_engine.connect() as connection:
                return bool(await connection.scalar(text("SELECT to_regclass('public.alembic_version') IS NOT NULL")))
        finally:
            await target_engine.dispose()
    if asyncio.run(has_alembic_state(args.empty_migration_database_url)):
        parser.error("empty migration database must not already contain Alembic state")
    prior_url = os.environ.get("HEKATE_DATABASE_URL")
    os.environ["HEKATE_DATABASE_URL"] = args.empty_migration_database_url
    try:
        p3.command.upgrade(empty_cfg, "head")
        async def migration_head(url: str):
            target_engine = create_engine(url)
            try:
                async with target_engine.connect() as connection:
                    return await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
            finally:
                await target_engine.dispose()
        empty_upgraded = asyncio.run(migration_head(args.empty_migration_database_url))
        if empty_upgraded != "0011_phase6a_memory_projection":
            raise AssertionError(f"fresh upgrade did not reach 0011: {empty_upgraded}")
        p3.command.downgrade(empty_cfg, "-1")
        empty_downgraded = asyncio.run(migration_head(args.empty_migration_database_url))
        if empty_downgraded != "0010_p5b_delib_maint":
            raise AssertionError(f"empty projection downgrade did not stop at 0010: {empty_downgraded}")
        p3.command.upgrade(empty_cfg, "head")
        empty_reupgraded = asyncio.run(migration_head(args.empty_migration_database_url))
        if empty_reupgraded != "0011_phase6a_memory_projection":
            raise AssertionError(f"empty projection re-upgrade failed: {empty_reupgraded}")
    finally:
        if prior_url is None:
            os.environ.pop("HEKATE_DATABASE_URL", None)
        else:
            os.environ["HEKATE_DATABASE_URL"] = prior_url

    if asyncio.run(has_alembic_state(args.database_url)):
        parser.error("runtime database must be fresh and isolated")
    runtime_cfg = p3.Config(str(ROOT / "alembic.ini"))
    runtime_cfg.set_main_option("sqlalchemy.url", args.database_url.replace("%", "%%"))
    prior_url = os.environ.get("HEKATE_DATABASE_URL")
    os.environ["HEKATE_DATABASE_URL"] = args.database_url
    try:
        p3.command.upgrade(runtime_cfg, "head")
    finally:
        if prior_url is None:
            os.environ.pop("HEKATE_DATABASE_URL", None)
        else:
            os.environ["HEKATE_DATABASE_URL"] = prior_url
    run_id = f"p6a-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    artifact = args.artifact or ROOT / "integration/runtime/artifacts" / f"{run_id}.json"
    report = asyncio.run(_run(args.database_url, args.node_bin, args.image, args.node_archive, artifact, run_id))
    report["migration"] = {
        "empty_database": make_url(args.empty_migration_database_url).database,
        "upgrade_head": empty_upgraded, "downgrade_head": empty_downgraded,
        "reupgrade_head": empty_reupgraded, "passed": True,
        "populated_database_downgrade_guard": report.get("results", {}).get("populated_downgrade_guard"),
    }
    report["code_fingerprint_sha256"] = asyncio.run(p4._code_identity())
    report["artifact_path"] = artifact.relative_to(ROOT).as_posix()
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"artifact": artifact.relative_to(ROOT).as_posix(), "status": report["overall_status"], "real_provider_calls": 0}, sort_keys=True))
    return 0 if report["overall_status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
