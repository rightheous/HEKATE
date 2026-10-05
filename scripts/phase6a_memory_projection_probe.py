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

from hekate.application import projections as projection_application
from hekate.application.projections import claim_pending_projections, project_position
from hekate.application.tasks import cancel, submit
from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.models import (
    AuthorizationSnapshot, MemoryProjection, MemoryProjectionObservation, ProjectionBinding,
    ProjectionWriteAuthorization, UserMessage,
)
from hekate.domain.types import OperationId, PrincipalId, ScopeId, StopReason, TaskId, TopicId
from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.infrastructure.letta.bridge_protocol import BridgeClient
from hekate.infrastructure.letta.provider_gateway import ProviderGatewayProfile
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.infrastructure.postgres.projection_repository import PostgresProjectionRepository
from hekate.settings import configured_local_actor, configured_task_execution, load_settings
from hekate.worker import service as worker_service


TOPIC = "phase6a-memory-primary"
ALT_TOPIC = "phase6a-memory-followup"
SERIAL_TOPIC_A = "phase6a-memory-serial-a"
SERIAL_TOPIC_B = "phase6a-memory-serial-b"
TASK_COMPETE_TOPIC = "phase6a-memory-task-compete"
AUTH_REVOKE_TOPIC = "phase6a-memory-auth-revoke"
READBACK_RACE_TOPIC = "phase6a-memory-readback-race"
V1_STATEMENT = "Phase6A durable Position marker: yellow bridge 71-alpha remains valid only within its stated jurisdiction."
V2_STATEMENT = "Phase6A durable Position marker: yellow bridge 71-alpha is valid within the stated jurisdiction and date window."
V3_STATEMENT = "Phase6A durable Position marker: yellow bridge 71-alpha requires confirmation of the date window."
ALT_STATEMENT = "Phase6A secondary topic marker: the blue cable applies only to the stated inspection scope."
SERIAL_STATEMENT_A = "Phase6A serialized projection marker: first writer retains its own agent lease."
SERIAL_STATEMENT_B = "Phase6A serialized projection marker: second topic waits for the first writer."
TASK_COMPETE_STATEMENT = "Phase6A queued Task must wait for the active projection writer."
AUTH_REVOKE_STATEMENT = "Phase6A authorization revocation marker must not reach runtime memory."
READBACK_RACE_V1 = "Phase6A read-back race baseline: source remains version one until the next commit."
READBACK_RACE_V2 = "Phase6A read-back race target: source advances to version two after the next commit."


class ProjectionGatewayBarrier:
    """One-shot ASGI barrier at the control-plane projection write boundary."""

    def __init__(self) -> None:
        self.action: str | None = None
        self.entered = asyncio.Event()
        self.release_event = asyncio.Event()
        self.completed = asyncio.Event()
        self._used = False

    def arm(self, action: str) -> None:
        if action not in {"authorize", "authorize_committed", "complete"}:
            raise ValueError("unsupported projection gateway barrier")
        self.action = action
        self.entered = asyncio.Event()
        self.release_event = asyncio.Event()
        self.completed = asyncio.Event()
        self._used = False

    async def intercept(self, action: str) -> bool:
        if self.action != action or self._used:
            return False
        self._used = True
        self.action = None
        self.entered.set()
        await self.release_event.wait()
        return True

    def release(self) -> None:
        self.release_event.set()


class ProjectionGatewayBarrierMiddleware:
    def __init__(self, app, *, barrier: ProjectionGatewayBarrier) -> None:
        self.app = app
        self.barrier = barrier

    async def __call__(self, scope, receive, send) -> None:
        held_action = None
        committed_authorize = False
        if scope.get("type") == "http":
            path = scope.get("path")
            if path == "/internal/memory-projection/authorize":
                if self.barrier.action == "authorize_committed":
                    committed_authorize = True
                    action = None
                else:
                    action = "authorize"
            elif path == "/internal/memory-projection/complete":
                action = "complete"
            else:
                action = None
            if action is not None and await self.barrier.intercept(action):
                held_action = action

        async def send_after_committed_authorize(message) -> None:
            nonlocal held_action
            if (
                committed_authorize and held_action is None
                and message.get("type") == "http.response.start"
                and message.get("status") == 200
                and await self.barrier.intercept("authorize_committed")
            ):
                held_action = "authorize_committed"
            await send(message)

        try:
            await self.app(scope, receive, send_after_committed_authorize if committed_authorize else send)
        finally:
            if held_action is not None:
                self.barrier.completed.set()


class ProjectionRuntimeProbe:
    """Counts real bridge memory calls and can lose one confirmed response."""

    def __init__(self, inner: object) -> None:
        self.inner = inner
        self.read_calls = 0
        self.write_calls = 0
        self.drop_next_write_response = False
        self.errors: list[dict[str, str]] = []
        self.read_observations: list[dict[str, object]] = []
        self.read_observation_values: list[MemoryProjectionObservation] = []
        self._read_barrier: tuple[asyncio.Event, asyncio.Event, str | None] | None = None

    def arm_read_barrier(self, operation_id: str | None = None) -> tuple[asyncio.Event, asyncio.Event]:
        if self._read_barrier is not None:
            raise RuntimeError("a runtime read barrier is already armed")
        entered, release = asyncio.Event(), asyncio.Event()
        self._read_barrier = (entered, release, operation_id)
        return entered, release

    def __getattr__(self, name: str):
        return getattr(self.inner, name)

    async def read_projected_memory(self, binding, topic_id, operation_id):
        self.read_calls += 1
        try:
            result = await self.inner.read_projected_memory(binding, topic_id, operation_id)
            self.read_observations.append({
                "topic_id": str(topic_id), "present": result.present,
                "source_version": result.source_version,
                "payload_digest": result.payload_digest,
                "memory_revision": result.memory_revision,
                "agent_fence": result.agent_fence,
            })
            self.read_observation_values.append(result)
            barrier = self._read_barrier
            if barrier is not None and (barrier[2] is None or barrier[2] == str(operation_id)):
                self._read_barrier = None
                barrier[0].set()
                await barrier[1].wait()
            return result
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


async def _acquire_probe_lease(factory, registry_id: str, owner: str):
    async with factory() as uow:
        lease = await uow.agents.acquire_lease(registry_id, owner, 120)
        await uow.commit()
    if lease is None:
        raise AssertionError(f"probe could not acquire its PostgreSQL agent lease: {owner}")
    return lease


async def _release_probe_lease(factory, lease) -> None:
    async with factory() as uow:
        await uow.agents.release_lease(lease)
        await uow.commit()


async def _runtime_read(factory, runtime, actor, registry, topic_id: str, operation_id: str):
    lease = await _acquire_probe_lease(factory, registry.registry_id, f"p6a-read:{uuid.uuid4()}")
    try:
        return await runtime.read_projected_memory(
            _binding(actor, registry, lease.owner, lease.fence), TopicId(topic_id), OperationId(operation_id),
        )
    finally:
        await _release_probe_lease(factory, lease)


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
    projection_barrier: ProjectionGatewayBarrier | None = None
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
        if head != "0012_projection_write_guard":
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
        projection_barrier = ProjectionGatewayBarrier()
        gateway.add_middleware(ProjectionGatewayBarrierMiddleware, barrier=projection_barrier)
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
            ("serialize-a", SERIAL_TOPIC_A, SERIAL_STATEMENT_A),
            ("serialize-b", SERIAL_TOPIC_B, SERIAL_STATEMENT_B),
            ("serialize-task-competitor", TASK_COMPETE_TOPIC, TASK_COMPETE_STATEMENT),
            ("auth-revoke", AUTH_REVOKE_TOPIC, AUTH_REVOKE_STATEMENT),
            ("readback-race-v1", READBACK_RACE_TOPIC, READBACK_RACE_V1),
            ("readback-race-v2", READBACK_RACE_TOPIC, READBACK_RACE_V2),
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
        runtime_read_v1 = await _runtime_read(factory, runtime_probe, actor, registry, TOPIC, str(status_v1["operation_id"]))
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
        runtime_read_v2 = await _runtime_read(factory, runtime_probe, actor, registry, TOPIC, str(status_v2["operation_id"]))
        if runtime_read_v2.source_version != 2 or runtime_read_v2.payload_digest != status_v2["payload_digest"]:
            raise AssertionError("pinned runtime read-back did not match the v2 projection digest")

        # Use two real PostgreSQL lease acquisitions to create a stale generation.
        # The upcoming v3 projection advances the pinned runtime's global agent
        # fence beyond both, independently of either topic's claim fence.
        old_lease = await _acquire_probe_lease(factory, registry.registry_id, f"p6a-old-agent-owner:{run_id}")
        await _release_probe_lease(factory, old_lease)
        replacement_lease = await _acquire_probe_lease(factory, registry.registry_id, f"p6a-new-agent-owner:{run_id}")
        if replacement_lease.fence <= old_lease.fence:
            raise AssertionError("PostgreSQL agent lease takeover did not advance its own fence")
        await _release_probe_lease(factory, replacement_lease)
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
        actual_after_lost_response = await _runtime_read(
            factory, runtime_probe, actor, registry, TOPIC,
            str(after_rollback_status["operation_id"] or v3_job.original_operation_id),
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
        final_v3_read = await _runtime_read(factory, runtime_probe, actor, registry, TOPIC, str(final_v3_status["operation_id"]))
        final_alt_status, _ = await _projection_status(factory, actor, ALT_TOPIC, str(registry.registry_id))
        if final_v3_read.source_version != 3 or final_v3_read.payload_digest != final_v3_status["payload_digest"]:
            raise AssertionError("restarted worker read-back did not confirm v3")

        if old_lease.fence >= final_v3_read.agent_fence:
            raise AssertionError("pinned runtime did not record a newer PostgreSQL agent lease fence")
        current_probe_lease = await _acquire_probe_lease(
            factory, registry.registry_id, f"p6a-current-agent-owner:{run_id}",
        )
        current_identity = _binding(actor, registry, current_probe_lease.owner, current_probe_lease.fence, "claim-probe", 1)
        conflict_value = json.loads(final_v3_read.payload or "{}")
        conflict_value["statement"] = "same version with different digest must conflict"
        conflict_payload = canonical_json(conflict_value)
        conflict_projection = MemoryProjection(
            operation_id=OperationId(f"phase6a-conflict:{run_id}"),
            request_hash=hashlib.sha256(f"conflict:{run_id}".encode()).hexdigest(),
            binding=current_identity, topic_id=TopicId(TOPIC), source_version=3,
            base_applied_version=2, format_version=2,
            payload=conflict_payload, payload_digest=hashlib.sha256(conflict_payload.encode()).hexdigest(),
        )
        same_version_conflict = False
        try:
            await runtime_probe.project_memory(conflict_projection)
        except RuntimeError as error:
            same_version_conflict = "same projection version is already bound" in str(error)
        if not same_version_conflict:
            raise AssertionError("same-version digest conflict was not rejected at the pinned runtime")

        stale_binding = _binding(
            actor, registry, old_lease.owner, old_lease.fence,
            f"stale-claim:{run_id}", 1,
        )
        stale_same_topic = MemoryProjection(
            operation_id=OperationId(f"phase6a-stale-same-topic:{run_id}"),
            request_hash=hashlib.sha256(f"stale-same:{run_id}".encode()).hexdigest(),
            binding=stale_binding, topic_id=TopicId(TOPIC), source_version=1,
            base_applied_version=0, format_version=2,
            payload=runtime_read_v1.payload or "{}", payload_digest=runtime_read_v1.payload_digest or "0" * 64,
        )
        stale_topic = "phase6a-stale-new-topic"
        stale_new_value = json.loads(final_v3_read.payload or "{}")
        stale_new_value["topic_id"] = stale_topic
        stale_new_value["source_version"] = 1
        stale_new_payload = canonical_json(stale_new_value)
        stale_new_topic = MemoryProjection(
            operation_id=OperationId(f"phase6a-stale-new-topic:{run_id}"),
            request_hash=hashlib.sha256(f"stale-new:{run_id}".encode()).hexdigest(),
            binding=stale_binding, topic_id=TopicId(stale_topic), source_version=1,
            base_applied_version=0, format_version=2,
            payload=stale_new_payload, payload_digest=hashlib.sha256(stale_new_payload.encode()).hexdigest(),
        )
        stale_same_topic_rejected = stale_new_topic_rejected = False
        for label, value in (("same", stale_same_topic), ("new", stale_new_topic)):
            try:
                await runtime_probe.project_memory(value)
            except RuntimeError as error:
                if "agent lease fence is stale across the memory namespace" in str(error):
                    if label == "same":
                        stale_same_topic_rejected = True
                    else:
                        stale_new_topic_rejected = True
        await _release_probe_lease(factory, current_probe_lease)
        after_stale_topic = await _runtime_read(
            factory, runtime_probe, actor, registry, TOPIC, str(final_v3_status["operation_id"]),
        )
        after_stale_new_topic = await _runtime_read(
            factory, runtime_probe, actor, registry, stale_topic, str(final_v3_status["operation_id"]),
        )
        if (
            not stale_same_topic_rejected or not stale_new_topic_rejected
            or after_stale_topic.source_version != 3
            or after_stale_topic.payload_digest != final_v3_status["payload_digest"]
            or after_stale_new_topic.present
            or after_stale_new_topic.memory_revision != final_v3_read.memory_revision
        ):
            raise AssertionError("real stale agent lease changed an existing or previously absent runtime topic")

        normal_phase6_provider_requests = fake.count()
        if normal_phase6_provider_requests != 4:
            raise AssertionError(f"normal Phase 6A path must use exactly four fake requests: {normal_phase6_provider_requests}")
        hardening_results: dict[str, object] = {}

        # Prepare two independent Position projections in PostgreSQL, then let
        # the actual worker claim both under the same worker id. The pinned
        # runtime pauses after committing a real MemFS write but before the
        # control-plane records its effect receipt.
        container.settings = replace(settings, memory_projection_enabled=False)
        serialization_tasks: dict[str, dict[str, object]] = {}
        for index in (4, 5):
            label, topic_id, statement = task_statements[index]
            task_id = await add_task(label, topic_id, statement)()
            stop, worker_task = start_worker(container)
            worker_runs.append((stop, worker_task))
            state = await _wait_task(engine, task_id)
            await _wait_worker(stop, worker_task)
            worker_runs.pop()
            if state["status"] != "COMPLETED" or state["responses"] != 1:
                raise AssertionError(f"serialization fixture Task did not complete: {task_id} {state}")
            serialization_tasks[topic_id] = {"task_id": task_id, "state": state}
        serialization_provider_request_delta = fake.count() - normal_phase6_provider_requests
        if serialization_provider_request_delta != 2:
            raise AssertionError("the two serialization Position Tasks did not use exactly two fake requests")

        container.settings = settings
        worker_name = settings.worker_id
        projection_runs: dict[str, dict[str, object]] = {}
        projection_finished: set[str] = set()
        projection_finished_event = asyncio.Event()
        both_projection_calls_started = asyncio.Event()
        original_project_position = worker_service.project_position
        original_prepare_queued_tasks = worker_service.prepare_queued_tasks
        prepare_monitor: dict[str, object] = {
            "enabled": False, "task_id": None, "event": asyncio.Event(), "returned": None,
        }

        async def capture_worker_projection(factory_arg, runtime_arg, actor_arg, job):
            key = str(job.topic_id)
            projection_runs[key] = {"job": job, "task": asyncio.current_task()}
            if len(projection_runs) == 2:
                both_projection_calls_started.set()
            try:
                return await original_project_position(factory_arg, runtime_arg, actor_arg, job)
            finally:
                projection_finished.add(key)
                projection_finished_event.set()

        async def observe_worker_task_prepare(*args, **kwargs):
            leases = await original_prepare_queued_tasks(*args, **kwargs)
            if prepare_monitor["enabled"] and prepare_monitor["task_id"] is not None:
                prepare_monitor["returned"] = len(leases)
                prepare_monitor["event"].set()
            return leases

        worker_service.project_position = capture_worker_projection
        worker_service.prepare_queued_tasks = observe_worker_task_prepare
        t2_write_calls_before = runtime_probe.write_calls
        t2_fake_requests_before = fake.count()
        projection_barrier.arm("complete")
        stop, worker_task = start_worker(container)
        worker_runs.append((stop, worker_task))
        try:
            await asyncio.wait_for(projection_barrier.entered.wait(), 40)
            await asyncio.wait_for(both_projection_calls_started.wait(), 10)
            async with engine.connect() as connection:
                guards = (await connection.execute(text("""
                    SELECT id,topic_id,lease_owner,lease_fence,claim_owner,claim_fence,state
                    FROM memory_projection_write_guards
                    WHERE scope=:scope AND state='AUTHORIZED'
                    ORDER BY created_at,id
                """), {"scope": str(scope)})).mappings().all()
            if len(guards) != 1:
                raise AssertionError(f"one pinned-runtime write should be at the completion barrier: {guards}")
            guard = dict(guards[0])
            winning_topic = str(guard["topic_id"])
            other_topic = SERIAL_TOPIC_B if winning_topic == SERIAL_TOPIC_A else SERIAL_TOPIC_A
            if winning_topic not in {SERIAL_TOPIC_A, SERIAL_TOPIC_B}:
                raise AssertionError(f"serialized projection wrote an unexpected topic: {winning_topic}")
            loser_job = projection_runs.get(other_topic, {}).get("job")
            winner_job = projection_runs.get(winning_topic, {}).get("job")
            winner_task = projection_runs.get(winning_topic, {}).get("task")
            if loser_job is None or winner_job is None or not isinstance(winner_task, asyncio.Task):
                raise AssertionError("worker did not concurrently start both distinct topic projection attempts")

            deadline = asyncio.get_running_loop().time() + 10
            while other_topic not in projection_finished and asyncio.get_running_loop().time() < deadline:
                projection_finished_event.clear()
                if other_topic not in projection_finished:
                    await asyncio.wait_for(projection_finished_event.wait(), 2)
            if other_topic not in projection_finished:
                raise AssertionError("second topic projection did not return while the first held the agent lease")

            async with engine.connect() as connection:
                active_lease = (await connection.execute(text("""
                    SELECT owner_worker,fence,expires_at FROM agent_leases WHERE registry_id=:registry
                """), {"registry": str(registry.registry_id)})).mappings().one()
                serialized_rows = (await connection.execute(text("""
                    SELECT topic_id,state,pending_reason,applied_version
                    FROM memory_projections WHERE scope=:scope AND topic_id IN (:a,:b)
                    ORDER BY topic_id
                """), {"scope": str(scope), "a": SERIAL_TOPIC_A, "b": SERIAL_TOPIC_B})).mappings().all()
            if (
                active_lease["owner_worker"] != guard["lease_owner"]
                or active_lease["fence"] != guard["lease_fence"]
                or not any(row["topic_id"] == other_topic and row["state"] == "PENDING"
                           and row["pending_reason"] == "persistent_HEKATE_lease_busy"
                           and row["applied_version"] == 0 for row in serialized_rows)
                or runtime_probe.write_calls - t2_write_calls_before != 1
            ):
                raise AssertionError(
                    "different topic did not wait behind the first task-specific agent lease: "
                    f"lease={dict(active_lease)}, rows={[dict(row) for row in serialized_rows]}, "
                    f"runtime_write_attempts={runtime_probe.write_calls - t2_write_calls_before}"
                )

            # A same-worker queued Task must not reenter the projection's agent
            # lease while the runtime's committed response is still unresolved.
            competitor_id = await add_task(*task_statements[6])()
            prepare_monitor["task_id"] = competitor_id
            prepare_monitor["enabled"] = True
            await asyncio.wait_for(prepare_monitor["event"].wait(), 12)
            if prepare_monitor["returned"] != 0:
                raise AssertionError("same-worker Task preparation acquired the active projection lease")
            async with engine.connect() as connection:
                competitor_status = await connection.scalar(
                    text("SELECT status FROM tasks WHERE id=:task"), {"task": competitor_id},
                )
                t2_guard_state = await connection.scalar(
                    text("SELECT state FROM memory_projection_write_guards WHERE id=:id"), {"id": guard["id"]},
                )
                competitor_provider_calls = await connection.scalar(text("""
                    SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id
                    WHERE o.owner_scope=:scope AND o.task_id=:task
                """), {"scope": str(scope), "task": competitor_id})
            if (
                competitor_status != "QUEUED" or t2_guard_state != "AUTHORIZED"
                or competitor_provider_calls != 0 or fake.count() != t2_fake_requests_before
            ):
                raise AssertionError("queued Task crossed an unresolved projection memory write")

            # Cancel the real worker's projection coroutine while the App Server
            # still owns the MemFS critical section. Expire its DB lease to prove
            # that an unconfirmed external write still fences a restarted owner.
            winner_task.cancel()
            cancelled_projection = False
            try:
                await winner_task
            except asyncio.CancelledError:
                cancelled_projection = True
            if not cancelled_projection:
                raise AssertionError("projection coroutine cancellation did not propagate")
            async with engine.begin() as connection:
                await connection.execute(text("""
                    UPDATE agent_leases SET expires_at=now()-interval '1 second'
                    WHERE registry_id=:registry AND owner_worker=:owner AND fence=:fence
                """), {
                    "registry": str(registry.registry_id), "owner": guard["lease_owner"],
                    "fence": guard["lease_fence"],
                })
            async with factory() as uow:
                takeover = await uow.agents.acquire_lease(
                    registry.registry_id, f"p6a-restarted-writer:{run_id}", 45,
                )
                await uow.commit()
            if takeover is not None:
                raise AssertionError("expired lease with an AUTHORIZED write guard was acquired by a new owner")
            async with engine.connect() as connection:
                retained_guard = await connection.scalar(
                    text("SELECT state FROM memory_projection_write_guards WHERE id=:id"), {"id": guard["id"]},
                )
                retained_lease = (await connection.execute(text("""
                    SELECT owner_worker,fence FROM agent_leases WHERE registry_id=:registry
                """), {"registry": str(registry.registry_id)})).mappings().one()
            if retained_guard != "AUTHORIZED" or retained_lease["owner_worker"] != guard["lease_owner"]:
                raise AssertionError("cancelling the Python worker released an unresolved runtime write owner")

            cancelled_competitor = await cancel(
                factory, actor, TaskId(competitor_id), StopReason.USER_CANCELLED,
            )
            stop.set()
            container.settings = replace(settings, memory_projection_enabled=False)
            projection_barrier.release()
            await asyncio.wait_for(projection_barrier.completed.wait(), 15)
            await _wait_worker(stop, worker_task)
            worker_runs.pop()
            worker_service.project_position = original_project_position
            worker_service.prepare_queued_tasks = original_prepare_queued_tasks

            async with engine.connect() as connection:
                resolved_guard = await connection.scalar(
                    text("SELECT state FROM memory_projection_write_guards WHERE id=:id"), {"id": guard["id"]},
                )
            if resolved_guard != "EFFECT_CONFIRMED":
                raise AssertionError(f"pinned runtime committed but its durable effect guard did not resolve: {resolved_guard}")
            recovery_receipt = await project_position(factory, runtime_probe, actor, winner_job)
            if recovery_receipt is None or recovery_receipt.state != "APPLIED":
                raise AssertionError(f"read-back did not recover the cancelled projection: {recovery_receipt}")
            if runtime_probe.write_calls - t2_write_calls_before != 1:
                raise AssertionError("cancelled write recovery issued a duplicate runtime memory write")
            winner_runtime_write_attempts = runtime_probe.write_calls - t2_write_calls_before
            if cancelled_competitor["state"] != "CANCELLED":
                raise AssertionError(f"queued Task cancellation failed to converge: {cancelled_competitor}")

            # A fresh actual worker resumes the other topic after the first
            # projection's verified read-back releases its own lease.
            container.settings = settings
            stop, worker_task = start_worker(container)
            worker_runs.append((stop, worker_task))
            status_a = await _wait_projection(engine, str(scope), SERIAL_TOPIC_A, 1, timeout=35)
            status_b = await _wait_projection(engine, str(scope), SERIAL_TOPIC_B, 1, timeout=35)
            await _wait_worker(stop, worker_task)
            worker_runs.pop()
            if status_a["state"] != "APPLIED" or status_b["state"] != "APPLIED":
                raise AssertionError("a waiting topic did not advance after the first projection was reconciled")
            serial_read_a = await _runtime_read(
                factory, runtime_probe, actor, registry, SERIAL_TOPIC_A, str(status_a["operation_id"]),
            )
            serial_read_b = await _runtime_read(
                factory, runtime_probe, actor, registry, SERIAL_TOPIC_B, str(status_b["operation_id"]),
            )
            if (
                serial_read_a.source_version != 1 or serial_read_a.payload_digest != status_a["payload_digest"]
                or serial_read_b.source_version != 1 or serial_read_b.payload_digest != status_b["payload_digest"]
            ):
                raise AssertionError("serialized topic projections do not match pinned runtime read-back")
            if runtime_probe.write_calls - t2_write_calls_before != 2:
                raise AssertionError("first-topic recovery or second-topic progress issued an unexpected runtime write count")
            if fake.count() != t2_fake_requests_before:
                raise AssertionError("Task/projection serialization caused an unauthorized provider request")
            hardening_results["same_worker_task_projection_serialization_and_cancel_recovery"] = {
                "worker_id": worker_name,
                "topics": [SERIAL_TOPIC_A, SERIAL_TOPIC_B],
                "worker_claimed_both_topics": True,
                "one_runtime_write_at_completion_barrier": True,
                "other_topic_wait_reason": "persistent_HEKATE_lease_busy",
                "queued_task_status_while_write_unresolved": competitor_status,
                "queued_task_provider_calls_while_write_unresolved": int(competitor_provider_calls),
                "projection_cancel_propagated": cancelled_projection,
                "expired_lease_new_owner_denied_while_write_guard_authorized": takeover is None,
                "durable_guard_after_runtime_completion": resolved_guard,
                "same_operation_readback_recovery": recovery_receipt.state,
                "runtime_write_attempts_for_cancelled_topic_including_recovery": winner_runtime_write_attempts,
                "both_topics_applied_after_recovery": [int(status_a["applied_version"]), int(status_b["applied_version"])],
                "runtime_readback_digests_match": True,
                "provider_request_delta": fake.count() - t2_fake_requests_before,
            }
        finally:
            projection_barrier.release()
            worker_service.project_position = original_project_position
            worker_service.prepare_queued_tasks = original_prepare_queued_tasks

        # Revoke authorization after the Task and projection have been prepared,
        # while the pinned runtime is inside its MemFS lock but before the
        # PostgreSQL write authorization handler runs.
        container.settings = replace(settings, memory_projection_enabled=False)
        auth_task = await add_task(*task_statements[7])()
        stop, worker_task = start_worker(container)
        worker_runs.append((stop, worker_task))
        auth_task_state = await _wait_task(engine, auth_task)
        await _wait_worker(stop, worker_task)
        worker_runs.pop()
        if auth_task_state["status"] != "COMPLETED" or auth_task_state["responses"] != 1:
            raise AssertionError(f"authorization-revocation fixture Task did not finish normally: {auth_task_state}")

        registry = await _registry(factory, actor)
        auth_status_before, _ = await _projection_status(factory, actor, AUTH_REVOKE_TOPIC, str(registry.registry_id))
        if auth_status_before["applied_version"] != 0:
            raise AssertionError("authorization-revocation fixture was projected before its write-boundary test")
        auth_operation_before = str(auth_status_before["operation_id"] or f"phase6a-auth-read:{run_id}")
        auth_runtime_before = await _runtime_read(
            factory, runtime_probe, actor, registry, AUTH_REVOKE_TOPIC, auth_operation_before,
        )
        if auth_runtime_before.present:
            raise AssertionError("authorization-revocation topic unexpectedly existed before projection")
        async with engine.connect() as connection:
            auth_task_before = dict((await connection.execute(text("""
                SELECT status,outcome,stop_reason,
                       (SELECT count(*) FROM task_responses WHERE task_id=tasks.id) AS responses
                FROM tasks WHERE id=:task
            """), {"task": auth_task})).mappings().one())
            auth_receipts_before = await connection.scalar(
                text("SELECT count(*) FROM position_commit_receipts WHERE scope=:scope"), {"scope": str(scope)},
            )
            auth_calls_before = await connection.scalar(text("""
                SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id
                WHERE o.owner_scope=:scope
            """), {"scope": str(scope)})
            auth_budget_before = dict((await connection.execute(text("""
                SELECT COALESCE(sum(spent_amount),0)::text AS spent,
                       COALESCE(sum(held_amount),0)::text AS held
                FROM budget_accounts WHERE scope_kind='SYSTEM' AND scope_ref='hekate'
            """))).mappings().one())

        container.settings = settings
        projection_barrier.arm("authorize")
        stop, worker_task = start_worker(container)
        worker_runs.append((stop, worker_task))
        await asyncio.wait_for(projection_barrier.entered.wait(), 40)
        async with engine.begin() as connection:
            revoked_epoch = await connection.scalar(text("""
                UPDATE authorization_scopes SET authz_epoch=authz_epoch+1
                WHERE id=:scope RETURNING authz_epoch
            """), {"scope": str(scope)})
        if revoked_epoch != 2:
            raise AssertionError(f"authorization epoch revocation did not advance from 1: {revoked_epoch}")
        projection_barrier.release()
        await asyncio.wait_for(projection_barrier.completed.wait(), 15)
        stop.set()
        await _wait_worker(stop, worker_task)
        worker_runs.pop()

        async with engine.connect() as connection:
            auth_status_denied = dict((await connection.execute(text("""
                SELECT desired_version,applied_version,observed_memory_version,observed_payload_digest,
                       payload_digest,state,pending_reason,operation_id,request_hash
                FROM memory_projections WHERE scope=:scope AND topic_id=:topic
            """), {"scope": str(scope), "topic": AUTH_REVOKE_TOPIC})).mappings().one())
            auth_operation = dict((await connection.execute(text("""
                SELECT state,failure_reason FROM memory_projection_operations
                WHERE id=:operation
            """), {"operation": auth_status_denied["operation_id"]})).mappings().one())
            denial_guards = (await connection.execute(text("""
                SELECT state,authz_epoch,observation FROM memory_projection_write_guards
                WHERE scope=:scope AND topic_id=:topic ORDER BY created_at,id
            """), {"scope": str(scope), "topic": AUTH_REVOKE_TOPIC})).mappings().all()
            auth_task_after = dict((await connection.execute(text("""
                SELECT status,outcome,stop_reason,
                       (SELECT count(*) FROM task_responses WHERE task_id=tasks.id) AS responses
                FROM tasks WHERE id=:task
            """), {"task": auth_task})).mappings().one())
            auth_receipts_after = await connection.scalar(
                text("SELECT count(*) FROM position_commit_receipts WHERE scope=:scope"), {"scope": str(scope)},
            )
            auth_calls_after = await connection.scalar(text("""
                SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id
                WHERE o.owner_scope=:scope
            """), {"scope": str(scope)})
            auth_budget_after = dict((await connection.execute(text("""
                SELECT COALESCE(sum(spent_amount),0)::text AS spent,
                       COALESCE(sum(held_amount),0)::text AS held
                FROM budget_accounts WHERE scope_kind='SYSTEM' AND scope_ref='hekate'
            """))).mappings().one())
        auth_runtime_denied = await _runtime_read(
            factory, runtime_probe, actor, registry, AUTH_REVOKE_TOPIC,
            str(auth_status_denied["operation_id"]),
        )
        if (
            auth_runtime_denied.present or auth_runtime_denied.source_version != 0
            or auth_runtime_denied.memory_revision != auth_runtime_before.memory_revision
            or auth_status_denied["applied_version"] != 0
            or auth_status_denied["state"] != "PENDING"
            or auth_status_denied["pending_reason"] != "runtime_memory_write_authorization_denied_before_effect"
            or auth_operation["state"] != "PENDING"
            or len(denial_guards) != 1 or denial_guards[0]["state"] != "NO_EFFECT"
            or denial_guards[0]["authz_epoch"] != 1
            or auth_task_after != auth_task_before
            or auth_receipts_after != auth_receipts_before
            or auth_calls_after != auth_calls_before
            or auth_budget_after != auth_budget_before
        ):
            raise AssertionError(
                "revoked epoch reached runtime memory or changed a committed Task/ledger: "
                f"projection={auth_status_denied}, operation={auth_operation}, guards={[dict(row) for row in denial_guards]}, "
                f"before={auth_runtime_before}, after={auth_runtime_denied}, task={auth_task_after}"
            )

        # Reprocess only after constructing the current actor snapshot. This is a
        # projection retry, not a new Task or inference.
        local_identity = yaml.safe_load((config_dir / "local.yaml").read_text(encoding="utf-8"))
        local_identity["identity"]["authz_epoch"] = 2
        (config_dir / "local.yaml").write_text(yaml.safe_dump(local_identity), encoding="utf-8")
        settings2 = load_settings(env, config_dir)
        actor2 = configured_local_actor(settings2)
        container.settings = settings2
        stop, worker_task = start_worker(container)
        worker_runs.append((stop, worker_task))
        auth_status_applied = await _wait_projection(engine, str(scope), AUTH_REVOKE_TOPIC, 1, timeout=35)
        await _wait_worker(stop, worker_task)
        worker_runs.pop()
        registry2 = await _registry(factory, actor2)
        auth_runtime_after_retry = await _runtime_read(
            factory, runtime_probe, actor2, registry2, AUTH_REVOKE_TOPIC,
            str(auth_status_applied["operation_id"]),
        )
        async with engine.connect() as connection:
            auth_task_final = dict((await connection.execute(text("""
                SELECT status,outcome,stop_reason,
                       (SELECT count(*) FROM task_responses WHERE task_id=tasks.id) AS responses
                FROM tasks WHERE id=:task
            """), {"task": auth_task})).mappings().one())
            auth_calls_final = await connection.scalar(text("""
                SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id
                WHERE o.owner_scope=:scope
            """), {"scope": str(scope)})
            denial_and_effect_states = (await connection.execute(text("""
                SELECT state,authz_epoch,source_version,payload_digest,observation
                FROM memory_projection_write_guards WHERE scope=:scope AND topic_id=:topic
                ORDER BY created_at,id
            """), {"scope": str(scope), "topic": AUTH_REVOKE_TOPIC})).mappings().all()
        if (
            auth_status_applied["applied_version"] != 1
            or not auth_runtime_after_retry.present
            or auth_runtime_after_retry.source_version != 1
            or auth_runtime_after_retry.payload_digest != auth_status_applied["payload_digest"]
            or auth_runtime_after_retry.memory_revision == auth_runtime_before.memory_revision
            or auth_task_final != auth_task_before
            or auth_calls_final != auth_calls_before
            or not any(row["state"] == "EFFECT_CONFIRMED" and row["authz_epoch"] == 2 for row in denial_and_effect_states)
        ):
            raise AssertionError("current-epoch projection retry did not apply without new Task inference")
        hardening_results["authorization_revocation_at_serialized_runtime_write_boundary"] = {
            "prepared_actor_epoch": 1, "database_epoch_at_gate_release": int(revoked_epoch),
            "old_epoch_write_guard": dict(denial_guards[0]),
            "old_epoch_operation_state": auth_operation["state"],
            "old_epoch_projection_state": auth_status_denied["state"],
            "old_epoch_applied_version": int(auth_status_denied["applied_version"]),
            "runtime_memory_before": {
                "present": auth_runtime_before.present, "version": auth_runtime_before.source_version,
                "digest": auth_runtime_before.payload_digest, "revision": auth_runtime_before.memory_revision,
            },
            "runtime_memory_after_denial": {
                "present": auth_runtime_denied.present, "version": auth_runtime_denied.source_version,
                "digest": auth_runtime_denied.payload_digest, "revision": auth_runtime_denied.memory_revision,
            },
            "memory_revision_unchanged_at_denial": auth_runtime_denied.memory_revision == auth_runtime_before.memory_revision,
            "task_response_and_position_receipt_unchanged": auth_task_after == auth_task_before and auth_receipts_after == auth_receipts_before,
            "provider_and_budget_ledger_unchanged": auth_calls_after == auth_calls_before and auth_budget_after == auth_budget_before,
            "current_epoch_retry_state": auth_status_applied["state"],
            "current_epoch_effect_guard_states": [dict(row) for row in denial_and_effect_states],
            "final_memory_version": auth_runtime_after_retry.source_version,
            "provider_requests_for_denial_and_projection_retry": 0,
        }

        # Reproduce the read-back race through two real workers and the pinned
        # App Server. A reads runtime v1, then its claim expires; B takes the same
        # deterministic topic lease and commits an AUTHORIZED v2 grant before the
        # runtime is allowed to rename the file.
        actor = actor2
        settings = settings2
        container.settings = replace(settings, memory_projection_enabled=False)
        readback_task_v1 = await add_task(*task_statements[8])()
        stop, worker_task = start_worker(container)
        worker_runs.append((stop, worker_task))
        readback_task_v1_state = await _wait_task(engine, readback_task_v1)
        await _wait_worker(stop, worker_task)
        worker_runs.remove((stop, worker_task))
        if readback_task_v1_state["status"] != "COMPLETED":
            raise AssertionError("read-back race v1 fixture Task did not complete")

        container.settings = settings
        stop, worker_task = start_worker(container)
        worker_runs.append((stop, worker_task))
        readback_v1_status = await _wait_projection(engine, str(scope), READBACK_RACE_TOPIC, 1)
        await _wait_worker(stop, worker_task)
        worker_runs.remove((stop, worker_task))

        container.settings = replace(settings, memory_projection_enabled=False)
        readback_task_v2 = await add_task(*task_statements[9])()
        stop, worker_task = start_worker(container)
        worker_runs.append((stop, worker_task))
        readback_task_v2_state = await _wait_task(engine, readback_task_v2)
        await _wait_worker(stop, worker_task)
        worker_runs.remove((stop, worker_task))
        if readback_task_v2_state["status"] != "COMPLETED":
            raise AssertionError("read-back race v2 fixture Task did not complete")

        container.settings = settings
        race_worker_name = settings.worker_id
        race_jobs: dict[int, object] = {}
        race_done: dict[int, asyncio.Event] = {}
        race_snapshots: dict[int, dict[str, object]] = {}
        original_race_project = worker_service.project_position
        original_capture_snapshot = projection_application._capture_read_back_snapshot

        async def capture_race_project(factory_arg, runtime_arg, actor_arg, job):
            if str(job.topic_id) == READBACK_RACE_TOPIC:
                race_jobs[int(job.fence)] = job
                race_done[int(job.fence)] = asyncio.Event()
                try:
                    return await original_race_project(factory_arg, runtime_arg, actor_arg, job)
                finally:
                    race_done[int(job.fence)].set()
            return await original_race_project(factory_arg, runtime_arg, actor_arg, job)

        async def capture_race_snapshot(factory_arg, job, projection):
            snapshot = await original_capture_snapshot(factory_arg, job, projection)
            if str(job.topic_id) == READBACK_RACE_TOPIC:
                race_snapshots[int(job.fence)] = {"projection": projection, "snapshot": snapshot}
            return snapshot

        worker_service.project_position = capture_race_project
        projection_application._capture_read_back_snapshot = capture_race_snapshot
        readback_fake_before = fake.count()
        readback_writes_before = runtime_probe.write_calls
        read_observation_start = len(runtime_probe.read_observations)
        read_entered, read_release = runtime_probe.arm_read_barrier()
        stop_a, worker_a = start_worker(container)
        worker_runs.append((stop_a, worker_a))
        stop_b = worker_b = None
        try:
            await asyncio.wait_for(read_entered.wait(), 40)
            if not race_jobs:
                raise AssertionError("worker A reached the pinned runtime before its durable claim was captured")
            a_fence = min(race_jobs)
            job_a = race_jobs[a_fence]
            snapshot_a = race_snapshots[a_fence]["snapshot"]
            projection_a = race_snapshots[a_fence]["projection"]
            a_runtime_observation = runtime_probe.read_observations[read_observation_start]
            a_runtime_observation_value = runtime_probe.read_observation_values[read_observation_start]
            if snapshot_a.candidates:
                raise AssertionError("worker A unexpectedly captured a pre-existing write guard")
            if (
                a_runtime_observation["topic_id"] != READBACK_RACE_TOPIC
                or a_runtime_observation["source_version"] != 1
                or a_runtime_observation["payload_digest"] != readback_v1_status["payload_digest"]
            ):
                raise AssertionError("worker A did not read back the real pinned-runtime v1 before B's grant")
            async with engine.connect() as connection:
                a_projection_before = dict((await connection.execute(text("""
                    SELECT state,operation_id,request_hash,claim_owner,claim_fence,
                           applied_version,desired_version,payload_digest
                    FROM memory_projections WHERE scope=:scope AND topic_id=:topic
                """), {"scope": str(scope), "topic": READBACK_RACE_TOPIC})).mappings().one())
            if a_projection_before["claim_fence"] != a_fence or a_projection_before["applied_version"] != 1:
                raise AssertionError(f"worker A did not own the intended v2 projection claim: {a_projection_before}")
            async with engine.begin() as connection:
                expired_claims = await connection.execute(text("""
                    UPDATE memory_projections SET claim_expires_at=now()-interval '1 second'
                    WHERE scope=:scope AND topic_id=:topic AND claim_owner=:owner AND claim_fence=:fence
                    RETURNING claim_fence
                """), {
                    "scope": str(scope), "topic": READBACK_RACE_TOPIC,
                    "owner": race_worker_name, "fence": a_fence,
                })
                if expired_claims.scalar_one_or_none() != a_fence:
                    raise AssertionError("could not expire exactly worker A's projection claim")

            # Same configured worker name and deterministic topic lease owner.
            projection_barrier.arm("authorize_committed")
            stop_b, worker_b = start_worker(container)
            worker_runs.append((stop_b, worker_b))
            await asyncio.wait_for(projection_barrier.entered.wait(), 40)
            b_fence = max(race_jobs)
            job_b = race_jobs[b_fence]
            snapshot_b = race_snapshots[b_fence]["snapshot"]
            projection_b = race_snapshots[b_fence]["projection"]
            if b_fence <= a_fence or job_a.worker_id != job_b.worker_id:
                raise AssertionError("worker B did not replace A with a newer claim under the same worker name")
            if (
                projection_a.binding.lease_owner != projection_b.binding.lease_owner
                or projection_a.binding.lease_fence != projection_b.binding.lease_fence
            ):
                raise AssertionError("A and B did not retain the same stable topic lease owner and agent fence")
            if snapshot_b.candidates:
                raise AssertionError("worker B captured the later authorization before its pinned read")

            async with engine.connect() as connection:
                guard_before_a = dict((await connection.execute(text("""
                    SELECT id,operation_id,request_hash,scope,registry_id,topic_id,state,
                           lease_owner,lease_fence,claim_owner,claim_fence,source_version,payload_digest
                    FROM memory_projection_write_guards
                    WHERE scope=:scope AND topic_id=:topic AND state='AUTHORIZED'
                    ORDER BY created_at DESC,id DESC LIMIT 1
                """), {"scope": str(scope), "topic": READBACK_RACE_TOPIC})).mappings().one())
                b_projection_before_a = dict((await connection.execute(text("""
                    SELECT state,operation_id,claim_owner,claim_fence,applied_version,desired_version
                    FROM memory_projections WHERE scope=:scope AND topic_id=:topic
                """), {"scope": str(scope), "topic": READBACK_RACE_TOPIC})).mappings().one())
                b_operation_before_a = await connection.scalar(text("""
                    SELECT state FROM memory_projection_operations WHERE id=:operation
                """), {"operation": str(guard_before_a["operation_id"])})
                lease_before_a = dict((await connection.execute(text("""
                    SELECT owner_worker,fence,expires_at FROM agent_leases WHERE registry_id=:registry
                """), {"registry": str(registry.registry_id)})).mappings().one())
            if (
                guard_before_a["state"] != "AUTHORIZED"
                or guard_before_a["operation_id"] != str(projection_b.operation_id)
                or guard_before_a["claim_owner"] != race_worker_name
                or guard_before_a["claim_fence"] != b_fence
                or b_projection_before_a["state"] != "CLAIMED"
                or b_projection_before_a["claim_fence"] != b_fence
                or b_projection_before_a["applied_version"] != 1
                or b_operation_before_a != "STARTED"
                or lease_before_a["owner_worker"] != projection_b.binding.lease_owner
                or lease_before_a["fence"] != projection_b.binding.lease_fence
            ):
                raise AssertionError("worker B's committed authorization was not fenced before external write")

            # Make the exact version/digest-match case decisive. Insert a second
            # valid PostgreSQL grant after B's empty pre-read snapshot, then run
            # resolution with a deliberately matching observation in a transaction
            # that is rolled back. This probes the candidate-ID boundary only; it
            # does not create or persist runtime-effect evidence.
            binding_b = projection_b.binding
            request_b = ProjectionWriteAuthorization(
                operation_id=projection_b.operation_id, request_hash=projection_b.request_hash,
                scope=binding_b.scope, registry_id=binding_b.registry_id,
                provider_agent_id=binding_b.provider_agent_id,
                creation_operation_id=binding_b.creation_operation_id,
                principal_id=binding_b.principal_id, policy_version=binding_b.policy_version,
                authz_epoch=binding_b.authz_epoch, lease_owner=binding_b.lease_owner,
                lease_fence=binding_b.lease_fence, claim_owner=job_b.worker_id,
                claim_fence=job_b.fence, topic_id=projection_b.topic_id,
                source_version=projection_b.source_version,
                payload_digest=projection_b.payload_digest,
                namespace="hekate.position.v1", target_path="hekate_positions.md",
            )
            matching_observation = MemoryProjectionObservation(
                present=True, topic_id=projection_b.topic_id,
                source_version=projection_b.source_version, format_version=2,
                payload_digest=projection_b.payload_digest, payload=projection_b.payload,
                lease_fence=binding_b.lease_fence, agent_fence=binding_b.lease_fence,
                memory_revision="probe-only-unpersisted-matching-observation", verified=True,
            )
            async with factory() as uow:
                synthetic_grant_id = await uow.projections.authorize_runtime_write(request_b)
                await uow.projections.resolve_write_guards_after_read(
                    job_b, snapshot_b, matching_observation,
                )
                synthetic_guard_state = await uow.session.scalar(text(
                    "SELECT state FROM memory_projection_write_guards WHERE id=:id"
                ), {"id": synthetic_grant_id})
                still_authorized = await uow.session.scalar(text("""
                    SELECT count(*) FROM memory_projection_write_guards
                    WHERE scope=:scope AND topic_id=:topic AND state='AUTHORIZED'
                """), {"scope": str(scope), "topic": READBACK_RACE_TOPIC})
                await uow.rollback()
            if synthetic_guard_state != "AUTHORIZED" or still_authorized != 2:
                raise AssertionError("a same-version/digest post-snapshot grant was resolved by an older observation")

            # Inject failure after a bounded candidate has been updated but before
            # the read-back transaction commits. A's actual v1 observation would
            # otherwise resolve B's v2 guard as NO_EFFECT; rollback must preserve it.
            snapshot_b_with_guard = await projection_application._capture_read_back_snapshot(
                factory, job_b, projection_b,
            )
            if [item.id for item in snapshot_b_with_guard.candidates] != [guard_before_a["id"]]:
                raise AssertionError("post-grant snapshot did not contain exactly B's durable guard")
            original_resolve_guards = PostgresProjectionRepository.resolve_write_guards_after_read

            async def resolve_then_inject_failure(repository, *args, **kwargs):
                await original_resolve_guards(repository, *args, **kwargs)
                raise RuntimeError("phase6a_injected_guard_resolution_failure_before_commit")

            PostgresProjectionRepository.resolve_write_guards_after_read = resolve_then_inject_failure
            rollback_injected = False
            try:
                try:
                    await projection_application._resolve_after_runtime_read(
                        factory, job_b, projection_b, snapshot_b_with_guard,
                        a_runtime_observation_value,
                    )
                except RuntimeError as error:
                    rollback_injected = "phase6a_injected_guard_resolution_failure_before_commit" in str(error)
            finally:
                PostgresProjectionRepository.resolve_write_guards_after_read = original_resolve_guards
            if not rollback_injected:
                raise AssertionError("guard resolution failure injection did not reach its pre-commit boundary")
            async with engine.connect() as connection:
                guard_after_rollback = await connection.scalar(text(
                    "SELECT state FROM memory_projection_write_guards WHERE id=:id"
                ), {"id": guard_before_a["id"]})
                projection_after_rollback = dict((await connection.execute(text("""
                    SELECT state,operation_id,claim_owner,claim_fence,applied_version,desired_version
                    FROM memory_projections WHERE scope=:scope AND topic_id=:topic
                """), {"scope": str(scope), "topic": READBACK_RACE_TOPIC})).mappings().one())
                operation_after_rollback = await connection.scalar(text(
                    "SELECT state FROM memory_projection_operations WHERE id=:id"
                ), {"id": guard_before_a["operation_id"]})
                lease_after_rollback = dict((await connection.execute(text("""
                    SELECT owner_worker,fence FROM agent_leases WHERE registry_id=:registry
                """), {"registry": str(registry.registry_id)})).mappings().one())
            if (
                guard_after_rollback != "AUTHORIZED"
                or projection_after_rollback != b_projection_before_a
                or operation_after_rollback != b_operation_before_a
                or lease_after_rollback != {
                    "owner_worker": lease_before_a["owner_worker"], "fence": lease_before_a["fence"],
                }
            ):
                raise AssertionError("failed guard cleanup transaction partially changed B's durable state")

            read_release.set()
            await asyncio.wait_for(race_done[a_fence].wait(), 20)
            async with engine.connect() as connection:
                guard_after_a = dict((await connection.execute(text("""
                    SELECT state,claim_owner,claim_fence,lease_owner,lease_fence,operation_id
                    FROM memory_projection_write_guards WHERE id=:id
                """), {"id": guard_before_a["id"]})).mappings().one())
                b_projection_after_a = dict((await connection.execute(text("""
                    SELECT state,operation_id,claim_owner,claim_fence,applied_version,desired_version
                    FROM memory_projections WHERE scope=:scope AND topic_id=:topic
                """), {"scope": str(scope), "topic": READBACK_RACE_TOPIC})).mappings().one())
                b_operation_after_a = await connection.scalar(text("""
                    SELECT state FROM memory_projection_operations WHERE id=:operation
                """), {"operation": str(guard_before_a["operation_id"])})
                lease_after_a = dict((await connection.execute(text("""
                    SELECT owner_worker,fence,expires_at FROM agent_leases WHERE registry_id=:registry
                """), {"registry": str(registry.registry_id)})).mappings().one())
            if (
                guard_after_a["state"] != "AUTHORIZED"
                or b_projection_after_a != b_projection_before_a
                or b_operation_after_a != b_operation_before_a
                or lease_after_a != lease_before_a
            ):
                raise AssertionError("stale worker A changed worker B's guard, claim, operation, watermark, or lease")

            # An expired agent lease remains fenced while B's exact runtime write
            # is unresolved. Then restore only its TTL so B can finish the same call.
            async with engine.begin() as connection:
                await connection.execute(text("""
                    UPDATE agent_leases SET expires_at=now()-interval '1 second'
                    WHERE registry_id=:registry AND owner_worker=:owner AND fence=:fence
                """), {
                    "registry": str(registry.registry_id), "owner": lease_after_a["owner_worker"],
                    "fence": lease_after_a["fence"],
                })
            async with factory() as uow:
                takeover = await uow.agents.acquire_lease(
                    registry.registry_id, f"p6a-readback-unrelated:{run_id}", 45,
                )
                await uow.commit()
            if takeover is not None:
                raise AssertionError("expired lease with B's AUTHORIZED guard allowed an unrelated owner")
            async with engine.begin() as connection:
                await connection.execute(text("""
                    UPDATE agent_leases SET expires_at=now()+interval '120 seconds'
                    WHERE registry_id=:registry AND owner_worker=:owner AND fence=:fence
                """), {
                    "registry": str(registry.registry_id), "owner": lease_after_a["owner_worker"],
                    "fence": lease_after_a["fence"],
                })

            projection_barrier.release()
            await asyncio.wait_for(projection_barrier.completed.wait(), 20)
            await asyncio.wait_for(race_done[b_fence].wait(), 30)
            race_v2_status = await _wait_projection(engine, str(scope), READBACK_RACE_TOPIC, 2)
            if race_v2_status["state"] != "APPLIED" or race_v2_status["applied_version"] != 2:
                raise AssertionError(f"worker B did not complete its pinned runtime write: {race_v2_status}")
            async with engine.connect() as connection:
                race_guard_after_b = dict((await connection.execute(text("""
                    SELECT state,claim_owner,claim_fence,source_version,payload_digest
                    FROM memory_projection_write_guards WHERE id=:id
                """), {"id": guard_before_a["id"]})).mappings().one())
                race_operation_after_b = dict((await connection.execute(text("""
                    SELECT state,observation FROM memory_projection_operations WHERE id=:operation
                """), {"operation": str(guard_before_a["operation_id"])})).mappings().one())
                race_audit_count = await connection.scalar(text("""
                    SELECT count(*) FROM audit_events WHERE owner_scope=:scope
                      AND event_kind='position.memory_projection.confirmed'
                      AND safe_payload->>'projection_operation_id'=:operation
                """), {"scope": str(scope), "operation": str(guard_before_a["operation_id"])})
            race_runtime_final = await _runtime_read(
                factory, runtime_probe, actor, registry, READBACK_RACE_TOPIC,
                str(guard_before_a["operation_id"]),
            )
            if (
                race_guard_after_b["state"] != "EFFECT_CONFIRMED"
                or race_guard_after_b["claim_fence"] != b_fence
                or race_operation_after_b["state"] != "COMPLETED"
                or race_runtime_final.source_version != 2
                or race_runtime_final.payload_digest != race_v2_status["payload_digest"]
                or race_audit_count != 1
            ):
                raise AssertionError("B's runtime effect did not converge once to its own guard and watermark")

            stop_b.set()
            await _wait_worker(stop_b, worker_b)
            worker_runs.remove((stop_b, worker_b))
            stop_a.set()
            await _wait_worker(stop_a, worker_a)
            worker_runs.remove((stop_a, worker_a))
            writes_before_restart = runtime_probe.write_calls
            restart_claim_tick = asyncio.Event()
            original_worker_claim = worker_service.claim_pending_projections

            async def observe_restart_claim(*args, **kwargs):
                claimed = await original_worker_claim(*args, **kwargs)
                restart_claim_tick.set()
                return claimed

            worker_service.claim_pending_projections = observe_restart_claim
            try:
                restart_stop, restart_worker = start_worker(container)
                worker_runs.append((restart_stop, restart_worker))
                await asyncio.wait_for(restart_claim_tick.wait(), 20)
                restart_stop.set()
                await _wait_worker(restart_stop, restart_worker)
                worker_runs.remove((restart_stop, restart_worker))
            finally:
                worker_service.claim_pending_projections = original_worker_claim
            if runtime_probe.write_calls != writes_before_restart:
                raise AssertionError("worker restart replayed the completed projection write")

            hardening_results["read_back_guard_snapshot_and_stale_claim_fencing"] = {
                "topic_id": READBACK_RACE_TOPIC,
                "worker_a": {"id": job_a.worker_id, "claim_fence": a_fence, "captured_guard_ids": []},
                "worker_b": {
                    "id": job_b.worker_id, "claim_fence": b_fence,
                    "captured_guard_ids_before_runtime_read": [candidate.id for candidate in snapshot_b.candidates],
                },
                "agent_lease": {
                    "owner": projection_b.binding.lease_owner,
                    "fence": projection_b.binding.lease_fence,
                    "same_for_a_and_b": (
                        projection_a.binding.lease_owner == projection_b.binding.lease_owner
                        and projection_a.binding.lease_fence == projection_b.binding.lease_fence
                    ),
                },
                "runtime_observation_a": a_runtime_observation,
                "same_version_digest_post_snapshot_grant": {
                    "candidate_ids_captured_before_read": [item.id for item in snapshot_b.candidates],
                    "grant_inserted_after_snapshot": True,
                    "matching_observation_version": matching_observation.source_version,
                    "matching_observation_digest": matching_observation.payload_digest,
                    "matching_observation_agent_fence": matching_observation.agent_fence,
                    "guard_state_before_test_transaction_rollback": synthetic_guard_state,
                    "authorized_guards_including_real_b_before_rollback": int(still_authorized),
                    "test_transaction_rolled_back_without_persisting_observation": True,
                },
                "read_back_transaction_rollback": {
                    "injected_after_candidate_update": rollback_injected,
                    "guard_after_rollback": guard_after_rollback,
                    "projection_after_rollback": projection_after_rollback,
                    "operation_after_rollback": operation_after_rollback,
                    "lease_after_rollback": lease_after_rollback,
                },
                "guard_and_projection_before_a_resumed": {
                    "guard": guard_before_a, "projection": b_projection_before_a,
                    "operation_state": b_operation_before_a, "lease": lease_before_a,
                },
                "after_a_resumed": {
                    "guard": guard_after_a, "projection": b_projection_after_a,
                    "operation_state": b_operation_after_a, "lease": lease_after_a,
                    "a_did_not_release_successor_lease": lease_after_a == lease_before_a,
                },
                "expired_lease_unrelated_takeover_denied": takeover is None,
                "after_b_runtime_write": {
                    "guard": race_guard_after_b, "operation": race_operation_after_b,
                    "projection_state": race_v2_status["state"],
                    "applied_version": race_v2_status["applied_version"],
                    "runtime_version": race_runtime_final.source_version,
                    "runtime_digest": race_runtime_final.payload_digest,
                    "projection_digest": race_v2_status["payload_digest"],
                    "confirmation_audit_count": int(race_audit_count),
                    "worker_restart_added_runtime_writes": runtime_probe.write_calls - writes_before_restart,
                },
                "fake_provider_request_delta": fake.count() - readback_fake_before,
                "runtime_write_delta": runtime_probe.write_calls - readback_writes_before,
            }
        finally:
            read_release.set()
            projection_barrier.release()
            worker_service.project_position = original_race_project
            projection_application._capture_read_back_snapshot = original_capture_snapshot

        report["hardening_scenarios"] = hardening_results

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
        completed_projection_ops = sum(row["state"] in {"COMPLETED", "SUPERSEDED"} for row in projection_ops)
        if (
            provider_call_count != fake.count()
            or normal_phase6_provider_requests != 4
            or serialization_provider_request_delta != 2
            or fake.count() != normal_phase6_provider_requests + serialization_provider_request_delta + 1 + 2
        ):
            raise AssertionError(
                "provider usage ledger differs from the normal and hardening fake Task requests: "
                f"db={provider_call_count}, fake={fake.count()}, normal={normal_phase6_provider_requests}, "
                f"serialization={serialization_provider_request_delta}"
            )
        if linked_projection_ops != len(projection_ops):
            raise AssertionError("projection operation provenance did not link to its parent runtime operation")
        if fake.count() != memory_provider_count_before + 7:
            # v3, the independent topic, two serialization fixtures, and the
            # authorization-revocation Task plus two read-back race Tasks use
            # seven requests; projection operations, retries, barriers, and the
            # blocked Task use none.
            raise AssertionError("projection work changed the expected fake-provider request delta")
        if runtime_probe.write_calls < 4 or confirmation_audits != completed_projection_ops:
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
                if "refusing to discard projection authorization and effect observations" not in str(error):
                    raise
                async with engine.connect() as connection:
                    guarded_head = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
                if guarded_head != "0012_projection_write_guard":
                    raise AssertionError("populated projection write-guard downgrade changed the schema head") from error
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
            "readback_race_v1": readback_task_v1, "readback_race_v2": readback_task_v2,
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
            "real_postgres_agent_lease_takeover": {
                "old_owner": old_lease.owner, "old_fence": old_lease.fence,
                "replacement_owner": replacement_lease.owner, "replacement_fence": replacement_lease.fence,
                "runtime_global_agent_fence": final_v3_read.agent_fence,
                "claim_fence_for_final_topic": int(final_v3_status["claim_fence"]),
            },
            "stale_agent_lease_rejected_on_existing_topic": stale_same_topic_rejected,
            "stale_agent_lease_rejected_on_new_topic": stale_new_topic_rejected,
            "stale_rejections_preserved_digest_version_and_runtime_revision": (
                after_stale_topic.source_version == 3
                and after_stale_topic.payload_digest == final_v3_status["payload_digest"]
                and not after_stale_new_topic.present
                and after_stale_new_topic.memory_revision == final_v3_read.memory_revision
            ),
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
            "namespace": "hekate.position.v1", "format_version": 2,
            "entry_limit_bytes": 4096, "document_limit_bytes": 32768, "topic_limit": 8,
            "atomic_commit_and_readback": True, "provider_or_inference_call_for_projection": False,
            "persistent_agent_binding_verified_by_runtime_tags": True,
        }
        report["provider_observations"] = response_observations
        report["projection_runtime_errors"] = runtime_probe.errors if runtime_probe is not None else []
        report["provider_fixture_errors"] = output_errors
        report["provider_requests_by_scenario"] = {
            "normal_phase6a_v1_v2_v3_other_topic": normal_phase6_provider_requests,
            "same_worker_serialization_position_tasks": serialization_provider_request_delta,
            "queued_task_blocked_by_active_projection": 0,
            "authorization_denial_and_current_epoch_projection_retry": 1,
            "read_back_guard_claim_race_v1_v2_tasks": 2,
        }
        report["runtime_memory_write_attempts"] = runtime_probe.write_calls if runtime_probe is not None else 0
        report["runtime_memory_commits_inferred_from_revision_changes"] = "recorded per hardening scenario; denied write left revision unchanged"
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
        if projection_barrier is not None:
            projection_barrier.release()
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


def _binding(
    actor, registry, lease_owner: str, lease_fence: int,
    claim_owner: str | None = None, claim_fence: int | None = None,
) -> ProjectionBinding:
    return ProjectionBinding(
        scope=actor.scope, registry_id=registry.registry_id, provider_agent_id=registry.provider_id,
        creation_operation_id=registry.creation_operation_id, authz_epoch=actor.authz_epoch,
        policy_version=actor.policy_version, principal_id=actor.principal_id,
        lease_owner=lease_owner, lease_fence=lease_fence,
        claim_owner=claim_owner, claim_fence=claim_fence,
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
        if empty_upgraded != "0012_projection_write_guard":
            raise AssertionError(f"fresh upgrade did not reach the projection write-guard head: {empty_upgraded}")
        p3.command.downgrade(empty_cfg, "-1")
        empty_downgraded = asyncio.run(migration_head(args.empty_migration_database_url))
        if empty_downgraded != "0011_phase6a_memory_projection":
            raise AssertionError(f"empty projection write-guard downgrade did not stop at 0011: {empty_downgraded}")
        p3.command.upgrade(empty_cfg, "head")
        empty_reupgraded = asyncio.run(migration_head(args.empty_migration_database_url))
        if empty_reupgraded != "0012_projection_write_guard":
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
