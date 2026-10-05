from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from unittest.mock import patch
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from alembic import command
from alembic.config import Config
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import integration_probe as p1  # pinned runtime builder and isolated Docker network helpers

from hekate.application.budgets import authorize_provider_call, consume_call_permit
from hekate.application.operations import admit_operation, prepare_runtime_session, record_dispatch_send_intent
from hekate.application.runtime_inbox import InboxBinding, RuntimeInboxPayload, process_runtime_observation
from hekate.application.tasks import cancel, revise
from hekate.domain.budget_math import price_usage
from hekate.domain.contracts import canonical_json, canonical_json_hash
from hekate.domain.errors import HekateError, StaleInput, StorageUnavailable, UnknownExecution
from hekate.domain.models import (
    AccountSnapshot,
    AdmissionRequest,
    AgentRecord,
    AuthorizationSnapshot,
    BillableCallIntent,
    ExecutionEnvelope,
    GuardBinding,
    InputChange,
    NormalizedUsage,
    PriceTable,
    ProviderCallPlan,
    ReservationRequest,
    RuntimeBinding,
    RuntimeLimits,
    Task,
)
from hekate.domain.types import (
    AccountingCallId,
    ActorContext,
    AttemptId,
    OperationId,
    PermitId,
    PrincipalId,
    ProviderAgentId,
    RegistryId,
    ReservationId,
    StopReason,
    ScopeId,
    TaskId,
    TaskStatus,
)
from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.infrastructure.letta.bridge_protocol import BridgeClient
from hekate.infrastructure.letta.provider_gateway import (
    ProviderGatewayProfile,
    create_provider_gateway,
)
from hekate.infrastructure.letta import provider_gateway as gateway_implementation
from hekate.worker import service as worker_implementation
from hekate.infrastructure.postgres import tables
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.settings import Settings, validate_settings
from hekate.worker.service import _collect_turn, _record_execution, dispatch_job, process_pending_inbox

FAKE_MODEL = p1.FAKE_MODEL
BASE_IMAGE = p1.DEFAULT_IMAGE
WORKER_ID = "phase3-probe-worker"


def reserve_port(host: str) -> int:
    with socket.socket() as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


class FakeProvider:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.behaviors: deque[str] = deque()
        self.requests: list[dict[str, object]] = []
        self.response_factory: Callable[[dict[str, object]], str] | None = None
        self.request_seen = threading.Event()
        self.release_block = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: object) -> None:
                return

            def _reply(self, status: int, body: bytes, content_type: str = "application/json") -> None:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:
                if self.path.rstrip("/") != "/v1/models":
                    self._reply(404, b'{"error":"not found"}')
                    return
                self._reply(200, json.dumps({
                    "object": "list",
                    "data": [{"id": FAKE_MODEL, "object": "model", "created": 0, "owned_by": "hekate-phase3-test"}],
                }).encode())

            def do_POST(self) -> None:
                if self.path != "/v1/chat/completions":
                    self._reply(404, b'{"error":"not found"}')
                    return
                try:
                    request_body = json.loads(self.rfile.read(int(self.headers.get("content-length", "0"))))
                except (ValueError, json.JSONDecodeError):
                    self._reply(400, b'{"error":"invalid JSON"}')
                    return
                with owner.lock:
                    sequence = len(owner.requests) + 1
                    behavior = owner.behaviors.popleft() if owner.behaviors else "normal"
                    owner.requests.append({
                        "sequence": sequence,
                        "response_id": None if behavior == "error" else f"fake-response-{sequence}",
                        "model": request_body.get("model"),
                        "stream": request_body.get("stream"),
                        "max_tokens": request_body.get("max_tokens"),
                        "max_completion_tokens": request_body.get("max_completion_tokens"),
                        "request_hash": hashlib.sha256(json.dumps(
                            request_body, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                        ).encode("utf-8")).hexdigest(),
                        "behavior": behavior,
                    })
                    response_factory = owner.response_factory
                owner.request_seen.set()
                if behavior == "block":
                    owner.release_block.wait(20)
                if behavior == "error":
                    self._reply(500, b'{"error":{"message":"isolated fake error"}}')
                    return
                response_id = f"fake-response-{sequence}"
                usage = {"prompt_tokens": 37, "completion_tokens": 4, "total_tokens": 41}
                try:
                    assistant_content = response_factory(request_body) if response_factory is not None else "Phase 3 fake response."
                except Exception:
                    self._reply(500, b'{"error":{"message":"isolated fake fixture rejected the request"}}')
                    return
                tools = request_body.get("tools")
                structured_tool = isinstance(tools, list) and any(
                    isinstance(tool, dict) and isinstance(tool.get("function"), dict)
                    and tool["function"].get("name") == "StructuredOutput"
                    for tool in tools
                )
                structured_call = structured_tool and isinstance(assistant_content, dict)
                call_id = f"fake-structured-call-{sequence}"
                if request_body.get("stream") is True:
                    if structured_call:
                        chunks = [
                            {"id": response_id, "object": "chat.completion.chunk", "created": 1,
                             "model": request_body.get("model"), "choices": [{"index": 0, "delta": {"role": "assistant", "tool_calls": [{"index": 0, "id": call_id, "type": "function", "function": {"name": "StructuredOutput", "arguments": json.dumps(assistant_content, ensure_ascii=False, separators=(",", ":"))}}]}, "finish_reason": None}]},
                            {"id": response_id, "object": "chat.completion.chunk", "created": 1,
                             "model": request_body.get("model"), "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]},
                            {"id": response_id, "object": "chat.completion.chunk", "created": 1,
                             "model": request_body.get("model"), "choices": [], "usage": usage},
                        ]
                    else:
                        chunks = [
                            {"id": response_id, "object": "chat.completion.chunk", "created": 1,
                             "model": request_body.get("model"), "choices": [{"index": 0, "delta": {"role": "assistant", "content": assistant_content}, "finish_reason": None}]},
                            {"id": response_id, "object": "chat.completion.chunk", "created": 1,
                             "model": request_body.get("model"), "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                            {"id": response_id, "object": "chat.completion.chunk", "created": 1,
                             "model": request_body.get("model"), "choices": [], "usage": usage},
                        ]
                    body = b"".join(b"data: " + json.dumps(item, separators=(",", ":")).encode() + b"\n\n" for item in chunks)
                    body += b"data: [DONE]\n\n"
                    self._reply(200, body, "text/event-stream")
                else:
                    message = {"role": "assistant", "content": None, "tool_calls": [{
                        "id": call_id, "type": "function",
                        "function": {"name": "StructuredOutput", "arguments": json.dumps(assistant_content, ensure_ascii=False, separators=(",", ":"))},
                    }]} if structured_call else {"role": "assistant", "content": assistant_content}
                    body = json.dumps({
                        "id": response_id,
                        "object": "chat.completion",
                        "model": request_body.get("model"),
                        "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if structured_call else "stop"}],
                        "usage": usage,
                    }, separators=(",", ":")).encode()
                    self._reply(200, body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return self.server.server_port

    def start(self) -> None:
        self.thread.start()

    def set_next(self, behavior: str) -> None:
        with self.lock:
            self.behaviors.append(behavior)
        if behavior == "block":
            self.request_seen.clear()
            self.release_block.clear()

    def set_response_factory(self, factory: Callable[[dict[str, object]], str] | None) -> None:
        with self.lock:
            self.response_factory = factory

    def count(self) -> int:
        with self.lock:
            return len(self.requests)

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class Phase3Sandbox(p1.DockerSandbox):
    container_address: str = ""

    def start_pinned_app_server(self, gateway_port: int, token: str) -> None:
        storage = self.state / "lc-local-backend" / "providers"
        storage.mkdir(parents=True, exist_ok=True)
        now = p1.utc_now()
        (storage / "auth.json").write_text(json.dumps({
            "version": 1,
            "providers": {
                "openai-compatible": {
                    "id": "phase3-fake-provider",
                    "name": "openai-compatible",
                    "provider_type": "openai-compatible",
                    "provider_category": "byok",
                    "auth": {"type": "api", "key": token},
                    "base_url": f"http://hekate-fake-provider:{gateway_port}/v1",
                    "created_at": now,
                    "updated_at": now,
                },
            },
        }, separators=(",", ":")), encoding="utf-8")
        self.token_path.write_text(token, encoding="utf-8")
        self.token_path.chmod(0o600)
        self.port = p1.APP_PORT
        p1.run([
            "docker", "run", "--detach", "--name", self.container,
            "--network", self.network,
            "--env", "HEKATE_REQUIRE_PROVIDER_BINDING=1",
            "--add-host", f"hekate-fake-provider:{self.gateway_address}",
            "--mount", f"type=bind,source={self.state},target=/root/.letta",
            "--mount", f"type=bind,source={self.token_path},target=/run/secrets/hekate-ws-token,readonly",
            self.image,
            "letta", "--backend", "local", "server", "--listen", f"ws://0.0.0.0:{p1.APP_PORT}",
            "--ws-auth", "capability-token", "--ws-token-file", "/run/secrets/hekate-ws-token",
        ], timeout=120)
        self.container_created = True
        end = time.monotonic() + 90
        while time.monotonic() < end:
            logs = subprocess.run(["docker", "logs", "--tail", "30", self.container], capture_output=True, text=True, check=False, timeout=15)
            if "Listening on ws://" in logs.stdout + logs.stderr:
                self.container_address = p1.run([
                    "docker", "inspect", "--format",
                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", self.container,
                ], timeout=10)
                return
            running = p1.run(["docker", "inspect", "--format", "{{.State.Running}}", self.container], timeout=10)
            if running != "true":
                raise p1.ProbeError("pinned App Server exited during startup")
            time.sleep(1)
        raise p1.ProbeError("pinned App Server did not become ready")


async def start_gateway_server(app, host: str, port: int):
    import uvicorn

    server = uvicorn.Server(uvicorn.Config(app, host=host, port=port, log_level="error", access_log=False))
    task = asyncio.create_task(server.serve())
    end = time.monotonic() + 10
    while not server.started and time.monotonic() < end:
        if task.done():
            await task
        await asyncio.sleep(0.05)
    if not server.started:
        raise RuntimeError("DB provider gateway did not start")
    return server, task


def _gateway_request(address: str, port: int, token: str, headers: dict[str, str], body: dict[str, object], path: str) -> tuple[int, bytes]:
    request = Request(
        f"http://{address}:{port}{path}",
        data=json.dumps(body, separators=(",", ":")).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json", **headers},
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            return response.status, response.read()
    except HTTPError as error:
        return error.code, error.read()
    except (URLError, TimeoutError) as error:
        return 599, str(error).encode()[:500]


async def gateway_request(address: str, port: int, token: str, headers: dict[str, str], body: dict[str, object], path: str = "/v1/chat/completions") -> tuple[int, bytes]:
    return await asyncio.to_thread(_gateway_request, address, port, token, headers, body, path)


def runtime_headers(binding: GuardBinding, operation_id: str, accounting_call_id: str, call_kind: str, max_output_tokens: int, model: str = FAKE_MODEL) -> dict[str, str]:
    return {
        "x-hekate-task-id": str(binding.task_id),
        "x-hekate-attempt-id": str(binding.attempt_id),
        "x-hekate-operation-id": operation_id,
        "x-hekate-agent-registry-id": str(binding.agent_registry_id),
        "x-hekate-provider-agent-id": str(binding.provider_agent_id),
        "x-hekate-input-revision": str(binding.input_revision),
        "x-hekate-fence": str(binding.fence),
        "x-hekate-conversation-id": str(binding.conversation_id),
        "x-hekate-accounting-call-id": accounting_call_id,
        "x-hekate-call-kind": call_kind,
        "x-hekate-model": model,
        "x-hekate-max-output-tokens": str(max_output_tokens),
    }


async def seed_agent(
    factory,
    runtime: LettaRuntimeAdapter,
    name: str,
    worker_id: str,
    *,
    call_plan: ProviderCallPlan,
    attempt_name: str = "attempt-1",
    operation_name: str = "operation-1",
    message: str = "Answer briefly using only the isolated fake provider.",
):
    task_id = TaskId(f"task:{name}")
    scope = ScopeId(f"scope:{name}")
    principal = PrincipalId(f"principal:{name}")
    registry_id = RegistryId(f"ha:{name}")
    task_account = f"task-budget:{name}"
    system_account = "system-budget:phase3"
    policy = "phase3-probe-policy-v1"
    provider = await runtime.create_agent({
        "owner": f"phase3:{name}",
        "creation_tag": f"{name}-{uuid.uuid4().hex[:8]}",
        "role": "hekate",
        "model": f"openai-compatible/{FAKE_MODEL}",
        "max_input_tokens": call_plan.max_input_tokens,
        "max_output_tokens": call_plan.max_output_tokens,
    }, OperationId(f"agent-create:{name}"))
    provider_id = ProviderAgentId(str(provider["provider_agent_id"]))
    base_binding = RuntimeBinding(
        task_id=task_id,
        attempt_id=AttemptId(f"attempt:{name}:{attempt_name}"),
        agent_registry_id=registry_id,
        provider_agent_id=provider_id,
        conversation_id="prepare-session",
        input_revision=1,
        fence=1,
    )
    now = datetime.now(UTC)
    async with factory() as uow:
        await uow.tasks.insert_scope(AuthorizationSnapshot(scope, principal, policy, 1))
        await uow.tasks.insert_task(Task(
            id=task_id,
            scope=scope,
            question="isolated Phase 3 runtime probe",
            input_revision=1,
            constraints_hash="a" * 64,
            status=TaskStatus.QUEUED,
            deadline=now + timedelta(minutes=4),
        ), {"probe": True})
        await uow.delivery.claim_operation(
            OperationId(f"agent-create:{name}"), scope, task_id, "agent.create", "b" * 64, {}, {},
        )
        await uow.agents.insert_intent(AgentRecord(
            registry_id=registry_id,
            owner_scope=scope,
            kind="hekate",
            creation_operation_id=OperationId(f"agent-create:{name}"),
            provider_id=provider_id,
            intended_state="READY",
            observation="PRESENT",
            policy_version=policy,
        ))
        await uow.budgets.create_account(AccountSnapshot(
            id=task_account, scope_kind="TASK", scope_ref=str(task_id), period_id="task-lifetime",
            limit_amount=Decimal("20"), spent_amount=Decimal(0), held_amount=Decimal(0),
        ))
        lease = await uow.agents.acquire_lease(registry_id, worker_id, 240)
        await uow.commit()
    base_binding = base_binding.model_copy(update={"fence": lease.fence})
    binding, session = await prepare_runtime_session(factory, runtime, base_binding, worker_id)
    operation_id = OperationId(f"operation:{name}:{operation_name}")
    attempt_id = AttemptId(f"attempt:{name}:{attempt_name}")
    reservation_id = ReservationId(f"reservation:{name}:{operation_name}")
    trusted = GuardBinding(
        task_id=task_id, attempt_id=attempt_id, agent_registry_id=registry_id,
        provider_agent_id=provider_id, principal_id=principal, scope=scope,
        input_revision=1, policy_version=policy, authz_epoch=1, fence=lease.fence,
        conversation_id=binding.conversation_id,
    )
    envelope = ExecutionEnvelope(
        task_id=task_id, attempt_id=attempt_id, operation_id=operation_id,
        principal_id=principal, scope=scope, input_revision=1,
        model_allowlist=(FAKE_MODEL,), pricing_version=call_plan.pricing_version,
        deadline=now + timedelta(minutes=3), max_input_tokens=call_plan.max_input_tokens,
        max_output_tokens=call_plan.max_output_tokens,
        billable_call_slots=call_plan.main_turn_calls + call_plan.compaction_calls + call_plan.retry_calls,
        max_tool_calls=0, fence=lease.fence, reservation_id=reservation_id,
    )
    request = AdmissionRequest(
        binding=trusted,
        reservation=ReservationRequest(
            id=reservation_id, operation_id=operation_id, purpose="operation_envelope",
            amount=Decimal("10"), task_id=task_id, task_account_id=task_account,
            system_account_id=system_account, pricing_version=call_plan.pricing_version,
            system_period_id="phase3-probe",
        ),
        envelope=envelope,
        attempt_kind="planning",
        parent_attempt_id=None,
        operation_kind="turn",
        payload={"message": message, "call_plan": call_plan.model_dump(mode="json")},
        lease_owner=worker_id,
    )
    receipt = await admit_operation(factory, request)
    return {
        "task_id": task_id,
        "scope": scope,
        "principal": principal,
        "registry_id": registry_id,
        "provider_id": provider_id,
        "task_account": task_account,
        "system_account": system_account,
        "policy": policy,
        "worker_id": worker_id,
        "lease": lease,
        "binding": trusted,
        "runtime_binding": binding,
        "envelope": envelope,
        "request": request,
        "receipt": receipt,
        "session": session,
        "profile": call_plan.profile_id,
    }


async def claim_one(factory, worker: str):
    async with factory() as uow:
        jobs = await uow.delivery.claim_jobs(worker, 1, 30)
        await uow.commit()
        return jobs[0] if jobs else None


async def fetch_operation(engine, operation_id: str) -> dict[str, object]:
    async with engine.connect() as connection:
        row = (await connection.execute(text("""
            SELECT o.state, o.dispatch_state, o.execution_state, a.status AS attempt_status,
                   h.state AS hold_state, b.status AS call_status, b.accounting_call_id,
                   b.call_kind, b.provider_call_id, cp.permit_id, cp.state AS permit_state,
                   u.completeness, u.settlement_state, u.evaluated_cost_usd
            FROM operations o
            LEFT JOIN attempts a ON a.operation_id=o.id
            LEFT JOIN agent_execution_holds h ON h.operation_id=o.id
            LEFT JOIN provider_calls b ON b.operation_id=o.id
            LEFT JOIN call_permits cp ON cp.accounting_call_id=b.accounting_call_id
            LEFT JOIN usage_projections u ON u.accounting_call_id=b.accounting_call_id
            WHERE o.id=:operation_id
            ORDER BY b.accounting_call_id
        """), {"operation_id": operation_id})).mappings().all()
    return {"rows": [dict(row) for row in row]}


def call_intent(context, call_plan, call_id: str, call_kind: str, profile: ProviderGatewayProfile, output_tokens: int) -> BillableCallIntent:
    request = context["request"]
    limits = RuntimeLimits(
        max_input_tokens=call_plan.max_input_tokens,
        max_output_tokens=output_tokens,
        max_billable_calls=request.envelope.billable_call_slots,
        deadline=request.envelope.deadline,
    )
    allocation = price_usage(NormalizedUsage(
        completeness="COMPLETE", input_tokens=call_plan.max_input_tokens,
        output_tokens=output_tokens, total_tokens=call_plan.max_input_tokens + output_tokens,
    ), profile.price_table).amount
    return BillableCallIntent(
        accounting_call_id=AccountingCallId(call_id),
        permit_id=PermitId(str(uuid.uuid5(uuid.NAMESPACE_URL, f"hekate:permit:{call_id}"))),
        operation_id=request.envelope.operation_id,
        call_kind=call_kind,
        slot_key=f"{call_kind}:{call_id}",
        binding=request.binding,
        model=FAKE_MODEL,
        allocation_amount=allocation,
        limits=limits,
        price_table=profile.price_table,
        permit_expires_at=min(request.envelope.deadline, datetime.now(UTC) + timedelta(seconds=25)),
        lease_owner=request.lease_owner,
        test_only=True,
        reservation_id=request.envelope.reservation_id,
    )


def actor_for(context) -> ActorContext:
    request = context["request"]
    lease = context["lease"]
    return ActorContext(
        principal_id=context["principal"],
        scope=context["scope"],
        authenticated_agent_registry_id=None,
        task_id=context["task_id"],
        attempt_id=request.binding.attempt_id,
        input_revision=request.binding.input_revision,
        policy_version=context["policy"],
        authz_epoch=1,
        fence=lease.fence,
    )


async def _gateway_task_state_race(
    factory,
    engine,
    fake: FakeProvider,
    runtime: LettaRuntimeAdapter,
    profile: ProviderGatewayProfile,
    gateway_address: str,
    gateway_port: int,
    private_token: str,
    run_id: str,
    mutation: str,
) -> dict[str, object]:
    plan = ProviderCallPlan(
        profile_id=profile.profile_id,
        model=FAKE_MODEL,
        pricing_version=profile.price_table.version,
        max_input_tokens=128,
        max_output_tokens=16,
        main_turn_calls=1,
        compaction_calls=0,
    )
    context = await seed_agent(
        factory, runtime, f"{run_id}:t5-{mutation}", WORKER_ID,
        call_plan=plan, attempt_name=f"t5-{mutation}", operation_name=f"t5-{mutation}",
    )
    job = await claim_one(factory, WORKER_ID)
    if job is None:
        raise RuntimeError(f"T5 {mutation} race had no outbox job")
    await record_dispatch_send_intent(factory, job, WORKER_ID)

    state: dict[str, object] = {"authorized": False, "mutation": None}
    actor = actor_for(context)
    original_authorize = gateway_implementation.authorize_provider_call

    async def authorize_then_mutate(uow_factory, intent):
        permit = await original_authorize(uow_factory, intent)
        state["authorized"] = True
        state["permit_id"] = str(permit.permit_id)
        if mutation == "revision":
            updated = await revise(
                uow_factory,
                actor,
                context["task_id"],
                1,
                InputChange(text="revision during permit admission", expected_revision=1, constraints={"probe": mutation}),
            )
            state["mutation"] = updated.input_revision
        else:
            cancelled = await cancel(uow_factory, actor, context["task_id"], StopReason.USER_CANCELLED)
            state["mutation"] = cancelled["state"]
        return permit

    operation_id = str(context["request"].envelope.operation_id)
    call_id = f"acc:{run_id}:t5-authorize-{mutation}"
    before = fake.count()
    with patch.object(gateway_implementation, "authorize_provider_call", new=authorize_then_mutate):
        status, _ = await gateway_request(
            gateway_address,
            gateway_port,
            private_token,
            runtime_headers(context["binding"], operation_id, call_id, "turn", 16),
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": mutation}]},
        )
    forward_delta = fake.count() - before
    rows = (await fetch_operation(engine, operation_id))["rows"]
    row = next((item for item in rows if item.get("accounting_call_id") == call_id), {})
    return {
        "authorized_before_mutation": state["authorized"],
        "task_mutation_result": state["mutation"],
        "gateway_status": status,
        "provider_forward_delta": forward_delta,
        "provider_call_state": row.get("call_status"),
        "permit_state": row.get("permit_state"),
        "blocked_before_forward": (
            state["authorized"] and status == 402 and forward_delta == 0
            and row.get("call_status") == "ALLOCATED" and row.get("permit_state") == "ISSUED"
        ),
    }


async def _gateway_storage_failure(
    factory,
    engine,
    fake: FakeProvider,
    runtime: LettaRuntimeAdapter,
    profile: ProviderGatewayProfile,
    gateway_address: str,
    gateway_port: int,
    private_token: str,
    run_id: str,
    stage: str,
) -> dict[str, object]:
    plan = ProviderCallPlan(
        profile_id=profile.profile_id,
        model=FAKE_MODEL,
        pricing_version=profile.price_table.version,
        max_input_tokens=128,
        max_output_tokens=16,
        main_turn_calls=1,
        compaction_calls=0,
    )
    context = await seed_agent(
        factory, runtime, f"{run_id}:t5-db-{stage}", WORKER_ID,
        call_plan=plan, attempt_name=f"t5-db-{stage}", operation_name=f"t5-db-{stage}",
    )
    job = await claim_one(factory, WORKER_ID)
    if job is None:
        raise RuntimeError(f"T5 {stage} failure had no outbox job")
    await record_dispatch_send_intent(factory, job, WORKER_ID)
    operation_id = str(context["request"].envelope.operation_id)
    call_id = f"acc:{run_id}:t5-db-{stage}"

    async def fail_storage(*_args, **_kwargs):
        raise StorageUnavailable(f"injected provider gateway {stage} failure")

    patch_target = "authorize_provider_call" if stage == "authorize" else "consume_call_permit"
    before = fake.count()
    with patch.object(gateway_implementation, patch_target, new=fail_storage):
        status, _ = await gateway_request(
            gateway_address,
            gateway_port,
            private_token,
            runtime_headers(context["binding"], operation_id, call_id, "turn", 16),
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": stage}]},
        )
    row = next((item for item in (await fetch_operation(engine, operation_id))["rows"] if item.get("accounting_call_id") == call_id), {})
    return {
        "gateway_status": status,
        "provider_forward_delta": fake.count() - before,
        "provider_call_state": row.get("call_status"),
        "permit_state": row.get("permit_state"),
        "blocked_before_forward": status == 402 and fake.count() == before,
    }


async def _gateway_post_forward_record_failure(
    factory,
    engine,
    fake: FakeProvider,
    runtime: LettaRuntimeAdapter,
    profile: ProviderGatewayProfile,
    gateway_app,
    gateway_address: str,
    gateway_port: int,
    private_token: str,
    run_id: str,
) -> dict[str, object]:
    plan = ProviderCallPlan(
        profile_id=profile.profile_id,
        model=FAKE_MODEL,
        pricing_version=profile.price_table.version,
        max_input_tokens=128,
        max_output_tokens=16,
        main_turn_calls=1,
        compaction_calls=0,
    )
    context = await seed_agent(
        factory, runtime, f"{run_id}:t5-post-forward-db-failure", WORKER_ID,
        call_plan=plan, attempt_name="t5-post-forward-db-failure", operation_name="t5-post-forward-db-failure",
    )
    job = await claim_one(factory, WORKER_ID)
    if job is None:
        raise RuntimeError("T5 post-forward recording failure had no outbox job")
    await record_dispatch_send_intent(factory, job, WORKER_ID)
    operation_id = str(context["request"].envelope.operation_id)
    call_id = f"acc:{run_id}:t5-post-forward-db-failure"
    before = fake.count()
    failures_before = gateway_app.state.metrics["observation_write_failures"]

    async def fail_observation(*_args, **_kwargs):
        raise StorageUnavailable("injected observation storage failure after provider response")

    with patch.object(gateway_implementation, "process_runtime_observation", new=fail_observation):
        first_status, _ = await gateway_request(
            gateway_address,
            gateway_port,
            private_token,
            runtime_headers(context["binding"], operation_id, call_id, "turn", 16),
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": "write failure after forward"}]},
        )
    first_forward_delta = fake.count() - before
    first_rows = (await fetch_operation(engine, operation_id))["rows"]
    first_call = next((item for item in first_rows if item.get("accounting_call_id") == call_id), {})
    replay_status, _ = await gateway_request(
        gateway_address,
        gateway_port,
        private_token,
        runtime_headers(context["binding"], operation_id, call_id, "turn", 16),
        {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": "replay after write failure"}]},
    )
    return {
        "first_gateway_status": first_status,
        "first_provider_forward_delta": first_forward_delta,
        "observation_write_failure_delta": gateway_app.state.metrics["observation_write_failures"] - failures_before,
        "call_state_after_write_failure": first_call.get("call_status"),
        "permit_state_after_write_failure": first_call.get("permit_state"),
        "settlement_state_after_write_failure": first_call.get("settlement_state"),
        "same_id_replay_status": replay_status,
        "replay_provider_forward_delta": fake.count() - before - first_forward_delta,
        "blocked_after_write_failure": (
            first_status == 200 and first_forward_delta == 1
            and gateway_app.state.metrics["observation_write_failures"] - failures_before == 1
            and first_call.get("call_status") == "CONSUMED"
            and first_call.get("permit_state") == "CONSUMED"
            and replay_status == 402 and fake.count() - before - first_forward_delta == 0
        ),
    }


async def _db_summary(engine, operation_id: str) -> dict[str, object]:
    info = await fetch_operation(engine, operation_id)
    rows = info["rows"]
    async with engine.connect() as connection:
        outbox = (await connection.execute(text("""
            SELECT status, send_intent_at, send_intent_owner, send_intent_fence, claim_fence
            FROM outbox WHERE operation_id=:operation_id AND kind='dispatch'
        """), {"operation_id": operation_id})).mappings().one()
        permit_counts = (await connection.execute(text("""
            SELECT count(*) AS permits, count(*) FILTER (WHERE state='CONSUMED') AS consumed
            FROM call_permits p JOIN provider_calls c USING (accounting_call_id)
            WHERE c.operation_id=:operation_id
        """), {"operation_id": operation_id})).mappings().one()
        ledger_count = await connection.scalar(text("SELECT count(*) FROM budget_ledger l JOIN provider_calls c USING (accounting_call_id) WHERE c.operation_id=:operation_id AND l.effect_type='SETTLE'"), {"operation_id": operation_id})
    return {
        "operation": rows[0] if rows else {},
        "outbox": dict(outbox),
        "permit_count": permit_counts["permits"],
        "consumed_permit_count": permit_counts["consumed"],
        "settlement_ledger_entries": ledger_count,
    }


async def _truncate(factory) -> None:
    async with factory() as uow:
        names = ", ".join(f'"{name}"' for name in tables.metadata().tables)
        await uow.session.execute(text(f"TRUNCATE TABLE {names} CASCADE"))
        await uow.commit()


async def _probe_database_url(database_url: str) -> None:
    parsed = make_url(database_url)
    if parsed.database != "hekate_phase3_test" or parsed.host not in {"127.0.0.1", "localhost"}:
        raise ValueError("probe requires the dedicated local hekate_phase3_test database")


def _locked_runtime(image: str, node: str, archive: Path) -> dict[str, object]:
    lock = p1.LOCK
    archive_hash = hashlib.sha256(archive.read_bytes()).hexdigest()
    if archive_hash != lock["bridge"]["node_archive_sha256"]:
        raise ValueError("Node archive checksum differs from versions.lock.json")
    version = subprocess.run([node, "--version"], check=True, capture_output=True, text=True, timeout=10).stdout.strip()
    if version != f"v{lock['bridge']['node_version']}":
        raise ValueError("Node runtime differs from versions.lock.json")
    labels = json.loads(p1.run(["docker", "image", "inspect", "--format", "{{json .Config.Labels}}", image]))
    if (
        labels.get("org.opencontainers.image.revision") != lock["app_server"]["source_commit"]
        or labels.get("io.hekate.runtime-patch.sha256") != lock["patches"][0]["sha256"]
    ):
        raise ValueError("pinned App Server image labels do not match versions.lock.json")
    base_image = f"{lock['app_server']['image']}@{lock['app_server']['image_digest']}"
    actual_base_image_id = p1.run(["docker", "image", "inspect", "--format", "{{.Id}}", base_image])
    if actual_base_image_id != lock["app_server"]["image_digest"]:
        raise ValueError("App Server base image ID differs from the locked digest")
    patched_image_id = p1.run(["docker", "image", "inspect", "--format", "{{.Id}}", image])
    patch_file = ROOT / lock["patches"][0]["file"]
    patch_hash = hashlib.sha256(patch_file.read_bytes()).hexdigest()
    if patch_hash != lock["patches"][0]["sha256"]:
        raise ValueError("runtime patch checksum differs from versions.lock.json")
    return {
        "node_version": version.removeprefix("v"),
        "node_archive_sha256": archive_hash,
        "sdk_version": lock["bridge"]["sdk"]["version"],
        "app_server_version": lock["app_server"]["letta_code_version"],
        "app_server_source_commit": lock["app_server"]["source_commit"],
        "app_server_base_image_digest": lock["app_server"]["image_digest"],
        "patched_image_id": patched_image_id,
        "runtime_patch_sha256": patch_hash,
        "bridge_protocol_version": lock["app_server"]["protocol_version"],
    }


async def _run_probe(database_url: str, node: str, image: str, archive: Path, artifact_path: Path, run_id: str) -> dict[str, object]:
    git_head = p1.run(["git", "rev-parse", "HEAD"])
    git_status = p1.run(["git", "status", "--porcelain"]).splitlines()
    report: dict[str, object] = {
        "schema_version": "1",
        "probe": "phase3-runtime-dispatch",
        "run_id": run_id,
        "executed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "base_commit": "6eb11d128cd697d93ef746b0e69d9e3d1dbfce22",
        "checked_out_head": git_head,
        "working_tree_dirty_at_probe": bool(git_status),
        "working_tree_change_count_at_probe": len(git_status),
        "branch": p1.run(["git", "branch", "--show-current"]),
        "real_provider_calls": 0,
        "runtime": {},
        "results": {},
        "limitations": [
            "Synthetic profile only; no real provider key or external provider URL was configured.",
            "G8 full-request tokenization remains unverified; the gateway enforces the admitted conservative input/output plan and does not claim exact input-token measurement.",
            "G7 physical provider-call identity depends on the pinned runtime patch; stock SDK capabilities remain false.",
            "No production readiness, API ingress, business loop, Critic lifecycle, same-execution resume, or operator recovery is claimed.",
        ],
        "overall_status": "blocked",
    }
    engine = None
    bridge = None
    sandbox = None
    fake = None
    gateway_server = None
    gateway_task = None
    gateway_app = None
    gateway_port = 0
    private_token: str | None = None
    gateway_counts: dict[str, object] = {"requests": 0, "status_counts": {}}
    temporary = tempfile.TemporaryDirectory(prefix="hekate-phase3-")
    workdir = Path(temporary.name)
    try:
        await _probe_database_url(database_url)
        runtime_info = _locked_runtime(image, node, archive)
        report["runtime"] = runtime_info
        p1.build_bridge(node)

        engine = create_engine(database_url)
        factory = create_uow_factory(engine)
        await _truncate(factory)
        async with factory() as uow:
            await uow.budgets.create_account(AccountSnapshot(
                id="system-budget:phase3", scope_kind="SYSTEM", scope_ref="hekate", period_id="phase3-probe",
                limit_amount=Decimal("100"), spent_amount=Decimal(0), held_amount=Decimal(0),
            ))
            await uow.commit()
        async with engine.connect() as connection:
            migration_head = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
            postgres_version = await connection.scalar(text("SHOW server_version"))
        if migration_head != "0007_phase5a_critic_workflows":
            raise ValueError("Phase 3 migration is not current")
        report["database"] = {"postgres_version": postgres_version, "migration_head": migration_head}

        fake = FakeProvider()
        fake.start()
        sandbox = Phase3Sandbox(workdir, run_id, image)
        sandbox.start_network()
        gateway_port = reserve_port(sandbox.gateway_address)
        private_token = secrets.token_urlsafe(40)
        profile = ProviderGatewayProfile(
            profile_id="phase3-fake-v1",
            price_table=PriceTable(
                model=FAKE_MODEL,
                version="synthetic-v1",
                input_usd_per_million=Decimal("1"),
                output_usd_per_million=Decimal("2"),
                synthetic=True,
            ),
            upstream_base_url=f"http://127.0.0.1:{fake.port}",
            upstream_api_key="isolated-fake-only",
            max_input_tokens=16_384,
            max_output_tokens=64,
            test_only=True,
        )
        gateway_app = create_provider_gateway(factory, profile, private_token, allow_test_profile=True)

        @gateway_app.middleware("http")
        async def count_gateway_request(request, call_next):
            gateway_counts["requests"] = int(gateway_counts["requests"]) + 1
            response = await call_next(request)
            status_counts = gateway_counts["status_counts"]
            status = str(response.status_code)
            status_counts[status] = int(status_counts.get(status, 0)) + 1
            return response

        gateway_server, gateway_task = await start_gateway_server(gateway_app, sandbox.gateway_address, gateway_port)
        sandbox.start_pinned_app_server(gateway_port, private_token)

        bridge = BridgeClient(node, ROOT / "bridge/letta/dist/main.js", env={
            "HEKATE_LETTA_URL": f"ws://{sandbox.container_address}:{p1.APP_PORT}",
            "HEKATE_LETTA_TOKEN": private_token,
            "HEKATE_REQUIRE_PROVIDER_BINDING": "1",
        })
        runtime = LettaRuntimeAdapter(bridge)
        capabilities = await runtime.verify_compatibility()
        report["bridge_capabilities"] = capabilities
        settings = Settings(
            database_url=database_url,
            node_bin=node,
            bridge_entry=ROOT / "bridge/letta/dist/main.js",
            letta_url=f"ws://{sandbox.container_address}:{p1.APP_PORT}",
            letta_token=private_token,
            worker_id=WORKER_ID,
            runtime_mode="test",
            config_dir=ROOT / "config",
            policy={}, models={}, pricing={},
        )
        from hekate.bootstrap import Container
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)

        plan_t1 = ProviderCallPlan(
            profile_id=profile.profile_id, model=FAKE_MODEL, pricing_version=profile.price_table.version,
            max_input_tokens=16_384, max_output_tokens=64, main_turn_calls=1, compaction_calls=0,
        )
        context_t1 = await seed_agent(factory, runtime, f"{run_id}:t1", WORKER_ID, call_plan=plan_t1)
        t1_job = await claim_one(factory, WORKER_ID)
        if t1_job is None:
            raise RuntimeError("T1 admitted operation had no claimable outbox job")
        before_t1 = fake.count()
        provider_quiescent_waiting = asyncio.Event()
        release_provider_quiescent = asyncio.Event()
        provider_quiescent_applied = asyncio.Event()
        terminal_received = asyncio.Event()
        original_gateway_processor = gateway_implementation.process_runtime_observation
        original_execution_recorder = worker_implementation._record_execution
        t1_operation_id = str(context_t1["request"].envelope.operation_id)

        async def delayed_provider_quiescent(factory_value, scope_value, key_value, payload_value, **kwargs):
            if (
                isinstance(payload_value, RuntimeInboxPayload)
                and payload_value.operation_id == t1_operation_id
                and payload_value.event_type == "provider_call"
                and payload_value.state == "QUIESCENT"
            ):
                provider_quiescent_waiting.set()
                await release_provider_quiescent.wait()
            result_value = await original_gateway_processor(
                factory_value, scope_value, key_value, payload_value, **kwargs,
            )
            if (
                isinstance(payload_value, RuntimeInboxPayload)
                and payload_value.operation_id == t1_operation_id
                and payload_value.event_type == "provider_call"
                and payload_value.state == "QUIESCENT"
            ):
                provider_quiescent_applied.set()
            return result_value

        async def observe_terminal(container_value, binding_value, operation_value, worker_value, state_value, source_value, *, outcome=None, reason=None):
            result_value = await original_execution_recorder(
                container_value, binding_value, operation_value, worker_value, state_value, source_value,
                outcome=outcome, reason=reason,
            )
            if str(operation_value) == t1_operation_id and state_value == "QUIESCENT":
                terminal_received.set()
            return result_value

        gateway_implementation.process_runtime_observation = delayed_provider_quiescent
        worker_implementation._record_execution = observe_terminal
        try:
            dispatch_task = asyncio.create_task(dispatch_job(container, t1_job, WORKER_ID))
            await asyncio.wait_for(provider_quiescent_waiting.wait(), timeout=30)
            await asyncio.wait_for(terminal_received.wait(), timeout=30)
            async with engine.connect() as connection:
                terminal_state = await connection.execute(text("""
                    SELECT processed_at, next_attempt_at, pending_reason
                    FROM inbox
                    WHERE payload->>'operation_id'=:operation_id
                      AND payload->>'event_type'='execution'
                      AND payload->>'state'='QUIESCENT'
                """), {"operation_id": t1_operation_id})
                terminal_row = terminal_state.mappings().one()
                pre_release_call_state = await connection.scalar(text(
                    "SELECT status FROM provider_calls WHERE operation_id=:operation_id"
                ), {"operation_id": t1_operation_id})
                pre_release_execution_state = await connection.scalar(text(
                    "SELECT execution_state FROM operations WHERE id=:operation_id"
                ), {"operation_id": t1_operation_id})
            terminal_waited_for_call = (
                terminal_row["processed_at"] is None
                and terminal_row["pending_reason"] == "call_termination_unconfirmed"
                and pre_release_call_state == "RUNNING"
                and pre_release_execution_state != "QUIESCENT"
            )
            release_provider_quiescent.set()
            await asyncio.wait_for(provider_quiescent_applied.wait(), timeout=30)
            await dispatch_task
        finally:
            release_provider_quiescent.set()
            gateway_implementation.process_runtime_observation = original_gateway_processor
            worker_implementation._record_execution = original_execution_recorder
        t1 = await _db_summary(engine, str(context_t1["request"].envelope.operation_id))
        t1["call_plan"] = plan_t1.model_dump(mode="json")
        t1["fake_provider_requests"] = fake.count() - before_t1
        t1["terminal_waited_for_individual_call_evidence"] = terminal_waited_for_call
        t1["terminal_applied_after_gateway_commit"] = t1["operation"].get("execution_state") == "QUIESCENT"
        t1["agent_tools"] = context_t1["session"].get("agent_tools", [])
        t1["tool_executor_calls"] = context_t1["session"].get("tool_executor_calls", 0)
        t1["blocked_tool_attempts"] = context_t1["session"].get("blocked_tool_attempts", 0)
        t1_call_id = str(t1["operation"]["accounting_call_id"])
        t1_ledger_before_collect = await _ledger_settle_count(engine, t1_call_id)
        t1_requests_before_collect = fake.count()
        await _collect_turn(
            container, context_t1["runtime_binding"], context_t1["request"].envelope.operation_id,
            WORKER_ID, datetime.now(UTC) + timedelta(seconds=2),
        )
        t1["repeated_collect_no_resend_or_duplicate_settlement"] = (
            fake.count() == t1_requests_before_collect
            and await _ledger_settle_count(engine, t1_call_id) == t1_ledger_before_collect
        )
        t1["disabled_toolset_confirmed"] = (
            t1["agent_tools"] == [] and t1["tool_executor_calls"] == 0 and t1["blocked_tool_attempts"] == 0
        )
        t1["passed"] = bool(
            t1["operation"].get("state") == "COMPLETED"
            and t1["operation"].get("dispatch_state") == "QUIESCENT"
            and t1["operation"].get("execution_state") == "QUIESCENT"
            and t1["operation"].get("attempt_status") == "SUCCEEDED"
            and t1["operation"].get("call_status") == "QUIESCENT"
            and t1["operation"].get("settlement_state") == "SETTLED"
            and t1["consumed_permit_count"] == 1
            and t1["fake_provider_requests"] == 1
            and t1["terminal_waited_for_individual_call_evidence"]
            and t1["terminal_applied_after_gateway_commit"]
            and t1["repeated_collect_no_resend_or_duplicate_settlement"]
            and t1["disabled_toolset_confirmed"]
        )
        report["results"]["T1_actual_db_runtime_gateway_fake_provider"] = t1

        t1_binding = context_t1["binding"]
        t1_operation = str(context_t1["request"].envelope.operation_id)
        count_after_t1 = fake.count()
        replay_status, replay_body = await gateway_request(
            sandbox.gateway_address, gateway_port, private_token,
            runtime_headers(t1_binding, t1_operation, t1_call_id, "turn", 64),
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 64, "messages": [{"role": "user", "content": "replay"}]},
        )
        report["results"]["T1_same_accounting_id_replay"] = {
            "status": replay_status,
            "error": replay_body.decode("utf-8", errors="replace") if replay_status == 599 else None,
            "provider_requests_before": count_after_t1,
            "provider_requests_after": fake.count(),
            "blocked_without_forward": replay_status == 402 and fake.count() == count_after_t1,
            "passed": replay_status == 402 and fake.count() == count_after_t1,
        }

        runtime_compaction_plan = ProviderCallPlan(
            profile_id=profile.profile_id, model=FAKE_MODEL, pricing_version=profile.price_table.version,
            max_input_tokens=8_192, max_output_tokens=64, main_turn_calls=1, compaction_calls=2,
        )
        runtime_compaction_context = await seed_agent(
            factory, runtime, f"{run_id}:t2-runtime-compaction", WORKER_ID,
            call_plan=runtime_compaction_plan, attempt_name="runtime-compaction",
            operation_name="runtime-compaction",
            message="Summarize this context and respond briefly. " + ("scope evidence context " * 2_000),
        )
        runtime_compaction_operation = str(runtime_compaction_context["request"].envelope.operation_id)
        runtime_compaction_job = await claim_one(factory, WORKER_ID)
        if runtime_compaction_job is None:
            raise RuntimeError("T2 runtime compaction operation had no claimable outbox job")
        before_runtime_compaction = fake.count()
        await dispatch_job(container, runtime_compaction_job, WORKER_ID)
        runtime_compaction_observation = await runtime.collect(
            runtime_compaction_context["runtime_binding"],
            runtime_compaction_context["request"].envelope.operation_id,
            wait_ms=0,
        )
        runtime_compaction_summary = await _db_summary(engine, runtime_compaction_operation)
        runtime_compaction_rows = (await fetch_operation(engine, runtime_compaction_operation))["rows"]
        runtime_call_kinds = {row["call_kind"] for row in runtime_compaction_rows if row["accounting_call_id"]}
        runtime_compaction_summary.update({
            "call_plan": runtime_compaction_plan.model_dump(mode="json"),
            "call_records": runtime_compaction_rows,
            "bridge_stderr_tail": bridge.stderr_text[-1_000:],
            "runtime_observation": {
                "state": runtime_compaction_observation.get("state"),
                "events": runtime_compaction_observation.get("events"),
            },
            "fake_provider_requests": fake.count() - before_runtime_compaction,
            "physical_call_count": len([row for row in runtime_compaction_rows if row["accounting_call_id"]]),
            "call_kinds": sorted(runtime_call_kinds),
            "all_calls_quiescent_and_settled": bool(runtime_compaction_rows) and all(
                row["call_status"] == "QUIESCENT" and row["completeness"] == "COMPLETE"
                and row["settlement_state"] == "SETTLED" for row in runtime_compaction_rows
                if row["accounting_call_id"]
            ),
        })
        runtime_compaction_summary["passed"] = bool(
            runtime_compaction_summary["operation"].get("state") == "COMPLETED"
            and runtime_compaction_summary["call_kinds"] == ["compaction", "turn"]
            and runtime_compaction_summary["consumed_permit_count"] == 2
            and runtime_compaction_summary["physical_call_count"] == 2
            and runtime_compaction_summary["fake_provider_requests"] == 2
            and runtime_compaction_summary["all_calls_quiescent_and_settled"]
        )
        report["results"]["T2_actual_runtime_compaction"] = runtime_compaction_summary

        plan_t2 = ProviderCallPlan(
            profile_id=profile.profile_id, model=FAKE_MODEL, pricing_version=profile.price_table.version,
            max_input_tokens=128, max_output_tokens=16, main_turn_calls=1, compaction_calls=3,
        )
        context_t2 = await seed_agent(factory, runtime, f"{run_id}:t2", WORKER_ID, call_plan=plan_t2, attempt_name="roles", operation_name="roles")
        operation_t2 = str(context_t2["request"].envelope.operation_id)
        claims_a, claims_b = await asyncio.gather(
            claim_one(factory, WORKER_ID),
            claim_one(factory, f"competitor:{run_id}"),
        )
        winners = [job for job in (claims_a, claims_b) if job is not None]
        if len(winners) != 1:
            raise RuntimeError("competing outbox workers did not produce exactly one claim")
        stale_job = winners[0]
        async with factory() as uow:
            await uow.session.execute(text(
                "UPDATE outbox SET claim_expires_at=now()-interval '1 second' WHERE id=:id"
            ), {"id": stale_job.id})
            await uow.commit()
        t2_job = await claim_one(factory, WORKER_ID)
        if t2_job is None:
            raise RuntimeError("T3 pre-send claim expiry was not reclaimable")
        try:
            await record_dispatch_send_intent(factory, stale_job, WORKER_ID)
            stale_fence_blocked = False
        except (StaleInput, HekateError):
            stale_fence_blocked = True
        await record_dispatch_send_intent(factory, t2_job, WORKER_ID)

        invalid_claims = runtime_headers(context_t2["binding"], operation_t2, f"bad:{run_id}", "compaction", 16)
        invalid_claims["x-hekate-attempt-id"] = "forged-attempt"
        invalid_binding_status, _ = await gateway_request(
            sandbox.gateway_address, gateway_port, private_token, invalid_claims,
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": "invalid binding"}]},
        )
        before_no_forward = fake.count()

        uncertain_id = f"acc:{run_id}:t5-consumed-before-send"
        uncertain_intent = call_intent(context_t2, plan_t2, uncertain_id, "compaction", profile, 16)
        uncertain_permit = await authorize_provider_call(factory, uncertain_intent)
        await consume_call_permit(factory, context_t2["binding"], WORKER_ID, uncertain_permit.permit_id, uncertain_permit.accounting_call_id)
        uncertain_replay_status, _ = await gateway_request(
            sandbox.gateway_address, gateway_port, private_token,
            runtime_headers(context_t2["binding"], operation_t2, uncertain_id, "compaction", 16),
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": "uncertain consumed permit"}]},
        )

        revoked_id = f"acc:{run_id}:t5-consume-failure"
        revoked_intent = call_intent(context_t2, plan_t2, revoked_id, "compaction", profile, 16)
        revoked_permit = await authorize_provider_call(factory, revoked_intent)
        async with factory() as uow:
            await uow.session.execute(text("UPDATE call_permits SET state='REVOKED', revoked_at=now() WHERE permit_id=:permit_id"), {"permit_id": str(revoked_permit.permit_id)})
            await uow.commit()
        try:
            await consume_call_permit(factory, context_t2["binding"], WORKER_ID, revoked_permit.permit_id, revoked_permit.accounting_call_id)
            consume_failure_blocked = False
        except HekateError:
            consume_failure_blocked = True

        revision_race = await _gateway_task_state_race(
            factory, engine, fake, runtime, profile, sandbox.gateway_address,
            gateway_port, private_token, run_id, "revision",
        )
        cancellation_race = await _gateway_task_state_race(
            factory, engine, fake, runtime, profile, sandbox.gateway_address,
            gateway_port, private_token, run_id, "cancellation",
        )
        authorization_db_failure = await _gateway_storage_failure(
            factory, engine, fake, runtime, profile, sandbox.gateway_address,
            gateway_port, private_token, run_id, "authorize",
        )
        consumption_db_failure = await _gateway_storage_failure(
            factory, engine, fake, runtime, profile, sandbox.gateway_address,
            gateway_port, private_token, run_id, "consume",
        )
        permit_failure_forward_delta = fake.count() - before_no_forward
        no_forward_after_permit_failures = permit_failure_forward_delta == 0
        post_forward_db_failure = await _gateway_post_forward_record_failure(
            factory, engine, fake, runtime, profile, gateway_app,
            sandbox.gateway_address, gateway_port, private_token, run_id,
        )

        compaction_id = f"acc:{run_id}:t2-compaction"
        compaction_status, _ = await gateway_request(
            sandbox.gateway_address, gateway_port, private_token,
            runtime_headers(context_t2["binding"], operation_t2, compaction_id, "compaction", 16),
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": "compaction"}]},
        )
        fake.set_next("error")
        main_id = f"acc:{run_id}:t2-main"
        main_status, _ = await gateway_request(
            sandbox.gateway_address, gateway_port, private_token,
            runtime_headers(context_t2["binding"], operation_t2, main_id, "turn", 16),
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": "main turn"}]},
        )
        before_retries = fake.count()
        retry_status, _ = await gateway_request(
            sandbox.gateway_address, gateway_port, private_token,
            runtime_headers(context_t2["binding"], operation_t2, f"acc:{run_id}:t2-retry", "turn", 16),
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": "500 retry"}]},
        )
        extra_compaction_status, _ = await gateway_request(
            sandbox.gateway_address, gateway_port, private_token,
            runtime_headers(context_t2["binding"], operation_t2, f"acc:{run_id}:t2-extra-compaction", "compaction", 16),
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": "extra compaction"}]},
        )
        after_retries = fake.count()

        async with factory() as uow:
            await uow.session.execute(text("UPDATE outbox SET claim_expires_at=now()-interval '1 second' WHERE id=:id"), {"id": t2_job.id})
            await uow.commit()
        reclaimed_after_marker = await claim_one(factory, f"restart:{run_id}")
        try:
            async with factory() as uow:
                await uow.delivery.ack_job(t2_job, WORKER_ID, t2_job.claim_fence)
                await uow.commit()
            stale_ack_blocked = False
        except StaleInput:
            stale_ack_blocked = True

        t2_summary = await _db_summary(engine, operation_t2)
        t2_summary["call_plan"] = plan_t2.model_dump(mode="json")
        t2_summary["call_records"] = (await fetch_operation(engine, operation_t2))["rows"]
        t2_summary.update({
            "compaction_status": compaction_status,
            "main_turn_status": main_status,
            "unapproved_retry_status": retry_status,
            "extra_compaction_status": extra_compaction_status,
            "invalid_binding_status": invalid_binding_status,
            "same_consumed_id_replay_status": uncertain_replay_status,
            "permit_consume_failure_blocked": consume_failure_blocked,
            "permit_failure_forward_delta": permit_failure_forward_delta,
            "retry_provider_request_delta": after_retries - before_retries,
            "concurrent_claim_count": len(winners),
            "stale_claim_fence_blocked": stale_fence_blocked,
            "stale_ack_blocked": stale_ack_blocked,
            "expired_send_marker_reclaimed": reclaimed_after_marker is not None,
        })

        unauthorized_status, _ = await gateway_request(
            sandbox.gateway_address, gateway_port, "wrong-token",
            runtime_headers(context_t2["binding"], operation_t2, f"acc:{run_id}:unauthorized", "turn", 16),
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": "unauthorized"}]},
        )
        open_proxy_status, _ = await gateway_request(
            sandbox.gateway_address, gateway_port, private_token,
            runtime_headers(context_t2["binding"], operation_t2, f"acc:{run_id}:proxy", "turn", 16),
            {"model": FAKE_MODEL, "stream": False, "max_tokens": 16, "messages": [{"role": "user", "content": "proxy"}]},
            path="/v1/anything",
        )
        from dataclasses import replace
        try:
            replace(profile, upstream_base_url="https://provider.example/v1").validate(allow_test_profile=True)
            external_route_rejected = False
        except ValueError:
            external_route_rejected = True
        try:
            validate_settings(replace(settings, runtime_mode="production", models={}, pricing={}))
            production_settings_rejected = False
        except ValueError as error:
            production_settings_rejected = "production dispatch stays closed" in str(error)

        compaction_call = (await fetch_operation(engine, operation_t2))["rows"]
        settled_compaction = next((row for row in compaction_call if row["accounting_call_id"] == compaction_id), {})
        ledger_before_duplicate = await _ledger_settle_count(engine, compaction_id)
        runtime_usage = RuntimeInboxPayload(
            event_type="runtime_usage",
            operation_id=operation_t2,
            accounting_call_id=compaction_id,
            source="bridge_event",
            observation_identity=f"phase3-runtime-usage:{compaction_id}",
            binding=InboxBinding.from_binding(context_t2["binding"]),
            provider_call_id=str(settled_compaction.get("provider_call_id")) if settled_compaction.get("provider_call_id") else None,
            usage={
                "source": "runtime_reported",
                "completeness": "COMPLETE",
                "input_tokens": 37,
                "output_tokens": 4,
                "total_tokens": 41,
            },
        )
        inbox_key = f"runtime:{operation_t2}:{runtime_usage.observation_identity}"
        raw_runtime_usage = runtime_usage.model_dump(mode="json", exclude_none=True)
        inbox_hash = canonical_json_hash(raw_runtime_usage)
        async with factory() as uow:
            pending_receipt = await uow.delivery.insert_inbox_once("letta-bridge", inbox_key, raw_runtime_usage, inbox_hash)
            pending_duplicate_receipt = await uow.delivery.insert_inbox_once("letta-bridge", inbox_key, raw_runtime_usage, inbox_hash)
            await uow.commit()
        pending_inbox_processed_count = await process_pending_inbox(factory)
        recovered_pending = await process_runtime_observation(factory, "letta-bridge", inbox_key, runtime_usage)
        repeated_usage = await process_runtime_observation(factory, "letta-bridge", inbox_key, runtime_usage)
        ledger_after_duplicate = await _ledger_settle_count(engine, compaction_id)
        conflict_usage = runtime_usage.model_copy(update={
            "usage": runtime_usage.usage.model_copy(update={"input_tokens": 38}),
        })
        conflicting_inbox = await process_runtime_observation(factory, "letta-bridge", inbox_key, conflict_usage)
        ledger_after_conflict = await _ledger_settle_count(engine, compaction_id)
        conflict_projection = await _usage_projection(engine, compaction_id)

        t1_operation_id = str(context_t1["request"].envelope.operation_id)
        terminal_event = RuntimeInboxPayload(
            event_type="execution",
            operation_id=t1_operation_id,
            accounting_call_id=f"execution:{t1_operation_id}",
            source="bridge_terminal",
            observation_identity=f"execution:{t1_operation_id}:quiescent:SUCCEEDED",
            binding=InboxBinding.from_binding(context_t1["runtime_binding"]),
            state="QUIESCENT",
            outcome="SUCCEEDED",
            lease_owner=WORKER_ID,
            observer_fence=context_t1["runtime_binding"].fence,
        )
        terminal_ledger_before = await _ledger_settle_count(engine, t1_call_id)
        terminal_replay = await process_runtime_observation(
            factory, "letta-bridge", terminal_event.observation_identity, terminal_event,
        )
        conflicting_terminal = await process_runtime_observation(
            factory,
            "letta-bridge",
            terminal_event.observation_identity,
            terminal_event.model_copy(update={"outcome": "FAILED"}),
        )
        t1_after_terminal_replay = await _db_summary(engine, t1_operation_id)
        terminal_ledger_after = await _ledger_settle_count(engine, t1_call_id)

        task_state_after_cancel = await cancel(
            factory, actor_for(context_t2), context_t2["task_id"], StopReason.USER_CANCELLED,
        )
        async with engine.connect() as connection:
            late_call_count_before = await connection.scalar(text(
                "SELECT count(*) FROM provider_calls WHERE operation_id=:operation_id"
            ), {"operation_id": operation_t2})
            late_operation_count_before = await connection.scalar(text(
                "SELECT count(*) FROM operations WHERE task_id=:task_id"
            ), {"task_id": str(context_t2["task_id"])})
        late_usage = RuntimeInboxPayload(
            event_type="runtime_usage",
            operation_id=operation_t2,
            accounting_call_id=main_id,
            source="delayed_fake_meter",
            observation_identity=f"late-usage-after-cancel:{main_id}",
            binding=InboxBinding.from_binding(context_t2["binding"]),
            usage={
                "source": "provider_reported",
                "completeness": "COMPLETE",
                "input_tokens": 37,
                "output_tokens": 4,
                "total_tokens": 41,
            },
        )
        late_usage_result = await process_runtime_observation(
            factory,
            "fake-provider-delayed-meter",
            late_usage.observation_identity,
            late_usage,
        )
        late_rows = (await fetch_operation(engine, operation_t2))["rows"]
        late_call = next((row for row in late_rows if row.get("accounting_call_id") == main_id), {})
        late_settlement_entries = await _ledger_settle_count(engine, main_id)
        async with engine.connect() as connection:
            late_call_count_after = await connection.scalar(text(
                "SELECT count(*) FROM provider_calls WHERE operation_id=:operation_id"
            ), {"operation_id": operation_t2})
            late_operation_count_after = await connection.scalar(text(
                "SELECT count(*) FROM operations WHERE task_id=:task_id"
            ), {"task_id": str(context_t2["task_id"])})

        fake.set_next("block")
        plan_t4 = ProviderCallPlan(
            profile_id=profile.profile_id, model=FAKE_MODEL, pricing_version=profile.price_table.version,
            max_input_tokens=16_384, max_output_tokens=64, main_turn_calls=1, compaction_calls=0,
        )
        context_t4 = await seed_agent(factory, runtime, f"{run_id}:t4", WORKER_ID, call_plan=plan_t4, attempt_name="disconnect", operation_name="disconnect")
        t4_job = await claim_one(factory, WORKER_ID)
        if t4_job is None:
            raise RuntimeError("T4 admitted operation had no claimable outbox job")
        before_t4 = fake.count()
        t4_task = asyncio.create_task(dispatch_job(container, t4_job, WORKER_ID))
        if not await asyncio.to_thread(fake.request_seen.wait, 25):
            raise RuntimeError("T4 fake provider did not observe the physical request")
        p1.run(["docker", "kill", "--signal", "KILL", sandbox.container], timeout=20)
        fake.release_block.set()
        try:
            await asyncio.wait_for(t4_task, timeout=25)
        except TimeoutError:
            t4_task.cancel()
            await asyncio.gather(t4_task, return_exceptions=True)
        t4_summary = await _db_summary(engine, str(context_t4["request"].envelope.operation_id))
        t4_summary["call_plan"] = plan_t4.model_dump(mode="json")
        t4_summary["call_records"] = (await fetch_operation(engine, str(context_t4["request"].envelope.operation_id)))["rows"]
        t4_summary["fake_provider_requests"] = fake.count() - before_t4
        t4_operation_id = str(context_t4["request"].envelope.operation_id)
        terminal_only = RuntimeInboxPayload(
            event_type="execution",
            operation_id=t4_operation_id,
            accounting_call_id=f"execution:{t4_operation_id}",
            source="bridge_terminal",
            observation_identity=f"phase3-sdk-terminal-only:{t4_operation_id}",
            binding=InboxBinding.from_binding(context_t4["runtime_binding"]),
            state="QUIESCENT",
            outcome="SUCCEEDED",
            lease_owner=WORKER_ID,
            observer_fence=context_t4["runtime_binding"].fence,
        )
        try:
            terminal_result = await process_runtime_observation(
                factory, "letta-bridge", terminal_only.observation_identity, terminal_only,
            )
            t4_summary["sdk_terminal_without_provider_quiescence_blocked"] = (
                terminal_result.get("processed") is False
                and terminal_result.get("pending_reason") == "call_termination_unconfirmed"
            )
        except UnknownExecution:
            t4_summary["sdk_terminal_without_provider_quiescence_blocked"] = False
        t4_terminal_state = await _db_summary(engine, t4_operation_id)
        t4_summary["state_after_sdk_terminal_only"] = t4_terminal_state["operation"].get("execution_state")
        restarted_engine = create_engine(database_url)
        restarted_factory = create_uow_factory(restarted_engine)
        try:
            new_attempt_binding = context_t4["runtime_binding"].model_copy(update={
                "attempt_id": AttemptId(f"attempt:{run_id}:t4:restart"),
            })
            try:
                await prepare_runtime_session(restarted_factory, runtime, new_attempt_binding, WORKER_ID)
                t4_summary["session_reprepare_blocked_by_db_hold"] = False
            except UnknownExecution:
                t4_summary["session_reprepare_blocked_by_db_hold"] = True
            t4_summary["new_claim_after_unknown"] = await claim_one(
                restarted_factory, f"new-worker:{run_id}",
            ) is not None
        finally:
            await restarted_engine.dispose()

        async with engine.connect() as connection:
            binding_conflict_count = await connection.scalar(text("SELECT count(*) FROM inbox WHERE provider_scope='letta-bridge' AND stable_event_key=:key"), {"key": inbox_key})
            conflict_audit_count = await connection.scalar(text("SELECT count(*) FROM audit_events WHERE event_kind='inbox.payload_conflict'"))
        try:
            await process_pending_inbox(factory)
            pending_inbox_reprocess = True
        except Exception:
            pending_inbox_reprocess = False

        report["results"]["T2_roles_retry_and_compaction_plan"] = {
            **t2_summary,
            "compaction_and_main_have_distinct_accounting_ids": len({compaction_id, main_id}) == 2,
            "unapproved_500_retry_blocked": retry_status == 402 and after_retries == before_retries,
            "compaction_limit_enforced": extra_compaction_status == 402 and after_retries == before_retries,
            "passed": (
                compaction_status == 200 and main_status == 500 and retry_status == 402
                and extra_compaction_status == 402 and t2_summary["consumed_permit_count"] == 3
            ),
        }
        report["results"]["T3_outbox_claim_fence_and_send_marker"] = {
            "concurrent_claim_count": len(winners),
            "pre_send_claim_expiry_reclaimed": t2_job.claim_fence > stale_job.claim_fence,
            "stale_claim_fence_blocked": stale_fence_blocked,
            "stale_ack_blocked": stale_ack_blocked,
            "expired_send_marker_reclaimed": reclaimed_after_marker is not None,
            "passed": len(winners) == 1 and t2_job.claim_fence > stale_job.claim_fence
            and stale_fence_blocked and stale_ack_blocked and reclaimed_after_marker is None,
        }
        report["results"]["T4_bridge_loss_keeps_unknown_hold"] = {
            **t4_summary,
            "passed": t4_summary["fake_provider_requests"] == 1
            and t4_summary["operation"].get("execution_state") == "UNKNOWN"
            and t4_summary["operation"].get("hold_state") == "UNKNOWN"
            and t4_summary["sdk_terminal_without_provider_quiescence_blocked"]
            and t4_summary["state_after_sdk_terminal_only"] == "UNKNOWN"
            and t4_summary["session_reprepare_blocked_by_db_hold"]
            and not t4_summary["new_claim_after_unknown"],
        }
        report["results"]["T5_db_and_permit_failures_do_not_forward"] = {
            "invalid_binding_status": invalid_binding_status,
            "same_consumed_id_replay_status": uncertain_replay_status,
            "revoked_permit_consume_blocked": consume_failure_blocked,
            "provider_forward_delta_for_failures": permit_failure_forward_delta,
            "authorization_then_revision_race": revision_race,
            "authorization_then_cancellation_race": cancellation_race,
            "authorize_storage_failure": authorization_db_failure,
            "consume_storage_failure": consumption_db_failure,
            "post_forward_observation_storage_failure": post_forward_db_failure,
            "passed": invalid_binding_status == 402 and uncertain_replay_status == 402
            and consume_failure_blocked and no_forward_after_permit_failures
            and revision_race["task_mutation_result"] == 2 and revision_race["blocked_before_forward"]
            and cancellation_race["task_mutation_result"] == "STOPPING" and cancellation_race["blocked_before_forward"]
            and authorization_db_failure["blocked_before_forward"]
            and consumption_db_failure["blocked_before_forward"]
            and post_forward_db_failure["blocked_after_write_failure"],
        }
        report["results"]["T6_inbox_replay_and_conflict"] = {
            "new_pending_row_inserted": not pending_receipt.duplicate,
            "unprocessed_insert_duplicate": pending_duplicate_receipt.duplicate,
            "unprocessed_row_applied": pending_inbox_processed_count == 1 and recovered_pending["processed"],
            "restart_pending_inbox_processed_count": pending_inbox_processed_count,
            "processed_duplicate_is_noop": repeated_usage["duplicate"] and ledger_after_duplicate == ledger_before_duplicate,
            "conflicting_payload_preserved": conflicting_inbox["conflict"] and binding_conflict_count >= 2 and conflict_audit_count >= 1,
            "conflict_projection_state": conflict_projection.get("settlement_state"),
            "late_conflict_did_not_reverse_spend": ledger_after_conflict == ledger_before_duplicate,
            "terminal_replay_is_noop": terminal_replay["duplicate"] and terminal_ledger_after == terminal_ledger_before,
            "conflicting_terminal_outcome_preserved": conflicting_terminal["conflict"]
            and t1_after_terminal_replay["operation"].get("state") == "COMPLETED",
            "late_usage_after_cancel_settled_original_call": task_state_after_cancel["state"] == "STOPPING"
            and late_usage_result["processed"] and late_call.get("provider_call_id") is None
            and late_call.get("settlement_state") == "SETTLED" and late_settlement_entries == 2
            and late_call_count_after == late_call_count_before
            and late_operation_count_after == late_operation_count_before,
            "late_usage_task_state": task_state_after_cancel["state"],
            "late_usage_accounting_call_id": main_id,
            "late_usage_call_state": late_call.get("call_status"),
            "late_usage_settlement_state": late_call.get("settlement_state"),
            "late_usage_settlement_ledger_entries": late_settlement_entries,
            "restart_pending_inbox_scan_ran": pending_inbox_processed_count == 1 and pending_inbox_reprocess,
            "passed": pending_duplicate_receipt.duplicate and pending_inbox_processed_count == 1
            and recovered_pending["processed"] and repeated_usage["duplicate"]
            and conflicting_inbox["conflict"] and ledger_after_conflict == ledger_before_duplicate
            and terminal_replay["duplicate"] and terminal_ledger_after == terminal_ledger_before
            and conflicting_terminal["conflict"] and t1_after_terminal_replay["operation"].get("state") == "COMPLETED"
            and task_state_after_cancel["state"] == "STOPPING" and late_usage_result["processed"]
            and late_call.get("settlement_state") == "SETTLED" and late_settlement_entries == 2
            and late_call_count_after == late_call_count_before
            and late_operation_count_after == late_operation_count_before,
        }
        report["results"]["T7_private_gateway_fail_closed"] = {
            "invalid_bearer_status": unauthorized_status,
            "open_proxy_status": open_proxy_status,
            "external_test_profile_rejected": external_route_rejected,
            "production_settings_rejected_without_profile": production_settings_rejected,
            "real_provider_calls": 0,
            "passed": unauthorized_status == 401 and open_proxy_status == 404
            and external_route_rejected and production_settings_rejected,
        }
        t1_ok = report["results"]["T1_actual_db_runtime_gateway_fake_provider"]["passed"]
        replay_ok = report["results"]["T1_same_accounting_id_replay"]["blocked_without_forward"]
        phases = [report["results"][key]["passed"] for key in (
            "T2_actual_runtime_compaction", "T2_roles_retry_and_compaction_plan", "T3_outbox_claim_fence_and_send_marker",
            "T4_bridge_loss_keeps_unknown_hold", "T5_db_and_permit_failures_do_not_forward",
            "T6_inbox_replay_and_conflict", "T7_private_gateway_fail_closed",
        )]
        report["overall_status"] = "pass" if t1_ok and replay_ok and all(phases) else "blocked"
    except Exception as error:
        report["failure"] = {"type": type(error).__name__, "message": str(error)[:500]}
        if error.__cause__ is not None:
            cause = error.__cause__
            report["failure"]["cause"] = {"type": type(cause).__name__, "message": str(cause)[:500]}
        if sandbox is not None and sandbox.container_created:
            logs = subprocess.run(
                ["docker", "logs", "--tail", "30", sandbox.container],
                capture_output=True, text=True, check=False, timeout=10,
            )
            log_tail = (logs.stdout + logs.stderr)[-2_000:]
            if private_token:
                log_tail = log_tail.replace(private_token, "[redacted]")
            report["failure"]["app_server_log_tail"] = log_tail
        report["overall_status"] = "blocked"
    finally:
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
            except Exception as cleanup_error:
                report.setdefault("cleanup_errors", []).append(type(cleanup_error).__name__)
        if engine is not None:
            try:
                async with engine.connect() as connection:
                    call_rows = (await connection.execute(text("""
                        SELECT p.accounting_call_id, p.operation_id, p.call_kind, p.status AS call_state,
                               cp.permit_id, cp.state AS permit_state, p.provider_call_id,
                               u.completeness, u.settlement_state
                        FROM provider_calls p
                        LEFT JOIN call_permits cp USING (accounting_call_id)
                        LEFT JOIN usage_projections u USING (accounting_call_id)
                        ORDER BY p.operation_id, p.accounting_call_id
                    """))).mappings().all()
                    account_rows = (await connection.execute(text("""
                        SELECT id, scope_kind, limit_amount, spent_amount, held_amount
                        FROM budget_accounts ORDER BY scope_kind, id
                    """))).mappings().all()
                    ledger_rows = (await connection.execute(text("""
                        SELECT effect_type, count(*) AS count
                        FROM budget_ledger GROUP BY effect_type ORDER BY effect_type
                    """))).mappings().all()
                    pending_calls = await connection.scalar(text("""
                        SELECT count(*) FROM provider_calls p
                        LEFT JOIN usage_projections u USING (accounting_call_id)
                        WHERE p.status != 'QUIESCENT'
                           OR coalesce(u.settlement_state, 'PENDING') != 'SETTLED'
                    """))
                    inbox_counts = (await connection.execute(text("""
                        SELECT count(*) AS rows,
                               count(*) FILTER (WHERE processed_at IS NOT NULL) AS processed,
                               count(*) FILTER (WHERE processed_at IS NULL) AS pending
                        FROM inbox
                    """))).mappings().one()
                fake_requests = list(fake.requests) if fake is not None else []
                fake_response_ids = [item["response_id"] for item in fake_requests if item.get("response_id")]
                pending_details = []
                for row in call_rows:
                    if row["call_state"] == "QUIESCENT" and row["settlement_state"] == "SETTLED":
                        continue
                    if row["permit_state"] == "REVOKED":
                        reason = "permit_revoked_before_forward"
                    elif row["permit_state"] == "ISSUED" and row["call_state"] == "ALLOCATED":
                        reason = "permit_not_consumed_proven_no_forward"
                    elif row["call_state"] != "QUIESCENT":
                        reason = "provider_call_not_confirmed_quiescent"
                    elif row["settlement_state"] == "CONFLICT":
                        reason = "usage_conflict_requires_reconciliation"
                    elif row["provider_call_id"] is None:
                        reason = "provider_response_or_usage_unobserved"
                    else:
                        reason = "usage_not_settled"
                    pending_details.append({
                        "accounting_call_id": row["accounting_call_id"],
                        "operation_id": row["operation_id"],
                        "call_state": row["call_state"],
                        "permit_state": row["permit_state"],
                        "settlement_state": row["settlement_state"],
                        "reason": reason,
                    })
                report["observed_totals"] = {
                    "provider_gateway_requests": gateway_counts,
                    "provider_gateway_metrics": dict(gateway_app.state.metrics) if gateway_app is not None else {},
                    "fake_provider_forward_requests": len(fake_requests),
                    "fake_provider_responses": fake_response_ids,
                    "database_provider_calls": [dict(row) for row in call_rows],
                    "database_call_count": len(call_rows),
                    "database_permit_count": sum(row["permit_id"] is not None for row in call_rows),
                    "database_consumed_permit_count": sum(row["permit_state"] == "CONSUMED" for row in call_rows),
                    "calls_pending_execution_or_settlement": int(pending_calls or 0),
                    "pending_call_reasons": pending_details,
                    "budget_accounts": [dict(row) for row in account_rows],
                    "ledger_effect_counts": [dict(row) for row in ledger_rows],
                    "inbox_counts": dict(inbox_counts),
                    "real_provider_calls": 0,
                }
            except Exception as metrics_error:
                report["measurement_error"] = type(metrics_error).__name__
        scenario_checks = {
            name: result["passed"]
            for name, result in report["results"].items()
            if isinstance(result, dict) and isinstance(result.get("passed"), bool)
        }
        report["verification"] = {
            "command": "uv run python scripts/phase3_runtime_probe.py",
            "scope": "PostgreSQL + pinned Letta App Server + Python bridge + DB gateway + isolated fake provider",
            "scenario_count": len(scenario_checks),
            "passed_count": sum(scenario_checks.values()),
            "failed_count": sum(not passed for passed in scenario_checks.values()),
            "scenario_results": scenario_checks,
        }
        if engine is not None:
            await engine.dispose()
        temporary.cleanup()
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        report["executed_at_finished"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        report["artifact"] = artifact_path.relative_to(ROOT).as_posix()
        artifact_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return report


async def _ledger_settle_count(engine, accounting_call_id: str) -> int:
    async with engine.connect() as connection:
        return int(await connection.scalar(text("SELECT count(*) FROM budget_ledger WHERE accounting_call_id=:id AND effect_type='SETTLE'"), {"id": accounting_call_id}))


async def _usage_projection(engine, accounting_call_id: str) -> dict[str, object]:
    async with engine.connect() as connection:
        row = (await connection.execute(text("SELECT completeness, settlement_state, has_conflict FROM usage_projections WHERE accounting_call_id=:id"), {"id": accounting_call_id})).mappings().one_or_none()
        return dict(row) if row else {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.environ.get("HEKATE_TEST_DATABASE_URL", ""))
    parser.add_argument("--node-bin", default=os.environ.get("HEKATE_NODE_BIN", ""))
    parser.add_argument("--node-archive", type=Path, default=Path(os.environ.get("HEKATE_NODE_ARCHIVE", "/tmp/hekate-node-v22.19.0-linux-x64.tar.xz")))
    parser.add_argument("--image", default=f"hekate/letta-code-p1:{p1.LOCK['app_server']['source_commit'][:8]}-{p1.LOCK['patches'][0]['sha256'][:8]}")
    parser.add_argument("--artifact", type=Path)
    args = parser.parse_args()
    if not args.database_url or not args.node_bin:
        parser.error("set --database-url/HEKATE_TEST_DATABASE_URL and --node-bin/HEKATE_NODE_BIN")
    run_id = f"p3-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    artifact = args.artifact or ROOT / "integration/runtime/artifacts" / f"{run_id}.json"
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", args.database_url.replace("%", "%%"))
    os.environ["HEKATE_DATABASE_URL"] = args.database_url
    command.upgrade(cfg, "head")
    report = asyncio.run(_run_probe(args.database_url, args.node_bin, args.image, args.node_archive, artifact, run_id))
    print(json.dumps({"artifact": str(artifact.relative_to(ROOT)), "status": report["overall_status"], "real_provider_calls": report["real_provider_calls"]}, sort_keys=True))
    return 0 if report["overall_status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
