from __future__ import annotations

import argparse
import hashlib
import json
import os
import selectors
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from hekate.domain.bridge_contracts import BRIDGE_COMMAND_ADAPTER, BRIDGE_REPLY_ADAPTER
from hekate.domain.contracts import MAX_CONTRACT_BYTES, check_json_payload


ROOT = Path(__file__).resolve().parents[1]
LOCK_PATH = ROOT / "integration/letta/versions.lock.json"
LOCK = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
FRAME_LIMIT = MAX_CONTRACT_BYTES
DEFAULT_IMAGE = (
    "ghcr.io/letta-ai/letta-code@sha256:"
    "dd72786542f76ada2df2634ac0e31b5866eba71f9b578058dbf3ebbe9d46aa60"
)
FAKE_MODEL = "hekate-fake-model"
FAKE_USAGE = {"prompt_tokens": 37, "completion_tokens": 2, "total_tokens": 39}
APP_PORT = 4500


class ProbeError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def run(command: list[str], *, timeout: int = 60, cwd: Path = ROOT) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode:
        detail = (completed.stderr or completed.stdout).strip()[-1_500:]
        raise ProbeError(f"{Path(command[0]).name} failed ({completed.returncode}): {detail}")
    return completed.stdout.strip()


class PermitLedger:
    """Probe-only permits keyed by operation and checked against provider-call metadata."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.pending: dict[str, list[dict[str, Any]]] = {}
        self.consumed_call_ids: set[str] = set()
        self.issued = 0
        self.consumed = 0
        self.denied = 0
        self.checks: list[dict[str, Any]] = []
        self.available = True

    def issue(
        self,
        operation_id: str,
        binding: dict[str, Any],
        *,
        model: str = FAKE_MODEL,
        max_output_tokens: int | None = 64,
        call_kind: str = "turn",
        ttl_seconds: float = 120,
    ) -> None:
        with self.lock:
            self.issued += 1
            permit = {
                "permit_id": f"probe-permit-{self.issued}",
                "operation_id": operation_id,
                "binding": dict(binding),
                "model": model,
                "max_output_tokens": max_output_tokens,
                "call_kind": call_kind,
                "expires_at": time.time() + ttl_seconds,
            }
            self.pending.setdefault(operation_id, []).append(permit)

    def consume(
        self,
        context: dict[str, Any],
        request: dict[str, Any],
    ) -> dict[str, Any] | None:
        with self.lock:
            operation_id = context.get("operation_id")
            call_id = context.get("accounting_call_id")
            permits = self.pending.get(operation_id, []) if isinstance(operation_id, str) else []
            reason = None
            permit = None
            if not self.available:
                reason = "permit_provider_unavailable"
            elif not isinstance(call_id, str) or not call_id:
                reason = "missing_accounting_call_id"
            elif call_id in self.consumed_call_ids:
                reason = "accounting_call_id_reused"
            elif not permits:
                reason = "no_permit_for_operation"
            else:
                binding_keys = (
                    "task_id", "attempt_id", "agent_registry_id", "provider_agent_id",
                    "input_revision", "fence", "conversation_id",
                )
                candidates = [p for p in permits if all(
                    context.get(key) == p["binding"].get(key) for key in binding_keys
                )]
                if not candidates:
                    reason = "binding_mismatch"
                else:
                    candidates = [p for p in candidates if context.get("call_kind") == p["call_kind"]]
                    if not candidates:
                        reason = "call_kind_mismatch"
                    else:
                        candidates = [p for p in candidates if
                                      context.get("model") == p["model"] and request.get("model") == p["model"]]
                        if not candidates:
                            reason = "model_mismatch"
                        else:
                            body_limit = request.get("max_tokens")
                            if body_limit is None:
                                body_limit = request.get("max_completion_tokens")
                            # The permit is a ceiling; the runtime may request fewer output tokens.
                            candidates = [p for p in candidates if
                                          isinstance(body_limit, int) and not isinstance(body_limit, bool) and
                                          0 < body_limit <= p["max_output_tokens"] and
                                          context.get("max_output_tokens") == p["max_output_tokens"]]
                            if not candidates:
                                reason = "output_limit_mismatch"
                            else:
                                candidates = [p for p in candidates if p["expires_at"] > time.time()]
                                if not candidates:
                                    reason = "permit_expired"
                                else:
                                    permit = candidates[0]

            if reason is not None:
                safe_binding = {key: context.get(key) for key in (
                    "task_id", "attempt_id", "agent_registry_id", "provider_agent_id",
                    "input_revision", "fence", "conversation_id",
                )}
                request_limit = request.get("max_tokens")
                if request_limit is None:
                    request_limit = request.get("max_completion_tokens")
                self.denied += 1
                self.checks.append({
                    "authorized": False,
                    "operation_id": operation_id if isinstance(operation_id, str) else None,
                    "accounting_call_id": call_id if isinstance(call_id, str) else None,
                    "binding": safe_binding,
                    "call_kind": context.get("call_kind"),
                    "model": context.get("model"),
                    "request_model": request.get("model"),
                    "context_max_output_tokens": context.get("max_output_tokens"),
                    "request_max_output_tokens": request_limit,
                    "reason": reason,
                })
                return None

            assert permit is not None and isinstance(operation_id, str) and isinstance(call_id, str)
            permit_index = permits.index(permit)
            permits.pop(permit_index)
            if not permits:
                self.pending.pop(operation_id, None)
            self.consumed_call_ids.add(call_id)
            self.consumed += 1
            self.checks.append({
                "authorized": True,
                "operation_id": permit["operation_id"],
                "permit_id": permit["permit_id"],
                "accounting_call_id": call_id,
                "binding": {key: context.get(key) for key in (
                    "task_id", "attempt_id", "agent_registry_id", "provider_agent_id",
                    "input_revision", "fence", "conversation_id",
                )},
                "call_kind": permit["call_kind"],
                "model": permit["model"],
                "max_output_tokens": permit["max_output_tokens"],
                "request_max_output_tokens": body_limit,
            })
            return permit

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "permits_issued": self.issued,
                "permits_consumed": self.consumed,
                "gateway_attempts": len(self.checks),
                "denied_attempts": self.denied,
                "checks": list(self.checks),
                "pending_operation_ids": sorted(self.pending),
            }


class FakeProvider:
    def __init__(self, gateway_address: str) -> None:
        self.ledger = PermitLedger()
        self.lock = threading.Lock()
        self.behaviors: deque[str] = deque()
        self.model_requests: list[dict[str, Any]] = []
        self.disconnect_request_seen = threading.Event()
        self.disconnect_release = threading.Event()
        self.model_discoveries = 0
        owner = self

        class BackendHandler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: Any) -> None:
                return

            def respond(self, status: int, value: dict[str, Any]) -> None:
                body = json.dumps(value, separators=(",", ":")).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def respond_stream(self, chunks: list[dict[str, Any]]) -> None:
                body = b"".join(
                    b"data: " + json.dumps(chunk, separators=(",", ":")).encode() + b"\n\n"
                    for chunk in chunks
                ) + b"data: [DONE]\n\n"
                self.send_response(200)
                self.send_header("content-type", "text/event-stream")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:
                if self.path.rstrip("/") != "/v1/models":
                    self.respond(404, {"error": "not found"})
                    return
                with owner.lock:
                    owner.model_discoveries += 1
                self.respond(200, {
                    "object": "list",
                    "data": [{
                        "id": FAKE_MODEL,
                        "object": "model",
                        "created": 0,
                        "owned_by": "hekate-phase1-probe",
                    }],
                })

            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers.get("content-length", "0")))
                try:
                    request = json.loads(body)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    self.respond(400, {"error": "invalid JSON"})
                    return
                with owner.lock:
                    model_request = {
                        "sequence": len(owner.model_requests) + 1,
                        "model": request.get("model"),
                        "max_tokens": request.get("max_tokens"),
                        "max_completion_tokens": request.get("max_completion_tokens"),
                        "stream": request.get("stream"),
                        "accounting_call_id": self.headers.get("x-hekate-accounting-call-id"),
                        "request_keys": sorted(request),
                        "request_header_names": sorted(name.lower() for name in self.headers.keys()),
                        "tool_names": [
                            tool.get("function", {}).get("name", "")
                            for tool in request.get("tools", [])
                            if isinstance(tool, dict)
                        ],
                    }
                    owner.model_requests.append(model_request)
                    behavior = owner.behaviors.popleft() if owner.behaviors else "normal"
                    model_request["behavior"] = behavior
                if behavior == "disconnect":
                    owner.disconnect_request_seen.set()
                    if not owner.disconnect_release.wait(30):
                        self.respond(504, {"error": {"message": "disconnect probe timed out", "type": "server_error"}})
                        return
                    self.respond(500, {"error": {"message": "connection intentionally interrupted", "type": "server_error"}})
                    return
                if behavior == "error":
                    self.respond(500, {"error": {"message": "probe provider error", "type": "server_error"}})
                    return
                call_id = f"fake-call-{model_request['sequence']:04d}"
                with owner.lock:
                    model_request["provider_response_id"] = call_id
                usage = dict(FAKE_USAGE)
                with owner.lock:
                    model_request["fake_usage"] = usage
                if behavior == "tool_call":
                    delta = {"role": "assistant", "tool_calls": [{
                        "index": 0,
                        "id": "fake-tool-call-1",
                        "type": "function",
                        "function": {
                            "name": "hekate_forbidden_probe",
                            "arguments": "{}",
                        },
                    }]}
                    finish_reason = "tool_calls"
                elif behavior == "empty":
                    delta = {"role": "assistant"}
                    finish_reason = "stop"
                elif behavior == "invalid_schema":
                    delta = {"role": "assistant", "content": "not json"}
                    finish_reason = "stop"
                else:
                    delta = {"role": "assistant", "content": "P1 OK"}
                    finish_reason = "stop"
                if request.get("stream") is True:
                    self.respond_stream([
                        {
                            "id": call_id,
                            "object": "chat.completion.chunk",
                            "created": int(time.time()),
                            "model": request.get("model", FAKE_MODEL),
                            "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
                        },
                        {
                            "id": call_id,
                            "object": "chat.completion.chunk",
                            "created": int(time.time()),
                            "model": request.get("model", FAKE_MODEL),
                            "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
                        },
                        {
                            "id": call_id,
                            "object": "chat.completion.chunk",
                            "created": int(time.time()),
                            "model": request.get("model", FAKE_MODEL),
                            "choices": [],
                            "usage": usage,
                        },
                    ])
                    return
                self.respond(200, {
                    "id": call_id,
                    "object": "chat.completion",
                    "created": int(time.time()),
                    "model": request.get("model", FAKE_MODEL),
                    "choices": [{
                        "index": 0,
                        "message": {**delta, "role": "assistant"},
                        "finish_reason": finish_reason,
                    }],
                    "usage": usage,
                })

        class GatewayHandler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: Any) -> None:
                return

            def respond(self, status: int, body: bytes, content_type: str = "application/json") -> None:
                self.send_response(status)
                self.send_header("content-type", content_type)
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def forward(
                self,
                method: str,
                body: bytes = b"",
                accounting_call_id: str | None = None,
            ) -> None:
                target = f"http://127.0.0.1:{owner.backend.server_port}{self.path}"
                request = Request(
                    target,
                    data=body if method != "GET" else None,
                    method=method,
                    headers={
                        "content-type": self.headers.get("content-type", "application/json"),
                        **({"x-hekate-accounting-call-id": accounting_call_id} if accounting_call_id else {}),
                    },
                )
                try:
                    with urlopen(request, timeout=30) as response:
                        self.respond(response.status, response.read(), response.headers.get("content-type", "application/json"))
                except HTTPError as error:
                    self.respond(error.code, error.read())
                except (TimeoutError, URLError, OSError):
                    self.respond(502, b'{"error":"fake backend unavailable"}')

            def do_GET(self) -> None:
                if self.path.rstrip("/") == "/v1/models":
                    self.forward("GET")
                else:
                    self.respond(404, b'{"error":"not found"}')

            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers.get("content-length", "0")))
                try:
                    request = json.loads(body)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    self.respond(400, b'{"error":"invalid JSON"}')
                    return
                def header(name: str) -> str | None:
                    value = self.headers.get(name)
                    return value if value else None
                def int_header(name: str, default: int | None = None) -> int | None:
                    value = header(name)
                    try:
                        return int(value) if value is not None else default
                    except ValueError:
                        return default
                context = {
                    "task_id": header("x-hekate-task-id"),
                    "attempt_id": header("x-hekate-attempt-id"),
                    "operation_id": header("x-hekate-operation-id"),
                    "agent_registry_id": header("x-hekate-agent-registry-id"),
                    "provider_agent_id": header("x-hekate-provider-agent-id"),
                    "input_revision": int_header("x-hekate-input-revision", -1),
                    "fence": int_header("x-hekate-fence", -1),
                    "conversation_id": header("x-hekate-conversation-id"),
                    "accounting_call_id": header("x-hekate-accounting-call-id"),
                    "call_kind": header("x-hekate-call-kind"),
                    "model": header("x-hekate-model"),
                    "max_output_tokens": int_header("x-hekate-max-output-tokens"),
                }
                permit = owner.ledger.consume(context, request)
                if permit is None:
                    self.respond(402, b'{"error":{"message":"no matching unused probe permit","type":"hekate_permit_denied"}}')
                    return
                self.forward("POST", body, context["accounting_call_id"])

        self.backend = ThreadingHTTPServer(("127.0.0.1", 0), BackendHandler)
        self.backend.daemon_threads = True
        self.gateway = ThreadingHTTPServer((gateway_address, 0), GatewayHandler)
        self.gateway.daemon_threads = True
        self.backend_thread = threading.Thread(target=self.backend.serve_forever, daemon=True)
        self.gateway_thread = threading.Thread(target=self.gateway.serve_forever, daemon=True)

    def start(self) -> None:
        self.backend_thread.start()
        self.gateway_thread.start()

    def set_next_behavior(self, behavior: str) -> None:
        with self.lock:
            if behavior == "disconnect":
                self.disconnect_request_seen.clear()
                self.disconnect_release.clear()
            self.behaviors.append(behavior)

    def wait_for_disconnect_request(self, timeout: float = 20) -> bool:
        return self.disconnect_request_seen.wait(timeout)

    def release_disconnect(self) -> None:
        self.disconnect_release.set()

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            requests = list(self.model_requests)
            discoveries = self.model_discoveries
        return {
            **self.ledger.snapshot(),
            "provider_request_count": len(requests),
            "model_discovery_count": discoveries,
            "model_requests": requests,
        }

    def stop(self) -> None:
        self.gateway.shutdown()
        self.backend.shutdown()
        self.gateway.server_close()
        self.backend.server_close()
        self.gateway_thread.join(timeout=2)
        self.backend_thread.join(timeout=2)


class DockerSandbox:
    def __init__(self, workdir: Path, run_id: str, image: str) -> None:
        suffix = run_id[-8:]
        self.network = f"hekate-p1-{suffix}"
        self.container = f"hekate-p1-{suffix}"
        self.image = image
        self.state = workdir / "letta-home"
        self.token_path = workdir / "app-server-token"
        self.gateway_address = ""
        self.port = 0
        self.network_created = False
        self.container_created = False

    def disconnect_app_server(self) -> None:
        run(["docker", "kill", "--signal", "KILL", self.container], timeout=20)

    def start_network(self) -> None:
        run(["docker", "network", "create", "--internal", "--driver", "bridge", self.network])
        self.network_created = True
        self.gateway_address = run([
            "docker", "network", "inspect",
            "--format", "{{(index .IPAM.Config 0).Gateway}}",
            self.network,
        ])
        if not self.gateway_address:
            raise ProbeError("isolated Docker network has no gateway address")

    def start_app_server(self, provider_port: int) -> None:
        storage = self.state / "lc-local-backend" / "providers"
        storage.mkdir(parents=True, exist_ok=True)
        timestamp = utc_now()
        (storage / "auth.json").write_text(json.dumps({
            "version": 1,
            "providers": {
                "openai-compatible": {
                    "id": "local-provider-openai-compatible",
                    "name": "openai-compatible",
                    "provider_type": "openai-compatible",
                    "provider_category": "byok",
                    "auth": {"type": "api", "key": "not-needed"},
                    "base_url": f"http://hekate-fake-provider:{provider_port}/v1",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            },
        }, separators=(",", ":")), encoding="utf-8")
        self.token_path.write_text(uuid.uuid4().hex + uuid.uuid4().hex, encoding="utf-8")
        self.token_path.chmod(0o600)
        self.port = APP_PORT
        run([
            "docker", "run", "--detach", "--name", self.container,
            "--network", self.network,
            "--env", "HEKATE_REQUIRE_PROVIDER_BINDING=1",
            "--add-host", f"hekate-fake-provider:{self.gateway_address}",
            "--mount", f"type=bind,source={self.state},target=/root/.letta",
            "--mount", f"type=bind,source={self.token_path},target=/run/secrets/hekate-ws-token,readonly",
            self.image,
            "letta", "--backend", "local",
            "server", "--listen", f"ws://0.0.0.0:{APP_PORT}",
            "--ws-auth", "capability-token",
            "--ws-token-file", "/run/secrets/hekate-ws-token",
        ], timeout=120)
        self.container_created = True
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            logs = subprocess.run(
                ["docker", "logs", "--tail", "40", self.container],
                capture_output=True,
                text=True,
                check=False,
                timeout=15,
            )
            output = logs.stdout + logs.stderr
            if "Listening on ws://" in output:
                return
            running = run([
                "docker", "inspect", "--format", "{{.State.Running}}", self.container,
            ], timeout=10)
            if running != "true":
                raise ProbeError(f"App Server exited during startup: {output[-1_500:]}")
            time.sleep(1)
        logs = subprocess.run(
            ["docker", "logs", "--tail", "40", self.container],
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
        output = logs.stdout + logs.stderr
        raise ProbeError(f"App Server did not listen on its private test port: {output[-1_500:]}")

    def stop(self) -> None:
        if self.container_created:
            subprocess.run(["docker", "rm", "--force", self.container], capture_output=True, check=False)
            self.container_created = False
        if self.network_created:
            subprocess.run(["docker", "network", "rm", self.network], capture_output=True, check=False)
            self.network_created = False

    def clear_state(self) -> None:
        if not self.state.exists():
            return
        cleanup = (
            'const fs = require("node:fs"); '
            'for (const name of fs.readdirSync("/cleanup")) '
            'fs.rmSync("/cleanup/" + name, {recursive:true, force:true})'
        )
        run([
            "docker", "run", "--rm", "--network", "none",
            "--mount", f"type=bind,source={self.state},target=/cleanup",
            "--entrypoint", "node", self.image, "-e", cleanup,
        ], timeout=60)


class BridgeProcess:
    def __init__(
        self,
        image: str,
        network: str,
        app_server: str,
        token: str,
        *,
        structured_output_probe: bool = False,
    ) -> None:
        self.stderr = tempfile.TemporaryFile()
        bridge_env = os.environ.copy()
        bridge_env["HEKATE_LETTA_TOKEN"] = token
        self.process = subprocess.Popen(
            [
                "docker", "run", "--rm", "--interactive",
                "--network", network,
                "--mount", f"type=bind,source={ROOT},target=/workspace,readonly",
                "--workdir", "/workspace",
                "--env", f"HEKATE_LETTA_URL=ws://{app_server}:{APP_PORT}",
                "--env", "HEKATE_LETTA_TOKEN",
                "--env", "HEKATE_REQUIRE_PROVIDER_BINDING=1",
                *( ["--env", "HEKATE_STRUCTURED_OUTPUT_PROBE=1"] if structured_output_probe else [] ),
                "--entrypoint", "node",
                image,
                "/workspace/bridge/letta/dist/main.js",
            ],
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr,
            bufsize=0,
            env=bridge_env,
        )
        assert self.process.stdin is not None and self.process.stdout is not None
        self.stdin = self.process.stdin
        self.stdout = self.process.stdout
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.stdout, selectors.EVENT_READ)
        self.pending = bytearray()

    def request(self, command: dict[str, Any], timeout: int = 60) -> dict[str, Any]:
        BRIDGE_COMMAND_ADAPTER.validate_python(command, strict=True)
        assert self.process.poll() is None
        payload = json.dumps(command, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
        if len(payload) > FRAME_LIMIT:
            raise ProbeError("probe generated an oversized command")
        self.stdin.write(payload)
        self.stdin.flush()
        deadline = time.monotonic() + timeout
        while True:
            newline = self.pending.find(b"\n")
            if newline >= 0:
                raw = bytes(self.pending[:newline])
                del self.pending[:newline + 1]
                check_json_payload(raw)
                response = BRIDGE_REPLY_ADAPTER.validate_json(raw, strict=True)
                if response.request_id != command["request_id"]:
                    raise ProbeError("bridge response request_id does not match request")
                if response.operation_id != command["operation_id"]:
                    raise ProbeError("bridge response operation_id does not match request")
                return response.model_dump(mode="json")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProbeError(f"bridge request timed out: {command['command']}")
            if not self.selector.select(remaining):
                raise ProbeError(f"bridge request timed out: {command['command']}")
            chunk = os.read(self.stdout.fileno(), 65_536)
            if not chunk:
                raise ProbeError(f"bridge exited ({self.process.poll()})")
            self.pending.extend(chunk)
            if len(self.pending) > FRAME_LIMIT:
                raise ProbeError("bridge response exceeds one MiB")

    def close(self) -> None:
        if self.process.poll() is None:
            self.stdin.close()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.selector.close()
        self.stdout.close()
        self.stderr.close()


def require_confirmed(response: dict[str, Any], label: str) -> dict[str, Any]:
    if response["status"] != "CONFIRMED":
        raise ProbeError(f"{label} returned {response['status']}: {response.get('error', 'no detail')}")
    return response.get("result") or {}


def operation_id(run_id: str, name: str) -> str:
    return f"{run_id}:{name}"


def request_id() -> str:
    return uuid.uuid4().hex


def command(run_id: str, name: str, command_name: str, **fields: Any) -> dict[str, Any]:
    return {
        "schema_version": "1",
        "request_id": request_id(),
        "operation_id": operation_id(run_id, name),
        "command": command_name,
        **fields,
    }


def prepare_session(
    bridge: BridgeProcess,
    run_id: str,
    name: str,
    provider_agent_id: str,
    registry_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = {
        "task_id": f"task-{run_id}",
        "attempt_id": f"attempt-{name}",
        "agent_registry_id": registry_id,
        "provider_agent_id": provider_agent_id,
        "input_revision": 1,
        "fence": 1,
    }
    result = require_confirmed(bridge.request(command(
        run_id, f"{name}-prepare", "session.prepare", binding=binding,
    )), "session.prepare")
    bound_session = {**binding, "conversation_id": result["conversation_id"]}
    return result, bound_session


def run_turn(
    bridge: BridgeProcess,
    run_id: str,
    name: str,
    binding: dict[str, Any],
    message: str,
    after_dispatch: Any | None = None,
) -> dict[str, Any]:
    operation = operation_id(run_id, name)
    sent = bridge.request(command(
        run_id, name, "session.turn", binding=binding, message=message,
    ))
    if sent["status"] not in {"CONFIRMED", "UNKNOWN"}:
        raise ProbeError(f"session.turn returned {sent['status']}: {sent.get('error', 'no detail')}")
    if after_dispatch is not None:
        after_dispatch()
    for attempt in range(6):
        response = bridge.request({
            "schema_version": "1",
            "request_id": request_id(),
            "operation_id": operation,
            "command": "events.collect",
            "binding": binding,
        }, timeout=130)
        result = response.get("result") or {}
        if result.get("state") != "RUNNING":
            break
        if attempt < 5:
            time.sleep(1)
    events = result.get("events", [])
    return {
        "dispatch_status": sent["status"],
        "dispatch_state": (sent.get("result") or {}).get("state"),
        "event_status": response["status"],
        "state": result.get("state", "UNKNOWN"),
        "event_types": [event.get("event_type") for event in events],
        "error_codes": [
            event.get("error_code")
            for event in events
            if event.get("error_code") is not None
        ],
        "usage_completeness": [
            event.get("usage", {}).get("completeness")
            for event in events
        ],
        "usage_events": [
            {
                "event_id": event["event_id"],
                "operation_id": event["operation_id"],
                "event_type": event["event_type"],
                "usage": event["usage"],
            }
            for event in events
            if event.get("event_type") == "usage_statistics"
        ],
        "tool_executor_calls": result.get("tool_executor_calls", 0),
        "blocked_tool_attempts": result.get("blocked_tool_attempts", 0),
    }


def node_runtime() -> tuple[str, str]:
    configured = os.environ.get("HEKATE_NODE_BIN")
    node = configured or shutil.which("node")
    if not node:
        raise ProbeError("Node.js 22.19.0 is required; set HEKATE_NODE_BIN or update PATH")
    version = run([node, "--version"], timeout=10)
    expected = f"v{LOCK['bridge']['node_version']}"
    if version != expected:
        raise ProbeError(f"Node.js {expected} required, found {version}")
    return node, version.removeprefix("v")


def build_patched_runtime_image(base_image: str) -> tuple[str, str]:
    pinned_base = f"{LOCK['app_server']['image']}@{LOCK['app_server']['image_digest']}"
    if base_image != pinned_base:
        raise ProbeError(f"only the locked Letta image is supported in this probe: {pinned_base}")
    patch_path = ROOT / "integration/letta/patches/provider-call-context-usage.patch"
    patch_sha = hashlib.sha256(patch_path.read_bytes()).hexdigest()
    locked_patch = LOCK["patches"][0]
    if patch_sha != locked_patch["sha256"]:
        raise ProbeError("pinned Letta runtime patch checksum mismatch")

    _, node_version = node_runtime()
    if node_version != "22.19.0":
        raise ProbeError("the Letta Code source build requires pinned Node.js 22.19.0")
    bun = shutil.which("bun")
    if not bun or run([bun, "--version"], timeout=10) != "1.3.14":
        raise ProbeError("Bun 1.3.14 is required to build the pinned Letta Code runtime")

    source_path = Path(os.environ.get("HEKATE_LETTA_CODE_SOURCE", "/tmp/hekate-letta-code"))
    temporary_source: tempfile.TemporaryDirectory[str] | None = None
    if not source_path.exists():
        temporary_source = tempfile.TemporaryDirectory(prefix="hekate-letta-code-")
        source_path = Path(temporary_source.name) / "source"
        run(["git", "init", str(source_path)])
        run(["git", "-C", str(source_path), "remote", "add", "origin", "https://github.com/letta-ai/letta-code.git"])
        run(["git", "-C", str(source_path), "fetch", "--depth", "1", "origin", LOCK["app_server"]["source_commit"]], timeout=180)
        run(["git", "-C", str(source_path), "checkout", "--detach", "FETCH_HEAD"])

    try:
        source_commit = run(["git", "-C", str(source_path), "rev-parse", "HEAD"])
        if source_commit != LOCK["app_server"]["source_commit"]:
            raise ProbeError(f"Letta Code source commit mismatch: {source_commit}")
        reverse = subprocess.run(
            ["git", "-C", str(source_path), "apply", "--reverse", "--check", str(patch_path)],
            capture_output=True,
            check=False,
        )
        if reverse.returncode:
            run(["git", "-C", str(source_path), "apply", str(patch_path)])
        diff = subprocess.run(
            ["git", "-C", str(source_path), "diff", "--binary"],
            capture_output=True,
            check=True,
        ).stdout
        # `git apply` leaves newly added patch files untracked. Include their
        # binary diff without staging anything in this pinned source checkout.
        untracked = subprocess.run(
            ["git", "-C", str(source_path), "ls-files", "--others", "--exclude-standard", "-z"],
            capture_output=True,
            check=True,
        ).stdout
        for relative in sorted(item for item in untracked.split(b"\0") if item):
            addition = subprocess.run(
                ["git", "-C", str(source_path), "diff", "--binary", "--no-index", "/dev/null", relative.decode()],
                capture_output=True,
                check=False,
            )
            if addition.returncode not in {0, 1}:
                raise ProbeError("could not inspect a new file in the pinned runtime patch")
            diff += addition.stdout
        if diff != patch_path.read_bytes():
            raise ProbeError("Letta Code checkout has changes outside the pinned runtime patch")

        run([bun, "install", "--frozen-lockfile"], timeout=600, cwd=source_path)
        run([bun, "run", "build"], timeout=600, cwd=source_path)
        bundle = source_path / "letta.js"
        if not bundle.is_file():
            raise ProbeError("patched Letta Code build did not produce letta.js")

        tag = f"hekate/letta-code-p1:{source_commit[:8]}-{patch_sha[:8]}"
        with tempfile.TemporaryDirectory(prefix="hekate-runtime-image-") as context_name:
            context = Path(context_name)
            shutil.copy2(bundle, context / "letta.js")
            (context / "Dockerfile").write_text(
                "ARG BASE_IMAGE\n"
                "FROM ${BASE_IMAGE}\n"
                f"LABEL org.opencontainers.image.revision={source_commit}\n"
                f"LABEL io.hekate.runtime-patch.sha256={patch_sha}\n"
                "COPY letta.js /usr/local/lib/node_modules/@letta-ai/letta-code/letta.js\n",
                encoding="utf-8",
            )
            run([
                "docker", "build", "--build-arg", f"BASE_IMAGE={base_image}",
                "--tag", tag, str(context),
            ], timeout=600)
        image_id = run(["docker", "image", "inspect", "--format", "{{.Id}}", tag])
        return tag, image_id
    finally:
        if temporary_source is not None:
            temporary_source.cleanup()


def build_bridge(node: str) -> None:
    npm = shutil.which("npm")
    if not npm:
        raise ProbeError("npm is required to build the bridge")
    run([npm, "--prefix", "bridge/letta", "run", "build"], timeout=120)
    if not (ROOT / "bridge/letta/dist/main.js").is_file():
        raise ProbeError("bridge build did not produce dist/main.js")


def base_report(run_id: str, image: str) -> dict[str, Any]:
    return {
        "schema_version": "1",
        "run_id": run_id,
        "executed_at": utc_now(),
        "profile": {
            "sdk": LOCK["bridge"]["sdk"],
            "node_version": LOCK["bridge"]["node_version"],
            "app_server": LOCK["app_server"],
            "app_server_image": image,
            "app_server_base_image": image,
            "runtime_patch": LOCK["patches"][0],
            "backend": "local",
            "model": f"openai-compatible/{FAKE_MODEL}",
            "network": "dedicated Docker bridge with --internal",
        },
        "gates": {
            gate: {
                "source_status": "not_run",
                "runtime_test_status": "not_run",
                "guarantees": [],
                "limitations": [],
            }
            for gate in ("G1", "G2", "G5", "G6", "G7", "G8", "G9")
        },
        "observations": {},
        "real_provider_calls": 0,
        "estimated_real_provider_cost_usd": 0,
        "overall_status": "blocked",
    }


def probe_permit_binding(provider: FakeProvider, run_id: str) -> dict[str, Any]:
    binding_a = {
        "task_id": f"task-{run_id}-permit-a",
        "attempt_id": "attempt-a",
        "agent_registry_id": "ha-permit-a",
        "provider_agent_id": "agent-permit-probe",
        "input_revision": 2,
        "fence": 7,
        "conversation_id": "conversation-permit-a",
    }


    binding_b = {**binding_a, "task_id": f"task-{run_id}-permit-b", "attempt_id": "attempt-b",
                 "agent_registry_id": "ha-permit-b", "conversation_id": "conversation-permit-b"}
    model = FAKE_MODEL
    output_limit = 64

    def headers(binding: dict[str, Any], operation: str, call_id: str, *, model_name: str = model,
                max_output_tokens: int = output_limit) -> dict[str, str]:
        return {
            "content-type": "application/json",
            "x-hekate-task-id": binding["task_id"],
            "x-hekate-attempt-id": binding["attempt_id"],
            "x-hekate-operation-id": operation,
            "x-hekate-agent-registry-id": binding["agent_registry_id"],
            "x-hekate-provider-agent-id": binding["provider_agent_id"],
            "x-hekate-input-revision": str(binding["input_revision"]),
            "x-hekate-fence": str(binding["fence"]),
            "x-hekate-conversation-id": binding["conversation_id"],
            "x-hekate-accounting-call-id": call_id,
            "x-hekate-call-kind": "turn",
            "x-hekate-model": model_name,
            "x-hekate-max-output-tokens": str(max_output_tokens),
        }

    def post(request_headers: dict[str, str], *, request_model: str = model,
             request_output_limit: int = output_limit) -> int:
        body = json.dumps({
            "model": request_model,
            "stream": False,
            "max_completion_tokens": request_output_limit,
            "messages": [{"role": "user", "content": "permit binding probe"}],
        }, separators=(",", ":")).encode()
        host, port = provider.gateway.server_address
        request = Request(f"http://{host}:{port}/v1/chat/completions", data=body,
                          headers=request_headers, method="POST")
        try:
            with urlopen(request, timeout=10) as response:
                response.read()
                return response.status
        except HTTPError as error:
            error.read()
            return error.code

    before = provider.snapshot()["provider_request_count"]
    background_status = post({"content-type": "application/json"})
    background_blocked = background_status == 402 and provider.snapshot()["provider_request_count"] == before
    op_a = operation_id(run_id, "permit-a")
    op_b = operation_id(run_id, "permit-b")
    provider.ledger.issue(op_a, binding_a)
    wrong_operation_status = post(headers(binding_b, op_b, "permit-cross-operation"))
    cross_operation_blocked = wrong_operation_status == 402 and provider.snapshot()["provider_request_count"] == before

    wrong_binding_status = post(headers(binding_b, op_a, "permit-wrong-binding"))
    wrong_binding_blocked = wrong_binding_status == 402 and provider.snapshot()["provider_request_count"] == before

    valid_call_id = "permit-valid-call-1"
    valid_status = post(headers(binding_a, op_a, valid_call_id))
    after_valid = provider.snapshot()["provider_request_count"]
    replay_status = post(headers(binding_a, op_a, valid_call_id))
    replay_blocked = replay_status == 402 and provider.snapshot()["provider_request_count"] == after_valid

    expired_op = operation_id(run_id, "permit-expired")
    provider.ledger.issue(expired_op, binding_a, ttl_seconds=-1)
    expired_status = post(headers(binding_a, expired_op, "permit-expired-call"))

    output_op = operation_id(run_id, "permit-output-mismatch")
    provider.ledger.issue(output_op, binding_a)
    output_status = post(headers(binding_a, output_op, "permit-output-call", max_output_tokens=128),
                         request_output_limit=128)

    model_op = operation_id(run_id, "permit-model-mismatch")
    provider.ledger.issue(model_op, binding_a)
    model_status = post(headers(binding_a, model_op, "permit-model-call", model_name="other-model"),
                        request_model="other-model")

    unavailable_op = operation_id(run_id, "permit-provider-unavailable")
    provider.ledger.issue(unavailable_op, binding_a)
    provider.ledger.available = False
    unavailable_status = post(headers(binding_a, unavailable_op, "permit-unavailable-call"))
    provider.ledger.available = True

    return {
        "background_without_operation": {
            "status": background_status,
            "blocked_before_provider": background_blocked,
        },
        "cross_operation": {"status": wrong_operation_status, "blocked_before_provider": cross_operation_blocked},
        "wrong_binding": {"status": wrong_binding_status, "blocked_before_provider": wrong_binding_blocked},
        "valid_call": {"status": valid_status, "provider_requests": after_valid - before},
        "replay": {"status": replay_status, "blocked_before_provider": replay_blocked},
        "expired": {"status": expired_status, "blocked_before_provider": expired_status == 402},
        "output_mismatch": {"status": output_status, "blocked_before_provider": output_status == 402},
        "model_mismatch": {"status": model_status, "blocked_before_provider": model_status == 402},
        "permit_provider_unavailable": {
            "status": unavailable_status,
            "blocked_before_provider": unavailable_status == 402,
        },
        "all_denials_prevented_forwarding": all((
            background_blocked, cross_operation_blocked, wrong_binding_blocked, replay_blocked,
            expired_status == 402, output_status == 402, model_status == 402,
            unavailable_status == 402,
        )),
    }


def reconcile_fake_usage(
    provider_snapshot: dict[str, Any],
    turns: dict[str, dict[str, Any]],
    expected_pending_call_ids: set[str] | None = None,
) -> dict[str, Any]:
    expected_pending_call_ids = expected_pending_call_ids or set()
    requests: dict[str, list[dict[str, Any]]] = {}
    for item in provider_snapshot["model_requests"]:
        if item.get("accounting_call_id"):
            requests.setdefault(item["accounting_call_id"], []).append(item)
    usage_by_call: dict[str, dict[str, Any]] = {}
    usage_event_counts: dict[str, int] = {}
    conflicting_calls: set[str] = set()
    for turn in turns.values():
        for event in turn.get("usage_events", []):
            usage = event.get("usage") or {}
            call_id = usage.get("accounting_call_id")
            if not call_id:
                continue
            usage_event_counts[call_id] = usage_event_counts.get(call_id, 0) + 1
            if event.get("event_type") == "usage_conflict":
                conflicting_calls.add(call_id)
            usage_by_call[call_id] = usage

    rows = []
    non_runtime_gateway_check_count = 0
    for check in provider_snapshot["checks"]:
        operation = check.get("operation_id")
        call_id = check.get("accounting_call_id")
        if operation not in turns:
            non_runtime_gateway_check_count += 1
            continue
        if not call_id:
            status = "MISMATCH" if check.get("authorized") else "NOT_FORWARDED"
            rows.append({
                "operation_id": operation,
                "call_kind": check.get("call_kind"),
                "accounting_call_id": None,
                "settlement": status,
                "provider_behavior": None,
                "bridge_usage": None,
                "bridge_usage_event_count": 0,
                "duplicate_usage_events": 0,
            })
            continue
        matched_requests = requests.get(call_id, [])
        request = matched_requests[0] if matched_requests else {}
        normalized = usage_by_call.get(call_id)
        fixture = request.get("fake_usage")
        if not check.get("authorized"):
            status = "MISMATCH" if matched_requests else "NOT_FORWARDED"
        elif len(matched_requests) > 1:
            status = "MISMATCH"
        elif not matched_requests:
            status = "FORWARDING_UNKNOWN"
        elif call_id in conflicting_calls:
            status = "MISMATCH"
        elif fixture:
            has_conflicting_value = bool(normalized) and any(
                normalized.get(key) is not None and normalized.get(key) != fixture.get(fixture_key)
                for key, fixture_key in (
                    ("input_tokens", "prompt_tokens"),
                    ("output_tokens", "completion_tokens"),
                    ("total_tokens", "total_tokens"),
                )
            )
            if has_conflicting_value or (
                normalized and normalized.get("provider_call_id") is not None and
                normalized.get("provider_call_id") != request.get("provider_response_id")
            ):
                status = "MISMATCH"
            elif bool(
                normalized and
                normalized.get("completeness") == "COMPLETE" and
                normalized.get("accounting_call_id") == call_id and
                normalized.get("provider_call_id") == request.get("provider_response_id") and
                normalized.get("input_tokens") == fixture.get("prompt_tokens") and
                normalized.get("output_tokens") == fixture.get("completion_tokens") and
                normalized.get("total_tokens") == fixture.get("total_tokens")
            ):
                status = "MATCHED"
            else:
                status = "MISSING_USAGE"
        elif (
            call_id in expected_pending_call_ids and
            request.get("behavior") == "error" and
            request.get("provider_response_id") is None and
            normalized is not None and
            normalized.get("completeness") == "UNKNOWN" and
            normalized.get("accounting_call_id") == call_id and
            normalized.get("provider_call_id") is None and
            not any(normalized.get(key) is not None for key in (
                "input_tokens", "output_tokens", "total_tokens", "cost_usd",
            ))
        ):
            status = "EXPECTED_PENDING"
        elif (
            call_id in expected_pending_call_ids and
            request.get("behavior") == "disconnect" and
            request.get("provider_response_id") is None and
            fixture is None and
            turns.get(operation, {}).get("state") == "UNKNOWN" and
            (
                normalized is None or
                (
                    normalized.get("completeness") == "UNKNOWN" and
                    normalized.get("accounting_call_id") == call_id and
                    normalized.get("provider_call_id") is None and
                    not any(normalized.get(key) is not None for key in (
                        "input_tokens", "output_tokens", "total_tokens", "cost_usd",
                    ))
                )
            )
        ):
            status = "EXPECTED_PENDING"
        else:
            status = "MISSING_USAGE"
        rows.append({
            "operation_id": operation,
            "call_kind": check.get("call_kind"),
            "accounting_call_id": call_id,
            "provider_behavior": request.get("behavior"),
            "provider_response_id": request.get("provider_response_id"),
            "fake_usage_fixture": fixture,
            "bridge_usage": normalized,
            "settlement": status,
            "bridge_usage_event_count": usage_event_counts.get(call_id, 0),
            "duplicate_usage_events": max(usage_event_counts.get(call_id, 0) - 1, 0),
        })

    checked_call_ids = {
        check.get("accounting_call_id") for check in provider_snapshot["checks"]
        if check.get("accounting_call_id")
    }
    for call_id, call_requests in requests.items():
        if call_id in checked_call_ids:
            continue
        rows.append({
            "operation_id": None,
            "call_kind": None,
            "accounting_call_id": call_id,
            "provider_behavior": call_requests[0].get("behavior"),
            "provider_response_id": call_requests[0].get("provider_response_id"),
            "fake_usage_fixture": call_requests[0].get("fake_usage"),
            "bridge_usage": usage_by_call.get(call_id),
            "settlement": "MISMATCH",
            "bridge_usage_event_count": usage_event_counts.get(call_id, 0),
            "duplicate_usage_events": max(usage_event_counts.get(call_id, 0) - 1, 0),
        })

    runtime_request_count = sum(
        len(requests.get(check.get("accounting_call_id"), []))
        for check in provider_snapshot["checks"]
        if check.get("authorized") and check.get("operation_id") in turns
    )
    return {
        "fixture_is_synthetic": True,
        "usage_count_scope": "events.collect records after bridge same-call updates are merged",
        "provider_usage_fixture": FAKE_USAGE,
        "calls": rows,
        "counts": {
            "runtime_provider_requests": runtime_request_count,
            "non_runtime_gateway_checks": non_runtime_gateway_check_count,
            "direct_gateway_provider_requests": sum(
                len(requests.get(check.get("accounting_call_id"), []))
                for check in provider_snapshot["checks"]
                if check.get("authorized") and check.get("operation_id") not in turns
            ),
            "gateway_attempts": len(provider_snapshot["checks"]),
            "gateway_denials": sum(not check.get("authorized") for check in provider_snapshot["checks"]),
            "permits_issued": provider_snapshot.get("permits_issued", 0),
            "permits_consumed": provider_snapshot.get("permits_consumed", 0),
            "provider_endpoint_requests": provider_snapshot.get("provider_request_count", len(provider_snapshot["model_requests"])),
        },
        "all_successful_calls_matched": all(
            row["settlement"] == "MATCHED"
            for row in rows if row.get("fake_usage_fixture") is not None
        ),
        "unsettled_call_count": sum(
            row["settlement"] not in {"MATCHED", "NOT_FORWARDED"}
            for row in rows
        ),
        "blocking_call_count": sum(
            row["settlement"] not in {"MATCHED", "EXPECTED_PENDING", "NOT_FORWARDED"}
            for row in rows
        ),
        "usage_event_counts": usage_event_counts,
        "usage_duplicate_counts": {
            call_id: max(count - 1, 0) for call_id, count in usage_event_counts.items()
        },
    }


def evaluate_g9(reconciliation: dict[str, Any]) -> bool:
    rows = reconciliation["calls"]
    allowed = {"MATCHED", "EXPECTED_PENDING", "NOT_FORWARDED"}
    matched_compactions = sum(
        row.get("call_kind") == "compaction" and row["settlement"] == "MATCHED"
        for row in rows
    )
    matched_turns = sum(
        row.get("call_kind") == "turn" and row["settlement"] == "MATCHED"
        for row in rows
    )
    return bool(
        any(row["settlement"] == "MATCHED" for row in rows) and
        matched_turns >= 1 and matched_compactions >= 2 and
        all(row["settlement"] in allowed for row in rows)
    )


def probe(base_image: str) -> dict[str, Any]:
    run_id = "p1-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    report = base_report(run_id, base_image)
    gates = report["gates"]
    agent_id: str | None = None
    compaction_agent_id: str | None = None

    try:
        image, image_id = build_patched_runtime_image(base_image)
        report["profile"].update({
            "app_server_image": image,
            "app_server_image_id": image_id,
        })
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        for gate in gates.values():
            gate["limitations"].append("Probe did not run because the pinned patched runtime image could not be built.")
        return report

    with tempfile.TemporaryDirectory(prefix=f"{run_id}-") as temporary:
        workdir = Path(temporary)
        sandbox = DockerSandbox(workdir, run_id, image)
        provider: FakeProvider | None = None
        bridge: BridgeProcess | None = None
        structured_bridge: BridgeProcess | None = None
        try:
            node, node_version = node_runtime()
            build_bridge(node)
            sandbox.start_network()
            provider = FakeProvider(sandbox.gateway_address)
            provider.start()
            sandbox.start_app_server(provider.gateway.server_port)
            token = sandbox.token_path.read_text(encoding="utf-8")
            bridge = BridgeProcess(image, sandbox.network, sandbox.container, token)

            hello = require_confirmed(bridge.request(command(
                run_id, "hello", "hello",
            )), "hello")
            gates["G1"]["source_status"] = "pass"
            gates["G1"]["guarantees"].append(
                f"SDK {LOCK['bridge']['sdk']['version']} connected through remote App Server transport."
            )

            owner = f"phase1-{uuid.uuid4().hex[:10]}"
            creation_tag = uuid.uuid4().hex
            created = require_confirmed(bridge.request(command(
                run_id,
                "agent-create",
                "agent.create",
                owner=owner,
                creation_tag=creation_tag,
                role="hekate",
                model=f"openai-compatible/{FAKE_MODEL}",
                max_input_tokens=16384,
                max_output_tokens=64,
            )), "agent.create")
            agent_id = created["provider_agent_id"]
            observed = require_confirmed(bridge.request(command(
                run_id, "agent-get", "agent.get", provider_agent_id=agent_id,
            )), "agent.get")
            listed = require_confirmed(bridge.request(command(
                run_id, "agent-list", "agent.list", owner=owner, creation_tag=creation_tag,
            )), "agent.list")
            gates["G2"]["source_status"] = "pass"
            gates["G2"]["guarantees"] = [
                "A tagged probe agent was created, retrieved, and listed by owner plus creation tag.",
                "The SDK provider agent ID remained distinct from the HEKATE registry ID.",
            ]

            registry_id = f"ha-{uuid.uuid4().hex[:12]}"
            prepared, binding = prepare_session(
                bridge, run_id, "tool-boundary", agent_id, registry_id,
            )
            app_info = prepared["app_server_info"]
            gates["G1"]["runtime_test_status"] = (
                "pass" if app_info.get("protocol_version") == LOCK["app_server"]["protocol_version"] else "fail"
            )
            gates["G1"]["guarantees"].append(
                f"App Server runtime reported protocol {app_info.get('protocol_version')} and "
                f"Letta Code {app_info.get('letta_code_version')}."
            )
            gates["G1"]["limitations"] = [
                "Advertised capability flags are recorded as claims, not as end-to-end proofs."
            ]

            provider.ledger.issue(operation_id(run_id, "tool-turn"), binding)
            provider.set_next_behavior("tool_call")
            tool_turn = run_turn(
                bridge,
                run_id,
                "tool-turn",
                binding,
                "Use the available tool and return its result.",
            )
            fake_after_tool = provider.snapshot()
            tool_request = fake_after_tool["model_requests"][-1] if fake_after_tool["model_requests"] else {}
            reopened, reopened_binding = prepare_session(
                bridge, run_id, "tool-boundary-reopened", agent_id, registry_id,
            )
            reopened_operation = operation_id(run_id, "tool-turn-reopened")
            provider.ledger.issue(reopened_operation, reopened_binding)
            provider.set_next_behavior("tool_call")
            reopened_turn = run_turn(
                bridge, run_id, "tool-turn-reopened", reopened_binding,
                "Request the unavailable test tool and return its result.",
            )
            fake_after_reopen = provider.snapshot()
            reopened_tool_request = fake_after_reopen["model_requests"][-1] if fake_after_reopen["model_requests"] else {}
            no_tools_first = not prepared.get("agent_tools") and not tool_request.get("tool_names")
            no_tools_reopened = not reopened.get("agent_tools") and not reopened_tool_request.get("tool_names")
            no_tool_execution = tool_turn["tool_executor_calls"] == reopened_turn["tool_executor_calls"] == 0
            tool_surface_audit = [
                {"surface": "Bash/shell", "registration": "client tool", "handler": "provider-turn executor", "reachable": False, "reason": "session uses allowedTools=[], toolset base=none, tools=[]; provider requests expose no tools", "evidence": "static session config plus dynamic before/after tool lists and absent-tool call; executor calls 0"},
                {"surface": "file read/write", "registration": "client tools", "handler": "client tool executor", "reachable": False, "reason": "same empty model tool boundary; no filesystem tool is registered", "evidence": "static session config plus dynamic empty provider tool list"},
                {"surface": "web/network", "registration": "client tools or provider HTTP", "handler": "client tool executor / fake gateway", "reachable": False, "reason": "no web tools; Docker network is --internal and only the test gateway is host-mapped, with permit checks on POSTs and synthetic responses", "evidence": "static network and gateway routes plus dynamic zero-tool request"},
                {"surface": "MCP", "registration": "interactive CLI MCP client", "handler": "MCP client tools", "reachable": False, "reason": "no MCP server config is present in the isolated App Server state and no MCP tools are attached to this session", "evidence": "static isolated state setup plus dynamic empty tool list"},
                {"surface": "native subagent", "registration": "Agent/Task client tool", "handler": "CLI subagent manager", "reachable": False, "reason": "no Agent/Task tool is attached; App Server is launched with the session toolset disabled", "evidence": "static session config plus dynamic absent-tool call; executor calls 0"},
            ]
            gates["G5"]["source_status"] = "pass"
            gates["G5"]["runtime_test_status"] = "pass" if no_tools_first and no_tools_reopened and no_tool_execution else "blocked"
            gates["G5"]["guarantees"] = [
                f"Provider tool lists were {tool_request.get('tool_names', [])} before close and "
                f"{reopened_tool_request.get('tool_names', [])} after session reprepare; executor calls were "
                f"{tool_turn['tool_executor_calls']} and {reopened_turn['tool_executor_calls']}.",
                f"No model-facing tools were observed before/after reprepare={no_tools_first and no_tools_reopened}; "
                "the test model also returned a tool call absent from its offered list.",
                "Pinned App Server session config disables the toolset (`allowedTools: []`, `base: none`, `tools: []`); static source audit found no reachable Bash, filesystem, web, MCP, or subagent tool route in this probe profile.",
            ]
            gates["G5"]["limitations"] = [
                "This result applies to the pinned local App Server profile and isolated probe state; it does not establish tool isolation for other backends or deployments."
            ]

            _, normal_binding = prepare_session(
                bridge, run_id, "normal-inference", agent_id, registry_id,
            )
            normal_operation = operation_id(run_id, "normal-turn")
            normal_before = provider.snapshot()
            provider.ledger.issue(normal_operation, normal_binding)
            normal_turn = run_turn(
                bridge, run_id, "normal-turn", normal_binding,
                "Return a brief confirmation.",
            )
            normal_after = provider.snapshot()
            normal_request = normal_after["model_requests"][-1] if normal_after["model_requests"] else {}
            normal_delta = {
                "provider_requests": normal_after["provider_request_count"] - normal_before["provider_request_count"],
                "permits_issued": normal_after["permits_issued"] - normal_before["permits_issued"],
                "permits_consumed": normal_after["permits_consumed"] - normal_before["permits_consumed"],
                "denied_attempts": normal_after["denied_attempts"] - normal_before["denied_attempts"],
            }

            _, retry_binding = prepare_session(
                bridge, run_id, "provider-retry", agent_id, registry_id,
            )
            retry_operation = operation_id(run_id, "retry-turn")
            retry_before = provider.snapshot()
            provider.ledger.issue(retry_operation, retry_binding)
            provider.set_next_behavior("error")
            retry_turn = run_turn(
                bridge, run_id, "retry-turn", retry_binding,
                "Return a brief confirmation.",
            )
            retry_after = provider.snapshot()
            retry_delta = {
                "provider_requests": retry_after["provider_request_count"] - retry_before["provider_request_count"],
                "gateway_attempts": retry_after["gateway_attempts"] - retry_before["gateway_attempts"],
                "permits_issued": retry_after["permits_issued"] - retry_before["permits_issued"],
                "permits_consumed": retry_after["permits_consumed"] - retry_before["permits_consumed"],
                "denied_attempts": retry_after["denied_attempts"] - retry_before["denied_attempts"],
            }

            _, empty_binding = prepare_session(
                bridge, run_id, "empty-response-retry", agent_id, registry_id,
            )
            empty_operation = operation_id(run_id, "empty-turn")
            empty_before = provider.snapshot()
            provider.ledger.issue(empty_operation, empty_binding)
            provider.set_next_behavior("empty")
            empty_turn = run_turn(
                bridge, run_id, "empty-turn", empty_binding,
                "Return a brief confirmation.",
            )
            empty_after = provider.snapshot()
            empty_delta = {
                "provider_requests": empty_after["provider_request_count"] - empty_before["provider_request_count"],
                "gateway_attempts": empty_after["gateway_attempts"] - empty_before["gateway_attempts"],
                "permits_issued": empty_after["permits_issued"] - empty_before["permits_issued"],
                "permits_consumed": empty_after["permits_consumed"] - empty_before["permits_consumed"],
                "denied_attempts": empty_after["denied_attempts"] - empty_before["denied_attempts"],
            }

            compaction_agent = require_confirmed(bridge.request(command(
                run_id, "compaction-agent-create", "agent.create",
                owner=owner, creation_tag=f"{creation_tag}-compaction", role="hekate",
                model=f"openai-compatible/{FAKE_MODEL}", max_input_tokens=8192, max_output_tokens=64,
            )), "compaction agent.create")
            compaction_agent_id = compaction_agent["provider_agent_id"]
            _, no_compaction_permit_binding = prepare_session(
                bridge, run_id, "compaction-no-permit", compaction_agent_id,
                f"ha-{uuid.uuid4().hex[:12]}",
            )
            no_compaction_permit_operation = operation_id(run_id, "compaction-no-permit")
            no_compaction_permit_before = provider.snapshot()
            provider.ledger.issue(
                no_compaction_permit_operation, no_compaction_permit_binding,
                call_kind="turn",
            )
            no_compaction_permit_turn = run_turn(
                bridge, run_id, "compaction-no-permit", no_compaction_permit_binding,
                "Return a brief confirmation.",
            )
            no_compaction_permit_after = provider.snapshot()
            no_compaction_permit_delta = {
                "provider_requests": no_compaction_permit_after["provider_request_count"] - no_compaction_permit_before["provider_request_count"],
                "gateway_attempts": no_compaction_permit_after["gateway_attempts"] - no_compaction_permit_before["gateway_attempts"],
                "permits_consumed": no_compaction_permit_after["permits_consumed"] - no_compaction_permit_before["permits_consumed"],
                "denied_attempts": no_compaction_permit_after["denied_attempts"] - no_compaction_permit_before["denied_attempts"],
            }
            no_compaction_permit_checks = [
                check for check in no_compaction_permit_after["checks"]
                if check.get("operation_id") == no_compaction_permit_operation
            ]
            compaction_without_permit_blocked = (
                no_compaction_permit_delta["provider_requests"] == 0 and
                no_compaction_permit_delta["permits_consumed"] == 0 and
                no_compaction_permit_delta["denied_attempts"] == 1 and
                any(check.get("call_kind") == "compaction" and
                    check.get("reason") == "call_kind_mismatch"
                    for check in no_compaction_permit_checks)
            )
            _, compaction_binding = prepare_session(
                bridge, run_id, "context-compaction", compaction_agent_id,
                f"ha-{uuid.uuid4().hex[:12]}",
            )
            warmup_operation = operation_id(run_id, "compaction-warmup")
            provider.ledger.issue(warmup_operation, compaction_binding, call_kind="compaction")
            provider.ledger.issue(warmup_operation, compaction_binding, call_kind="turn")
            warmup_turn = run_turn(
                bridge, run_id, "compaction-warmup", compaction_binding,
                "Return a brief confirmation.",
            )
            compaction_operation = operation_id(run_id, "context-compaction-turn")
            compaction_before = provider.snapshot()
            provider.ledger.issue(
                compaction_operation, compaction_binding, call_kind="compaction",
            )
            provider.ledger.issue(compaction_operation, compaction_binding, call_kind="turn")
            compaction_turn = run_turn(
                bridge, run_id, "context-compaction-turn", compaction_binding,
                "Summarize this context and respond briefly. " + ("scope evidence context " * 2_000),
            )
            compaction_after = provider.snapshot()
            compaction_delta = {
                "provider_requests": compaction_after["provider_request_count"] - compaction_before["provider_request_count"],
                "gateway_attempts": compaction_after["gateway_attempts"] - compaction_before["gateway_attempts"],
                "permits_issued": compaction_after["permits_issued"] - compaction_before["permits_issued"],
                "permits_consumed": compaction_after["permits_consumed"] - compaction_before["permits_consumed"],
                "denied_attempts": compaction_after["denied_attempts"] - compaction_before["denied_attempts"],
            }
            compaction_checks = [
                check for check in compaction_after["checks"]
                if check.get("operation_id") == compaction_operation
            ]
            compaction_observed = (
                "compaction" in compaction_turn["event_types"] and
                any(check.get("authorized") and check.get("call_kind") == "compaction"
                    for check in compaction_checks)
            )

            structured_bridge = BridgeProcess(
                image, sandbox.network, sandbox.container, token,
                structured_output_probe=True,
            )
            _, structured_binding = prepare_session(
                structured_bridge, run_id, "structured-output", agent_id, registry_id,
            )
            structured_operation = operation_id(run_id, "structured-output-turn")
            structured_before = provider.snapshot()
            provider.ledger.issue(structured_operation, structured_binding)
            provider.set_next_behavior("invalid_schema")
            structured_turn = run_turn(
                structured_bridge, run_id, "structured-output-turn", structured_binding,
                "Return a Position Commit matching the requested JSON Schema.",
            )
            structured_after = provider.snapshot()
            structured_delta = {
                "provider_requests": structured_after["provider_request_count"] - structured_before["provider_request_count"],
                "gateway_attempts": structured_after["gateway_attempts"] - structured_before["gateway_attempts"],
                "permits_issued": structured_after["permits_issued"] - structured_before["permits_issued"],
                "permits_consumed": structured_after["permits_consumed"] - structured_before["permits_consumed"],
                "denied_attempts": structured_after["denied_attempts"] - structured_before["denied_attempts"],
            }
            structured_bridge.close()
            structured_bridge = None

            _, denied_binding = prepare_session(
                bridge, run_id, "permit-denied", agent_id, registry_id,
            )
            denied_before = provider.snapshot()
            denied_turn = run_turn(
                bridge, run_id, "permit-denied", denied_binding,
                "Return a brief confirmation.",
            )
            denied_after = provider.snapshot()
            denied_delta = {
                "provider_requests": denied_after["provider_request_count"] - denied_before["provider_request_count"],
                "gateway_attempts": denied_after["gateway_attempts"] - denied_before["gateway_attempts"],
                "permits_issued": denied_after["permits_issued"] - denied_before["permits_issued"],
                "permits_consumed": denied_after["permits_consumed"] - denied_before["permits_consumed"],
                "denied_attempts": denied_after["denied_attempts"] - denied_before["denied_attempts"],
            }
            permit_binding_tests = probe_permit_binding(provider, run_id)

            gates["G2"]["runtime_test_status"] = (
                "pass"
                if observed["present"] and agent_id in listed["provider_agent_ids"]
                else "fail"
            )
            gates["G2"]["limitations"] = [
                "Creation-response-loss recovery is deferred to G3.",
            ]

            normal_checks = [
                check for check in normal_after["checks"]
                if check.get("operation_id") == normal_operation and check.get("authorized")
            ]
            normal_bound = (
                len(normal_checks) == 1 and
                normal_checks[0].get("binding") == normal_binding and
                normal_checks[0].get("accounting_call_id") == normal_request.get("accounting_call_id") and
                normal_checks[0].get("call_kind") == "turn"
            )
            compaction_bound = (
                compaction_observed and
                compaction_delta["provider_requests"] == compaction_delta["permits_consumed"] == 2 and
                compaction_delta["permits_issued"] == 2 and
                {check.get("call_kind") for check in compaction_checks if check.get("authorized")} == {"compaction", "turn"} and
                all(check.get("binding") == compaction_binding for check in compaction_checks if check.get("authorized"))
            )
            g6_pass = (
                normal_bound and normal_turn["state"] == "COMPLETE" and
                retry_delta["provider_requests"] == retry_delta["permits_issued"] == retry_delta["permits_consumed"] == 1 and
                retry_delta["gateway_attempts"] > retry_delta["provider_requests"] and retry_delta["denied_attempts"] >= 1 and
                empty_delta["provider_requests"] == empty_delta["permits_consumed"] == 1 and
                denied_delta["provider_requests"] == denied_delta["permits_consumed"] == 0 and
                denied_delta["denied_attempts"] >= 1 and
                permit_binding_tests["all_denials_prevented_forwarding"] and
                compaction_without_permit_blocked and compaction_bound
            )
            gates["G6"]["source_status"] = "pass"
            gates["G6"]["runtime_test_status"] = "pass" if g6_pass else "blocked"
            gates["G6"]["guarantees"] = [
                f"Normal provider call matched task/attempt/operation/agent/revision/fence and accounting ID: {normal_bound}; counts={normal_delta}.",
                f"Cross-operation, binding, replay, expiry, model, output, missing-context and permit-provider-unavailable cases were blocked before provider={permit_binding_tests['all_denials_prevented_forwarding']}.",
                f"500 retry counts={retry_delta}; empty response counts={empty_delta}; zero-permit counts={denied_delta}; compaction without its own permit was blocked={compaction_without_permit_blocked}; observed compaction counts={compaction_delta}.",
            ]
            gates["G6"]["limitations"] = [
                "This is an in-memory probe permit provider, not the production atomic budget ledger.",
                "Coverage is limited to the observed local preflight compaction path and its following turn.",
            ]

            retry_blocked = (
                retry_delta["gateway_attempts"] > retry_delta["provider_requests"] and
                retry_delta["denied_attempts"] >= 1
            )
            structured_test_pass = (
                structured_delta["provider_requests"] == 1 and
                structured_delta["permits_consumed"] == 1 and
                structured_delta["gateway_attempts"] == 1 and
                "structured_output_error" in structured_turn["error_codes"]
            )
            gates["G7"]["source_status"] = "pass"
            gates["G7"]["runtime_test_status"] = "blocked"
            gates["G7"]["guarantees"] = [
                f"500 retry was denied before a second provider request={retry_blocked}; counts={retry_delta}.",
                f"Position Commit schema-invalid response with SDK maxRetries=0 used exactly one provider request and produced structured_output_error={structured_test_pass}; counts={structured_delta}.",
            ]
            gates["G7"]["limitations"] = [
                "Pinned SDK 0.8.25 AppServerSession calls watchTransportDisconnect without recoverWhenIdle; RemoteClientSessionCore closes an in-flight turn and session on disconnect. The recoverWhenIdle path is Cloud-only, and App Server resumeSession() rehydrates a conversation without an in-flight execution cursor. Same-execution resume after transport restoration is unsupported by this profile and remains untested.",
                "The empty-response scenario ended after one request; no automatic retry was observed in this runtime path.",
            ]

            gates["G8"]["source_status"] = "pass"
            gates["G8"]["runtime_test_status"] = "blocked"
            gates["G8"]["guarantees"] = [
                f"Agent readback contained context_window_limit={(observed.get('model_settings') or {}).get('context_window_limit')!r} and max_tokens={(observed.get('model_settings') or {}).get('max_tokens')!r}; fake provider observed model={normal_request.get('model')!r}, max_tokens="
                f"{normal_request.get('max_tokens')!r}, max_completion_tokens="
                f"{normal_request.get('max_completion_tokens')!r}.",
            ]
            gates["G8"]["limitations"] = [
                "hekate-fake-model has no tokenizer profile, so the fake provider request exposes no exact input-token count and an under/over-bound probe cannot prove the cap. A v0.1 model profile must provide a validated tokenizer and exact accounting for the full provider request (system prompt, tool schemas, memory, and history), reserve output tokens from the context window, and reject over-bound input before forwarding.",
            ]

            turns = {
                operation_id(run_id, name): turn for name, turn in (
                    ("tool-turn", tool_turn),
                    ("tool-turn-reopened", reopened_turn),
                    ("normal-turn", normal_turn),
                    ("retry-turn", retry_turn),
                    ("empty-turn", empty_turn),
                    ("compaction-no-permit", no_compaction_permit_turn),
                    ("compaction-warmup", warmup_turn),
                    ("context-compaction-turn", compaction_turn),
                    ("structured-output-turn", structured_turn),
                    ("permit-denied", denied_turn),
                )
            }
            provider_snapshot = provider.snapshot()
            expected_pending_call_ids = {
                check["accounting_call_id"]
                for check in provider_snapshot["checks"]
                if check.get("authorized") and
                check.get("operation_id") == retry_operation and
                check.get("accounting_call_id") and
                any(
                    request.get("accounting_call_id") == check["accounting_call_id"] and
                    request.get("behavior") == "error"
                    for request in provider_snapshot["model_requests"]
                )
            }
            usage_reconciliation = reconcile_fake_usage(
                provider_snapshot, turns, expected_pending_call_ids,
            )
            provider_error_unknown_linked = any(
                row["operation_id"] == retry_operation and
                row["settlement"] == "EXPECTED_PENDING"
                for row in usage_reconciliation["calls"]
            )
            usage_reconciliation["provider_error_unknown_linked"] = provider_error_unknown_linked
            gates["G9"]["source_status"] = "pass"
            gates["G9"]["runtime_test_status"] = "pass" if evaluate_g9(usage_reconciliation) else "blocked"
            gates["G9"]["guarantees"] = [
                "The prior loss was in bridge summarizeSdkMessage, which replaced every SDK message's usage with UNKNOWN; SDK 0.8.25 carries the stream_event.event payload. The bridge now normalizes that payload and the fake usage reaches Python reconciliation with its trusted accounting call ID.",
                f"Successful authorized turn and compaction calls matched provider fixtures and both IDs; G9 evaluation={evaluate_g9(usage_reconciliation)}.",
                f"Provider HTTP 500 retained its accounting call ID as explicitly expected pending, without pretending it settled={provider_error_unknown_linked}.",
                f"Runtime requests={usage_reconciliation['counts']['runtime_provider_requests']}; non-runtime gateway checks={usage_reconciliation['counts']['non_runtime_gateway_checks']}; direct gateway provider requests={usage_reconciliation['counts']['direct_gateway_provider_requests']}; denied gateway attempts={usage_reconciliation['counts']['gateway_denials']}; total provider endpoint requests={usage_reconciliation['counts']['provider_endpoint_requests']}.",
                "Pinned SDK source defers the terminal result for a 100 ms trailing-usage grace after stop_reason.",
                f"Per-call settlement rows={len(usage_reconciliation['calls'])}; unsettled calls={usage_reconciliation['unsettled_call_count']}; blocking mismatches/missing/unknown={usage_reconciliation['blocking_call_count']}.",
            ]
            gates["G9"]["limitations"] = [
                "The fixture is synthetic, not provider billing evidence. The 500 call remains EXPECTED_PENDING because the fake provider returned no usage; this is visible and excluded from the settled count. The fake provider sends usage in the final response, so delayed-after-stop delivery was source-checked but not runtime-probed; usage arriving after the SDK's 100 ms grace remains unresolved. events.collect exposes records after same-call merge, so raw late/duplicate SDK deliveries are not counted by this probe.",
            ]

            report["observations"] = {
                "hello": hello,
                "agent_lifecycle": {
                    "created_provider_agent_id": agent_id,
                    "retrieved_present": observed["present"],
                    "listed": agent_id in listed["provider_agent_ids"],
                    "model": observed.get("model"),
                    "max_output_tokens_setting": (observed.get("model_settings") or {}).get("max_tokens"),
                    "context_window_limit_setting": (observed.get("model_settings") or {}).get("context_window_limit"),
                },
                "app_server_info": app_info,
                "tool_boundary": {
                    "session_ready_agent_tools": prepared.get("agent_tools", []),
                    "provider_tool_names": tool_request.get("tool_names", []),
                    "turn": tool_turn,
                    "reprepared_agent_tools": reopened.get("agent_tools", []),
                    "reprepared_provider_tool_names": reopened_tool_request.get("tool_names", []),
                    "reprepared_turn": reopened_turn,
                    "provider_request_count": fake_after_tool["provider_request_count"],
                },
                "tool_surface_audit": tool_surface_audit,
                "normal_turn": {"turn": normal_turn, "delta": normal_delta},
                "provider_error_retry": {"turn": retry_turn, "delta": retry_delta},
                "empty_response_retry": {"turn": empty_turn, "delta": empty_delta},
                "zero_permit_turn": {"turn": denied_turn, "delta": denied_delta},
                "permit_binding_negative_tests": permit_binding_tests,
                "compaction_without_permit": {
                    "turn": no_compaction_permit_turn,
                    "checks": no_compaction_permit_checks,
                    "blocked_before_provider": compaction_without_permit_blocked,
                    "delta": no_compaction_permit_delta,
                },
                "compaction": {
                    "warmup": warmup_turn,
                    "turn": compaction_turn,
                    "operation_checks": compaction_checks,
                    "observed": compaction_observed,
                    "delta": compaction_delta,
                },
                "structured_output": {
                    "schema": "Position Commit v1",
                    "sdk_max_retries": 0,
                    "turn": structured_turn,
                    "delta": structured_delta,
                    "test_pass": structured_test_pass,
                },
                "input_limit_profile": {
                    "model": FAKE_MODEL,
                    "tokenizer_profile": None,
                    "app_server_context_window_limit": (observed.get("model_settings") or {}).get("context_window_limit"),
                    "exact_token_cap_test": "not_run_no_tokenizer_profile",
                    "v0_1_support_condition": "validated tokenizer plus exact full-request accounting for system prompt, tool schemas, memory, and history; reserve output tokens from the context window and reject over-bound input before provider forwarding",
                },
                "usage_reconciliation": usage_reconciliation,
                "usage_delivery_profile": {
                    "original_drop_layer": "bridge summarizeSdkMessage replaced every SDK message's usage with UNKNOWN",
                    "sdk_message_type": "stream_event",
                    "sdk_payload_type": "SDKStreamEventPayload",
                    "sdk_trailing_usage_grace_ms": 100,
                    "delayed_after_stop_runtime_probe": "not_run_fake_usage_arrived_in_final_response",
                },
                "fake_provider": provider.snapshot(),
                "network_internal": True,
                "node_version": node_version,
            }

            compaction_deletion = require_confirmed(bridge.request(command(
                run_id, "compaction-agent-delete", "agent.delete",
                provider_agent_id=compaction_agent_id,
            )), "compaction agent.delete")
            deletion = require_confirmed(bridge.request(command(
                run_id, "agent-delete", "agent.delete", provider_agent_id=agent_id,
            )), "agent.delete")
            after_delete = require_confirmed(bridge.request(command(
                run_id, "agent-post-delete", "agent.get", provider_agent_id=agent_id,
            )), "agent.get after delete")
            gates["G2"]["runtime_test_status"] = (
                "pass" if not deletion["present"] and not after_delete["present"] else "fail"
            )
            report["observations"]["agent_lifecycle"].update({
                "compaction_agent_deleted_present": compaction_deletion["present"],
                "deleted_present": deletion["present"],
                "post_delete_present": after_delete["present"],
            })

            transport_agent = require_confirmed(bridge.request(command(
                run_id, "transport-agent-create", "agent.create",
                owner=owner, creation_tag=f"{creation_tag}-transport", role="hekate",
                model=f"openai-compatible/{FAKE_MODEL}", max_input_tokens=16384, max_output_tokens=64,
            )), "transport agent.create")
            transport_agent_id = transport_agent["provider_agent_id"]
            _, transport_binding = prepare_session(
                bridge, run_id, "transport-disconnect", transport_agent_id,
                f"ha-{uuid.uuid4().hex[:12]}",
            )
            transport_operation = operation_id(run_id, "transport-disconnect")
            transport_before = provider.snapshot()
            provider.ledger.issue(transport_operation, transport_binding)
            provider.set_next_behavior("disconnect")

            def disconnect_after_forwarding() -> None:
                if not provider.wait_for_disconnect_request():
                    raise ProbeError("fake provider did not observe the transport-fault request")
                try:
                    sandbox.disconnect_app_server()
                finally:
                    provider.release_disconnect()

            transport_turn = run_turn(
                bridge, run_id, "transport-disconnect", transport_binding,
                "Return a brief confirmation.", after_dispatch=disconnect_after_forwarding,
            )
            transport_after = provider.snapshot()
            transport_delta = {
                "provider_requests": transport_after["provider_request_count"] - transport_before["provider_request_count"],
                "gateway_attempts": transport_after["gateway_attempts"] - transport_before["gateway_attempts"],
                "permits_consumed": transport_after["permits_consumed"] - transport_before["permits_consumed"],
                "denied_attempts": transport_after["denied_attempts"] - transport_before["denied_attempts"],
            }
            transport_check = next(
                check for check in reversed(transport_after["checks"])
                if check.get("operation_id") == transport_operation
            )
            transport_unknown = (
                transport_turn["state"] == "UNKNOWN" and
                transport_delta["provider_requests"] == 1 and
                transport_delta["permits_consumed"] == 1 and
                transport_check.get("authorized") is True
            )
            gates["G7"]["guarantees"].append(
                f"App Server process loss after dispatch left execution UNKNOWN and issued no retry={transport_unknown}; counts={transport_delta}."
            )
            gates["G7"]["limitations"].append(
                "This fault probe kills the isolated App Server after the fake gateway observes the request; the unresolved call remains pending and the profile does not resume the same execution."
            )
            report["observations"]["transport_disconnect"] = {
                "turn": transport_turn,
                "delta": transport_delta,
                "gateway_check": transport_check,
                "execution_unknown_without_retry": transport_unknown,
            }

            turns[transport_operation] = transport_turn
            provider_snapshot = provider.snapshot()
            expected_pending_call_ids = {
                check["accounting_call_id"]
                for check in provider_snapshot["checks"]
                if check.get("authorized") and
                check.get("operation_id") in {retry_operation, transport_operation} and
                check.get("accounting_call_id") and
                any(
                    request.get("accounting_call_id") == check["accounting_call_id"] and
                    request.get("behavior") in {"error", "disconnect"}
                    for request in provider_snapshot["model_requests"]
                )
            }
            usage_reconciliation = reconcile_fake_usage(
                provider_snapshot, turns, expected_pending_call_ids,
            )
            provider_error_unknown_linked = any(
                row["operation_id"] == retry_operation and
                row["settlement"] == "EXPECTED_PENDING"
                for row in usage_reconciliation["calls"]
            )
            usage_reconciliation["provider_error_unknown_linked"] = provider_error_unknown_linked
            transport_pending_linked = any(
                row["accounting_call_id"] == transport_check.get("accounting_call_id") and
                row["settlement"] == "EXPECTED_PENDING"
                for row in usage_reconciliation["calls"]
            )
            usage_reconciliation["transport_pending_linked"] = transport_pending_linked
            gates["G9"]["runtime_test_status"] = (
                "pass" if evaluate_g9(usage_reconciliation) and provider_error_unknown_linked and transport_pending_linked
                else "blocked"
            )
            gates["G9"]["guarantees"] = [
                "The prior loss was in bridge summarizeSdkMessage, which replaced every SDK message's usage with UNKNOWN; SDK 0.8.25 carries the stream_event.event payload. The bridge now normalizes that payload and preserves accounting call IDs.",
                f"All successful authorized turn and compaction calls matched fake usage and provider response IDs; G9 evaluation={evaluate_g9(usage_reconciliation)}.",
                f"Provider HTTP 500 is linked as EXPECTED_PENDING, without claiming settlement={provider_error_unknown_linked}.",
                f"Post-dispatch App Server process loss is UNKNOWN with no automatic retry, and its call is EXPECTED_PENDING by gateway accounting ID={transport_unknown and transport_pending_linked}.",
                f"Runtime requests={usage_reconciliation['counts']['runtime_provider_requests']}; non-runtime gateway checks={usage_reconciliation['counts']['non_runtime_gateway_checks']}; direct gateway provider requests={usage_reconciliation['counts']['direct_gateway_provider_requests']}; gateway denials={usage_reconciliation['counts']['gateway_denials']}; permits issued/consumed={usage_reconciliation['counts']['permits_issued']}/{usage_reconciliation['counts']['permits_consumed']}; total provider endpoint requests={usage_reconciliation['counts']['provider_endpoint_requests']}.",
                "Pinned SDK source defers terminal results for a 100 ms trailing-usage grace after stop_reason.",
                f"Per-call rows={len(usage_reconciliation['calls'])}; pending or unsettled calls={usage_reconciliation['unsettled_call_count']}; blocking missing/mismatch/forwarding-unknown calls={usage_reconciliation['blocking_call_count']}.",
            ]
            gates["G9"]["limitations"][0] = (
                "The fixture is synthetic, not provider billing evidence. HTTP 500 and transport-loss calls remain EXPECTED_PENDING because no usage was delivered; neither is reported as settled. The 100 ms SDK trailing-usage behavior is source-checked only: this local runtime emits usage before stop_reason, while that grace path is for hosted streams that send usage after stop_reason. events.collect exposes records after same-call merge, so raw late/duplicate SDK deliveries are not counted by this probe."
            )
            report["observations"]["usage_reconciliation"] = usage_reconciliation
            report["observations"]["fake_provider"] = provider_snapshot
            report["observations"]["usage_delivery_profile"]["delayed_after_stop_runtime_probe"] = (
                "not_applicable_local_runtime_emits_usage_before_stop_reason; hosted_stream_grace_source_checked"
            )
        except Exception as error:
            report["error"] = f"{type(error).__name__}: {error}"
            for gate in gates.values():
                if gate["runtime_test_status"] == "not_run":
                    gate["limitations"].append("Probe stopped before this runtime path ran.")
        finally:
            if structured_bridge is not None:
                structured_bridge.close()
            if bridge is not None:
                bridge.close()
            if agent_id and provider is not None and sandbox.port:
                # The bridge's normal path deletes the tagged test agent. If a probe failed,
                # container teardown still discards the only test state directory.
                pass
            sandbox.stop()
            if provider is not None:
                provider.stop()
            sandbox.clear_state()

    report["overall_status"] = (
        "pass"
        if all(gate["runtime_test_status"] == "pass" for gate in gates.values())
        else "blocked"
    )
    return report


def write_reports(report: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_name = f"{report['run_id']}.json"
    artifact_path = output_dir / "artifacts" / artifact_name
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    report["evidence_artifact"] = str(artifact_path.relative_to(ROOT))
    matrix = {
        "schema_version": "1",
        "run_id": report["run_id"],
        "executed_at": report["executed_at"],
        "selected_profile": report["profile"],
        "overall_status": report["overall_status"],
        "gates": {
            gate_id: {
                **gate,
                "evidence_artifacts": [str(artifact_path.relative_to(ROOT))],
            }
            for gate_id, gate in report["gates"].items()
        },
        "fake_provider_counts": {
            key: report.get("observations", {}).get("fake_provider", {}).get(key)
            for key in (
                "provider_request_count",
                "permits_issued",
                "permits_consumed",
                "denied_attempts",
                "gateway_attempts",
            )
        },
        "real_provider_calls": report["real_provider_calls"],
        "estimated_real_provider_cost_usd": report["estimated_real_provider_cost_usd"],
        "evidence_artifacts": [str(artifact_path.relative_to(ROOT))],
        "error": report.get("error"),
    }
    (output_dir / "capability-matrix.json").write_text(
        json.dumps(matrix, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifact_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(matrix, ensure_ascii=False, indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe pinned Letta App Server gates using a local fake provider.")
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "integration/letta")
    args = parser.parse_args()
    report = probe(args.image)
    write_reports(report, args.output_dir)
    return 0 if report["overall_status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
