from __future__ import annotations

import argparse
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
APP_PORT = 4500


class ProbeError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def run(command: list[str], *, timeout: int = 60) -> str:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode:
        detail = (completed.stderr or completed.stdout).strip()[-1_500:]
        raise ProbeError(f"{Path(command[0]).name} failed ({completed.returncode}): {detail}")
    return completed.stdout.strip()


def content_length(text: Any) -> int:
    if isinstance(text, str):
        return len(text)
    if isinstance(text, list):
        return sum(content_length(item) for item in text)
    if isinstance(text, dict):
        return sum(content_length(value) for value in text.values())
    return 0


class PermitLedger:
    """Probe-only one-use permits; this is not an operational budget ledger."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.pending: deque[dict[str, str]] = deque()
        self.issued = 0
        self.consumed = 0
        self.denied = 0
        self.checks: list[dict[str, Any]] = []

    def issue(self, operation_id: str) -> None:
        with self.lock:
            self.issued += 1
            self.pending.append({
                "permit_id": f"probe-permit-{self.issued}",
                "operation_id": operation_id,
            })

    def consume(self) -> dict[str, str] | None:
        with self.lock:
            if not self.pending:
                self.denied += 1
                self.checks.append({"authorized": False, "operation_id": None})
                return None
            permit = self.pending.popleft()
            self.consumed += 1
            self.checks.append({
                "authorized": True,
                "operation_id": permit["operation_id"],
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
            }


class FakeProvider:
    def __init__(self, gateway_address: str) -> None:
        self.ledger = PermitLedger()
        self.lock = threading.Lock()
        self.behaviors: deque[str] = deque()
        self.model_requests: list[dict[str, Any]] = []
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
                    owner.model_requests.append({
                        "sequence": len(owner.model_requests) + 1,
                        "model": request.get("model"),
                        "max_tokens": request.get("max_tokens"),
                        "max_completion_tokens": request.get("max_completion_tokens"),
                        "stream": request.get("stream"),
                        "estimated_input_tokens": max(
                            1,
                            sum(content_length(message.get("content")) for message in request.get("messages", [])) // 4,
                        ),
                        "tool_names": [
                            tool.get("function", {}).get("name", "")
                            for tool in request.get("tools", [])
                            if isinstance(tool, dict)
                        ],
                    })
                    behavior = owner.behaviors.popleft() if owner.behaviors else "normal"
                    call_id = f"fake-call-{len(owner.model_requests):04d}"
                    owner.model_requests[-1]["provider_call_id"] = call_id
                    owner.model_requests[-1]["behavior"] = behavior
                if behavior == "error":
                    self.respond(500, {"error": {"message": "probe provider error", "type": "server_error"}})
                    return
                prompt_tokens = owner.model_requests[-1]["estimated_input_tokens"]
                with owner.lock:
                    owner.model_requests[-1]["fake_usage"] = {
                        "prompt_tokens": prompt_tokens,
                        "completion_tokens": 2,
                        "total_tokens": prompt_tokens + 2,
                    }
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
                            "usage": {
                                "prompt_tokens": prompt_tokens,
                                "completion_tokens": 2,
                                "total_tokens": prompt_tokens + 2,
                            },
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
                    "usage": {
                        "prompt_tokens": prompt_tokens,
                        "completion_tokens": 2,
                        "total_tokens": prompt_tokens + 2,
                    },
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

            def forward(self, method: str, body: bytes = b"") -> None:
                target = f"http://127.0.0.1:{owner.backend.server_port}{self.path}"
                request = Request(
                    target,
                    data=body if method != "GET" else None,
                    method=method,
                    headers={"content-type": self.headers.get("content-type", "application/json")},
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
                permit = owner.ledger.consume()
                if permit is None:
                    self.respond(402, b'{"error":{"message":"no unused probe permit","type":"hekate_permit_denied"}}')
                    return
                self.forward("POST", body)

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
            self.behaviors.append(behavior)

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
    def __init__(self, image: str, network: str, app_server: str, token: str) -> None:
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
                "--env", "HEKATE_TOOL_BOUNDARY_PROBE=1",
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
) -> dict[str, Any]:
    operation = operation_id(run_id, name)
    sent = bridge.request(command(
        run_id, name, "session.turn", binding=binding, message=message,
    ))
    if sent["status"] not in {"CONFIRMED", "UNKNOWN"}:
        raise ProbeError(f"session.turn returned {sent['status']}: {sent.get('error', 'no detail')}")
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
    return {
        "dispatch_status": sent["status"],
        "dispatch_state": (sent.get("result") or {}).get("state"),
        "event_status": response["status"],
        "state": result.get("state", "UNKNOWN"),
        "event_types": [event.get("event_type") for event in result.get("events", [])],
        "error_codes": [
            event.get("error_code")
            for event in result.get("events", [])
            if event.get("error_code") is not None
        ],
        "usage_completeness": [
            event.get("usage", {}).get("completeness")
            for event in result.get("events", [])
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


def probe(image: str) -> dict[str, Any]:
    run_id = "p1-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    report = base_report(run_id, image)
    gates = report["gates"]
    agent_id: str | None = None

    with tempfile.TemporaryDirectory(prefix=f"{run_id}-") as temporary:
        workdir = Path(temporary)
        sandbox = DockerSandbox(workdir, run_id, image)
        provider: FakeProvider | None = None
        bridge: BridgeProcess | None = None
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

            provider.ledger.issue(operation_id(run_id, "tool-turn"))
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
            tool_attempt_observed = tool_turn["blocked_tool_attempts"] > 0
            no_tool_execution = tool_turn["tool_executor_calls"] == 0
            no_unexpected_tools = set(prepared.get("agent_tools", [])) <= {"hekate_forbidden_probe"}
            no_unexpected_tools &= set(tool_request.get("tool_names", [])) <= {"hekate_forbidden_probe"}
            gates["G5"]["source_status"] = "pass"
            gates["G5"]["runtime_test_status"] = (
                "pass" if tool_attempt_observed and no_tool_execution and no_unexpected_tools else "blocked"
            )
            gates["G5"]["guarantees"] = [
                "Agent creation passed baseTools:[]; the session used the default client toolset constrained to one SDK allowlist entry and an empty bridge executor allowlist.",
                f"Provider advertised tool names: {tool_request.get('tool_names', [])}; executor calls: "
                f"{tool_turn['tool_executor_calls']}; guarded denied attempts: {tool_turn['blocked_tool_attempts']}.",
            ]
            gates["G5"]["limitations"] = [
                "Only the inert test probe tool is exercised; production client-tool dispatch is not enabled by this phase."
            ]

            _, normal_binding = prepare_session(
                bridge, run_id, "normal-inference", agent_id, registry_id,
            )
            normal_operation = operation_id(run_id, "normal-turn")
            normal_before = provider.snapshot()
            provider.ledger.issue(normal_operation)
            normal_turn = run_turn(
                bridge, run_id, "normal-turn", normal_binding,
                "Return a brief confirmation.",
            )
            normal_after = provider.snapshot()
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
            provider.ledger.issue(retry_operation)
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
            provider.ledger.issue(empty_operation)
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

            all_provider_requests = provider.snapshot()["model_requests"]
            normal_request = next(
                (item for item in reversed(all_provider_requests) if item["behavior"] == "normal"),
                {},
            )
            output_limit_observed = normal_request.get("max_tokens") == 64 or normal_request.get("max_completion_tokens") == 64
            input_limit_observed = (observed.get("model_settings") or {}).get("context_window_limit") == 16384
            model_observed = normal_request.get("model") == FAKE_MODEL

            gates["G2"]["runtime_test_status"] = (
                "pass"
                if observed["present"] and agent_id in listed["provider_agent_ids"]
                else "fail"
            )
            gates["G2"]["limitations"] = [
                "Creation-response-loss recovery is deferred to G3.",
            ]

            g6_pass = (
                normal_delta["provider_requests"] == normal_delta["permits_issued"] == normal_delta["permits_consumed"] == 1
                and normal_turn["state"] == "COMPLETE"
                and retry_delta["provider_requests"] == retry_delta["permits_issued"] == retry_delta["permits_consumed"]
                and empty_delta["provider_requests"] == empty_delta["permits_issued"] == empty_delta["permits_consumed"]
                and denied_delta["provider_requests"] == 0
                and denied_delta["permits_consumed"] == 0
                and denied_delta["denied_attempts"] >= 1
            )
            gates["G6"]["source_status"] = "pass"
            gates["G6"]["runtime_test_status"] = "pass" if g6_pass else "fail"
            gates["G6"]["guarantees"] = [
                "The local App Server used an isolated Docker network with --internal and its only model provider was the fake-provider gateway.",
                "Every fake inference forwarded to the model endpoint consumed exactly one test-only permit; unpermitted attempts were denied before the fake model endpoint.",
                f"Observed normal turn: {normal_delta}; provider-error path: {retry_delta}; empty-response path: {empty_delta}; zero-permit turn: {denied_delta}.",
            ]
            gates["G6"]["limitations"] = [
                "This is a probe-only egress gate and permit source, not the production atomic budget ledger.",
                "Compaction was not triggered; source trace shows compaction calls use LocalPiModelsRuntime.streamSimple and therefore the same configured provider endpoint.",
                "No real provider credentials or models were used.",
            ]

            retry_blocked = any(
                delta["gateway_attempts"] > delta["provider_requests"] and delta["denied_attempts"] >= 1
                for delta in (retry_delta, empty_delta)
            )
            gates["G7"]["source_status"] = "pass"
            gates["G7"]["runtime_test_status"] = "blocked"
            gates["G7"]["guarantees"] = [
                f"One-permit retry enforcement observed={retry_blocked}. Provider-error path: {retry_delta}; empty-response path: {empty_delta}.",
            ]
            gates["G7"]["limitations"] = [
                "The profile did not enable SDK structured-output repair; transport reconnection was not exercised, so the full gate remains blocked.",
                "The external gate enforces one model request per issued permit; it does not claim a Letta step limit."
            ]

            gates["G8"]["source_status"] = "pass"
            gates["G8"]["runtime_test_status"] = (
                "blocked" if model_observed and output_limit_observed and input_limit_observed else "fail"
            )
            gates["G8"]["guarantees"] = [
                f"Agent readback contained context_window_limit={(observed.get('model_settings') or {}).get('context_window_limit')!r} and max_tokens={(observed.get('model_settings') or {}).get('max_tokens')!r}; fake provider observed model={normal_request.get('model')!r}, max_tokens="
                f"{normal_request.get('max_tokens')!r}, max_completion_tokens="
                f"{normal_request.get('max_completion_tokens')!r}.",
            ]
            gates["G8"]["limitations"] = [
                "The input limit maps to App Server context_window_limit, not a provider request field; the fake provider cannot independently attest the exact tokenizer count, so exact input-cap enforcement remains blocked."
            ]

            usage_observed = any(
                event == "result" for event in normal_turn["event_types"]
            ) and "UNKNOWN" in normal_turn["usage_completeness"]
            gates["G9"]["source_status"] = "blocked"
            gates["G9"]["runtime_test_status"] = "fail" if usage_observed else "blocked"
            gates["G9"]["guarantees"] = [
                "The fake provider returned a call ID and usage values in its OpenAI-compatible response.",
                "The bridge event stream reported usage completeness UNKNOWN and did not expose provider_call_id or token quantities.",
            ]
            gates["G9"]["limitations"] = [
                "Error, cancellation, duplicate-terminal, and compaction usage reconciliation remain unverified."
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
                    "provider_request_count": fake_after_tool["provider_request_count"],
                },
                "normal_turn": {"turn": normal_turn, "delta": normal_delta},
                "provider_error_retry": {"turn": retry_turn, "delta": retry_delta},
                "empty_response_retry": {"turn": empty_turn, "delta": empty_delta},
                "zero_permit_turn": {"turn": denied_turn, "delta": denied_delta},
                "fake_provider": provider.snapshot(),
                "network_internal": True,
                "node_version": node_version,
            }

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
                "deleted_present": deletion["present"],
                "post_delete_present": after_delete["present"],
            })
        except Exception as error:
            report["error"] = f"{type(error).__name__}: {error}"
            for gate in gates.values():
                if gate["runtime_test_status"] == "not_run":
                    gate["limitations"].append("Probe stopped before this runtime path ran.")
        finally:
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
