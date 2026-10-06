#!/usr/bin/env python3
"""Exercise init-local/doctor/gateway/worker/ask/task through the ordinary CLI.

This is an integration driver only: it creates isolated services, invokes CLI
commands, and reads PostgreSQL/runtime observations. It never calls Task or
result application functions to manufacture a successful outcome.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from typing import Any, Callable
from urllib.request import Request, urlopen

import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import phase3_runtime_probe as p3  # noqa: E402
import phase3b_single_hekate_probe as p3b  # noqa: E402
import phase6c_ollama_qwen_probe as p6c  # noqa: E402
from hekate.infrastructure.letta.qwen_ollama import (  # noqa: E402
    load_qwen_candidate_profile,
    qwen35_native_json_schema_test_execution_profile,
)

LOCAL_EXAMPLE = ROOT / "config/local.example"
ARTIFACTS = ROOT / "integration/runtime/artifacts"
GLOBAL_REAL_RUN_GUARD = ROOT / ".hekate-local/phase6e-real-generation-authorization.json"
DEFAULT_IMAGE = "hekate/letta-code-p1:0bb6f741-70e39563"
CLI_OBSERVATIONS: list[dict[str, object]] = []


def now_utc() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def free_port() -> int:
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return int(stream.getsockname()[1])


def run(command: list[str], *, env: dict[str, str] | None = None, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command, cwd=ROOT, env=env, capture_output=True, text=True,
        check=False, timeout=timeout,
    )


def require_ok(result: subprocess.CompletedProcess[str], label: str) -> str:
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed ({result.returncode}): {redact((result.stderr or result.stdout)[-700:])}")
    return result.stdout.strip()


def redact(value: str) -> str:
    value = re.sub(r"(?i)(postgresql(?:\+psycopg)?://)[^\s/@]+@", r"\1[redacted]@", value)
    value = re.sub(r"(?i)(bearer\s+)[^\s]+", r"\1[redacted]", value)
    for key in ("HEKATE_LETTA_TOKEN", "HEKATE_PROVIDER_GATEWAY_TOKEN", "HEKATE_DATABASE_URL"):
        secret = os.environ.get(key)
        if secret:
            value = value.replace(secret, "[redacted]")
    return value


def cli(env: dict[str, str], *args: str, stdin: str | None = None, timeout: int = 60) -> tuple[dict[str, Any], str]:
    started = time.monotonic()
    result = subprocess.run(
        [sys.executable, "-m", "hekate", *args], cwd=ROOT, env=env,
        input=stdin, capture_output=True, text=True, check=False, timeout=timeout,
    )
    if result.returncode != 0:
        CLI_OBSERVATIONS.append({
            "argv": ["hekate", *args], "exit_code": result.returncode,
            "stdin_sha256": hashlib.sha256((stdin or "").encode("utf-8")).hexdigest() if stdin is not None else None,
            "elapsed_seconds": round(time.monotonic() - started, 3), "result": "ERROR",
        })
        raise RuntimeError(f"hekate {' '.join(args[:1])} failed ({result.returncode}): {redact((result.stderr or result.stdout)[-600:])}")
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        raise RuntimeError("CLI returned a non-object JSON response")
    task = value.get("task")
    CLI_OBSERVATIONS.append({
        "argv": ["hekate", *args], "exit_code": result.returncode,
        "stdin_sha256": hashlib.sha256((stdin or "").encode("utf-8")).hexdigest() if stdin is not None else None,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "task_id": task.get("task_id") if isinstance(task, dict) else None,
        "task_state": task.get("state") if isinstance(task, dict) else None,
        "timed_out": value.get("timed_out"),
        "result": "OK",
    })
    return value, result.stderr[-500:]


def write_yaml(path: Path, value: object) -> None:
    path.write_text(yaml.safe_dump(value, sort_keys=False, allow_unicode=True), encoding="utf-8")


def local_config(
    run_dir: Path, name: str, scope: str, db_url: str, worker_id: str, gateway_port: int,
) -> tuple[Path, Path, str, str]:
    config_dir = run_dir / f"config-{name}"
    state_dir = run_dir / f"state-{name}"
    config_dir.mkdir(parents=True)
    for filename in ("local.yaml", "models.yaml", "policy.yaml", "pricing.yaml"):
        shutil.copyfile(LOCAL_EXAMPLE / filename, config_dir / filename)
    local = yaml.safe_load((config_dir / "local.yaml").read_text(encoding="utf-8"))
    local["identity"]["scope_id"] = scope
    local["identity"]["principal_id"] = f"{name}-operator"
    local["runtime"]["worker_id"] = worker_id
    local["paths"]["state_dir"] = str(state_dir)
    local["paths"]["archive_dir"] = str(state_dir / "archive")
    local["gateway"]["port"] = gateway_port
    write_yaml(config_dir / "local.yaml", local)
    token = secrets.token_hex(32)
    app_token = secrets.token_hex(32)
    return config_dir, state_dir, token, app_token


def base_env(
    config_dir: Path, db_url: str, worker_id: str, gateway_token: str, app_token: str,
    node_bin: str, letta_port: int, runtime_mode: str = "local",
) -> dict[str, str]:
    env = {
        **os.environ,
        "HEKATE_CONFIG_DIR": str(config_dir),
        "HEKATE_PROJECT_DIR": str(ROOT),
        "HEKATE_DATABASE_URL": db_url,
        "HEKATE_RUNTIME_MODE": runtime_mode,
        "HEKATE_WORKER_ID": worker_id,
        "HEKATE_NODE_BIN": node_bin,
        "HEKATE_BRIDGE_ENTRY": str(ROOT / "bridge/letta/dist/main.js"),
        "HEKATE_LETTA_URL": f"ws://127.0.0.1:{letta_port}",
        "HEKATE_LETTA_TOKEN": app_token,
        "HEKATE_PROVIDER_GATEWAY_TOKEN": gateway_token,
        "HEKATE_MEMORY_PROJECTION_ENABLED": "false",
    }
    # A shell from another probe must not accidentally install a stale
    # one-shot file in either the synthetic or local gateway process.
    env.pop("HEKATE_LOCAL_GENERATION_LEDGER", None)
    return env


def install_fake_profile(config_dir: Path, fake_port: int) -> None:
    candidate = load_qwen_candidate_profile()
    execution_profile, prices = qwen35_native_json_schema_test_execution_profile(candidate)
    local = yaml.safe_load((config_dir / "local.yaml").read_text(encoding="utf-8"))
    local["gateway"]["upstream_base_url"] = f"http://127.0.0.1:{fake_port}"
    local["gateway"]["upstream_api_key"] = "isolated-fake-only"
    write_yaml(config_dir / "local.yaml", local)
    write_yaml(config_dir / "models.yaml", {"version": "phase6e-fake-v1", "hekate": {
        "profile_id": execution_profile.profile_id,
        "model": f"openai-compatible/{execution_profile.model}",
        "provider_model": execution_profile.model,
        "model_revision": execution_profile.model_revision,
        "execution_profile": "qwen35_native_json_schema_test_v2",
        "context_window_tokens": execution_profile.context_window_tokens,
        "letta_context_estimator_tokens": candidate.letta_context_estimator_tokens,
        "max_input_tokens": execution_profile.max_input_tokens,
        "max_output_tokens": execution_profile.max_output_tokens,
        "max_compaction_calls": 0,
        "agent_system_prompt": candidate.agent_system_prompt,
    }})
    write_yaml(config_dir / "pricing.yaml", {
        "version": prices.version,
        "effective_at": prices.effective_at,
        "prices": {execution_profile.model: {
            "input_usd_per_million": str(prices.input_usd_per_million),
            "output_usd_per_million": str(prices.output_usd_per_million),
        }},
    })


def start_process(command: list[str], env: dict[str, str], log_path: Path) -> subprocess.Popen[bytes]:
    log_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    stream = log_path.open("ab", buffering=0)
    os.chmod(log_path, 0o600)
    try:
        return subprocess.Popen(command, cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
                                stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
    finally:
        stream.close()


def stop_process(process: subprocess.Popen[bytes] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=12)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def http_json(url: str, token: str | None = None, timeout: int = 3) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with urlopen(Request(url, headers=headers), timeout=timeout) as response:
        value = json.loads(response.read(1_048_577).decode("utf-8", "strict"))
    if not isinstance(value, dict):
        raise ValueError("local service returned a non-object JSON response")
    return value


def wait_http(url: str, token: str | None = None, timeout: int = 45) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return http_json(url, token)
        except Exception as error:
            last = error
            time.sleep(0.25)
    raise TimeoutError(f"local service did not become ready ({type(last).__name__ if last else 'timeout'})")


def start_gateway(env: dict[str, str], state_dir: Path, label: str) -> subprocess.Popen[bytes]:
    process = start_process([sys.executable, "-m", "hekate", "gateway"], env, state_dir / f"{label}-gateway.log")
    port = int(yaml.safe_load((Path(env["HEKATE_CONFIG_DIR"]) / "local.yaml").read_text())[
        "gateway"]["port"])
    try:
        health = wait_http(f"http://127.0.0.1:{port}/healthz")
        if health.get("status") != "ok":
            raise RuntimeError("provider gateway health check failed")
        http_json(f"http://127.0.0.1:{port}/v1/models", env["HEKATE_PROVIDER_GATEWAY_TOKEN"])
        return process
    except BaseException:
        stop_process(process)
        raise


def verify_image(image: str) -> dict[str, str]:
    lock = json.loads((ROOT / "integration/letta/versions.lock.json").read_text(encoding="utf-8"))
    labels = json.loads(require_ok(run([
        "docker", "image", "inspect", "--format", "{{json .Config.Labels}}", image,
    ]), "Docker image inspection"))
    image_id = require_ok(run(["docker", "image", "inspect", "--format", "{{.Id}}", image]), "Docker image ID inspection")
    base_ref = f"{lock['app_server']['image']}@{lock['app_server']['image_digest']}"
    base_id = require_ok(run(["docker", "image", "inspect", "--format", "{{.Id}}", base_ref]), "pinned base image inspection")
    patch_path = ROOT / lock["patches"][0]["file"]
    patch_digest = hashlib.sha256(patch_path.read_bytes()).hexdigest()
    if (
        labels.get("org.opencontainers.image.revision") != lock["app_server"]["source_commit"]
        or labels.get("io.hekate.runtime-patch.sha256") != lock["patches"][0]["sha256"]
        or patch_digest != lock["patches"][0]["sha256"]
        or base_id != lock["app_server"]["image_digest"]
    ):
        raise RuntimeError("pinned App Server image, source, or patch identity differs from versions.lock.json")
    return {
        "image": image,
        "image_id": image_id,
        "base_image": base_ref,
        "base_image_id": base_id,
        "source_commit": lock["app_server"]["source_commit"],
        "runtime_patch_sha256": patch_digest,
        "app_server_version": lock["app_server"]["letta_code_version"],
        "sdk_version": lock["bridge"]["sdk"]["version"],
        "protocol_version": lock["app_server"]["protocol_version"],
    }


def start_app_server(
    image: str, container_name: str, state_dir: Path, app_token: str,
    gateway_port: int, gateway_token: str, letta_port: int,
) -> None:
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    token_path = state_dir / "app-server-token"
    token_path.write_text(app_token, encoding="utf-8")
    os.chmod(token_path, 0o600)
    command = [
        "docker", "run", "--detach", "--rm", "--name", container_name,
        "--network", "host",
        "--env", "HEKATE_REQUIRE_PROVIDER_BINDING=1",
        "--env", f"HEKATE_PROJECTION_GUARD_URL=http://127.0.0.1:{gateway_port}/internal/memory-projection/authorize",
        "--env", f"HEKATE_PROJECTION_GUARD_TOKEN={gateway_token}",
        "--mount", f"type=bind,source={state_dir},target=/root/.letta",
        "--mount", f"type=bind,source={token_path},target=/run/secrets/hekate-ws-token,readonly",
        image, "letta", "--backend", "local", "server", "--listen", f"ws://127.0.0.1:{letta_port}",
        "--ws-auth", "capability-token", "--ws-token-file", "/run/secrets/hekate-ws-token",
    ]
    require_ok(run(command, timeout=90), "pinned Letta App Server start")
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        status = run(["docker", "inspect", "--format", "{{.State.Running}}", container_name], timeout=10)
        if status.returncode != 0 or status.stdout.strip() != "true":
            logs = run(["docker", "logs", "--tail", "30", container_name], timeout=10)
            raise RuntimeError(f"pinned Letta App Server exited: {(logs.stdout + logs.stderr)[-1000:]}")
        logs = run(["docker", "logs", "--tail", "30", container_name], timeout=10)
        if "Listening on ws://" in (logs.stdout + logs.stderr):
            return
        time.sleep(0.5)
    raise TimeoutError("pinned Letta App Server did not report its loopback listener")


def stop_app_server(container_name: str) -> None:
    run(["docker", "stop", "--time", "10", container_name], timeout=20)


def start_worker(env: dict[str, str], state_dir: Path, label: str) -> subprocess.Popen[bytes]:
    process = start_process([sys.executable, "-m", "hekate", "worker"], env, state_dir / f"{label}-worker.log")
    time.sleep(0.4)
    if process.poll() is not None:
        raise RuntimeError(f"hekate worker exited during startup; inspect {state_dir / f'{label}-worker.log'}")
    return process


def wait_doctor_ready(env: dict[str, str], timeout: int = 60) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        result = run([sys.executable, "-m", "hekate", "doctor"], env=env, timeout=20)
        try:
            last = json.loads(result.stdout)
        except json.JSONDecodeError:
            last = {"status": "INVALID_JSON", "exit_code": result.returncode}
        if result.returncode == 0 and last.get("status") == "READY":
            return last
        time.sleep(1)
    return last


def write_guard(path: Path, value: dict[str, Any], *, exclusive: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    flags = os.O_WRONLY | os.O_CREAT | (os.O_EXCL if exclusive else os.O_TRUNC)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _json_safe(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if value.__class__.__name__ == "Decimal":
        return str(value)
    return value


def db_read(engine, query: str, params: dict[str, object] | None = None) -> list[dict[str, object]]:
    with engine.connect() as connection:
        rows = connection.execute(text(query), params or {}).mappings().all()
    return [_json_safe(dict(row)) for row in rows]


def db_meta(engine) -> dict[str, object]:
    return {
        "postgres_version": db_read(engine, "SHOW server_version")[0]["server_version"],
        "migration_head": db_read(engine, "SELECT version_num FROM alembic_version")[0]["version_num"],
    }


def counts(engine, scope_id: str) -> dict[str, int]:
    row = db_read(engine, """
        SELECT
          (SELECT count(*) FROM authorization_scopes) AS scopes,
          (SELECT count(*) FROM tasks WHERE owner_scope=:scope) AS tasks,
          (SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.owner_scope=:scope) AS provider_calls,
          (SELECT count(*) FROM call_permits cp JOIN provider_calls p USING(accounting_call_id)
             JOIN operations o ON o.id=p.operation_id WHERE o.owner_scope=:scope) AS permits,
          (SELECT count(*) FROM task_responses r JOIN tasks t ON t.id=r.task_id WHERE t.owner_scope=:scope) AS responses
    """, {"scope": scope_id})[0]
    return {key: int(value) for key, value in row.items()}


def task_db_snapshot(engine, task_id: str) -> dict[str, object]:
    task = db_read(engine, "SELECT id, owner_scope, input_revision, status, outcome, stop_reason, provider_calls FROM tasks WHERE id=:task", {"task": task_id})
    calls = db_read(engine, """
        SELECT p.accounting_call_id, p.operation_id, p.attempt_id, p.registry_id,
               p.model, p.status AS call_status, p.execution_mode, p.price_synthetic,
               p.measurement_status, p.request_digest, p.profile_digest, p.measurement_data,
               p.provider_call_id, cp.permit_id, cp.state AS permit_state,
               up.completeness AS usage_completeness, up.settlement_state,
               up.input_tokens, up.output_tokens, up.total_tokens,
               up.evaluated_cost_usd, up.reported_cost_usd
        FROM provider_calls p
        JOIN operations o ON o.id=p.operation_id
        LEFT JOIN call_permits cp ON cp.permit_id=p.permit_id
        LEFT JOIN usage_projections up ON up.accounting_call_id=p.accounting_call_id
        WHERE o.task_id=:task
        ORDER BY p.created_at, p.accounting_call_id
    """, {"task": task_id})
    response = db_read(engine, "SELECT operation_id, attempt_id, registry_id, response_text, outcome, stop_reason, proposal FROM task_responses WHERE task_id=:task", {"task": task_id})
    ledger = db_read(engine, """
        SELECT l.effect_type, count(*) AS entries,
               coalesce(sum(l.held_delta),0) AS held_delta,
               coalesce(sum(l.spent_delta),0) AS spent_delta
        FROM budget_ledger l JOIN budget_reservations r ON r.id=l.reservation_id
        JOIN operations o ON o.id=r.operation_id WHERE o.task_id=:task
        GROUP BY l.effect_type ORDER BY l.effect_type
    """, {"task": task_id})
    return {"task": task[0] if task else None, "calls": calls, "responses": response, "ledger": ledger}


def db_counts_for_doctor(engine, scope_id: str) -> dict[str, int]:
    return counts(engine, scope_id)


def capture_summaries(paths: dict[str, Path]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, path in paths.items():
        if not path.is_file():
            result[name] = {"present": False, "path": str(path.relative_to(ROOT))}
            continue
        raw = path.read_bytes()
        result[name] = {
            "present": True,
            "path": str(path.relative_to(ROOT)),
            "utf8_bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "bounded_at_or_below_65536_bytes": len(raw) <= 65_536,
        }
    return result


def configure_pg(run_id: str) -> tuple[str, int, str, str]:
    password = secrets.token_hex(24)
    port = free_port()
    container = f"hekate-p6e-{run_id[-8:]}"
    volume = f"hekate-p6e-{run_id[-8:]}"
    require_ok(run(["docker", "volume", "create", volume]), "PostgreSQL volume creation")
    require_ok(run([
        "docker", "run", "--detach", "--name", container,
        "--publish", f"127.0.0.1:{port}:5432",
        "--env", "POSTGRES_USER=hekate", "--env", f"POSTGRES_PASSWORD={password}",
        "--env", "POSTGRES_DB=postgres", "--volume", f"{volume}:/var/lib/postgresql/data",
        "postgres:16.15",
    ], timeout=120), "PostgreSQL start")
    db_url = f"postgresql+psycopg://hekate:{password}@127.0.0.1:{port}"
    plain_url = f"postgresql+psycopg://hekate:{password}@127.0.0.1:{port}/postgres"
    admin_engine = create_engine(plain_url)
    deadline = time.monotonic() + 90
    try:
        while time.monotonic() < deadline:
            try:
                with admin_engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                    connection.commit()
                break
            except Exception:
                time.sleep(0.5)
        else:
            raise TimeoutError("new isolated PostgreSQL did not become ready")
        with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text("CREATE DATABASE hekate_p6e_fake"))
            connection.execute(text("CREATE DATABASE hekate_p6e_real"))
    finally:
        admin_engine.dispose()
    return db_url, port, container, volume


def db_url_for(base: str, db_name: str) -> str:
    parsed = make_url(base)
    return parsed.set(database=db_name).render_as_string(hide_password=False)


def run_migration_checks(env: dict[str, str]) -> dict[str, object]:
    down = run([sys.executable, "-m", "alembic", "downgrade", "0013_provider_token_measurement"], env=env, timeout=90)
    require_ok(down, "isolated empty database downgrade")
    up = run([sys.executable, "-m", "alembic", "upgrade", "head"], env=env, timeout=90)
    require_ok(up, "isolated empty database re-upgrade")
    check = run([sys.executable, "-m", "alembic", "check"], env=env, timeout=90)
    require_ok(check, "Alembic metadata check")
    return {"downgrade_to_0013": "PASS", "reupgrade_to_head": "PASS", "metadata_check": "PASS"}


def run_fake_preflight(
    run_id: str, run_dir: Path, db_base_url: str, node_bin: str, image: str,
) -> dict[str, object]:
    database_url = db_url_for(db_base_url, "hekate_p6e_fake")
    scope = f"p6e-fake-{run_id[-12:]}"
    worker_id = f"p6e-fake-worker-{run_id[-8:]}"
    gateway_port, letta_port = free_port(), free_port()
    config_dir, state_dir, gateway_token, app_token = local_config(
        run_dir, "fake", scope, database_url, worker_id, gateway_port,
    )
    env = base_env(config_dir, database_url, worker_id, gateway_token, app_token, node_bin, letta_port)
    engine = create_engine(database_url)
    fake = p3.FakeProvider()
    fake.set_response_factory(lambda request: p3b._output_from_request(request))
    original_fake_model = p3.FAKE_MODEL
    p3.FAKE_MODEL = load_qwen_candidate_profile().model
    gateway_process = worker_process = None
    container_name = f"hekate-p6e-fake-{run_id[-8:]}"
    result: dict[str, object] = {"status": "BLOCKED", "scope_id": scope, "task_id": None}
    try:
        fake.start()
        init_first, _ = cli(env, "init-local")
        result["init_local_first"] = init_first
        result["postgres"] = db_meta(engine)
        if init_first.get("scope_created") is not True:
            raise AssertionError("fresh fake scope was not created by init-local")
        result["migration_checks"] = run_migration_checks(env)
        before_replay = db_counts_for_doctor(engine, scope)
        auth_path = state_dir / "letta/lc-local-backend/providers/auth.json"
        auth_mtime = auth_path.stat().st_mtime_ns
        init_replay, _ = cli(env, "init-local")
        if init_replay.get("scope_created") is not False or auth_path.stat().st_mtime_ns != auth_mtime:
            raise AssertionError("init-local replay changed scope or provider auth file")
        if db_counts_for_doctor(engine, scope) != before_replay:
            raise AssertionError("init-local replay changed database rows")
        result["init_local_replay"] = {
            "scope_created": init_replay.get("scope_created"),
            "row_counts_unchanged": True,
            "provider_auth_mtime_unchanged": True,
        }
        conflict_config = run_dir / "config-conflicting-principal"
        conflict_config.mkdir()
        for filename in ("local.yaml", "models.yaml", "policy.yaml", "pricing.yaml"):
            shutil.copyfile(config_dir / filename, conflict_config / filename)
        conflict_local = yaml.safe_load((conflict_config / "local.yaml").read_text(encoding="utf-8"))
        conflict_local["identity"]["principal_id"] = "different-principal-must-not-replace"
        write_yaml(conflict_config / "local.yaml", conflict_local)
        conflict_env = dict(env, HEKATE_CONFIG_DIR=str(conflict_config))
        conflict_auth = auth_path
        conflict_auth_mtime = conflict_auth.stat().st_mtime_ns
        conflict_result = run([sys.executable, "-m", "hekate", "init-local"], env=conflict_env, timeout=60)
        CLI_OBSERVATIONS.append({
            "argv": ["hekate", "init-local"], "exit_code": conflict_result.returncode,
            "result": "EXPECTED_REJECTION", "reason": "conflicting principal on existing scope",
        })
        if (
            conflict_result.returncode == 0
            or conflict_auth.stat().st_mtime_ns != conflict_auth_mtime
            or db_counts_for_doctor(engine, scope) != before_replay
        ):
            raise AssertionError("init-local replaced a conflicting principal or wrote provider auth before rejection")
        result["conflicting_principal_replay"] = {
            "exit_code": conflict_result.returncode,
            "scope_rows_unchanged": True,
            "provider_auth_unchanged": True,
            "error_reported": bool(conflict_result.stderr),
        }
        prestart, _ = cli(env, "doctor")
        if prestart.get("status") != "PRESTART_OK" or prestart.get("inference_requested") is not False:
            raise AssertionError("doctor did not report the initialized pre-start state")
        result["doctor_prestart"] = prestart
        result["doctor_read_only"] = {
            "before": before_replay,
            "after": db_counts_for_doctor(engine, scope),
        }
        install_fake_profile(config_dir, fake.port)
        env["HEKATE_RUNTIME_MODE"] = "test"
        gateway_process = start_gateway(env, state_dir, "fake")
        start_app_server(image, container_name, state_dir / "letta", app_token,
                         gateway_port, gateway_token, letta_port)
        ready = wait_doctor_ready(env)
        if ready.get("status") != "READY" or ready.get("inference_requested") is not False:
            raise AssertionError(f"doctor did not verify the running fake-runtime path: {ready.get('status')}")
        result["doctor_running"] = ready
        worker_process = start_worker(env, state_dir, "fake")
        request_key = f"p6e-fake-{run_id}"
        question = "Return a short isolated fake-provider response."
        ask, _ = cli(env, "ask", "--request-key", request_key, "--wait-seconds", "30", stdin=question, timeout=60)
        task = ask.get("task")
        result["ask"] = ask
        result["fake_provider"] = {"requests": fake.requests, "request_count": fake.count()}
        if isinstance(task, dict) and isinstance(task.get("task_id"), str):
            result["task_id"] = str(task["task_id"])
            result["task_database_snapshot"] = task_db_snapshot(engine, str(task["task_id"]))
        try:
            failed_metrics = http_json(f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token)
            result["gateway_metrics_after_ask"] = failed_metrics
        except Exception as metrics_error:
            result["gateway_metrics_after_ask"] = {"unavailable": type(metrics_error).__name__}
        if not isinstance(task, dict) or task.get("state") != "COMPLETED" or not isinstance(task.get("response"), str):
            raise AssertionError(f"ordinary fake-provider ask did not complete with a response (state={task.get('state') if isinstance(task, dict) else 'missing'})")
        task_id = str(task["task_id"])
        result.update({"task_id": task_id, "request_key": request_key, "ask": ask})
        if fake.count() != 1:
            raise AssertionError(f"fake provider saw {fake.count()} requests; expected exactly one")
        metrics = wait_http(f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token)
        if metrics.get("upstream_forward_attempts") != 1:
            raise AssertionError("fake gateway forwarding count is not one")
        before_restart = task_db_snapshot(engine, task_id)
        fake_call = (before_restart.get("calls") or [None])[0]
        if (
            len(before_restart.get("calls", [])) != 1
            or not isinstance(fake_call, dict)
            or fake_call.get("execution_mode") != "synthetic_test"
            or fake_call.get("price_synthetic") is not True
            or fake_call.get("usage_completeness") != "COMPLETE"
            or fake_call.get("settlement_state") != "SETTLED"
        ):
            raise AssertionError("fake preflight did not persist one synthetic, settled provider call")
        result["fake_accounting_classification"] = {
            "provider_calls": len(before_restart.get("calls", [])),
            "execution_mode": fake_call.get("execution_mode"),
            "price_synthetic": fake_call.get("price_synthetic"),
            "usage_completeness": fake_call.get("usage_completeness"),
            "settlement_state": fake_call.get("settlement_state"),
        }
        stop_process(worker_process)
        worker_process = start_worker(env, state_dir, "fake-restarted")
        replay, _ = cli(env, "ask", "--request-key", request_key, "--wait-seconds", "0", stdin=question, timeout=60)
        after_restart = task_db_snapshot(engine, task_id)
        metrics_after = http_json(f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token)
        if replay.get("task", {}).get("task_id") != task_id or fake.count() != 1:
            raise AssertionError("fake same-key replay created a new task or provider request")
        if before_restart != after_restart or metrics_after.get("upstream_forward_attempts") != 1:
            raise AssertionError("fake replay changed provider/accounting/result effects")
        result["restart_replay"] = {
            "same_task_id": True,
            "provider_requests_before_after": [1, fake.count()],
            "provider_gateway_attempts_before_after": [metrics.get("upstream_forward_attempts"), metrics_after.get("upstream_forward_attempts")],
            "database_effects_unchanged": True,
            "before": before_restart,
            "after": after_restart,
        }
        result["fake_provider"] = {"requests": fake.requests, "request_count": fake.count()}
        result["status"] = "PASS"
        return result
    except BaseException as error:
        result["status"] = "FAILED"
        result["error"] = {"type": type(error).__name__, "message": redact(str(error)[:600])}
        result["fake_provider"] = {"requests": fake.requests, "request_count": fake.count()}
        if gateway_process is not None and gateway_process.poll() is None:
            try:
                result["gateway_metrics_at_failure"] = http_json(
                    f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token,
                )
            except Exception as metrics_error:
                result["gateway_metrics_at_failure"] = {"unavailable": type(metrics_error).__name__}
        gateway_log = state_dir / "fake-gateway.log"
        if gateway_log.is_file():
            result["gateway_log_tail"] = redact("".join(gateway_log.read_text(encoding="utf-8", errors="replace").splitlines(True)[-30:]))
        return result
    finally:
        stop_process(worker_process)
        stop_app_server(container_name)
        stop_process(gateway_process)
        fake.stop()
        p3.FAKE_MODEL = original_fake_model
        engine.dispose()


def _safe_model_context(candidate, reserve_guard) -> dict[str, object]:
    verified = p6c._verify_live_ollama_metadata(candidate)
    loaded = p6c._observe_current_loaded_runner(candidate)
    model_load: dict[str, object] | None = None
    if loaded.get("loaded") is not True:
        reserve_guard()
        model_load = p6c._ollama_model_only_load(candidate, timeout_seconds=600)
        loaded = p6c._observe_current_loaded_runner(candidate)
    if loaded.get("context_verified_for_this_loaded_runner") is not True:
        raise RuntimeError("pinned Qwen loaded context was not verified at the 8192-token gate")
    return {
        "metadata_version": candidate.ollama_version,
        "model_manifest_digest": candidate.model_manifest_digest,
        "metadata_verified": True,
        "loaded": True,
        "loaded_context_tokens": loaded.get("context_length_tokens"),
        "required_context_tokens": candidate.context_window_tokens,
        "model_only_load": model_load,
        "read_only_metadata_summary": {
            key: verified.get(key) for key in (
                "version", "manifest_digest", "model_size_bytes", "tokenizer_array_sha256",
                "template_sha256", "llama_cpp_version", "loaded_context_verified",
            ) if key in verified
        },
        "inference_requests_before_task": 0,
    }


def run_real_qwen(
    run_id: str, run_dir: Path, db_base_url: str, node_bin: str, image: str,
    publish: Callable[[dict[str, object]], None],
) -> dict[str, object]:
    database_url = db_url_for(db_base_url, "hekate_p6e_real")
    scope = f"p6e-real-{run_id[-12:]}"
    worker_id = f"p6e-real-worker-{run_id[-8:]}"
    gateway_port, letta_port = free_port(), free_port()
    config_dir, state_dir, gateway_token, app_token = local_config(
        run_dir, "real", scope, database_url, worker_id, gateway_port,
    )
    env = base_env(config_dir, database_url, worker_id, gateway_token, app_token, node_bin, letta_port)
    engine = create_engine(database_url)
    gateway_process = worker_process = None
    container_name = f"hekate-p6e-qwen-{run_id[-8:]}"
    attempt_ledger = state_dir / "qwen-one-shot-attempt.json"
    output_capture_paths = {
        "bridge_observations": state_dir / "qwen-output-observations.jsonl",
        "app_server_assistant": state_dir / "qwen-app-server-assistant.txt",
        "sdk_assistant": state_dir / "qwen-sdk-assistant.txt",
        "sdk_result": state_dir / "qwen-sdk-result.txt",
        "bridge_assistant": state_dir / "qwen-bridge-assistant.txt",
    }
    result: dict[str, object] = {"status": "BLOCKED", "scope_id": scope, "task_id": None}

    def checkpoint() -> None:
        publish(result)

    try:
        if GLOBAL_REAL_RUN_GUARD.exists():
            raise RuntimeError("the Phase 6E one-real-generation authorization has already been consumed")
        init_first, _ = cli(env, "init-local")
        result["postgres"] = db_meta(engine)
        if init_first.get("scope_created") is not True:
            raise AssertionError("fresh Qwen scope was not created by init-local")
        result["init_local"] = init_first
        checkpoint()
        before_doctor = db_counts_for_doctor(engine, scope)
        prestart, _ = cli(env, "doctor")
        after_doctor = db_counts_for_doctor(engine, scope)
        if prestart.get("status") != "PRESTART_OK" or prestart.get("database_modified") is not False or before_doctor != after_doctor:
            raise AssertionError("Qwen pre-start doctor was not read-only or did not report PRESTART_OK")
        result["doctor_prestart"] = prestart
        result["doctor_prestart_db_counts"] = {"before": before_doctor, "after": after_doctor}
        checkpoint()

        candidate = load_qwen_candidate_profile()
        def reserve_qwen_allowance() -> None:
            if not GLOBAL_REAL_RUN_GUARD.exists():
                write_guard(GLOBAL_REAL_RUN_GUARD, {
                    "schema_version": "1", "run_id": run_id,
                    "state": "AUTHORIZED_ONE_NEW_TASK_ONLY", "created_at": now_utc(),
                    "scope_id": scope, "database": "hekate_p6e_real",
                    "model_manifest_digest": candidate.model_manifest_digest,
                    "upstream_generations_allowed": 1, "model_only_load_attempts": 1,
                    "note": "Exclusive Phase 6E marker; not a reused Phase 6D approval ledger.",
                }, exclusive=True)

        model_context = _safe_model_context(candidate, reserve_qwen_allowance)
        result["qwen_model_preflight"] = model_context
        result["config_limits"] = {
            "context_tokens": 8192, "input_tokens": 6144, "output_tokens": 2048,
            "main_turn_calls": 1, "compaction_calls": 0, "retry_calls": 0,
            "critic_enabled": False, "continuation_enabled": False,
            "sdk_portable_output_format": False, "native_json_schema": True,
            "temperature": 0, "tools": [],
        }
        reserve_qwen_allowance()
        if attempt_ledger.exists():
            raise RuntimeError("fresh local gateway one-shot attempt ledger unexpectedly exists")
        env["HEKATE_LOCAL_GENERATION_LEDGER"] = str(attempt_ledger)
        for path in output_capture_paths.values():
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
        env.update({
            "HEKATE_OUTPUT_OBSERVATION_FILE": str(output_capture_paths["bridge_observations"]),
            "HEKATE_OUTPUT_OBSERVATION_RAW": "1",
            "HEKATE_OUTPUT_APP_SERVER_RAW_FILE": str(output_capture_paths["app_server_assistant"]),
            "HEKATE_OUTPUT_ASSISTANT_RAW_FILE": str(output_capture_paths["sdk_assistant"]),
            "HEKATE_OUTPUT_SDK_RESULT_RAW_FILE": str(output_capture_paths["sdk_result"]),
            "HEKATE_OUTPUT_BRIDGE_RAW_FILE": str(output_capture_paths["bridge_assistant"]),
        })
        checkpoint()

        gateway_process = start_gateway(env, state_dir, "qwen")
        start_app_server(image, container_name, state_dir / "letta", app_token,
                         gateway_port, gateway_token, letta_port)
        ready = wait_doctor_ready(env)
        if ready.get("status") != "READY" or ready.get("inference_requested") is not False:
            raise AssertionError(f"ordinary local doctor did not verify services: {ready.get('status')}")
        result["doctor_running"] = ready
        checkpoint()
        worker_process = start_worker(env, state_dir, "qwen")
        request_key = f"p6e-qwen-{run_id}"
        question = "17과 25의 합을 한국어 한 문장으로 답해줘."
        ask, _ = cli(env, "ask", "--request-key", request_key, "--wait-seconds", "360", stdin=question, timeout=420)
        task = ask.get("task")
        if not isinstance(task, dict) or not isinstance(task.get("task_id"), str):
            raise AssertionError("ordinary Qwen ask did not return a Task identity")
        task_id = task["task_id"]
        deadline = time.monotonic() + 600
        while task.get("state") not in {"COMPLETED", "FAILED", "CANCELLED"} and time.monotonic() < deadline:
            time.sleep(2)
            task, _ = cli(env, "task", task_id, timeout=30)
        result.update({"task_id": task_id, "request_key": request_key, "question": question, "initial_ask": ask, "final_task": task})
        result["output_capture_files"] = capture_summaries(output_capture_paths)
        checkpoint()
        qwen_metrics = http_json(f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token)
        db_before_replay = task_db_snapshot(engine, task_id)
        ledger_value = json.loads(attempt_ledger.read_text(encoding="utf-8")) if attempt_ledger.exists() else None
        result["qwen_gateway_metrics_before_replay"] = qwen_metrics
        result["qwen_database_before_replay"] = db_before_replay
        result["durable_gateway_attempt_ledger"] = ledger_value
        checkpoint()
        if qwen_metrics.get("upstream_forward_attempts") != 1:
            raise AssertionError("real local gateway did not observe exactly one provider forward attempt")
        if ledger_value is None or ledger_value.get("task_id") != task_id:
            raise AssertionError("general gateway did not consume the durable one-shot attempt gate")

        stop_process(worker_process)
        worker_process = start_worker(env, state_dir, "qwen-restarted")
        replay, _ = cli(env, "ask", "--request-key", request_key, "--wait-seconds", "0", stdin=question, timeout=60)
        task_after_replay = replay.get("task")
        db_after_replay = task_db_snapshot(engine, task_id)
        qwen_metrics_after = http_json(f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token)
        ledger_after = json.loads(attempt_ledger.read_text(encoding="utf-8"))
        if task_after_replay.get("task_id") != task_id:
            raise AssertionError("same-key replay returned a different Task")
        if db_before_replay != db_after_replay or ledger_value != ledger_after:
            raise AssertionError("same-key replay changed DB call, usage, settlement, response, or attempt ledger")
        if qwen_metrics_after.get("upstream_forward_attempts") != 1:
            raise AssertionError("same-key replay forwarded a second request upstream")
        result["restart_replay"] = {
            "same_task_id": True,
            "provider_forward_attempts_before_after": [qwen_metrics.get("upstream_forward_attempts"), qwen_metrics_after.get("upstream_forward_attempts")],
            "database_effects_unchanged": True,
            "one_shot_ledger_unchanged": True,
            "before": db_before_replay,
            "after": db_after_replay,
        }
        checkpoint()
        if task.get("state") != "COMPLETED" or not isinstance(task.get("response"), str) or "42" not in task["response"]:
            result["status"] = "FAILED_OUTPUT_OR_EXECUTION"
            return result
        call = (db_before_replay.get("calls") or [None])[0]
        if not isinstance(call, dict):
            result["status"] = "FAILED_NO_CALL_RECORD"
            return result
        measurement = call.get("measurement_data") or {}
        usage_match = call.get("input_tokens") == measurement.get("measured_input_tokens")
        one_call = len(db_before_replay.get("calls", [])) == 1
        profile_digest = (
            ready.get("checks", {}).get("configuration", {}).get("profile_digest")
            if isinstance(ready.get("checks"), dict)
            and isinstance(ready["checks"].get("configuration"), dict)
            else None
        )
        settled = (
            one_call
            and db_before_replay.get("task", {}).get("provider_calls") == 1
            and call.get("model") == "orcarouter/Qwen3.8-27B-Uncensored:iq4_xs"
            and call.get("measurement_status") == "MEASURED"
            and call.get("profile_digest") == profile_digest
            and call.get("call_status") == "QUIESCENT"
            and call.get("permit_state") == "CONSUMED"
            and call.get("execution_mode") == "local_candidate"
            and call.get("price_synthetic") is False
            and call.get("usage_completeness") == "COMPLETE"
            and call.get("settlement_state") == "SETTLED"
            and usage_match
            and call.get("input_tokens") + call.get("output_tokens") == call.get("total_tokens")
        )
        result["accounting_gate"] = {
            "one_call_record": one_call,
            "task_provider_call_count": (db_before_replay.get("task") or {}).get("provider_calls"),
            "model": call.get("model"),
            "measurement_status": call.get("measurement_status"),
            "profile_digest_matches_running_doctor": call.get("profile_digest") == profile_digest,
            "status": call.get("call_status"),
            "permit": call.get("permit_state"),
            "execution_mode": call.get("execution_mode"),
            "price_synthetic": call.get("price_synthetic"),
            "usage_completeness": call.get("usage_completeness"),
            "settlement": call.get("settlement_state"),
            "measured_input_tokens": measurement.get("measured_input_tokens"),
            "provider_input_tokens": call.get("input_tokens"),
            "input_usage_matches_measurement": usage_match,
            "evaluated_cost_usd": call.get("evaluated_cost_usd"),
            "settled": settled,
            "cost_note": "The configured local external tariff is $0; this is not a claim of zero host or energy cost.",
        }
        result["status"] = "PASS" if settled and task.get("response") == db_before_replay["responses"][0]["response_text"] else "FAILED_ACCOUNTING_OR_RESPONSE"
        checkpoint()
        return result
    except BaseException as error:
        result["status"] = "FAILED_AFTER_ONE_SHOT" if attempt_ledger.exists() else "BLOCKED"
        result["error"] = {"type": type(error).__name__, "message": str(error)[:600]}
        result["one_shot_attempt_ledger_exists"] = attempt_ledger.exists()
        if attempt_ledger.exists():
            try:
                result["durable_gateway_attempt_ledger"] = json.loads(attempt_ledger.read_text(encoding="utf-8"))
            except Exception:
                result["durable_gateway_attempt_ledger"] = {"present": True, "contents": "unreadable"}
        result["output_capture_files"] = capture_summaries(output_capture_paths)
        if gateway_process is not None and gateway_process.poll() is None:
            try:
                metrics = http_json(f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token)
                result["qwen_gateway_metrics_at_failure"] = metrics
            except Exception as metrics_error:
                result["qwen_gateway_metrics_at_failure"] = {"unavailable": type(metrics_error).__name__}
        checkpoint()
        raise
    finally:
        stop_process(worker_process)
        stop_app_server(container_name)
        stop_process(gateway_process)
        engine.dispose()


def fingerprint() -> tuple[str, list[dict[str, str]]]:
    prefixes = ("src/hekate/", "bridge/letta/src/", "config/local.example/", "migrations/", "tests/unit/")
    exact = {
        ".gitignore", "README.md", "pyproject.toml", "uv.lock",
        "scripts/phase6e_local_cli_probe.py", "scripts/build_pinned_letta_runtime.py",
        "docs/local-quickstart.md", "docs/implementation/phase6e-local-cli.md",
    }
    listing = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True).stdout
    paths = {item.decode() for item in listing.split(b"\0") if item}
    paths.update(exact)
    paths.update(str(path.relative_to(ROOT)) for prefix in prefixes for path in (ROOT / prefix).rglob("*") if path.is_file())
    entries: list[dict[str, str]] = []
    digest = hashlib.sha256()
    for name in sorted(path for path in paths if (ROOT / path).is_file()):
        data = (ROOT / name).read_bytes()
        file_digest = hashlib.sha256(data).hexdigest()
        digest.update(name.encode("utf-8") + b"\0" + bytes.fromhex(file_digest))
        entries.append({"path": name, "sha256": file_digest})
    return digest.hexdigest(), entries


def _finish(report: dict[str, object], path: Path) -> int:
    digest, files = fingerprint()
    report["code_fingerprint_sha256"] = digest
    report["fingerprinted_files"] = files
    report["artifact_path"] = str(path.relative_to(ROOT))
    report["ordinary_cli_observations"] = CLI_OBSERVATIONS
    report["finalized_at"] = now_utc()
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    print(json.dumps({"status": report.get("status"), "artifact": str(path), "fingerprint": digest}, ensure_ascii=False))
    return 0 if report.get("status") == "PASS" else 1


def main() -> int:
    run_id = "p6e-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    artifact_path = ARTIFACTS / f"{run_id}-local-cli.json"
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    node_bin = os.environ.get("HEKATE_NODE_BIN", "")
    image = os.environ.get("HEKATE_LETTA_IMAGE", DEFAULT_IMAGE)
    report: dict[str, object] = {
        "schema_version": "1", "probe": "phase6e-local-cli", "run_id": run_id,
        "executed_at": now_utc(), "baseline_sha": "0b9f80918bb628298349909921a1197747f6b35d",
        "baseline_branch": "phase6c-ollama-qwen-profile", "branch": None, "head": None,
        "integration_command": "HEKATE_NODE_BIN=<pinned Node 22.19.0> HEKATE_LETTA_IMAGE=<pinned image> uv run --locked python scripts/phase6e_local_cli_probe.py",
        "real_provider_inference_calls": 0, "status": "BLOCKED",
        "production_dispatch": "BLOCKED",
        "excluded": ["HTTP product API", "production approval", "Critic/Position/projection Qwen validation", "G7/G8"],
        "unexecuted": [],
        "preliminary_fake_preflight_attempts": [
            {
                "artifact": "integration/runtime/artifacts/p6e-20261006T210326Z-89269cf7-local-cli.json",
                "outcome": "failed before fake CLI execution",
                "reason": "probe inspected Alembic head before init-local had created alembic_version",
                "provider_inference_calls": 0,
            },
            {
                "artifact": "integration/runtime/artifacts/p6e-20261006T210425Z-2d04b89e-local-cli.json",
                "outcome": "failed before fake CLI execution",
                "reason": "init-local ran Alembic inside the active event loop; migration now runs in a worker thread",
                "provider_inference_calls": 0,
            },
            {
                "artifact": "integration/runtime/artifacts/p6e-20261006T210450Z-eac51aac-local-cli.json",
                "outcome": "scope was created but probe's created flag was incorrect",
                "reason": "scope creation now uses INSERT RETURNING; existing scope is not recreated",
                "provider_inference_calls": 0,
            },
            {
                "artifact": "integration/runtime/artifacts/p6e-20261006T210606Z-27d4d2b3-local-cli.json",
                "outcome": "fake Task stopped before any provider forward",
                "reason": "generic fake profile caused a forbidden automatic compaction; fixture now uses the pinned Qwen system prompt and zero-compaction profile",
                "provider_inference_calls": 0,
            },
        ],
    }
    try:
        report["head"] = require_ok(run(["git", "rev-parse", "HEAD"]), "Git HEAD inspection")
        report["branch"] = require_ok(run(["git", "branch", "--show-current"]), "Git branch inspection")
        changes = require_ok(run(["git", "status", "--porcelain"]), "Git status inspection")
        report["working_tree_dirty"] = bool(changes)
        report["working_tree_change_count"] = len(changes.splitlines())
        report["legacy_states"] = {
            "prior_phase6d_unknown_and_failed_tasks": "not queried or modified; prior database is not selected",
            "legacy_unsettled_call_fixtures": "not queried or modified; prior database is not selected",
            "new_run_projection": "disabled",
        }
        if GLOBAL_REAL_RUN_GUARD.exists():
            raise RuntimeError("the Phase 6E one-real-generation guard already exists; no Qwen request will be retried")
        if not node_bin:
            node_bin = shutil.which("node") or ""
        if not node_bin or require_ok(run([node_bin, "--version"]), "pinned Node inspection") != "v22.19.0":
            raise RuntimeError("set HEKATE_NODE_BIN to pinned Node.js 22.19.0 before the integration probe")
        if not (ROOT / "bridge/letta/dist/main.js").is_file():
            raise RuntimeError("build the private bridge before running this probe")
        runtime = verify_image(image)
        report["runtime"] = runtime
        run_dir = ROOT / ".hekate-local" / run_id
        run_dir.mkdir(parents=True, mode=0o700)
        db_base, db_port, pg_container, pg_volume = configure_pg(run_id)
        report["database"] = {"container": pg_container, "volume": pg_volume, "host": "127.0.0.1", "port": db_port}
        report["fake_preflight"] = run_fake_preflight(run_id, run_dir, db_base, node_bin, image)
        if report["fake_preflight"].get("status") != "PASS":
            report["status"] = "BLOCKED_FAKE_PREFLIGHT"
            report["unexecuted"].append("real Qwen generation: fake CLI/runtime preflight did not pass")
            return _finish(report, artifact_path)
        if GLOBAL_REAL_RUN_GUARD.exists():
            raise RuntimeError("the Phase 6E one-real-generation guard already exists; no Qwen request was sent")
        def publish_real_progress(value: dict[str, object]) -> None:
            report["real_qwen"] = value
            metrics = value.get("qwen_gateway_metrics_before_replay") or value.get("qwen_gateway_metrics_at_failure")
            if isinstance(metrics, dict):
                report["real_provider_inference_calls"] = metrics.get("upstream_forward_attempts", 0)
            elif value.get("one_shot_attempt_ledger_exists") is True:
                report["real_provider_inference_calls"] = "UNKNOWN_UP_TO_ONE"

        report["real_qwen"] = run_real_qwen(run_id, run_dir, db_base, node_bin, image, publish_real_progress)
        report["real_provider_inference_calls"] = 1 if (
            isinstance(report["real_qwen"].get("qwen_gateway_metrics_before_replay"), dict)
            and report["real_qwen"]["qwen_gateway_metrics_before_replay"].get("upstream_forward_attempts") == 1
        ) else 0
        report["status"] = "PASS" if report["real_qwen"].get("status") == "PASS" else "FAILED_AFTER_ONE_SHOT"
        report["unexecuted"].extend([
            "No second Qwen request is permitted by this run.",
            "No Qwen Critic, Position commit, or memory projection was exercised.",
        ])
        report["legacy_states"] = {
            "prior_phase6d_unknown_and_failed_tasks": "not queried or modified; prior database was not used",
            "legacy_unsettled_call_fixtures": "not queried or modified; prior database was not used",
            "new_run_projection": "not enabled or exercised",
        }
        return _finish(report, artifact_path)
    except BaseException as error:
        real_result = report.get("real_qwen")
        if isinstance(real_result, dict) and real_result.get("one_shot_attempt_ledger_exists") is True:
            report["status"] = "FAILED_AFTER_GATEWAY_ATTEMPT"
        elif GLOBAL_REAL_RUN_GUARD.exists():
            report["status"] = "FAILED_AFTER_MODEL_PREFLIGHT"
        else:
            report["status"] = "ERROR_BEFORE_QWEN_AUTHORIZATION"
        report["error"] = {"type": type(error).__name__, "message": redact(str(error)[:800])}
        report["unexecuted"].append("Any boundary after the recorded error is unverified.")
        return _finish(report, artifact_path)


if __name__ == "__main__":
    raise SystemExit(main())
