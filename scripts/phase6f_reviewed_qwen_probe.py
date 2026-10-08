#!/usr/bin/env python3
"""Run Phase 6F through the ordinary CLI, first with a context-checking fake and then explicitly with local Qwen."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import selectors
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import phase3_runtime_probe as p3  # noqa: E402
import phase5a_critic_synthesis_probe as p5a  # noqa: E402
import phase6c_ollama_qwen_probe as p6c  # noqa: E402
import phase6e_local_cli_probe as p6e  # noqa: E402
from hekate.domain.contracts import canonical_json_hash  # noqa: E402
from hekate.infrastructure.letta.qwen_local_profile import (  # noqa: E402
    qwen35_reviewed_native_json_schema_local_execution_profile,
)
from hekate.infrastructure.letta.reviewed_qwen_profile import (  # noqa: E402
    PROFILE_ID,
    critic_turn_output_schema,
    hekate_turn_output_schema,
)
from hekate.infrastructure.letta.qwen_ollama import load_qwen_candidate_profile  # noqa: E402
from hekate.bootstrap import build_container, close_container  # noqa: E402
from hekate.application.projections import verify_projection  # noqa: E402
from hekate.domain.types import ObservationState, ProviderAgentId, TopicId  # noqa: E402
from hekate.application.runtime_inbox import RuntimeInboxPayload  # noqa: E402
from hekate.settings import configured_local_actor, load_settings  # noqa: E402
from hekate.infrastructure.postgres.database import create_engine as create_app_engine, create_uow_factory  # noqa: E402
from hekate.domain.types import OperationId, RegistryId  # noqa: E402

ARTIFACTS = ROOT / "integration/runtime/artifacts"
REVIEWED_EXAMPLE = ROOT / "config/local-reviewed.example"
PERSONAL_EXAMPLE = ROOT / "config/personal-local.example"
BASELINE_SHA = "09202b8e46a0fcf61cd692a466a8e18ca5b56c34"
DEFAULT_IMAGE = "hekate/letta-code-p1:0bb6f741-0c6a0fc3"
TOPIC_PREFIX = "p6f-reviewed-topic-"
EVIDENCE_A = (
    "Synthetic evaluation fixture, not a service fact: candidate A has monthly cost 10 and p95 latency 120 ms. "
    "Candidate B has monthly cost 14 and p95 latency 80 ms."
)
EVIDENCE_B = (
    "Synthetic evaluation fixture, not a service fact: the decision limits are monthly cost at most 12 and p95 latency "
    "at most 150 ms. These values are test assumptions, not a performance guarantee for a real service."
)
POSITION_STATEMENT = (
    "Under the submitted test assumptions, candidate A fits the monthly budget and p95 limit; verify these values "
    "against the target environment before relying on them."
)


def now_utc() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def free_port() -> int:
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return int(stream.getsockname()[1])


def run(command: list[str], *, env: dict[str, str] | None = None, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, check=False, timeout=timeout)


def require_ok(result: subprocess.CompletedProcess[str], label: str) -> str:
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed ({result.returncode}): {(result.stderr or result.stdout)[-900:]}")
    return result.stdout.strip()


def write_yaml(path: Path, value: object) -> None:
    path.write_text(yaml.safe_dump(value, allow_unicode=True, sort_keys=False), encoding="utf-8")


def configure_postgres(run_id: str) -> tuple[str, int, str, str]:
    password = secrets.token_hex(24)
    port = free_port()
    suffix = run_id[-8:]
    container = f"hekate-p6f-{suffix}"
    volume = f"hekate-p6f-{suffix}"
    require_ok(run(["docker", "volume", "create", volume]), "isolated PostgreSQL volume creation")
    require_ok(run([
        "docker", "run", "--detach", "--name", container,
        "--publish", f"127.0.0.1:{port}:5432",
        "--env", "POSTGRES_USER=hekate", "--env", f"POSTGRES_PASSWORD={password}",
        "--env", "POSTGRES_DB=postgres", "--volume", f"{volume}:/var/lib/postgresql/data",
        "postgres:16.15",
    ], timeout=120), "isolated PostgreSQL start")
    base = f"postgresql+psycopg://hekate:{password}@127.0.0.1:{port}"
    admin = create_engine(base + "/postgres")
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                with admin.connect() as connection:
                    connection.execute(text("SELECT 1"))
                    connection.commit()
                break
            except Exception:
                time.sleep(0.25)
        else:
            raise TimeoutError("isolated PostgreSQL did not become ready")
        with admin.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text("CREATE DATABASE hekate_p6f_fake"))
            connection.execute(text("CREATE DATABASE hekate_p6f_real"))
            connection.execute(text("CREATE DATABASE hekate_p6f_personal_migration"))
    finally:
        admin.dispose()
    return base, port, container, volume


def _db_url(base: str, name: str) -> str:
    return make_url(base).set(database=name).render_as_string(hide_password=False)


def _migration_cycle(env: dict[str, str], db_base: str) -> dict[str, object]:
    database_url = _db_url(db_base, "hekate_p6f_personal_migration")
    isolated_env = dict(env, HEKATE_DATABASE_URL=database_url)
    require_ok(run([sys.executable, "-m", "alembic", "upgrade", "head"], env=isolated_env, timeout=120), "empty migration database upgrade")
    return {"database": "hekate_p6f_personal_migration", **p6e.run_migration_checks(isolated_env)}


def _dump_restore_fixture(db_base: str, db_container: str, run_dir: Path) -> dict[str, object]:
    restore_db = "hekate_p6f_personal_restore"
    admin = create_engine(_db_url(db_base, "postgres"))
    try:
        with admin.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text(f"CREATE DATABASE {restore_db}"))
    finally:
        admin.dispose()
    dump_path = run_dir / "personal-fixture.dump"
    descriptor = os.open(dump_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)
    pg_dump_version = require_ok(run([
        "docker", "exec", "--user", "postgres", db_container, "pg_dump", "--version",
    ]), "isolated PostgreSQL pg_dump version")
    pg_restore_version = require_ok(run([
        "docker", "exec", "--user", "postgres", db_container, "pg_restore", "--version",
    ]), "isolated PostgreSQL pg_restore version")
    dumped = subprocess.run([
        "docker", "exec", "--user", "postgres", db_container, "pg_dump",
        "--format=custom", "--no-owner", "--username=hekate", "--dbname=hekate_p6f_fake",
    ], cwd=ROOT, capture_output=True, check=False, timeout=180)
    if dumped.returncode != 0:
        raise RuntimeError(f"isolated fixture pg_dump failed: {dumped.stderr.decode('utf-8', 'replace')[-800:]}")
    dump_path.write_bytes(dumped.stdout)
    os.chmod(dump_path, 0o600)
    restored = subprocess.run([
        "docker", "exec", "--interactive", "--user", "postgres", db_container,
        "pg_restore", "--exit-on-error", "--no-owner", "--username=hekate", f"--dbname={restore_db}",
    ], cwd=ROOT, input=dumped.stdout, capture_output=True, check=False, timeout=180)
    if restored.returncode != 0:
        raise RuntimeError(f"isolated fixture pg_restore failed: {restored.stderr.decode('utf-8', 'replace')[-800:]}")
    return {
        "dump_tool": pg_dump_version,
        "restore_tool": pg_restore_version,
        "dump_bytes": dump_path.stat().st_size,
        "dump_sha256": hashlib.sha256(dump_path.read_bytes()).hexdigest(),
        "restore_database": restore_db,
        "mode": oct(dump_path.stat().st_mode & 0o777),
        "status": "PASS",
    }


async def _verify_restored_runtime(env: dict[str, str], topic_id: str) -> dict[str, object]:
    settings = load_settings(env, Path(env["HEKATE_CONFIG_DIR"]))
    container = await build_container(settings)
    try:
        actor = configured_local_actor(settings)
        async with container.uow_factory() as uow:
            registry = await uow.agents.get_persistent_scope(actor.scope)
            if registry is None or registry.provider_id is None:
                raise AssertionError("restored database has no persistent HEKATE provider binding")
            await uow.commit()
        observed = await container.runtime.observe_agent(ProviderAgentId(str(registry.provider_id)))
        if (
            observed.state != ObservationState.PRESENT or observed.provider_agent_id != registry.provider_id
            or observed.owner != str(registry.owner_scope) or observed.creation_tag != str(registry.creation_operation_id)
            or observed.role != "hekate"
        ):
            raise AssertionError("restored Letta state does not match the PostgreSQL persistent HEKATE binding")
        projection = await verify_projection(
            container.uow_factory, container.runtime, actor, TopicId(topic_id),
        )
        if projection.state != "APPLIED" or projection.observed_version != projection.desired_version:
            raise AssertionError("restored runtime memory did not match the restored authoritative Position")
        return {
            "registry_id": str(registry.registry_id), "provider_agent_id": str(registry.provider_id),
            "binding_observation": "PRESENT_OWNER_CREATION_TAG_ROLE_MATCH",
            "projection_state": projection.state, "desired_version": projection.desired_version,
            "applied_version": projection.applied_version,
            "observed_version": projection.observed_version,
            "observed_digest_matches": projection.observed_digest == projection.payload_digest,
        }
    finally:
        await close_container(container)


def _managed_child_pids(log_path: Path) -> dict[str, int]:
    body = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
    values = {
        "gateway": re.search(r"Started private gateway \(PID (\d+)\)", body),
        "worker": re.search(r"Started HEKATE worker \(PID (\d+)\)", body),
    }
    if any(value is None for value in values.values()):
        raise AssertionError(f"hekate run did not durably report both owned child PIDs in {log_path.name}")
    return {name: int(match.group(1)) for name, match in values.items() if match is not None}


def _copy_restore_config(
    source_config: Path, restore_root: Path, gateway_port: int,
    letta_port: int, fake_port: int,
) -> tuple[Path, Path]:
    restore_config = restore_root / "config"
    restore_state = restore_root / "state"
    restore_config.mkdir(parents=True, mode=0o700)
    restore_state.mkdir(parents=True, mode=0o700)
    for filename in ("local.yaml", "models.yaml", "policy.yaml", "pricing.yaml"):
        shutil.copyfile(source_config / filename, restore_config / filename)
    local = yaml.safe_load((restore_config / "local.yaml").read_text(encoding="utf-8"))
    local["runtime"]["worker_id"] = "personal-restore-verifier"
    local["gateway"]["port"] = gateway_port
    local["gateway"]["upstream_base_url"] = f"http://127.0.0.1:{fake_port}"
    local["paths"]["state_dir"] = str(restore_state)
    local["paths"]["archive_dir"] = str(restore_state / "archive")
    write_yaml(restore_config / "local.yaml", local)
    return restore_config, restore_state


async def _queue_usage_replay(database_url: str, task_ids: tuple[str, str], run_id: str) -> dict[str, object]:
    engine = create_app_engine(database_url)
    try:
        factory = create_uow_factory(engine)
        async with factory() as uow:
            row = (await uow.session.execute(text("""
                SELECT p.operation_id, p.accounting_call_id, p.provider_call_id, o.binding,
                       u.source, u.observation_identity, u.completeness, u.input_tokens,
                       u.output_tokens, u.cache_tokens, u.reasoning_tokens, u.total_tokens,
                       u.reported_cost_usd
                FROM usage_observations u
                JOIN provider_calls p ON p.accounting_call_id=u.accounting_call_id
                JOIN operations o ON o.id=p.operation_id
                WHERE o.task_id IN (:task_a,:task_b) AND u.completeness='COMPLETE'
                ORDER BY u.received_at, u.id LIMIT 1
            """), {"task_a": task_ids[0], "task_b": task_ids[1]})).mappings().one_or_none()
            if row is None:
                raise AssertionError("no completed persisted usage observation exists for reconciliation replay")
            stored_binding = row["binding"]
            binding_fields = (
                "task_id", "attempt_id", "agent_registry_id", "provider_agent_id",
                "conversation_id", "input_revision", "fence",
            )
            if not isinstance(stored_binding, dict) or any(field not in stored_binding for field in binding_fields):
                raise AssertionError("persisted operation does not contain a complete runtime identity binding")
            usage = {
                "source": row["source"], "completeness": row["completeness"],
                "input_tokens": row["input_tokens"], "output_tokens": row["output_tokens"],
                "cache_tokens": row["cache_tokens"], "reasoning_tokens": row["reasoning_tokens"],
                "total_tokens": row["total_tokens"],
                "reported_cost_usd": str(row["reported_cost_usd"]) if row["reported_cost_usd"] is not None else None,
            }
            payload = {
                "event_type": "runtime_usage", "operation_id": row["operation_id"],
                "accounting_call_id": row["accounting_call_id"], "source": "bridge_event",
                "observation_identity": row["observation_identity"],
                "provider_call_id": row["provider_call_id"],
                "binding": {field: stored_binding[field] for field in binding_fields}, "usage": usage,
            }
            payload = RuntimeInboxPayload.model_validate(payload, strict=True).model_dump(
                mode="json", exclude_none=True,
            )
            identity = f"personal-reconcile-replay:{run_id}:{row['accounting_call_id']}"
            receipt = await uow.delivery.insert_inbox_once(
                "letta-bridge", identity, payload, canonical_json_hash(payload),
            )
            if receipt.conflict or receipt.duplicate:
                raise AssertionError("personal reconciliation replay fixture identity was not unique")
            await uow.commit()
            return {
                "inbox_id": receipt.id, "operation_id": row["operation_id"],
                "accounting_call_id": row["accounting_call_id"],
                "observation_identity": row["observation_identity"],
                "source_observation_already_persisted": True,
            }
    finally:
        await engine.dispose()


def _unknown_fixture_snapshot(engine, task_id: str, operation_id: str) -> dict[str, object]:
    rows = p6e.db_read(engine, """
        SELECT o.id, o.state, o.dispatch_state, o.execution_state, o.last_error,
               h.state AS hold_state, h.quiescent_at,
               (SELECT count(*) FROM provider_calls p WHERE p.operation_id=o.id) AS provider_call_rows,
               (SELECT held_amount::text FROM budget_accounts b
                 WHERE b.scope_kind='TASK' AND b.scope_ref=o.task_id) AS task_budget_held
        FROM operations o JOIN agent_execution_holds h ON h.operation_id=o.id
        WHERE o.id=:operation AND o.task_id=:task
    """, {"operation": operation_id, "task": task_id})
    if len(rows) != 1:
        raise AssertionError("UNKNOWN reconciliation fixture disappeared")
    return dict(rows[0])


def _seed_unknown_reconciliation_fixture(env: dict[str, str], engine, scope: str, run_id: str) -> dict[str, object]:
    task_result, _ = p6e.cli(
        env, "ask", "--request-key", f"owner-{run_id}-unknown-sentinel", "--wait-seconds", "0",
        stdin="Isolated reconciliation preservation fixture; do not execute.",
    )
    task = task_result.get("task")
    if not isinstance(task, dict) or task.get("state") != "QUEUED":
        raise AssertionError("unknown sentinel Task was not admitted without worker execution")
    task_id = str(task["task_id"])
    registry = p6e.db_read(engine, """
        SELECT id, provider_agent_id FROM agent_registry
        WHERE owner_scope=:scope AND role='hekate' AND persistence='persistent'
    """, {"scope": scope})
    if len(registry) != 1:
        raise AssertionError("personal scope does not have exactly one persistent HEKATE registry")
    operation_id = str(uuid.uuid4())
    binding = {
        "task_id": task_id, "attempt_id": "unresolved-fixture-attempt",
        "agent_registry_id": registry[0]["id"], "provider_agent_id": registry[0]["provider_agent_id"],
        "conversation_id": "unresolved-fixture-conversation", "input_revision": 1, "fence": 1,
    }

    async def write_fixture() -> None:
        app_engine = create_app_engine(env["HEKATE_DATABASE_URL"])
        try:
            factory = create_uow_factory(app_engine)
            async with factory() as uow:
                await uow.session.execute(text("""
                    INSERT INTO operations(
                        id,owner_scope,task_id,kind,request_hash,state,dispatch_state,execution_state,
                        binding,envelope,observation,last_error
                    ) VALUES (
                        :id,:scope,:task,'test.unknown_preservation',:hash,'UNKNOWN','UNKNOWN','UNKNOWN',
                        CAST(:binding AS jsonb),CAST(:envelope AS jsonb),'{}'::jsonb,
                        'synthetic unresolved fixture without provider-call evidence'
                    )
                """), {
                    "id": operation_id, "scope": scope, "task": task_id,
                    "hash": canonical_json_hash({"fixture": run_id, "task_id": task_id}),
                    "binding": json.dumps(binding), "envelope": json.dumps({"task_id": task_id}),
                })
                await uow.agents.create_execution_hold(
                    RegistryId(str(registry[0]["id"])), OperationId(operation_id),
                )
                await uow.session.execute(text("""
                    UPDATE agent_execution_holds SET state='UNKNOWN', reason=:reason
                    WHERE operation_id=:operation
                """), {
                    "operation": operation_id,
                    "reason": "synthetic preservation fixture; no termination evidence exists",
                })
                await uow.commit()
        finally:
            await app_engine.dispose()

    asyncio.run(write_fixture())
    before = _unknown_fixture_snapshot(engine, task_id, operation_id)
    return {"task_id": task_id, "operation_id": operation_id, "before": before}


def _verify_backup_restore(
    *, db_base: str, db_container: str, run_dir: Path, env: dict[str, str], config_dir: Path, state_dir: Path,
    report: dict[str, object], database_engine, fake, managed_run: subprocess.Popen[bytes],
    app_server_name: str, app_token: str, gateway_token: str, image: str,
    task_id: str, task_ids: tuple[str, str], topic_id: str, evidence_ids: list[str],
    task_snapshots: dict[str, object], run_id: str,
) -> dict[str, object]:
    log_path = state_dir / "fake-restarted-run.log"
    child_pids = _managed_child_pids(log_path)
    os.kill(child_pids["worker"], signal.SIGTERM)
    exit_code = managed_run.wait(timeout=30)
    if exit_code == 0 or not all(_pid_is_gone(pid) for pid in child_pids.values()):
        raise AssertionError("unexpected managed worker exit did not stop run and its remaining gateway child")
    after_worker_exit = p6e.cli(env, "doctor")[0]
    app_running = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", app_server_name],
        cwd=ROOT, capture_output=True, text=True, check=True, timeout=20,
    ).stdout.strip().lower() == "true"
    if (
        after_worker_exit.get("checks", {}).get("provider_gateway", {}).get("status") != "NOT_RUNNING"
        or after_worker_exit.get("checks", {}).get("letta_runtime", {}).get("status") != "READY"
        or not app_running or fake.count() != 4
    ):
        raise AssertionError("unexpected child exit stopped an external service or changed fake provider count")
    report["run_child_exit"] = {
        "worker_exit_code": exit_code, "managed_gateway_stopped": _pid_is_gone(child_pids["gateway"]),
        "external_app_server_survived": app_running, "fake_provider_requests_unchanged": fake.count() == 4,
    }

    sigint_run, _ = _start_managed_run(env, state_dir, "personal-sigint")
    sigint_log = state_dir / "personal-sigint-run.log"
    sigint_pids = _managed_child_pids(sigint_log)
    sigint_run.send_signal(signal.SIGINT)
    sigint_exit = sigint_run.wait(timeout=30)
    after_sigint = p6e.cli(env, "doctor")[0]
    app_running = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", app_server_name],
        cwd=ROOT, capture_output=True, text=True, check=True, timeout=20,
    ).stdout.strip().lower() == "true"
    if (
        sigint_exit != 0 or not all(_pid_is_gone(pid) for pid in sigint_pids.values())
        or after_sigint.get("checks", {}).get("provider_gateway", {}).get("status") != "NOT_RUNNING"
        or not app_running or fake.count() != 4
    ):
        raise AssertionError("SIGINT did not stop only run-owned children cleanly")
    if any(p6e.task_db_snapshot(database_engine, selected) != value for selected, value in task_snapshots.items()):
        raise AssertionError("run shutdown changed stored Task accounting or answer")
    report["run_sigint"] = {
        "exit_code": sigint_exit, "owned_children_stopped": True,
        "external_app_server_survived": app_running, "fake_provider_requests_unchanged": fake.count() == 4,
        "database_and_persistent_state_preserved": True,
    }

    replay = asyncio.run(_queue_usage_replay(env["HEKATE_DATABASE_URL"], task_ids, run_id))
    reconcile_before = {
        selected: p6e.task_db_snapshot(database_engine, selected) for selected in task_ids
    }
    usage_count_before = p6e.db_read(database_engine, """
        SELECT count(*) AS count FROM usage_observations u
        JOIN provider_calls p ON p.accounting_call_id=u.accounting_call_id
        JOIN operations o ON o.id=p.operation_id WHERE o.task_id IN (:task_a,:task_b)
    """, {"task_a": task_ids[0], "task_b": task_ids[1]})[0]["count"]
    read_only = _json_cli(env, "reconcile")
    applied = _json_cli(env, "reconcile", "--apply")
    replayed = _json_cli(env, "reconcile", "--apply")
    reconcile_after = {
        selected: p6e.task_db_snapshot(database_engine, selected) for selected in task_ids
    }
    usage_count_after = p6e.db_read(database_engine, """
        SELECT count(*) AS count FROM usage_observations u
        JOIN provider_calls p ON p.accounting_call_id=u.accounting_call_id
        JOIN operations o ON o.id=p.operation_id WHERE o.task_id IN (:task_a,:task_b)
    """, {"task_a": task_ids[0], "task_b": task_ids[1]})[0]["count"]
    applied_inbox = applied.get("apply", {}).get("inbox", [])
    pending_rows = read_only.get("pending_inbox", [])
    if (
        reconcile_before != reconcile_after or usage_count_before != usage_count_after
        or fake.count() != 4
        or not any(item.get("id") == replay["inbox_id"] for item in pending_rows)
        or len(applied_inbox) != 1 or applied_inbox[0].get("id") != replay["inbox_id"]
        or applied_inbox[0].get("processed") is not True
        or replayed.get("apply", {}).get("inbox")
        or any(value.get("inference_requested") is not False for value in (read_only, applied, replayed))
    ):
        raise AssertionError("reconcile did not process the durable usage observation exactly once without new effects")
    report["reconciliation_observation_replay"] = {
        "fixture": replay, "first_apply": applied.get("apply"),
        "second_apply": replayed.get("apply"), "stored_calls_unchanged": reconcile_before == reconcile_after,
        "usage_observation_count_before_after": [usage_count_before, usage_count_after],
        "fake_provider_requests": fake.count(), "inference_requested": False,
    }

    p6e.stop_app_server(app_server_name)
    restore_root = run_dir / "restore"
    restore_root.mkdir(mode=0o700)
    backup = _dump_restore_fixture(db_base, db_container, restore_root)
    restore_gateway_port, restore_letta_port = free_port(), free_port()
    restored_database_url = _db_url(db_base, str(backup["restore_database"]))
    restore_config_dir, restored_state = _copy_restore_config(
        config_dir, restore_root,
        restore_gateway_port, restore_letta_port, fake.port,
    )
    shutil.copytree(state_dir / "archive", restored_state / "archive", copy_function=shutil.copy2)
    restored_letta_state = restored_state / "letta"
    restored_letta_state.mkdir(mode=0o700)
    copied_letta_state = p6e.run([
        "docker", "run", "--rm", "--network", "none", "--user", "0",
        "--mount", f"type=bind,source={state_dir / 'letta'},target=/source,readonly",
        "--mount", f"type=bind,source={restored_letta_state},target=/target",
        "--entrypoint", "python", image,
        "-c", 'import shutil; shutil.copytree("/source", "/target", dirs_exist_ok=True, copy_function=shutil.copy2, ignore=shutil.ignore_patterns("app-server-token"))',
    ], timeout=180)
    p6e.require_ok(copied_letta_state, "stopped pinned Letta state copy into isolated restore")
    restored_capture = restored_state / "provider-requests.jsonl"
    capture_fd = os.open(restored_capture, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(capture_fd)
    restored_env = {
        **env, "HEKATE_CONFIG_DIR": str(restore_config_dir),
        "HEKATE_DATABASE_URL": restored_database_url,
        "HEKATE_ARCHIVE_DIR": str(restored_state / "archive"),
        "HEKATE_PROVIDER_REQUEST_OBSERVATION_FILE": str(restored_capture),
        "HEKATE_LETTA_URL": f"ws://127.0.0.1:{restore_letta_port}",
        "HEKATE_WORKER_ID": "personal-restore-verifier",
    }
    restored_gateway = None
    restored_app_server_name = f"hekate-p6f-restore-{uuid.uuid4().hex[:8]}"
    restore_result: dict[str, object] = {}
    try:
        restored_gateway = p6e.start_gateway(restored_env, restored_state, "restored")
        p6e.start_app_server(
            image, restored_app_server_name, restored_state / "letta", app_token,
            restore_gateway_port, gateway_token, restore_letta_port,
        )
        doctor = p6e.wait_doctor_ready(restored_env)
        if doctor.get("status") != "READY" or doctor.get("inference_requested") is not False:
            raise AssertionError("restored personal configuration did not pass read-only pinned-runtime doctor")
        evidence = _json_cli(restored_env, "evidence", "show", evidence_ids[0])
        expected_hash = hashlib.sha256((EVIDENCE_A + "\n").encode("utf-8")).hexdigest()
        position = _json_cli(restored_env, "position", "show", topic_id)
        history = _json_cli(restored_env, "position", "history", topic_id, "--after-version", "0", "--limit", "50")
        task = _json_cli(restored_env, "task", task_id)
        if (
            evidence.get("content_hash") != expected_hash or evidence.get("content") != EVIDENCE_A + "\n"
            or position.get("current_version") != 1 or len(history.get("items", [])) != 1
            or task.get("state") != "COMPLETED" or not task.get("response")
        ):
            raise AssertionError("restored PostgreSQL/archive lost Evidence, Position history, or Task response")
        runtime = asyncio.run(_verify_restored_runtime(restored_env, topic_id))
        restored_metrics = p6e.http_json(
            f"http://127.0.0.1:{restore_gateway_port}/internal/metrics", gateway_token,
        )
        if fake.count() != 4 or restored_metrics.get("upstream_forward_attempts") != 0:
            raise AssertionError("restore verification unexpectedly called the fake provider")
        restore_result = {
            "postgres_dump_restore": backup, "read_only_doctor": doctor.get("status"),
            "evidence_digest_matches": evidence.get("content_hash") == expected_hash,
            "position_current_and_history": {"current_version": position.get("current_version"), "history_items": len(history.get("items", []))},
            "task_state": task.get("state"), "registry_and_projection": runtime,
            "letta_state_copy": {
                "method": "isolated root helper using pinned image", "completed": True,
                "ephemeral_app_server_token_reissued": True,
            },
            "restored_gateway_forwards": restored_metrics.get("upstream_forward_attempts"),
            "provider_requests_unchanged": fake.count(), "model_loads": 0,
        }
    finally:
        p6e.stop_app_server(restored_app_server_name)
        p6e.stop_process(restored_gateway)
    return restore_result


def prepare_config(
    run_dir: Path, run_id: str, mode: str, database_url: str, gateway_port: int, letta_port: int,
    fake_port: int | None = None, *, personal_local: bool = False,
) -> tuple[Path, Path, Path | None, str, str, dict[str, str]]:
    scope = f"p6f-{mode}-{run_id[-12:]}"
    worker_id = f"p6f-{mode}-worker-{run_id[-8:]}"
    config_dir = run_dir / "config"
    state_dir = run_dir / "state"
    config_dir.mkdir(parents=True, mode=0o700)
    state_dir.mkdir(parents=True, mode=0o700)
    example = PERSONAL_EXAMPLE if personal_local else REVIEWED_EXAMPLE
    for filename in ("local.yaml", "models.yaml", "policy.yaml", "pricing.yaml"):
        shutil.copyfile(example / filename, config_dir / filename)
    local = yaml.safe_load((config_dir / "local.yaml").read_text(encoding="utf-8"))
    local["identity"].update({
        "scope_id": scope,
        "principal_id": f"p6f-{mode}-operator-{run_id[-8:]}",
        "policy_version": f"p6f-{mode}-policy-v1-{run_id[-6:]}",
        "authz_epoch": 1,
    })
    local["runtime"]["worker_id"] = worker_id
    local["gateway"]["port"] = gateway_port
    local["paths"]["state_dir"] = str(state_dir)
    local["paths"]["archive_dir"] = str(state_dir / "archive")
    if not personal_local:
        local["paths"]["generation_allowance"] = str(state_dir / "generation-allowance.json")
    if fake_port is not None:
        local["gateway"]["upstream_base_url"] = f"http://127.0.0.1:{fake_port}"
        local["gateway"]["upstream_api_key"] = "p6f-isolated-fake-only"
    write_yaml(config_dir / "local.yaml", local)

    candidate = load_qwen_candidate_profile()
    profile, _prices = qwen35_reviewed_native_json_schema_local_execution_profile(candidate)
    allowance = None
    if not personal_local:
        allowance = state_dir / "generation-allowance.json"
        allowance.write_text(json.dumps({
            "schema_version": "1", "run_id": run_id, "scope_id": scope,
            "profile_id": PROFILE_ID, "profile_digest": profile.content_digest,
            "max_generations": 4,
            "task_request_keys": {
                "task_a": f"p6f-{mode}-{run_id}-task-a",
                "task_b": f"p6f-{mode}-{run_id}-task-b",
            },
            "claims": {},
        }, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
        os.chmod(allowance, 0o600)
    request_capture = state_dir / "provider-requests.jsonl"
    descriptor = os.open(request_capture, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)
    gateway_token = secrets.token_urlsafe(40)
    app_token = secrets.token_urlsafe(40)
    env = {
        **os.environ,
        "HEKATE_CONFIG_DIR": str(config_dir),
        "HEKATE_PROJECT_DIR": str(ROOT),
        "HEKATE_DATABASE_URL": database_url,
        "HEKATE_RUNTIME_MODE": "local",
        "HEKATE_WORKER_ID": worker_id,
        "HEKATE_NODE_BIN": os.environ.get("HEKATE_NODE_BIN", shutil.which("node") or ""),
        "HEKATE_BRIDGE_ENTRY": str(ROOT / "bridge/letta/dist/main.js"),
        "HEKATE_LETTA_URL": f"ws://127.0.0.1:{letta_port}",
        "HEKATE_LETTA_TOKEN": app_token,
        "HEKATE_PROVIDER_GATEWAY_TOKEN": gateway_token,
        "HEKATE_ARCHIVE_DIR": str(state_dir / "archive"),
        "HEKATE_MEMORY_PROJECTION_ENABLED": "true",
        "HEKATE_PROVIDER_REQUEST_OBSERVATION_FILE": str(request_capture),
    }
    env.pop("HEKATE_LOCAL_GENERATION_LEDGER", None)
    env["PATH"] = f"{Path(env['HEKATE_NODE_BIN']).parent}:{env.get('PATH', '')}"
    return config_dir, state_dir, allowance, scope, worker_id, {
        "database_url": database_url,
        "gateway_token": gateway_token,
        "app_token": app_token,
        "request_capture": str(request_capture),
        "env_json": json.dumps(env),
    }


def _env(bundle: dict[str, str]) -> dict[str, str]:
    return json.loads(bundle["env_json"])


def _capsule(request: dict[str, object]) -> tuple[str, dict[str, object]]:
    found = p5a._request_capsule(request)
    if found is None:
        raise ValueError("provider request omitted Task Capsule JSON")
    return found


def _message_texts(request: dict[str, object]) -> list[str]:
    return list(p5a._strings(request.get("messages")))


def _remove_capsule(content: str) -> str:
    marker = "Task Capsule JSON:\n"
    if marker not in content:
        return content
    prefix, suffix = content.split(marker, 1)
    suffix = suffix.lstrip()
    _value, consumed = json.JSONDecoder().raw_decode(suffix)
    return prefix + suffix[consumed:]


class ReviewedResponses:
    """Fake output follows observed request context and generated role contract."""

    def __init__(
        self, topic_id: str, evidence_texts: tuple[str, str], observations: list[dict[str, object]],
        *, auto_register: bool = False,
    ) -> None:
        self.topic_id = topic_id
        self.evidence_texts = evidence_texts
        self.observations = observations
        self.tasks: dict[str, dict[str, object]] = {}
        self.auto_register = auto_register
        self.condition = threading.Condition()

    def expect_task(self, task_id: str, request_key: str, evidence_ids: list[str], *, kind: str) -> None:
        with self.condition:
            self.tasks[task_id] = {"request_key": request_key, "evidence_ids": sorted(evidence_ids), "kind": kind}
            self.condition.notify_all()

    def __call__(self, request: dict[str, object]) -> str:
        prompt, capsule = _capsule(request)
        binding = __import__("re").search(
            r"Trusted runtime binding: task_id=([^;]+); attempt_id=([^;]+); agent_registry_id=([^;]+); input_revision=(\d+)\.",
            prompt,
        )
        if binding is None:
            raise ValueError("provider request omitted trusted Task/attempt/registry/revision binding")
        task_id, attempt_id, registry_id, revision = binding.groups()
        message_text = "\n".join(_message_texts(request)).lower()
        if "server output contract (generated from the strict python" in message_text:
            raise ValueError("reviewed native schema was duplicated as a large inline prompt document")
        with self.condition:
            if task_id not in self.tasks:
                if self.auto_register:
                    self.tasks[task_id] = {
                        "request_key": None,
                        "evidence_ids": sorted(str(item) for item in capsule.get("evidence_refs", [])),
                        "kind": "task_b" if capsule.get("target_position") is not None else "task_a",
                    }
                else:
                    self.condition.wait_for(lambda: task_id in self.tasks, timeout=10)
            expected = self.tasks.get(task_id)
        if expected is None or capsule.get("task_id") != task_id or capsule.get("attempt_id") != attempt_id or capsule.get("input_revision") != int(revision):
            raise ValueError("provider request Task Capsule differs from its registered Task and trusted binding")
        evidence_ids = sorted(str(item) for item in capsule.get("evidence_refs", []))
        evidence = capsule.get("task_data", {}).get("evidence", [])
        if evidence_ids != expected["evidence_ids"] or len(evidence) != 2:
            raise ValueError("request did not contain exactly the two selected Evidence records")
        excerpts = [item.get("excerpt", "") for item in evidence if isinstance(item, dict)]
        if len(excerpts) != 2 or any(text not in "\n".join(excerpts) for text in self.evidence_texts):
            raise ValueError("request did not contain the bounded UTF-8 Evidence excerpts")

        role = capsule.get("reasoning_role")
        synthesis = capsule.get("synthesis_context")
        if role == "critic":
            stage, contract, schema = "task_a_critic_review", "critic_turn_output_v1", critic_turn_output_schema()[0]
            target = capsule.get("review_target")
            if capsule.get("mode") != "targeted_review" or not isinstance(target, dict):
                raise ValueError("Critic request is not targeted review")
            if not all(isinstance(target.get(name), str) and target[name].strip() for name in (
                "purpose", "target_uncertainty", "expected_decision_impact",
            )) or not target.get("candidate_conclusion"):
                raise ValueError("Critic request omitted the accepted candidate or concrete review target")
            if target["candidate_conclusion"].get("status") != "done":
                raise ValueError("Critic candidate was not an uncommitted HEKATE conclusion")
        elif role == "hekate" and isinstance(synthesis, dict):
            stage, contract, schema = "task_a_synthesis", "hekate_turn_output_v1", hekate_turn_output_schema()[0]
            critic = synthesis.get("critic_conclusion")
            dissent = synthesis.get("dissent")
            if not isinstance(critic, dict) or not isinstance(dissent, list) or not dissent:
                raise ValueError("synthesis request omitted the durable Critic conclusion or dissent")
            if not critic.get("objections") or critic["objections"][0].get("claim") != "Test assumptions need independent validation before operational use.":
                raise ValueError("synthesis request did not carry the actual Critic objection")
            dissent_ids = [str(item["id"]) for item in dissent]
            operation = __import__("re").search(r"Commit operation_id: ([^\n]+)", prompt)
            if operation is None:
                raise ValueError("synthesis request omitted the server-selected commit operation")
        elif role == "hekate":
            task_kind = expected["kind"]
            if task_kind == "task_a":
                stage, contract, schema = "task_a_planning", "hekate_turn_output_v1", hekate_turn_output_schema()[0]
                if capsule.get("topic_id") != self.topic_id or capsule.get("target_position") is not None or capsule.get("base_position_version") != 0:
                    raise ValueError("planning did not receive the new topic's independent-exploration snapshot")
            else:
                stage, contract, schema = "task_b_answer", "hekate_turn_output_v1", hekate_turn_output_schema()[0]
                target = capsule.get("target_position")
                if capsule.get("topic_id") != self.topic_id or capsule.get("mode") != "targeted_review":
                    raise ValueError("Task B did not use targeted review for the existing Position")
                target_body = target.get("body") if isinstance(target, dict) else None
                target_statement = target_body.get("statement") if isinstance(target_body, dict) else None
                if (
                    not isinstance(target, dict) or target.get("version") != 1
                    or target.get("topic_id") != self.topic_id
                    or not isinstance(target.get("summary"), str) or not target["summary"].strip()
                    or not isinstance(target_statement, str) or not target_statement.strip()
                ):
                    raise ValueError("Task B omitted the authoritative PostgreSQL Position v1 snapshot")
                outside = "\n".join(_remove_capsule(value) for value in _message_texts(request))
                projection_seen = (
                    "HEKATE Position memory projection" in outside
                    and target_statement in outside
                    and '"source_version":1' in outside
                    and self.topic_id in outside
                )
                if not projection_seen:
                    raise ValueError("Task B provider request omitted read-back verified projected memory outside its capsule")
        else:
            raise ValueError("provider request used an untrusted or unsupported reasoning role")

        response_format = request.get("response_format")
        if response_format != {"type": "json_schema", "json_schema": {"schema": schema}}:
            raise ValueError("final provider request did not carry the role's generated native JSON Schema")
        if request.get("temperature") != 0 or request.get("tools") not in (None, []):
            raise ValueError("reviewed Qwen request changed the fixed temperature or tool policy")

        conclusion: dict[str, object] = {
            "schema_version": "1", "task_id": task_id, "attempt_id": attempt_id,
            "agent_id": registry_id, "status": "done",
            "assessment": {
                "statement": "Candidate A satisfies the stated synthetic limits, subject to checking the assumptions.",
                "confidence": {"level": "medium", "basis": ["the two explicitly selected synthetic Evidence records"]},
            },
            "evidence_used": evidence_ids, "objections": [],
            "assumptions": ["the supplied cost and latency values are test assumptions"],
            "unresolved": [],
            "recommended_next_step": {"type": "none"},
            "position_recommendation": {"action": "update", "summary": "Store the bounded decision with its test-assumption caveat."},
        }
        if stage == "task_a_planning":
            if "Critic" not in capsule.get("objective", "") or "Position" not in capsule.get("objective", ""):
                raise ValueError("Task A question did not request one Critic review before Position storage")
            output: dict[str, object] = {
                "schema_version": "1",
                "proposal": {
                    "schema_version": "1", "action": "spawn", "role": "critic",
                    "purpose": "Check whether the synthetic cost and latency assumptions are being treated as operational facts.",
                    "target_uncertainty": "The candidate metrics are fixture assumptions and may differ from the target environment.",
                    "expected_decision_impact": "If the candidate metrics do not hold, the selected candidate may no longer meet the cost limit.",
                    "task_id": task_id,
                },
                "conclusion": conclusion,
            }
        elif stage == "task_a_critic_review":
            conclusion["assessment"] = {
                "statement": "The candidate meets the stated bounds only under the synthetic fixture values.",
                "confidence": {"level": "medium", "basis": ["reviewed bounded Task evidence"]},
            }
            conclusion["objections"] = [{
                "id": "O1", "severity": "high",
                "claim": "Test assumptions need independent validation before operational use.",
                "condition": "if the fixture values are used to make a live service decision",
                "suggested_validation": "measure monthly cost and p95 latency in the target environment",
            }]
            output = {"schema_version": "1", "conclusion": conclusion}
        elif stage == "task_a_synthesis":
            body = {
                "statement": POSITION_STATEMENT,
                "applicability": ["Only to the supplied synthetic candidate metrics and decision thresholds."],
                "confidence": {"level": "medium", "basis": ["the two selected Evidence records", "the stored Critic review"]},
                "evidence_refs": evidence_ids, "assumptions": ["fixture metrics are test assumptions"],
                "dissent_refs": dissent_ids,
                "uncertainty": "Validate cost and p95 latency against the target environment before operational use.",
            }
            output = {
                "schema_version": "1",
                "proposal": {
                    "schema_version": "1", "action": "commit", "operation_id": operation.group(1),
                    "task_id": task_id, "topic_id": self.topic_id,
                    "base_version": capsule["base_position_version"], "input_revision": int(revision),
                    "proposed_position": body,
                    "reason_for_change": "Persist the candidate decision with the Critic's validation condition.",
                },
                "conclusion": conclusion,
            }
        else:
            if "dissent_ids" in locals():
                raise AssertionError("Critic-only context unexpectedly entered Task B")
            if "Critic 검토" not in capsule.get("objective", ""):
                raise ValueError("Task B question did not constrain the request to a short answer")
            output = {
                "schema_version": "1", "proposal": {
                    "schema_version": "1", "action": "answer",
                    "answer": "Candidate A fits the stated test limits, but its cost and latency values must be checked in the target environment.",
                },
                "conclusion": conclusion,
            }

        self.observations.append({
            "stage": stage, "task_id": task_id, "attempt_id": attempt_id, "registry_id": registry_id,
            "output_contract": contract, "schema_sha256": canonical_json_hash(schema),
            "evidence_ids": evidence_ids,
            "evidence_excerpt_sha256": [hashlib.sha256(item.encode("utf-8")).hexdigest() for item in excerpts],
            "projection_memory_readback_in_actual_task_b_request": stage == "task_b_answer",
            "critic_dissent_ids_in_synthesis": dissent_ids if stage == "task_a_synthesis" else [],
        })
        return json.dumps(output, ensure_ascii=False, separators=(",", ":"))


def _initialize_allowance_and_capture(state_dir: Path) -> None:
    os.chmod(state_dir, 0o700)
    capture = state_dir / "provider-requests.jsonl"
    if capture.is_symlink() or capture.stat().st_mode & 0o777 != 0o600:
        raise ValueError("provider request capture must be a pre-created private file")


def _wait_for_task_and_workflow(engine, task_id: str, timeout: int = 900) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    last: dict[str, object] = {}
    while time.monotonic() < deadline:
        task_rows = p6e.db_read(engine, "SELECT status, outcome, stop_reason FROM tasks WHERE id=:task", {"task": task_id})
        if task_rows:
            last["task"] = task_rows[0]
        workflow = p6e.db_read(engine, """
            SELECT w.stage, w.input_revision, w.planning_operation_id, w.critic_registry_id,
                   w.critic_conclusion_id, w.synthesis_operation_id, w.delete_operation_id,
                   t.input_revision AS task_revision, ar.intended_state AS critic_state
            FROM critic_workflows w JOIN tasks t ON t.id=w.task_id
            JOIN agent_registry ar ON ar.id=w.critic_registry_id WHERE w.task_id=:task
        """, {"task": task_id})
        if workflow:
            last["workflow"] = workflow[0]
        call_states = p6e.db_read(engine, """
            SELECT p.status, p.operation_id, cp.state AS permit_state
            FROM provider_calls p JOIN operations o ON o.id=p.operation_id
            LEFT JOIN call_permits cp ON cp.permit_id=p.permit_id WHERE o.task_id=:task ORDER BY p.created_at
        """, {"task": task_id})
        last["call_states"] = call_states
        operation_states = p6e.db_read(
            engine, "SELECT id, kind, state FROM operations WHERE task_id=:task ORDER BY created_at",
            {"task": task_id},
        )
        last["operation_states"] = operation_states
        if any(item["state"] == "UNKNOWN" for item in operation_states):
            raise RuntimeError(f"runtime operation became UNKNOWN; no further generation is allowed: {last}")
        if any(item["status"] == "UNKNOWN" for item in call_states):
            raise RuntimeError(f"provider execution became UNKNOWN; no further local generation is allowed: {last}")
        status = last.get("task", {}).get("status") if isinstance(last.get("task"), dict) else None
        if status in {"COMPLETED", "FAILED", "CANCELLED", "NEEDS_USER_INPUT"}:
            return last
        time.sleep(0.5)
    raise TimeoutError(f"Task did not reach a terminal state in {timeout}s: {last}")


def _await_workflow_projection(engine, scope: str, task_id: str, topic_id: str, timeout: int = 90) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    last: dict[str, object] = {}
    while time.monotonic() < deadline:
        rows = p6e.db_read(engine, """
            SELECT mp.desired_version, mp.applied_version, mp.observed_memory_version,
                   mp.observed_payload_digest, mp.payload_digest, mp.state, mp.pending_reason,
                   mp.attempt_count, w.stage, ar.intended_state AS critic_state
            FROM memory_projections mp
            LEFT JOIN critic_workflows w ON w.task_id=:task
            LEFT JOIN agent_registry ar ON ar.id=w.critic_registry_id
            WHERE mp.scope=:scope AND mp.topic_id=:topic
        """, {"scope": scope, "topic": topic_id, "task": task_id})
        if rows:
            last = rows[0]
        if last.get("state") == "APPLIED" and last.get("applied_version") == 1 and last.get("stage") == "COMPLETE" and last.get("critic_state") == "DELETED":
            return last
        if last.get("state") in {"DRIFT", "CONFLICT"}:
            raise RuntimeError(f"Projection entered a terminal failure state: {last}")
        time.sleep(0.5)
    raise TimeoutError(f"Critic cleanup and Position projection did not converge: {last}")


def _workflow_snapshot(engine, task_id: str, scope: str) -> dict[str, object]:
    workflow = p6e.db_read(engine, """
        SELECT w.stage, w.parent_attempt_id, w.planning_operation_id, w.input_revision,
               w.critic_registry_id, w.create_operation_id, w.review_attempt_id,
               w.review_operation_id, w.critic_conclusion_id, w.synthesis_attempt_id,
               w.synthesis_operation_id, w.delete_operation_id, ar.provider_agent_id,
               ar.intended_state AS critic_state
        FROM critic_workflows w JOIN agent_registry ar ON ar.id=w.critic_registry_id WHERE w.task_id=:task
    """, {"task": task_id})
    agents = p6e.db_read(engine, """
        SELECT id AS registry_id, role, persistence, provider_agent_id, intended_state
        FROM agent_registry WHERE owner_scope=:scope ORDER BY role, id
    """, {"scope": scope})
    dissent = p6e.db_read(engine, """
        SELECT d.id, d.body FROM dissent d JOIN conclusions c ON c.id=d.conclusion_id
        WHERE d.scope=:scope AND c.task_id=:task ORDER BY d.id
    """, {"scope": scope, "task": task_id})
    return {"workflow": workflow[0] if workflow else None, "agents": agents, "dissent": dissent}


def _projection_snapshot(engine, scope: str, topic_id: str) -> dict[str, object] | None:
    rows = p6e.db_read(engine, """
        SELECT desired_version, applied_version, observed_memory_version,
               observed_payload_digest, payload_digest, state, pending_reason,
               operation_id, request_hash, attempt_count
        FROM memory_projections WHERE scope=:scope AND topic_id=:topic
    """, {"scope": scope, "topic": topic_id})
    return rows[0] if rows else None


def _financial_snapshot(engine, task_id: str, scope: str) -> dict[str, object]:
    accounts = p6e.db_read(engine, """
        SELECT scope_kind, scope_ref, period_id, limit_amount::text, spent_amount::text, held_amount::text
        FROM budget_accounts
        WHERE (scope_kind='TASK' AND scope_ref=:task)
           OR (scope_kind='SYSTEM' AND scope_ref='hekate')
        ORDER BY scope_kind, scope_ref, period_id
    """, {"task": task_id})
    reservations = p6e.db_read(engine, """
        SELECT o.kind AS operation_kind, br.purpose, br.amount::text AS amount,
               br.status, COALESCE(sum(ra.held_amount),0)::text AS held_amount
        FROM operations o LEFT JOIN budget_reservations br ON br.operation_id=o.id
        LEFT JOIN reservation_accounts ra ON ra.reservation_id=br.id
        WHERE o.task_id=:task GROUP BY o.kind, br.purpose, br.amount, br.status
        ORDER BY o.kind, br.purpose
    """, {"task": task_id})
    receipts = p6e.db_read(engine, """
        SELECT operation_id, request_hash, registry_id, receipt, created_at
        FROM position_commit_receipts WHERE scope=:scope ORDER BY created_at, operation_id
    """, {"scope": scope})
    return {"accounts": accounts, "reservations": reservations, "position_commit_receipts": receipts}


def _read_capture(path: Path) -> list[dict[str, object]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = json.loads(line)
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _chat_cli(
    env: dict[str, str], topic_id: str, evidence_ids: list[str], question: str,
    *, replay: bool = False, inspect_position: bool = False,
) -> tuple[str, str, str]:
    lines = [f"/topic {topic_id}", *(f"/evidence {evidence_id}" for evidence_id in evidence_ids)]
    if inspect_position:
        lines.extend(("/status", "/position", "/history"))
    lines.append(question)
    if replay:
        lines.extend(("/task", "/replay"))
    lines.append("/quit")
    # The interactive CLI consumes each line as one command or one Task question.
    result = subprocess.run(
        [sys.executable, "-m", "hekate", "chat", "--wait-seconds", "900"],
        cwd=ROOT, env=env, input="\n".join(lines) + "\n", capture_output=True, text=True,
        check=False, timeout=1_000,
    )
    if result.returncode != 0:
        raise RuntimeError(f"hekate chat failed ({result.returncode}): {(result.stderr or result.stdout)[-900:]}")
    import re
    keys = re.findall(r"Request key: ([A-Za-z0-9-]+)", result.stdout)
    task_ids = re.findall(r"Task ([0-9a-f-]{36}):", result.stdout)
    expected_tasks = 2 if replay else 1
    if not keys or len(set(keys)) != 1 or len(task_ids) != expected_tasks or len(set(task_ids)) != 1:
        raise AssertionError("chat did not expose one stable request key and the expected same Task receipt")
    if "COMPLETED" not in result.stdout:
        raise AssertionError("chat did not display the stored completed Task result")
    return task_ids[0], keys[0], result.stdout


def _chat_wait_then_interrupt(
    env: dict[str, str], engine, scope: str, topic_id: str, evidence_ids: list[str], question: str,
    *, existing_task_id: str,
) -> tuple[str, str, str]:
    process = subprocess.Popen(
        [sys.executable, "-m", "hekate", "chat", "--wait-seconds", "900"],
        cwd=ROOT, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        assert process.stdin is not None
        process.stdin.write(("\n".join([
            f"/topic {topic_id}", *(f"/evidence {item}" for item in evidence_ids),
            "/status", "/position", "/history", question,
        ]) + "\n").encode("utf-8"))
        process.stdin.close()
        assert process.stdout is not None
        os.set_blocking(process.stdout.fileno(), False)
        task_id = None
        output = ""
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            rows = p6e.db_read(engine, "SELECT id,status FROM tasks WHERE owner_scope=:scope ORDER BY created_at", {"scope": scope})
            new_rows = [row for row in rows if str(row["id"]) != existing_task_id]
            if new_rows:
                if len(new_rows) != 1 or new_rows[0]["status"] != "QUEUED":
                    raise AssertionError("chat Task was not left queued while the managed worker was stopped")
                task_id = str(new_rows[0]["id"])
            if task_id is not None:
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    while time.monotonic() < deadline:
                        ready = selector.select(timeout=0.1)
                        if not ready:
                            if process.poll() is not None:
                                break
                            continue
                        try:
                            chunk = os.read(process.stdout.fileno(), 4096).decode("utf-8", "replace")
                        except BlockingIOError:
                            continue
                        output += chunk
                        if "Ctrl+C stops waiting, not the Task." in output:
                            break
                if "Ctrl+C stops waiting, not the Task." in output:
                    break
                if process.poll() is not None:
                    raise RuntimeError(f"chat exited before installing its wait-only interrupt handler: {output[-500:]}")
            if process.poll() is not None:
                raise RuntimeError(f"chat exited before the new Task receipt: {output[-500:]}")
            time.sleep(0.05)
        if task_id is None:
            raise TimeoutError("chat did not durably submit its Task while the worker was stopped")
        before = p6e.task_db_snapshot(engine, task_id)
        if before["task"]["provider_calls"] != 0 or before["calls"]:
            raise AssertionError("chat wait fixture unexpectedly reached a provider call")
        process.send_signal(signal.SIGINT)
        code = process.wait(timeout=15)
        remaining = b""
        try:
            while True:
                chunk = os.read(process.stdout.fileno(), 4096)
                if not chunk:
                    break
                remaining += chunk
        except BlockingIOError:
            pass
        stdout = output + remaining.decode("utf-8", "replace")
        stderr = process.stderr.read().decode("utf-8", "replace") if process.stderr is not None else ""
        if code != 0:
            raise RuntimeError(f"chat Ctrl+C exited unexpectedly ({code}): {stderr[-500:]} {stdout[-500:]}")
        keys = re.findall(r"Request key: ([A-Za-z0-9-]+)", stdout)
        visible_ids = re.findall(r"Task ([0-9a-f-]{36}):", stdout)
        if (
            len(keys) != 1 or visible_ids != [task_id]
            or "The Task is still active" not in stdout
            or "FAILED" in stdout or "CANCELLED" in stdout
        ):
            raise AssertionError("chat Ctrl+C did not preserve and report the submitted Task identity/state")
        after = p6e.task_db_snapshot(engine, task_id)
        if before != after or after["task"]["status"] != "QUEUED":
            raise AssertionError("stopping chat wait changed the Task, response, permit, or accounting state")
        return task_id, keys[0], stdout
    finally:
        if process.poll() is None:
            p6e.stop_process(process)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                stream.close()


def _json_cli(env: dict[str, str], *args: str) -> dict[str, object]:
    result = run([sys.executable, "-m", "hekate", *args], env=env, timeout=60)
    if result.returncode != 0:
        raise RuntimeError(f"hekate {' '.join(args[:1])} failed ({result.returncode}): {(result.stderr or result.stdout)[-900:]}")
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        raise ValueError("CLI did not return a JSON object")
    return value


def _start_managed_run(env: dict[str, str], state_dir: Path, label: str) -> tuple[subprocess.Popen[bytes], dict[str, Any]]:
    process = p6e.start_process(
        [sys.executable, "-m", "hekate", "run"], env, state_dir / f"{label}-run.log",
    )
    try:
        deadline = time.monotonic() + 90
        doctor: dict[str, Any] = {}
        while time.monotonic() < deadline:
            if process.poll() is not None:
                log_path = state_dir / f"{label}-run.log"
                tail = log_path.read_text(encoding="utf-8", errors="replace")[-1_200:] if log_path.exists() else ""
                raise RuntimeError(f"hekate run exited with status {process.returncode}: {tail}")
            doctor = p6e.cli(env, "doctor")[0]
            if doctor.get("status") == "READY":
                break
            time.sleep(0.5)
        if doctor.get("status") != "READY":
            log_path = state_dir / f"{label}-run.log"
            tail = log_path.read_text(encoding="utf-8", errors="replace")[-1_200:] if log_path.exists() else ""
            raise RuntimeError(f"hekate run did not reach readiness: {doctor.get('status')}; {tail}")
        if process.poll() is not None:
            raise RuntimeError("hekate run exited after reporting readiness")
        return process, doctor
    except BaseException:
        p6e.stop_process(process)
        raise


def _pid_is_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def _validate_capture_records(
    records: list[dict[str, object]], topic_id: str, evidence_ids: list[str], request_keys: dict[str, str],
) -> dict[str, dict[str, object]]:
    schemas = {
        "hekate_turn_output_v1": hekate_turn_output_schema(),
        "critic_turn_output_v1": critic_turn_output_schema(),
    }
    by_stage: dict[str, dict[str, object]] = {}
    for record in records:
        body = record.get("request_body")
        if not isinstance(body, dict):
            raise ValueError("captured provider request is missing its final body")
        raw = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if hashlib.sha256(raw).hexdigest() != record.get("request_digest"):
            raise ValueError("captured request body differs from the digest measured by the gateway")
        contract = str(record.get("output_contract"))
        expected = schemas.get(contract)
        response_format = body.get("response_format")
        if expected is None or response_format != {"type": "json_schema", "json_schema": {"schema": expected[0]}}:
            raise ValueError("captured final provider body does not contain the generated schema for its DB-bound contract")
        if body.get("temperature") != 0 or body.get("tools") not in (None, []):
            raise ValueError("captured final provider body violates the fixed temperature or tool boundary")
        prompt, capsule = _capsule(body)
        if capsule.get("topic_id") != topic_id:
            raise ValueError("captured Task Capsule has a different topic")
        got_ids = sorted(str(item) for item in capsule.get("evidence_refs", []))
        if got_ids != sorted(evidence_ids):
            raise ValueError("captured Task Capsule does not match the selected Evidence set")
        excerpt_text = "\n".join(
            item.get("excerpt", "") for item in capsule.get("task_data", {}).get("evidence", []) if isinstance(item, dict)
        )
        if EVIDENCE_A not in excerpt_text or EVIDENCE_B not in excerpt_text:
            raise ValueError("captured provider request lacks the imported bounded Evidence excerpts")
        key = record.get("task_request_key")
        role = capsule.get("reasoning_role")
        context = capsule.get("synthesis_context")
        if key == request_keys["task_a"]:
            stage = "planning" if role == "hekate" and not isinstance(context, dict) and capsule.get("mode") == "independent_exploration" else (
                "critic_review" if role == "critic" else "synthesis" if role == "hekate" and isinstance(context, dict) else "invalid"
            )
        elif key == request_keys["task_b"]:
            outside = "\n".join(_remove_capsule(value) for value in _message_texts(body))
            target = capsule.get("target_position")
            target_body = target.get("body") if isinstance(target, dict) else None
            target_statement = target_body.get("statement") if isinstance(target_body, dict) else None
            stage = "task_b_answer" if (
                role == "hekate" and capsule.get("mode") == "targeted_review"
                and isinstance(target, dict) and target.get("version") == 1
                and target.get("topic_id") == topic_id
                and isinstance(target.get("summary"), str) and target["summary"].strip()
                and isinstance(target_statement, str) and target_statement.strip()
                and target_statement in outside and '"source_version":1' in outside
                and topic_id in outside and "HEKATE Position memory projection" in outside
            ) else "invalid"
        else:
            stage = "invalid"
        if stage == "invalid" or stage in by_stage:
            raise ValueError(f"captured final request has an unapproved or duplicated workflow stage: {stage}")
        if stage == "planning":
            if capsule.get("target_position") is not None or capsule.get("base_position_version") != 0:
                raise ValueError("planning request does not carry the empty authoritative Position snapshot")
        elif stage == "critic_review":
            target = capsule.get("review_target")
            candidate = target.get("candidate_conclusion") if isinstance(target, dict) else None
            if (
                capsule.get("mode") != "targeted_review"
                or not isinstance(candidate, dict)
                or candidate.get("status") != "done"
                or not all(isinstance(target.get(name), str) and target[name].strip() for name in (
                    "purpose", "target_uncertainty", "expected_decision_impact",
                ))
            ):
                raise ValueError("Critic request does not carry the bounded candidate and concrete review target")
        elif stage == "synthesis":
            synthesis = capsule.get("synthesis_context")
            critic = synthesis.get("critic_conclusion") if isinstance(synthesis, dict) else None
            dissent = synthesis.get("dissent") if isinstance(synthesis, dict) else None
            objections = critic.get("objections") if isinstance(critic, dict) else None
            if (
                not isinstance(critic, dict)
                or not isinstance(objections, list)
                or not isinstance(dissent, list)
                or any(not isinstance(item, dict) for item in objections)
                or any(not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"] for item in dissent)
            ):
                raise ValueError("synthesis request omits the stored Critic conclusion or dissent list")
        elif stage == "task_b_answer":
            target = capsule.get("target_position")
            if (
                not isinstance(target, dict)
                or target.get("topic_id") != topic_id
                or target.get("version") != 1
                or not isinstance(target.get("summary"), str)
                or not target["summary"].strip()
            ):
                raise ValueError("Task B request does not carry the authoritative Position summary")
        expected_contract = "critic_turn_output_v1" if stage == "critic_review" else "hekate_turn_output_v1"
        if contract != expected_contract:
            raise ValueError(f"captured final request schema does not match trusted stage {stage}")
        expected_binding = {
            "planning": ("hekate.turn", "planning", "hekate", "persistent"),
            "critic_review": ("critic.review", "critic_review", "critic", "ephemeral"),
            "synthesis": ("hekate.synthesis", "synthesis", "hekate", "persistent"),
            "task_b_answer": ("hekate.turn", "planning", "hekate", "persistent"),
        }[stage]
        if (
            record.get("operation_kind"), record.get("attempt_kind"),
            record.get("registry_kind"), record.get("registry_persistence"),
        ) != expected_binding:
            raise ValueError(f"captured provider request does not match its DB-bound runtime role for {stage}")
        by_stage[stage] = {
            "operation_id": record.get("operation_id"),
            "accounting_call_id": record.get("accounting_call_id"),
            "task_id": record.get("task_id"),
            "attempt_id": record.get("attempt_id"),
            "registry_id": record.get("registry_id"),
            "output_contract": contract,
            "request_digest": record.get("request_digest"),
            "profile_digest": record.get("profile_digest"),
            "input_schema_sha256": expected[1],
            "messages_sha256": canonical_json_hash(body.get("messages")),
            "context_checked": True,
            **({
                "critic_objection_count": len(objections),
                "durable_dissent_count": len(dissent),
            } if stage == "synthesis" else {}),
        }
    return by_stage


def _fingerprint() -> tuple[str, list[dict[str, str]]]:
    listing = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True).stdout
    paths = {
        item.decode() for item in listing.split(b"\0") if item
        and "__pycache__" not in Path(item.decode()).parts
        and Path(item.decode()).suffix not in {".pyc", ".pyo"}
    }
    paths.update({
        "scripts/phase6e_local_cli_probe.py", "scripts/phase6f_reviewed_qwen_probe.py",
        "tests/unit/test_phase6f_generation_allowance.py", "README.md", "docs/local-quickstart.md",
        "docs/personal-use.md",
    })
    paths.update(str(path.relative_to(ROOT)) for base in (
        ROOT / "src/hekate", ROOT / "tests/unit", ROOT / "config/local-reviewed.example",
        ROOT / "config/personal-local.example", ROOT / "migrations/versions",
    ) for path in base.rglob("*") if path.is_file()
        and "__pycache__" not in path.parts and path.suffix not in {".pyc", ".pyo"})
    entries: list[dict[str, str]] = []
    digest = hashlib.sha256()
    for name in sorted(
        path for path in paths if (ROOT / path).is_file()
        and "__pycache__" not in Path(path).parts
        and Path(path).suffix not in {".pyc", ".pyo"}
    ):
        body = (ROOT / name).read_bytes()
        file_digest = hashlib.sha256(body).hexdigest()
        digest.update(name.encode("utf-8") + b"\0" + bytes.fromhex(file_digest))
        entries.append({"path": name, "sha256": file_digest})
    return digest.hexdigest(), entries


def _new_run_report(mode: str, run_id: str, *, personal_local: bool = False) -> dict[str, object]:
    candidate = load_qwen_candidate_profile()
    local_profile, prices = qwen35_reviewed_native_json_schema_local_execution_profile(candidate)
    lock = json.loads((ROOT / "integration/letta/versions.lock.json").read_text(encoding="utf-8"))
    return {
        "schema_version": "1", "probe": "personal-local-cli" if personal_local else "phase6f-reviewed-qwen",
        "personal_local": personal_local, "mode": mode,
        "run_id": run_id, "executed_at": now_utc(), "baseline_sha": BASELINE_SHA,
        "branch": None, "head": None, "status": "BLOCKED", "production_dispatch": "BLOCKED",
        "actual_provider_generations": 0, "fake_provider_requests": 0,
        "ollama_model_load_requests": 0 if mode == "fake" else None,
        "hosted_provider_calls": 0 if mode == "fake" else None,
        "external_provider_calls": 0 if mode == "fake" else None,
        "candidate": {
            "model": candidate.model, "model_manifest_digest": candidate.model_manifest_digest,
            "ollama_version": candidate.ollama_version,
            "ollama_source_revision": candidate.ollama_source_revision,
            "profile_id": local_profile.profile_id, "profile_digest": local_profile.content_digest,
            "pricing_version": prices.version, "pricing_synthetic": prices.synthetic,
            "hekate_schema_sha256": hekate_turn_output_schema()[1],
            "critic_schema_sha256": critic_turn_output_schema()[1],
        },
        "runtime_lock": {
            "app_server_version": lock["app_server"]["letta_code_version"],
            "sdk_version": lock["bridge"]["sdk"]["version"],
            "protocol_version": lock["app_server"]["protocol_version"],
            "node_version": lock["bridge"]["node_version"],
            "app_server_source_commit": lock["app_server"]["source_commit"],
            "runtime_patch_sha256": lock["patches"][0]["sha256"],
        },
        "legacy_state": {
            "Phase 6D/6E databases and prior UNKNOWN calls": "not queried or modified",
            "Phase 6E allowance and state": "not read, reset, or reused",
            "old local Qwen profile": "preserved and separately validated",
        },
        "unexecuted": [],
    }


def _finalize(report: dict[str, object], artifact: Path) -> int:
    digest, files = _fingerprint()
    report["code_fingerprint_sha256"] = digest
    report["fingerprinted_files"] = files
    report["artifact_path"] = str(artifact.relative_to(ROOT))
    report["finalized_at"] = now_utc()
    artifact.parent.mkdir(parents=True, exist_ok=True)
    temp = artifact.with_suffix(artifact.suffix + ".tmp")
    temp.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.chmod(temp, 0o600)
    os.replace(temp, artifact)
    print(json.dumps({"status": report.get("status"), "artifact": str(artifact), "fingerprint": digest}, ensure_ascii=False))
    return 0 if report.get("status") == "PASS" else 1


def _run_mode(
    mode: str, run_id: str, run_dir: Path, report: dict[str, object], *, image: str,
    node_bin: str, personal_local: bool = False,
) -> None:
    fake = None
    gateway = worker = managed_run = None
    container = f"hekate-p6f-runtime-{run_id[-8:]}"
    database_engine = None
    bundle: dict[str, str] = {}
    env: dict[str, str] | None = None
    state_dir: Path | None = None
    gateway_token: str | None = None
    gateway_port = letta_port = 0
    scope = ""
    try:
        db_base, db_port, pg_container, pg_volume = configure_postgres(run_id)
        database_name = "hekate_p6f_fake" if mode == "fake" else "hekate_p6f_real"
        database_url = _db_url(db_base, database_name)
        database_engine = create_engine(database_url)
        gateway_port, letta_port = free_port(), free_port()
        mode_run_dir = run_dir / mode
        mode_run_dir.mkdir(parents=True, mode=0o700)
        config_dir, state_dir, allowance, scope, _worker_id, bundle = prepare_config(
            mode_run_dir, run_id, mode, database_url, gateway_port, letta_port,
            fake_port=None, personal_local=personal_local,
        )
        _initialize_allowance_and_capture(state_dir)
        env = _env(bundle)
        gateway_token = bundle["gateway_token"]
        capture_path = Path(bundle["request_capture"])
        report["private_state_dir"] = str(state_dir)
        report["database"] = {
            "container": pg_container, "volume": pg_volume, "port": db_port,
            "database": database_name, "host": "127.0.0.1",
        }
        report["scope_id"] = scope
        report["gateway_port"] = gateway_port
        report["runtime_state"] = "isolated new state directory; persisted for review"

        init, _ = p6e.cli(env, "init-local")
        report["init_local"] = init
        report["postgres"] = p6e.db_meta(database_engine)
        if init.get("scope_created") is not True:
            raise AssertionError("new Phase 6F database did not create its isolated authorization scope")
        if personal_local:
            report["migration_checks"] = _migration_cycle(env, database_url)
        pre_doctor, _ = p6e.cli(env, "doctor")
        if pre_doctor.get("status") != "PRESTART_OK" or pre_doctor.get("inference_requested") is not False:
            raise AssertionError("new local reviewed config did not pass read-only prestart doctor")
        report["doctor_prestart"] = pre_doctor

        candidate = load_qwen_candidate_profile()
        real_model_preflight: dict[str, object] | None = None
        if mode == "real":
            # This only reads the pinned Ollama metadata and loaded-runner state. It is delayed until fake passes.
            metadata = p6c._verify_live_ollama_metadata(candidate)
            loaded = p6c._observe_current_loaded_runner(candidate)
            model_only_load = None
            if loaded.get("loaded") is not True:
                model_only_load = p6c._ollama_model_only_load(candidate, timeout_seconds=600)
                loaded = p6c._observe_current_loaded_runner(candidate)
            if loaded.get("context_verified_for_this_loaded_runner") is not True:
                raise RuntimeError("Ollama loaded runner context was not verified at the fixed 8192 token gate")
            real_model_preflight = {
                "ollama_version": metadata.get("version"),
                "model_manifest_digest": metadata.get("manifest_digest"),
                "model_size_bytes": metadata.get("model_size_bytes"),
                "tokenizer_array_sha256": metadata.get("tokenizer_array_sha256"),
                "template_sha256": metadata.get("template_sha256"),
                "loaded_context_tokens": loaded.get("context_length_tokens"),
                "required_context_tokens": 8192,
                "model_only_load": model_only_load,
                "inference_requests_before_task": 0,
            }
            report["qwen_preflight"] = real_model_preflight
        else:
            fake = p3.FakeProvider()
            original_model = p3.FAKE_MODEL
            p3.FAKE_MODEL = candidate.model
            report["_fake_original_model"] = original_model
            fake.start()
            local = yaml.safe_load((config_dir / "local.yaml").read_text(encoding="utf-8"))
            local["gateway"]["upstream_base_url"] = f"http://127.0.0.1:{fake.port}"
            local["gateway"]["upstream_api_key"] = "p6f-isolated-fake-only"
            write_yaml(config_dir / "local.yaml", local)
            report["fake_provider_port"] = fake.port

        env["HEKATE_RUNTIME_MODE"] = "test" if mode == "fake" else "local"
        env["HEKATE_NODE_BIN"] = node_bin
        env["HEKATE_MEMORY_PROJECTION_ENABLED"] = "true"
        env["PATH"] = f"{Path(node_bin).parent}:{env.get('PATH', '')}"
        runtime = p6e.verify_image(image)
        report["pinned_runtime"] = runtime
        if personal_local:
            # The App Server remains external; run owns only its gateway and worker children.
            gateway = p6e.start_gateway(env, state_dir, f"{mode}-bootstrap")
            p6e.start_app_server(
                image, container, state_dir / "letta", bundle["app_token"], gateway_port,
                gateway_token, letta_port,
            )
            p6e.stop_process(gateway)
            gateway = None
            managed_run, ready = _start_managed_run(env, state_dir, mode)
            report["run_process_management"] = {
                "started_gateway_and_worker": True,
                "app_server_external": True,
                "doctor_status": ready.get("status"),
            }
        else:
            gateway = p6e.start_gateway(env, state_dir, mode)
            p6e.start_app_server(
                image, container, state_dir / "letta", bundle["app_token"], gateway_port,
                gateway_token, letta_port,
            )
            ready = p6e.wait_doctor_ready(env)
            worker = p6e.start_worker(env, state_dir, mode)
        if ready.get("status") != "READY" or ready.get("inference_requested") is not False:
            raise AssertionError(f"pinned runtime doctor did not reach READY: {ready.get('status')}")
        report["doctor_running"] = ready

        observation_list: list[dict[str, object]] = []
        topic_id = TOPIC_PREFIX + run_id[-12:]
        evidence_files = [mode_run_dir / "evidence-a.txt", mode_run_dir / "evidence-b.txt"]
        for path, body in zip(evidence_files, (EVIDENCE_A, EVIDENCE_B), strict=True):
            path.write_text(body + "\n", encoding="utf-8")
            if len(path.read_bytes()) > 1024:
                raise AssertionError("Phase 6F Evidence fixture exceeded 1 KiB")
        imported = []
        expires = (datetime.now(UTC) + timedelta(days=30)).isoformat().replace("+00:00", "Z")
        for index, path in enumerate(evidence_files, start=1):
            value, _ = p6e.cli(
                env, "evidence", "import", str(path), "--request-key", f"p6f-{mode}-{run_id}-evidence-{index}",
                "--kind", "document", "--retention-class", "fixture-retained", "--expires-at", expires,
            )
            imported.append(value)
        evidence_ids = [str(value["id"]) for value in imported]
        responses = ReviewedResponses(
            topic_id, (EVIDENCE_A, EVIDENCE_B), observation_list,
            auto_register=personal_local,
        )
        if fake is not None:
            fake.set_response_factory(responses)

        task_a_key = f"owner-{run_id}-decision" if personal_local else f"p6f-{mode}-{run_id}-task-a"
        task_b_key = f"owner-{run_id}-reuse" if personal_local else f"p6f-{mode}-{run_id}-task-b"
        question_a = (
            "두 근거의 수치는 테스트 가정입니다. 첫 응답은 반드시 proposal.action=spawn, role=critic으로 한 명의 "
            "Critic 검토를 요청하세요. 이 planning 응답에서는 answer, request_information, abstain, commit을 제출하지 마세요. "
            "Critic 결과를 받은 뒤 같은 HEKATE가 검토를 반영해 후보와 적용 조건을 Position으로 저장하세요. "
            "추가 검토나 continuation은 요청하지 마세요."
        )
        if personal_local:
            task_a_id, task_a_key, chat_a = _chat_cli(
                env, topic_id, evidence_ids, question_a, replay=True,
            )
            report["chat_task_a"] = {
                "task_id": task_a_id, "request_key": task_a_key,
                "same_key_replay_in_chat": True, "stored_response_displayed": True,
                "topic_and_evidence_selection": True,
            }
        else:
            ask_a, _ = p6e.cli(
                env, "ask", "--request-key", task_a_key, "--wait-seconds", "0", "--topic-id", topic_id,
                "--evidence-id", evidence_ids[0], "--evidence-id", evidence_ids[1], stdin=question_a,
            )
            task_a = ask_a.get("task")
            if not isinstance(task_a, dict) or not isinstance(task_a.get("task_id"), str):
                raise AssertionError("ordinary CLI Task A submission omitted its Task ID")
            task_a_id = str(task_a["task_id"])
            responses.expect_task(task_a_id, task_a_key, evidence_ids, kind="task_a")
        result_a = _wait_for_task_and_workflow(database_engine, task_a_id)
        report["task_a"] = {"task_id": task_a_id, "request_key": task_a_key, "question": question_a, **result_a}
        if result_a.get("task", {}).get("status") != "COMPLETED":
            raise AssertionError(f"Task A did not complete its reviewed synthesis: {result_a}")
        work = result_a.get("workflow")
        if not isinstance(work, dict) or work.get("stage") != "COMPLETE":
            raise AssertionError(f"Task A did not create, review, synthesize, and retire one Critic: {work}")
        if mode == "fake":
            if len(observation_list) != 3 or {item["stage"] for item in observation_list} != {
                "task_a_planning", "task_a_critic_review", "task_a_synthesis",
            }:
                raise AssertionError(f"Task A fake provider did not observe planning/review/synthesis exactly once: {observation_list}")
        else:
            task_a_capture = _validate_capture_records(
                _read_capture(capture_path), topic_id, evidence_ids,
                {"task_a": task_a_key, "task_b": task_b_key},
            )
            if set(task_a_capture) != {"planning", "critic_review", "synthesis"}:
                raise AssertionError(f"Task A captured provider requests do not prove planning/review/synthesis exactly once: {sorted(task_a_capture)}")
            report["task_a_provider_request_stages"] = task_a_capture
        projection = _await_workflow_projection(database_engine, scope, task_a_id, topic_id)
        report["task_a_workflow"] = _workflow_snapshot(database_engine, task_a_id, scope)
        report["task_a_financial_state"] = _financial_snapshot(database_engine, task_a_id, scope)
        report["projection_after_task_a"] = projection
        report["task_a_position"] = p6e.cli(env, "position", "show", topic_id)[0]
        report["task_a_history"] = p6e.cli(env, "position", "history", topic_id, "--after-version", "0", "--limit", "50")[0]
        if report["task_a_position"].get("current_version") != 1 or len(report["task_a_history"].get("items", [])) != 1:
            raise AssertionError("Task A did not produce exactly Position version 1")
        if personal_local:
            report["gateway_metrics_before_restart"] = p6e.http_json(
                f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token,
            )

        # Keep App Server memory/runtime state and PostgreSQL intact while replacing only the worker process.
        if personal_local:
            first_run_log = state_dir / f"{mode}-run.log"
            p6e.stop_process(managed_run)
            child_pids = _managed_child_pids(first_run_log)
            child_processes_stopped = all(_pid_is_gone(pid) for pid in child_pids.values())
            managed_run = None
            after_run_stop = p6e.cli(env, "doctor")[0]
            running = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", container],
                cwd=ROOT, capture_output=True, text=True, check=True, timeout=20,
            ).stdout.strip()
            if (
                not child_processes_stopped
                or after_run_stop.get("checks", {}).get("provider_gateway", {}).get("status") != "NOT_RUNNING"
                or after_run_stop.get("checks", {}).get("letta_runtime", {}).get("status") != "READY"
                or running.lower() != "true"
                or fake is None or fake.count() != 3
            ):
                raise AssertionError("hekate run stopped an external service or failed to keep its fake provider independent")
            report["run_process_management"].update({
                "app_server_survived_managed_shutdown": True,
                "fake_provider_survived_managed_shutdown": True,
                "managed_children_stopped": child_processes_stopped,
                "gateway_and_worker_restart_pending": True,
            })
        else:
            p6e.stop_process(worker)
            worker = p6e.start_worker(env, state_dir, f"{mode}-restarted")
        question_b = "현재 저장한 판단과 적용 조건을 짧게 설명해 주세요. 추가 Critic 검토나 Position 변경은 요청하지 마세요."
        if personal_local:
            task_b_id, task_b_key, chat_b = _chat_wait_then_interrupt(
                env, database_engine, scope, topic_id, evidence_ids, question_b,
                existing_task_id=task_a_id,
            )
            report["chat_task_b"] = {
                "task_id": task_b_id, "request_key": task_b_key,
                "position_and_history_commands": True, "status_command": True,
                "ctrl_c_stopped_wait_only": True, "task_remained_queued": True,
            }
            managed_run, _resumed_doctor = _start_managed_run(env, state_dir, f"{mode}-restarted")
            report["run_process_management"]["gateway_and_worker_restarted"] = True
        else:
            ask_b, _ = p6e.cli(
                env, "ask", "--request-key", task_b_key, "--wait-seconds", "0", "--topic-id", topic_id,
                "--evidence-id", evidence_ids[0], "--evidence-id", evidence_ids[1], stdin=question_b,
            )
            task_b = ask_b.get("task")
            if not isinstance(task_b, dict) or not isinstance(task_b.get("task_id"), str):
                raise AssertionError("ordinary CLI Task B submission omitted its Task ID")
            task_b_id = str(task_b["task_id"])
            responses.expect_task(task_b_id, task_b_key, evidence_ids, kind="task_b")
        result_b = _wait_for_task_and_workflow(database_engine, task_b_id)
        report["task_b"] = {"task_id": task_b_id, "request_key": task_b_key, "question": question_b, **result_b}
        if result_b.get("task", {}).get("status") != "COMPLETED":
            raise AssertionError(f"Task B did not return the existing Position summary: {result_b}")
        if mode == "fake":
            if not any(item["stage"] == "task_b_answer" and item["projection_memory_readback_in_actual_task_b_request"] for item in observation_list):
                raise AssertionError("Task B fake provider did not verify read-back memory in the actual request")
        else:
            task_b_capture = _validate_capture_records(
                _read_capture(capture_path), topic_id, evidence_ids,
                {"task_a": task_a_key, "task_b": task_b_key},
            )
            if set(task_b_capture) != {"planning", "critic_review", "synthesis", "task_b_answer"}:
                raise AssertionError(f"Task B captured requests do not prove memory reuse after worker restart: {sorted(task_b_capture)}")
            report["task_b_provider_request_stages"] = task_b_capture
        position_b = p6e.cli(env, "position", "show", topic_id)[0]
        history_b = p6e.cli(env, "position", "history", topic_id, "--after-version", "0", "--limit", "50")[0]
        if position_b.get("current_version") != 1 or len(history_b.get("items", [])) != 1:
            raise AssertionError("Task B changed the authoritative Position version")
        report["task_b_position"] = position_b
        report["task_b_history"] = history_b

        before_replay = {
            task_a_id: p6e.task_db_snapshot(database_engine, task_a_id),
            task_b_id: p6e.task_db_snapshot(database_engine, task_b_id),
            "projection": _projection_snapshot(database_engine, scope, topic_id),
            "workflow": _workflow_snapshot(database_engine, task_a_id, scope),
            "financial": _financial_snapshot(database_engine, task_a_id, scope),
        }
        allowance_before = json.loads(allowance.read_text(encoding="utf-8")) if allowance is not None else None
        for key, question, ids in ((task_a_key, question_a, evidence_ids), (task_b_key, question_b, evidence_ids)):
            replay, _ = p6e.cli(
                env, "ask", "--request-key", key, "--wait-seconds", "0", "--topic-id", topic_id,
                "--evidence-id", ids[0], "--evidence-id", ids[1], stdin=question,
            )
            expected_task = task_a_id if key == task_a_key else task_b_id
            if replay.get("task", {}).get("task_id") != expected_task:
                raise AssertionError("same-key Task replay returned a different Task")
        after_replay = {
            task_a_id: p6e.task_db_snapshot(database_engine, task_a_id),
            task_b_id: p6e.task_db_snapshot(database_engine, task_b_id),
            "projection": _projection_snapshot(database_engine, scope, topic_id),
            "workflow": _workflow_snapshot(database_engine, task_a_id, scope),
            "financial": _financial_snapshot(database_engine, task_a_id, scope),
        }
        allowance_after = json.loads(allowance.read_text(encoding="utf-8")) if allowance is not None else None
        if before_replay != after_replay or allowance_before != allowance_after:
            raise AssertionError("same-key replay changed Task, accounting, Position, projection, workflow, or generation claims")
        report["same_key_replay"] = {
            "database_effects_unchanged": True,
            "allowance_unchanged": allowance_before == allowance_after,
            "provider_request_count": len(_read_capture(capture_path)),
            "before": before_replay, "after": after_replay,
        }
        captures = _read_capture(capture_path)
        capture_stages = _validate_capture_records(
            captures, topic_id, evidence_ids, {"task_a": task_a_key, "task_b": task_b_key},
        )
        if set(capture_stages) != {"planning", "critic_review", "synthesis", "task_b_answer"}:
            raise AssertionError(f"final provider request capture did not contain the four approved stages: {capture_stages.keys()}")
        allowance_value = json.loads(allowance.read_text(encoding="utf-8")) if allowance is not None else None
        if personal_local:
            caps = p6e.db_read(database_engine, "SELECT id,provider_calls,max_provider_calls FROM tasks WHERE id IN (:a,:b) ORDER BY id", {"a": task_a_id, "b": task_b_id})
            calls_by_task = {str(row["id"]): (row["provider_calls"], row["max_provider_calls"]) for row in caps}
            if calls_by_task.get(task_a_id) != (3, 3) or calls_by_task.get(task_b_id) != (1, 3):
                raise AssertionError(f"durable per-Task generation cap was not enforced: {calls_by_task}")
            if allowance is not None or (state_dir / "generation-allowance.json").exists():
                raise AssertionError("personal CLI created or used the verification allowance")
            report["per_task_generation_cap"] = {
                "task_a_planning_critic_synthesis": calls_by_task[task_a_id],
                "task_b_answer": calls_by_task[task_b_id],
                "limit": 3,
                "same_key_replay_created_no_call": True,
            }
        elif (
            allowance_value is None
            or len(allowance_value.get("claims", {})) != 4
            or set(allowance_value["claims"]) != {
                "task_a_planning", "task_a_critic_review", "task_a_synthesis", "task_b_answer",
            }
        ):
            raise AssertionError("durable generation allowance does not contain exactly the four reviewed workflow claims")

        metrics = p6e.http_json(f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token)
        expected_upstream_count = 4
        gateway_attempts = metrics.get("upstream_forward_attempts", 0)
        if personal_local:
            first_gateway_attempts = report["gateway_metrics_before_restart"].get("upstream_forward_attempts", 0)
            if first_gateway_attempts != 3:
                raise AssertionError(f"pre-restart gateway observed {first_gateway_attempts} forwards, expected three")
            gateway_attempts += first_gateway_attempts
        if gateway_attempts != expected_upstream_count:
            raise AssertionError(f"private gateway observed {gateway_attempts} forwards, expected four across worker restart")
        real_generations = expected_upstream_count if mode == "real" else 0
        all_task_snapshots = {
            task_a_id: p6e.task_db_snapshot(database_engine, task_a_id),
            task_b_id: p6e.task_db_snapshot(database_engine, task_b_id),
        }
        calls = [call for item in all_task_snapshots.values() for call in item.get("calls", [])]
        if len(calls) != 4:
            raise AssertionError(f"expected four physical-call accounting rows, observed {len(calls)}")
        call_checks = []
        for call in calls:
            measurement = call.get("measurement_data") or {}
            exact_input_usage = call.get("input_tokens") == measurement.get("measured_input_tokens")
            good = (
                call.get("call_status") == "QUIESCENT"
                and call.get("permit_state") == "CONSUMED"
                and call.get("usage_completeness") == "COMPLETE"
                and call.get("settlement_state") == "SETTLED"
                and call.get("measurement_status") == "MEASURED"
                and call.get("profile_digest") == report["candidate"]["profile_digest"]
                and call.get("execution_mode") == ("synthetic_test" if mode == "fake" else "local_candidate")
                and call.get("price_synthetic") is (mode == "fake")
                and (mode == "fake" or exact_input_usage)
            )
            call_checks.append({
                "accounting_call_id": call.get("accounting_call_id"), "operation_id": call.get("operation_id"),
                "permit_state": call.get("permit_state"), "call_status": call.get("call_status"),
                "usage_completeness": call.get("usage_completeness"), "settlement_state": call.get("settlement_state"),
                "measurement_status": call.get("measurement_status"),
                "measured_input_tokens": measurement.get("measured_input_tokens"),
                "provider_input_tokens": call.get("input_tokens"), "input_usage_matches_measurement": exact_input_usage,
                "price_synthetic": call.get("price_synthetic"), "evaluated_cost_usd": call.get("evaluated_cost_usd"),
                "passed": good,
            })
        if any(item["passed"] is not True for item in call_checks):
            raise AssertionError(f"provider call completion or accounting did not match evidence: {call_checks}")
        if mode == "fake" and fake is not None and fake.count() != 4:
            raise AssertionError(f"fake provider received {fake.count()} requests, expected exactly four")

        report["normal_cli_path"] = {
            "task_a_id": task_a_id, "task_b_id": task_b_id,
            "topic_id": topic_id, "evidence_ids": evidence_ids,
            "position_version": position_b.get("current_version"),
            "critic_cleanup": report["task_a_workflow"].get("workflow", {}).get("critic_state"),
            "projection": projection,
            "provider_request_stages": capture_stages,
            "fake_provider_requests": fake.count() if fake is not None else 0,
            "gateway_forward_attempts": gateway_attempts,
            "accounting_checks": call_checks,
            "provider_calls": calls,
        }
        if personal_local:
            report["backup_restore"] = _verify_backup_restore(
                db_base=db_base, db_container=pg_container, run_dir=run_dir, env=env,
                config_dir=config_dir, state_dir=state_dir,
                report=report, database_engine=database_engine, fake=fake, managed_run=managed_run,
                app_server_name=container, app_token=bundle["app_token"],
                gateway_token=gateway_token, image=image,
                task_id=task_a_id, task_ids=(task_a_id, task_b_id), topic_id=topic_id,
                evidence_ids=evidence_ids, task_snapshots=all_task_snapshots, run_id=run_id,
            )
            managed_run = None
            unknown = _seed_unknown_reconciliation_fixture(env, database_engine, scope, run_id)
            unknown_before = unknown["before"]
            read_only_unknown = _json_cli(env, "reconcile")
            apply_unknown = _json_cli(env, "reconcile", "--apply")
            replay_unknown = _json_cli(env, "reconcile", "--apply")
            unknown_after = _unknown_fixture_snapshot(
                database_engine, str(unknown["task_id"]), str(unknown["operation_id"]),
            )
            if (
                unknown_before != unknown_after or fake is None or fake.count() != 4
                or unknown_before["state"] != "UNKNOWN" or unknown_before["hold_state"] != "UNKNOWN"
                or unknown_before["quiescent_at"] is not None or unknown_before["provider_call_rows"] != 0
                or not any(row.get("id") == unknown["operation_id"] for row in read_only_unknown.get("incomplete_operations", []))
                or any(value.get("inference_requested") is not False for value in (read_only_unknown, apply_unknown, replay_unknown))
            ):
                raise AssertionError("reconcile changed an unconfirmed UNKNOWN operation or its execution hold")
            report["reconciliation_unknown_preservation"] = {
                **unknown, "after": unknown_after,
                "first_apply": apply_unknown.get("apply"),
                "second_apply": replay_unknown.get("apply"),
                "unknown_state_before_apply": apply_unknown.get("unknown_state_before_apply"),
                "unknown_state_after_apply": replay_unknown.get("unknown_state_after_apply"),
                "provider_call_rows": 0, "fake_provider_requests": fake.count(),
                "financial_hold_fabricated": False,
            }
        report["generation_allowance"] = ({
            "path": str(allowance), "file_mode": oct(allowance.stat().st_mode & 0o777),
            "max_generations": allowance_value["max_generations"],
            "claim_stages": sorted(allowance_value["claims"]),
            "claim_count": len(allowance_value["claims"]),
        } if allowance is not None and allowance_value is not None else None)
        report["request_capture"] = {
            "path": str(capture_path),
            "sha256": hashlib.sha256(capture_path.read_bytes()).hexdigest(),
            "file_mode": oct(capture_path.stat().st_mode & 0o777),
        }
        report["task_calls_and_cost_state"] = all_task_snapshots
        report["task_financial_state"] = {
            task_a_id: report.get("task_a_financial_state"),
            task_b_id: _financial_snapshot(database_engine, task_b_id, scope),
        }
        report["provider_gateway_metrics"] = {
            "before_gateway_restart": report.get("gateway_metrics_before_restart"),
            "after_gateway_restart": metrics,
            "aggregate_upstream_forward_attempts": gateway_attempts,
        } if personal_local else metrics
        report["reviewed_request_observations"] = observation_list
        report["fake_provider_requests"] = fake.count() if fake is not None else 0
        report["actual_provider_generations"] = real_generations
        report["unsettled_or_unknown"] = []
        report["projection"] = {
            "database_authoritative": True,
            "desired_version": projection.get("desired_version"),
            "applied_version": projection.get("applied_version"),
            "observed_memory_version": projection.get("observed_memory_version"),
            "observed_payload_digest_matches": projection.get("observed_payload_digest") == projection.get("payload_digest"),
            "state": projection.get("state"),
            "worker_restarted_before_task_b": True,
            "task_b_actual_request_reused_projection": True,
        }
        report["status"] = "PASS"
        report["production_dispatch"] = "BLOCKED"
    except BaseException as error:
        report["status"] = "FAILED"
        report["error"] = {"type": type(error).__name__, "message": str(error)[:1200]}
        if mode == "fake":
            report["actual_provider_generations"] = 0
        else:
            # A forwarded call consumes the one-shot allowance even if the
            # operation later fails. Keep the conservative upstream count.
            report["actual_provider_generations"] = None
        if database_engine is not None and scope:
            try:
                report["database_failure_snapshot"] = p6e.db_read(database_engine, """
                    SELECT t.id, t.status, t.stop_reason,
                           (SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id WHERE o.task_id=t.id) AS calls
                    FROM tasks t WHERE t.owner_scope=:scope ORDER BY t.created_at
                """, {"scope": scope})
            except Exception as snapshot_error:
                report["database_failure_snapshot_error"] = type(snapshot_error).__name__
        if gateway is not None and gateway.poll() is None and gateway_token:
            try:
                gateway_metrics = p6e.http_json(
                    f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token,
                )
                report["gateway_metrics_at_failure"] = gateway_metrics
                if mode == "real":
                    attempts = gateway_metrics.get("upstream_forward_attempts")
                    streams = gateway_metrics.get("provider_stream_observations", [])
                    report["provider_forward_attempts_at_failure"] = attempts
                    report["confirmed_stream_responses_at_failure"] = len(streams)
                    report["unconfirmed_provider_outcomes_at_failure"] = max(
                        0,
                        int(attempts) - len(streams),
                    )
                    report["actual_provider_generations"] = int(attempts)
            except Exception as metrics_error:
                if mode == "real":
                    report["actual_provider_generations"] = None
                    report["provider_outcome"] = "UNKNOWN_METRICS_UNAVAILABLE"
                report["gateway_metrics_error"] = type(metrics_error).__name__
        capture = Path(bundle["request_capture"]) if bundle else None
        if capture is not None and capture.exists():
            report["request_capture_at_failure"] = {
                "path": str(capture), "sha256": hashlib.sha256(capture.read_bytes()).hexdigest(),
                "records": len(_read_capture(capture)), "mode": oct(capture.stat().st_mode & 0o777),
            }
        report["unexecuted"] = [
            "Any Phase 6F boundary after the recorded failure is unverified.",
            "Do not send another real generation to repair this run.",
        ]
    finally:
        p6e.stop_process(worker)
        p6e.stop_process(managed_run)
        p6e.stop_app_server(container)
        p6e.stop_process(gateway)
        if fake is not None:
            fake.stop()
            original = report.pop("_fake_original_model", None)
            if original is not None:
                p3.FAKE_MODEL = original
        if database_engine is not None:
            database_engine.dispose()
        if report.get("database", {}).get("container"):
            # Keep the named volume and data, but stop this run's database process cleanly.
            run(["docker", "stop", "--time", "10", str(report["database"]["container"])], timeout=20)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("fake", "real"), default="fake", help="default is fake; real Qwen requires --fake-artifact")
    parser.add_argument("--fake-artifact", type=Path, help="current-code PASS artifact required before real mode")
    parser.add_argument("--personal-local", action="store_true", help="exercise the owner CLI with synthetic fake provider only")
    args = parser.parse_args()
    if args.personal_local and args.mode != "fake":
        parser.error("--personal-local is fake-provider-only; it never loads or calls Qwen")
    label = "personal-local-fake" if args.personal_local else f"p6f-{args.mode}"
    run_id = f"{label}-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    artifact = ARTIFACTS / f"{run_id}.json" if args.personal_local else ARTIFACTS / f"{run_id}-reviewed-qwen.json"
    report = _new_run_report(args.mode, run_id, personal_local=args.personal_local)
    try:
        report["branch"] = subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip()
        report["head"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        report["worktree_changes"] = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).splitlines()
        if report["head"] != BASELINE_SHA or report["branch"] != "phase6f-reviewed-qwen":
            raise RuntimeError("Phase 6F integration worktree differs from its requested baseline branch and SHA")
        node_bin = os.environ.get("HEKATE_NODE_BIN") or shutil.which("node") or ""
        if not node_bin or p6e.require_ok(run([node_bin, "--version"]), "pinned Node version") != "v22.19.0":
            raise RuntimeError("set HEKATE_NODE_BIN to the pinned Node.js 22.19.0 binary")
        command = ["uv", "run", "python", "scripts/phase6f_reviewed_qwen_probe.py", "--mode", args.mode]
        if args.personal_local:
            command.append("--personal-local")
        report["executed_command"] = {
            "argv": command,
            "environment": {"HEKATE_NODE_BIN": node_bin},
        }
        image = os.environ.get("HEKATE_LETTA_IMAGE", DEFAULT_IMAGE)
        if not (ROOT / "bridge/letta/dist/main.js").is_file():
            p3.p1.build_bridge(node_bin)
        report["node"] = {"path": node_bin, "version": "v22.19.0"}
        report["runtime_image_preflight"] = p6e.verify_image(image)
        report["bridge_build"] = "existing pinned bridge artifact used" if (ROOT / "bridge/letta/dist/main.js").is_file() else "built"
        if args.mode == "real":
            if args.fake_artifact is None or not args.fake_artifact.is_file():
                raise RuntimeError("real mode requires the artifact from a passing fake full-workflow run")
            fake_report = json.loads(args.fake_artifact.read_text(encoding="utf-8"))
            fingerprint, _files = _fingerprint()
            if fake_report.get("status") != "PASS" or fake_report.get("code_fingerprint_sha256") != fingerprint:
                raise RuntimeError("real mode requires a PASS fake artifact matching the current code fingerprint")
            report["fake_preflight_artifact"] = str(args.fake_artifact)
            report["fake_preflight_status"] = fake_report.get("status")
            report["fake_preflight_code_fingerprint"] = fake_report.get("code_fingerprint_sha256")
        run_root = Path.home() / ".local/share/hekate/phase6f-reviewed-qwen" / run_id
        run_root.mkdir(parents=True, exist_ok=False, mode=0o700)
        os.chmod(run_root, 0o700)
        report["preserved_run_root"] = str(run_root)
        _run_mode(
            args.mode, run_id, run_root, report, image=image, node_bin=node_bin,
            personal_local=args.personal_local,
        )
    except BaseException as error:
        report["status"] = "BLOCKED" if args.mode == "real" and report.get("actual_provider_generations") == 0 else "FAILED"
        report["error"] = {"type": type(error).__name__, "message": str(error)[:1200]}
        report["unexecuted"] = ["The normal CLI workflow did not pass all Phase 6F completion boundaries."]
    report["real_provider_calls"] = report.get("actual_provider_generations", 0)
    return _finalize(report, artifact)


if __name__ == "__main__":
    raise SystemExit(main())
