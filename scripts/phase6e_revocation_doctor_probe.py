#!/usr/bin/env python3
"""Verify revoked-scope settlement and loaded-runner doctor readiness.

This driver uses a new disposable PostgreSQL instance, the pinned Letta App
Server, and an isolated fake upstream. It never loads Ollama or runs Qwen.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import yaml
from sqlalchemy import text
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import phase3_runtime_probe as p3  # noqa: E402
import phase3b_single_hekate_probe as p3b  # noqa: E402
import phase6e_local_cli_probe as p6e  # noqa: E402


def now_utc() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _fingerprint() -> tuple[str, list[dict[str, str]]]:
    tracked = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True,
    ).stdout
    paths = {part.decode() for part in tracked.split(b"\0") if part}
    paths.update({
        "scripts/phase6e_local_cli_probe.py",
        "scripts/phase6e_revocation_doctor_probe.py",
        "docs/implementation/phase6e-local-cli.md",
        "docs/local-quickstart.md",
        "tests/unit/test_phase6e_doctor_context.py",
        "tests/persistence/test_phase2_postgres.py",
    })
    paths.update(
        str(path.relative_to(ROOT))
        for base in ("src/hekate", "tests/unit", "tests/persistence", "migrations", "config/local.example")
        for path in (ROOT / base).rglob("*") if path.is_file()
    )
    digest = hashlib.sha256()
    entries: list[dict[str, str]] = []
    for name in sorted(name for name in paths if (ROOT / name).is_file()):
        raw = (ROOT / name).read_bytes()
        checksum = hashlib.sha256(raw).hexdigest()
        digest.update(name.encode("utf-8") + b"\0" + bytes.fromhex(checksum))
        entries.append({"path": name, "sha256": checksum})
    return digest.hexdigest(), entries


def _run_pg(run_id: str) -> tuple[str, int, str, str]:
    password = secrets.token_urlsafe(30)
    port = p6e.free_port()
    suffix = run_id[-8:]
    container = f"hekate-p6e-revoke-{suffix}"
    volume = f"hekate-p6e-revoke-{suffix}"
    p6e.require_ok(p6e.run(["docker", "volume", "create", volume]), "isolated PostgreSQL volume")
    p6e.require_ok(p6e.run([
        "docker", "run", "--detach", "--name", container,
        "--publish", f"127.0.0.1:{port}:5432",
        "--env", "POSTGRES_USER=hekate", "--env", f"POSTGRES_PASSWORD={password}",
        "--env", "POSTGRES_DB=postgres", "--volume", f"{volume}:/var/lib/postgresql/data",
        "postgres:16.15",
    ], timeout=120), "isolated PostgreSQL start")
    base = f"postgresql+psycopg://hekate:{password}@127.0.0.1:{port}"
    admin = p6e.create_engine(p6e.db_url_for(base, "postgres"))
    try:
        deadline = time.monotonic() + 90
        while True:
            try:
                with admin.connect() as connection:
                    connection.execute(text("SELECT 1"))
                    connection.commit()
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise TimeoutError("new isolated PostgreSQL did not become ready")
                time.sleep(0.25)
        with admin.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text("CREATE DATABASE hekate_p6e_fake"))
            connection.execute(text("CREATE DATABASE hekate_phase2_test"))
    finally:
        admin.dispose()
    return base, port, container, volume


def _test_command(env: dict[str, str], pattern: str) -> dict[str, object]:
    command = [
        sys.executable, "-m", "unittest", "discover", "-s", "tests/persistence",
        "-p", "test_phase2_postgres.py", "-k", pattern, "-v",
    ]
    result = p6e.run(command, env=env, timeout=240)
    return {
        "command": command,
        "exit_code": result.returncode,
        "stdout_tail": (result.stdout or "")[-3000:],
        "stderr_tail": (result.stderr or "")[-3000:],
        "status": "PASS" if result.returncode == 0 else "FAIL",
    }


def _task_state(engine, task_id: str) -> dict[str, object]:
    return {
        "task": p6e.db_read(engine, """
            SELECT id, owner_scope, input_revision, status, outcome, stop_reason, provider_calls
            FROM tasks WHERE id=:task
        """, {"task": task_id}),
        "operation": p6e.db_read(engine, """
            SELECT id, state, dispatch_state, execution_state, last_error
            FROM operations WHERE task_id=:task ORDER BY created_at
        """, {"task": task_id}),
        "attempts": p6e.db_read(engine, """
            SELECT id, kind, status, operation_id FROM attempts WHERE task_id=:task ORDER BY created_at
        """, {"task": task_id}),
        "provider_calls": p6e.db_read(engine, """
            SELECT p.accounting_call_id, p.status AS call_status, cp.state AS permit_state,
                   u.completeness, u.settlement_state, u.input_tokens, u.output_tokens,
                   u.evaluated_cost_usd
            FROM provider_calls p
            LEFT JOIN call_permits cp ON cp.permit_id=p.permit_id
            LEFT JOIN usage_projections u ON u.accounting_call_id=p.accounting_call_id
            JOIN operations o ON o.id=p.operation_id
            WHERE o.task_id=:task ORDER BY p.created_at
        """, {"task": task_id}),
        "response": p6e.db_read(engine, """
            SELECT operation_id, attempt_id, registry_id, response_text, outcome, stop_reason, proposal
            FROM task_responses WHERE task_id=:task
        """, {"task": task_id}),
        "turn_results": p6e.db_read(engine, """
            SELECT tr.inbox_id, tr.processing_state, tr.rejection_reason,
                   tr.conclusion_id, c.eligible AS conclusion_eligible
            FROM turn_results tr LEFT JOIN conclusions c ON c.id=tr.conclusion_id
            WHERE tr.task_id=:task
        """, {"task": task_id}),
        "budget_accounts": p6e.db_read(engine, """
            SELECT scope_kind, scope_ref, spent_amount, held_amount
            FROM budget_accounts WHERE scope_kind='TASK' AND scope_ref=:task
        """, {"task": task_id}),
        "budget_ledger": p6e.db_read(engine, """
            SELECT l.effect_type, count(*) AS rows,
                   coalesce(sum(l.spent_delta),0) AS spent_delta,
                   coalesce(sum(l.held_delta),0) AS held_delta
            FROM budget_ledger l JOIN budget_reservations r ON r.id=l.reservation_id
            JOIN operations o ON o.id=r.operation_id WHERE o.task_id=:task
            GROUP BY l.effect_type ORDER BY l.effect_type
        """, {"task": task_id}),
        "position_version_count": p6e.db_read(engine,
            "SELECT count(*) AS count FROM position_versions WHERE task_id=:task", {"task": task_id})[0]["count"],
    }


async def _replay_observations(database_url: str, rows: list[dict[str, object]], call_id: str):
    from hekate.application.budgets import settle_call
    from hekate.application.runtime_inbox import process_runtime_observation
    from hekate.domain.types import AccountingCallId
    from hekate.infrastructure.postgres.database import create_engine, create_uow_factory

    engine = create_engine(database_url)
    factory = create_uow_factory(engine)
    replayed = []
    try:
        for row in rows:
            response = await process_runtime_observation(
                factory, str(row["provider_scope"]), str(row["stable_event_key"]), row["payload"],
            )
            replayed.append({
                "event_type": row["payload"].get("event_type"),
                "duplicate": response.get("duplicate"),
                "processed": response.get("processed"),
                "conflict": response.get("conflict", False),
            })
        settlement = await settle_call(factory, AccountingCallId(call_id))
        return replayed, {
            "settled": settlement.settled,
            "actual_cost": str(settlement.actual_cost) if settlement.actual_cost is not None else None,
            "pending_reason": settlement.pending_reason,
        }
    finally:
        await engine.dispose()


def run_fake_revocation_flow(
    run_id: str, run_dir: Path, database_url: str, node_bin: str, image: str,
) -> dict[str, object]:
    fake_model = "phase6e-fake-chat-v1"
    profile_id = "phase6e-revocation-fake-v1"
    pricing_version = "phase6e-revocation-synthetic-v1"
    scope = f"p6e-revoke-{run_id[-12:]}"
    worker_id = f"p6e-revoke-worker-{run_id[-8:]}"
    gateway_port, letta_port = p6e.free_port(), p6e.free_port()
    config_dir, state_dir, gateway_token, app_token = p6e.local_config(
        run_dir, "revoked", scope, database_url, worker_id, gateway_port,
    )
    local_env = p6e.base_env(
        config_dir, database_url, worker_id, gateway_token, app_token, node_bin, letta_port,
    )
    env = dict(local_env)
    engine = p6e.create_engine(database_url)
    fake = p3.FakeProvider()
    fake.set_response_factory(lambda request: p3b._output_from_request(request))
    original_model = p3.FAKE_MODEL
    p3.FAKE_MODEL = fake_model
    gateway = worker = None
    container = f"hekate-p6e-revoke-letta-{run_id[-8:]}"
    request_key = f"p6e-revoke-{run_id}"
    question = "Return one brief answer from the isolated fake provider."
    ask: dict[str, object] = {}
    ask_thread: threading.Thread | None = None
    result: dict[str, object] = {"status": "BLOCKED", "scope_id": scope, "request_key": request_key}
    try:
        fake.start()
        initialized, _ = p6e.cli(local_env, "init-local")
        result["init_local"] = initialized
        result["postgres"] = p6e.db_meta(engine)
        if initialized.get("scope_created") is not True:
            raise AssertionError("new fake authorization scope was not initialized")
        local_config = yaml.safe_load((config_dir / "local.yaml").read_text(encoding="utf-8"))
        local_config["gateway"]["upstream_base_url"] = f"http://127.0.0.1:{fake.port}"
        local_config["gateway"]["upstream_api_key"] = "isolated-fake-only"
        p6e.write_yaml(config_dir / "local.yaml", local_config)
        p6e.write_yaml(config_dir / "models.yaml", {"version": profile_id, "hekate": {
            "profile_id": profile_id,
            "model": f"openai-compatible/{fake_model}",
            "provider_model": fake_model,
            "model_revision": "phase6e-fake-chat-contract-v1",
            "context_window_tokens": 65536,
            "max_input_tokens": 32768,
            "max_output_tokens": 2048,
            "max_compaction_calls": 0,
        }})
        p6e.write_yaml(config_dir / "pricing.yaml", {
            "version": pricing_version,
            "effective_at": "2026-10-01T00:00:00Z",
            "prices": {fake_model: {
                "input_usd_per_million": "1",
                "output_usd_per_million": "2",
            }},
        })
        env["HEKATE_RUNTIME_MODE"] = "test"
        result["fake_execution_profile"] = {
            "profile_id": profile_id,
            "model": fake_model,
            "model_revision": "phase6e-fake-chat-contract-v1",
            "pricing_version": pricing_version,
            "synthetic_prices_usd_per_million": {"input": "1", "output": "2"},
            "compaction_limit": 0,
            "real_model_load": False,
        }
        gateway = p6e.start_gateway(env, state_dir, "revoked")
        p6e.start_app_server(image, container, state_dir / "letta", app_token,
                             gateway_port, gateway_token, letta_port)
        doctor_before = p6e.db_counts_for_doctor(engine, scope)
        doctor, _ = p6e.cli(env, "doctor")
        doctor_after = p6e.db_counts_for_doctor(engine, scope)
        result["fake_runtime_doctor"] = {
            "status": doctor.get("status"),
            "inference_requested": doctor.get("inference_requested"),
            "database_modified": doctor.get("database_modified"),
            "database_counts_unchanged": doctor_before == doctor_after,
            "ollama_checked": "ollama" in doctor.get("checks", {}),
        }
        if doctor.get("status") != "READY" or doctor.get("inference_requested") is not False or doctor_before != doctor_after:
            raise AssertionError("read-only synthetic runtime doctor did not pass")
        worker = p6e.start_worker(env, state_dir, "revoked")
        fake.set_next("block")

        def ask_task() -> None:
            try:
                completed = subprocess.run(
                    [sys.executable, "-m", "hekate", "ask", "--request-key", request_key,
                     "--wait-seconds", "70"],
                    cwd=ROOT, env=env, input=question, capture_output=True,
                    text=True, timeout=90, check=False,
                )
                ask["exit_code"] = completed.returncode
                ask["stdout_tail"] = completed.stdout[-2000:]
                ask["stderr_tail"] = completed.stderr[-2000:]
                if completed.returncode == 0:
                    try:
                        ask["value"] = json.loads(completed.stdout)
                    except json.JSONDecodeError:
                        ask["stdout_json"] = False
            except BaseException as error:
                ask["error"] = {"type": type(error).__name__, "message": str(error)[:500]}

        ask_thread = threading.Thread(target=ask_task, name=f"{run_id}-ask", daemon=True)
        ask_thread.start()
        if not fake.request_seen.wait(50):
            raise TimeoutError("fake provider did not observe the admitted request")
        task_rows = p6e.db_read(engine, """
            SELECT task_id FROM task_submissions WHERE owner_scope=:scope AND request_key=:key
        """, {"scope": scope, "key": request_key})
        if len(task_rows) != 1 or not task_rows[0]["task_id"]:
            raise AssertionError("Task receipt was not durable at the provider barrier")
        task_id = str(task_rows[0]["task_id"])
        before_revoke = _task_state(engine, task_id)
        if fake.count() != 1 or not before_revoke["provider_calls"] or before_revoke["provider_calls"][0]["permit_state"] != "CONSUMED":
            raise AssertionError("fake request did not cross the consumed-permit boundary exactly once")
        budget_at_barrier = before_revoke["budget_accounts"][0]
        if (
            Decimal(str(budget_at_barrier["spent_amount"])) != Decimal(0)
            or Decimal(str(budget_at_barrier["held_amount"])) <= Decimal(0)
        ):
            raise AssertionError("provider barrier did not preserve the pre-settlement held budget")
        result["task_id"] = task_id
        result["at_provider_barrier"] = before_revoke
        result["budget_at_provider_barrier"] = budget_at_barrier
        result["fake_requests_at_barrier"] = fake.count()

        with engine.begin() as connection:
            changed = connection.execute(text(
                "UPDATE authorization_scopes SET active=false WHERE id=:scope RETURNING id"
            ), {"scope": scope}).scalar_one_or_none()
        if changed != scope:
            raise AssertionError("isolated authorization scope could not be revoked")
        auth_path = state_dir / "letta/lc-local-backend/providers/auth.json"
        auth_mtime = auth_path.stat().st_mtime_ns
        revoke_ask = subprocess.run(
            [sys.executable, "-m", "hekate", "ask", "--request-key", request_key + "-blocked",
             "--wait-seconds", "0"],
            cwd=ROOT, env=env, input="This must be rejected before Task creation.",
            capture_output=True, text=True, timeout=30, check=False,
        )
        if revoke_ask.returncode == 0:
            raise AssertionError("new Task submission succeeded after scope revocation")
        new_submission_count = p6e.db_read(engine, """
            SELECT count(*) AS count FROM task_submissions WHERE owner_scope=:scope
        """, {"scope": scope})[0]["count"]
        if new_submission_count != 1 or fake.count() != 1:
            raise AssertionError("revoked new Task created rows or reached the fake provider")
        init_replay = p6e.run([sys.executable, "-m", "hekate", "init-local"], env=local_env, timeout=60)
        active_after_init = p6e.db_read(engine,
            "SELECT active FROM authorization_scopes WHERE id=:scope", {"scope": scope})[0]["active"]
        if init_replay.returncode == 0 or active_after_init or auth_path.stat().st_mtime_ns != auth_mtime:
            raise AssertionError("init-local replay reactivated the revoked scope or rewrote provider auth")
        result["revocation_gates"] = {
            "new_task_rejected": revoke_ask.returncode != 0,
            "submission_rows": new_submission_count,
            "fake_requests_before_and_after": [1, fake.count()],
            "init_local_replay_rejected": init_replay.returncode != 0,
            "scope_remained_inactive": not active_after_init,
            "provider_auth_unchanged": auth_path.stat().st_mtime_ns == auth_mtime,
        }
        request_metrics = p6e.http_json(
            f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token,
        )
        result["gateway_metrics_at_revocation"] = request_metrics
        fake.release_block.set()
        ask_thread.join(timeout=90)
        if ask_thread.is_alive():
            raise TimeoutError("fake CLI wait remained active after releasing the provider")
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            current = _task_state(engine, task_id)
            row = current["task"][0] if current["task"] else {}
            if row.get("status") in {"FAILED", "COMPLETED", "CANCELLED"}:
                break
            time.sleep(0.2)
        final = _task_state(engine, task_id)
        task_row = final["task"][0] if final["task"] else {}
        response = final["response"][0] if final["response"] else {}
        turn_result = final["turn_results"][0] if final["turn_results"] else {}
        call = final["provider_calls"][0] if final["provider_calls"] else {}
        operation = final["operation"][0] if final["operation"] else {}
        budget_after = final["budget_accounts"][0] if final["budget_accounts"] else {}
        result["after_revoked_result"] = final
        if not (
            task_row.get("status") == "FAILED"
            and task_row.get("stop_reason") == "POLICY"
            and response.get("outcome") == "FAILED"
            and response.get("stop_reason") == "POLICY"
            and "authorization_scope_revoked" in str(response.get("response_text"))
            and call.get("call_status") == "QUIESCENT"
            and call.get("permit_state") == "CONSUMED"
            and call.get("completeness") == "COMPLETE"
            and call.get("settlement_state") == "SETTLED"
            and operation.get("execution_state") == "QUIESCENT"
            and turn_result.get("processing_state") == "REJECTED"
            and turn_result.get("rejection_reason") == "authorization_scope_revoked"
            and final["position_version_count"] == 0
            and Decimal(str(budget_after["spent_amount"])) > Decimal(0)
            and Decimal(str(budget_after["held_amount"])) == Decimal(0)
        ):
            raise AssertionError(
                "revoked execution did not settle then converge to server POLICY failure: "
                + json.dumps({"task": task_row, "response": response, "turn_result": turn_result,
                              "call": call, "operation": operation, "position_versions": final["position_version_count"]},
                             default=str, sort_keys=True)
            )
        result["settlement_observation"] = {
            "complete_usage": call.get("completeness"),
            "settlement": call.get("settlement_state"),
            "provider_usage_tokens": {
                "input": call.get("input_tokens"), "output": call.get("output_tokens"),
            },
            "evaluated_cost_usd": call.get("evaluated_cost_usd"),
            "task_budget_before": {
                "spent": budget_at_barrier.get("spent_amount"),
                "held": budget_at_barrier.get("held_amount"),
            },
            "task_budget_after": {
                "spent": budget_after.get("spent_amount"),
                "held": budget_after.get("held_amount"),
            },
            "synthetic_spent_nonzero": Decimal(str(budget_after["spent_amount"])) > Decimal(0),
            "unused_hold_released": Decimal(str(budget_after["held_amount"])) == Decimal(0),
            "task_response_is_server_policy": True,
            "position_versions": final["position_version_count"],
        }
        ask_query_denied = ask.get("exit_code") != 0 and "authorization scope is revoked" in str(ask.get("stderr_tail", ""))
        if ask.get("exit_code") not in {0, None} and not ask_query_denied:
            raise RuntimeError(f"ask returned an unexpected error after provider release: {ask.get('stderr_tail', '')[-500:]}")
        result["ask_client_after_revoke"] = {
            "exit_code": ask.get("exit_code"),
            "task_failed_policy_in_database": task_row.get("status") == "FAILED" and task_row.get("stop_reason") == "POLICY",
            "revoked_task_read_denied": ask_query_denied,
            "stderr_tail": ask.get("stderr_tail", "")[-600:],
        }

        event_rows = p6e.db_read(engine, """
            SELECT provider_scope, stable_event_key, payload FROM inbox
            WHERE payload->>'operation_id'=:operation
              AND payload->>'event_type' IN ('provider_call','execution')
              AND processed_at IS NOT NULL
            ORDER BY received_at, stable_event_key
        """, {"operation": operation["id"]})
        if not event_rows:
            raise AssertionError("no processed runtime observations were persisted for replay")
        call_id = str(call["accounting_call_id"])
        before_replay = _task_state(engine, task_id)
        stop = worker
        worker = None
        p6e.stop_process(stop)
        worker = p6e.start_worker(env, state_dir, "revoked-restarted")
        replayed, settlement_replay = asyncio.run(_replay_observations(database_url, event_rows, call_id))
        after_replay = _task_state(engine, task_id)
        after_metrics = p6e.http_json(
            f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token,
        )
        if before_replay != after_replay or fake.count() != 1 or after_metrics.get("upstream_forward_attempts") != 1:
            raise AssertionError("worker restart, observation replay, or idempotent settle changed effects")
        if not settlement_replay.get("settled") or any(
            item["conflict"] or not item["processed"] for item in replayed
        ):
            raise AssertionError("stored runtime observation replay did not remain a processed duplicate")
        result["restart_observation_replay"] = {
            "worker_restarted": True,
            "events": replayed,
            "settle_call_after_revoke": settlement_replay,
            "state_unchanged": True,
            "fake_upstream_requests": fake.count(),
            "gateway_forward_attempts": after_metrics.get("upstream_forward_attempts"),
        }
        result["status"] = "PASS"
        return result
    except BaseException as error:
        result["status"] = "FAILED"
        result["error"] = {"type": type(error).__name__, "message": str(error)[:1800]}
        result["fake_request_count"] = fake.count()
        if ask_thread is not None and ask_thread.is_alive():
            ask_thread.join(timeout=3)
        result["ask_client_diagnostic"] = {
            key: value for key, value in ask.items()
            if key not in {"stdout_tail", "stderr_tail"}
        }
        result["ask_stderr_tail"] = p6e.redact(str(ask.get("stderr_tail", ""))[-1400:])
        for label in ("revoked-gateway.log", "revoked-worker.log"):
            path = state_dir / label
            if path.is_file():
                result[label.removesuffix(".log").replace("-", "_") + "_log_tail"] = p6e.redact(
                    path.read_text(encoding="utf-8", errors="replace")[-2400:]
                )
        if container:
            logs = p6e.run(["docker", "logs", "--tail", "50", container], timeout=10)
            result["app_server_log_tail"] = p6e.redact((logs.stdout + logs.stderr)[-2400:])
        task_id_value = result.get("task_id")
        if not task_id_value:
            ask_value = ask.get("value")
            receipt = ask_value.get("receipt") if isinstance(ask_value, dict) else None
            if isinstance(receipt, dict):
                task_id_value = receipt.get("task_id")
        if task_id_value:
            result["task_id"] = str(task_id_value)
            try:
                result["failure_snapshot"] = _task_state(engine, str(task_id_value))
            except Exception as snapshot_error:
                result["failure_snapshot_error"] = type(snapshot_error).__name__
        try:
            result["failure_gateway_metrics"] = p6e.http_json(
                f"http://127.0.0.1:{gateway_port}/internal/metrics", gateway_token,
            )
        except Exception as metrics_error:
            result["failure_gateway_metrics_error"] = type(metrics_error).__name__
        return result
    finally:
        fake.release_block.set()
        if ask_thread is not None and ask_thread.is_alive():
            ask_thread.join(timeout=5)
        p6e.stop_process(worker)
        p6e.stop_app_server(container)
        p6e.stop_process(gateway)
        fake.stop()
        p3.FAKE_MODEL = original_model
        engine.dispose()


def main() -> int:
    run_id = "p6e-revoke-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    artifact = ROOT / "integration/runtime/artifacts" / f"{run_id}-revocation-doctor.json"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    branch = subprocess.run(["git", "branch", "--show-current"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    report: dict[str, object] = {
        "schema_version": "1",
        "probe": "phase6e-revocation-doctor",
        "run_id": run_id,
        "executed_at": now_utc(),
        "branch": branch,
        "head": head,
        "baseline_sha": "0b9f80918bb628298349909921a1197747f6b35d",
        "status": "BLOCKED",
        "qwen_generation_attempts": 0,
        "actual_provider_calls": 0,
        "production_dispatch": "BLOCKED",
        "prior_phase6d_and_phase6e_databases": "not selected, queried, or modified",
        "prior_qwen_one_shot_authorization": "not read, used, or modified",
        "excluded": ["Qwen model load or generation", "Critic", "Position/projection Qwen integration", "G7/G8", "production dispatch"],
        "commands": [],
    }
    container = volume = None
    try:
        node_bin = os.environ.get("HEKATE_NODE_BIN") or ""
        if not node_bin:
            fallback = "/tmp/p6d-node-v22.19.0-linux-x64/node-v22.19.0-linux-x64/bin/node"
            node_bin = fallback if Path(fallback).is_file() else (shutil.which("node") or "")
        if not node_bin or p6e.require_ok(p6e.run([node_bin, "--version"]), "Node version check") != "v22.19.0":
            raise RuntimeError("pinned Node.js 22.19.0 is required for the fake runtime path")
        bridge = ROOT / "bridge/letta/dist/main.js"
        if not bridge.is_file():
            raise RuntimeError("pinned Letta bridge build is missing")
        image = os.environ.get("HEKATE_LETTA_IMAGE", p6e.DEFAULT_IMAGE)
        report["runtime"] = p6e.verify_image(image)
        report["node_version"] = "v22.19.0"
        report["bridge"] = str(bridge.relative_to(ROOT))
        report["commands"].append("HEKATE_NODE_BIN=<pinned-22.19.0> uv run --locked python scripts/phase6e_revocation_doctor_probe.py")

        run_dir = Path(tempfile.mkdtemp(prefix=f"{run_id}-"))
        pg_base, pg_port, container, volume = _run_pg(run_id)
        report["database_instance"] = {
            "container": container,
            "volume": volume,
            "host": "127.0.0.1",
            "port": pg_port,
            "image": "postgres:16.15",
            "prior_database_used": False,
        }
        test_url = p6e.db_url_for(pg_base, "hekate_phase2_test")
        test_env = dict(os.environ, HEKATE_DATABASE_URL=test_url,
                        HEKATE_TEST_DATABASE_URL=test_url, HEKATE_PROJECT_DIR=str(ROOT))
        migration = p6e.run([sys.executable, "-m", "alembic", "upgrade", "head"], env=test_env, timeout=120)
        p6e.require_ok(migration, "isolated persistence-test migration")
        metadata = p6e.run([sys.executable, "-m", "alembic", "check"], env=test_env, timeout=90)
        p6e.require_ok(metadata, "isolated persistence-test Alembic check")
        report["isolated_persistence_db"] = {
            "migration": "PASS",
            "metadata_check": "PASS",
            "migration_head": "0014_local_dispatch_identity",
        }
        persistence: list[dict[str, object]] = []
        for pattern in (
            "phase6e_revocation",
            "phase6e_revoked",
            "t5_unknown_hold_survives_reconnect_and_new_fence",
            "t6_conflicting_usage_keeps_pending_hold",
            "ta_malformed_persisted_binding_fails_all_settlement_entrypoints",
        ):
            row = _test_command(test_env, pattern)
            persistence.append({"filter": pattern, **row})
            report["focused_postgres_tests"] = persistence
            if row["status"] != "PASS":
                raise AssertionError(f"isolated PostgreSQL test filter failed: {pattern}")

        fake_url = p6e.db_url_for(pg_base, "hekate_p6e_fake")
        report["fake_provider_flow"] = run_fake_revocation_flow(
            run_id, run_dir, fake_url, node_bin, image,
        )
        if report["fake_provider_flow"]["status"] != "PASS":
            raise AssertionError("fake-provider revocation flow failed: " + str(report["fake_provider_flow"].get("error")))
        report["postgres_runtime"] = report["fake_provider_flow"]["postgres"]
        report["doctor_fixtures"] = {
            "command": "uv run --locked python -m unittest discover -s tests/unit -p 'test_phase6e_doctor_context.py' -v",
            "status": "RUNNING_AFTER_INTEGRATION",
        }
        unit = p6e.run([
            sys.executable, "-m", "unittest", "discover", "-s", "tests/unit",
            "-p", "test_phase6e_doctor_context.py", "-v",
        ], timeout=120)
        report["doctor_fixtures"] = {
            "command": "uv run --locked python -m unittest discover -s tests/unit -p 'test_phase6e_doctor_context.py' -v",
            "exit_code": unit.returncode,
            "status": "PASS" if unit.returncode == 0 else "FAIL",
            "stdout_tail": (unit.stdout or "")[-3000:],
            "stderr_tail": (unit.stderr or "")[-3000:],
        }
        if unit.returncode != 0:
            raise AssertionError("whole-doctor loaded-context fixtures failed")
        report["unexecuted"] = [
            "actual Ollama metadata/load checks",
            "Qwen model load/generation",
            "Critic, Position, memory projection Qwen path",
            "G7/G8 and production dispatch",
        ]
        report["status"] = "PASS"
    except BaseException as error:
        report["status"] = "FAILED"
        report["error"] = {"type": type(error).__name__, "message": str(error)[:700]}
        report.setdefault("unexecuted", []).append("remaining checks after first failure")
    finally:
        report["container_cleanup"] = {"container": container, "volume": volume, "removed": False}
        if container:
            p6e.run(["docker", "rm", "-f", container], timeout=30)
        if volume:
            p6e.run(["docker", "volume", "rm", volume], timeout=30)
        report["container_cleanup"]["removed"] = True
        if "run_dir" in locals():
            shutil.rmtree(run_dir, ignore_errors=True)
        fingerprint, entries = _fingerprint()
        report["code_fingerprint_sha256"] = fingerprint
        report["fingerprinted_files"] = entries
        report["artifact_path"] = str(artifact.relative_to(ROOT))
        report["finalized_at"] = now_utc()
        temporary = artifact.with_suffix(artifact.suffix + ".tmp")
        temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.chmod(temporary, 0o600)
        os.replace(temporary, artifact)
        print(json.dumps({"status": report["status"], "artifact": str(artifact), "fingerprint": fingerprint}))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
