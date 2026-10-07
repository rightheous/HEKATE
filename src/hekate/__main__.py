from __future__ import annotations

import argparse
import asyncio
import hashlib
from collections.abc import Sequence
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import signal
import socket
import sys
from uuid import uuid4

from sqlalchemy import text

from hekate.application import budgets, tasks
from hekate.application.results import process_pending_results
from hekate.application.local_setup import initialize_local
from hekate.bootstrap import build_container, close_container
from hekate.diagnostics import doctor as run_doctor
from hekate.domain.errors import HekateError
from hekate.domain.models import EvidenceInput, ReadLimits, UserMessage, VersionCursor
from hekate.domain.types import AccountingCallId, EvidenceId, ScopeId, StopReason, TaskId, TopicId
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.settings import (
    configured_critic_execution, configured_deliberation, configured_local_actor,
    configured_task_execution, load_settings, validate_local_settings,
)
from hekate.worker.service import process_inbox_row, process_pending_inbox, run_worker
from hekate.infrastructure.letta.gateway_entry import serve_gateway


def _settings():
    config_dir = Path(os.environ.get("HEKATE_CONFIG_DIR", "config"))
    return load_settings(os.environ, config_dir)


def _write(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def _run_async(awaitable, *, show_validation_error: bool = False) -> int:
    try:
        return asyncio.run(awaitable)
    except (HekateError, ValueError, OSError) as error:
        detail = f": {str(error)[:240]}" if isinstance(error, HekateError) or (
            show_validation_error and isinstance(error, ValueError)
        ) else ""
        print(f"hekate: {type(error).__name__}{detail}", file=sys.stderr)
        return 2


async def _worker() -> int:
    settings = _settings()
    container = await build_container(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await run_worker(container, stop)
    finally:
        await close_container(container)
    return 0


async def _init_local() -> int:
    result = await initialize_local(_settings())
    _write(result)
    return 0


async def _gateway() -> int:
    await serve_gateway(_settings())
    return 0


async def _doctor() -> int:
    result = await run_doctor(_settings())
    _write(result)
    return 0 if result["status"] in {"READY", "PRESTART_OK"} else 1


async def _task_command(command: str, args: argparse.Namespace) -> int:
    settings = _settings()
    if not settings.database_url.startswith("postgresql+psycopg://"):
        raise ValueError("HEKATE_DATABASE_URL must be configured for PostgreSQL")
    actor = configured_local_actor(settings)
    engine = create_engine(settings.database_url)
    factory = create_uow_factory(engine)
    try:
        if command == "ask":
            from hekate.settings import validate_settings

            validate_settings(settings)
            config = configured_task_execution(settings)
            raw = sys.stdin.buffer.read(65_537)
            question = raw.decode("utf-8", "strict")
            receipt = await tasks.submit(factory, actor, UserMessage(
                text=question,
                topic_id=TopicId(args.topic_id) if args.topic_id else None,
                evidence_refs=tuple(EvidenceId(value) for value in args.evidence_id),
            ), args.request_key, config)
            task_id = TaskId(str(receipt["task_id"]))
            deadline = asyncio.get_running_loop().time() + args.wait_seconds
            view = await tasks.get_task(factory, actor, task_id)
            while view["state"] not in {"COMPLETED", "FAILED", "CANCELLED"} and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(min(0.25, max(0, deadline - asyncio.get_running_loop().time())))
                view = await tasks.get_task(factory, actor, task_id)
            _write({"receipt": receipt, "task": view, "timed_out": view["state"] not in {"COMPLETED", "FAILED", "CANCELLED"}})
        elif command == "task":
            _write(await tasks.get_task(factory, actor, TaskId(args.task_id)))
        else:
            _write(await tasks.cancel(factory, actor, TaskId(args.task_id), StopReason.USER_CANCELLED))
        return 0
    finally:
        await engine.dispose()


async def _knowledge_command(args: argparse.Namespace) -> int:
    from hekate.application import evidence, positions

    settings = _settings()
    actor = configured_local_actor(settings)
    engine = create_engine(settings.database_url)
    factory = create_uow_factory(engine)
    try:
        if args.command == "evidence" and args.evidence_command == "import":
            path = Path(args.file)
            with path.open("rb") as stream:
                content = stream.read(1_048_577)
            source = EvidenceInput(
                schema_version="1", id=EvidenceId(str(uuid4())), kind=args.kind,
                source_uri=args.source_uri or path.resolve().as_uri(),
                retrieved_at=datetime.now(UTC), content_hash=hashlib.sha256(content).hexdigest(),
                access_scope="local-import", retention_class=args.retention_class,
                expiry_at=datetime.fromisoformat(args.expires_at.replace("Z", "+00:00")),
            )
            result = await evidence.register(
                factory, actor, source, content, settings.archive_dir, request_key=args.request_key,
            )
            _write(result.model_dump(mode="json"))
        elif args.command == "evidence":
            result = await evidence.read_scoped(
                factory, actor, EvidenceId(args.evidence_id), ReadLimits(max_bytes=32_768), settings.archive_dir,
            )
            _write(result.model_dump(mode="json"))
        elif args.position_command == "show":
            result = await positions.read_current(factory, actor, TopicId(args.topic_id))
            _write(result.model_dump(mode="json"))
        else:
            result = await positions.read_history(
                factory, actor, TopicId(args.topic_id),
                VersionCursor(after_version=args.after_version, limit=args.limit),
            )
            _write(result.model_dump(mode="json"))
        return 0
    finally:
        await engine.dispose()


_TERMINAL_TASK_STATES = {"COMPLETED", "FAILED", "CANCELLED"}


async def _wait_task(factory, actor, task_id: TaskId, seconds: float) -> tuple[dict[str, object], bool]:
    loop = asyncio.get_running_loop()
    stop_wait = asyncio.Event()
    loop.add_signal_handler(signal.SIGINT, stop_wait.set)
    deadline = loop.time() + seconds
    try:
        view = await tasks.get_task(factory, actor, task_id)
        while view["state"] not in _TERMINAL_TASK_STATES and loop.time() < deadline:
            if stop_wait.is_set():
                return view, True
            try:
                await asyncio.wait_for(stop_wait.wait(), timeout=min(0.5, max(0.01, deadline - loop.time())))
            except TimeoutError:
                pass
            if not stop_wait.is_set():
                view = await tasks.get_task(factory, actor, task_id)
        return view, False
    finally:
        loop.remove_signal_handler(signal.SIGINT)


def _show_chat_task(view: dict[str, object], *, waiting_stopped: bool = False, waiting_timeout: bool = False) -> None:
    print(f"Task {view['task_id']}: {view['state']}")
    if waiting_stopped:
        print("Waiting stopped. The Task is still active; use /task or /cancel.")
    elif waiting_timeout:
        print("Wait limit reached. The Task is still active; use /task or /cancel.")
    elif view.get("response"):
        print(view["response"])
    elif view.get("stop_reason"):
        print(f"Stopped: {view['stop_reason']}")


async def _chat_command(args: argparse.Namespace) -> int:
    settings = _settings()
    from hekate.settings import validate_settings

    validate_settings(settings)
    actor = configured_local_actor(settings)
    config = configured_task_execution(settings)
    engine = create_engine(settings.database_url)
    factory = create_uow_factory(engine)
    topic: TopicId | None = None
    evidence: tuple[EvidenceId, ...] = ()
    last_message: UserMessage | None = None
    last_key: str | None = None
    current_task: TaskId | None = None

    async def submit_and_show(message: UserMessage, request_key: str) -> None:
        nonlocal current_task
        print(f"Request key: {request_key}", flush=True)
        receipt = await tasks.submit(factory, actor, message, request_key, config)
        current_task = TaskId(str(receipt["task_id"]))
        print(f"Waiting on Task {current_task}; Ctrl+C stops waiting, not the Task.", flush=True)
        view, interrupted = await _wait_task(factory, actor, current_task, args.wait_seconds)
        _show_chat_task(
            view, waiting_stopped=interrupted,
            waiting_timeout=not interrupted and view["state"] not in _TERMINAL_TASK_STATES,
        )

    print("HEKATE chat. /help lists commands; /quit exits without cancelling Tasks.")
    try:
        while True:
            try:
                line = input("hekate> ")
            except EOFError:
                print()
                return 0
            if not line.strip():
                continue
            if line.startswith("/"):
                command, _, rest = line.partition(" ")
                value = rest.strip()
                if command in {"/quit", "/exit"}:
                    return 0
                if command == "/help":
                    print("/status /task [id] /cancel [id] /topic [id|off] /evidence [id ...|off] /position /history [topic] /replay /quit")
                elif command == "/topic":
                    topic = None if value.lower() == "off" else TopicId(value) if value else topic
                    print(f"Default topic: {topic or 'off'}")
                elif command == "/evidence":
                    if value.lower() == "off":
                        evidence = ()
                    elif value:
                        evidence = tuple(dict.fromkeys((*evidence, *(EvidenceId(item) for item in value.replace(",", " ").split()))))
                    print("Default Evidence: " + (", ".join(map(str, evidence)) if evidence else "off"))
                elif command == "/task":
                    selected = TaskId(value) if value else current_task
                    if selected is None:
                        print("No Task selected yet.")
                    else:
                        _write(await tasks.get_task(factory, actor, selected))
                elif command == "/cancel":
                    selected = TaskId(value) if value else current_task
                    if selected is None:
                        print("No Task selected yet.")
                    else:
                        _write(await tasks.cancel(factory, actor, selected, StopReason.USER_CANCELLED))
                elif command == "/status":
                    await _status_command(json_output=False)
                elif command == "/position":
                    if topic is None:
                        print("Choose a topic with /topic first.")
                    else:
                        from hekate.application import positions
                        result = await positions.read_current(factory, actor, topic)
                        _write(result.model_dump(mode="json"))
                elif command == "/history":
                    selected = TopicId(value) if value else topic
                    if selected is None:
                        print("Choose a topic with /topic first.")
                    else:
                        from hekate.application import positions
                        result = await positions.read_history(factory, actor, selected, VersionCursor())
                        _write(result.model_dump(mode="json"))
                elif command == "/replay":
                    if last_message is None or last_key is None:
                        print("There is no prior request to replay in this chat.")
                    else:
                        print(f"Replaying the same request key: {last_key}")
                        await submit_and_show(last_message, last_key)
                else:
                    print("Unknown command. Use /help.")
                continue

            last_message = UserMessage(text=line, topic_id=topic, evidence_refs=evidence)
            last_key = f"chat-{uuid4().hex}"
            try:
                await submit_and_show(last_message, last_key)
            except HekateError as error:
                print(f"Request not confirmed: {type(error).__name__}. Use /replay to submit the same request key.")
            except (OSError, TimeoutError) as error:
                print(f"Request not confirmed: {type(error).__name__}. Use /replay to submit the same request key.")
    finally:
        await engine.dispose()


async def _status_snapshot() -> dict[str, object]:
    settings = _settings()
    actor = configured_local_actor(settings)
    doctor = await run_doctor(settings)
    checks = doctor.get("checks", {})
    configuration = checks.get("configuration", {}) if isinstance(checks, dict) else {}
    readiness = {
        key: value.get("status") for key, value in checks.items()
        if isinstance(value, dict) and key in {
            "configuration", "profile_assets", "postgres", "authorization", "ollama",
            "provider_gateway", "letta_runtime", "node_bridge",
        }
    } if isinstance(checks, dict) else {}
    if readiness.get("postgres") != "READY":
        return {
            "status": doctor.get("status"),
            "profile": configuration.get("profile_id") if isinstance(configuration, dict) else None,
            "readiness": readiness, "tasks": {}, "operations": [],
            "pending_settlement_calls": None, "task_budget_usd": None,
            "critic_delete_pending": None, "projection": [], "archive_deletion": {},
            "latest_failure": None, "production_dispatch_approved": False,
        }
    engine = create_engine(settings.database_url)
    scope = str(actor.scope)
    try:
        async with engine.connect() as connection:
            task_rows = (await connection.execute(text(
                "SELECT status, count(*) AS count FROM tasks WHERE owner_scope=:scope GROUP BY status ORDER BY status"
            ), {"scope": scope})).mappings().all()
            operation_rows = (await connection.execute(text(
                "SELECT state, execution_state, dispatch_state, count(*) AS count FROM operations "
                "WHERE owner_scope=:scope GROUP BY state, execution_state, dispatch_state "
                "ORDER BY state, execution_state, dispatch_state"
            ), {"scope": scope})).mappings().all()
            budget = (await connection.execute(text(
                "SELECT coalesce(sum(spent_amount),0) AS spent, coalesce(sum(held_amount),0) AS held "
                "FROM budget_accounts WHERE scope_kind='TASK' AND scope_ref IN "
                "(SELECT id FROM tasks WHERE owner_scope=:scope)"
            ), {"scope": scope})).mappings().one()
            pending_settlement = await connection.scalar(text(
                "SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id "
                "LEFT JOIN usage_projections u ON u.accounting_call_id=p.accounting_call_id "
                "WHERE o.owner_scope=:scope AND p.status NOT IN ('EXPIRED','REVOKED') AND "
                "(p.status!='QUIESCENT' OR u.settlement_state IS NULL OR u.settlement_state!='SETTLED')"
            ), {"scope": scope})
            critic_pending = await connection.scalar(text(
                "SELECT count(*) FROM critic_workflows WHERE owner_scope=:scope AND stage='DELETE_PENDING'"
            ), {"scope": scope})
            projection_rows = (await connection.execute(text(
                "SELECT state, count(*) AS count, coalesce(sum(desired_version-applied_version),0) AS versions "
                "FROM memory_projections WHERE scope=:scope GROUP BY state ORDER BY state"
            ), {"scope": scope})).mappings().all()
            archive_rows = (await connection.execute(text(
                "SELECT a.storage_state, count(DISTINCT a.artifact_ref) AS count FROM artifacts a "
                "JOIN evidence e ON e.artifact_ref=a.artifact_ref "
                "WHERE e.scope=:scope AND a.storage_state IN ('DELETE_PENDING','DELETE_FAILED') "
                "GROUP BY a.storage_state ORDER BY a.storage_state"
            ), {"scope": scope})).mappings().all()
            latest_failure = (await connection.execute(text(
                "SELECT id, status, stop_reason, created_at FROM tasks WHERE owner_scope=:scope "
                "AND status IN ('FAILED','CANCELLED') ORDER BY created_at DESC LIMIT 1"
            ), {"scope": scope})).mappings().one_or_none()
        return {
            "status": doctor.get("status"),
            "profile": configuration.get("profile_id") if isinstance(configuration, dict) else None,
            "readiness": readiness,
            "tasks": {row["status"]: row["count"] for row in task_rows},
            "operations": [dict(row) for row in operation_rows],
            "pending_settlement_calls": pending_settlement,
            "task_budget_usd": {"spent": str(budget["spent"]), "held": str(budget["held"])},
            "critic_delete_pending": critic_pending,
            "projection": [dict(row) for row in projection_rows],
            "archive_deletion": {row["storage_state"]: row["count"] for row in archive_rows},
            "latest_failure": ({
                "id": latest_failure["id"], "status": latest_failure["status"],
                "stop_reason": latest_failure["stop_reason"],
                "created_at": latest_failure["created_at"].isoformat(),
            } if latest_failure else None),
            "production_dispatch_approved": False,
        }
    finally:
        await engine.dispose()


async def _status_command(*, json_output: bool) -> int:
    value = await _status_snapshot()
    if json_output:
        _write(value)
    else:
        print(f"HEKATE: {value['status']}  profile: {value['profile'] or 'unavailable'}")
        print("Tasks: " + (", ".join(f"{key} {count}" for key, count in value["tasks"].items()) or "none"))
        if value["task_budget_usd"] is not None:
            print(f"Task budget: spent ${value['task_budget_usd']['spent']}, held ${value['task_budget_usd']['held']}")
        print(f"Pending settlement calls: {value['pending_settlement_calls']}; Critic deletion: {value['critic_delete_pending']}")
        print(f"Position projections: {value['projection']}; archive deletion: {value['archive_deletion']}")
        if value["latest_failure"]:
            failure = value["latest_failure"]
            print(f"Latest failure: {failure['id']} {failure['status']} {failure['stop_reason'] or ''}".rstrip())
        print("Production dispatch: blocked")
    return 0


async def _reconcile_command(args: argparse.Namespace) -> int:
    settings = _settings()
    actor = configured_local_actor(settings)
    engine = create_engine(settings.database_url)
    factory = create_uow_factory(engine)
    scope = ScopeId(actor.scope)
    report: dict[str, object] = {"mode": "apply" if args.apply else "read_only", "scope": str(scope)}
    try:
        async with factory() as uow:
            inbox_rows = await uow.delivery.pending_inbox(args.limit, scope)
            result_ids = await uow.tasks.list_pending_turn_results(args.limit, scope)
            pending_calls = await uow.budgets.list_pending_calls(args.limit, scope)
            await uow.commit()
        async with engine.connect() as connection:
            unknown_snapshot = (await connection.execute(text(
                "SELECT "
                "(SELECT count(*) FROM operations WHERE owner_scope=:scope AND (state='UNKNOWN' OR execution_state='UNKNOWN')) AS operations, "
                "(SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id "
                " WHERE o.owner_scope=:scope AND p.status='UNKNOWN') AS calls, "
                "(SELECT coalesce(sum(b.held_amount),0) FROM budget_accounts b WHERE b.scope_kind='TASK' "
                " AND b.scope_ref IN (SELECT id FROM tasks WHERE owner_scope=:scope)) AS task_held"
            ), {"scope": str(scope)})).mappings().one()
            operations = (await connection.execute(text(
                "SELECT id, kind, state, execution_state, dispatch_state FROM operations "
                "WHERE owner_scope=:scope AND (state IN ('CLAIMED','UNKNOWN') OR execution_state='UNKNOWN' "
                "OR dispatch_state IN ('INTENT_RECORDED','SEND_INTENT','DISPATCHED','UNKNOWN')) "
                "ORDER BY created_at, id LIMIT :limit"
            ), {"scope": str(scope), "limit": args.limit})).mappings().all()
            outbox = (await connection.execute(text(
                "SELECT o.id, o.kind, o.status, o.operation_id FROM outbox o JOIN operations p ON p.id=o.operation_id "
                "WHERE p.owner_scope=:scope AND o.status!='ACKED' ORDER BY o.created_at, o.id LIMIT :limit"
            ), {"scope": str(scope), "limit": args.limit})).mappings().all()
            critics = (await connection.execute(text(
                "SELECT task_id, critic_registry_id, delete_operation_id, stage FROM critic_workflows "
                "WHERE owner_scope=:scope AND stage='DELETE_PENDING' ORDER BY updated_at LIMIT :limit"
            ), {"scope": str(scope), "limit": args.limit})).mappings().all()
            artifacts = (await connection.execute(text(
                "SELECT DISTINCT a.artifact_ref, a.storage_state FROM artifacts a "
                "JOIN evidence e ON e.artifact_ref=a.artifact_ref WHERE e.scope=:scope "
                "AND a.storage_state IN ('DELETE_PENDING','DELETE_FAILED') ORDER BY a.artifact_ref LIMIT :limit"
            ), {"scope": str(scope), "limit": args.limit})).mappings().all()
            projections = (await connection.execute(text(
                "SELECT topic_id, target_registry_id, desired_version, applied_version, state, pending_reason "
                "FROM memory_projections WHERE scope=:scope AND state!='APPLIED' "
                "ORDER BY updated_at LIMIT :limit"
            ), {"scope": str(scope), "limit": args.limit})).mappings().all()
        report.update({
            "incomplete_operations": [dict(row) for row in operations],
            "pending_inbox": [{"id": row["id"], "event_type": row["payload"].get("event_type")} for row in inbox_rows],
            "pending_turn_results": list(result_ids),
            "pending_calls": [{
                "accounting_call_id": row["accounting_call_id"], "operation_id": row["operation_id"],
                "status": row["status"], "completeness": row["completeness"],
                "settlement_state": row["settlement_state"], "has_conflict": row["has_conflict"],
            } for row in pending_calls],
            "pending_outbox": [dict(row) for row in outbox],
            "critic_deletion": [dict(row) for row in critics],
            "archive_deletion": [dict(row) for row in artifacts],
            "projection": [dict(row) for row in projections],
            "unknown_state_before_apply": {
                "operations": unknown_snapshot["operations"],
                "calls": unknown_snapshot["calls"],
                "task_held_usd": str(unknown_snapshot["task_held"]),
            },
            "inference_requested": False,
        })
        if args.apply:
            applied_inbox = []
            for row in inbox_rows:
                try:
                    outcome = await process_inbox_row(
                        factory, row, processor_owner=settings.worker_id,
                        hekate_config=configured_task_execution(settings),
                        critic_config=configured_critic_execution(settings),
                    )
                    applied_inbox.append({"id": row["id"], "processed": outcome.get("processed", False)})
                except Exception as error:
                    applied_inbox.append({"id": row["id"], "error": type(error).__name__})
            try:
                applied_results = await process_pending_results(
                    factory, args.limit, hekate_config=configured_task_execution(settings),
                    critic_config=configured_critic_execution(settings), owner_scope=scope,
                )
            except Exception as error:
                applied_results = {"error": type(error).__name__}
            settled = []
            for row in pending_calls:
                if (
                    row["status"] == "QUIESCENT"
                    and row["completeness"] in {"COMPLETE", "PARTIAL"}
                    and not row["has_conflict"]
                    and row["settlement_state"] != "SETTLED"
                ):
                    from hekate.application.budgets import reconcile_observed_call
                    try:
                        result = await reconcile_observed_call(factory, AccountingCallId(row["accounting_call_id"]))
                        settled.append({
                            "accounting_call_id": row["accounting_call_id"],
                            "settled": result.settled if result is not None else False,
                        })
                    except Exception as error:
                        settled.append({
                            "accounting_call_id": row["accounting_call_id"],
                            "error": type(error).__name__,
                        })
            report["apply"] = {
                "inbox": applied_inbox, "turn_results_processed": applied_results,
                "settlements": settled,
            }
            async with engine.connect() as connection:
                after_unknowns = (await connection.execute(text(
                    "SELECT "
                    "(SELECT count(*) FROM operations WHERE owner_scope=:scope AND (state='UNKNOWN' OR execution_state='UNKNOWN')) AS operations, "
                    "(SELECT count(*) FROM provider_calls p JOIN operations o ON o.id=p.operation_id "
                    " WHERE o.owner_scope=:scope AND p.status='UNKNOWN') AS calls, "
                    "(SELECT coalesce(sum(b.held_amount),0) FROM budget_accounts b WHERE b.scope_kind='TASK' "
                    " AND b.scope_ref IN (SELECT id FROM tasks WHERE owner_scope=:scope)) AS task_held"
                ), {"scope": str(scope)})).mappings().one()
            report["unknown_state_after_apply"] = {
                "operations": after_unknowns["operations"],
                "calls": after_unknowns["calls"],
                "task_held_usd": str(after_unknowns["task_held"]),
            }
        _write(report)
        return 0
    finally:
        await engine.dispose()


async def _run_command() -> int:
    settings = _settings()
    if settings.runtime_mode == "local":
        validate_local_settings(settings)
    elif settings.runtime_mode == "test":
        from hekate.settings import validate_settings
        validate_settings(settings)
    else:
        raise ValueError("run requires an explicitly enabled local or synthetic test profile")
    readiness = await run_doctor(settings)
    checks = readiness.get("checks", {})
    required = ["configuration", "profile_assets", "postgres", "authorization", "node_bridge", "letta_runtime"]
    if settings.runtime_mode == "local":
        required.append("ollama")
    blocked = [key for key in required if not isinstance(checks.get(key), dict) or checks[key].get("status") != "READY"]
    if blocked:
        raise ValueError("run preflight failed: " + ", ".join(blocked))
    gateway = checks.get("provider_gateway", {})
    gateway_status = gateway.get("status") if isinstance(gateway, dict) else None
    if gateway_status not in {"READY", "NOT_RUNNING"}:
        raise ValueError("run preflight failed: provider gateway is not ready")
    children: list[asyncio.subprocess.Process] = []
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    waits: dict[asyncio.Task[int], asyncio.subprocess.Process] = {}
    stop_task = asyncio.create_task(stop.wait())
    try:
        if gateway_status == "NOT_RUNNING":
            bind = settings.local["gateway"]
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind((bind["host"], bind["port"]))
            except OSError as error:
                raise ValueError("configured provider gateway port is already in use") from error
            finally:
                probe.close()
            children.append(await asyncio.create_subprocess_exec(
                sys.executable, "-m", "hekate", "gateway", cwd=settings.project_dir,
            ))
            waits[asyncio.create_task(children[-1].wait())] = children[-1]
            print(f"Started private gateway (PID {children[-1].pid}).", flush=True)
            host, port = bind["host"], bind["port"]
            for _ in range(100):
                if stop.is_set():
                    return 130
                try:
                    _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=0.2)
                    writer.close()
                    await writer.wait_closed()
                    break
                except (OSError, TimeoutError):
                    if children[-1].returncode is not None:
                        raise ValueError("managed provider gateway exited during startup")
                    await asyncio.sleep(0.1)
            else:
                raise ValueError("managed provider gateway did not become ready")
        if stop.is_set():
            return 130
        children.append(await asyncio.create_subprocess_exec(
            sys.executable, "-m", "hekate", "worker", cwd=settings.project_dir,
        ))
        waits[asyncio.create_task(children[-1].wait())] = children[-1]
        print(f"Started HEKATE worker (PID {children[-1].pid}). Press Ctrl+C to stop these child processes.", flush=True)
        done, _ = await asyncio.wait((*waits.keys(), stop_task), return_when=asyncio.FIRST_COMPLETED)
        unexpected = next((task for task in done if task in waits), None)
        if unexpected is not None:
            code = unexpected.result()
            print(f"Managed process exited with status {code}; stopping remaining managed processes.", flush=True)
            return code or 1
        return 0
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        for child in children:
            if child.returncode is None:
                child.terminate()
        try:
            await asyncio.wait_for(asyncio.gather(*(child.wait() for child in children)), timeout=10)
        except TimeoutError:
            for child in children:
                if child.returncode is None:
                    child.kill()
            await asyncio.gather(*(child.wait() for child in children))
        for task in waits:
            if not task.done():
                task.cancel()
        if not stop_task.done():
            stop_task.cancel()
        await asyncio.gather(*waits, stop_task, return_exceptions=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hekate")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("worker")
    subparsers.add_parser("init-local", help="apply migrations and initialize the fixed local scope")
    subparsers.add_parser("gateway", help="run the private loopback provider gateway")
    subparsers.add_parser("doctor", help="read-only local configuration and service checks")
    ask = subparsers.add_parser("ask", help="submit a question from stdin")
    ask.add_argument("--request-key", required=True)
    ask.add_argument("--wait-seconds", type=float, default=30.0)
    ask.add_argument("--topic-id")
    ask.add_argument("--evidence-id", action="append", default=[])
    chat = subparsers.add_parser("chat", help="interactive local Task session")
    chat.add_argument("--wait-seconds", type=float, default=900.0)
    run = subparsers.add_parser("run", help="run the private gateway and worker in the foreground")
    run.set_defaults(_run=True)
    status = subparsers.add_parser("status", help="show bounded owner-scoped local status")
    status.add_argument("--json", action="store_true")
    reconcile = subparsers.add_parser("reconcile", help="inspect or apply durable observations")
    reconcile.add_argument("--apply", action="store_true", help="process persisted observations and confirmed usage only")
    reconcile.add_argument("--limit", type=int, default=100)
    task = subparsers.add_parser("task", help="show a Task in the configured scope")
    task.add_argument("task_id")
    cancel = subparsers.add_parser("cancel", help="request Task cancellation")
    cancel.add_argument("task_id")
    evidence = subparsers.add_parser("evidence")
    evidence_subparsers = evidence.add_subparsers(dest="evidence_command", required=True)
    evidence_import = evidence_subparsers.add_parser("import")
    evidence_import.add_argument("file")
    evidence_import.add_argument("--request-key", required=True)
    evidence_import.add_argument("--kind", required=True)
    evidence_import.add_argument("--retention-class", required=True)
    evidence_import.add_argument("--expires-at", required=True)
    evidence_import.add_argument("--source-uri")
    evidence_show = evidence_subparsers.add_parser("show")
    evidence_show.add_argument("evidence_id")
    position = subparsers.add_parser("position")
    position_subparsers = position.add_subparsers(dest="position_command", required=True)
    position_show = position_subparsers.add_parser("show")
    position_show.add_argument("topic_id")
    position_history = position_subparsers.add_parser("history")
    position_history.add_argument("topic_id")
    position_history.add_argument("--after-version", type=int, default=0)
    position_history.add_argument("--limit", type=int, default=50)
    args = parser.parse_args(argv)
    if args.command == "init-local":
        return _run_async(_init_local())
    if args.command == "gateway":
        return _run_async(_gateway())
    if args.command == "doctor":
        return _run_async(_doctor())
    if args.command == "worker":
        return _run_async(_worker())
    if args.command == "run":
        return _run_async(_run_command(), show_validation_error=True)
    if args.command == "chat":
        if not 0 <= args.wait_seconds <= 86_400:
            parser.error("--wait-seconds must be between 0 and 86400")
        return _run_async(_chat_command(args))
    if args.command == "status":
        return _run_async(_status_command(json_output=args.json))
    if args.command == "reconcile":
        if not 1 <= args.limit <= 100:
            parser.error("--limit must be between 1 and 100")
        return _run_async(_reconcile_command(args))
    if args.command in {"ask", "task", "cancel"}:
        if args.command == "ask" and not 0 <= args.wait_seconds <= 3_600:
            parser.error("--wait-seconds must be between 0 and 3600")
        return _run_async(_task_command(args.command, args))
    if args.command in {"evidence", "position"}:
        if args.command == "position" and args.position_command == "history":
            if args.after_version < 0 or not 1 <= args.limit <= 200:
                parser.error("history requires --after-version >= 0 and --limit between 1 and 200")
        return _run_async(_knowledge_command(args))
    parser.error(f"{args.command} is outside the supported local CLI scope")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        raise SystemExit(130)
