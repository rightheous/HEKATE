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
import sys
from uuid import uuid4

from hekate.application import tasks
from hekate.bootstrap import build_container, close_container
from hekate.domain.models import EvidenceInput, ReadLimits, UserMessage, VersionCursor
from hekate.domain.types import EvidenceId, StopReason, TaskId, TopicId
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.settings import (
    configured_local_actor, configured_task_execution, load_settings,
)
from hekate.worker.service import run_worker


def _settings():
    config_dir = Path(os.environ.get("HEKATE_CONFIG_DIR", "config"))
    return load_settings(os.environ, config_dir)


def _write(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


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


async def _task_command(command: str, args: argparse.Namespace) -> int:
    settings = _settings()
    if not settings.database_url.startswith("postgresql+psycopg://"):
        raise ValueError("HEKATE_DATABASE_URL must be configured for PostgreSQL")
    actor = configured_local_actor(settings)
    engine = create_engine(settings.database_url)
    factory = create_uow_factory(engine)
    try:
        if command == "ask":
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hekate")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("worker")
    ask = subparsers.add_parser("ask", help="submit a question from stdin")
    ask.add_argument("--request-key", required=True)
    ask.add_argument("--wait-seconds", type=float, default=30.0)
    ask.add_argument("--topic-id")
    ask.add_argument("--evidence-id", action="append", default=[])
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
    subparsers.add_parser("serve")
    subparsers.add_parser("reconcile")
    subparsers.add_parser("doctor")
    args = parser.parse_args(argv)
    if args.command == "worker":
        return asyncio.run(_worker())
    if args.command in {"ask", "task", "cancel"}:
        if args.command == "ask" and not 0 <= args.wait_seconds <= 3_600:
            parser.error("--wait-seconds must be between 0 and 3600")
        return asyncio.run(_task_command(args.command, args))
    if args.command in {"evidence", "position"}:
        if args.command == "position" and args.position_command == "history":
            if args.after_version < 0 or not 1 <= args.limit <= 200:
                parser.error("history requires --after-version >= 0 and --limit between 1 and 200")
        return asyncio.run(_knowledge_command(args))
    parser.error(f"{args.command} is outside the supported local CLI scope")


if __name__ == "__main__":
    raise SystemExit(main())
