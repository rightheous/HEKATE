from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
import json
import os
from pathlib import Path
import signal
import sys

from hekate.application import tasks
from hekate.bootstrap import build_container, close_container
from hekate.domain.models import UserMessage
from hekate.domain.types import StopReason, TaskId
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
            receipt = await tasks.submit(factory, actor, UserMessage(text=question), args.request_key, config)
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hekate")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("worker")
    ask = subparsers.add_parser("ask", help="submit a question from stdin")
    ask.add_argument("--request-key", required=True)
    ask.add_argument("--wait-seconds", type=float, default=30.0)
    task = subparsers.add_parser("task", help="show a Task in the configured scope")
    task.add_argument("task_id")
    cancel = subparsers.add_parser("cancel", help="request Task cancellation")
    cancel.add_argument("task_id")
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
    parser.error(f"{args.command} is outside the Phase 3B single-user CLI scope")


if __name__ == "__main__":
    raise SystemExit(main())
