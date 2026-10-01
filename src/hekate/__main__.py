from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
import os
from pathlib import Path
import signal

from hekate.bootstrap import build_container, close_container
from hekate.settings import load_settings
from hekate.worker.service import run_worker


async def _worker() -> int:
    config_dir = Path(os.environ.get("HEKATE_CONFIG_DIR", "config"))
    settings = load_settings(os.environ, config_dir)
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hekate")
    parser.add_argument("command", choices=("serve", "worker", "reconcile", "doctor"))
    args = parser.parse_args(argv)
    if args.command == "worker":
        return asyncio.run(_worker())
    parser.error(f"{args.command} is outside the Phase 3 runtime-dispatch scope")


if __name__ == "__main__":
    raise SystemExit(main())
