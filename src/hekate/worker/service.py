from __future__ import annotations

import asyncio
from datetime import datetime

from hekate.bootstrap import Container
from hekate.domain.models import Job


async def run_worker(container: Container, stop_event: asyncio.Event) -> None:
    raise NotImplementedError


async def dispatch_job(job: Job) -> None:
    raise NotImplementedError


async def consume_runtime_events() -> None:
    raise NotImplementedError


async def heartbeat_leases() -> None:
    raise NotImplementedError


async def drain_shutdown(deadline: datetime) -> None:
    raise NotImplementedError
