from __future__ import annotations

from dataclasses import dataclass

from hekate.ports.runtime import AgentRuntime
from hekate.ports.store import UowFactory
from hekate.settings import Settings


@dataclass(slots=True)
class Container:
    settings: Settings
    runtime: AgentRuntime
    uow_factory: UowFactory
    database: object


async def build_container(settings: Settings) -> Container:
    raise NotImplementedError


async def close_container(container: Container) -> None:
    raise NotImplementedError
