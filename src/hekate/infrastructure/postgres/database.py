from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hekate.domain.models import DatabaseHealth
from hekate.ports.store import UnitOfWork
from hekate.settings import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(settings.database_url)


async def open_uow() -> UnitOfWork:
    raise NotImplementedError


async def check_database() -> DatabaseHealth:
    raise NotImplementedError
