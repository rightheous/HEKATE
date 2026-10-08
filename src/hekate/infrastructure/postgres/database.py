from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from hekate.domain.errors import StorageUnavailable
from hekate.domain.models import DatabaseHealth
from hekate.ports.store import UnitOfWork
from hekate.settings import Settings

from .agent_repository import PostgresAgentRepository
from .budget_repository import PostgresBudgetRepository
from .delivery_repository import PostgresDeliveryRepository
from .knowledge_repository import PostgresKnowledgeRepository
from .task_repository import PostgresTaskRepository
from .critic_workflow_repository import PostgresCriticWorkflowRepository
from .deliberation_repository import PostgresDeliberationRepository
from .projection_repository import PostgresProjectionRepository


def create_engine(settings: Settings | str) -> AsyncEngine:
    url = settings.database_url if isinstance(settings, Settings) else settings
    if not url.startswith("postgresql+psycopg://"):
        raise ValueError("HEKATE requires the PostgreSQL psycopg async URL")
    return create_async_engine(url, pool_pre_ping=True)


class PostgresUnitOfWork:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self.session: AsyncSession | None = None
        self._entered = False
        self.tasks: PostgresTaskRepository
        self.agents: PostgresAgentRepository
        self.budgets: PostgresBudgetRepository
        self.delivery: PostgresDeliveryRepository
        self.knowledge: PostgresKnowledgeRepository
        self.critic_workflows: PostgresCriticWorkflowRepository
        self.deliberation: PostgresDeliberationRepository
        self.projections: PostgresProjectionRepository

    async def __aenter__(self) -> PostgresUnitOfWork:
        if self._entered:
            raise RuntimeError("UnitOfWork instances cannot be reused")
        self._entered = True
        self.session = self._sessions()
        self.tasks = PostgresTaskRepository(self.session)
        self.agents = PostgresAgentRepository(self.session)
        self.budgets = PostgresBudgetRepository(self.session)
        self.delivery = PostgresDeliveryRepository(self.session)
        self.knowledge = PostgresKnowledgeRepository(self.session)
        self.critic_workflows = PostgresCriticWorkflowRepository(self.session)
        self.deliberation = PostgresDeliberationRepository(self.session)
        self.projections = PostgresProjectionRepository(self.session)
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self.session is None:
            return
        try:
            if isinstance(exc, SQLAlchemyError):
                await self.session.rollback()
                raise StorageUnavailable("database transaction failed") from exc
            if self.session.in_transaction():
                await self.session.rollback()
        except SQLAlchemyError as error:
            raise StorageUnavailable("database rollback failed") from error
        finally:
            await self.session.close()

    async def commit(self) -> None:
        if self.session is None:
            raise RuntimeError("UnitOfWork is not active")
        try:
            await self.session.commit()
        except SQLAlchemyError as error:
            raise StorageUnavailable("database commit failed") from error

    async def rollback(self) -> None:
        if self.session is None:
            raise RuntimeError("UnitOfWork is not active")
        try:
            await self.session.rollback()
        except SQLAlchemyError as error:
            raise StorageUnavailable("database rollback failed") from error


def create_uow_factory(engine: AsyncEngine) -> Callable[[], UnitOfWork]:
    sessions = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    return lambda: PostgresUnitOfWork(sessions)


async def check_database(engine: AsyncEngine) -> DatabaseHealth:
    try:
        async with engine.connect() as connection:
            version = await connection.scalar(text("SELECT version()"))
            head = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
            return DatabaseHealth(
                available=True,
                postgres_version=str(version).split(",", 1)[0],
                migration_head=head,
            )
    except SQLAlchemyError:
        return DatabaseHealth(available=False, postgres_version=None, migration_head=None)
