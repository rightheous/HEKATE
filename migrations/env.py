"""Alembic entry point; online/offline runners are wired during DB implementation."""

from alembic import context

from hekate.infrastructure.postgres.tables import metadata

target_metadata = metadata()


def run_migrations_offline() -> None:
    raise NotImplementedError


def run_migrations_online() -> None:
    raise NotImplementedError
