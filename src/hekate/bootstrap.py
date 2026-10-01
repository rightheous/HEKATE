from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text

from hekate.domain.errors import StorageUnavailable
from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.infrastructure.letta.bridge_protocol import BridgeClient
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.ports.runtime import AgentRuntime
from hekate.ports.store import UowFactory
from hekate.settings import Settings, validate_settings


@dataclass(slots=True)
class Container:
    settings: Settings
    runtime: AgentRuntime
    uow_factory: UowFactory
    database: object


async def build_container(settings: Settings) -> Container:
    validate_settings(settings)
    engine = create_engine(settings.database_url)
    bridge = BridgeClient(
        settings.node_bin,
        settings.bridge_entry,
        env={
            "HEKATE_LETTA_URL": settings.letta_url,
            **({"HEKATE_LETTA_TOKEN": settings.letta_token} if settings.letta_token else {}),
        },
    )
    runtime = LettaRuntimeAdapter(bridge)
    try:
        await runtime.verify_compatibility()
        factory = create_uow_factory(engine)
        async with factory() as uow:
            health = await uow.session.execute(text("SELECT 1"))
            if health.scalar_one() != 1:
                raise StorageUnavailable("database health check failed")
            await uow.commit()
        return Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
    except BaseException:
        await bridge.close()
        await engine.dispose()
        raise


async def close_container(container: Container) -> None:
    bridge = getattr(container.runtime, "bridge", None)
    if bridge is not None:
        await bridge.close()
    await container.database.dispose()
