from __future__ import annotations

from fastapi import FastAPI

from hekate.api.routes import router
from hekate.bootstrap import Container
from hekate.domain.models import HealthReport


def create_app(container: Container) -> FastAPI:
    app = FastAPI(title="HEKATE")
    app.state.container = container
    app.include_router(router)
    return app


async def readiness(container: Container) -> HealthReport:
    raise NotImplementedError
