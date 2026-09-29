from __future__ import annotations

import asyncio

from hekate.domain.models import CapabilityReport, DeploymentProfile


async def probe_all(profile: DeploymentProfile) -> CapabilityReport:
    raise NotImplementedError


def main() -> int:
    asyncio.run(probe_all({}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
