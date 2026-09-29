from __future__ import annotations

import asyncio

from hekate.domain.models import EvalSpec, EvaluationArtifact
from hekate.evaluation.runner import run_suite


async def evaluate(spec: EvalSpec) -> EvaluationArtifact:
    return await run_suite(spec)


def main() -> int:
    asyncio.run(evaluate({}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
