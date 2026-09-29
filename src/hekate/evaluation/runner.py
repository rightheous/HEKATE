from __future__ import annotations

from hekate.domain.models import (
    EvalSpec, EvaluationArtifact, GradingBatch, TrialResult,
)


async def run_case(case: object, arm: str, budget: object) -> TrialResult:
    raise NotImplementedError


async def run_suite(spec: EvalSpec) -> EvaluationArtifact:
    raise NotImplementedError


def make_blinded_sample(results: object) -> GradingBatch:
    raise NotImplementedError
