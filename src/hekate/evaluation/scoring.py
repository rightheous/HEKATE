from __future__ import annotations

from hekate.domain.models import (
    Comparison, GateReport, QualityScore,
)


def score_trial(answer: object, rubric: object) -> QualityScore:
    raise NotImplementedError


def compare_paired(trials: object) -> Comparison:
    raise NotImplementedError


def evaluate_release_gates(report: object, thresholds: object) -> GateReport:
    raise NotImplementedError
