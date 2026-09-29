from __future__ import annotations

from collections.abc import Mapping, Sequence

from .models import (
    Attempt, Capsule, ConclusionCapsule, EvidenceView, JsonSchema, RuntimeBinding,
    TaskCapsule, TaskSnapshot,
)


def build_task_capsule(
    snapshot: TaskSnapshot, attempt: Attempt, evidence: Sequence[EvidenceView]
) -> TaskCapsule:
    raise NotImplementedError


def parse_conclusion(payload: bytes) -> ConclusionCapsule:
    raise NotImplementedError


def validate_capsule_binding(capsule: Capsule, binding: RuntimeBinding) -> None:
    raise NotImplementedError


def export_schemas() -> Mapping[str, JsonSchema]:
    raise NotImplementedError
