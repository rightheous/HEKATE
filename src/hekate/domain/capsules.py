from __future__ import annotations

from collections.abc import Mapping, Sequence

from pydantic import TypeAdapter

from .contracts import MAX_CONTRACT_BYTES, check_json_payload
from .models import (
    Attempt, ConclusionCapsule, EvidenceInput, EvidenceView,
    ExpectedOutput, HekateProposal, HekateTurnOutput, JsonSchema, PositionCommitRequest, RuntimeBinding, TaskCapsule,
    RuntimeLimitsCapsule,
    TaskSnapshot,
)
from .types import Revision


def build_task_capsule(
    snapshot: TaskSnapshot, attempt: Attempt, evidence: Sequence[EvidenceView], *, max_output_tokens: int | None = None,
) -> TaskCapsule:
    if attempt.task_id != snapshot.task.id or attempt.input_revision != snapshot.task.input_revision:
        raise ValueError("Task Capsule snapshot and attempt do not match")
    if evidence:
        raise ValueError("Phase 3B does not retrieve or attach Evidence")
    return TaskCapsule(
        schema_version="1",
        task_id=snapshot.task.id,
        attempt_id=attempt.id,
        input_revision=attempt.input_revision,
        objective=snapshot.task.question,
        reasoning_role="hekate",
        mode="independent_exploration",
        premises=(),
        evidence_refs=(),
        target_position=None,
        constraints=(),
        expected_output=ExpectedOutput(schema="hekate_turn_output_v1"),
        runtime_limits=RuntimeLimitsCapsule(
            max_output_tokens=max_output_tokens,
            deadline_at=snapshot.task.deadline.isoformat(),
        ),
        capability_profile="reasoning-readonly",
    )


def parse_conclusion(payload: bytes) -> ConclusionCapsule:
    return ConclusionCapsule.model_validate_json(check_json_payload(payload), strict=True)


def parse_hekate_turn_output(payload: bytes) -> HekateTurnOutput:
    return HekateTurnOutput.model_validate_json(check_json_payload(payload), strict=True)


def parse_task_capsule(payload: bytes) -> TaskCapsule:
    return TaskCapsule.model_validate_json(check_json_payload(payload), strict=True)


def parse_position_commit(payload: bytes) -> PositionCommitRequest:
    return PositionCommitRequest.model_validate_json(check_json_payload(payload), strict=True)


def parse_evidence(payload: bytes) -> EvidenceInput:
    return EvidenceInput.model_validate_json(check_json_payload(payload), strict=True)


def validate_capsule_binding(
    capsule: ConclusionCapsule, binding: RuntimeBinding
) -> Revision:
    """Compare wire claims with trusted binding values; perform no auth or lookup."""
    mismatches = [
        name
        for name, actual, expected in (
            ("task_id", capsule.task_id, binding.task_id),
            ("attempt_id", capsule.attempt_id, binding.attempt_id),
            ("agent_id", capsule.agent_id, binding.agent_registry_id),
        )
        if actual != expected
    ]
    if capsule.input_revision is not None and capsule.input_revision != binding.input_revision:
        mismatches.append("input_revision")
    if mismatches:
        raise ValueError(f"capsule does not match runtime binding: {', '.join(mismatches)}")
    return binding.input_revision


def export_schemas() -> Mapping[str, JsonSchema]:
    from .bridge_contracts import bridge_schema

    models = {
        "task-capsule.v1.schema.json": TaskCapsule,
        "conclusion-capsule.v1.schema.json": ConclusionCapsule,
        "hekate-proposal.v1.schema.json": TypeAdapter(HekateProposal),
        "position-commit.v1.schema.json": PositionCommitRequest,
        "hekate-turn-output.v1.schema.json": HekateTurnOutput,
    }
    schemas: dict[str, JsonSchema] = {}
    for filename, model in models.items():
        schema = (
            model.json_schema(mode="validation")
            if isinstance(model, TypeAdapter)
            else model.model_json_schema(mode="validation")
        )
        schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
        schema["$id"] = f"urn:hekate:contract:{filename.removesuffix('.v1.schema.json')}:v1"
        schema["$comment"] = (
            f"Parser enforces a {MAX_CONTRACT_BYTES}-byte UTF-8 JSON payload limit."
        )
        schemas[filename] = schema
    schemas["bridge.v1.schema.json"] = bridge_schema()
    return schemas
