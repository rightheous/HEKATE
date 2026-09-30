from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from hekate.domain.capsules import (
    export_schemas,
    parse_conclusion,
    parse_evidence,
    parse_position_commit,
    parse_task_capsule,
    validate_capsule_binding,
)
from hekate.domain.contracts import MAX_CONTRACT_BYTES, MAX_CONTRACT_ITEMS
from hekate.domain.models import EvidenceRecord, RuntimeBinding
from hekate.domain.proposals import parse_hekate_proposal
from hekate.domain.types import ScopeId
from scripts.export_contracts import write_schemas


TASK_CAPSULE = {
    "schema_version": "1",
    "task_id": "dt_0023",
    "attempt_id": "at_001",
    "input_revision": 7,
    "objective": "현재 설계의 주요 실패 조건을 검토하라.",
    "reasoning_role": "critic",
    "mode": "targeted_review",
    "premises": [
        {
            "id": "P1",
            "text": "Control Plane은 단일 서비스로 시작한다.",
            "kind": "design_constraint",
        }
    ],
    "evidence_refs": ["E12"],
    "target_position": {
        "topic_id": "backend_architecture",
        "version": 10,
        "summary": "...",
    },
    "constraints": [
        "검증되지 않은 사실을 외부 관찰처럼 제시하지 않는다.",
        "반론의 성립 조건을 명시한다.",
    ],
    "expected_output": {"schema": "conclusion_capsule_v1"},
    "runtime_limits": {
        "max_output_tokens": 3000,
        "max_tool_calls": 3,
        "deadline_at": "...",
    },
    "capability_profile": "reasoning-readonly",
}

CONCLUSION_CAPSULE = {
    "schema_version": "1",
    "task_id": "dt_0023",
    "attempt_id": "at_001",
    "agent_id": "ha_0042",
    "status": "done",
    "assessment": {
        "statement": "중복 생성 및 늦은 결과에 대한 처리 규칙이 필요하다.",
        "confidence": {
            "level": "high",
            "basis": ["정상 경로 외의 상태 전이가 정의되어 있지 않다."],
        },
    },
    "evidence_used": ["E12"],
    "objections": [
        {
            "id": "O1",
            "severity": "high",
            "claim": "생성 응답 유실 시 agent가 중복 생성될 수 있다.",
            "condition": "provider가 생성 요청의 idempotency를 보장하지 않을 때",
            "suggested_validation": "응답 유실 fault injection",
        }
    ],
    "assumptions": [],
    "unresolved": [],
    "recommended_next_step": {"type": "none"},
    "position_recommendation": {
        "action": "modify",
        "summary": "장애 복구 계약을 추가한다.",
    },
}

POSITION_COMMIT = {
    "operation_id": "op_commit_0042",
    "task_id": "dt_0023",
    "topic_id": "backend_architecture",
    "base_version": 10,
    "input_revision": 7,
    "proposed_position": {
        "statement": "현재 규모에서는 Python 단일 구현을 우선한다.",
        "applicability": ["현재 성능 요구사항을 유지하는 동안"],
        "confidence": {
            "level": "medium",
            "basis": ["개발 비용과 운영 복잡도를 우선했다."],
        },
        "evidence_refs": ["E12"],
        "assumptions": ["향후 3개월 내 처리량이 급증하지 않는다."],
        "dissent_refs": ["D4"],
    },
    "reason_for_change": "운영 인력과 일정 제약이 갱신되었다.",
}

EVIDENCE = {
    "id": "E12",
    "kind": "external_observation",
    "source_uri": "archive://documents/design-review-01",
    "locator": "section 3",
    "retrieved_at": "2026-01-01T10:00:00Z",
    "observed_at": None,
    "content_hash": "sha256:...",
    "derived_from": [],
    "access_scope": "project_hekate",
    "retention_class": "project",
}


def wire(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def test_source_examples_parse_as_contracts() -> None:
    task = parse_task_capsule(wire(TASK_CAPSULE))
    conclusion = parse_conclusion(wire(CONCLUSION_CAPSULE))
    commit = parse_position_commit(wire(POSITION_COMMIT))
    evidence = parse_evidence(wire(EVIDENCE))

    assert task.expected_output.schema_id == "conclusion_capsule_v1"
    assert task.runtime_limits.deadline_at == "..."
    assert conclusion.assessment.confidence.level == "high"
    assert conclusion.assessment.confidence.missing_evidence == ()
    assert commit.proposed_position.applicability == ("현재 성능 요구사항을 유지하는 동안",)
    assert commit.schema_version == "1"
    assert not hasattr(commit.proposed_position, "scope")
    assert evidence.schema_version == "1"
    assert evidence.access_scope == "project_hekate"

    record = EvidenceRecord.from_input(
        evidence,
        {"project_hekate": ScopeId("scope-42")},
        content_version="v1",
        availability="available",
        access_epoch=4,
    )
    assert record.scope == "scope-42"
    assert record.access_scope == "project_hekate"
    assert record.content_version == "v1"
    assert record.availability == "available"
    assert record.access_epoch == 4
    with pytest.raises(ValueError, match="unmapped access_scope"):
        EvidenceRecord.from_input(
            evidence,
            {},
            content_version="v1",
            availability="available",
            access_epoch=4,
        )

    derived = parse_evidence(
        wire({**EVIDENCE, "derived_from": ["E11"], "root_source_ids": ["E10"]})
    )
    derived_record = EvidenceRecord.from_input(
        derived,
        {"project_hekate": ScopeId("scope-42")},
        content_version="v1",
        availability="available",
        access_epoch=4,
    )
    assert derived_record.derived_from == ("E11",)
    assert derived_record.root_source_ids == ("E10",)


@pytest.mark.parametrize(
    ("parser", "source", "path", "value"),
    [
        (parse_conclusion, CONCLUSION_CAPSULE, ("assessment", "confidence", "level"), 0.8),
        (parse_position_commit, POSITION_COMMIT, ("proposed_position", "confidence", "level"), 0.8),
    ],
)
def test_numeric_confidence_is_rejected(parser, source, path, value) -> None:
    invalid = copy.deepcopy(source)
    node = invalid
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(ValueError):
        parser(wire(invalid))


@pytest.mark.parametrize(
    ("parser", "source", "path"),
    [
        (parse_conclusion, CONCLUSION_CAPSULE, ("assessment", "confidence")),
        (parse_conclusion, CONCLUSION_CAPSULE, ("objections", 0, "condition")),
        (parse_position_commit, POSITION_COMMIT, ("proposed_position", "confidence", "basis")),
    ],
)
def test_missing_core_nested_fields_are_rejected(parser, source, path) -> None:
    invalid = copy.deepcopy(source)
    node = invalid
    for key in path[:-1]:
        node = node[key]
    del node[path[-1]]
    with pytest.raises(ValueError):
        parser(wire(invalid))


@pytest.mark.parametrize(
    ("parser", "source", "path"),
    [
        (parse_conclusion, CONCLUSION_CAPSULE, ("unrecognized",)),
        (parse_conclusion, CONCLUSION_CAPSULE, ("assessment", "unrecognized")),
        (parse_conclusion, CONCLUSION_CAPSULE, ("objections", 0, "unrecognized")),
        (parse_position_commit, POSITION_COMMIT, ("proposed_position", "unrecognized")),
        (parse_task_capsule, TASK_CAPSULE, ("premises", 0, "unrecognized")),
    ],
)
def test_unknown_fields_are_rejected_at_every_level(parser, source, path) -> None:
    invalid = copy.deepcopy(source)
    node = invalid
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = True
    with pytest.raises(ValueError):
        parser(wire(invalid))


def test_duplicate_keys_and_oversized_json_are_rejected() -> None:
    duplicate = b'{"schema_version":"1","schema_version":"1"}'
    with pytest.raises(ValueError, match="duplicate JSON key"):
        parse_conclusion(duplicate)

    with pytest.raises(ValueError, match="exceeds"):
        parse_conclusion(b" " * (MAX_CONTRACT_BYTES + 1))

    too_many = {**TASK_CAPSULE, "constraints": ["x"] * (MAX_CONTRACT_ITEMS + 1)}
    with pytest.raises(ValueError):
        parse_task_capsule(wire(too_many))


def test_binding_checks_task_attempt_revision_and_agent() -> None:
    source = {**CONCLUSION_CAPSULE, "input_revision": 7}
    capsule = parse_conclusion(wire(source))
    binding = RuntimeBinding(
        task_id="dt_0023",
        attempt_id="at_001",
        agent_registry_id="ha_0042",
        provider_agent_id="agent-xxxx",
        conversation_id="conversation-1",
        input_revision=7,
        fence=1,
    )
    assert binding.agent_registry_id != binding.provider_agent_id
    assert validate_capsule_binding(capsule, binding) == 7
    provider_changed = binding.model_copy(update={"provider_agent_id": "other-provider-id"})
    assert validate_capsule_binding(capsule, provider_changed) == 7

    for field, value in (
        ("task_id", "other-task"),
        ("attempt_id", "other-attempt"),
        ("input_revision", 8),
        ("agent_registry_id", "other-registry-id"),
    ):
        with pytest.raises(ValueError, match="agent_id" if field == "agent_registry_id" else field):
            validate_capsule_binding(capsule, binding.model_copy(update={field: value}))

    unbound = parse_conclusion(wire(CONCLUSION_CAPSULE))
    assert unbound.input_revision is None
    assert validate_capsule_binding(unbound, binding) == binding.input_revision


def test_discriminated_proposal_union_and_nested_position_are_strict() -> None:
    proposal = {
        "schema_version": "1",
        "action": "commit",
        "operation_id": "op_commit_0042",
        "task_id": "dt_0023",
        "topic_id": "backend_architecture",
        "base_version": 10,
        "input_revision": 7,
        "proposed_position": POSITION_COMMIT["proposed_position"],
        "reason_for_change": "운영 인력과 일정 제약이 갱신되었다.",
    }
    assert parse_hekate_proposal(wire(proposal)).action == "commit"

    proposal["proposed_position"] = {
        **POSITION_COMMIT["proposed_position"],
        "unknown": True,
    }
    with pytest.raises(ValueError):
        parse_hekate_proposal(wire(proposal))


def test_checked_in_schemas_match_the_generator(tmp_path) -> None:
    report = write_schemas(tmp_path)
    checked_in = Path(__file__).resolve().parents[2] / "contracts" / "generated"
    schemas = export_schemas()
    expected_files = {
        "bridge.v1.schema.json",
        "task-capsule.v1.schema.json",
        "conclusion-capsule.v1.schema.json",
        "hekate-proposal.v1.schema.json",
        "position-commit.v1.schema.json",
    }
    assert set(schemas) == expected_files
    assert set(report.schemas) == expected_files
    assert {path.name for path in checked_in.glob("*.json")} == expected_files
    assert (tmp_path / "bridge.v1.schema.json").exists()

    def assert_closed_objects(value: object) -> None:
        if isinstance(value, dict):
            if value.get("type") == "object":
                open_maps = {
                    "Model Settings": True,
                    "Capabilities": {"type": "boolean"},
                    "App Server Info": True,
                }
                expected = open_maps.get(value.get("title"), False)
                assert value.get("additionalProperties") == expected
            for child in value.values():
                assert_closed_objects(child)
        elif isinstance(value, list):
            for child in value:
                assert_closed_objects(child)

    for filename in report.schemas:
        assert (checked_in / filename).read_bytes() == (tmp_path / filename).read_bytes()
        generated = json.loads((tmp_path / filename).read_text(encoding="utf-8"))
        assert generated["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        if filename == "bridge.v1.schema.json":
            assert generated["$comment"] == (
                "Private JSONL bridge contract. Frames are limited to 1 MiB by the transport."
            )
        else:
            assert generated["$comment"] == (
                f"Parser enforces a {MAX_CONTRACT_BYTES}-byte UTF-8 JSON payload limit."
            )
        if generated.get("type") == "object":
            assert generated["additionalProperties"] is False
        assert_closed_objects(generated)

    for filename in ("task-capsule.v1.schema.json", "conclusion-capsule.v1.schema.json"):
        schema = schemas[filename]
        assert "schema_version" in schema["required"]
        assert schema["properties"]["schema_version"]["const"] == "1"

    proposal_definitions = schemas["hekate-proposal.v1.schema.json"]["$defs"]
    for definition in proposal_definitions.values():
        if "action" in definition.get("properties", {}):
            assert "schema_version" in definition["required"]
            assert definition["properties"]["schema_version"]["const"] == "1"

    position_schema = schemas["position-commit.v1.schema.json"]
    assert position_schema["properties"]["schema_version"]["const"] == "1"
    assert "schema_version" not in position_schema["required"]
