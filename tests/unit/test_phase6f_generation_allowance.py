from __future__ import annotations

import json
import os
import hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from hekate.infrastructure.letta.provider_gateway import _append_reviewed_request_observation, _claim_reviewed_generation
from hekate.infrastructure.letta.reviewed_qwen_profile import qwen35_reviewed_native_json_schema_test_execution_profile


def _stage(stage: str, scope: str) -> tuple[dict[str, str], dict[str, object]]:
    profile, _ = qwen35_reviewed_native_json_schema_test_execution_profile()
    values = {
        "task_a_planning": ("task-a", "request-a", "attempt-a-planning", "op-a-planning", "hekate.turn", "planning", "hekate_turn_output_v1", "hekate", "persistent"),
        "task_a_critic_review": ("task-a", "request-a", "attempt-a-critic", "op-a-critic", "critic.review", "critic_review", "critic_turn_output_v1", "critic", "ephemeral"),
        "task_a_synthesis": ("task-a", "request-a", "attempt-a-synthesis", "op-a-synthesis", "hekate.synthesis", "synthesis", "hekate_turn_output_v1", "hekate", "persistent"),
        "task_b_answer": ("task-b", "request-b", "attempt-b-planning", "op-b-planning", "hekate.turn", "planning", "hekate_turn_output_v1", "hekate", "persistent"),
    }
    task, request_key, attempt, operation, operation_kind, attempt_kind, contract, registry_kind, persistence = values[stage]
    claims = {"task_id": task, "operation_id": operation, "accounting_call_id": f"call-{stage}", "model": profile.model}
    trusted = {
        "scope_id": scope,
        "task_id": task,
        "task_request_key": request_key,
        "attempt_id": attempt,
        "operation_id": operation,
        "operation_kind": operation_kind,
        "attempt_kind": attempt_kind,
        "registry_id": f"registry-{stage}",
        "registry_kind": registry_kind,
        "registry_persistence": persistence,
        "output_contract": contract,
    }
    return claims, trusted


def test_reviewed_allowance_survives_restart_and_serializes_concurrent_physical_calls(tmp_path: Path) -> None:
    profile, _ = qwen35_reviewed_native_json_schema_test_execution_profile()
    state_dir = tmp_path / "private-state"
    state_dir.mkdir(mode=0o700)
    os.chmod(state_dir, 0o700)
    path = state_dir / "allowance.json"
    path.write_text(json.dumps({
        "schema_version": "1",
        "run_id": "phase6f-test-run",
        "scope_id": "scope-phase6f",
        "profile_id": profile.profile_id,
        "profile_digest": profile.content_digest,
        "max_generations": 4,
        "task_request_keys": {"task_a": "request-a", "task_b": "request-b"},
        "claims": {},
    }), encoding="utf-8")
    os.chmod(path, 0o600)
    stages = ["task_a_planning", "task_a_critic_review", "task_a_synthesis", "task_b_answer"]

    def claim(stage: str) -> str:
        claims, trusted = _stage(stage, "scope-phase6f")
        _claim_reviewed_generation(path, claims, trusted, f"request-digest-{stage}", profile.content_digest)
        return stage

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert set(pool.map(claim, stages)) == set(stages)
    first_state = json.loads(path.read_text(encoding="utf-8"))
    assert set(first_state["claims"]) == set(stages)
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.with_name("allowance.json.lock").stat().st_mode & 0o777 == 0o600

    # A gateway/process restart sees the durable stage claim and cannot spend it again.
    with pytest.raises(FileExistsError):
        claim("task_a_planning")
    assert json.loads(path.read_text(encoding="utf-8")) == first_state

    claims, trusted = _stage("task_b_answer", "scope-from-another-run")
    with pytest.raises(ValueError, match="identity or limits"):
        _claim_reviewed_generation(path, claims, trusted, "unauthorized-digest", profile.content_digest)
    assert json.loads(path.read_text(encoding="utf-8")) == first_state


def test_reviewed_provider_capture_records_only_the_measured_body_in_private_state(tmp_path: Path) -> None:
    state_dir = tmp_path / "private-capture"
    state_dir.mkdir(mode=0o700)
    os.chmod(state_dir, 0o700)
    path = state_dir / "requests.jsonl"
    path.touch(mode=0o600)
    os.chmod(path, 0o600)
    body = {"model": "local-qwen", "messages": [{"role": "user", "content": "bounded prompt"}], "temperature": 0}
    raw = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    _append_reviewed_request_observation(
        path,
        {"operation_id": "operation-1", "accounting_call_id": "call-1"},
        {
            "scope_id": "scope-1", "task_id": "task-1", "task_request_key": "request-1",
            "attempt_id": "attempt-1", "registry_id": "registry-1", "registry_kind": "hekate",
            "registry_persistence": "persistent", "operation_kind": "hekate.turn",
            "attempt_kind": "planning", "output_contract": "hekate_turn_output_v1",
        },
        raw, hashlib.sha256(raw).hexdigest(), "f" * 64,
    )
    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["request_body"] == body
    assert record["request_digest"] == hashlib.sha256(raw).hexdigest()
    assert "authorization" not in record and "gateway_token" not in record
    assert path.stat().st_mode & 0o777 == 0o600
