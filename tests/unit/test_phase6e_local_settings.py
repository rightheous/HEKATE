from __future__ import annotations

from pathlib import Path

import pytest

from hekate.infrastructure.letta.gateway_entry import gateway_profile
from hekate.infrastructure.letta.provider_gateway import _claim_local_attempt
from hekate.settings import load_settings, validate_local_settings


ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "config/local.example"


def _settings(config_dir: Path = EXAMPLE):
    return load_settings({
        "HEKATE_DATABASE_URL": "postgresql+psycopg://local:local@127.0.0.1/hekate",
        "HEKATE_LETTA_URL": "ws://127.0.0.1:8283",
        "HEKATE_RUNTIME_MODE": "local",
        "HEKATE_WORKER_ID": "phase6e-unit-worker",
        "HEKATE_PROJECT_DIR": str(ROOT),
    }, config_dir)


def test_fixed_local_profile_and_external_tariff_are_distinct_from_fake() -> None:
    settings = _settings()
    validate_local_settings(settings)
    profile = gateway_profile(settings)
    assert profile.execution_mode == "local_candidate"
    assert profile.price_table.synthetic is False
    assert profile.profile_id == "local-qwen35-native-json-schema-test-v2"
    assert profile.max_input_tokens == 6_144
    assert profile.max_output_tokens == 2_048


@pytest.mark.parametrize("mutation", ["model", "external_upstream", "limits"])
def test_local_setup_rejects_profile_route_or_limit_drift(tmp_path: Path, mutation: str) -> None:
    import yaml

    target = tmp_path / "config"
    target.mkdir()
    for name in ("local.yaml", "models.yaml", "policy.yaml", "pricing.yaml"):
        (target / name).write_bytes((EXAMPLE / name).read_bytes())
    if mutation == "model":
        value = yaml.safe_load((target / "models.yaml").read_text())
        value["hekate"]["provider_model"] = "some-other-model"
        (target / "models.yaml").write_text(yaml.safe_dump(value), encoding="utf-8")
    elif mutation == "external_upstream":
        value = yaml.safe_load((target / "local.yaml").read_text())
        value["gateway"]["upstream_base_url"] = "https://example.invalid"
        (target / "local.yaml").write_text(yaml.safe_dump(value), encoding="utf-8")
    else:
        value = yaml.safe_load((target / "models.yaml").read_text())
        value["hekate"]["max_compaction_calls"] = 1
        (target / "models.yaml").write_text(yaml.safe_dump(value), encoding="utf-8")
    settings = _settings(target)
    with pytest.raises(ValueError):
        validate_local_settings(settings)


def test_local_one_shot_ledger_is_durable_and_exclusive(tmp_path: Path) -> None:
    path = tmp_path / "state" / "one-shot.json"
    claims = {
        "task_id": "task-1", "operation_id": "operation-1",
        "accounting_call_id": "call-1", "model": "qwen",
    }
    _claim_local_attempt(path, claims, "request-digest", "profile-digest")
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        _claim_local_attempt(path, claims, "request-digest", "profile-digest")
    assert '"state":"ATTEMPT_RESERVED"' in path.read_text(encoding="utf-8")
