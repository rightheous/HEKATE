from __future__ import annotations

from pathlib import Path

import pytest

from hekate.infrastructure.letta.gateway_entry import gateway_profile
from hekate.infrastructure.letta.provider_gateway import _claim_local_attempt
from hekate.settings import load_settings, validate_local_settings


ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "config/local.example"
PERSONAL_EXAMPLE = ROOT / "config/personal-local.example"


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


def test_reviewed_local_profile_is_explicit_and_keeps_role_configs_on_one_immutable_bundle() -> None:
    settings = _settings(ROOT / "config/local-reviewed.example")
    validate_local_settings(settings)
    profile = gateway_profile(settings)
    assert profile.profile_id == "local-qwen35-reviewed-native-json-schema-v1"
    assert profile.execution_mode == "local_candidate"
    assert profile.price_table.synthetic is False
    assert profile.generation_allowance == (ROOT / "config/local-reviewed.example/state/qwen-reviewed-generation-allowance.json").resolve()
    assert profile.execution_profile is not None
    from hekate.settings import configured_critic_execution, configured_task_execution

    assert configured_task_execution(settings).profile_digest == configured_critic_execution(settings).profile_digest


def test_personal_local_profile_uses_database_task_cap_without_probe_allowance() -> None:
    from hekate.settings import configured_task_execution

    settings = _settings(PERSONAL_EXAMPLE)
    validate_local_settings(settings)
    profile = gateway_profile(settings)
    assert profile.personal_local is True
    assert profile.generation_allowance is None
    assert configured_task_execution(settings).max_generations_per_task == 3


def test_disabled_deliberation_does_not_validate_unused_phase5b_caps() -> None:
    from hekate.settings import configured_deliberation

    settings = _settings(ROOT / "config/local-reviewed.example")
    configured = configured_deliberation(settings)
    assert configured.enabled is False


@pytest.mark.parametrize("mutation", ["critic_role", "critic_cap", "allowance_outside_state"])
def test_reviewed_local_setup_rejects_role_cap_or_allowance_drift(tmp_path: Path, mutation: str) -> None:
    import yaml

    source = ROOT / "config/local-reviewed.example"
    target = tmp_path / "config"
    target.mkdir()
    for name in ("local.yaml", "models.yaml", "policy.yaml", "pricing.yaml"):
        (target / name).write_bytes((source / name).read_bytes())
    if mutation == "critic_role":
        value = yaml.safe_load((target / "models.yaml").read_text())
        value["critic"]["reasoning_role"] = "hekate"
        (target / "models.yaml").write_text(yaml.safe_dump(value), encoding="utf-8")
    elif mutation == "critic_cap":
        value = yaml.safe_load((target / "policy.yaml").read_text())
        value["critic"]["max_review_rounds"] = 2
        (target / "policy.yaml").write_text(yaml.safe_dump(value), encoding="utf-8")
    else:
        value = yaml.safe_load((target / "local.yaml").read_text())
        value["paths"]["generation_allowance"] = "../outside/allowance.json"
        (target / "local.yaml").write_text(yaml.safe_dump(value), encoding="utf-8")
    with pytest.raises(ValueError):
        validate_local_settings(_settings(target))


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
