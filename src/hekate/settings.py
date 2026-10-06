from __future__ import annotations

import shutil
import subprocess
from dataclasses import replace
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit

import yaml

from hekate.domain.models import TaskExecutionConfig
from hekate.domain.types import ActorContext, PrincipalId, ScopeId
from hekate.infrastructure.letta.token_accounting import test_execution_profile


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    node_bin: str
    bridge_entry: Path
    letta_url: str
    letta_token: str | None
    worker_id: str
    runtime_mode: str
    config_dir: Path
    policy: Mapping[str, object]
    models: Mapping[str, object]
    pricing: Mapping[str, object]
    local: Mapping[str, object] = field(default_factory=dict)
    archive_dir: Path = Path(".hekate-archive")
    memory_projection_enabled: bool = False
    project_dir: Path = Path.cwd()
    letta_turn_timeout_ms: int = 240_000


@dataclass(frozen=True, slots=True)
class DeliberationConfig:
    enabled: bool
    max_critic_agents: int = 1
    max_reviews: int = 2
    max_hekate_continuations: int = 1
    max_syntheses: int = 2


def configured_deliberation(settings: Settings) -> DeliberationConfig:
    value = settings.policy.get("deliberation")
    if not isinstance(value, dict):
        return DeliberationConfig(enabled=False)
    enabled = value.get("enabled") is True
    expected = {
        "max_critic_agents": 1, "max_review_rounds": 2,
        "max_hekate_continuations": 1, "max_syntheses_per_task": 2,
    }
    if any(type(value.get(key)) is not int or value.get(key) != limit for key, limit in expected.items()):
        raise ValueError("Phase 5B deliberation limits must be explicitly fixed at 1/2/1/2")
    return DeliberationConfig(enabled=enabled)


def _yaml(path: Path) -> Mapping[str, object]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"configuration must be a YAML object: {path.name}")
    return value


def load_settings(env: Mapping[str, str], config_dir: Path) -> Settings:
    root = Path(env.get("HEKATE_PROJECT_DIR", Path(__file__).resolve().parents[2])).expanduser().resolve()
    config_dir = config_dir.expanduser().resolve()
    local = _yaml(config_dir / "local.yaml") if (config_dir / "local.yaml").is_file() else {}
    projection_value = env.get("HEKATE_MEMORY_PROJECTION_ENABLED", "false").strip().lower()
    if projection_value not in {"true", "false", "1", "0"}:
        raise ValueError("HEKATE_MEMORY_PROJECTION_ENABLED must be true/false or 1/0")
    return Settings(
        database_url=env.get("HEKATE_DATABASE_URL", ""),
        node_bin=env.get("HEKATE_NODE_BIN", "node"),
        bridge_entry=Path(env.get("HEKATE_BRIDGE_ENTRY", root / "bridge/letta/dist/main.js")),
        letta_url=env.get("HEKATE_LETTA_URL", ""),
        letta_token=env.get("HEKATE_LETTA_TOKEN") or None,
        worker_id=env.get("HEKATE_WORKER_ID", str((local.get("runtime") or {}).get("worker_id", ""))
                         if isinstance(local.get("runtime"), dict) else ""),
        runtime_mode=env.get("HEKATE_RUNTIME_MODE", "production"),
        config_dir=config_dir,
        policy=_yaml(config_dir / "policy.yaml"),
        models=_yaml(config_dir / "models.yaml"),
        pricing=_yaml(config_dir / "pricing.yaml"),
        local=local,
        archive_dir=Path(env.get(
            "HEKATE_ARCHIVE_DIR",
            config_dir / str((local.get("paths") or {}).get("archive_dir", "state/archive"))
            if isinstance(local.get("paths"), dict) else root / ".hekate-archive",
        )).expanduser().resolve(),
        memory_projection_enabled=projection_value in {"true", "1"},
        project_dir=root,
        letta_turn_timeout_ms=(local.get("runtime", {}).get("letta_turn_timeout_ms", 240_000)
                               if isinstance(local.get("runtime"), dict) else 240_000),
    )


def configured_task_execution(settings: Settings) -> TaskExecutionConfig:
    limits = settings.policy.get("limits")
    profile = settings.models.get("hekate")
    if not isinstance(limits, dict) or not isinstance(profile, dict):
        raise ValueError("explicit task limits and HEKATE model profile are required")
    letta_model = profile.get("model")
    model = profile.get("provider_model")
    profile_id = profile.get("profile_id")
    model_revision = profile.get("model_revision")
    context_window_tokens = profile.get("context_window_tokens")
    agent_system_prompt = profile.get("agent_system_prompt")
    letta_context_estimator_tokens: int | None = None
    sdk_output_format = True
    if agent_system_prompt is not None and (
        not isinstance(agent_system_prompt, str) or not agent_system_prompt or len(agent_system_prompt) > 2048
    ):
        raise ValueError("agent_system_prompt must be a non-empty string up to 2048 characters")
    pricing_version = settings.pricing.get("version")
    prices = settings.pricing.get("prices")
    price = prices.get(model) if isinstance(prices, dict) and isinstance(model, str) else None
    if not isinstance(price, dict):
        raise ValueError("explicit HEKATE pricing is required")

    def decimal_value(value: object, name: str, *, allow_zero: bool = False) -> Decimal:
        if not isinstance(value, (str, int, Decimal)) or isinstance(value, bool):
            raise ValueError(f"{name} must be configured as a decimal string")
        try:
            parsed = Decimal(str(value))
        except InvalidOperation as error:
            raise ValueError(f"{name} is not a valid Decimal") from error
        if not parsed.is_finite() or parsed < 0 or (not allow_zero and parsed == 0):
            raise ValueError(f"{name} must be finite and {'nonnegative' if allow_zero else 'positive'}")
        return parsed

    def integer_value(value: object, name: str) -> int:
        if type(value) is not int or value < 1:
            raise ValueError(f"{name} must be an explicitly configured positive integer")
        return value

    def nonnegative_integer(value: object, name: str) -> int:
        if type(value) is not int or value < 0:
            raise ValueError(f"{name} must be an explicitly configured nonnegative integer")
        return value

    if not all(isinstance(value, str) and value for value in (letta_model, model, profile_id, pricing_version, model_revision)):
        raise ValueError("fixed Letta model, provider model, model revision, profile_id, and pricing version are required")
    if settings.runtime_mode not in {"test", "local"}:
        raise ValueError("production provider profiles remain blocked pending model, tokenizer, renderer, and pricing evidence")
    context_window = integer_value(context_window_tokens, "context_window_tokens")
    max_input_tokens = integer_value(profile.get("max_input_tokens"), "max_input_tokens")
    max_output_tokens = integer_value(profile.get("max_output_tokens"), "max_output_tokens")
    pricing_effective_at = settings.pricing.get("effective_at")
    if not isinstance(pricing_effective_at, str) or not pricing_effective_at:
        raise ValueError("pricing effective_at is required for the fixed test contract")
    input_price = decimal_value(price.get("input_usd_per_million"), "input_usd_per_million", allow_zero=True)
    output_price = decimal_value(price.get("output_usd_per_million"), "output_usd_per_million", allow_zero=True)
    profile_kind = profile.get("execution_profile")
    if settings.runtime_mode == "local" and profile_kind != "qwen35_native_json_schema_test_v2":
        raise ValueError("local mode only accepts the frozen native-schema Qwen candidate profile")
    if profile_kind is None:
        if agent_system_prompt is not None:
            raise ValueError("custom agent_system_prompt is supported only by the frozen Qwen test profile")
        execution_profile, _ = test_execution_profile(
            profile_id=profile_id, model=model, model_revision=model_revision,
            context_window_tokens=context_window, max_input_tokens=max_input_tokens,
            max_output_tokens=max_output_tokens, pricing_version=pricing_version,
            input_usd_per_million=input_price, output_usd_per_million=output_price,
            pricing_effective_at=pricing_effective_at,
        )
    elif profile_kind in {"qwen35_test_v1", "qwen35_native_json_schema_test_v2"}:
        from hekate.infrastructure.letta.qwen_ollama import (
            load_qwen_candidate_profile, qwen35_native_json_schema_test_execution_profile,
            qwen35_test_execution_profile,
        )

        candidate = load_qwen_candidate_profile()
        execution_profile, qwen_prices = (
            qwen35_native_json_schema_test_execution_profile(candidate)
            if profile_kind == "qwen35_native_json_schema_test_v2"
            else qwen35_test_execution_profile(candidate)
        )
        if (
            profile_id != execution_profile.profile_id
            or model != execution_profile.model
            or model_revision != execution_profile.model_revision
            or context_window != execution_profile.context_window_tokens
            or max_input_tokens != execution_profile.max_input_tokens
            or max_output_tokens != execution_profile.max_output_tokens
            or pricing_version != qwen_prices.version
            or input_price != qwen_prices.input_usd_per_million
            or output_price != qwen_prices.output_usd_per_million
            or pricing_effective_at != qwen_prices.effective_at
            or letta_model != f"openai-compatible/{model}"
            or agent_system_prompt != candidate.agent_system_prompt
            or profile.get("letta_context_estimator_tokens") != candidate.letta_context_estimator_tokens
        ):
            raise ValueError("Qwen test settings differ from the frozen offline candidate contract")
        letta_context_estimator_tokens = candidate.letta_context_estimator_tokens
        sdk_output_format = candidate.sdk_output_format
    else:
        raise ValueError("unknown tokenizer profile; only the pinned fake-chat and Qwen test contracts are supported")
    return TaskExecutionConfig(
        task_budget_usd=decimal_value(limits.get("task_budget_usd"), "task_budget_usd"),
        system_daily_budget_usd=decimal_value(limits.get("system_daily_budget_usd"), "system_daily_budget_usd"),
        deadline_seconds=integer_value(limits.get("task_deadline_seconds"), "task_deadline_seconds"),
        profile_id=profile_id,
        letta_model=letta_model,
        model=model,
        pricing_version=pricing_version,
        input_usd_per_million=input_price,
        output_usd_per_million=output_price,
        max_input_tokens=max_input_tokens,
        max_output_tokens=max_output_tokens,
        max_compaction_calls=nonnegative_integer(profile.get("max_compaction_calls"), "max_compaction_calls"),
        context_window_tokens=context_window,
        model_revision=model_revision,
        profile_digest=execution_profile.content_digest,
        pricing_effective_at=pricing_effective_at,
        agent_system_prompt=agent_system_prompt,
        letta_context_estimator_tokens=letta_context_estimator_tokens,
        sdk_output_format=sdk_output_format,
    )


def configured_critic_execution(settings: Settings) -> TaskExecutionConfig | None:
    policy = settings.policy.get("critic")
    if not isinstance(policy, dict) or policy.get("enabled") is not True:
        return None
    deliberation = settings.policy.get("deliberation")
    phase5b = isinstance(deliberation, dict) and deliberation.get("enabled") is True
    required_caps = {
        "max_agents_per_task": 1,
        "max_review_rounds": 2 if phase5b else 1,
        "max_syntheses_per_task": 2 if phase5b else 1,
    }
    if any(policy.get(key) != value or type(policy.get(key)) is not int for key, value in required_caps.items()):
        raise ValueError("Critic limits must match the explicitly enabled Phase 5A/5B bounds")
    profile = settings.models.get("critic")
    if not isinstance(profile, dict):
        raise ValueError("Critic is enabled without a fixed Critic model profile")
    profile_settings = replace(settings, models={"hekate": profile})
    return configured_task_execution(profile_settings)


_EXECUTION_CONFIG_FIELDS = frozenset({
    "task_budget_usd", "system_daily_budget_usd", "deadline_seconds", "profile_id",
    "letta_model", "model", "pricing_version", "input_usd_per_million",
    "output_usd_per_million", "max_input_tokens", "max_output_tokens",
    "max_compaction_calls", "context_window_tokens", "model_revision", "profile_digest", "pricing_effective_at",
})


def execution_config_snapshot(config: TaskExecutionConfig) -> dict[str, object]:
    """Persist the trusted, bounded profile that authorized a durable workflow step."""
    value = {
        "task_budget_usd": str(config.task_budget_usd),
        "system_daily_budget_usd": str(config.system_daily_budget_usd),
        "deadline_seconds": config.deadline_seconds,
        "profile_id": config.profile_id,
        "letta_model": config.letta_model,
        "model": config.model,
        "pricing_version": config.pricing_version,
        "input_usd_per_million": str(config.input_usd_per_million),
        "output_usd_per_million": str(config.output_usd_per_million),
        "max_input_tokens": config.max_input_tokens,
        "max_output_tokens": config.max_output_tokens,
        "max_compaction_calls": config.max_compaction_calls,
        "context_window_tokens": config.context_window_tokens,
        "model_revision": config.model_revision,
        "profile_digest": config.profile_digest,
        "pricing_effective_at": config.pricing_effective_at,
    }
    if config.agent_system_prompt is not None:
        value["agent_system_prompt"] = config.agent_system_prompt
    if config.letta_context_estimator_tokens is not None:
        value["letta_context_estimator_tokens"] = config.letta_context_estimator_tokens
    if not config.sdk_output_format:
        value["sdk_output_format"] = False
    return value


def restore_execution_config(value: Mapping[str, object]) -> TaskExecutionConfig:
    """Rebuild only a complete persisted server profile; never read model routing from output."""
    optional_fields = {"agent_system_prompt", "letta_context_estimator_tokens", "sdk_output_format"}
    persisted_fields = frozenset(value)
    if not _EXECUTION_CONFIG_FIELDS <= persisted_fields or not (persisted_fields - _EXECUTION_CONFIG_FIELDS) <= optional_fields:
        raise ValueError("persisted workflow execution profile is incomplete or contains unknown fields")
    agent_system_prompt = value.get("agent_system_prompt")
    if agent_system_prompt is not None and (
        not isinstance(agent_system_prompt, str) or not agent_system_prompt or len(agent_system_prompt) > 2048
    ):
        raise ValueError("persisted workflow execution profile has an invalid agent system prompt")
    decimal_names = {
        "task_budget_usd", "system_daily_budget_usd",
        "input_usd_per_million", "output_usd_per_million",
    }
    string_names = {"profile_id", "letta_model", "model", "pricing_version", "model_revision", "profile_digest", "pricing_effective_at"}
    integer_names = {"deadline_seconds", "max_input_tokens", "max_output_tokens", "max_compaction_calls", "context_window_tokens"}
    if "letta_context_estimator_tokens" in value:
        integer_names.add("letta_context_estimator_tokens")
    if "sdk_output_format" in value and type(value["sdk_output_format"]) is not bool:
        raise ValueError("persisted workflow execution profile has an invalid SDK output-format setting")
    if any(not isinstance(value.get(name), str) or not value[name] for name in string_names):
        raise ValueError("persisted workflow execution profile has an invalid identity")
    if any(type(value.get(name)) is not int for name in integer_names):
        raise ValueError("persisted workflow execution profile has an invalid limit")
    if any(not isinstance(value.get(name), str) for name in decimal_names):
        raise ValueError("persisted workflow execution profile has an invalid price")
    try:
        decimals = {name: Decimal(value[name]) for name in decimal_names}
    except InvalidOperation as error:
        raise ValueError("persisted workflow execution profile has an invalid price") from error
    if any(not number.is_finite() or number < 0 for number in decimals.values()):
        raise ValueError("persisted workflow execution profile has an invalid price")
    limits = [value[name] for name in integer_names]
    if any(number < 0 for number in limits) or value["deadline_seconds"] < 1 \
            or value["max_input_tokens"] < 1 or value["max_output_tokens"] < 1:
        raise ValueError("persisted workflow execution profile has invalid limits")
    if "letta_context_estimator_tokens" in value and value["letta_context_estimator_tokens"] < 1:
        raise ValueError("persisted workflow execution profile has invalid App Server context estimator limit")
    return TaskExecutionConfig(
        **decimals,
        deadline_seconds=value["deadline_seconds"],
        profile_id=value["profile_id"], letta_model=value["letta_model"],
        model=value["model"], pricing_version=value["pricing_version"],
        max_input_tokens=value["max_input_tokens"],
        max_output_tokens=value["max_output_tokens"],
        max_compaction_calls=value["max_compaction_calls"],
        context_window_tokens=value["context_window_tokens"],
        model_revision=value["model_revision"],
        profile_digest=value["profile_digest"],
        pricing_effective_at=value["pricing_effective_at"],
        agent_system_prompt=agent_system_prompt,
        letta_context_estimator_tokens=value.get("letta_context_estimator_tokens"),
        sdk_output_format=value.get("sdk_output_format", True),
    )


def configured_local_actor(settings: Settings) -> ActorContext:
    identity = settings.local.get("identity")
    if not isinstance(identity, dict):
        raise ValueError("config/local.yaml must define a trusted identity")
    principal = identity.get("principal_id")
    scope = identity.get("scope_id")
    policy_version = identity.get("policy_version")
    authz_epoch = identity.get("authz_epoch")
    if not all(isinstance(value, str) and value for value in (principal, scope, policy_version)):
        raise ValueError("local identity requires principal_id, scope_id, and policy_version")
    if type(authz_epoch) is not int or authz_epoch < 0:
        raise ValueError("local identity requires a nonnegative authz_epoch")
    return ActorContext(
        principal_id=PrincipalId(principal),
        scope=ScopeId(scope),
        authenticated_agent_registry_id=None,
        task_id=None,
        attempt_id=None,
        input_revision=None,
        policy_version=policy_version,
        authz_epoch=authz_epoch,
        fence=0,
    )


def validate_settings(settings: Settings) -> None:
    if not settings.database_url.startswith("postgresql+psycopg://"):
        raise ValueError("HEKATE requires PostgreSQL via psycopg")
    if settings.runtime_mode not in {"production", "test", "local"}:
        raise ValueError("HEKATE_RUNTIME_MODE must be production, test, or explicitly enabled local")
    if not settings.worker_id or len(settings.worker_id) > 128:
        raise ValueError("HEKATE_WORKER_ID must be a stable unique worker identity")
    if urlsplit(settings.letta_url).scheme not in {"ws", "wss"} or not urlsplit(settings.letta_url).hostname:
        raise ValueError("HEKATE_LETTA_URL must be a configured WebSocket URL")
    node = shutil.which(settings.node_bin) or settings.node_bin
    if not settings.bridge_entry.is_file():
        raise ValueError("built private JSONL bridge entry is missing")
    version = subprocess.run([node, "--version"], capture_output=True, text=True, check=True, timeout=10).stdout.strip()
    if version != "v22.19.0":
        raise ValueError("HEKATE bridge requires the pinned Node.js 22.19.0 runtime")
    if settings.runtime_mode == "local":
        validate_local_settings(settings)
    if settings.runtime_mode == "production":
        # There is no production evidence bundle or operator approval record in this repository.
        # YAML booleans and a nonempty price table cannot establish an exact provider request contract.
        raise ValueError("no approved model, tokenizer, renderer, and pricing evidence; production dispatch stays closed")


def validate_local_settings(settings: Settings) -> None:
    if settings.runtime_mode != "local":
        raise ValueError("local setup requires HEKATE_RUNTIME_MODE=local")
    value = settings.local
    if value.get("schema_version") != "1" or value.get("enabled") is not True:
        raise ValueError("local.yaml must explicitly enable schema_version 1 local execution")
    identity = value.get("identity")
    runtime = value.get("runtime")
    gateway = value.get("gateway")
    if not all(isinstance(item, dict) for item in (identity, runtime, gateway)):
        raise ValueError("local.yaml requires identity, runtime, and gateway objects")
    if runtime.get("execution_mode") != "local_candidate":
        raise ValueError("local execution must explicitly select local_candidate mode")
    if not isinstance(runtime.get("worker_id"), str) or not runtime["worker_id"]:
        raise ValueError("local runtime requires an explicit stable worker_id")
    if gateway.get("host") != "127.0.0.1" or type(gateway.get("port")) is not int or not 1 <= gateway["port"] <= 65_535:
        raise ValueError("local provider gateway must bind to 127.0.0.1 and a valid port")
    upstream = gateway.get("upstream_base_url")
    if not isinstance(upstream, str) or upstream.rstrip("/") != "http://127.0.0.1:19191":
        raise ValueError("local Qwen upstream must use the fixed loopback tunnel at 127.0.0.1:19191")
    if not isinstance(gateway.get("upstream_api_key"), str) or not gateway["upstream_api_key"]:
        raise ValueError("local gateway requires an explicit upstream credential field")
    turn_timeout = runtime.get("letta_turn_timeout_ms", 240_000)
    if type(turn_timeout) is not int or not 1 <= turn_timeout <= 240_000:
        raise ValueError("local Letta turn timeout must be between 1 and 240000 ms")
    paths = value.get("paths", {})
    if not isinstance(paths, dict) or not isinstance(paths.get("state_dir"), str) or not paths["state_dir"]:
        raise ValueError("local.yaml must define a separate local paths.state_dir")
    if not settings.project_dir.is_dir():
        raise ValueError("HEKATE_PROJECT_DIR must point to the checkout containing the pinned bridge and assets")
    if not settings.database_url.startswith("postgresql+psycopg://"):
        raise ValueError("local execution requires PostgreSQL via psycopg")
    letta = urlsplit(settings.letta_url)
    if letta.scheme not in {"ws", "wss"} or not letta.hostname:
        raise ValueError("HEKATE_LETTA_URL must be a valid App Server WebSocket URL")
    if not settings.worker_id or len(settings.worker_id) > 128:
        raise ValueError("HEKATE_WORKER_ID must be a stable unique worker identity")
    _ = configured_local_actor(settings)
    limits = settings.policy.get("limits")
    if not isinstance(limits, dict) or limits.get("task_deadline_seconds") != 900:
        raise ValueError("local Qwen run requires the explicit 900-second Task deadline")
    if settings.policy.get("tool_grants") != [] or settings.policy.get("allowed_actions") != [
        "answer", "request_information", "abstain", "commit",
    ]:
        raise ValueError("local policy must disable tools and keep the bounded HEKATE action set")
    critic = settings.policy.get("critic")
    deliberation = settings.policy.get("deliberation")
    if not isinstance(critic, dict) or critic.get("enabled") is not False or not isinstance(deliberation, dict) or deliberation.get("enabled") is not False:
        raise ValueError("local execution requires Critic and automatic continuation to remain disabled")
    config = configured_task_execution(settings)
    if (
        config.profile_id != "local-qwen35-native-json-schema-test-v2"
        or config.model != "orcarouter/Qwen3.8-27B-Uncensored:iq4_xs"
        or config.context_window_tokens != 8_192
        or config.max_input_tokens != 6_144
        or config.max_output_tokens != 2_048
        or config.max_compaction_calls != 0
    ):
        raise ValueError("local Qwen limits must remain fixed at 8192/6144/2048 with compaction disabled")
