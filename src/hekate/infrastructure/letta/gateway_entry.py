from __future__ import annotations

import os
from pathlib import Path

from hekate.infrastructure.letta.provider_gateway import ProviderGatewayProfile, create_provider_gateway
from hekate.infrastructure.letta.qwen_ollama import (
    qwen35_native_json_schema_test_execution_profile,
)
from hekate.infrastructure.letta.reviewed_qwen_profile import qwen35_reviewed_native_json_schema_test_execution_profile
from hekate.infrastructure.letta.qwen_local_profile import (
    qwen35_native_json_schema_local_execution_profile,
    qwen35_reviewed_native_json_schema_local_execution_profile,
)
from hekate.infrastructure.letta.token_accounting import test_profile_and_price_for_config
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.settings import Settings, configured_task_execution, validate_local_settings, validate_settings


def gateway_profile(settings: Settings) -> ProviderGatewayProfile:
    # The private gateway must be startable before Letta and the bridge. This is
    # the deliberate bootstrap order: database + fixed profile, then gateway,
    # then App Server, then worker.
    if settings.runtime_mode == "local":
        validate_local_settings(settings)
    else:
        validate_settings(settings)
    profile_config = settings.models.get("hekate")
    if not isinstance(profile_config, dict):
        raise ValueError("HEKATE provider profile is missing")
    profile_kind = profile_config.get("execution_profile")
    config = configured_task_execution(settings)
    if profile_kind == "qwen35_native_json_schema_test_v2":
        profile, prices = (
            qwen35_native_json_schema_local_execution_profile()
            if settings.runtime_mode == "local"
            else qwen35_native_json_schema_test_execution_profile()
        )
    elif profile_kind == "qwen35_reviewed_native_json_schema_v1":
        profile, prices = (
            qwen35_reviewed_native_json_schema_local_execution_profile()
            if settings.runtime_mode == "local"
            else qwen35_reviewed_native_json_schema_test_execution_profile()
        )
    elif settings.runtime_mode == "test":
        profile, prices = test_profile_and_price_for_config(config)
    else:
        raise ValueError("local gateway accepts only the fixed Qwen candidate profile")
    if profile.content_digest != config.profile_digest:
        raise ValueError("configured Task and provider gateway profile digest differ")
    local = settings.local
    gateway = local.get("gateway")
    if not isinstance(gateway, dict):
        raise ValueError("local.yaml must define the provider gateway")
    execution_mode = "local_candidate" if settings.runtime_mode == "local" else "synthetic_test"
    one_shot_attempt_ledger = None
    ledger_value = os.environ.get("HEKATE_LOCAL_GENERATION_LEDGER")
    if ledger_value:
        if execution_mode != "local_candidate":
            raise ValueError("HEKATE_LOCAL_GENERATION_LEDGER is only supported for the local candidate")
        paths = settings.local.get("paths")
        state_value = paths.get("state_dir") if isinstance(paths, dict) else None
        if not isinstance(state_value, str) or not state_value:
            raise ValueError("local.yaml must define paths.state_dir for the durable generation gate")
        state_path = Path(state_value).expanduser()
        if not state_path.is_absolute():
            state_path = settings.config_dir / state_path
        state_path = state_path.resolve()
        one_shot_attempt_ledger = Path(ledger_value).expanduser().resolve()
        if not one_shot_attempt_ledger.is_relative_to(state_path) or one_shot_attempt_ledger == state_path:
            raise ValueError("local generation ledger must be stored under the configured state directory")
        if one_shot_attempt_ledger.is_symlink():
            raise ValueError("local generation ledger cannot be a symlink")
    generation_allowance = None
    request_observation_file = None
    personal_local = settings.local.get("workflow_mode") == "personal_local_v1"
    observation_value = os.environ.get("HEKATE_PROVIDER_REQUEST_OBSERVATION_FILE")
    if profile_kind == "qwen35_reviewed_native_json_schema_v1":
        if ledger_value and not personal_local:
            raise ValueError("reviewed Qwen mode uses its isolated four-generation allowance, not the Phase 6E one-shot ledger")
        paths = settings.local.get("paths")
        state_value = paths.get("state_dir") if isinstance(paths, dict) else None
        allowance_value = paths.get("generation_allowance") if isinstance(paths, dict) else None
        if not isinstance(state_value, str) or not state_value:
            raise ValueError("reviewed local settings require paths.state_dir")
        state_path = Path(state_value).expanduser()
        if not state_path.is_absolute():
            state_path = settings.config_dir / state_path
        state_path = state_path.resolve()
        if personal_local:
            if allowance_value is not None or ledger_value:
                raise ValueError("personal local mode cannot use a verification allowance")
        else:
            if not isinstance(allowance_value, str) or not allowance_value:
                raise ValueError("reviewed local settings require paths.generation_allowance")
            generation_allowance = Path(allowance_value).expanduser()
            if not generation_allowance.is_absolute():
                generation_allowance = settings.config_dir / generation_allowance
            if generation_allowance.is_symlink() or not generation_allowance.parent.resolve().is_relative_to(state_path):
                raise ValueError("reviewed generation allowance must be a regular private-state file")
            generation_allowance = generation_allowance.resolve()
            if (
                generation_allowance == state_path
                or not generation_allowance.is_relative_to(state_path)
                or generation_allowance.is_symlink()
            ):
                raise ValueError("reviewed generation allowance must be a regular private-state file")
        if observation_value:
            request_observation_file = Path(observation_value).expanduser()
            if not request_observation_file.is_absolute():
                request_observation_file = settings.config_dir / request_observation_file
            if request_observation_file.is_symlink() or not request_observation_file.parent.resolve().is_relative_to(state_path):
                raise ValueError("reviewed provider request capture must stay under the isolated local state directory")
            request_observation_file = request_observation_file.resolve()
            if (
                request_observation_file == state_path
                or not request_observation_file.is_relative_to(state_path)
                or request_observation_file.is_symlink()
            ):
                raise ValueError("reviewed provider request capture must stay under the isolated local state directory")
    elif observation_value:
        raise ValueError("provider request capture is only supported by the reviewed Qwen profile")
    elif profile_kind != "qwen35_reviewed_native_json_schema_v1" and settings.runtime_mode == "local":
        local_paths = settings.local.get("paths")
        if isinstance(local_paths, dict) and local_paths.get("generation_allowance") is not None:
            raise ValueError("simple local Qwen mode cannot inherit a reviewed generation allowance")
    return ProviderGatewayProfile(
        profile_id=profile.profile_id,
        price_table=prices,
        upstream_base_url=str(gateway.get("upstream_base_url", "")),
        upstream_api_key=str(gateway.get("upstream_api_key", "")),
        max_input_tokens=config.max_input_tokens,
        max_output_tokens=config.max_output_tokens,
        test_only=True,
        execution_profile=profile,
        execution_mode=execution_mode,
        one_shot_attempt_ledger=one_shot_attempt_ledger,
        generation_allowance=generation_allowance,
        request_observation_file=request_observation_file,
        personal_local=personal_local,
    )


async def serve_gateway(settings: Settings) -> None:
    import uvicorn

    local_gateway = settings.local.get("gateway")
    if not isinstance(local_gateway, dict):
        raise ValueError("local.yaml must define the provider gateway")
    host = str(local_gateway.get("host", ""))
    port = local_gateway.get("port")
    if host != "127.0.0.1" or type(port) is not int or not 1 <= port <= 65_535:
        raise ValueError("provider gateway must bind to loopback and a valid port")
    token = os.environ.get("HEKATE_PROVIDER_GATEWAY_TOKEN", "")
    if len(token) < 32:
        raise ValueError("HEKATE_PROVIDER_GATEWAY_TOKEN must contain at least 32 characters")
    if not settings.database_url.startswith("postgresql+psycopg://"):
        raise ValueError("HEKATE_DATABASE_URL must be configured for PostgreSQL")
    profile = gateway_profile(settings)
    engine = create_engine(settings.database_url)
    try:
        app = create_provider_gateway(
            create_uow_factory(engine), profile, token, allow_test_profile=True,
        )
        server = uvicorn.Server(uvicorn.Config(
            app, host=host, port=port, log_level="info", access_log=False,
        ))
        await server.serve()
    finally:
        await engine.dispose()
