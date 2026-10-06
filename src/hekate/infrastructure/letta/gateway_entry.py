from __future__ import annotations

import os
from pathlib import Path

from hekate.infrastructure.letta.provider_gateway import ProviderGatewayProfile, create_provider_gateway
from hekate.infrastructure.letta.qwen_ollama import (
    qwen35_native_json_schema_test_execution_profile,
)
from hekate.infrastructure.letta.qwen_local_profile import qwen35_native_json_schema_local_execution_profile
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
