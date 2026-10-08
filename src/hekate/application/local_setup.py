from __future__ import annotations

import asyncio
from alembic import command
from alembic.config import Config
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import tempfile

from hekate.domain.models import AuthorizationSnapshot
from hekate.settings import Settings, configured_local_actor, validate_local_settings
from hekate.infrastructure.postgres.database import check_database, create_engine, create_uow_factory


def _state_dir(settings: Settings) -> Path:
    paths = settings.local.get("paths")
    if not isinstance(paths, dict) or not isinstance(paths.get("state_dir"), str):
        raise ValueError("local.yaml must define paths.state_dir")
    state = Path(paths["state_dir"]).expanduser()
    return (state if state.is_absolute() else settings.config_dir / state).resolve()


def _write_provider_auth(settings: Settings) -> Path:
    gateway = settings.local.get("gateway")
    if not isinstance(gateway, dict):
        raise ValueError("local.yaml must define the provider gateway")
    token = os.environ.get("HEKATE_PROVIDER_GATEWAY_TOKEN", "")
    if len(token) < 32:
        raise ValueError("HEKATE_PROVIDER_GATEWAY_TOKEN must contain at least 32 characters")
    state = _state_dir(settings)
    providers_dir = state / "letta" / "lc-local-backend" / "providers"
    providers_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    auth_path = providers_dir / "auth.json"
    if auth_path.is_symlink():
        raise ValueError("refusing to replace a symlinked Letta provider config")
    existing: dict[str, object] = {"version": 1, "providers": {}}
    if auth_path.exists():
        try:
            parsed = json.loads(auth_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError("existing Letta provider config is unreadable; refusing to replace it") from error
        if not isinstance(parsed, dict) or parsed.get("version") != 1 or not isinstance(parsed.get("providers"), dict):
            raise ValueError("existing Letta provider config has an unknown shape; refusing to replace it")
        existing = parsed
    providers = dict(existing["providers"])
    current = providers.get("openai-compatible")
    if current is not None and (
        not isinstance(current, dict)
        or current.get("id") != "hekate-local-gateway"
    ):
        raise ValueError("openai-compatible provider is owned by another local runtime config")
    now = datetime.now(UTC).isoformat()
    provider = {
        "id": "hekate-local-gateway",
        "name": "openai-compatible",
        "provider_type": "openai-compatible",
        "provider_category": "byok",
        "auth": {"type": "api", "key": token},
        "base_url": f"http://{gateway['host']}:{gateway['port']}/v1",
        "created_at": current.get("created_at", now) if isinstance(current, dict) else now,
        "updated_at": current.get("updated_at", now) if isinstance(current, dict) else now,
    }
    if current == provider:
        return auth_path
    provider["updated_at"] = now
    providers["openai-compatible"] = provider
    payload = json.dumps({"version": 1, "providers": providers}, separators=(",", ":"))
    descriptor, temporary_name = tempfile.mkstemp(prefix=".auth.", suffix=".tmp", dir=providers_dir)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, auth_path)
        os.chmod(auth_path, 0o600)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return auth_path


def upgrade_local_database(settings: Settings) -> None:
    config_path = settings.project_dir / "alembic.ini"
    if not config_path.is_file():
        raise ValueError("HEKATE_PROJECT_DIR does not contain alembic.ini")
    config = Config(str(config_path))
    config.set_main_option("sqlalchemy.url", settings.database_url.replace("%", "%%"))
    command.upgrade(config, "head")


async def initialize_local(settings: Settings) -> dict[str, object]:
    validate_local_settings(settings)
    if not settings.database_url.startswith("postgresql+psycopg://"):
        raise ValueError("HEKATE_DATABASE_URL must be configured for PostgreSQL")
    # Alembic's async environment owns its own asyncio.run(); do not invoke it
    # on the CLI event loop used by this coroutine.
    await asyncio.to_thread(upgrade_local_database, settings)
    actor = configured_local_actor(settings)
    engine = create_engine(settings.database_url)
    factory = create_uow_factory(engine)
    try:
        async with factory() as uow:
            created = await uow.tasks.ensure_local_scope(AuthorizationSnapshot(
                scope=actor.scope,
                principal_id=actor.principal_id,
                policy_version=actor.policy_version,
                authz_epoch=actor.authz_epoch,
            ))
            await uow.commit()
        # Do not change the runtime's provider routing until the configured
        # identity has been accepted by PostgreSQL. A revoked or mismatched
        # scope must remain revoked/mismatched after a failed init-local.
        provider_auth_path = _write_provider_auth(settings)
        health = await check_database(engine)
        if not health.available:
            raise RuntimeError("PostgreSQL became unavailable after local initialization")
        return {
            "initialized": True,
            "scope_created": created,
            "scope_id": str(actor.scope),
            "principal_id": str(actor.principal_id),
            "policy_version": actor.policy_version,
            "authz_epoch": actor.authz_epoch,
            "postgres_version": health.postgres_version,
            "migration_head": health.migration_head,
            "letta_provider_config": str(provider_auth_path),
        }
    finally:
        await engine.dispose()
