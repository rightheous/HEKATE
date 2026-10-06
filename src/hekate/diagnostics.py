from __future__ import annotations

import asyncio
import errno
import hashlib
import json
import os
import shutil
import subprocess
import urllib.error
import urllib.request
from pathlib import Path
from typing import Mapping

from sqlalchemy import text

from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.infrastructure.letta.bridge_protocol import BridgeClient
from hekate.infrastructure.letta.gateway_entry import gateway_profile
from hekate.infrastructure.letta.qwen_ollama import (
    hekate_turn_output_schema,
    load_qwen_candidate_profile,
    qwen35_native_json_schema_test_execution_profile,
)
from hekate.infrastructure.postgres.database import check_database, create_engine
from hekate.settings import (
    Settings, configured_local_actor, configured_task_execution,
    validate_local_settings, validate_settings,
)


def _read_json(url: str, *, token: str | None = None, method: str = "GET", body: object | None = None) -> object:
    payload = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = {"Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, data=payload, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.loads(response.read(1_048_577).decode("utf-8", "strict"))


def _connection_refused(error: BaseException) -> bool:
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, OSError) and current.errno == errno.ECONNREFUSED:
            return True
        reason = getattr(current, "reason", None)
        if isinstance(reason, BaseException):
            pending.append(reason)
        for chained in (current.__cause__, current.__context__):
            if chained is not None:
                pending.append(chained)
    return False


async def _authorization_status(settings: Settings) -> dict[str, object]:
    actor = configured_local_actor(settings)
    engine = create_engine(settings.database_url)
    try:
        async with engine.connect() as connection:
            row = (await connection.execute(text(
                "SELECT principal_id, policy_version, authz_epoch, active "
                "FROM authorization_scopes WHERE id=:scope"
            ), {"scope": str(actor.scope)})).mappings().one_or_none()
        if row is None:
            return {"status": "NOT_INITIALIZED", "scope_id": str(actor.scope)}
        matches = (row["principal_id"], row["policy_version"], row["authz_epoch"]) == (
            str(actor.principal_id), actor.policy_version, actor.authz_epoch,
        )
        return {
            "status": "READY" if matches and row["active"] else "REVOKED" if not row["active"] else "CONFIG_MISMATCH",
            "scope_id": str(actor.scope),
            "principal_matches": row["principal_id"] == str(actor.principal_id),
            "policy_matches": row["policy_version"] == actor.policy_version,
            "epoch_matches": row["authz_epoch"] == actor.authz_epoch,
            "active": bool(row["active"]),
        }
    finally:
        await engine.dispose()


def _ollama_status(settings: Settings) -> dict[str, object]:
    candidate = load_qwen_candidate_profile()
    base = str(settings.local.get("gateway", {}).get("upstream_base_url", "")).rstrip("/")
    try:
        version = _read_json(base + "/api/version")
        tags = _read_json(base + "/api/tags")
        runners = _read_json(base + "/api/ps")
        models = tags.get("models", []) if isinstance(tags, dict) else []
        loaded = runners.get("models", []) if isinstance(runners, dict) else []
        if not isinstance(models, list) or not isinstance(loaded, list):
            return {"status": "PROTOCOL_MISMATCH", "generation_requested": False}
        matching = [item for item in models if isinstance(item, dict) and item.get("name") == candidate.model]
        loaded_model = next((item for item in loaded if isinstance(item, dict) and item.get("name") == candidate.model), None)
        digest = matching[0].get("digest") if matching else None
        context = loaded_model.get("context_length") if loaded_model else None
        version_matches = isinstance(version, dict) and version.get("version") == candidate.ollama_version
        identity_matches = bool(
            len(matching) == 1
            and digest == candidate.model_manifest_digest
            and version_matches
        )
        if not identity_matches:
            status = "MODEL_MISSING_OR_CHANGED"
            context_status = "NOT_VERIFIED"
        elif loaded_model is None:
            status = "NOT_LOADED"
            context_status = "NOT_LOADED"
        elif type(context) is not int:
            status = "CONTEXT_UNVERIFIED"
            context_status = "UNVERIFIED"
        elif context < candidate.context_window_tokens:
            status = "CONTEXT_INSUFFICIENT"
            context_status = "INSUFFICIENT"
        else:
            status = "READY"
            context_status = "SUFFICIENT"
        return {
            "status": status,
            "identity_status": "READY" if identity_matches else "MISMATCH",
            "version": version.get("version") if isinstance(version, dict) else None,
            "expected_version": candidate.ollama_version,
            "model_present": bool(matching),
            "manifest_matches": digest == candidate.model_manifest_digest,
            "loaded": loaded_model is not None,
            "loaded_context_tokens": context,
            "loaded_context_status": context_status,
            "model_metadata_context_tokens": candidate.model_metadata_context_tokens,
            "required_context_tokens": candidate.context_window_tokens,
            "context_sufficient": context_status == "SUFFICIENT",
            "context_verified": context_status == "SUFFICIENT",
            "generation_requested": False,
        }
    except Exception as error:
        status = "NOT_RUNNING" if _connection_refused(error) else "UNAVAILABLE"
        return {"status": status, "reason": type(error).__name__, "generation_requested": False}


async def doctor(settings: Settings) -> dict[str, object]:
    checks: dict[str, object] = {}
    try:
        if settings.runtime_mode == "local":
            validate_local_settings(settings)
        else:
            validate_settings(settings)
        task_config = configured_task_execution(settings)
        profile = gateway_profile(settings)
        checks["configuration"] = {
            "status": "READY",
            "runtime_mode": settings.runtime_mode,
            "profile_id": task_config.profile_id,
            "profile_digest": task_config.profile_digest,
            "execution_mode": profile.execution_mode,
            "price_synthetic": profile.price_table.synthetic,
            "production_dispatch_approved": False,
        }
    except Exception as error:
        checks["configuration"] = {"status": "ERROR", "reason": type(error).__name__}
        return {"status": "NOT_READY", "checks": checks, "inference_requested": False, "database_modified": False}

    try:
        schema, schema_digest = hekate_turn_output_schema()
        lock_path = settings.project_dir / "integration/letta/versions.lock.json"
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
        patch_spec = lock["patches"][0]
        patch_path = settings.project_dir / patch_spec["file"]
        patch_digest = hashlib.sha256(patch_path.read_bytes()).hexdigest()
        if settings.runtime_mode == "local":
            candidate = load_qwen_candidate_profile()
            identity_ok = (
                candidate.model == profile.price_table.model
                and candidate.model_manifest_digest == task_config.model_revision
                and profile.execution_profile is not None
                and profile.execution_profile.content_digest == task_config.profile_digest
                and schema_digest in profile.execution_profile.evidence_digests
            )
            identity = {
                "candidate_profile_digest": candidate.content_digest,
                "model_manifest_digest": candidate.model_manifest_digest,
                "tokenizer_asset_sha256": candidate.tokenizer_asset_sha256,
                "renderer_sha256": candidate.renderer_sha256,
            }
        else:
            test_profile_config = settings.models.get("hekate")
            if isinstance(test_profile_config, dict) and test_profile_config.get("execution_profile") == "qwen35_native_json_schema_test_v2":
                candidate = load_qwen_candidate_profile()
                expected_test_profile, _ = qwen35_native_json_schema_test_execution_profile(candidate)
                identity_ok = bool(
                    profile.execution_profile is not None
                    and profile.execution_profile.content_digest == task_config.profile_digest
                    and profile.execution_profile.provider == "ollama-local"
                    and profile.profile_id == expected_test_profile.profile_id
                    and profile.price_table.model == candidate.model
                    and profile.price_table.synthetic
                    and task_config.model_revision == candidate.model_manifest_digest
                    and schema_digest in profile.execution_profile.evidence_digests
                )
                identity = {
                    "synthetic_qwen_fixture": True,
                    "candidate_profile_digest": candidate.content_digest,
                    "model_manifest_digest": candidate.model_manifest_digest,
                    "synthetic_tariff": profile.price_table.synthetic,
                }
            else:
                identity_ok = bool(
                    profile.execution_profile is not None
                    and profile.execution_profile.content_digest == task_config.profile_digest
                    and profile.price_table.synthetic
                    and profile.execution_profile.provider == "hekate-fake-provider"
                )
                identity = {
                    "test_profile_id": profile.profile_id,
                    "synthetic_tariff": profile.price_table.synthetic,
                }
        identity_ok = identity_ok and patch_digest == patch_spec["sha256"] and bool(schema)
        checks["profile_assets"] = {
            "status": "READY" if identity_ok else "MISMATCH",
            **identity,
            "output_schema_sha256": schema_digest,
            "runtime_patch_sha256": patch_digest,
            "runtime_patch_matches_lock": patch_digest == patch_spec["sha256"],
            "expected_app_server_image": f"{lock['app_server']['image']}@{lock['app_server']['image_digest']}",
            "app_server_image_observation": "not exposed by the pinned bridge hello protocol",
        }
    except Exception as error:
        checks["profile_assets"] = {"status": "ERROR", "reason": type(error).__name__}

    engine = create_engine(settings.database_url)
    try:
        db = await check_database(engine)
        checks["postgres"] = {
            "status": "READY" if db.available else "NOT_RUNNING",
            "version": db.postgres_version,
            "migration_head": db.migration_head,
        }
    finally:
        await engine.dispose()
    try:
        checks["authorization"] = await _authorization_status(settings)
    except Exception as error:
        checks["authorization"] = {"status": "UNAVAILABLE", "reason": type(error).__name__}

    checks["node_bridge"] = {
        "status": "NOT_READY",
        "node_bin": settings.node_bin,
        "bridge_entry": str(settings.bridge_entry),
        "expected_version": "22.19.0",
    }
    node = shutil.which(settings.node_bin) or settings.node_bin
    try:
        result = subprocess.run([node, "--version"], capture_output=True, text=True, check=True, timeout=5)
        version = result.stdout.strip()
        built = settings.bridge_entry.is_file()
        checks["node_bridge"].update({
            "status": "READY" if version == "v22.19.0" and built else "MISMATCH",
            "version": version,
            "bridge_built": built,
        })
    except Exception as error:
        checks["node_bridge"]["reason"] = type(error).__name__
    checks["ollama"] = await asyncio.to_thread(_ollama_status, settings)
    checks["provider_gateway"] = await asyncio.to_thread(_gateway_status, settings)
    checks["letta_runtime"] = await _runtime_status(settings)
    required = {"configuration", "profile_assets", "postgres", "authorization", "node_bridge"}
    ready = all(isinstance(checks.get(key), Mapping) and checks[key].get("status") == "READY" for key in required)
    services = {"provider_gateway", "letta_runtime"}
    if settings.runtime_mode == "local":
        services.add("ollama")
    services_ready = all(isinstance(checks.get(key), Mapping) and checks[key].get("status") == "READY" for key in services)
    service_checks = [checks.get(key) for key in services]
    service_statuses = [
        item.get("status") if isinstance(item, Mapping) else None for item in service_checks
    ]
    services_absent_or_ready = all(
        status in {"READY", "NOT_RUNNING", "NOT_LOADED"} for status in service_statuses
    )
    prestart = ready and services_absent_or_ready and not services_ready
    return {
        "status": "READY" if ready and services_ready else "PRESTART_OK" if prestart else "NOT_READY",
        "checks": checks,
        "inference_requested": False,
        "database_modified": False,
    }


def _gateway_status(settings: Settings) -> dict[str, object]:
    gateway = settings.local.get("gateway")
    if not isinstance(gateway, dict):
        return {"status": "NOT_CONFIGURED"}
    base = f"http://{gateway.get('host')}:{gateway.get('port')}"
    try:
        health = _read_json(base + "/healthz")
        token = os.environ.get("HEKATE_PROVIDER_GATEWAY_TOKEN", "")
        models = _read_json(base + "/v1/models", token=token)
        profile = gateway_profile(settings)
        available = models.get("data", []) if isinstance(models, dict) else []
        matches = any(isinstance(item, dict) and item.get("id") == profile.price_table.model for item in available)
        return {
            "status": "READY" if health.get("status") == "ok" and matches else "CONFIG_MISMATCH",
            "model_route_matches": matches,
            "endpoint": base,
        }
    except Exception as error:
        status = "NOT_RUNNING" if _connection_refused(error) else "UNAVAILABLE"
        return {"status": status, "reason": type(error).__name__, "endpoint": base}


async def _runtime_status(settings: Settings) -> dict[str, object]:
    bridge = BridgeClient(
        settings.node_bin,
        settings.bridge_entry,
        env={
            "HEKATE_LETTA_URL": settings.letta_url,
            **({"HEKATE_LETTA_TOKEN": settings.letta_token} if settings.letta_token else {}),
            "HEKATE_LETTA_TURN_TIMEOUT_MS": str(settings.letta_turn_timeout_ms),
        },
    )
    runtime = LettaRuntimeAdapter(bridge)
    try:
        report = await runtime.verify_compatibility()
        return {
            "status": "READY",
            "sdk_version": report.get("sdk_version"),
            "protocol_version": report.get("protocol_version"),
            "capabilities": report.get("capabilities"),
            "generation_requested": False,
        }
    except Exception as error:
        if _connection_refused(error):
            status = "NOT_RUNNING"
        elif isinstance(error, (ValueError, RuntimeError)):
            status = "CONFIG_MISMATCH"
        else:
            status = "UNAVAILABLE"
        return {"status": status, "reason": type(error).__name__, "generation_requested": False}
    finally:
        await bridge.close()
