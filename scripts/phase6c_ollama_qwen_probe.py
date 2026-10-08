from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import urllib.error
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

import yaml
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
LOCAL_SMOKE_LEDGER = ROOT / "integration/runtime/artifacts/p6d-qwen-single-attempt-ledger.json"
INDEPENDENT_LOCAL_SMOKE_LEDGER = ROOT / "integration/runtime/artifacts/p6d-qwen-independent-authorized-attempt-ledger.json"
NATIVE_SCHEMA_LOCAL_SMOKE_LEDGER = ROOT / "integration/runtime/artifacts/p6d-qwen-native-schema-authorized-attempt-ledger.json"
OUTPUT_OBSERVED_LOCAL_SMOKE_LEDGER = ROOT / "integration/runtime/artifacts/p6d-qwen-output-observed-authorized-attempt-ledger.json"
OUTPUT_OBSERVED_FOLLOWUP_LOCAL_SMOKE_LEDGER = ROOT / "integration/runtime/artifacts/p6d-qwen-output-observed-followup-authorized-attempt-ledger.json"
PREVIOUS_LOCAL_SMOKE_ARTIFACT = ROOT / "integration/runtime/artifacts/p6d-20261006T042104Z-b6afec32-ollama-qwen-runtime.json"
PREVIOUS_FAILED_LOCAL_SMOKE_ARTIFACT = ROOT / "integration/runtime/artifacts/p6d-20261006T150156Z-7f915cee-ollama-qwen-runtime.json"
PREVIOUS_NATIVE_SCHEMA_FAILURE_ARTIFACT = ROOT / "integration/runtime/artifacts/p6d-20261006T162211Z-18c1dad6-ollama-qwen-runtime.json"
OUTPUT_OBSERVED_PREFLIGHT_ARTIFACT = ROOT / "integration/runtime/artifacts/p6d-20261006T190449Z-4d4c7316-output-delivery.json"
OUTPUT_OBSERVED_FOLLOWUP_PREFLIGHT_ARTIFACT = ROOT / "integration/runtime/artifacts/p6d-20261006T192628Z-27d0c82c-output-delivery.json"
PREVIOUS_OUTPUT_OBSERVED_ARTIFACT = ROOT / "integration/runtime/artifacts/p6d-20261006T190724Z-ab1a653c-output-observed-qwen.json"
OUTPUT_OBSERVED_DIAGNOSIS_ARTIFACT = ROOT / "integration/runtime/artifacts/p6d-20261006T193535Z-9c58f9a2-qwen-output-diagnosis.json"

import integration_probe as p1
import phase3_runtime_probe as p3
import phase3b_single_hekate_probe as p3b

import hekate.infrastructure.letta.provider_gateway as gateway_module
from hekate.bootstrap import Container
from hekate.domain.capsules import parse_hekate_turn_output
from hekate.domain.models import AuthorizationSnapshot
from hekate.domain.types import PrincipalId, ScopeId, TaskId
from hekate.infrastructure.letta.adapter import LettaRuntimeAdapter
from hekate.infrastructure.letta.bridge_protocol import BridgeClient
from hekate.infrastructure.letta.provider_gateway import ProviderGatewayProfile, create_provider_gateway
from hekate.infrastructure.letta.qwen_ollama import (
    hekate_turn_output_schema,
    load_qwen_candidate_profile,
    measure_qwen35_request,
    qwen35_native_json_schema_test_execution_profile,
    qwen35_test_execution_profile,
    validate_qwen_candidate_profile,
)
from hekate.infrastructure.postgres.database import create_engine, create_uow_factory
from hekate.settings import configured_local_actor, configured_task_execution, load_settings
from hekate.worker.service import run_worker


_FINGERPRINT_FILES = (
    "pyproject.toml",
    "config/local-candidates.yaml",
    "scripts/build_qwen35_unicode_flags.py",
    "scripts/phase3_runtime_probe.py",
    "scripts/phase6c_ollama_qwen_probe.py",
    "scripts/export_contracts.py",
    "contracts/generated/bridge.v1.schema.json",
    "src/hekate/domain/models.py",
    "src/hekate/domain/bridge_contracts.py",
    "src/hekate/settings.py",
    "src/hekate/application/operations.py",
    "src/hekate/application/lifecycle.py",
    "src/hekate/application/turns.py",
    "src/hekate/ports/runtime.py",
    "src/hekate/infrastructure/letta/adapter.py",
    "src/hekate/infrastructure/letta/provider_gateway.py",
    "src/hekate/infrastructure/letta/token_accounting.py",
    "src/hekate/infrastructure/letta/qwen_ollama.py",
    "bridge/letta/src/protocol.ts",
    "bridge/letta/src/main.ts",
    "src/hekate/infrastructure/letta/assets/qwen35_installed_tokenizer.v1.json",
    "src/hekate/infrastructure/letta/assets/qwen35_unicode_flags.v1.json",
    "integration/runtime/fixtures/qwen35-renderer-tokenizer-v1.json",
    "tests/unit/test_qwen_ollama.py",
    "tests/unit/test_provider_gateway_sse.py",
    "tests/unit/test_phase6d_local_probe_guard.py",
    "integration/letta/versions.lock.json",
    "integration/letta/patches/provider-call-context-usage.patch",
    "docs/implementation/phase6c-ollama-qwen-profile.md",
    "docs/implementation/phase6d-local-qwen-smoke.md",
)


def _ollama_read_json(method: str, path: str, payload: dict[str, object] | None = None) -> object:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode() if payload is not None else None
    request = urllib.request.Request(
        f"http://127.0.0.1:19191{path}", data=body,
        headers={"Content-Type": "application/json"} if body is not None else {}, method=method,
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status != 200:
            raise RuntimeError(f"read-only Ollama metadata route {path} returned {response.status}")
        value = json.loads(response.read())
    if not isinstance(value, dict):
        raise ValueError(f"read-only Ollama metadata route {path} returned a non-object")
    return value


def _verify_live_ollama_metadata(candidate) -> dict[str, object]:
    version = _ollama_read_json("GET", "/api/version")
    tags = _ollama_read_json("GET", "/api/tags")
    ps = _ollama_read_json("GET", "/api/ps")
    models = _ollama_read_json("GET", "/v1/models")
    show_verbose = _ollama_read_json("POST", "/api/show", {"model": candidate.model, "verbose": True})
    show_short = _ollama_read_json("POST", "/api/show", {"model": candidate.model, "verbose": False})
    tagged = tags.get("models")
    if not isinstance(tagged, list):
        raise ValueError("Ollama /api/tags response has no model list")
    model_record = next((item for item in tagged if isinstance(item, dict) and item.get("name") == candidate.model), None)
    if not isinstance(model_record, dict) or model_record.get("digest") != candidate.model_manifest_digest:
        raise ValueError("live Ollama model manifest differs from the pinned candidate")
    if model_record.get("size") != 16_240_185_450:
        raise ValueError("live Ollama model size differs from the pinned candidate")
    version_string = version.get("version")
    if version_string != candidate.ollama_version:
        raise ValueError("live Ollama version differs from the pinned renderer source")
    listed = models.get("data")
    if not isinstance(listed, list) or not any(isinstance(item, dict) and item.get("id") == candidate.model for item in listed):
        raise ValueError("Ollama OpenAI-compatible model discovery does not list the pinned model")
    model_info = show_verbose.get("model_info")
    if not isinstance(model_info, dict):
        raise ValueError("verbose Ollama model metadata is missing")
    expected_arrays = {
        "tokenizer.ggml.tokens": candidate.tokenizer_tokens_sha256,
        "tokenizer.ggml.merges": candidate.tokenizer_merges_sha256,
        "tokenizer.ggml.token_type": candidate.tokenizer_token_types_sha256,
    }
    array_hashes: dict[str, str] = {}
    for key, expected in expected_arrays.items():
        value = model_info.get(key)
        if not isinstance(value, list):
            raise ValueError(f"Ollama verbose model response omitted {key}")
        digest = hashlib.sha256(json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        if digest != expected:
            raise ValueError(f"live installed Qwen tokenizer differs at {key}")
        array_hashes[key] = digest
    template = show_verbose.get("template")
    if not isinstance(template, str) or hashlib.sha256(template.encode()).hexdigest() != candidate.model_template_sha256:
        raise ValueError("live model template differs from its read-only pinned identity")
    ps_models = ps.get("models")
    if not isinstance(ps_models, list):
        raise ValueError("Ollama /api/ps response has no model list")
    short_info = show_short.get("model_info")
    return {
        "metadata_requests": 6,
        "routes": [
            "GET /api/version", "GET /api/tags", "GET /api/ps", "GET /v1/models",
            "POST /api/show verbose=true", "POST /api/show verbose=false",
        ],
        "ollama_version": version_string,
        "model": candidate.model,
        "manifest_digest": model_record["digest"],
        "model_size_bytes": model_record["size"],
        "openai_model_list_contains_exact_tag": True,
        "installed_tokenizer_array_sha256": array_hashes,
        "template_bytes": len(template.encode()),
        "template_sha256": hashlib.sha256(template.encode()).hexdigest(),
        "loaded_models_observed": [
            {key: item.get(key) for key in ("name", "model", "size", "digest", "expires_at", "context_length") if key in item}
            for item in ps_models if isinstance(item, dict)
        ],
        "loaded_context_verified": False,
        "gguf_blob_digest_source": "pinned local manifest/blob metadata; not inferred from /api/tags manifest digest",
        "parameter_count_observed": short_info.get("general.parameter_count") if isinstance(short_info, dict) else None,
        "inference_requests": 0,
    }


def _ollama_model_only_load(candidate, *, timeout_seconds: float = 600) -> dict[str, object]:
    """Load the pinned runner with Ollama's documented empty-prompt load path."""
    payload = json.dumps({
        "model": candidate.model,
        "prompt": "",
        "stream": False,
        "keep_alive": "10m",
    }, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        "http://127.0.0.1:19191/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    opener = urllib.request.build_opener(NoRedirect)
    with opener.open(request, timeout=timeout_seconds) as response:
        if response.status != 200:
            raise RuntimeError(f"Ollama model-only load returned HTTP {response.status}")
        raw = response.read(65_537)
    if len(raw) > 65_536:
        raise ValueError("Ollama model-only load response exceeded 64 KiB")
    value = json.loads(raw.decode("utf-8", "strict"))
    if not isinstance(value, dict):
        raise ValueError("Ollama model-only load returned a non-object")
    if (
        value.get("model") != candidate.model
        or value.get("done") is not True
        or value.get("done_reason") != "load"
        or value.get("response") != ""
        or any(value.get(key) not in (None, 0) for key in ("prompt_eval_count", "eval_count"))
    ):
        raise ValueError("Ollama did not confirm a no-generation model-only load")
    return {
        "route": "POST /api/generate",
        "request_model": candidate.model,
        "request_prompt_bytes": 0,
        "request_stream": False,
        "request_keep_alive": "10m",
        "request_options": {},
        "http_status": 200,
        "response_model": value.get("model"),
        "done": value.get("done"),
        "done_reason": value.get("done_reason"),
        "response_bytes": len(str(value.get("response", "")).encode("utf-8")),
        "prompt_eval_count": value.get("prompt_eval_count"),
        "eval_count": value.get("eval_count"),
        "generation_observed": False,
    }


def _observe_loaded_context(candidate, *, timeout_seconds: float = 30) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    last_models: list[object] = []
    while time.monotonic() < deadline:
        observation = _observe_current_loaded_runner(candidate)
        models = observation["loaded_models"]
        last_models = models if isinstance(models, list) else []
        if observation["context_verified_for_this_loaded_runner"]:
            return {key: value for key, value in observation.items() if key != "loaded_models"}
        time.sleep(0.5)
    raise TimeoutError(f"model-only load completed but /api/ps did not show the model; loaded_entries={len(last_models)}")


def _observe_current_loaded_runner(candidate) -> dict[str, object]:
    """Take one live /api/ps snapshot and verify the exact loaded model/context."""
    ps = _ollama_read_json("GET", "/api/ps")
    models = ps.get("models")
    if not isinstance(models, list):
        raise ValueError("Ollama /api/ps response has no model list")
    loaded = [item for item in models if isinstance(item, dict) and item.get("name") == candidate.model]
    if len(loaded) > 1:
        raise ValueError("Ollama reports duplicate loaded entries for the pinned model")
    if not loaded:
        return {
            "route": "GET /api/ps",
            "model": candidate.model,
            "loaded": False,
            "context_verified_for_this_loaded_runner": False,
            "loaded_models": models,
        }
    item = loaded[0]
    context_length = item.get("context_length")
    if item.get("digest") != candidate.model_manifest_digest:
        raise ValueError("loaded Ollama model digest differs from the pinned base model")
    if type(context_length) is not int or context_length < candidate.context_window_tokens:
        raise ValueError("loaded Ollama context is absent or below the fixed 8192-token provider gate")
    return {
        "route": "GET /api/ps",
        "model": item.get("name"),
        "reported_model": item.get("model"),
        "manifest_digest": item.get("digest"),
        "context_length_tokens": context_length,
        "minimum_required_tokens": candidate.context_window_tokens,
        "context_verified_for_this_loaded_runner": True,
        "loaded": True,
        "size_bytes": item.get("size"),
        "size_vram_bytes": item.get("size_vram"),
        "expires_at": item.get("expires_at"),
        "model_count": len(models),
        "loaded_models": models,
    }


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_ledger_new(value: dict[str, object]) -> None:
    LOCAL_SMOKE_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(LOCAL_SMOKE_LEDGER, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise
    _fsync_directory(LOCAL_SMOKE_LEDGER.parent)


def _update_ledger(run_id: str, updates: dict[str, object]) -> dict[str, object]:
    current = json.loads(LOCAL_SMOKE_LEDGER.read_text(encoding="utf-8"))
    if not isinstance(current, dict) or current.get("run_id") != run_id:
        raise RuntimeError("single-call smoke ledger is corrupt or belongs to another run")
    current.update(updates)
    temporary = LOCAL_SMOKE_LEDGER.with_name(f".{LOCAL_SMOKE_LEDGER.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(current, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, LOCAL_SMOKE_LEDGER)
        _fsync_directory(LOCAL_SMOKE_LEDGER.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return current


def _reserve_local_smoke_ledger(
    run_id: str,
    candidate,
    database_target: dict[str, object],
    authorization: dict[str, object] | None = None,
) -> None:
    value: dict[str, object] = {
        "schema_version": "1",
        "run_id": run_id,
        "scope_id": f"phase6d-qwen-{run_id}",
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "state": "AUTHORIZED_WAITING_FOR_CONTEXT",
        "model_load_attempts": 0,
        "upstream_attempts": 0,
        "target": {
            "model": candidate.model,
            "manifest_digest": candidate.model_manifest_digest,
            "profile_digest": candidate.content_digest,
            "endpoint": "http://127.0.0.1:19191/v1/chat/completions",
            "database": database_target,
        },
        "attempt": None,
        "response": None,
    }
    if authorization is not None:
        value["authorization"] = authorization
    _write_ledger_new(value)


def _read_local_smoke_ledger() -> dict[str, object] | None:
    if not LOCAL_SMOKE_LEDGER.exists():
        return None
    try:
        value = json.loads(LOCAL_SMOKE_LEDGER.read_text(encoding="utf-8"))
    except Exception as error:
        raise RuntimeError("single-call smoke ledger exists but cannot be read; do not retry") from error
    if not isinstance(value, dict):
        raise RuntimeError("single-call smoke ledger exists with an invalid shape; do not retry")
    return value


def _local_ledger_summary() -> dict[str, object] | None:
    value = _read_local_smoke_ledger()
    if value is None:
        return None
    return {
        "run_id": value.get("run_id"),
        "state": value.get("state"),
        "model_load_attempts": value.get("model_load_attempts"),
        "upstream_attempts": value.get("upstream_attempts"),
    }


def _local_unexecuted_boundaries(upstream_attempts: int) -> list[str]:
    if upstream_attempts:
        first = "The one authorized Task generation attempt was used; no retry or second local inference was made."
    else:
        first = "The authorized Task generation request did not reach the upstream; no retry was made."
    return [
        first,
        "Critic, continuation, compaction, and tools were not run.",
        "G8 production request and usage validation remains unresolved.",
    ]


def _is_expected_korean_arithmetic_response(value: object) -> bool:
    if not isinstance(value, str):
        return False
    normalized = value.strip()
    return bool(
        normalized
        and "```" not in normalized
        and "17" in normalized
        and "25" in normalized
        and "42" in normalized
        and any("가" <= char <= "힣" for char in normalized)
        and "\n" not in normalized
    )


def _authorize_independent_attempt_after_unknown() -> dict[str, object]:
    prior_ledger = _read_local_smoke_ledger()
    if prior_ledger is None or prior_ledger.get("upstream_attempts") != 0:
        raise ValueError("the prior local attempt ledger is missing or its single upstream attempt was consumed")
    try:
        artifact_bytes = PREVIOUS_LOCAL_SMOKE_ARTIFACT.read_bytes()
        prior_artifact = json.loads(artifact_bytes)
    except Exception as error:
        raise ValueError("the prior local attempt artifact is unavailable; independent authorization cannot be bound") from error
    if (
        not isinstance(prior_artifact, dict)
        or prior_artifact.get("run_id") != prior_ledger.get("run_id")
        or prior_artifact.get("overall_status") != "blocked"
        or prior_artifact.get("upstream_generation_attempts") != 0
        or prior_artifact.get("actual_provider_calls") != 0
        or prior_artifact.get("provider_send_observations") != []
        or prior_artifact.get("provider_response_observations") != []
        or prior_artifact.get("single_attempt_ledger", {}).get("upstream_attempts") != 0
    ):
        raise ValueError("the prior run does not prove zero upstream attempts; refusing a new allowance")
    authorizations = prior_artifact.get("provider_authorization_attempts")
    first_authorization = authorizations[0] if isinstance(authorizations, list) and authorizations else {}
    return {
        "type": "explicit_independent_test_after_unknown",
        "recorded_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "max_upstream_generation_attempts": 1,
        "prior_run_id": prior_ledger.get("run_id"),
        "prior_artifact": PREVIOUS_LOCAL_SMOKE_ARTIFACT.relative_to(ROOT).as_posix(),
        "prior_artifact_sha256": hashlib.sha256(artifact_bytes).hexdigest(),
        "prior_database": prior_artifact.get("database_target", {}).get("database"),
        "prior_task_id": first_authorization.get("task_id"),
        "prior_operation_id": first_authorization.get("operation_id"),
        "prior_accounting_call_id": first_authorization.get("accounting_call_id"),
        "prior_permit_id": first_authorization.get("permit_id"),
        "prior_unknown_records_modified": False,
        "prior_upstream_attempts": 0,
    }


def _authorize_native_schema_attempt_after_failed_output() -> dict[str, object]:
    """Bind the new one-shot allowance to prior artifacts without modifying them."""
    diagnostic_path = ROOT / "integration/runtime/artifacts/p6d-20261006T150950Z-ffa98a36-independent-attempt-diagnostic.json"
    try:
        diagnostic_bytes = diagnostic_path.read_bytes()
        diagnostic = json.loads(diagnostic_bytes)
        failed_bytes = PREVIOUS_FAILED_LOCAL_SMOKE_ARTIFACT.read_bytes()
        failed = json.loads(failed_bytes)
    except Exception as error:
        raise ValueError("the prior failed local attempt evidence is unavailable") from error
    execution_artifact = diagnostic.get("execution_artifact") if isinstance(diagnostic, dict) else None
    turn = failed.get("results", {}).get("pinned_runtime_turn") if isinstance(failed, dict) else None
    if (
        not isinstance(diagnostic, dict)
        or not isinstance(execution_artifact, dict)
        or not isinstance(failed, dict)
        or not isinstance(turn, dict)
        or execution_artifact.get("path") != PREVIOUS_FAILED_LOCAL_SMOKE_ARTIFACT.relative_to(ROOT).as_posix()
        or execution_artifact.get("sha256") != hashlib.sha256(failed_bytes).hexdigest()
        or execution_artifact.get("execution_status") != "failed_after_single_upstream_attempt"
        or diagnostic.get("independent_execution", {}).get("upstream_generation_attempts") != 1
        or diagnostic.get("independent_execution", {}).get("actual_local_provider_calls") != 1
        or diagnostic.get("prior_unknown_execution", {}).get("modified_or_retried") is not False
        or failed.get("overall_status") != "failed_after_single_upstream_attempt"
        or failed.get("upstream_generation_attempts") != 1
        or failed.get("actual_provider_calls") != 1
        or failed.get("single_attempt_ledger", {}).get("upstream_attempts") != 1
        or failed.get("task_observation", {}).get("state") != "FAILED"
        or turn.get("result_and_schema_validation", {}).get("processing_state") != "REJECTED"
    ):
        raise ValueError("prior Qwen run does not match the recorded one-call schema-validation failure")
    database = failed.get("database_target", {}).get("database")
    if not isinstance(database, str) or not database:
        raise ValueError("prior failed run has no database identity")
    return {
        "type": "user_authorized_native_json_schema_followup",
        "recorded_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "max_upstream_generation_attempts": 1,
        "prior_failed_run_id": failed.get("run_id"),
        "prior_failed_artifact": PREVIOUS_FAILED_LOCAL_SMOKE_ARTIFACT.relative_to(ROOT).as_posix(),
        "prior_failed_artifact_sha256": hashlib.sha256(failed_bytes).hexdigest(),
        "prior_diagnostic_artifact": diagnostic_path.relative_to(ROOT).as_posix(),
        "prior_diagnostic_artifact_sha256": hashlib.sha256(diagnostic_bytes).hexdigest(),
        "prior_database": database,
        "prior_unknown_records_modified": False,
        "prior_failed_task_modified_or_retried": False,
        "authorization_basis": "active user instruction explicitly permits one new independent local Qwen attempt after offline native-schema verification",
    }


def _authorize_output_observed_independent_attempt() -> dict[str, object]:
    """Record the new user-authorized allowance without touching prior tasks or ledgers."""
    try:
        failed_bytes = PREVIOUS_NATIVE_SCHEMA_FAILURE_ARTIFACT.read_bytes()
        failed = json.loads(failed_bytes)
        fake_bytes = OUTPUT_OBSERVED_PREFLIGHT_ARTIFACT.read_bytes()
        fake = json.loads(fake_bytes)
    except Exception as error:
        raise ValueError("the prior native-schema failure and fake delivery evidence must be readable") from error
    results = fake.get("results") if isinstance(fake, dict) else None
    delivery = results.get("output_delivery_reproduction") if isinstance(results, dict) else None
    complete = delivery.get("complete_chain") if isinstance(delivery, dict) else None
    incomplete = delivery.get("incomplete_chain") if isinstance(delivery, dict) else None
    terminal = delivery.get("terminal_content_comparison") if isinstance(delivery, dict) else None
    pinned = results.get("pinned_runtime_turn") if isinstance(results, dict) else None

    def adapter_fixture_matches(chain: object, expected_bytes: int) -> bool:
        if not isinstance(chain, dict):
            return False
        stages = chain.get("assistant_content_stages")
        adapter = stages.get("provider_adapter_internal_text_delta") if isinstance(stages, dict) else None
        return bool(
            chain.get("all_observed_assistant_content_matches_provider_fixture") is True
            and chain.get("gateway_sse_wire_matches_fake_provider_wire") is True
            and isinstance(adapter, dict)
            and adapter.get("directly_instrumented") is True
            and adapter.get("event_count") == 265
            and adapter.get("input_provider_content", {}).get("utf8_bytes") == expected_bytes
            and adapter.get("emitted_text_delta", {}).get("utf8_bytes") == expected_bytes
            and adapter.get("emitted_text_delta", {}).get("per_delta_matches_input") is True
        )

    if (
        not isinstance(failed, dict)
        or failed.get("execution_mode") != "one_local_ollama_smoke"
        or failed.get("overall_status") != "failed_after_single_upstream_attempt"
        or failed.get("upstream_generation_attempts") != 1
        or failed.get("actual_provider_calls") != 1
        or failed.get("single_attempt_ledger", {}).get("upstream_attempts") != 1
        or not isinstance(fake, dict)
        or fake.get("execution_mode") != "pinned_fake_provider_output_delivery"
        or fake.get("overall_status") != "pass"
        or fake.get("fake_provider_requests") != 2
        or fake.get("actual_provider_calls") != 0
        or fake.get("upstream_generation_attempts") != 0
        or fake.get("output_observation_diagnostics", {}).get("enabled") is not True
        or not isinstance(pinned, dict)
        or pinned.get("fake_context_and_policy_verified") is not True
        or not adapter_fixture_matches(complete, 760)
        or not adapter_fixture_matches(incomplete, 759)
        or not isinstance(terminal, dict)
        or terminal.get("complete_task_accepted") is not True
        or terminal.get("incomplete_task_rejected") is not True
    ):
        raise ValueError("prior evidence does not prove a separate failed attempt and successful fake delivery diagnostic")
    previous_database = failed.get("database_target", {}).get("database")
    if not isinstance(previous_database, str) or not previous_database:
        raise ValueError("prior actual attempt has no recorded isolated database")
    return {
        "type": "user_authorized_new_independent_output_observed_attempt",
        "recorded_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "max_upstream_generation_attempts": 1,
        "prior_failed_run_id": failed.get("run_id"),
        "prior_failed_artifact": PREVIOUS_NATIVE_SCHEMA_FAILURE_ARTIFACT.relative_to(ROOT).as_posix(),
        "prior_failed_artifact_sha256": hashlib.sha256(failed_bytes).hexdigest(),
        "prior_fake_run_id": fake.get("run_id"),
        "prior_fake_artifact": OUTPUT_OBSERVED_PREFLIGHT_ARTIFACT.relative_to(ROOT).as_posix(),
        "prior_fake_artifact_sha256": hashlib.sha256(fake_bytes).hexdigest(),
        "prior_database": previous_database,
        "prior_failed_task_modified_or_retried": False,
        "prior_unknown_records_modified": False,
        "authorization_basis": "current user instruction explicitly permits one fresh independent actual Qwen generation attempt with bounded output observation",
    }


def _authorize_output_observed_followup_attempt() -> dict[str, object]:
    """Bind the current one-shot allowance to the previous observed run and its diagnosis."""
    try:
        previous_bytes = PREVIOUS_OUTPUT_OBSERVED_ARTIFACT.read_bytes()
        previous = json.loads(previous_bytes)
        diagnosis_bytes = OUTPUT_OBSERVED_DIAGNOSIS_ARTIFACT.read_bytes()
        diagnosis = json.loads(diagnosis_bytes)
        ledger = json.loads(OUTPUT_OBSERVED_LOCAL_SMOKE_LEDGER.read_text(encoding="utf-8"))
        preflight_bytes = OUTPUT_OBSERVED_FOLLOWUP_PREFLIGHT_ARTIFACT.read_bytes()
        preflight = json.loads(preflight_bytes)
    except Exception as error:
        raise ValueError("the consumed output-observation run and its follow-up evidence must be readable") from error

    actual = diagnosis.get("actual_run") if isinstance(diagnosis, dict) else None
    boundaries = actual.get("boundaries") if isinstance(actual, dict) else None
    provider = boundaries.get("ollama_choice_0_content") if isinstance(boundaries, dict) else None
    gateway = boundaries.get("gateway_received_and_yielded_choice_0") if isinstance(boundaries, dict) else None
    adapter_in = boundaries.get("provider_adapter_received_content") if isinstance(boundaries, dict) else None
    adapter_out = boundaries.get("provider_adapter_emitted_text_delta") if isinstance(boundaries, dict) else None
    app_server = boundaries.get("app_server_assistant_stream") if isinstance(boundaries, dict) else None
    sdk = boundaries.get("sdk_assistant_messages") if isinstance(boundaries, dict) else None
    persisted = boundaries.get("postgresql_raw_output") if isinstance(boundaries, dict) else None
    preflight_results = preflight.get("results") if isinstance(preflight, dict) else None
    delivery = preflight_results.get("output_delivery_reproduction") if isinstance(preflight_results, dict) else None
    complete = delivery.get("complete_chain") if isinstance(delivery, dict) else None
    incomplete = delivery.get("incomplete_chain") if isinstance(delivery, dict) else None
    terminal = delivery.get("terminal_content_comparison") if isinstance(delivery, dict) else None
    fake_task = preflight_results.get("pinned_runtime_turn") if isinstance(preflight_results, dict) else None
    replay = diagnosis.get("fake_verification", {}).get("timeout_240ms") if isinstance(diagnosis, dict) else None

    def adapter_preflight_matches(chain: object, expected_bytes: int) -> bool:
        if not isinstance(chain, dict):
            return False
        stages = chain.get("assistant_content_stages")
        adapter = stages.get("provider_adapter_internal_text_delta") if isinstance(stages, dict) else None
        return bool(
            chain.get("all_observed_assistant_content_matches_provider_fixture") is True
            and chain.get("gateway_sse_wire_matches_fake_provider_wire") is True
            and isinstance(adapter, dict)
            and adapter.get("directly_instrumented") is True
            and adapter.get("event_count") == 265
            and adapter.get("input_provider_content", {}).get("utf8_bytes") == expected_bytes
            and adapter.get("emitted_text_delta", {}).get("utf8_bytes") == expected_bytes
            and adapter.get("emitted_text_delta", {}).get("per_delta_matches_input") is True
        )

    if (
        not isinstance(previous, dict)
        or previous.get("execution_mode") != "one_local_ollama_output_observation_diagnostic"
        or previous.get("overall_status") != "failed_after_single_upstream_attempt"
        or previous.get("actual_provider_calls") != 1
        or previous.get("upstream_generation_attempts") != 1
        or previous.get("single_attempt_ledger", {}).get("upstream_attempts") != 1
        or not isinstance(ledger, dict)
        or ledger.get("run_id") != previous.get("run_id")
        or ledger.get("upstream_attempts") != 1
        or ledger.get("state") != "UPSTREAM_RESPONSE_READ"
        or not isinstance(previous.get("task_observation"), dict)
        or previous["task_observation"].get("state") != "FAILED"
        or previous["task_observation"].get("stop_reason") != "POLICY"
        or diagnosis.get("probe") != "phase6d-one-authorized-qwen-output-diagnosis"
        or not isinstance(actual, dict)
        or actual.get("artifact") != PREVIOUS_OUTPUT_OBSERVED_ARTIFACT.relative_to(ROOT).as_posix()
        or actual.get("source_artifact_sha256") != hashlib.sha256(previous_bytes).hexdigest()
        or actual.get("actual_provider_calls") != 1
        or actual.get("authorized_generation_attempts") != 1
        or actual.get("failure_diagnosis", {}).get("recomputed_first_divergence") != "sdk_assistant_messages:content_mismatch"
        or actual.get("failure_diagnosis", {}).get("sdk_timeout_ms") != 30_000
        or not isinstance(provider, dict)
        or not isinstance(gateway, dict)
        or not isinstance(adapter_in, dict)
        or not isinstance(adapter_out, dict)
        or not isinstance(app_server, dict)
        or not isinstance(sdk, dict)
        or not isinstance(persisted, dict)
        or any(item.get("utf8_bytes") != 763 for item in (provider, gateway, adapter_in, adapter_out, app_server))
        or sdk.get("utf8_bytes") != 669
        or persisted.get("utf8_bytes") != 669
        or actual.get("actual_same_key_replay", {}).get("no_new_upstream_request") is not True
        or not isinstance(preflight, dict)
        or preflight.get("execution_mode") != "pinned_fake_provider_output_delivery"
        or preflight.get("overall_status") != "pass"
        or preflight.get("fake_provider_requests") != 2
        or preflight.get("actual_provider_calls") != 0
        or preflight.get("upstream_generation_attempts") != 0
        or not isinstance(delivery, dict)
        or not adapter_preflight_matches(complete, 760)
        or not adapter_preflight_matches(incomplete, 759)
        or not isinstance(terminal, dict)
        or terminal.get("complete_task_accepted") is not True
        or terminal.get("incomplete_task_rejected") is not True
        or not isinstance(fake_task, dict)
        or fake_task.get("fake_context_and_policy_verified") is not True
        or fake_task.get("same_key_replay", {}).get("no_new_accounting_effect") is not True
        or not isinstance(replay, dict)
        or replay.get("probe_status") != "pass"
        or replay.get("actual_provider_calls") != 0
        or replay.get("sdk_timeout_truncation_reproduced") is not False
        or replay.get("stale_source_binding_rejected") is not True
        or replay.get("transport_complete_through_postgresql") is not True
    ):
        raise ValueError("prior observed output and follow-up fake evidence do not authorize a fresh one-shot attempt")

    previous_database = previous.get("database_target", {}).get("database")
    if not isinstance(previous_database, str) or not previous_database:
        raise ValueError("previous output-observation run has no database identity")
    return {
        "type": "user_authorized_fresh_output_observed_followup",
        "recorded_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "max_upstream_generation_attempts": 1,
        "prior_run_id": previous.get("run_id"),
        "prior_artifact": PREVIOUS_OUTPUT_OBSERVED_ARTIFACT.relative_to(ROOT).as_posix(),
        "prior_artifact_sha256": hashlib.sha256(previous_bytes).hexdigest(),
        "prior_ledger_upstream_attempts": ledger.get("upstream_attempts"),
        "diagnosis_artifact": OUTPUT_OBSERVED_DIAGNOSIS_ARTIFACT.relative_to(ROOT).as_posix(),
        "diagnosis_artifact_sha256": hashlib.sha256(diagnosis_bytes).hexdigest(),
        "prior_fake_artifact": OUTPUT_OBSERVED_FOLLOWUP_PREFLIGHT_ARTIFACT.relative_to(ROOT).as_posix(),
        "prior_fake_artifact_sha256": hashlib.sha256(preflight_bytes).hexdigest(),
        "prior_database": previous_database,
        "prior_task_modified_or_retried": False,
        "prior_unknown_records_modified": False,
        "authorization_basis": "current user instruction explicitly permits at most one fresh independent Qwen generation in a new Task",
    }


def _validate_local_send_binding(
    *,
    execution_profile_digest: str,
    request_digest: str,
    authorization: dict[str, object] | None,
    consumed_permit: dict[str, object] | None,
) -> None:
    if (
        not isinstance(authorization, dict)
        or authorization.get("authorization") != "permitted"
        or authorization.get("request_digest") != request_digest
        or authorization.get("profile_digest") != execution_profile_digest
        or not isinstance(consumed_permit, dict)
        or consumed_permit.get("consumed") is not True
        or consumed_permit.get("accounting_call_id") != authorization.get("accounting_call_id")
        or consumed_permit.get("permit_id") != authorization.get("permit_id")
        or consumed_permit.get("request_digest") != request_digest
        or consumed_permit.get("profile_digest") != execution_profile_digest
    ):
        raise ValueError("provider_bytes_do_not_match_measured_consumed_permit")


def _prepare_local_send_observation(
    candidate,
    execution_profile_digest: str,
    request: urllib.request.Request,
    authorization: dict[str, object] | None,
    consumed_permit: dict[str, object] | None,
) -> dict[str, object]:
    """Validate the exact Request body before the only local provider socket open."""
    if (
        request.full_url != "http://127.0.0.1:19191/v1/chat/completions"
        or request.get_method() != "POST"
        or not isinstance(request.data, bytes)
    ):
        raise ValueError("non_allowlisted_url_method_or_body")
    request_digest = hashlib.sha256(request.data).hexdigest()
    _validate_local_send_binding(
        execution_profile_digest=execution_profile_digest,
        request_digest=request_digest,
        authorization=authorization,
        consumed_permit=consumed_permit,
    )
    try:
        normalized = json.loads(request.data.decode("utf-8", "strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("provider_request_is_not_strict_utf8_json") from error
    if (
        not isinstance(normalized, dict)
        or normalized.get("model") != candidate.model
        or normalized.get("reasoning_effort") != "none"
        or normalized.get("tools") not in (None, [])
        or normalized.get("temperature") != 0
    ):
        raise ValueError("qwen_request_profile_policy_mismatch")
    output_limit = normalized.get("max_tokens", normalized.get("max_completion_tokens"))
    if type(output_limit) is not int or output_limit > candidate.max_output_tokens:
        raise ValueError("qwen_output_limit_mismatch")
    execution_profile, _prices = qwen35_native_json_schema_test_execution_profile(candidate)
    if execution_profile.content_digest != execution_profile_digest:
        raise ValueError("native_json_schema_execution_profile_digest_mismatch")
    _normalized, expected_bytes, measurement = measure_qwen35_request(
        normalized,
        execution_profile,
        output_limit,
        output_contract="hekate_turn_output_v1",
        native_schema_injected=True,
    )
    if (
        expected_bytes != request.data
        or measurement.request_digest != request_digest
        or measurement.profile_digest != execution_profile_digest
        or measurement.measured_input_tokens != authorization.get("measured_input_tokens")
    ):
        raise ValueError("final_schema_request_differs_from_native_profile_measurement")
    schema, schema_digest = hekate_turn_output_schema()
    return {
        "method": request.get_method(),
        "endpoint": request.full_url,
        "request_bytes": len(request.data),
        "request_sha256": request_digest,
        "profile_digest": execution_profile_digest,
        "request_model": normalized["model"],
        "temperature": normalized.get("temperature"),
        "reasoning_effort": normalized.get("reasoning_effort"),
        "tools_count": len(normalized.get("tools", [])) if isinstance(normalized.get("tools", []), list) else None,
        "native_response_format_present": normalized.get("response_format") == {
            "type": "json_schema", "json_schema": {"schema": schema},
        },
        "native_schema_sha256": schema_digest,
        "measured_input_tokens": authorization.get("measured_input_tokens"),
        "output_limit_tokens": output_limit,
        "accounting_call_id": authorization.get("accounting_call_id"),
        "permit_id": authorization.get("permit_id"),
        "operation_id": authorization.get("operation_id"),
        "task_id": authorization.get("task_id"),
        "attempt_id": authorization.get("attempt_id"),
        "registry_id": authorization.get("registry_id"),
        "input_revision": authorization.get("input_revision"),
        "fence": authorization.get("fence"),
        "permit_consumed_before_send": True,
    }


def _record_pre_socket_guard_rejection(run_id: str, reason: str) -> None:
    ledger = _read_local_smoke_ledger()
    if ledger is not None and ledger.get("run_id") == run_id and ledger.get("upstream_attempts") == 0:
        _update_ledger(run_id, {
            "state": "PRE_SOCKET_GUARD_REJECTED",
            "guard_rejection_reason": reason,
        })


def _install_local_upstream_guard(
    candidate,
    execution_profile_digest: str,
    run_id: str,
    provider_authorizations: list[dict[str, object]],
    consumed_permits: list[dict[str, object]],
    provider_send_observations: list[dict[str, object]],
    provider_response_observations: list[dict[str, object]],
    capture_paths: dict[str, Path],
):
    """Durably count the one allowed request immediately before urllib opens its socket."""
    original_factory = gateway_module._no_redirect_handler
    send_lock = threading.Lock()
    class ObservedResponse:
        def __init__(self, upstream, attempt: dict[str, object]):
            self._upstream = upstream
            self._attempt = attempt
            self._request_digest = str(attempt["request_sha256"])
            self._body = bytearray()
            self._body_size = 0
            self._body_digest = hashlib.sha256()
            self._body_truncated = False
            self._wire_capture_bytes = 0
            self._wire_read_chunks: list[dict[str, object]] = []
            self._wire_capture_error: str | None = None
            try:
                self._wire_capture_fd = os.open(capture_paths["provider_sse"], os.O_WRONLY | os.O_TRUNC)
            except OSError as error:
                self._wire_capture_fd = None
                self._wire_capture_error = type(error).__name__
            self._finalized = False

        def __getattr__(self, name: str):
            return getattr(self._upstream, name)

        def read(self, *args, **kwargs):
            value = self._upstream.read(*args, **kwargs)
            if isinstance(value, bytes):
                if value:
                    self._wire_read_chunks.append({
                        "sequence": len(self._wire_read_chunks) + 1,
                        "received_monotonic_ns": time.monotonic_ns(),
                        "utf8_bytes": len(value),
                    })
                self._body_size += len(value)
                self._body_digest.update(value)
                remaining = max(0, 2_097_152 - len(self._body))
                if remaining:
                    self._body.extend(value[:remaining])
                if len(value) > remaining:
                    self._body_truncated = True
                if self._wire_capture_fd is not None:
                    wire_remaining = max(0, 2_097_152 - self._wire_capture_bytes)
                    try:
                        captured = value[:wire_remaining]
                        offset = 0
                        while offset < len(captured):
                            offset += os.write(self._wire_capture_fd, captured[offset:])
                        self._wire_capture_bytes += len(captured)
                        if len(value) > wire_remaining:
                            self._body_truncated = True
                    except OSError as error:
                        self._wire_capture_error = type(error).__name__
            if not args or not value:
                self._record_response()
            return value

        def _record_response(self):
            if self._finalized:
                return
            self._finalized = True
            body = bytes(self._body)
            content_type = self._upstream.headers.get("Content-Type", "application/json")
            if self._wire_capture_fd is not None:
                try:
                    os.fsync(self._wire_capture_fd)
                finally:
                    os.close(self._wire_capture_fd)
                    self._wire_capture_fd = None
            observed: dict[str, object] = {
                "http_status": getattr(self._upstream, "status", None),
                "content_type": content_type,
                "response_bytes": self._body_size,
                "response_sha256": self._body_digest.hexdigest(),
                "response_instrumentation_truncated": self._body_truncated,
                "request_sha256": self._request_digest,
                "task_id": self._attempt.get("task_id"),
                "operation_id": self._attempt.get("operation_id"),
                "attempt_id": self._attempt.get("attempt_id"),
                "registry_id": self._attempt.get("registry_id"),
                "accounting_call_id": self._attempt.get("accounting_call_id"),
                "permit_id": self._attempt.get("permit_id"),
                "provider_response_wire": _capture_file_summary(capture_paths["provider_sse"]),
                "provider_response_wire_capture_bytes": self._wire_capture_bytes,
                "provider_response_wire_capture_error": self._wire_capture_error,
                "wire_read_chunk_order": self._wire_read_chunks,
            }
            if not self._body_truncated:
                payloads: list[dict[str, object]] = []
                choice_zero = bytearray()
                choice_zero_hash = hashlib.sha256()
                choice_zero_bytes = 0
                choice_zero_events = 0
                frame_order: list[dict[str, object]] = []
                if "text/event-stream" in content_type.lower():
                    observed["sse_done_marker"] = False
                    for line in body.splitlines():
                        line = line.strip()
                        if not line.startswith(b"data:"):
                            continue
                        data = line[5:].strip()
                        if data == b"[DONE]":
                            observed["sse_done_marker"] = True
                            frame_order.append({"sequence": len(frame_order) + 1, "event": "done_marker"})
                            continue
                        try:
                            event = json.loads(data.decode("utf-8", "strict"))
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            continue
                        if isinstance(event, dict):
                            payloads.append(event)
                            item: dict[str, object] = {
                                "sequence": len(frame_order) + 1,
                                "event": "provider_sse_data",
                                "provider_response_id": event.get("id"),
                                "choice_events": [],
                                "usage_present": isinstance(event.get("usage"), dict),
                            }
                            choices = event.get("choices")
                            if isinstance(choices, list):
                                for choice in choices:
                                    if not isinstance(choice, dict):
                                        continue
                                    index = choice.get("index")
                                    delta = choice.get("delta")
                                    content = delta.get("content") if isinstance(delta, dict) else None
                                    choice_event: dict[str, object] = {
                                        "choice_index": index,
                                        "channel": "assistant" if isinstance(content, str) and content else "metadata",
                                        "finish_reason": choice.get("finish_reason"),
                                        "content_utf8_bytes": len(content.encode("utf-8")) if isinstance(content, str) else 0,
                                        "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest()
                                            if isinstance(content, str) else hashlib.sha256(b"").hexdigest(),
                                    }
                                    if index == 0 and isinstance(content, str) and content:
                                        content_bytes = content.encode("utf-8", "strict")
                                        choice_zero_events += 1
                                        choice_zero_bytes += len(content_bytes)
                                        choice_zero_hash.update(content_bytes)
                                        remaining = max(0, 65_536 - len(choice_zero))
                                        choice_zero.extend(content_bytes[:remaining])
                                    if isinstance(delta, dict) and any(
                                        isinstance(delta.get(field), str) and delta.get(field)
                                        for field in ("reasoning", "reasoning_content", "analysis")
                                    ):
                                        choice_event["channel"] = "reasoning" if choice_event["channel"] == "metadata" else "assistant_and_reasoning"
                                    item["choice_events"].append(choice_event)
                            frame_order.append(item)
                else:
                    try:
                        payload = json.loads(body.decode("utf-8", "strict"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        payload = None
                    if isinstance(payload, dict):
                        payloads.append(payload)
                observed["event_order"] = frame_order
                for payload in payloads:
                    usage = payload.get("usage")
                    if isinstance(usage, dict):
                        observed["provider_usage"] = {
                            key: usage.get(key)
                            for key in ("prompt_tokens", "completion_tokens", "total_tokens")
                        }
                    if isinstance(payload.get("id"), str):
                        observed["provider_response_id"] = payload["id"]
                    choices = payload.get("choices")
                    if isinstance(choices, list):
                        for choice in choices:
                            if isinstance(choice, dict) and choice.get("finish_reason") is not None:
                                observed["finish_reason"] = choice.get("finish_reason")
                if "text/event-stream" in content_type.lower():
                    content_summary = _write_bounded_assistant_text(
                        capture_paths["provider_choice_0"], _decode_utf8_prefix(bytes(choice_zero)),
                    )
                    observed["provider_choice_0"] = {
                        "choice_index": 0,
                        "channel": "assistant",
                        "delta_events": choice_zero_events,
                        "utf8_bytes": choice_zero_bytes,
                        "sha256": choice_zero_hash.hexdigest(),
                        "captured": content_summary,
                        "truncated": choice_zero_bytes > 65_536 or self._body_truncated,
                    }
            provider_response_observations.append(observed)
            _update_ledger(run_id, {"state": "UPSTREAM_RESPONSE_READ", "response": observed})

        def close(self):
            try:
                return self._upstream.close()
            finally:
                if self._body:
                    self._record_response()

    class OneShotOpener:
        def __init__(self):
            class NoRedirect(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, req, fp, code, msg, headers, newurl):
                    return None

            self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

        def open(self, request, *args, **kwargs):
            with send_lock:
                ledger = _read_local_smoke_ledger()
                if (
                    ledger is None
                    or ledger.get("run_id") != run_id
                    or ledger.get("upstream_attempts") != 0
                ):
                    raise RuntimeError("single upstream generation attempt has already been consumed")
                authorization = provider_authorizations[-1] if provider_authorizations else None
                consume = consumed_permits[-1] if consumed_permits else None
                try:
                    attempt = _prepare_local_send_observation(
                        candidate, execution_profile_digest, request, authorization, consume,
                    )
                    try:
                        loaded_runner = _observe_current_loaded_runner(candidate)
                    except Exception as error:
                        _record_pre_socket_guard_rejection(
                            run_id, f"loaded_runner_observation_{type(error).__name__}",
                        )
                        raise ValueError(
                            "pinned Qwen runner/context could not be reverified immediately before send",
                        ) from error
                    loaded_runner.pop("loaded_models", None)
                    if loaded_runner.get("context_verified_for_this_loaded_runner") is not True:
                        raise ValueError("pinned Qwen runner/context is no longer verified immediately before send")
                    attempt["live_loaded_runner_pre_send"] = loaded_runner
                except ValueError as error:
                    _record_pre_socket_guard_rejection(run_id, str(error))
                    raise RuntimeError("final provider bytes are not bound to the measured consumed PostgreSQL permit") from error
                request_digest = attempt["request_sha256"]
                if not isinstance(request_digest, str):
                    raise RuntimeError("pre-socket guard returned an invalid request digest")
                # O_EXCL created the one-go marker before model load; this atomic update
                # is fsynced before the only permitted network open.
                _update_ledger(run_id, {
                    "state": "UPSTREAM_SEND_ATTEMPTED",
                    "upstream_attempts": 1,
                    "attempt": attempt,
                })
                provider_send_observations.append({**attempt, "socket_open_invoked": True})
            try:
                response = self._opener.open(request, *args, **kwargs)
            except urllib.error.HTTPError as error:
                observation = {
                    "http_status": error.code,
                    "content_type": error.headers.get("Content-Type", "application/json"),
                    "request_sha256": request_digest,
                    "http_error": type(error).__name__,
                }
                _update_ledger(run_id, {"state": "UPSTREAM_HTTP_ERROR", "response": observation})
                raise
            except Exception as error:
                _update_ledger(run_id, {
                    "state": "UPSTREAM_OPEN_UNKNOWN",
                    "response": {"error_type": type(error).__name__, "request_sha256": request_digest},
                })
                raise
            headers = response.headers
            _update_ledger(run_id, {"state": "UPSTREAM_RESPONSE_HEADERS", "response": {
                "http_status": getattr(response, "status", None),
                "content_type": headers.get("Content-Type", "application/json"),
                "request_sha256": request_digest,
            }})
            return ObservedResponse(response, attempt)

    gateway_module._no_redirect_handler = OneShotOpener
    return lambda: setattr(gateway_module, "_no_redirect_handler", original_factory)


def _assert_database_empty_before_migration(database_url: str) -> None:
    async def inspect() -> int:
        engine = create_engine(database_url)
        try:
            async with engine.connect() as connection:
                return int(await connection.scalar(text("""
                    SELECT count(*) FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname='public' AND c.relkind IN ('r','p','S','v','m','f')
                """)))
        finally:
            await engine.dispose()
    relation_count = asyncio.run(inspect())
    if relation_count != 0:
        raise ValueError("local smoke database must have no public relations before migration")


def _fingerprint() -> tuple[str, list[dict[str, object]]]:
    combined = hashlib.sha256()
    files: list[dict[str, object]] = []
    for relative in sorted(_FINGERPRINT_FILES):
        raw = (ROOT / relative).read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        files.append({"path": relative, "bytes": len(raw), "sha256": digest})
        combined.update(relative.encode("utf-8") + b"\0" + bytes.fromhex(digest))
    return combined.hexdigest(), files


def _prepare_output_capture_files(artifact: Path) -> tuple[Path, dict[str, Path]]:
    directory = artifact.with_suffix("").with_name(f"{artifact.stem}-capture")
    directory.mkdir(mode=0o700)
    paths = {
        "provider_sse": directory / "provider-response.sse",
        "provider_choice_0": directory / "provider-choice-0.txt",
        "adapter_observations": directory / "adapter-observations.jsonl",
        "bridge_observations": directory / "bridge-observations.jsonl",
        "app_server_assistant": directory / "app-server-assistant.txt",
        "sdk_assistant": directory / "sdk-assistant.txt",
        "sdk_result": directory / "sdk-result.txt",
        "bridge_raw_output": directory / "bridge-raw-output.txt",
        "postgres_raw_output": directory / "postgres-raw-output.txt",
        "fake_complete_sse": directory / "fake-complete-response.sse",
        "fake_incomplete_sse": directory / "fake-incomplete-response.sse",
    }
    for path in paths.values():
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)
    return directory, paths


def _capture_file_summary(path: Path) -> dict[str, object]:
    if not path.is_file():
        return {"path": path.relative_to(ROOT).as_posix(), "present": False}
    raw = path.read_bytes()
    return {
        "path": path.relative_to(ROOT).as_posix(),
        "present": bool(raw),
        "utf8_bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def _write_bounded_assistant_text(path: Path, value: str, limit: int = 65_536) -> dict[str, object]:
    full = value.encode("utf-8", "strict")
    prefix = full[:limit]
    while prefix:
        try:
            prefix.decode("utf-8", "strict")
            break
        except UnicodeDecodeError:
            prefix = prefix[:-1]
    descriptor = os.open(path, os.O_WRONLY | os.O_TRUNC)
    try:
        offset = 0
        while offset < len(prefix):
            offset += os.write(descriptor, prefix[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return {
        "path": path.relative_to(ROOT).as_posix(),
        "captured_utf8_bytes": len(prefix),
        "full_utf8_bytes": len(full),
        "sha256": hashlib.sha256(prefix).hexdigest(),
        "full_sha256": hashlib.sha256(full).hexdigest(),
        "truncated": len(prefix) < len(full),
    }


def _decode_utf8_prefix(value: bytes, limit: int = 65_536) -> str:
    prefix = value[:limit]
    while prefix:
        try:
            return prefix.decode("utf-8", "strict")
        except UnicodeDecodeError:
            prefix = prefix[:-1]
    return ""


def _write_captured_bytes(path: Path, value: bytes, limit: int) -> dict[str, object]:
    prefix = value[:limit]
    descriptor = os.open(path, os.O_WRONLY | os.O_TRUNC)
    try:
        offset = 0
        while offset < len(prefix):
            offset += os.write(descriptor, prefix[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return {
        "path": path.relative_to(ROOT).as_posix(),
        "captured_bytes": len(prefix),
        "full_bytes": len(value),
        "sha256": hashlib.sha256(prefix).hexdigest(),
        "full_sha256": hashlib.sha256(value).hexdigest(),
        "truncated": len(prefix) < len(value),
    }


def _historical_output_mismatch_summary() -> dict[str, object]:
    artifact = ROOT / "integration/runtime/artifacts/p6d-20261006T162211Z-18c1dad6-ollama-qwen-runtime.json"
    raw = artifact.read_bytes()
    value = json.loads(raw)
    turn = value["results"]["pinned_runtime_turn"]
    provider = turn["provider_sse_content_observation"]["choices"][0]
    result = turn["result_and_schema_validation"]
    send = turn["provider_send_observations"][0]
    call = turn["postgresql_call_and_permit"]
    same_identity = all((
        turn.get("task_id") == result.get("task_id") == call.get("task_id") == send.get("task_id"),
        turn.get("operation_id") == result.get("operation_id") == call.get("operation_id") == send.get("operation_id"),
        turn.get("attempt_id") == result.get("attempt_id") == call.get("attempt_id") == send.get("attempt_id"),
        turn.get("registry_id") == result.get("registry_id") == call.get("registry_id") == send.get("registry_id"),
        call.get("accounting_call_id") == send.get("accounting_call_id"),
        provider.get("index") == 0,
    ))
    if not same_identity:
        raise ValueError("historical provider and persisted output observations do not share the same Task/call identity")
    return {
        "artifact": artifact.relative_to(ROOT).as_posix(),
        "artifact_sha256": hashlib.sha256(raw).hexdigest(),
        "task_id": turn.get("task_id"),
        "operation_id": turn.get("operation_id"),
        "attempt_id": turn.get("attempt_id"),
        "registry_id": turn.get("registry_id"),
        "accounting_call_id": call.get("accounting_call_id"),
        "permit_id": call.get("permit_id"),
        "identity_matches_across_provider_and_database": same_identity,
        "provider_choice_0": {
            "content_delta_events": provider.get("content_delta_events"),
            "utf8_bytes": provider.get("content_utf8_bytes"),
            "sha256": provider.get("content_sha256"),
            "finish_reason": provider.get("finish_reason"),
            "sse_done_seen": turn["provider_sse_content_observation"].get("done_seen"),
        },
        "postgresql_bridge_raw_output": {
            "utf8_bytes": result.get("raw_output_bytes"),
            "sha256": result.get("output_hash"),
            "processing_state": result.get("processing_state"),
        },
        "historical_raw_provider_content_preserved": False,
        "observed_divergence_interval": "provider choice 0 to bridge-persisted raw_output; App Server and SDK intermediate hashes were not recorded",
        "specific_root_cause": "unresolved; hashes cannot reconstruct the lost raw content",
    }


def _output_delivery_sse_fixture(
    request: dict[str, object], assistant_text: str, response_id: str, usage: dict[str, int],
) -> tuple[list[bytes], dict[str, object]]:
    request_text = json.dumps(request.get("messages"), ensure_ascii=False, separators=(",", ":"))
    incomplete = "P6D_OUTPUT_DELIVERY_INCOMPLETE" in request_text
    transmitted_text = assistant_text[:-1] if incomplete and assistant_text.endswith("}") else assistant_text
    if incomplete and transmitted_text == assistant_text:
        raise ValueError("incomplete fixture could not remove its final JSON delimiter")

    target_delta_count = 265
    if len(transmitted_text) < target_delta_count:
        raise ValueError("output-delivery fixture cannot provide 265 non-empty content deltas")
    pieces = [
        transmitted_text[index * len(transmitted_text) // target_delta_count:
                         (index + 1) * len(transmitted_text) // target_delta_count]
        for index in range(target_delta_count)
    ]
    if any(not piece for piece in pieces):
        raise ValueError("output-delivery fixture produced an empty content delta")
    finish_same_frame = incomplete

    model = request.get("model")
    frames: list[dict[str, object]] = [{
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": 1,
        "model": model,
        "choices": [{
            "index": 0,
            "delta": {"reasoning_content": "격리된 reasoning 채널이며 답변 본문이 아니다."},
            "finish_reason": None,
        }],
    }]
    for index, piece in enumerate(pieces):
        final_piece = index == len(pieces) - 1
        choice: dict[str, object] = {
            "index": 0,
            "delta": {"content": piece},
            "finish_reason": "stop" if final_piece and finish_same_frame else None,
        }
        frames.append({
            "id": response_id,
            "object": "chat.completion.chunk",
            "created": 1,
            "model": model,
            "choices": [choice],
        })
    if not finish_same_frame:
        frames.append({
            "id": response_id,
            "object": "chat.completion.chunk",
            "created": 1,
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        })
    frames.append({
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": 1,
        "model": model,
        "choices": [],
        "usage": usage,
    })
    frame_bytes: list[bytes] = []
    for index, frame in enumerate(frames):
        line = b"data: " + json.dumps(
            frame, ensure_ascii=False, separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")
        frame_bytes.append(line + (b"\r\n\r\n" if index % 2 else b"\n\n"))
    frame_bytes.append(b"data: [DONE]\r\n\r\n")
    wire = b"".join(frame_bytes)

    boundaries = {min(offset, len(wire)) for offset in range(113, len(wire), 113)}
    utf8_split_offsets = [
        offset for offset in range(1, len(wire))
        if wire[offset] & 0xC0 == 0x80 and wire[offset - 1] & 0xC0 == 0xC0
    ]
    boundaries.update(utf8_split_offsets)
    cuts = [0, *sorted(offset for offset in boundaries if 0 < offset < len(wire)), len(wire)]
    network_chunks = [wire[left:right] for left, right in zip(cuts, cuts[1:])]
    observation: dict[str, object] = {
        "fixture_kind": "synthetic_multidelta_output_delivery",
        "scenario": "incomplete_policy_rejection" if incomplete else "complete_acceptance",
        "response_id": response_id,
        "choice_index": 0,
        "assistant_text": transmitted_text,
        "assistant_text_utf8_bytes": len(transmitted_text.encode("utf-8")),
        "assistant_text_sha256": hashlib.sha256(transmitted_text.encode("utf-8")).hexdigest(),
        "complete_fixture_text": assistant_text,
        "complete_fixture_sha256": hashlib.sha256(assistant_text.encode("utf-8")).hexdigest(),
        "content_delta_events": len(pieces),
        "finish_reason_same_frame_as_final_content": finish_same_frame,
        "finish_reason_separate_frame": not finish_same_frame,
        "reasoning_utf8_bytes": len("격리된 reasoning 채널이며 답변 본문이 아니다.".encode("utf-8")),
        "reasoning_sha256": hashlib.sha256("격리된 reasoning 채널이며 답변 본문이 아니다.".encode("utf-8")).hexdigest(),
        "event_count_including_usage": len(frames),
        "done_marker_sent": True,
        "line_endings": "alternating LF and CRLF",
        "provider_sse_wire_utf8_bytes": len(wire),
        "provider_sse_wire_sha256": hashlib.sha256(wire).hexdigest(),
        "http_chunked_transfer": True,
        "http_chunk_count": len(network_chunks),
        "http_chunk_boundaries_inside_utf8_codepoint": len(utf8_split_offsets),
        "http_chunk_utf8_split_offsets_sample": utf8_split_offsets[:12],
    }
    return network_chunks, observation


def _load_captured_actual_sse(path: Path) -> dict[str, object]:
    artifact_root = (ROOT / "integration/runtime/artifacts").resolve()
    artifact_path = path.resolve(strict=True)
    if not artifact_path.is_relative_to(artifact_root) or not artifact_path.is_file():
        raise ValueError("captured SSE replay must reference an artifact under integration/runtime/artifacts")
    artifact_bytes = artifact_path.read_bytes()
    artifact = json.loads(artifact_bytes)
    if not isinstance(artifact, dict):
        raise ValueError("captured actual run artifact has an invalid shape")
    turn = artifact.get("results", {}).get("pinned_runtime_turn") if isinstance(artifact.get("results"), dict) else None
    if (
        artifact.get("actual_provider_calls") != 1
        or artifact.get("upstream_generation_attempts") != 1
        or not isinstance(turn, dict)
        or turn.get("provider_response_is_valid_json") is not True
        or turn.get("task_state") != "FAILED"
        or turn.get("accepted_user_response") is not None
    ):
        raise ValueError("SSE replay source must be the one-call failed run with a complete JSON provider response")
    responses = turn.get("provider_response_observations")
    response = responses[0] if isinstance(responses, list) and responses else None
    if not isinstance(response, dict) or response.get("http_status") != 200 or response.get("finish_reason") != "stop":
        raise ValueError("captured provider response is not a completed HTTP 200 stop stream")
    diagnostics = artifact.get("output_observation_diagnostics")
    capture_directory = diagnostics.get("capture_directory") if isinstance(diagnostics, dict) else None
    response_wire = response.get("provider_response_wire")
    choice_record = response.get("provider_choice_0")
    if not isinstance(capture_directory, str) or not isinstance(response_wire, dict) or not isinstance(choice_record, dict):
        raise ValueError("actual artifact does not identify bounded SSE and choice-zero captures")
    capture_root = (ROOT / capture_directory).resolve(strict=True)
    if not capture_root.is_relative_to(artifact_root):
        raise ValueError("captured response directory escapes integration/runtime/artifacts")
    wire_record_path = response_wire.get("path")
    choice_record_path = choice_record.get("captured", {}).get("path") if isinstance(choice_record.get("captured"), dict) else None
    if not isinstance(wire_record_path, str) or not isinstance(choice_record_path, str):
        raise ValueError("captured actual response does not have bounded raw paths")
    wire_path = (ROOT / wire_record_path).resolve(strict=True)
    choice_path = (ROOT / choice_record_path).resolve(strict=True)
    if not wire_path.is_relative_to(capture_root) or not choice_path.is_relative_to(capture_root):
        raise ValueError("captured response paths escape their artifact capture directory")
    wire = wire_path.read_bytes()
    choice_text = choice_path.read_bytes()
    if len(wire) > 2_097_152 or response_wire.get("truncated") is True or response.get("response_instrumentation_truncated") is True:
        raise ValueError("captured provider SSE exceeds the 2 MiB complete-replay bound")
    if (
        len(wire) != response_wire.get("utf8_bytes")
        or hashlib.sha256(wire).hexdigest() != response_wire.get("sha256")
        or len(choice_text) != choice_record.get("utf8_bytes")
        or hashlib.sha256(choice_text).hexdigest() != choice_record.get("sha256")
        or choice_record.get("truncated") is True
    ):
        raise ValueError("captured provider bytes no longer match the immutable run artifact hashes")
    separator = re.compile(rb"\r\n\r\n|\n\n")
    frames: list[bytes] = []
    start = 0
    for match in separator.finditer(wire):
        frames.append(wire[start:match.end()])
        start = match.end()
    if start != len(wire) or not frames:
        raise ValueError("captured SSE has an incomplete final event frame")
    nested_observation = turn.get("provider_sse_content_observation")
    event_order = nested_observation.get("event_order") if isinstance(nested_observation, dict) else None
    if not isinstance(event_order, list) or len(event_order) != len(frames):
        raise ValueError("captured SSE frames do not match the observed provider event sequence")

    assistant_parts: list[str] = []
    assistant_delta_count = 0
    finish_reasons: list[str] = []
    done_seen = False
    for frame in frames:
        line = frame.rstrip(b"\r\n")
        if not line.startswith(b"data: "):
            raise ValueError("captured provider SSE frame has an unsupported field layout")
        payload = line[6:]
        if payload == b"[DONE]":
            done_seen = True
            continue
        try:
            item = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("captured provider SSE contains invalid JSON data") from error
        if not isinstance(item, dict):
            raise ValueError("captured provider SSE data must be JSON objects")
        choices = item.get("choices")
        if not isinstance(choices, list):
            raise ValueError("captured provider SSE choice shape is incomplete")
        for choice in choices:
            if not isinstance(choice, dict) or choice.get("index") != 0:
                continue
            delta = choice.get("delta")
            if isinstance(delta, dict):
                if any(isinstance(delta.get(key), str) and delta[key] for key in ("reasoning", "reasoning_content", "analysis")):
                    raise ValueError("captured response unexpectedly contains raw reasoning; refusing to replay it")
                content = delta.get("content")
                if isinstance(content, str) and content:
                    assistant_parts.append(content)
                    assistant_delta_count += 1
            if isinstance(choice.get("finish_reason"), str):
                finish_reasons.append(choice["finish_reason"])
    reconstructed = "".join(assistant_parts).encode("utf-8", "strict")
    if (
        not done_seen
        or finish_reasons != ["stop"]
        or assistant_delta_count != choice_record.get("delta_events")
        or reconstructed != choice_text
    ):
        raise ValueError("captured SSE assistant content or terminal events disagree with provider observations")
    try:
        parse_hekate_turn_output(reconstructed)
    except Exception as error:
        raise ValueError("captured provider choice 0 does not satisfy the strict HekateTurnOutput contract") from error

    measurement = turn.get("postgresql_call_and_permit", {}).get("measurement_data")
    measured_at = measurement.get("measured_at") if isinstance(measurement, dict) else None
    bridge_path = capture_root / "bridge-observations.jsonl"
    first_assistant_at: int | None = None
    for line in bridge_path.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            isinstance(item, dict)
            and item.get("kind") == "app_server_stream_event"
            and item.get("operation_id") == turn.get("operation_id")
            and item.get("channel") == "assistant"
            and type(item.get("received_at_unix_ms")) is int
        ):
            first_assistant_at = item["received_at_unix_ms"]
            break
    if not isinstance(measured_at, str) or first_assistant_at is None:
        raise ValueError("source artifact lacks the measured-send to first-assistant timing observation")
    measured_timestamp = datetime.fromisoformat(measured_at.replace("Z", "+00:00")).timestamp()
    first_delay = first_assistant_at / 1000 - measured_timestamp
    if first_delay < 0 or first_delay > 30:
        raise ValueError("recorded first-token delay is outside the bounded replay range")
    timing: list[float] = [first_delay]
    previous_timestamp: int | None = None
    for event in event_order:
        if not isinstance(event, dict) or type(event.get("received_monotonic_ns")) is not int:
            raise ValueError("provider SSE event is missing its recorded receive time")
        timestamp = event["received_monotonic_ns"]
        if previous_timestamp is not None:
            delay = (timestamp - previous_timestamp) / 1_000_000_000
            if delay < 0 or delay > 5:
                raise ValueError("recorded inter-event delay is outside the bounded replay range")
            timing.append(delay)
        previous_timestamp = timestamp
    if len(timing) != len(frames) or sum(timing) > 60:
        raise ValueError("captured SSE timing exceeds the bounded replay window")
    return {
        "source_artifact": artifact_path.relative_to(ROOT).as_posix(),
        "source_artifact_sha256": hashlib.sha256(artifact_bytes).hexdigest(),
        "source_run_id": artifact.get("run_id"),
        "source_task_id": turn.get("task_id"),
        "source_operation_id": turn.get("operation_id"),
        "source_accounting_call_id": turn.get("postgresql_call_and_permit", {}).get("accounting_call_id"),
        "source_provider_response_id": response.get("provider_response_id"),
        "provider_sse_path": wire_path.relative_to(ROOT).as_posix(),
        "provider_sse_sha256": hashlib.sha256(wire).hexdigest(),
        "provider_sse_utf8_bytes": len(wire),
        "provider_choice_0_path": choice_path.relative_to(ROOT).as_posix(),
        "provider_choice_0_sha256": hashlib.sha256(choice_text).hexdigest(),
        "provider_choice_0_utf8_bytes": len(choice_text),
        "provider_choice_0_delta_events": assistant_delta_count,
        "provider_choice_0_schema_valid": True,
        "finish_reason": "stop",
        "done_marker_seen": done_seen,
        "initial_delay_seconds": first_delay,
        "inter_event_delays_seconds": timing[1:],
        "total_replay_stream_seconds": sum(timing),
        "frames": frames,
    }


def _read_output_delivery_observations(path: Path) -> dict[str, dict[str, object]]:
    by_operation: dict[str, dict[str, object]] = {}
    if not path.is_file():
        return by_operation
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict) or not isinstance(item.get("operation_id"), str):
            continue
        operation_id = item["operation_id"]
        slot = by_operation.setdefault(operation_id, {})
        if item.get("kind") == "app_server_stream_snapshot":
            slot["app_server"] = item
        elif item.get("kind") == "sdk_turn_result":
            slot["sdk_turn"] = item
            app_server = item.get("app_server")
            if isinstance(app_server, dict):
                slot["app_server"] = app_server
    return by_operation


def _summarize_app_server_stream(
    observations_path: Path, operation_id: str, raw_path: Path,
) -> dict[str, object]:
    events: list[dict[str, object]] = []
    if observations_path.is_file():
        for line in observations_path.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (
                isinstance(item, dict)
                and item.get("kind") == "app_server_stream_event"
                and item.get("operation_id") == operation_id
            ):
                events.append(item)
    assistant = [item for item in events if item.get("channel") == "assistant"]
    reasoning = [item for item in events if item.get("channel") == "reasoning"]
    raw = raw_path.read_bytes() if raw_path.is_file() else b""
    return {
        "directly_observed": bool(events),
        "assistant": {
            "event_count": len(assistant),
            "utf8_bytes": len(raw) if raw else sum(
                int(item.get("utf8_bytes", 0)) for item in assistant
                if type(item.get("utf8_bytes")) is int
            ),
            "sha256": hashlib.sha256(raw).hexdigest() if raw else None,
            "delta_sha256_in_order": [item.get("sha256") for item in assistant],
            "raw_capture": _capture_file_summary(raw_path),
            "truncated": len(raw) >= 65_536,
        },
        "reasoning": {
            "event_count": len(reasoning),
            "utf8_bytes_by_delta": [item.get("utf8_bytes") for item in reasoning],
            "sha256_by_delta": [item.get("sha256") for item in reasoning],
            "raw_text_retained": False,
        },
        "event_order": [
            {
                key: item.get(key)
                for key in (
                    "sequence", "event_type", "channel", "run_id", "message_id",
                    "otid", "received_at_unix_ms", "utf8_bytes", "sha256",
                )
            }
            for item in events
        ],
        "terminal_order": [
            item.get("event_type") for item in events
            if item.get("event_type") in {"usage_statistics", "stop_reason"}
        ],
    }


async def _wait_for_app_server_terminal(
    observations_path: Path, operation_id: str, *, timeout_seconds: float = 90,
) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        if observations_path.is_file():
            for line in observations_path.read_text(encoding="utf-8").splitlines():
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (
                    isinstance(item, dict)
                    and item.get("kind") == "app_server_stream_event"
                    and item.get("operation_id") == operation_id
                    and item.get("event_type") == "stop_reason"
                ):
                    return True
        await asyncio.sleep(0.05)
    return False


def _diagnose_captured_actual_run(artifact_path: Path) -> dict[str, object]:
    replay = _load_captured_actual_sse(artifact_path)
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    turn = artifact["results"]["pinned_runtime_turn"]
    capture_dir = (ROOT / artifact["output_observation_diagnostics"]["capture_directory"]).resolve(strict=True)
    operation_id = str(turn["operation_id"])
    app_events: list[dict[str, object]] = []
    for line in (capture_dir / "bridge-observations.jsonl").read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get("kind") == "app_server_stream_event" and item.get("operation_id") == operation_id:
            app_events.append(item)
    assistant_events = [item for item in app_events if item.get("channel") == "assistant"]
    adapter = _summarize_adapter_observations(capture_dir / "adapter-observations.jsonl", operation_id)
    provider_text = (capture_dir / "provider-choice-0.txt").read_bytes()
    sdk_text = (capture_dir / "sdk-assistant.txt").read_bytes()
    postgres_text = (capture_dir / "postgres-raw-output.txt").read_bytes()
    sdk_result = next((item for item in _read_output_delivery_observations(
        capture_dir / "bridge-observations.jsonl",
    ).get(operation_id, {}).values() if isinstance(item, dict) and item.get("kind") == "sdk_turn_result"), None)
    sdk_summary = sdk_result if isinstance(sdk_result, dict) else {}
    provider_hashes = [
        event.get("choice_events", [{}])[0].get("content_delta_sha256")
        for event in turn["provider_sse_content_observation"].get("event_order", [])
        if isinstance(event, dict) and isinstance(event.get("choice_events"), list)
        and event["choice_events"] and event["choice_events"][0].get("channel") == "assistant"
    ]
    app_hashes = [item.get("sha256") for item in assistant_events]
    sdk_assistant = sdk_summary.get("sdk_assistant") if isinstance(sdk_summary.get("sdk_assistant"), dict) else {}
    db_result = turn.get("result_and_schema_validation", {})
    app_bytes = sum(int(item.get("utf8_bytes", 0)) for item in assistant_events)
    return {
        "source_artifact": replay["source_artifact"],
        "source_artifact_sha256": replay["source_artifact_sha256"],
        "task_id": turn.get("task_id"),
        "operation_id": operation_id,
        "accounting_call_id": turn.get("postgresql_call_and_permit", {}).get("accounting_call_id"),
        "provider_response_id": replay.get("source_provider_response_id"),
        "provider_choice_0": {
            "utf8_bytes": len(provider_text),
            "sha256": hashlib.sha256(provider_text).hexdigest(),
            "delta_count": len(provider_hashes),
            "complete_json": turn.get("provider_response_is_valid_json"),
        },
        "gateway": turn.get("provider_sse_content_observation", {}).get("choices", [{}])[0],
        "adapter": adapter,
        "app_server": {
            "assistant_event_count": len(assistant_events),
            "assistant_utf8_bytes_from_event_records": app_bytes,
            "delta_hashes_match_adapter": app_hashes == adapter.get("input_provider_content", {}).get("delta_sha256_in_order"),
            "delta_hashes_match_provider": app_hashes == provider_hashes,
            "terminal_events_after_sdk_result": [
                item.get("event_type") for item in app_events
                if item.get("event_type") in {"usage_statistics", "stop_reason"}
            ],
        },
        "sdk": {
            "assistant_event_count_at_result": sdk_assistant.get("event_count"),
            "assistant_utf8_bytes": len(sdk_text),
            "assistant_sha256": hashlib.sha256(sdk_text).hexdigest(),
            "sdk_result_bytes": sdk_summary.get("sdk_result"),
            "sdk_error_or_result_order": sdk_summary.get("sdk_message_order"),
            "sdk_text_is_provider_prefix": provider_text.startswith(sdk_text),
        },
        "postgresql": {
            "raw_output_utf8_bytes": len(postgres_text),
            "raw_output_sha256": hashlib.sha256(postgres_text).hexdigest(),
            "same_as_sdk_text": postgres_text == sdk_text,
            "processing_state": db_result.get("processing_state"),
            "task_state": turn.get("task_state"),
        },
        "first_divergence": "SDK assistant stream consumer: SDK returned after 240/270 assistant deltas; the App Server observer later received the remaining 30 deltas, usage_statistics, and stop_reason. PostgreSQL stored the same 669-byte SDK prefix.",
        "timeout_evidence": {
            "sdk_request_timeout_ms": 30_000,
            "assistant_stream_duration_seconds": replay.get("total_replay_stream_seconds"),
            "first_assistant_delay_seconds": replay.get("initial_delay_seconds"),
            "pinned_sdk_source": "bridge/letta/node_modules/@letta-ai/letta-agent-sdk/src/remote-turn-coordinator.ts:activateTurn",
            "timeout_contract": "RemoteTurnCoordinator starts a setTimeout(requestTimeoutMs) when the turn is activated and fails that turn at the configured timeout.",
        },
    }


def _summarize_adapter_observations(path: Path, operation_id: str) -> dict[str, object]:
    records: list[dict[str, object]] = []
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict) and item.get("operation_id") == operation_id:
                records.append(item)
    assistant = [item for item in records if item.get("channel") == "assistant"]
    reasoning = [item for item in records if item.get("channel") == "reasoning"]
    captured = "".join(
        item.get("assistant_content_delta", "")
        for item in assistant
        if isinstance(item.get("assistant_content_delta"), str)
    )
    captured_bytes = captured.encode("utf-8", "strict")
    input_delta_hashes = [item.get("input_sha256") for item in assistant]
    output_delta_hashes = [item.get("emitted_text_delta_sha256") for item in assistant]
    return {
        "directly_instrumented": True,
        "operation_id": operation_id,
        "event_count": len(assistant),
        "input_provider_content": {
            "utf8_bytes": len(captured_bytes),
            "sha256": hashlib.sha256(captured_bytes).hexdigest(),
            "delta_sha256_in_order": input_delta_hashes,
            "truncated": any(item.get("assistant_content_delta_truncated") is True for item in assistant),
        },
        "emitted_text_delta": {
            "utf8_bytes": len(captured_bytes),
            "sha256": hashlib.sha256(captured_bytes).hexdigest(),
            "delta_sha256_in_order": output_delta_hashes,
            "per_delta_matches_input": input_delta_hashes == output_delta_hashes,
            "truncated": any(item.get("assistant_content_delta_truncated") is True for item in assistant),
        },
        "reasoning": {
            "event_count": len(reasoning),
            "utf8_bytes_by_delta": [item.get("input_utf8_bytes") for item in reasoning],
            "sha256_by_delta": [item.get("input_sha256") for item in reasoning],
            "raw_text_retained": False,
        },
        "event_order": [
            {
                "sequence": item.get("sequence"),
                "stage": item.get("stage"),
                "channel": item.get("channel"),
                "content_index": item.get("content_index"),
                "message_id": item.get("message_id"),
                "accounting_call_id": item.get("accounting_call_id"),
                "provider_call_id": item.get("provider_call_id"),
            }
            for item in records
        ],
        "accounting_call_ids": sorted({item.get("accounting_call_id") for item in records if item.get("accounting_call_id")}),
        "provider_response_ids": [item.get("provider_call_id") for item in records if item.get("provider_call_id")],
    }


def _build_actual_output_observation(
    turn: dict[str, object], capture_paths: dict[str, Path],
) -> dict[str, object]:
    operation_id = turn.get("operation_id")
    if not isinstance(operation_id, str):
        return {"complete": False, "reason": "operation_identity_missing"}
    provider_responses = turn.get("provider_response_observations")
    response = provider_responses[0] if isinstance(provider_responses, list) and provider_responses else {}
    gateway_observation = turn.get("provider_sse_content_observation")
    gateway_choices = gateway_observation.get("choices") if isinstance(gateway_observation, dict) else None
    gateway_choice = next(
        (item for item in gateway_choices if isinstance(item, dict) and item.get("index") == 0),
        None,
    ) if isinstance(gateway_choices, list) else None
    bridge_observations = _read_output_delivery_observations(capture_paths["bridge_observations"])
    bridge_operation = bridge_observations.get(operation_id, {})
    app_server_snapshot = bridge_operation.get("app_server")
    sdk_turn = bridge_operation.get("sdk_turn")
    app_server_stream = _summarize_app_server_stream(
        capture_paths["bridge_observations"], operation_id, capture_paths["app_server_assistant"],
    )
    app_server_assistant = app_server_stream.get("assistant")
    sdk_assistant = sdk_turn.get("sdk_assistant") if isinstance(sdk_turn, dict) else None
    sdk_result = sdk_turn.get("sdk_result") if isinstance(sdk_turn, dict) else None
    bridge_raw = sdk_turn.get("bridge_raw_output") if isinstance(sdk_turn, dict) else None
    adapter = _summarize_adapter_observations(capture_paths["adapter_observations"], operation_id)
    result = turn.get("result_and_schema_validation")
    result = result if isinstance(result, dict) else {}
    provider_choice = response.get("provider_choice_0") if isinstance(response, dict) else None
    if not isinstance(provider_choice, dict) and isinstance(gateway_choice, dict):
        provider_choice = {
            "delta_events": gateway_choice.get("content_delta_events"),
            "utf8_bytes": gateway_choice.get("content_utf8_bytes"),
            "sha256": gateway_choice.get("content_sha256"),
            "captured": _capture_file_summary(capture_paths["provider_choice_0"]),
            "truncated": False,
        }
    expected_bytes = provider_choice.get("utf8_bytes") if isinstance(provider_choice, dict) else None
    expected_hash = provider_choice.get("sha256") if isinstance(provider_choice, dict) else None
    expected_delta_count = provider_choice.get("delta_events") if isinstance(provider_choice, dict) else None
    gateway_event_order = gateway_observation.get("event_order") if isinstance(gateway_observation, dict) else None
    gateway_event_order = gateway_event_order if isinstance(gateway_event_order, list) else []
    provider_delta_hashes = [
        event["choice_events"][0].get("content_delta_sha256")
        for event in gateway_event_order
        if isinstance(event, dict)
        and isinstance(event.get("choice_events"), list)
        and event["choice_events"]
        and event["choice_events"][0].get("channel") == "assistant"
    ]
    app_server_stage = dict(app_server_assistant) if isinstance(app_server_assistant, dict) else {}
    app_server_stage["matches_provider_content_by_delta"] = bool(
        app_server_stream.get("directly_observed") is True
        and app_server_stage.get("event_count") == expected_delta_count
        and app_server_stage.get("utf8_bytes") == expected_bytes
        and app_server_stage.get("delta_sha256_in_order") == provider_delta_hashes
    )
    app_server_stage["aggregate_raw_hash_available"] = app_server_stage.get("sha256") is not None
    stages: dict[str, object] = {
        "ollama_choice_0_content": {
            "event_count": provider_choice.get("delta_events") if isinstance(provider_choice, dict) else None,
            "utf8_bytes": expected_bytes,
            "sha256": expected_hash,
            "captured": provider_choice.get("captured") if isinstance(provider_choice, dict) else None,
            "truncated": provider_choice.get("truncated") if isinstance(provider_choice, dict) else None,
        },
        "gateway_received_and_yielded_choice_0": {
            "event_count": gateway_choice.get("content_delta_events") if isinstance(gateway_choice, dict) else None,
            "utf8_bytes": gateway_choice.get("content_utf8_bytes") if isinstance(gateway_choice, dict) else None,
            "sha256": gateway_choice.get("content_sha256") if isinstance(gateway_choice, dict) else None,
            "event_order": gateway_observation.get("event_order") if isinstance(gateway_observation, dict) else None,
        },
        "provider_adapter_received_content": adapter.get("input_provider_content"),
        "provider_adapter_emitted_text_delta": adapter.get("emitted_text_delta"),
        "app_server_assistant_stream": app_server_stage,
        "sdk_assistant_messages": sdk_assistant,
        "sdk_result_text": sdk_result,
        "bridge_raw_output": bridge_raw,
        "postgresql_raw_output": {
            "utf8_bytes": result.get("raw_output_bytes"),
            "sha256": result.get("output_hash"),
            "capture": result.get("raw_output_capture"),
        },
    }
    order = (
        "ollama_choice_0_content",
        "gateway_received_and_yielded_choice_0",
        "provider_adapter_received_content",
        "provider_adapter_emitted_text_delta",
        "app_server_assistant_stream",
        "sdk_assistant_messages",
        "sdk_result_text",
        "bridge_raw_output",
        "postgresql_raw_output",
    )
    first_divergence = None
    for name in order:
        observed = stages.get(name)
        if not isinstance(observed, dict):
            first_divergence = f"{name}:observation_missing"
            break
        if name == "app_server_assistant_stream":
            if observed.get("matches_provider_content_by_delta") is not True:
                first_divergence = f"{name}:content_mismatch"
                break
            continue
        if observed.get("utf8_bytes") != expected_bytes or observed.get("sha256") != expected_hash:
            first_divergence = f"{name}:content_mismatch"
            break
    provider_content_path = capture_paths["provider_choice_0"]
    provider_content = provider_content_path.read_bytes() if provider_content_path.exists() else b""
    try:
        provider_json_value = json.loads(provider_content.decode("utf-8", "strict"))
        provider_json_parse = {"valid_json": True, "top_level_type": type(provider_json_value).__name__}
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        provider_json_parse = {"valid_json": False, "error_type": type(error).__name__}
    provider_send = (turn.get("provider_send_observations") or turn.get("provider_authorization_attempts") or [{}])[0]
    provider_response_id = (
        response.get("provider_response_id") if isinstance(response, dict) else None
    ) or (gateway_observation.get("provider_call_id") if isinstance(gateway_observation, dict) else None)
    identities = {
        "task_id": turn.get("task_id"),
        "operation_id": operation_id,
        "attempt_id": turn.get("attempt_id"),
        "registry_id": turn.get("registry_id"),
        "accounting_call_id": turn.get("postgresql_call_and_permit", {}).get("accounting_call_id")
            if isinstance(turn.get("postgresql_call_and_permit"), dict) else None,
        "permit_id": turn.get("postgresql_call_and_permit", {}).get("permit_id")
            if isinstance(turn.get("postgresql_call_and_permit"), dict) else None,
        "provider_response_id": provider_response_id,
        "provider_identity_source": "provider_send_observation" if turn.get("provider_send_observations") else "pre_send_permit_authorization_and_single_fake_request",
        "adapter_operation_id": adapter.get("operation_id"),
        "adapter_accounting_call_ids": adapter.get("accounting_call_ids"),
        "adapter_provider_response_ids": adapter.get("provider_response_ids"),
        "bridge_operation_id": sdk_turn.get("operation_id") if isinstance(sdk_turn, dict) else None,
    }
    identity_matches = bool(
        identities["task_id"] == provider_send.get("task_id")
        and identities["operation_id"] == provider_send.get("operation_id") == identities["adapter_operation_id"] == identities["bridge_operation_id"]
        and identities["attempt_id"] == provider_send.get("attempt_id")
        and identities["registry_id"] == provider_send.get("registry_id")
        and identities["accounting_call_id"] == provider_send.get("accounting_call_id")
        and identities["permit_id"] == provider_send.get("permit_id")
        and identities["provider_response_id"] in (identities["adapter_provider_response_ids"] or [])
        and result.get("task_id") == identities["task_id"]
        and result.get("operation_id") == identities["operation_id"]
        and result.get("attempt_id") == identities["attempt_id"]
        and result.get("registry_id") == identities["registry_id"]
    )
    return {
        "complete": first_divergence is None and identity_matches,
        "identities": identities,
        "identity_matches_across_provider_adapter_bridge_and_database": identity_matches,
        "assistant_content_stages": stages,
        "ordered_boundaries": list(order),
        "first_divergence": first_divergence,
        "provider_choice_0_json_parse": provider_json_parse,
        "event_and_terminal_order": {
            "provider_sse": response.get("event_order") if isinstance(response, dict) and response.get("event_order") else gateway_observation.get("event_order") if isinstance(gateway_observation, dict) else None,
            "gateway_sse": gateway_observation.get("event_order") if isinstance(gateway_observation, dict) else None,
            "provider_adapter": adapter.get("event_order"),
            "app_server": app_server_stream.get("event_order"),
            "sdk": sdk_turn.get("sdk_message_order") if isinstance(sdk_turn, dict) else None,
        },
        "app_server_stream_observer": app_server_stream,
        "reasoning_summary_only": {
            "gateway": gateway_choice.get("reasoning_channels") if isinstance(gateway_choice, dict) else None,
            "provider_adapter": adapter.get("reasoning"),
            "app_server": app_server_stream.get("reasoning"),
            "sdk": sdk_turn.get("sdk_reasoning") if isinstance(sdk_turn, dict) else None,
        },
        "capture_files": {name: _capture_file_summary(path) for name, path in capture_paths.items()},
    }


def _write_test_config(config_dir: Path, candidate, execution_profile, prices, run_id: str, *, local_smoke: bool) -> None:
    config_dir.mkdir(parents=True)
    identity_name = f"phase6d-qwen-{run_id}" if local_smoke else "phase6c-qwen-probe"
    identity = {
        "principal_id": f"principal:{identity_name}",
        "scope_id": identity_name,
        "policy_version": f"{identity_name}-policy-v1",
        "authz_epoch": 1,
    }
    (config_dir / "local.yaml").write_text(yaml.safe_dump({"identity": identity}, sort_keys=False), encoding="utf-8")
    (config_dir / "policy.yaml").write_text(yaml.safe_dump({"limits": {
        "task_budget_usd": "1.00",
        "system_daily_budget_usd": "10.00",
        "task_deadline_seconds": 240,
    }}, sort_keys=False), encoding="utf-8")
    (config_dir / "models.yaml").write_text(yaml.safe_dump({"hekate": {
        "execution_profile": "qwen35_native_json_schema_test_v2",
        "profile_id": execution_profile.profile_id,
        "agent_system_prompt": candidate.agent_system_prompt,
        "model": f"openai-compatible/{candidate.model}",
        "provider_model": candidate.model,
        "max_input_tokens": candidate.max_input_tokens,
        "max_output_tokens": candidate.max_output_tokens,
        "max_compaction_calls": 0,
        "model_revision": candidate.model_manifest_digest,
        "context_window_tokens": candidate.context_window_tokens,
        "letta_context_estimator_tokens": candidate.letta_context_estimator_tokens,
    }}, sort_keys=False), encoding="utf-8")
    (config_dir / "pricing.yaml").write_text(yaml.safe_dump({
        "version": prices.version,
        "effective_at": prices.effective_at,
        "prices": {candidate.model: {
            "input_usd_per_million": str(prices.input_usd_per_million),
            "output_usd_per_million": str(prices.output_usd_per_million),
        }},
    }, sort_keys=False), encoding="utf-8")


async def _run(
    database_url: str, node: str, image: str, node_archive: Path, artifact: Path,
    run_id: str, *, local_smoke: bool, output_delivery_reproduction: bool = False,
    local_authorization: dict[str, object] | None = None,
    output_observation_diagnostics: bool = False,
    captured_sse_replay: dict[str, object] | None = None,
    bridge_request_timeout_ms: int = 240_000,
) -> dict[str, object]:
    captured_replay = captured_sse_replay is not None
    parsed = make_url(database_url)
    report: dict[str, object] = {
        "schema_version": "1",
        "probe": "phase6d-captured-actual-sse-replay" if captured_replay else "phase6d-output-observed-local-qwen" if output_observation_diagnostics and local_smoke else "phase6d-output-delivery-path" if output_delivery_reproduction else "phase6c-ollama-qwen-profile-runtime",
        "run_id": run_id,
        **({"local_attempt_authorization": local_authorization} if local_authorization is not None else {}),
        **({"captured_actual_sse_replay": {key: value for key, value in captured_sse_replay.items() if key != "frames"}} if captured_replay else {}),
        "executed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "command": {
            "entrypoint": "scripts/phase6c_ollama_qwen_probe.py",
            "arguments": [
                "--database-url", "<fresh loopback PostgreSQL URL; credentials redacted>",
                "--node-bin", node,
                "--node-archive", str(node_archive),
                "--image", image,
            ] + (["--execute-local-ollama"] if local_smoke else [])
              + (["--authorized-native-schema-followup"] if isinstance(local_authorization, dict) and local_authorization.get("type") == "user_authorized_native_json_schema_followup" else [])
              + (["--authorized-output-observation-attempt"] if isinstance(local_authorization, dict) and local_authorization.get("type") == "user_authorized_new_independent_output_observed_attempt" else [])
              + (["--authorized-output-observation-followup"] if isinstance(local_authorization, dict) and local_authorization.get("type") == "user_authorized_fresh_output_observed_followup" else [])
              + (["--output-delivery-reproduction"] if output_delivery_reproduction else [])
              + (["--replay-captured-sse", str(captured_sse_replay.get("source_artifact")),
                  "--bridge-request-timeout-ms", str(bridge_request_timeout_ms)] if captured_replay else []),
            "execution_mode": (
                "pinned_fake_provider_captured_actual_sse_replay" if captured_replay else
                "one_local_ollama_output_observation_diagnostic" if output_observation_diagnostics and local_smoke else
                "one_local_ollama_smoke" if local_smoke else
                "pinned_fake_provider_output_delivery" if output_delivery_reproduction else "pinned_fake_provider"
            ),
        },
        "branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip(),
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "database_target": {"host": parsed.host, "port": parsed.port, "database": parsed.database},
        "execution_mode": (
            "pinned_fake_provider_captured_actual_sse_replay" if captured_replay else
            "one_local_ollama_output_observation_diagnostic" if output_observation_diagnostics and local_smoke else
            "one_local_ollama_smoke" if local_smoke else
            "pinned_fake_provider_output_delivery" if output_delivery_reproduction else "pinned_fake_provider"
        ),
        "real_provider_calls": 0,
        "fake_provider_requests": 0,
        "production_dispatch": "blocked",
        "deferred_boundaries": {
            "g7_same_execution_resume": "deferred",
            "g8_production_exact_request_validation": "pending actual provider usage and production approval",
            "letta_memory_projection": "not executed in Phase 6C",
            "production_dispatch": "blocked",
        },
        "overall_status": "blocked",
        "results": {},
        "unexecuted": [
            "No Ollama inference request or model load was made; the actual captured SSE was replayed through the pinned fake provider."
            if captured_replay else
            "No Ollama inference request or model load was made; output-delivery fixture uses only the pinned fake provider."
            if output_delivery_reproduction else
            "No Ollama inference request was made by the default fake-provider run; no model was loaded or changed."
            if not local_smoke else "The one authorized local Qwen test has not completed.",
            "The fake-provider usage record is synthetic and does not verify Qwen output usage semantics.",
            "G8 full-request tokenization beyond the observed single request remains unresolved.",
        ],
    }
    if captured_replay:
        report["source_run_reanalysis"] = _diagnose_captured_actual_run(
            ROOT / str(captured_sse_replay["source_artifact"]),
        )
    if output_delivery_reproduction:
        report["historical_output_mismatch"] = _historical_output_mismatch_summary()
    stage = "setup"
    engine = bridge = sandbox = fake = gateway_server = gateway_task = gateway_app = None
    original_authorize_provider_call = gateway_module.authorize_provider_call
    original_consume_call_permit = gateway_module.consume_call_permit
    restore_local_upstream_guard = None
    provider_authorization_attempts: list[dict[str, object]] = []
    consumed_permits: list[dict[str, object]] = []
    provider_send_observations: list[dict[str, object]] = []
    provider_response_observations: list[dict[str, object]] = []
    worker_task: asyncio.Task | None = None
    stop = asyncio.Event()
    old_model = p3.FAKE_MODEL
    turn_contract_observations: list[dict[str, object]] = []
    captured_bodies: list[dict[str, object]] = []
    fake_request_shapes: list[dict[str, object]] = []
    fake_fixture_failures: list[dict[str, str]] = []
    fake_output_delivery_fixtures: list[dict[str, object]] = []
    captured_sse_replay_observations: list[dict[str, object]] = []
    temporary = tempfile.TemporaryDirectory(prefix=f"{run_id}-")
    state_path = Path(temporary.name)
    capture_dir: Path | None = None
    capture_paths: dict[str, Path] = {}
    if output_observation_diagnostics or output_delivery_reproduction or captured_replay:
        capture_dir, capture_paths = _prepare_output_capture_files(artifact)
        output_observation_file = capture_paths["bridge_observations"]
        report["output_observation_diagnostics"] = {
            "enabled": True,
            "raw_scope": "public arithmetic Task only; assistant output capped at 64 KiB and SSE at 2 MiB",
            "reasoning_capture": "length, digest, channel and event order only; no reasoning text retained",
            "files": {name: path.relative_to(ROOT).as_posix() for name, path in capture_paths.items()},
            "capture_directory": capture_dir.relative_to(ROOT).as_posix(),
        }
    else:
        output_observation_file = state_path / "output-delivery-observations.jsonl"
    try:
        stage = "validate_candidate_and_read_live_metadata"
        if parsed.host not in {"127.0.0.1", "localhost"} or not str(parsed.database).startswith("hekate_phase6c_"):
            raise ValueError("probe requires a fresh loopback database named hekate_phase6c_*")
        candidate = load_qwen_candidate_profile()
        validate_qwen_candidate_profile(candidate)
        execution_profile, prices = qwen35_native_json_schema_test_execution_profile(candidate)
        output_schema, output_schema_digest = hekate_turn_output_schema()
        checked_in_schema = json.loads(
            (ROOT / "contracts/generated/hekate-turn-output.v1.schema.json").read_text(encoding="utf-8"),
        )
        if output_schema != checked_in_schema:
            raise ValueError("native response_format source differs from the checked-in generated Hekate schema")
        if output_delivery_reproduction or captured_replay:
            report["live_ollama_metadata"] = {
                "observed": False,
                "reason": "captured-response fake-provider replay; no Ollama connection or generation",
            }
        else:
            report["live_ollama_metadata"] = _verify_live_ollama_metadata(candidate)
        grammar_tools = {
            name: shutil.which(name)
            for name in ("go", "llama-server", "llama-cli", "json_schema_to_grammar.py")
        }
        report["native_schema_preflight"] = {
            "output_contract": "hekate_turn_output_v1",
            "schema_source": "Python HekateTurnOutput export_schemas()",
            "checked_in_generated_schema_matches": True,
            "schema_sha256": output_schema_digest,
            "candidate_profile_id": candidate.profile_id,
            "candidate_profile_digest": candidate.content_digest,
            "execution_profile_id": execution_profile.profile_id,
            "execution_profile_digest": execution_profile.content_digest,
            "sdk_output_format": candidate.sdk_output_format,
            "temperature": 0,
            "tools_enabled": False,
            "response_format_field_in_prompt_token_measurement": False,
            "schema_remains_embedded_in_measured_prompt": True,
            "grammar_control_tokens": "not separately measured or claimed",
            "offline_schema_to_grammar_tools": grammar_tools,
            "offline_schema_to_grammar_available": any(grammar_tools.values()),
            "offline_schema_to_grammar_executed": False,
        }
        report["qwen_candidate"] = {
            "model": candidate.model,
            "ollama_version": candidate.ollama_version,
            "ollama_source_revision": candidate.ollama_source_revision,
            "manifest_digest": candidate.model_manifest_digest,
            "gguf_blob_digest": candidate.gguf_model_blob_digest,
            "architecture": candidate.model_architecture,
            "parameter_count": candidate.model_parameter_count,
            "quantization": candidate.model_quantization,
            "metadata_context_tokens": candidate.model_metadata_context_tokens,
            "effective_context_policy_tokens": candidate.context_window_tokens,
            "letta_context_estimator_tokens": candidate.letta_context_estimator_tokens,
            "effective_context_verified": candidate.runtime_context_verified,
            "max_input_tokens": candidate.max_input_tokens,
            "max_output_tokens": candidate.max_output_tokens,
            "sdk_output_format": candidate.sdk_output_format,
            "native_response_format": "json_schema",
            "native_output_contract": "hekate_turn_output_v1",
            "native_output_schema_sha256": output_schema_digest,
            "temperature": 0,
            "request_support": list(candidate.request_support),
            "tokenizer_asset_sha256": candidate.tokenizer_asset_sha256,
            "tokenizer_implementation": candidate.tokenizer_implementation,
            "agent_system_prompt_sha256": candidate.agent_system_prompt_sha256,
            "renderer": candidate.renderer_id,
            "renderer_source_sha256": candidate.renderer_sha256,
            "request_normalizer_sha256": candidate.request_normalizer_sha256,
            "profile_digest": candidate.content_digest,
            "native_execution_profile_digest": execution_profile.content_digest,
            "metadata_identity_verified": candidate.metadata_identity_verified,
            "offline_reference_verified": candidate.offline_reference_verified,
            "inference_usage_verified": candidate.inference_usage_verified,
            "dispatch_approved": candidate.operational_dispatch_approved,
        }
        reference_path = ROOT / "integration/runtime/fixtures/qwen35-renderer-tokenizer-v1.json"
        reference_bytes = reference_path.read_bytes()
        reference = json.loads(reference_bytes)
        report["offline_reference"] = {
            "fixture": reference_path.relative_to(ROOT).as_posix(),
            "fixture_sha256": hashlib.sha256(reference_bytes).hexdigest(),
            "model_identity": reference.get("identity"),
            "tokenizer_reference": reference.get("tokenizer_reference"),
            "renderer_reference": reference.get("renderer_reference"),
            "golden_token_vectors": len(reference.get("raw_token_vectors", [])),
            "golden_rendered_prompts": len(reference.get("rendered_prompts", [])),
            "comparison_test": "tests.unit.test_qwen_ollama.QwenOllamaProfileTests.test_installed_tokenizer_and_pinned_renderer_goldens",
        }
        p3.FAKE_MODEL = candidate.model

        stage = "verify_pinned_runtime_and_build_bridge"
        runtime_lock = p3._locked_runtime(image, node, node_archive)
        p1.build_bridge(node)
        report["runtime"] = runtime_lock
        report["execution_profile"] = {
            "profile_id": execution_profile.profile_id,
            "profile_digest": execution_profile.content_digest,
            "sdk_output_format": False,
            "native_schema_sha256": output_schema_digest,
            "temperature": 0,
            "request_profile_policy": "server-generated HekateTurnOutput v1 JSON Schema is inserted before measurement and permit binding",
        }
        report["execution_limits"] = {
            "task_deadline_seconds": 240,
            "bridge_request_timeout_ms": bridge_request_timeout_ms,
            "provider_transport_timeout_seconds": 60,
            "max_input_tokens": candidate.max_input_tokens,
            "max_output_tokens": candidate.max_output_tokens,
            "provider_context_gate_tokens": candidate.context_window_tokens,
            "max_compaction_calls": 0,
            "automatic_provider_retry": False,
            "critic_or_continuation": False,
            "tools": False,
            "thinking": False,
            "sdk_native_output_format": False,
        }

        stage = "connect_to_fresh_postgres"
        engine = create_engine(database_url)
        factory = create_uow_factory(engine)
        async with engine.connect() as connection:
            stage = "read_postgres_version"
            postgres_version = await connection.scalar(text("SHOW server_version"))
            stage = "read_migration_head"
            migration_head = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
            stage = "verify_database_is_fresh"
            existing_tasks = int(await connection.scalar(text("SELECT count(*) FROM tasks")))
        if migration_head != "0013_provider_token_measurement" or existing_tasks != 0:
            raise ValueError("Phase 6C database must be fresh and at the current Phase 6B migration head")
        report["database"] = {
            "postgres_version": postgres_version,
            "migration_head": migration_head,
            "fresh_database_before_probe": True,
        }

        identity_name = f"phase6d-qwen-{run_id}" if local_smoke else "phase6c-qwen-probe"
        principal_id = f"principal:{identity_name}"
        scope_id = identity_name
        policy_version = f"{identity_name}-policy-v1"
        stage = "insert_test_authorization_scope"
        async with factory() as uow:
            await uow.tasks.insert_scope(AuthorizationSnapshot(
                scope=ScopeId(scope_id),
                principal_id=PrincipalId(principal_id),
                policy_version=policy_version,
                authz_epoch=1,
            ))
            await uow.commit()

        config_dir = state_path / "config"
        _write_test_config(config_dir, candidate, execution_profile, prices, run_id, local_smoke=local_smoke)
        env = os.environ.copy()
        env.update({
            "HEKATE_DATABASE_URL": database_url,
            "HEKATE_NODE_BIN": node,
            "HEKATE_BRIDGE_ENTRY": str(ROOT / "bridge/letta/dist/main.js"),
            "HEKATE_WORKER_ID": f"{run_id}-worker",
            "HEKATE_RUNTIME_MODE": "test",
            "HEKATE_CONFIG_DIR": str(config_dir),
            "HEKATE_MEMORY_PROJECTION_ENABLED": "false",
        })
        env["PATH"] = f"{Path(node).parent}:{env.get('PATH', '')}"

        stage = "start_fake_provider_or_local_upstream"
        if local_smoke:
            upstream_base_url = "http://127.0.0.1:19191"
            upstream_api_key = "not-needed-local-only"
        else:
            fake = p3.FakeProvider()

            def check_response(request: dict[str, object]) -> str:
                messages = request.get("messages")
                tools = request.get("tools")
                response_format = request.get("response_format")
                output_limit = request.get("max_tokens")
                if output_limit is None:
                    output_limit = request.get("max_completion_tokens")
                fake_request_shapes.append({
                    "model": request.get("model"),
                    "max_tokens": request.get("max_tokens"),
                    "max_completion_tokens": request.get("max_completion_tokens"),
                    "output_limit_tokens": output_limit,
                    "message_roles": [
                        message.get("role") for message in messages if isinstance(message, dict)
                    ] if isinstance(messages, list) else [],
                    "message_count": len(messages) if isinstance(messages, list) else 0,
                    "tool_names": [
                        tool.get("function", {}).get("name")
                        for tool in tools
                        if isinstance(tool, dict) and isinstance(tool.get("function"), dict)
                    ] if isinstance(tools, list) else [],
                    "response_format_type": response_format.get("type")
                        if isinstance(response_format, dict) else None,
                    "response_schema_sha256": hashlib.sha256(json.dumps(
                        response_format.get("json_schema", {}).get("schema")
                        if isinstance(response_format, dict) and isinstance(response_format.get("json_schema"), dict)
                        else None,
                        ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
                    ).encode("utf-8")).hexdigest(),
                    "temperature": request.get("temperature"),
                    "store_field_forwarded": "store" in request,
                    "task_capsule_present": any(
                        isinstance(message, dict) and isinstance(message.get("content"), str)
                        and "Task Capsule JSON:\n" in message["content"]
                        for message in messages
                    ) if isinstance(messages, list) else False,
                    "output_schema_in_prompt": any(
                        isinstance(message, dict) and isinstance(message.get("content"), str)
                        and "HEKATE server output contract (generated from the strict Python HekateTurnOutput model):" in message["content"]
                        for message in messages
                    ) if isinstance(messages, list) else False,
                    "structured_output_grammar_sent": isinstance(response_format, dict),
                })
                if (
                    request.get("model") != candidate.model
                    or request.get("reasoning_effort") != "none"
                    or request.get("temperature") != 0
                    or request.get("tools") not in (None, [])
                    or response_format != {
                        "type": "json_schema", "json_schema": {"schema": output_schema},
                    }
                    or type(output_limit) is not int
                    or output_limit < 4
                    or output_limit > candidate.max_output_tokens
                ):
                    raise ValueError("fake provider rejected a request outside the fixed Qwen smoke candidate")
                captured_bodies.append(request)
                try:
                    if captured_replay:
                        serialized_messages = json.dumps(messages, ensure_ascii=False, separators=(",", ":"))
                        if "17과 25의 합을 한국어 한 문장으로 답해줘." not in serialized_messages:
                            raise ValueError("captured response replay requires the public arithmetic question")
                    output = p3b._output_from_request(request, turn_contract_observations)
                    if output_delivery_reproduction and "P6D_OUTPUT_DELIVERY_" in str(request.get("messages")):
                        value = json.loads(output)
                        answer = '합은 42입니다. "그대로"\n마지막 줄도 보존합니다.'
                        value["proposal"]["answer"] = answer
                        rendered = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
                        padding_bytes = 760 - len(rendered.encode("utf-8"))
                        if padding_bytes < 0:
                            raise ValueError("synthetic valid output base is larger than the historical 760-byte observation")
                        value["proposal"]["answer"] += "." * padding_bytes
                        output = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
                        if len(output.encode("utf-8")) != 760:
                            raise ValueError("synthetic valid output does not match the historical 760-byte stream size")
                    return output
                except Exception as error:
                    fake_fixture_failures.append({
                        "error_type": type(error).__name__,
                        "reason": str(error)[:240],
                    })
                    raise

            fake.set_response_factory(check_response)
            if output_delivery_reproduction:
                def stream_output_delivery_fixture(
                    request: dict[str, object], assistant_text: str,
                    response_id: str, usage: dict[str, int],
                ) -> tuple[list[bytes], dict[str, object]]:
                    chunks, observation = _output_delivery_sse_fixture(
                        request, assistant_text, response_id, usage,
                    )
                    fake_output_delivery_fixtures.append(observation)
                    capture_path = capture_paths[
                        "fake_incomplete_sse"
                        if observation.get("scenario") == "incomplete_policy_rejection"
                        else "fake_complete_sse"
                    ]
                    _write_captured_bytes(capture_path, b"".join(chunks), 2_097_152)
                    return chunks, observation

                fake.set_sse_response_factory(stream_output_delivery_fixture)
            elif captured_replay:
                frames = captured_sse_replay.get("frames")
                inter_event_delays = captured_sse_replay.get("inter_event_delays_seconds")
                initial_delay = captured_sse_replay.get("initial_delay_seconds")
                source_choice_path = ROOT / str(captured_sse_replay["provider_choice_0_path"])
                source_choice = source_choice_path.read_bytes()
                if not isinstance(frames, list) or not isinstance(inter_event_delays, list) or type(initial_delay) not in {int, float}:
                    raise ValueError("captured replay frames or timing metadata are invalid")
                body_delays = [0.0, *(float(value) for value in inter_event_delays)]
                total_replay_seconds = float(initial_delay) + sum(body_delays)
                if len(frames) != len(body_delays) or any(not isinstance(frame, bytes) for frame in frames):
                    raise ValueError("captured replay frame and timing counts differ")

                def stream_captured_actual_response(
                    request: dict[str, object], assistant_text: str,
                    response_id: str, usage: dict[str, int],
                ) -> tuple[list[bytes | tuple[bytes, float]], dict[str, object]]:
                    if len(fake.requests) != 1 or captured_sse_replay_observations:
                        raise ValueError("captured actual response is restricted to one fake upstream request")
                    messages = request.get("messages")
                    request_text = json.dumps(messages, ensure_ascii=False, separators=(",", ":"))
                    schema_observation = fake_request_shapes[-1] if fake_request_shapes else {}
                    if (
                        "17과 25의 합을 한국어 한 문장으로 답해줘." not in request_text
                        or request.get("model") != candidate.model
                        or request.get("temperature") != 0
                        or request.get("tools") not in (None, [])
                        or schema_observation.get("response_schema_sha256") != output_schema_digest
                    ):
                        raise ValueError("pinned runtime request does not match the fixed arithmetic/schema replay contract")
                    wire = b"".join(frames)
                    provider_wire_record = _write_captured_bytes(capture_paths["provider_sse"], wire, 2_097_152)
                    provider_choice_record = _write_captured_bytes(capture_paths["provider_choice_0"], source_choice, 65_536)
                    observation = {
                        "fixture_kind": "replay_of_bounded_real_qwen_sse",
                        "source_artifact": captured_sse_replay.get("source_artifact"),
                        "source_provider_response_id": captured_sse_replay.get("source_provider_response_id"),
                        "fake_http_response_id": response_id,
                        "source_provider_sse_sha256": captured_sse_replay.get("provider_sse_sha256"),
                        "source_provider_sse_utf8_bytes": captured_sse_replay.get("provider_sse_utf8_bytes"),
                        "replayed_provider_sse": provider_wire_record,
                        "source_choice_0_sha256": captured_sse_replay.get("provider_choice_0_sha256"),
                        "replayed_choice_0": provider_choice_record,
                        "provider_sse_matches_source": provider_wire_record.get("sha256") == captured_sse_replay.get("provider_sse_sha256"),
                        "choice_0_matches_source": provider_choice_record.get("sha256") == captured_sse_replay.get("provider_choice_0_sha256"),
                        "frame_count": len(frames),
                        "http_transport": "fake HTTP/1.1 chunked; one SSE frame per transfer chunk",
                        "timing_fidelity": "a measured-send to first-App-Server-assistant delay estimate is applied before fake HTTP response headers; original provider-header timing is unavailable; subsequent SSE frame intervals are replayed; original HTTP chunk boundaries are not preserved",
                        "initial_response_delay_seconds": float(initial_delay),
                        "body_inter_event_seconds": sum(body_delays),
                        "total_replay_stream_seconds": total_replay_seconds,
                        "fake_usage_fixture": usage,
                    }
                    if not observation["provider_sse_matches_source"] or not observation["choice_0_matches_source"]:
                        raise ValueError("captured actual SSE or choice content did not match its source hashes")
                    captured_sse_replay_observations.append(observation)
                    return list(zip(frames, body_delays, strict=True)), observation

                fake.set_sse_response_factory(stream_captured_actual_response)
            fake.start()
            upstream_base_url = f"http://127.0.0.1:{fake.port}"
            upstream_api_key = "isolated-fake-only"
        stage = "start_isolated_runtime_and_gateway"
        sandbox = p3.Phase3Sandbox(state_path, run_id, image)
        if output_observation_diagnostics or output_delivery_reproduction or captured_replay:
            sandbox.adapter_observation_directory = capture_dir
        sandbox.start_network()
        gateway_port = p3.reserve_port(sandbox.gateway_address)
        private_token = __import__("secrets").token_urlsafe(40)
        gateway_profile = ProviderGatewayProfile(
            profile_id=execution_profile.profile_id,
            price_table=prices,
            upstream_base_url=upstream_base_url,
            upstream_api_key=upstream_api_key,
            max_input_tokens=execution_profile.max_input_tokens,
            max_output_tokens=execution_profile.max_output_tokens,
            test_only=True,
            execution_profile=execution_profile,
        )
        async def observe_provider_authorization(factory_arg, intent):
            measurement = intent.measurement
            binding = intent.binding
            observation = {
                "call_kind": intent.call_kind,
                "model": intent.model,
                "measured_input_tokens": measurement.measured_input_tokens if measurement else None,
                "requested_output_tokens": measurement.requested_output_tokens if measurement else None,
                "request_digest": measurement.request_digest if measurement else None,
                "profile_digest": measurement.profile_digest if measurement else None,
                "accounting_call_id": str(intent.accounting_call_id),
                "permit_id": str(intent.permit_id),
                "operation_id": str(intent.operation_id),
                "task_id": str(binding.task_id),
                "attempt_id": str(binding.attempt_id),
                "registry_id": str(binding.agent_registry_id),
                "input_revision": binding.input_revision,
                "fence": binding.fence,
            }
            try:
                result = await original_authorize_provider_call(factory_arg, intent)
            except Exception as error:
                observation["authorization"] = "rejected"
                observation["reason_type"] = type(error).__name__
                provider_authorization_attempts.append(observation)
                raise
            observation["authorization"] = "permitted"
            provider_authorization_attempts.append(observation)
            return result

        async def observe_permit_consumption(*args, **kwargs):
            result = await original_consume_call_permit(*args, **kwargs)
            consumed_permits.append({
                "permit_id": str(result.permit_id),
                "accounting_call_id": str(result.accounting_call_id),
                "operation_id": str(result.operation_id),
                "consumed": result.consumed,
                "request_digest": result.request_digest,
                "profile_digest": result.profile_digest,
            })
            return result

        gateway_module.authorize_provider_call = observe_provider_authorization
        gateway_module.consume_call_permit = observe_permit_consumption
        if local_smoke:
            restore_local_upstream_guard = _install_local_upstream_guard(
                candidate, execution_profile.content_digest, run_id, provider_authorization_attempts, consumed_permits,
                provider_send_observations, provider_response_observations, capture_paths,
            )
            _reserve_local_smoke_ledger(run_id, candidate, {
                "host": parsed.host, "port": parsed.port, "database": parsed.database,
            }, authorization=local_authorization)
            stage = "observe_or_load_local_runner_without_generation"
            initial_runner = await asyncio.to_thread(_observe_current_loaded_runner, candidate)
            initial_runner.pop("loaded_models", None)
            load_observation: dict[str, object] = {
                "attempted": False,
                "reason": "already_loaded_with_verified_identity_and_context"
                if initial_runner.get("context_verified_for_this_loaded_runner") else "runner_not_loaded",
                "generation_observed": False,
            }
            if initial_runner.get("context_verified_for_this_loaded_runner") is not True:
                _update_ledger(run_id, {
                    "state": "MODEL_ONLY_LOAD_STARTED",
                    "model_load_attempts": 1,
                })
                load_observation = await asyncio.to_thread(_ollama_model_only_load, candidate)
                load_observation["attempted"] = True
                context_observation = await asyncio.to_thread(_observe_loaded_context, candidate)
            else:
                context_observation = initial_runner
            report["ollama_execution_context"] = {
                "initial_loaded_runner": initial_runner,
                "model_only_load": load_observation,
                "loaded_runner": context_observation,
            }
            _update_ledger(run_id, {
                "state": "MODEL_LOADED_CONTEXT_VERIFIED",
                "model_only_load": load_observation,
                "loaded_context": context_observation,
            })
            if not context_observation.get("context_verified_for_this_loaded_runner"):
                raise ValueError("loaded runner context was not confirmed before Task admission")
            report["pre_request_gate"] = {
                "task_provider_request_sent": False,
                "single_attempt_marker_reserved": True,
                "loaded_context_verified": True,
            }
        gateway_app = create_provider_gateway(factory, gateway_profile, private_token, allow_test_profile=True)
        gateway_server, gateway_task = await p3.start_gateway_server(gateway_app, sandbox.gateway_address, gateway_port)
        sandbox.start_pinned_app_server(gateway_port, private_token)

        letta_url = f"ws://{sandbox.container_address}:{p1.APP_PORT}"
        settings = load_settings(env, config_dir)
        actor = configured_local_actor(settings)
        execution_config = configured_task_execution(settings)
        if execution_config.profile_digest != execution_profile.content_digest:
            raise AssertionError("configured Task profile is not bound to the frozen Qwen test contract")
        bridge = BridgeClient(node, ROOT / "bridge/letta/dist/main.js", env={
            "HEKATE_LETTA_URL": letta_url,
            "HEKATE_LETTA_TOKEN": private_token,
            "HEKATE_REQUIRE_PROVIDER_BINDING": "1",
            "HEKATE_LETTA_TURN_TIMEOUT_MS": str(bridge_request_timeout_ms),
            **({
                "HEKATE_OUTPUT_OBSERVATION_FILE": str(output_observation_file),
                "HEKATE_OUTPUT_OBSERVATION_RAW": "1",
                "HEKATE_OUTPUT_APP_SERVER_RAW_FILE": str(capture_paths["app_server_assistant"]),
                "HEKATE_OUTPUT_ASSISTANT_RAW_FILE": str(capture_paths["sdk_assistant"]),
                "HEKATE_OUTPUT_SDK_RESULT_RAW_FILE": str(capture_paths["sdk_result"]),
                "HEKATE_OUTPUT_BRIDGE_RAW_FILE": str(capture_paths["bridge_raw_output"]),
            } if output_observation_diagnostics or output_delivery_reproduction or captured_replay else {}),
        })
        runtime = LettaRuntimeAdapter(bridge)
        stage = "verify_pinned_letta_bridge"
        runtime_capabilities = await runtime.verify_compatibility()
        container = Container(settings=settings, runtime=runtime, uow_factory=factory, database=engine)
        worker_task = asyncio.create_task(run_worker(container, stop))

        stage = "submit_single_task_and_wait_for_runtime_result"
        request_key = f"phase6c-{run_id}"
        question = (
            "17과 25의 합을 한국어 한 문장으로 답해줘."
            if local_smoke or captured_replay else
            "P6D_OUTPUT_DELIVERY_COMPLETE: 한국어와 JSON 문자열 보존을 검사한다. 따옴표 \"그대로\"와 줄바꿈을 포함한다.\n마지막 필드와 닫는 괄호까지 그대로 전달하라."
            if output_delivery_reproduction else
            "Using this public synthetic probe, explain briefly why exact request measurement matters."
        )
        receipt = await p3b._cli(
            env, "ask", "--request-key", request_key, "--wait-seconds", "0",
            stdin=question.encode("utf-8"),
        )
        task_id = TaskId(str(receipt["receipt"]["task_id"]))
        task_view = await p3b._wait_for_state(factory, actor, task_id, {"COMPLETED", "FAILED"}, timeout=250)
        async with engine.connect() as connection:
            provider_agent_id = await connection.scalar(text(
                "SELECT provider_agent_id FROM agent_registry WHERE owner_scope=:scope AND role='hekate'"
            ), {"scope": scope_id})
        if not isinstance(provider_agent_id, str) or not provider_agent_id:
            raise AssertionError("pinned runtime did not persist the HEKATE provider binding")
        agent_file_key = base64.b64encode(provider_agent_id.encode("utf-8")).decode("ascii").rstrip("=")
        agent_file = sandbox.state / "lc-local-backend" / "agents" / f"{agent_file_key}.json"
        if not agent_file.is_file():
            raise AssertionError("pinned runtime's persisted agent configuration is unavailable")
        agent_record = json.loads(agent_file.read_text(encoding="utf-8"))
        model_settings = agent_record.get("model_settings")
        if not isinstance(model_settings, dict):
            raise AssertionError("pinned runtime agent has no model settings")
        agent_profile = {
            "provider_agent_id": provider_agent_id,
            "system_prompt_chars": len(str(agent_record.get("system", ""))),
            "context_window_limit": model_settings.get("context_window_limit"),
            "max_tokens": model_settings.get("max_tokens"),
            "system_prompt_sha256": hashlib.sha256(str(agent_record.get("system", "")).encode("utf-8")).hexdigest(),
            "configured_context_window_tokens": candidate.context_window_tokens,
            "configured_letta_context_estimator_tokens": candidate.letta_context_estimator_tokens,
            "configured_input_ceiling_tokens": candidate.max_input_tokens,
            "configured_output_ceiling_tokens": candidate.max_output_tokens,
            "configured_system_prompt_sha256": candidate.agent_system_prompt_sha256,
        }
        if (
            agent_profile["context_window_limit"] != candidate.letta_context_estimator_tokens
            or agent_profile["max_tokens"] != candidate.max_output_tokens
            or agent_record.get("system") != candidate.agent_system_prompt
            or agent_profile["system_prompt_sha256"] != candidate.agent_system_prompt_sha256
        ):
            raise AssertionError("pinned runtime agent did not receive separate context and output ceilings")
        report["letta_agent_profile"] = agent_profile
        report["task_observation"] = {
            "task_id": str(task_id),
            "state": task_view.get("state"),
            "stop_reason": task_view.get("stop_reason"),
            "response_present": bool(task_view.get("response")),
            "response_sha256": hashlib.sha256(str(task_view.get("response", "")).encode("utf-8")).hexdigest(),
            "result_status": task_view.get("result_status"),
            "accepted_user_response": (
                task_view.get("response")
                if task_view.get("state") == "COMPLETED" and task_view.get("result_status") == "ACCEPTED"
                else None
            ),
        }
        if (
            len(provider_authorization_attempts) != 1
            or provider_authorization_attempts[0].get("call_kind") != "turn"
            or provider_authorization_attempts[0].get("model") != candidate.model
            or provider_authorization_attempts[0].get("requested_output_tokens") != candidate.max_output_tokens
            or provider_authorization_attempts[0].get("profile_digest") != execution_profile.content_digest
            or provider_authorization_attempts[0].get("authorization") != "permitted"
        ):
            raise AssertionError("pinned runtime did not authorize exactly one main turn call without compaction")
        if local_smoke:
            if fake is not None or int(gateway_app.state.metrics["upstream_forward_attempts"]) != 1:
                raise AssertionError("local smoke must attempt at most one authorized upstream request")
            request_body = None
            fake_observation = None
            normalized = None
            forwarded_digest = None
            exact_measurement = None
            requested_output = candidate.max_output_tokens
            provider_request_count = int(gateway_app.state.metrics["upstream_forward_attempts"])
            if len(consumed_permits) != 1 or consumed_permits[0].get("consumed") is not True:
                raise AssertionError("the one local request was not backed by a consumed database permit")
        else:
            if len(captured_bodies) != 1 or fake is None or fake.count() != 1:
                raise AssertionError("one-turn/no-compaction probe did not produce exactly one fake provider request")
            request_body = captured_bodies[0]
            requested_output = request_body.get("max_tokens")
            if type(requested_output) is not int:
                raise AssertionError("normalized request did not expose the Ollama max_tokens ceiling")
            normalized, forwarded_body, exact_measurement = measure_qwen35_request(
                request_body, execution_profile, requested_output,
                output_contract="hekate_turn_output_v1", native_schema_injected=True,
            )
            fake_observation = fake.requests[0]
            forwarded_digest = hashlib.sha256(forwarded_body).hexdigest()
            if fake_observation.get("raw_request_sha256") != forwarded_digest:
                raise AssertionError("gateway's measured digest differs from the exact bytes received upstream")
            if request_body.get("reasoning_effort") != "none" or request_body.get("tools") not in (None, []):
                raise AssertionError("fake upstream observed a request outside no-thinking, no-tool smoke policy")
            provider_request_count = fake.count()

        stage = "verify_provider_measurement_and_permit_rows"
        task_sql = str(task_id)
        async with engine.connect() as connection:
            call_row = (await connection.execute(text("""
                SELECT p.accounting_call_id, p.permit_id, p.operation_id, p.attempt_id,
                       p.registry_id, o.task_id, p.input_revision, p.fence, p.lease_owner,
                       p.status AS call_status, p.call_kind, p.model,
                       p.allocation_amount::text AS allocation_amount, p.measurement_status,
                       p.request_digest, p.profile_digest, p.measurement_data,
                       cp.state AS permit_state, cp.request_digest AS permit_request_digest,
                       cp.profile_digest AS permit_profile_digest,
                       u.completeness AS usage_completeness, u.settlement_state,
                       u.input_tokens, u.output_tokens, u.total_tokens, u.evaluated_cost_usd::text AS evaluated_cost_usd
                FROM provider_calls p
                JOIN operations o ON o.id=p.operation_id
                JOIN attempts a ON a.id=p.attempt_id
                JOIN call_permits cp ON cp.accounting_call_id=p.accounting_call_id
                LEFT JOIN usage_projections u ON u.accounting_call_id=p.accounting_call_id
                WHERE o.task_id=:task
            """), {"task": task_sql})).mappings().one()
            counts = (await connection.execute(text("""
                SELECT count(p.accounting_call_id) AS provider_calls,
                       count(p.accounting_call_id) FILTER (WHERE cp.state='CONSUMED') AS consumed_permits,
                       count(p.accounting_call_id) FILTER (WHERE p.measurement_status='MEASURED') AS measured_calls,
                       count(p.accounting_call_id) FILTER (WHERE p.call_kind='compaction') AS compaction_calls
                FROM operations o
                LEFT JOIN provider_calls p ON p.operation_id=o.id
                LEFT JOIN call_permits cp ON cp.accounting_call_id=p.accounting_call_id
                WHERE o.task_id=:task
            """), {"task": task_sql})).mappings().one()
            accounts = (await connection.execute(text("""
                SELECT scope_kind, spent_amount::text AS spent_amount, held_amount::text AS held_amount
                FROM budget_accounts
                WHERE id IN (SELECT 'task-budget:' || id FROM tasks WHERE id=:task)
                   OR (scope_kind='SYSTEM' AND period_id=to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD'))
                ORDER BY scope_kind
            """), {"task": task_sql})).mappings().all()
            result_row = (await connection.execute(text("""
                SELECT tr.inbox_id, tr.processing_state, tr.task_id, tr.attempt_id,
                       tr.operation_id, tr.registry_id, tr.input_revision,
                       tr.conclusion_id, tr.output_hash, tr.raw_output,
                       octet_length(tr.raw_output) AS raw_output_bytes,
                       c.validation_status, c.eligible, c.payload_hash
                FROM turn_results tr
                LEFT JOIN conclusions c ON c.id=tr.conclusion_id
                WHERE tr.task_id=:task
                ORDER BY tr.created_at DESC
                LIMIT 1
            """), {"task": task_sql})).mappings().one_or_none()
            task_response_row = (await connection.execute(text("""
                SELECT task_id, input_revision, operation_id, attempt_id, registry_id,
                       response_text, outcome, stop_reason
                FROM task_responses WHERE task_id=:task
            """), {"task": task_sql})).mappings().one_or_none()
            execution_row = (await connection.execute(text("""
                SELECT o.id AS operation_id, o.kind AS operation_kind,
                       o.state AS operation_state, o.dispatch_state, o.execution_state,
                       a.id AS attempt_id, a.status AS attempt_state, a.input_revision,
                       r.id AS registry_id, r.provider_agent_id, r.intended_state,
                       r.observed_state, r.persistence, r.role
                FROM operations o
                JOIN attempts a ON a.operation_id=o.id
                JOIN agent_registry r ON r.id=a.agent_registry_id
                WHERE o.task_id=:task AND o.kind='hekate.turn'
                ORDER BY o.created_at DESC
                LIMIT 1
            """), {"task": task_sql})).mappings().one_or_none()
        call_row = dict(call_row)
        result_row = dict(result_row) if result_row is not None else None
        if result_row is not None:
            database_raw_output = result_row.pop("raw_output", None)
            if isinstance(database_raw_output, str):
                database_raw_bytes = database_raw_output.encode("utf-8", "strict")
                if capture_paths:
                    result_row["raw_output_capture"] = _write_bounded_assistant_text(
                        capture_paths["postgres_raw_output"], database_raw_output,
                    )
                else:
                    result_row["raw_output_capture"] = {
                        "captured_utf8_bytes": min(len(database_raw_bytes), 65_536),
                        "full_utf8_bytes": len(database_raw_bytes),
                        "sha256": hashlib.sha256(database_raw_bytes[:65_536]).hexdigest(),
                        "full_sha256": hashlib.sha256(database_raw_bytes).hexdigest(),
                        "truncated": len(database_raw_bytes) > 65_536,
                        "persisted": False,
                    }
        task_response_row = dict(task_response_row) if task_response_row is not None else None
        execution_row = dict(execution_row) if execution_row is not None else None
        counts = {key: int(value) for key, value in dict(counts).items()}
        settle_deadline = asyncio.get_running_loop().time() + 30
        while call_row["settlement_state"] != "SETTLED" and asyncio.get_running_loop().time() < settle_deadline:
            await asyncio.sleep(0.2)
            async with engine.connect() as connection:
                call_row = dict((await connection.execute(text("""
                    SELECT p.accounting_call_id, p.status AS call_status, p.call_kind, p.model,
                           o.task_id,
                           p.permit_id, p.operation_id, p.attempt_id, p.registry_id,
                           p.input_revision, p.fence, p.lease_owner,
                           p.allocation_amount::text AS allocation_amount, p.measurement_status,
                           p.request_digest, p.profile_digest, p.measurement_data,
                           cp.state AS permit_state, cp.request_digest AS permit_request_digest,
                           cp.profile_digest AS permit_profile_digest,
                           u.completeness AS usage_completeness, u.settlement_state,
                           u.input_tokens, u.output_tokens, u.total_tokens, u.evaluated_cost_usd::text AS evaluated_cost_usd
                    FROM provider_calls p
                    JOIN operations o ON o.id=p.operation_id
                    JOIN call_permits cp ON cp.accounting_call_id=p.accounting_call_id
                    LEFT JOIN usage_projections u ON u.accounting_call_id=p.accounting_call_id
                    WHERE o.task_id=:task
                """), {"task": task_sql})).mappings().one())
        measurement_data = call_row["measurement_data"]
        if not isinstance(measurement_data, dict):
            raise AssertionError("PostgreSQL call row has no persisted request measurement")
        actual_request_digest = (
            provider_send_observations[0].get("request_sha256")
            if local_smoke and len(provider_send_observations) == 1
            else forwarded_digest
        )
        if (
            call_row["measurement_status"] != "MEASURED"
            or call_row["request_digest"] != actual_request_digest
            or call_row["permit_request_digest"] != call_row["request_digest"]
            or call_row["profile_digest"] != execution_profile.content_digest
            or call_row["permit_profile_digest"] != execution_profile.content_digest
            or (
                not local_smoke
                and measurement_data.get("measured_input_tokens") != exact_measurement.measured_input_tokens
            )
            or counts != {"provider_calls": 1, "consumed_permits": 1, "measured_calls": 1, "compaction_calls": 0}
        ):
            raise AssertionError("PostgreSQL measurement, profile, or single-use permit binding is inconsistent")
        if Decimal(call_row["allocation_amount"]) != Decimal("0"):
            raise AssertionError("synthetic external tariff is not represented as zero monetary allocation")

        measured_input_tokens = measurement_data.get("measured_input_tokens")
        provider_input_tokens = call_row["input_tokens"]
        input_token_delta = (
            provider_input_tokens - measured_input_tokens
            if type(provider_input_tokens) is int and type(measured_input_tokens) is int else None
        )
        output_limit_tokens = requested_output
        binding_match = bool(
            result_row is not None
            and execution_row is not None
            and result_row.get("processing_state") == "ACCEPTED"
            and result_row.get("validation_status") == "VALIDATED"
            and result_row.get("eligible") is True
            and task_response_row is not None
            and result_row.get("task_id") == task_sql
            and result_row.get("operation_id") == call_row.get("operation_id")
            and result_row.get("attempt_id") == call_row.get("attempt_id")
            and result_row.get("registry_id") == call_row.get("registry_id")
            and result_row.get("input_revision") == call_row.get("input_revision")
            and task_response_row.get("operation_id") == call_row.get("operation_id")
            and task_response_row.get("attempt_id") == call_row.get("attempt_id")
            and task_response_row.get("registry_id") == call_row.get("registry_id")
            and task_response_row.get("input_revision") == call_row.get("input_revision")
            and task_response_row.get("response_text") == task_view.get("response")
            and execution_row.get("operation_id") == call_row.get("operation_id")
            and execution_row.get("attempt_id") == call_row.get("attempt_id")
            and execution_row.get("registry_id") == call_row.get("registry_id")
        )
        task_response_summary = None
        if task_response_row is not None:
            task_response_text = task_response_row.get("response_text")
            task_response_summary = {
                key: value for key, value in task_response_row.items() if key != "response_text"
            }
            task_response_summary["response_present"] = isinstance(task_response_text, str) and bool(task_response_text)
            task_response_summary["response_bytes"] = len(task_response_text.encode("utf-8")) if isinstance(task_response_text, str) else None
            task_response_summary["response_sha256"] = hashlib.sha256(
                task_response_text.encode("utf-8") if isinstance(task_response_text, str) else b"",
            ).hexdigest()
        app_server_terminal_observed = False
        if captured_replay and isinstance(call_row.get("operation_id"), str):
            app_server_terminal_observed = await _wait_for_app_server_terminal(
                capture_paths["bridge_observations"], str(call_row["operation_id"]),
            )
        report["app_server_terminal_event_observed"] = app_server_terminal_observed if captured_replay else None
        stream_observation = (
            gateway_app.state.last_provider_stream_observation
            if gateway_app is not None else None
        )
        stream_choice = None
        if isinstance(stream_observation, dict) and isinstance(stream_observation.get("choices"), list):
            stream_choice = next((item for item in stream_observation["choices"] if isinstance(item, dict) and item.get("index") == 0), None)
        provider_bridge_content_match = bool(
            isinstance(stream_choice, dict)
            and result_row is not None
            and stream_choice.get("content_sha256") == result_row.get("output_hash")
            and stream_choice.get("content_utf8_bytes") == result_row.get("raw_output_bytes")
        )

        stage = "restart_worker_and_replay_same_request_key"
        before_replay = await p3b._accounting_effects(engine, task_id)
        stop.set()
        await asyncio.wait_for(worker_task, timeout=15)
        stop = asyncio.Event()
        worker_task = asyncio.create_task(run_worker(container, stop))
        replay = await p3b._cli(
            env, "ask", "--request-key", request_key, "--wait-seconds", "0",
            stdin=question.encode("utf-8"),
        )
        await asyncio.sleep(0.5)
        after_replay = await p3b._accounting_effects(engine, task_id)
        final_upstream_count = (
            int(gateway_app.state.metrics["upstream_forward_attempts"])
            if local_smoke else fake.count()
        )
        if replay["receipt"]["task_id"] != task_sql or final_upstream_count != 1 or after_replay != before_replay:
            raise AssertionError("same-key Task replay created another fake provider request")

        incomplete_delivery: dict[str, object] | None = None
        if output_delivery_reproduction:
            stage = "submit_incomplete_multidelta_output"
            incomplete_request_key = f"phase6d-output-delivery-incomplete-{run_id}"
            incomplete_question = (
                "P6D_OUTPUT_DELIVERY_INCOMPLETE: 이 합성 응답은 마지막 JSON 닫는 괄호가 빠지므로 채택되면 안 된다."
            )
            incomplete_receipt = await p3b._cli(
                env, "ask", "--request-key", incomplete_request_key, "--wait-seconds", "0",
                stdin=incomplete_question.encode("utf-8"),
            )
            incomplete_task_id = TaskId(str(incomplete_receipt["receipt"]["task_id"]))
            incomplete_task_view = await p3b._wait_for_state(
                factory, actor, incomplete_task_id, {"COMPLETED", "FAILED"}, timeout=250,
            )
            incomplete_task_sql = str(incomplete_task_id)
            async with engine.connect() as connection:
                incomplete_row = (await connection.execute(text("""
                    SELECT tr.inbox_id, tr.processing_state, tr.task_id, tr.attempt_id,
                           tr.operation_id, tr.registry_id, tr.input_revision,
                           tr.conclusion_id, tr.output_hash, octet_length(tr.raw_output) AS raw_output_bytes,
                           c.validation_status, c.eligible
                    FROM turn_results tr
                    LEFT JOIN conclusions c ON c.id=tr.conclusion_id
                    WHERE tr.task_id=:task
                    ORDER BY tr.created_at DESC LIMIT 1
                """), {"task": incomplete_task_sql})).mappings().one_or_none()
                incomplete_call = (await connection.execute(text("""
                    SELECT p.accounting_call_id, p.operation_id, p.attempt_id, p.registry_id,
                           p.status AS call_status, p.provider_call_id,
                           cp.state AS permit_state, u.completeness AS usage_completeness,
                           u.settlement_state
                    FROM provider_calls p
                    JOIN operations o ON o.id=p.operation_id
                    JOIN call_permits cp ON cp.accounting_call_id=p.accounting_call_id
                    LEFT JOIN usage_projections u ON u.accounting_call_id=p.accounting_call_id
                    WHERE o.task_id=:task
                """), {"task": incomplete_task_sql})).mappings().one_or_none()
                incomplete_response = (await connection.execute(text("""
                    SELECT outcome, stop_reason, octet_length(response_text) AS response_bytes
                    FROM task_responses WHERE task_id=:task
                """), {"task": incomplete_task_sql})).mappings().one_or_none()
            if incomplete_row is None or incomplete_call is None:
                raise AssertionError("incomplete stream did not reach durable result and accounting records")
            incomplete_row = dict(incomplete_row)
            incomplete_call = dict(incomplete_call)
            incomplete_response = dict(incomplete_response) if incomplete_response is not None else None
            if (
                incomplete_task_view.get("state") != "FAILED"
                or incomplete_task_view.get("stop_reason") != "POLICY"
                or incomplete_row.get("processing_state") != "REJECTED"
                or incomplete_row.get("conclusion_id") is not None
                or incomplete_call.get("call_status") != "QUIESCENT"
                or incomplete_call.get("permit_state") != "CONSUMED"
                or incomplete_call.get("usage_completeness") != "COMPLETE"
                or incomplete_call.get("settlement_state") != "SETTLED"
                or len(fake_output_delivery_fixtures) != 2
                or fake.count() != 2
            ):
                raise AssertionError("incomplete provider output was not preserved and rejected without retry")
            before_incomplete_replay = await p3b._accounting_effects(engine, incomplete_task_id)
            stage = "restart_worker_and_replay_incomplete_request_key"
            stop.set()
            await asyncio.wait_for(worker_task, timeout=15)
            stop = asyncio.Event()
            worker_task = asyncio.create_task(run_worker(container, stop))
            incomplete_replay = await p3b._cli(
                env, "ask", "--request-key", incomplete_request_key, "--wait-seconds", "0",
                stdin=incomplete_question.encode("utf-8"),
            )
            after_incomplete_replay = await p3b._accounting_effects(engine, incomplete_task_id)
            if (
                incomplete_replay["receipt"]["task_id"] != incomplete_task_sql
                or fake.count() != 2
                or after_incomplete_replay != before_incomplete_replay
            ):
                raise AssertionError("incomplete Task replay changed provider or accounting effects")

            provider_rows = gateway_app.state.provider_stream_observations
            complete_operation_id = str(call_row["operation_id"])
            complete_gateway = next((row for row in provider_rows if row.get("operation_id") == complete_operation_id), None)
            incomplete_gateway = next(
                (row for row in provider_rows if row.get("operation_id") == incomplete_row.get("operation_id")),
                None,
            )
            complete_fixture = fake_output_delivery_fixtures[0]
            incomplete_fixture = fake_output_delivery_fixtures[1]
            if complete_gateway is None or incomplete_gateway is None:
                raise AssertionError("gateway stream observations are not correlated to Task operations")
            incomplete_delivery = {
                "task_id": incomplete_task_sql,
                "operation_id": incomplete_row.get("operation_id"),
                "attempt_id": incomplete_row.get("attempt_id"),
                "accounting_call_id": incomplete_call.get("accounting_call_id"),
                "registry_id": incomplete_row.get("registry_id"),
                "task_state": incomplete_task_view.get("state"),
                "stop_reason": incomplete_task_view.get("stop_reason"),
                "result": incomplete_row,
                "server_response": incomplete_response,
                "provider_fixture": incomplete_fixture,
                "gateway_stream_observation": incomplete_gateway,
                "same_key_replay": {
                    "same_task_id": incomplete_replay["receipt"]["task_id"] == incomplete_task_sql,
                    "provider_requests_before": 2,
                    "provider_requests_after": fake.count(),
                    "accounting_effects_unchanged": after_incomplete_replay == before_incomplete_replay,
                },
                "complete_scenario_gateway_stream_observation": complete_gateway,
                "complete_scenario_fixture": complete_fixture,
            }

        report["results"] = {
            "pinned_runtime_turn": {
                "task_id": task_sql,
                "operation_id": call_row.get("operation_id"),
                "attempt_id": call_row.get("attempt_id"),
                "registry_id": call_row.get("registry_id"),
                "agent_profile": agent_profile,
                "provider_authorization_attempts": provider_authorization_attempts,
                "consumed_permits": consumed_permits,
                "task_state": task_view["state"],
                "task_stop_reason": task_view.get("stop_reason"),
                "task_response_present": bool(task_view.get("response")),
                "task_response_sha256": hashlib.sha256(str(task_view.get("response", "")).encode()).hexdigest(),
                "accepted_user_response": task_view.get("response") if binding_match else None,
                "task_response_record": task_response_summary,
                "runtime_operation_and_attempt": execution_row,
                "result_and_schema_validation": result_row,
                "accepted_result_binding_matches_permit": binding_match,
                "runtime_capabilities": runtime_capabilities,
                "session_compaction_limit": execution_config.max_compaction_calls,
                "fake_provider_requests": 0 if local_smoke else fake.count(),
                "local_upstream_forward_attempts": provider_request_count if local_smoke else 0,
                "actual_provider_calls": len(provider_send_observations) if local_smoke else 0,
                "provider_send_observations": provider_send_observations,
                "provider_response_observations": provider_response_observations,
                "provider_sse_content_observation": stream_observation,
                "provider_sse_matches_bridge_raw_output": provider_bridge_content_match if local_smoke else None,
                "provider_request": {
                    "model": candidate.model if local_smoke else request_body.get("model"),
                    "keys": None if local_smoke else sorted(request_body),
                    "raw_forwarded_body_sha256": actual_request_digest if local_smoke else fake_observation["raw_request_sha256"],
                    "canonical_body_sha256": None if local_smoke else fake_observation["request_hash"],
                    "forwarded_bytes_match_measurement_digest": None if local_smoke else forwarded_digest == fake_observation["raw_request_sha256"],
                    "reasoning_effort": provider_send_observations[0].get("reasoning_effort")
                        if local_smoke and provider_send_observations else request_body.get("reasoning_effort") if request_body else None,
                    "max_tokens": requested_output,
                    "tools_count": provider_send_observations[0].get("tools_count")
                        if local_smoke and provider_send_observations else len(request_body.get("tools", [])) if request_body and isinstance(request_body.get("tools", []), list) else None,
                    "json_schema_grammar_format_present": provider_send_observations[0].get("native_response_format_present")
                        if local_smoke and provider_send_observations else isinstance(request_body.get("response_format"), dict) if request_body else None,
                    "context_request_verified_by_fake_fixture": False if local_smoke else bool(turn_contract_observations),
                },
                "qwen_measurement": {
                    "input_tokens": measurement_data.get("measured_input_tokens"),
                    "provider_reported_input_tokens": call_row["input_tokens"],
                    "input_token_delta_provider_minus_measured": input_token_delta,
                    "provider_reported_output_tokens": call_row["output_tokens"],
                    "provider_reported_total_tokens": call_row["total_tokens"],
                    "output_limit_tokens": requested_output,
                    "context_window_tokens": candidate.context_window_tokens,
                    "fits_context": measurement_data.get("measured_input_tokens", candidate.max_input_tokens + 1) + requested_output <= candidate.context_window_tokens,
                    "request_digest": call_row["request_digest"],
                    "profile_digest": call_row["profile_digest"],
                    "tokenizer_identity": measurement_data.get("tokenizer_identity"),
                    "renderer_identity": measurement_data.get("renderer_identity"),
                    "normalized_model": candidate.model if local_smoke else normalized.get("model"),
                },
                "postgresql_call_and_permit": call_row,
                "counts": counts,
                "budget_accounts": [dict(row) for row in accounts],
                "usage_observation": {
                    "completeness": call_row["usage_completeness"],
                    "input_tokens": call_row["input_tokens"],
                    "output_tokens": call_row["output_tokens"],
                    "total_tokens": call_row["total_tokens"],
                    "evaluated_external_tariff_usd": call_row["evaluated_cost_usd"],
                    "synthetic_fake_provider": not local_smoke,
                    "local_qwen_usage_semantics_verified": False,
                },
                "request_contract_observation": turn_contract_observations,
                "same_key_replay": {
                    "same_task_id": replay["receipt"]["task_id"] == task_sql,
                    "fake_provider_requests_after_replay": 0 if local_smoke else fake.count(),
                    "provider_accounting_effects_after_replay": after_replay,
                    "provider_accounting_effects_before_replay": before_replay,
                    "no_new_fake_request": final_upstream_count == 1,
                    "no_new_upstream_request": final_upstream_count == 1,
                    "no_new_accounting_effect": after_replay == before_replay,
                },
            },
            "production_gate": {
                "local_candidate_dispatch_approved": candidate.operational_dispatch_approved,
                "candidate_runtime_context_verified": candidate.runtime_context_verified,
                "candidate_inference_usage_verified": candidate.inference_usage_verified,
                "gateway_profile_test_only": gateway_profile.test_only,
                "production_dispatch": "blocked",
            },
        }
        if incomplete_delivery is not None:
            report["results"]["output_delivery_reproduction"] = incomplete_delivery
            output_observations = _read_output_delivery_observations(output_observation_file)

            def content_chain(
                fixture: dict[str, object], gateway_observation: dict[str, object],
                operation_id: str, database_result: dict[str, object],
            ) -> dict[str, object]:
                operation_observation = output_observations.get(operation_id, {})
                app_server_snapshot = operation_observation.get("app_server")
                sdk_turn = operation_observation.get("sdk_turn")
                app_server = app_server_snapshot.get("assistant") if isinstance(app_server_snapshot, dict) else None
                sdk_assistant = sdk_turn.get("sdk_assistant") if isinstance(sdk_turn, dict) else None
                sdk_result = sdk_turn.get("sdk_result") if isinstance(sdk_turn, dict) else None
                bridge_raw = sdk_turn.get("bridge_raw_output") if isinstance(sdk_turn, dict) else None
                adapter = _summarize_adapter_observations(capture_paths["adapter_observations"], operation_id)
                gateway_choices = gateway_observation.get("choices")
                gateway_assistant = next(
                    (item for item in gateway_choices if isinstance(item, dict) and item.get("index") == 0),
                    None,
                ) if isinstance(gateway_choices, list) else None
                expected_hash = fixture.get("assistant_text_sha256")
                expected_bytes = fixture.get("assistant_text_utf8_bytes")
                stages: dict[str, object] = {
                    "fake_provider_choice_0": {
                        "event_count": fixture.get("content_delta_events"),
                        "utf8_bytes": expected_bytes,
                        "sha256": expected_hash,
                    },
                    "gateway_received_and_yielded_choice_0": {
                        "event_count": gateway_assistant.get("content_delta_events") if isinstance(gateway_assistant, dict) else None,
                        "utf8_bytes": gateway_assistant.get("content_utf8_bytes") if isinstance(gateway_assistant, dict) else None,
                        "sha256": gateway_assistant.get("content_sha256") if isinstance(gateway_assistant, dict) else None,
                    },
                    "provider_adapter_received_content": adapter["input_provider_content"],
                    "provider_adapter_emitted_text_delta": adapter["emitted_text_delta"],
                    "pinned_app_server_assistant_stream": {
                        "event_count": app_server.get("event_count") if isinstance(app_server, dict) else None,
                        "utf8_bytes": app_server.get("utf8_bytes") if isinstance(app_server, dict) else None,
                        "sha256": app_server.get("sha256") if isinstance(app_server, dict) else None,
                    },
                    "sdk_assistant_messages": {
                        "event_count": sdk_assistant.get("event_count") if isinstance(sdk_assistant, dict) else None,
                        "utf8_bytes": sdk_assistant.get("utf8_bytes") if isinstance(sdk_assistant, dict) else None,
                        "sha256": sdk_assistant.get("sha256") if isinstance(sdk_assistant, dict) else None,
                    },
                    "sdk_result_text": sdk_result,
                    "bridge_raw_output": bridge_raw,
                    "postgresql_raw_output": {
                        "utf8_bytes": database_result.get("raw_output_bytes"),
                        "sha256": database_result.get("output_hash"),
                    },
                    "provider_sse_wire": {
                        "utf8_bytes": fixture.get("provider_sse_wire_utf8_bytes"),
                        "sha256": fixture.get("provider_sse_wire_sha256"),
                    },
                    "gateway_received_and_yielded_sse_wire": {
                        "utf8_bytes": gateway_observation.get("received_and_yielded_wire_utf8_bytes"),
                        "sha256": gateway_observation.get("received_and_yielded_wire_sha256"),
                    },
                    "reasoning_stays_separate": {
                        "provider_fixture": {
                            "utf8_bytes": fixture.get("reasoning_utf8_bytes"),
                            "sha256": fixture.get("reasoning_sha256"),
                        },
                        "gateway": gateway_assistant.get("reasoning_channels") if isinstance(gateway_assistant, dict) else None,
                        "pinned_app_server": app_server_snapshot.get("reasoning") if isinstance(app_server_snapshot, dict) else None,
                        "sdk": sdk_turn.get("sdk_reasoning") if isinstance(sdk_turn, dict) else None,
                    },
                    "provider_adapter_internal_text_delta": adapter,
                }
                compared = [
                    stages["fake_provider_choice_0"],
                    stages["gateway_received_and_yielded_choice_0"],
                    stages["provider_adapter_received_content"],
                    stages["provider_adapter_emitted_text_delta"],
                    stages["pinned_app_server_assistant_stream"],
                    stages["sdk_assistant_messages"],
                    stages["sdk_result_text"],
                    stages["bridge_raw_output"],
                    stages["postgresql_raw_output"],
                ]
                matches = all(
                    isinstance(item, dict)
                    and item.get("utf8_bytes") == expected_bytes
                    and item.get("sha256") == expected_hash
                    for item in compared
                )
                gateway_wire = stages["gateway_received_and_yielded_sse_wire"]
                provider_wire = stages["provider_sse_wire"]
                wire_matches = (
                    isinstance(gateway_wire, dict)
                    and isinstance(provider_wire, dict)
                    and gateway_wire == provider_wire
                )
                return {
                    "operation_id": operation_id,
                    "assistant_content_stages": stages,
                    "all_observed_assistant_content_matches_provider_fixture": matches,
                    "gateway_sse_wire_matches_fake_provider_wire": wire_matches,
                }

            def first_content_mismatch(chain: dict[str, object], fixture: dict[str, object]) -> str | None:
                stages = chain.get("assistant_content_stages")
                if not isinstance(stages, dict):
                    return "assistant_content_stages:observation_missing"
                expected_bytes = fixture.get("assistant_text_utf8_bytes")
                expected_hash = fixture.get("assistant_text_sha256")
                ordered_stages = (
                    "fake_provider_choice_0",
                    "gateway_received_and_yielded_choice_0",
                    "provider_adapter_received_content",
                    "provider_adapter_emitted_text_delta",
                    "pinned_app_server_assistant_stream",
                    "sdk_assistant_messages",
                    "sdk_result_text",
                    "bridge_raw_output",
                    "postgresql_raw_output",
                )
                for name in ordered_stages:
                    observation = stages.get(name)
                    if not isinstance(observation, dict):
                        return f"{name}:observation_missing"
                    if (
                        observation.get("utf8_bytes") != expected_bytes
                        or observation.get("sha256") != expected_hash
                    ):
                        return f"{name}:content_mismatch"
                return None

            complete_chain = content_chain(
                fake_output_delivery_fixtures[0], complete_gateway,
                str(call_row["operation_id"]), result_row or {},
            )
            incomplete_chain = content_chain(
                fake_output_delivery_fixtures[1], incomplete_gateway,
                str(incomplete_delivery["operation_id"]), incomplete_delivery["result"],
            )
            report["results"]["output_delivery_reproduction"]["complete_chain"] = complete_chain
            report["results"]["output_delivery_reproduction"]["incomplete_chain"] = incomplete_chain
            report["results"]["output_delivery_reproduction"]["terminal_content_comparison"] = {
                "complete_task_accepted": task_view.get("state") == "COMPLETED" and result_row.get("processing_state") == "ACCEPTED",
                "incomplete_task_rejected": incomplete_delivery.get("task_state") == "FAILED"
                    and incomplete_delivery.get("result", {}).get("processing_state") == "REJECTED",
                "complete_fixture_matches_historical_size_count_and_terminal_shape": (
                    complete_fixture.get("assistant_text_utf8_bytes") == 760
                    and complete_fixture.get("content_delta_events") == 265
                    and complete_fixture.get("finish_reason_separate_frame") is True
                    and complete_fixture.get("done_marker_sent") is True
                ),
                "incomplete_fixture_is_one_byte_short_and_uses_same_frame_terminal": (
                    incomplete_fixture.get("assistant_text_utf8_bytes") == 759
                    and incomplete_fixture.get("content_delta_events") == 265
                    and incomplete_fixture.get("finish_reason_same_frame_as_final_content") is True
                    and incomplete_fixture.get("done_marker_sent") is True
                ),
            }
            report["results"]["output_delivery_reproduction"]["first_observed_mismatch"] = first_content_mismatch(
                complete_chain, fake_output_delivery_fixtures[0],
            )
            if (
                not complete_chain["all_observed_assistant_content_matches_provider_fixture"]
                or not complete_chain["gateway_sse_wire_matches_fake_provider_wire"]
                or not incomplete_chain["all_observed_assistant_content_matches_provider_fixture"]
                or not incomplete_chain["gateway_sse_wire_matches_fake_provider_wire"]
            ):
                raise AssertionError("multidelta output changed between provider, gateway, pinned runtime, bridge, or PostgreSQL")
        if captured_replay:
            replay_turn = report["results"]["pinned_runtime_turn"]
            output_chain = _build_actual_output_observation(replay_turn, capture_paths)
            replay_turn["output_delivery_chain"] = output_chain
            replay_turn["provider_response_is_valid_json"] = output_chain.get("provider_choice_0_json_parse", {}).get("valid_json")
            replay_turn["first_output_divergence"] = output_chain.get("first_divergence")
            source_choice_value = json.loads((ROOT / str(captured_sse_replay["provider_choice_0_path"])).read_text(encoding="utf-8"))
            source_conclusion = source_choice_value.get("conclusion", {}) if isinstance(source_choice_value, dict) else {}
            replay_binding = {
                "task_id": task_sql,
                "attempt_id": str(call_row.get("attempt_id")),
                "agent_id": str(call_row.get("registry_id")),
            }
            source_binding = {
                "task_id": source_conclusion.get("task_id"),
                "attempt_id": source_conclusion.get("attempt_id"),
                "agent_id": source_conclusion.get("agent_id"),
            }
            binding_same = source_binding == replay_binding
            app_server_stage = output_chain.get("app_server_stream_observer", {}).get("assistant", {})
            provider_choice_stage = output_chain.get("assistant_content_stages", {}).get("ollama_choice_0_content", {})
            sdk_stage = output_chain.get("assistant_content_stages", {}).get("sdk_assistant_messages", {})
            db_stage = output_chain.get("assistant_content_stages", {}).get("postgresql_raw_output", {})
            source_bytes = captured_sse_replay.get("provider_choice_0_utf8_bytes")
            source_hash = captured_sse_replay.get("provider_choice_0_sha256")
            transport_complete = bool(
                output_chain.get("identity_matches_across_provider_adapter_bridge_and_database") is True
                and output_chain.get("first_divergence") is None
                and output_chain.get("provider_choice_0_json_parse", {}).get("valid_json") is True
                and output_chain.get("app_server_stream_observer", {}).get("terminal_order") == ["usage_statistics", "stop_reason"]
                and app_server_stage.get("utf8_bytes") == source_bytes
                and app_server_stage.get("sha256") == source_hash
                and sdk_stage.get("utf8_bytes") == source_bytes
                and sdk_stage.get("sha256") == source_hash
                and db_stage.get("utf8_bytes") == source_bytes
                and db_stage.get("sha256") == source_hash
            )
            sdk_timeout_reproduced = bool(
                bridge_request_timeout_ms == 30_000
                and output_chain.get("first_divergence") == "sdk_assistant_messages:content_mismatch"
                and app_server_stage.get("utf8_bytes") == source_bytes
                and app_server_stage.get("sha256") == source_hash
                and type(sdk_stage.get("utf8_bytes")) is int
                and sdk_stage.get("utf8_bytes", source_bytes) < source_bytes
                and (ROOT / str(capture_paths["provider_choice_0"].relative_to(ROOT))).read_bytes().startswith(
                    capture_paths["sdk_assistant"].read_bytes(),
                )
                and db_stage.get("utf8_bytes") == sdk_stage.get("utf8_bytes")
                and db_stage.get("sha256") == sdk_stage.get("sha256")
            )
            task_failed_closed = bool(
                task_view.get("state") == "FAILED"
                and task_view.get("stop_reason") == "POLICY"
                and task_view.get("response")
                and result_row is not None
                and result_row.get("processing_state") == "REJECTED"
                and task_view.get("response") != source_choice_value.get("proposal", {}).get("answer")
            )
            expected_replay_outcome = (
                "sdk_timeout_truncation_reproduced"
                if bridge_request_timeout_ms == 30_000
                else "complete_transport_replay_with_stale_source_binding_rejected"
            )
            replay_result = {
                "expected_outcome": expected_replay_outcome,
                "bridge_request_timeout_ms": bridge_request_timeout_ms,
                "provider_response_complete_and_schema_shaped": output_chain.get("provider_choice_0_json_parse", {}).get("valid_json") is True,
                "first_output_divergence": output_chain.get("first_divergence"),
                "transport_complete_through_postgresql": transport_complete,
                "sdk_timeout_truncation_reproduced": sdk_timeout_reproduced,
                "app_server_terminal_event_observed": app_server_terminal_observed,
                "source_binding": source_binding,
                "replay_binding": replay_binding,
                "source_binding_matches_replay": binding_same,
                "stale_source_binding_must_not_be_accepted": not binding_same,
                "task_failed_closed_under_strict_binding": task_failed_closed,
                "fake_upstream_requests": fake.count() if fake is not None else 0,
                "same_key_replay_no_new_upstream_or_accounting_effect": final_upstream_count == 1 and after_replay == before_replay,
                "output_chain": output_chain,
            }
            report["results"]["captured_sse_replay"] = replay_result
            fake_response_observation = (
                fake.sse_response_observations[0]
                if fake is not None and len(fake.sse_response_observations) == 1
                else {}
            )
            captured_fake_response_complete = bool(
                len(captured_sse_replay_observations) == 1
                and captured_sse_replay_observations[0].get("provider_sse_matches_source") is True
                and captured_sse_replay_observations[0].get("choice_0_matches_source") is True
                and fake_response_observation.get("fake_http_status") == 200
                and fake_response_observation.get("fake_http_headers_sent") is True
                and fake_response_observation.get("fake_http_body_completed") is True
                and fake_response_observation.get("fake_http_body_bytes_sent")
                    == captured_sse_replay.get("provider_sse_utf8_bytes")
            )
            replay_result["captured_fake_http_response_complete"] = captured_fake_response_complete
            replay_result["captured_fake_http_observation"] = fake_response_observation
            local_success = bool(
                fake is not None
                and fake.count() == 1
                and len(fake_request_shapes) == 1
                and len(provider_authorization_attempts) == 1
                and provider_authorization_attempts[0].get("authorization") == "permitted"
                and len(consumed_permits) == 1
                and consumed_permits[0].get("consumed") is True
                and captured_fake_response_complete
                and call_row.get("call_status") == "QUIESCENT"
                and call_row.get("permit_state") == "CONSUMED"
                and call_row.get("usage_completeness") == "COMPLETE"
                and call_row.get("settlement_state") == "SETTLED"
                and final_upstream_count == 1
                and after_replay == before_replay
                and task_failed_closed
                and app_server_terminal_observed
                and (sdk_timeout_reproduced if bridge_request_timeout_ms == 30_000 else transport_complete and not binding_same)
            )
        common_success = bool(
            task_view["state"] == "COMPLETED"
            and binding_match
            and measurement_data.get("measured_input_tokens", candidate.max_input_tokens + 1) + requested_output <= candidate.context_window_tokens
            and str(measurement_data.get("tokenizer_identity", "")).startswith("gguf-gpt2/qwen35:")
            and str(measurement_data.get("renderer_identity", "")).startswith("ollama/0.34.0:qwen3.5:")
            and call_row["call_status"] == "QUIESCENT"
            and call_row["permit_state"] == "CONSUMED"
            and call_row["usage_completeness"] == "COMPLETE"
            and call_row["settlement_state"] == "SETTLED"
            and replay["receipt"]["task_id"] == task_sql
            and final_upstream_count == 1
            and after_replay == before_replay
        )
        if local_smoke:
            _local_ledger = _read_local_smoke_ledger()
            qwen_answer_check = _is_expected_korean_arithmetic_response(task_view.get("response"))
            provider_usage_matches_database = bool(
                len(provider_response_observations) == 1
                and provider_response_observations[0].get("provider_usage") == {
                    "prompt_tokens": call_row["input_tokens"],
                    "completion_tokens": call_row["output_tokens"],
                    "total_tokens": call_row["total_tokens"],
                }
            )
            report["results"]["pinned_runtime_turn"].update({
                "loaded_context_verified": bool(
                    isinstance(report.get("ollama_execution_context"), dict)
                    and report["ollama_execution_context"].get("loaded_runner", {}).get("context_verified_for_this_loaded_runner")
                ),
                "provider_usage_matches_database": provider_usage_matches_database,
                "provider_usage_input_matches_measurement": input_token_delta == 0,
                "expected_arithmetic_answer_observed": qwen_answer_check,
                "durable_single_attempt_ledger": {
                    "path": LOCAL_SMOKE_LEDGER.relative_to(ROOT).as_posix(),
                    "state": _local_ledger.get("state") if _local_ledger else None,
                    "model_load_attempts": _local_ledger.get("model_load_attempts") if _local_ledger else None,
                    "upstream_attempts": _local_ledger.get("upstream_attempts") if _local_ledger else None,
                },
            })
            local_success = bool(
                common_success
                and report["results"]["pinned_runtime_turn"]["loaded_context_verified"]
                and len(provider_authorization_attempts) == 1
                and len(consumed_permits) == 1
                and consumed_permits[0].get("consumed") is True
                and len(provider_send_observations) == 1
                and len(provider_response_observations) == 1
                and provider_response_observations[0].get("http_status") == 200
                and provider_response_observations[0].get("finish_reason") in {"stop", "length"}
                and provider_usage_matches_database
                and input_token_delta == 0
                and provider_bridge_content_match
                and isinstance(stream_observation, dict)
                and stream_observation.get("done_seen") is True
                and qwen_answer_check
                and requested_output <= candidate.max_output_tokens
                and _local_ledger is not None
                and _local_ledger.get("upstream_attempts") == 1
            )
        else:
            fake_shape = fake_request_shapes[0] if fake_request_shapes else {}
            expected_fake_requests = 2 if output_delivery_reproduction else 1
            def fake_shape_matches_contract(shape: dict[str, object]) -> bool:
                return bool(
                    shape.get("task_capsule_present") is True
                    and shape.get("output_schema_in_prompt") is True
                    and shape.get("structured_output_grammar_sent") is True
                    and shape.get("response_schema_sha256") == output_schema_digest
                    and shape.get("temperature") == 0
                    and shape.get("store_field_forwarded") is False
                    and shape.get("tool_names") == []
                    and isinstance(shape.get("output_limit_tokens"), int)
                    and 4 <= shape.get("output_limit_tokens", 0) <= candidate.max_output_tokens
                )
            report["results"]["pinned_runtime_turn"].update({
                "fake_context_and_policy_verified": bool(fake_request_shapes)
                    and all(fake_shape_matches_contract(shape) for shape in fake_request_shapes),
            })
            if captured_replay:
                local_success = bool(
                    local_success
                    and fake is not None
                    and fake.count() == expected_fake_requests
                    and len(fake_request_shapes) == expected_fake_requests
                    and report["results"]["pinned_runtime_turn"]["fake_context_and_policy_verified"]
                    and all(fake_shape_matches_contract(shape) for shape in fake_request_shapes)
                )
            else:
                local_success = bool(
                    common_success
                    and fake is not None
                    and fake.count() == expected_fake_requests
                    and len(fake_request_shapes) == expected_fake_requests
                    and report["results"]["pinned_runtime_turn"]["fake_context_and_policy_verified"]
                    and all(fake_shape_matches_contract(shape) for shape in fake_request_shapes)
                    and (
                        not output_delivery_reproduction
                        or (
                            incomplete_delivery is not None
                            and report["results"]["output_delivery_reproduction"]["terminal_content_comparison"] == {
                                "complete_task_accepted": True,
                                "incomplete_task_rejected": True,
                                "complete_fixture_matches_historical_size_count_and_terminal_shape": True,
                                "incomplete_fixture_is_one_byte_short_and_uses_same_frame_terminal": True,
                            }
                        )
                    )
                )
        if not local_success:
            raise AssertionError("pinned runtime, provider permit, or replay contract did not converge")
        report["overall_status"] = "pass"
    except Exception as error:
        report["failure_stage"] = stage
        report["error"] = f"{type(error).__name__}: {str(error)[:1200]}"
        ledger = _local_ledger_summary() if local_smoke else None
        report["overall_status"] = (
            "failed_after_single_upstream_attempt"
            if isinstance(ledger, dict) and ledger.get("upstream_attempts") == 1
            else "blocked"
        )
    finally:
        stop.set()
        if worker_task is not None:
            try:
                await asyncio.wait_for(worker_task, timeout=15)
            except Exception as error:
                report["worker_shutdown_error"] = f"{type(error).__name__}: {str(error)[:300]}"
        if gateway_server is not None:
            gateway_server.should_exit = True
        if gateway_task is not None:
            try:
                await asyncio.wait_for(gateway_task, timeout=10)
            except Exception as error:
                report["gateway_shutdown_error"] = f"{type(error).__name__}: {str(error)[:300]}"
        if bridge is not None:
            await bridge.close()
        if fake is not None:
            report["fake_provider_requests"] = fake.count()
            report["fake_sse_http_observations"] = fake.sse_response_observations
            fake.stop()
        if sandbox is not None:
            sandbox.stop()
            try:
                sandbox.clear_state()
            except Exception as error:
                report["sandbox_state_cleanup_error"] = f"{type(error).__name__}: {str(error)[:300]}"
        if engine is not None:
            await engine.dispose()
        try:
            temporary.cleanup()
        except OSError as error:
            report["temporary_cleanup_error"] = f"{type(error).__name__}: {str(error)[:300]}"
        p3.FAKE_MODEL = old_model
        gateway_module.authorize_provider_call = original_authorize_provider_call
        gateway_module.consume_call_permit = original_consume_call_permit
        if restore_local_upstream_guard is not None:
            restore_local_upstream_guard()
        report["executed_at_finished"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        fingerprint, files = _fingerprint()
        report["code_fingerprint_sha256"] = fingerprint
        report["fingerprinted_files"] = files
        report["artifact"] = artifact.relative_to(ROOT).as_posix()
        ledger = _local_ledger_summary() if local_smoke else None
        ledger_full = _read_local_smoke_ledger() if local_smoke else None
        report["single_attempt_ledger"] = ledger
        report["upstream_generation_attempts"] = len(provider_send_observations) if local_smoke else 0
        if local_smoke:
            report["unexecuted"] = _local_unexecuted_boundaries(report["upstream_generation_attempts"])
        report["gateway_forward_counter"] = (
            int(gateway_app.state.metrics["upstream_forward_attempts"])
            if local_smoke and gateway_app is not None else 0
        )
        reached_response = bool(
            provider_response_observations
            or isinstance((ledger_full or {}).get("response"), dict)
            and isinstance((ledger_full or {}).get("response", {}).get("http_status"), int)
        )
        report["real_provider_calls"] = 1 if local_smoke and reached_response else 0
        report["actual_provider_calls"] = report["real_provider_calls"]
        report["generation_completion_confirmed"] = bool(
            local_smoke
            and provider_response_observations
            and provider_response_observations[-1].get("http_status") == 200
            and provider_response_observations[-1].get("finish_reason") in {"stop", "length"}
        )
        if local_smoke and report.get("overall_status") == "pass":
            final_result = report.get("results", {}).get("pinned_runtime_turn", {})
            ledger_updated = _update_ledger(run_id, {
                "state": "TASK_COMPLETED_USAGE_SETTLED_AND_REPLAY_CHECKED",
                "finished_at": report["executed_at_finished"],
                "task_id": report.get("task_observation", {}).get("task_id"),
                "provider_generation_completed": report["generation_completion_confirmed"],
                "usage_settlement_state": final_result.get("postgresql_call_and_permit", {}).get("settlement_state"),
            })
            report["single_attempt_ledger"] = {
                "run_id": ledger_updated.get("run_id"),
                "state": ledger_updated.get("state"),
                "model_load_attempts": ledger_updated.get("model_load_attempts"),
                "upstream_attempts": ledger_updated.get("upstream_attempts"),
            }
        report["metadata_observation"] = report.get("live_ollama_metadata", {
            "metadata_requests": 0, "inference_requests": 0, "status": "not_observed",
        })
        report["provider_authorization_attempts"] = provider_authorization_attempts
        report["consumed_permits"] = consumed_permits
        report["provider_send_observations"] = provider_send_observations
        report["provider_response_observations"] = provider_response_observations
        report["fake_request_shapes"] = fake_request_shapes
        report["fake_fixture_failures"] = fake_fixture_failures
        if captured_sse_replay_observations:
            report["captured_sse_replay_observations"] = captured_sse_replay_observations
        if capture_paths:
            capture_summaries = {name: _capture_file_summary(path) for name, path in capture_paths.items()}
            diagnostics = report.get("output_observation_diagnostics")
            if isinstance(diagnostics, dict):
                diagnostics["file_summaries"] = capture_summaries
                diagnostics["assistant_raw_limit_bytes"] = 65_536
                diagnostics["provider_sse_limit_bytes"] = 2_097_152
                diagnostics["reasoning_raw_retained"] = False
            if local_smoke or captured_replay:
                turn = report.get("results", {}).get("pinned_runtime_turn")
                if isinstance(turn, dict):
                    output_chain = _build_actual_output_observation(turn, capture_paths)
                    turn["output_delivery_chain"] = output_chain
                    turn["provider_response_is_valid_json"] = output_chain.get("provider_choice_0_json_parse", {}).get("valid_json")
                    turn["first_output_divergence"] = output_chain.get("first_divergence")
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return report


def main() -> int:
    global LOCAL_SMOKE_LEDGER
    parser = argparse.ArgumentParser(description="Run Qwen or output-delivery verification through PostgreSQL and the pinned App Server.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--node-bin", required=True)
    parser.add_argument("--node-archive", type=Path, default=Path("/tmp/hekate-node-v22.19.0-linux-x64.tar.xz"))
    parser.add_argument("--image", default=f"hekate/letta-code-p1:{p1.LOCK['app_server']['source_commit'][:8]}-{p1.LOCK['patches'][0]['sha256'][:8]}")
    parser.add_argument("--artifact", type=Path)
    parser.add_argument(
        "--execute-local-ollama", action="store_true",
        help="run the one permitted local Qwen generation attempt after live context verification",
    )
    parser.add_argument(
        "--authorized-native-schema-followup", action="store_true",
        help="use a new fixed one-shot ledger after native-schema offline and fake-provider verification",
    )
    parser.add_argument(
        "--authorized-output-observation-attempt", action="store_true",
        help="authorize one fresh actual Qwen Task with durable bounded provider-to-PostgreSQL output observation",
    )
    parser.add_argument(
        "--authorized-output-observation-followup", action="store_true",
        help="use a distinct one-shot ledger for the current user-authorized independent output-observation Task",
    )
    parser.add_argument(
        "--output-delivery-reproduction", action="store_true",
        help="run complete and incomplete multi-delta SSE fixtures through the pinned fake-provider runtime; never call Ollama",
    )
    parser.add_argument(
        "--replay-captured-sse", type=Path,
        help="replay one previously captured complete actual SSE response through the pinned fake provider; never call Ollama",
    )
    parser.add_argument(
        "--bridge-request-timeout-ms", type=int, default=240_000,
        help="pinned SDK turn timeout for isolated replay comparison (1 through 240000 ms)",
    )
    args = parser.parse_args()
    if args.output_delivery_reproduction and (args.execute_local_ollama or args.replay_captured_sse):
        parser.error("output-delivery fixtures and captured SSE replay are separate fake-provider-only modes")
    if args.replay_captured_sse and args.execute_local_ollama:
        parser.error("captured response replay is fake-provider-only and cannot run local Ollama")
    if not 1 <= args.bridge_request_timeout_ms <= 240_000:
        parser.error("--bridge-request-timeout-ms must be from 1 through 240000")
    if args.authorized_native_schema_followup and not args.execute_local_ollama:
        parser.error("native-schema authorization applies only to an actual local Ollama smoke")
    if args.authorized_output_observation_attempt and not args.execute_local_ollama:
        parser.error("output observation authorization applies only to an actual local Ollama Task")
    if args.authorized_output_observation_followup and not args.execute_local_ollama:
        parser.error("output-observation follow-up authorization applies only to an actual local Ollama Task")
    if sum((args.authorized_output_observation_attempt, args.authorized_output_observation_followup, args.authorized_native_schema_followup)) > 1:
        parser.error("each actual Task must use its own single-attempt authorization mode")
    parsed = make_url(args.database_url)
    if parsed.host not in {"127.0.0.1", "localhost"} or not str(parsed.database).startswith("hekate_phase6c_"):
        parser.error("--database-url must target a fresh loopback hekate_phase6c_* database")
    captured_sse_replay = None
    if args.replay_captured_sse:
        if not str(parsed.database).startswith("hekate_phase6c_replay_"):
            parser.error("captured SSE replay requires a new hekate_phase6c_replay_* database")
        if args.authorized_native_schema_followup or args.authorized_output_observation_attempt or args.authorized_output_observation_followup:
            parser.error("actual-attempt authorizations cannot be combined with fake captured-SSE replay")
        try:
            captured_sse_replay = _load_captured_actual_sse(args.replay_captured_sse)
        except Exception as error:
            parser.error(f"captured SSE replay source failed closed: {type(error).__name__}: {error}")
    if args.execute_local_ollama:
        if not str(parsed.database).startswith("hekate_phase6c_smoke_"):
            parser.error("local smoke requires a separately created hekate_phase6c_smoke_* database")
        local_authorization = None
        if args.authorized_output_observation_followup:
            try:
                local_authorization = _authorize_output_observed_followup_attempt()
            except Exception as error:
                parser.error(f"output-observed follow-up authorization failed closed: {type(error).__name__}: {error}")
            if parsed.database == local_authorization.get("prior_database"):
                parser.error("output-observed follow-up requires a different database")
            LOCAL_SMOKE_LEDGER = OUTPUT_OBSERVED_FOLLOWUP_LOCAL_SMOKE_LEDGER
            if LOCAL_SMOKE_LEDGER.exists():
                parser.error("the output-observed follow-up one-attempt ledger already exists; no rerun is allowed")
        elif args.authorized_output_observation_attempt:
            try:
                local_authorization = _authorize_output_observed_independent_attempt()
            except Exception as error:
                parser.error(f"output-observed attempt authorization failed closed: {type(error).__name__}: {error}")
            if parsed.database == local_authorization.get("prior_database"):
                parser.error("output-observed Qwen attempt requires a different database")
            LOCAL_SMOKE_LEDGER = OUTPUT_OBSERVED_LOCAL_SMOKE_LEDGER
            if LOCAL_SMOKE_LEDGER.exists():
                parser.error("the output-observed one-attempt ledger already exists; no rerun is allowed")
        elif args.authorized_native_schema_followup:
            try:
                local_authorization = _authorize_native_schema_attempt_after_failed_output()
            except Exception as error:
                parser.error(f"native-schema follow-up authorization failed closed: {type(error).__name__}: {error}")
            if parsed.database == local_authorization.get("prior_database"):
                parser.error("the native-schema follow-up requires a different database")
            LOCAL_SMOKE_LEDGER = NATIVE_SCHEMA_LOCAL_SMOKE_LEDGER
            if LOCAL_SMOKE_LEDGER.exists():
                parser.error("the native-schema one-attempt ledger already exists; no rerun is allowed")
        else:
            LOCAL_SMOKE_LEDGER = ROOT / "integration/runtime/artifacts/p6d-qwen-single-attempt-ledger.json"
            prior = _local_ledger_summary()
            if prior is not None:
                parser.error(
                    "the one-go local smoke ledger already exists; no rerun is allowed "
                    f"(run_id={prior.get('run_id')}, state={prior.get('state')}, "
                    f"model_load_attempts={prior.get('model_load_attempts')}, "
                    f"upstream_attempts={prior.get('upstream_attempts')})"
                )
    else:
        local_authorization = None
    run_id = f"p6d-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    artifact_suffix = (
        "output-observed-qwen-followup" if args.authorized_output_observation_followup else
        "output-observed-qwen" if args.authorized_output_observation_attempt else
        "output-delivery" if args.output_delivery_reproduction else
        f"captured-sse-{args.bridge_request_timeout_ms}ms" if args.replay_captured_sse else
        "ollama-qwen-runtime"
    )
    artifact = args.artifact or ROOT / "integration/runtime/artifacts" / f"{run_id}-{artifact_suffix}.json"
    if not artifact.is_absolute():
        artifact = ROOT / artifact
    artifact = artifact.resolve()
    if artifact.parent != (ROOT / "integration/runtime/artifacts").resolve() or artifact.exists():
        parser.error("artifact must be a new path under integration/runtime/artifacts")
    try:
        _assert_database_empty_before_migration(args.database_url)
    except Exception as error:
        parser.error(f"database is not an empty disposable database; no migration was applied: {type(error).__name__}: {error}")
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", args.database_url.replace("%", "%%"))
    os.environ["HEKATE_DATABASE_URL"] = args.database_url
    command.upgrade(cfg, "head")
    report = asyncio.run(_run(
        args.database_url, args.node_bin, args.image, args.node_archive, artifact, run_id,
        local_smoke=args.execute_local_ollama,
        output_delivery_reproduction=args.output_delivery_reproduction,
        local_authorization=local_authorization,
        output_observation_diagnostics=args.authorized_output_observation_attempt or args.authorized_output_observation_followup,
        captured_sse_replay=captured_sse_replay,
        bridge_request_timeout_ms=args.bridge_request_timeout_ms,
    ))
    print(json.dumps({
        "artifact": report.get("artifact"),
        "status": report.get("overall_status"),
        "fake_provider_requests": report.get("fake_provider_requests"),
        "real_provider_calls": report.get("real_provider_calls"),
        "upstream_generation_attempts": report.get("upstream_generation_attempts"),
        "error": report.get("error"),
    }, sort_keys=True))
    return 0 if report.get("overall_status") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
