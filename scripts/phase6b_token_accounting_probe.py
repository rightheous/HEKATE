from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]

_FINGERPRINT_FILES = (
    "pyproject.toml",
    "uv.lock",
    "config/models.yaml",
    "config/pricing.yaml",
    "src/hekate/application/budgets.py",
    "src/hekate/application/turns.py",
    "src/hekate/domain/budget_math.py",
    "src/hekate/domain/models.py",
    "src/hekate/settings.py",
    "src/hekate/infrastructure/letta/provider_gateway.py",
    "src/hekate/infrastructure/letta/token_accounting.py",
    "src/hekate/infrastructure/letta/assets/cl100k_base.tiktoken",
    "src/hekate/infrastructure/letta/assets/test_chat_contract.v1.json",
    "src/hekate/infrastructure/postgres/budget_repository.py",
    "src/hekate/infrastructure/postgres/tables.py",
    "migrations/versions/0013_provider_request_measurements.py",
    "scripts/phase3_runtime_probe.py",
    "scripts/phase5b_bounded_deliberation_probe.py",
    "scripts/phase6a_memory_projection_probe.py",
    "scripts/phase6b_token_accounting_probe.py",
    "tests/contract/test_phase3_bridge_and_gateway.py",
    "tests/persistence/test_phase2_postgres.py",
    "tests/unit/test_token_accounting.py",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fingerprint() -> tuple[str, list[dict[str, object]]]:
    rows: list[dict[str, object]] = []
    combined = hashlib.sha256()
    relatives = set(_FINGERPRINT_FILES)
    for root in (ROOT / "src/hekate", ROOT / "migrations/versions"):
        for path in root.rglob("*"):
            if path.is_file() and (path.suffix == ".py" or "assets" in path.parts):
                relatives.add(path.relative_to(ROOT).as_posix())
    for root in (ROOT / "tests/unit", ROOT / "tests/contract"):
        relatives.update(path.relative_to(ROOT).as_posix() for path in root.rglob("*.py") if path.is_file())
    docs = ROOT / "docs/implementation/phase6b-token-accounting.md"
    if docs.is_file():
        relatives.add(docs.relative_to(ROOT).as_posix())
    for relative in sorted(relatives):
        path = ROOT / relative
        payload = path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        rows.append({"path": relative, "bytes": len(payload), "sha256": digest})
        combined.update(relative.encode("utf-8") + b"\0" + bytes.fromhex(digest))
    return combined.hexdigest(), rows


def _validate_databases(values: dict[str, str]) -> dict[str, str]:
    names: dict[str, str] = {}
    for label, url in values.items():
        parsed = make_url(url)
        if parsed.host not in {"127.0.0.1", "localhost"} or not parsed.database:
            raise ValueError(f"{label} must use a named loopback PostgreSQL database")
        if label.startswith("p3_") and not parsed.database.startswith("hekate_phase3_"):
            raise ValueError("Phase 3 regression database must use hekate_phase3_* name")
        if label.startswith("p6a_") and not parsed.database.startswith("hekate_phase6a_"):
            raise ValueError("Phase 6A regression databases must use hekate_phase6a_* names")
        if label.startswith("p5b_") and not parsed.database.startswith("hekate_phase5b_"):
            raise ValueError("Phase 5B regression databases must use hekate_phase5b_* names")
        if parsed.database in names:
            raise ValueError(f"{label} and {names[parsed.database]} must be distinct databases")
        names[parsed.database] = label
    return names


def _run_child(label: str, script: str, arguments: list[str], artifact: Path,
               environment: dict[str, str], database_url_values: list[str]) -> dict[str, object]:
    command = [sys.executable, str(ROOT / script), *arguments, "--artifact", str(artifact)]
    completed = subprocess.run(
        command, cwd=ROOT, env=environment, text=True, capture_output=True, timeout=2400, check=False,
    )
    stdout = completed.stdout[-3_000:]
    stderr = completed.stderr[-3_000:]
    for secret_url in database_url_values:
        stdout = stdout.replace(secret_url, "<redacted-database-url>")
        stderr = stderr.replace(secret_url, "<redacted-database-url>")
    child_report = json.loads(artifact.read_text(encoding="utf-8")) if artifact.is_file() else None
    status = child_report.get("overall_status") if isinstance(child_report, dict) else None
    if completed.returncode != 0 and status is None:
        status = "failed_without_artifact"
    return {
        "label": label,
        "script": script,
        "redacted_command": [Path(command[0]).name, script, "<isolated-database-arguments>", "--artifact", artifact.relative_to(ROOT).as_posix()],
        "exit_code": completed.returncode,
        "overall_status": status,
        "artifact": artifact.relative_to(ROOT).as_posix() if artifact.is_file() else None,
        "stdout_tail": stdout,
        "stderr_tail": stderr,
        "report": child_report,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 6B token-accounting checks and affected runtime regressions.")
    parser.add_argument("--phase3-database-url", required=True)
    parser.add_argument("--phase6a-database-url", required=True)
    parser.add_argument("--phase6a-empty-migration-database-url", required=True)
    parser.add_argument("--phase5b-database-url", required=True)
    parser.add_argument("--phase5b-empty-migration-database-url", required=True)
    parser.add_argument("--node-bin", required=True)
    parser.add_argument("--node-archive", type=Path, default=Path("/tmp/hekate-node-v22.19.0-linux-x64.tar.xz"))
    parser.add_argument("--reuse-phase3-artifact", type=Path)
    parser.add_argument("--reuse-phase6a-artifact", type=Path)
    parser.add_argument("--reuse-phase5b-artifact", type=Path)
    parser.add_argument("--phase3-verifier-artifact", type=Path)
    lock = json.loads((ROOT / "integration/letta/versions.lock.json").read_text(encoding="utf-8"))
    parser.add_argument(
        "--image",
        default=f"hekate/letta-code-p1:{lock['app_server']['source_commit'][:8]}-{lock['patches'][0]['sha256'][:8]}",
    )
    parser.add_argument("--artifact", type=Path)
    args = parser.parse_args()

    databases = {
        "p3_runtime": args.phase3_database_url,
        "p6a_runtime": args.phase6a_database_url,
        "p6a_empty": args.phase6a_empty_migration_database_url,
        "p5b_runtime": args.phase5b_database_url,
        "p5b_empty": args.phase5b_empty_migration_database_url,
    }
    database_names = _validate_databases(databases)
    run_id = f"p6b-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    artifact = args.artifact or ROOT / "integration/runtime/artifacts" / f"{run_id}.json"
    if not artifact.is_absolute():
        artifact = ROOT / artifact
    artifact = artifact.resolve()
    if artifact.parent != (ROOT / "integration/runtime/artifacts").resolve():
        parser.error("artifact path must remain under integration/runtime/artifacts")
    if artifact.exists():
        parser.error("artifact path already exists; choose a new UTC artifact name")

    environment = os.environ.copy()
    environment["PATH"] = f"{Path(args.node_bin).resolve().parent}{os.pathsep}{environment.get('PATH', '')}"
    children: list[dict[str, object]] = []
    urls = list(databases.values())
    stage_specs = (
        (
            "phase3_runtime_and_final_gateway_guards", "scripts/phase3_runtime_probe.py",
            ["--database-url", args.phase3_database_url, "--node-bin", args.node_bin,
             "--node-archive", str(args.node_archive), "--image", args.image],
            f"{run_id}-phase3.json",
        ),
        (
            "phase6a_projection_and_full_request_regression", "scripts/phase6a_memory_projection_probe.py",
            ["--database-url", args.phase6a_database_url,
             "--empty-migration-database-url", args.phase6a_empty_migration_database_url,
             "--node-bin", args.node_bin, "--node-archive", str(args.node_archive), "--image", args.image],
            f"{run_id}-phase6a.json",
        ),
        (
            "phase5b_deliberation_and_unknown_hold_regression", "scripts/phase5b_bounded_deliberation_probe.py",
            ["--database-url", args.phase5b_database_url,
             "--empty-migration-database-url", args.phase5b_empty_migration_database_url,
             "--node-bin", args.node_bin, "--node-archive", str(args.node_archive), "--image", args.image],
            f"{run_id}-phase5b.json",
        ),
    )
    reuse_paths = {
        "phase3_runtime_and_final_gateway_guards": args.reuse_phase3_artifact,
        "phase6a_projection_and_full_request_regression": args.reuse_phase6a_artifact,
        "phase5b_deliberation_and_unknown_hold_regression": args.reuse_phase5b_artifact,
    }
    if any(path is not None for path in reuse_paths.values()):
        if any(path is None for path in reuse_paths.values()):
            parser.error("reuse mode requires all three completed child artifacts")
        for label, source_path in reuse_paths.items():
            source = source_path.resolve()
            if ROOT not in source.parents or not source.is_file():
                parser.error(f"reused child artifact is missing or outside the repository: {source_path}")
            child_report = json.loads(source.read_text(encoding="utf-8"))
            children.append({
                "label": label,
                "script": child_report.get("probe"),
                "redacted_command": ["reuse_completed_child_artifact", source.relative_to(ROOT).as_posix()],
                "exit_code": 0 if child_report.get("overall_status") == "pass" else 1,
                "overall_status": child_report.get("overall_status"),
                "artifact": source.relative_to(ROOT).as_posix(),
                "stdout_tail": "",
                "stderr_tail": "",
                "report": child_report,
            })
    else:
        for label, script, command_arguments, child_name in stage_specs:
            child_artifact = ROOT / "integration/runtime/artifacts" / child_name
            if child_artifact.exists():
                raise FileExistsError(child_artifact)
            children.append(_run_child(label, script, command_arguments, child_artifact, environment, urls))

    reports = {str(child["label"]): child.get("report") for child in children}
    p3 = reports.get("phase3_runtime_and_final_gateway_guards") or {}
    p6a = reports.get("phase6a_projection_and_full_request_regression") or {}
    p5b = reports.get("phase5b_deliberation_and_unknown_hold_regression") or {}
    p3_results = p3.get("results", {}) if isinstance(p3, dict) else {}
    p6b_measurement = p6a.get("phase6b_token_accounting", {}) if isinstance(p6a, dict) else {}
    phase3_verifier: dict[str, object] | None = None
    phase3_verifier_ok = False
    if args.phase3_verifier_artifact is not None:
        verifier_path = args.phase3_verifier_artifact.resolve()
        if ROOT not in verifier_path.parents or not verifier_path.is_file():
            parser.error("Phase 3 verifier artifact is missing or outside the repository")
        verifier_report = json.loads(verifier_path.read_text(encoding="utf-8"))
        verification = verifier_report.get("verification") or {}
        phase3b_relative = verification.get("phase3b_artifact")
        phase3b_path = ROOT / phase3b_relative if isinstance(phase3b_relative, str) else None
        phase3b_report = json.loads(phase3b_path.read_text(encoding="utf-8")) if phase3b_path and phase3b_path.is_file() else {}
        phase3_verifier_ok = bool(
            verifier_report.get("overall_status") == "pass"
            and verifier_report.get("real_provider_calls") == 0
            and verification.get("status") == "passed"
            and verification.get("commands_failed") == 0
            and phase3b_report.get("overall_status") == "pass"
            and (phase3b_report.get("verification") or {}).get("status") == "passed"
        )
        phase3_verifier = {
            "artifact": verifier_path.relative_to(ROOT).as_posix(),
            "phase3b_artifact": phase3b_relative,
            "verification_status": verification.get("status"),
            "commands_run": verification.get("commands_run"),
            "commands_passed": verification.get("commands_passed"),
            "commands_failed": verification.get("commands_failed"),
            "phase3_scenarios_passed": verification.get("runtime_scenarios", {}).get("passed_count"),
            "phase3_scenarios_failed": verification.get("runtime_scenarios", {}).get("failed_count"),
            "phase3b_scenarios_passed": verification.get("phase3b_runtime_scenarios", {}).get("passed_count"),
            "phase3b_scenarios_failed": verification.get("phase3b_runtime_scenarios", {}).get("failed_count"),
            "real_provider_calls": verifier_report.get("real_provider_calls"),
        }
    checks = {
        "t1_input_and_context_boundaries": bool(
            isinstance(p3_results, dict)
            and (p3_results.get("T8_phase6b_final_request_token_guards") or {}).get("passed")
        ),
        "t2_profile_and_asset_fail_closed": bool(
            isinstance(p3_results, dict)
            and (p3_results.get("T7_private_gateway_fail_closed") or {}).get("passed")
            and (p3_results.get("T8_phase6b_final_request_token_guards") or {}).get("unsupported_input", {}).get("status") == 402
        ),
        "t3_request_and_profile_binding": bool(
            isinstance(p3_results, dict)
            and (p3_results.get("T8_phase6b_final_request_token_guards") or {}).get("passed")
            and (p3_results.get("T1_same_accounting_id_replay") or {}).get("passed")
        ),
        "t4_actual_runtime_full_request_and_memory": bool(
            p6a.get("overall_status") == "pass"
            and p6b_measurement.get("raw_gateway_request_digest_matches_fake_upstream") is True
            and p6b_measurement.get("independent_tiktoken_count_matches_gateway") is True
            and p6b_measurement.get("permit_measurements_match_call_measurements") is True
        ),
        "t5_actual_runtime_compaction": bool(
            isinstance(p3_results, dict)
            and (p3_results.get("T2_actual_runtime_compaction") or {}).get("passed")
            and (p3_results.get("T2_actual_runtime_compaction") or {}).get("all_physical_calls_measured")
        ),
        "t6_phase_restarts_and_accounting_regressions": bool(
            p6a.get("overall_status") == "pass" and p5b.get("overall_status") == "pass"
        ),
        "zero_real_provider_calls_and_production_blocked": bool(
            isinstance(p3, dict) and p3.get("real_provider_calls") == 0
            and (p3_results.get("T7_private_gateway_fail_closed") or {}).get("production_settings_rejected_without_profile") is True
            and isinstance(p6a, dict) and p6a.get("real_provider_calls") == 0
            and p6a.get("production_dispatch") == "blocked"
            and isinstance(p5b, dict) and p5b.get("real_provider_calls") == 0
            and (p5b.get("dispatch_safety") or {}).get("production_dispatch") == "blocked"
        ),
        "phase3_legacy_verifier": phase3_verifier_ok,
    }
    fingerprint, files = _fingerprint()
    cfg_hashes = {
        name: _sha256(ROOT / f"config/{name}.yaml") for name in ("models", "pricing")
    }
    report = {
        "schema_version": "1",
        "probe": "phase6b-provider-token-accounting",
        "run_id": run_id,
        "executed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "baseline_head": "5ed7acba82ee6d3633d7f22e3a195a8534f25a34",
        "branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip(),
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "code_fingerprint_sha256": fingerprint,
        "file_digests": files,
        "configuration_digests": cfg_hashes,
        "configuration_state": {"models": "UNCONFIGURED_FOR_PRODUCTION", "pricing": "UNCONFIGURED_FOR_PRODUCTION"},
        "database_names": database_names,
        "checks": checks,
        "overall_status": "pass" if all(checks.values()) and all(item.get("overall_status") == "pass" for item in children) else "blocked",
        "test_profile": {
            "verification_state": p6b_measurement.get("verification_state"),
            "production_profile_state": p6b_measurement.get("production_profile_state"),
            "profile_id": p6b_measurement.get("profile_id"),
            "profile_digest": p6b_measurement.get("profile_digest"),
            "tokenizer_asset": "src/hekate/infrastructure/letta/assets/cl100k_base.tiktoken",
            "tokenizer_sha256": _sha256(ROOT / "src/hekate/infrastructure/letta/assets/cl100k_base.tiktoken"),
            "renderer_contract": "src/hekate/infrastructure/letta/assets/test_chat_contract.v1.json",
            "renderer_contract_sha256": _sha256(ROOT / "src/hekate/infrastructure/letta/assets/test_chat_contract.v1.json"),
        },
        "token_measurements": {
            "phase3_boundary": p3_results.get("T8_phase6b_final_request_token_guards") if isinstance(p3_results, dict) else None,
            "phase3_compaction": p3_results.get("T2_actual_runtime_compaction") if isinstance(p3_results, dict) else None,
            "phase6a_runtime_calls": p6b_measurement.get("calls"),
            "phase6a_independent_request_checks": {
                key: p6b_measurement.get(key) for key in (
                    "measured_physical_calls", "permit_measurements_match_call_measurements",
                    "raw_gateway_request_digest_matches_fake_upstream", "independent_tiktoken_count_matches_gateway",
                    "preflight_measurement_is_separate_from_provider_usage",
                )
            },
        },
        "accounting_and_deferred_states": {
            "phase3_pending_call_reasons": (p3.get("observed_totals") or {}).get("pending_call_reasons") if isinstance(p3, dict) else None,
            "phase5b_artifact": next((item.get("artifact") for item in children if item["label"].startswith("phase5b_")), None),
            "phase6a_isolated_database_unsettled_calls": (p6a.get("accounting") or {}).get("unknown_or_unsettled_rows_in_isolated_phase6a_database") if isinstance(p6a, dict) else None,
            "production_dispatch": "blocked",
            "actual_provider_api_calls": 0,
            "g7_same_execution_resume": "deferred",
            "g8_production_tokenization": "blocked_pending_model_and_request_semantics_evidence",
        },
        "children": [{key: value for key, value in item.items() if key != "report"} | {
            "database": (item.get("report") or {}).get("database") if isinstance(item.get("report"), dict) else None,
            "fake_provider_requests": (item.get("report") or {}).get("fake_provider_requests")
            or ((item.get("report") or {}).get("accounting") or {}).get("fake_provider_requests")
            or ((item.get("report") or {}).get("observed_totals") or {}).get("fake_provider_forward_requests"),
            "real_provider_calls": (item.get("report") or {}).get("real_provider_calls") if isinstance(item.get("report"), dict) else None,
        } for item in children],
        "phase3_verifier": phase3_verifier,
        "commands": [item["redacted_command"] for item in children] + (
            [["reuse_phase3_verifier_artifact", phase3_verifier["artifact"]]] if phase3_verifier else []
        ),
        "child_artifacts_reused": any(
            item["redacted_command"][0] == "reuse_completed_child_artifact" for item in children
        ),
        "unexecuted": [
            "No production-model tokenizer/framing/pricing verification or external provider request was run.",
            "Generated JSON schemas are unchanged and were not regenerated.",
        ],
    }
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"artifact": artifact.relative_to(ROOT).as_posix(), "status": report["overall_status"], "real_provider_calls": 0}, sort_keys=True))
    return 0 if report["overall_status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
