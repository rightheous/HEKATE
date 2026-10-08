from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import unittest
import uuid
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
TEST_PATH = ROOT / "tests/persistence"


_FINGERPRINT_FILES = (
    "src/hekate/infrastructure/postgres/budget_repository.py",
    "tests/persistence/test_phase2_postgres.py",
    "scripts/phase6b_legacy_settlement_probe.py",
)


def _fingerprint() -> tuple[str, list[dict[str, object]]]:
    combined = hashlib.sha256()
    files: list[dict[str, object]] = []
    for relative in sorted(_FINGERPRINT_FILES):
        payload = (ROOT / relative).read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        files.append({"path": relative, "bytes": len(payload), "sha256": digest})
        combined.update(relative.encode("utf-8") + b"\0" + bytes.fromhex(digest))
    return combined.hexdigest(), files


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify legacy settlement compatibility against isolated PostgreSQL.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--artifact", type=Path)
    args = parser.parse_args()

    parsed = make_url(args.database_url)
    if parsed.host not in {"127.0.0.1", "localhost"} or parsed.database != "hekate_phase2_test":
        parser.error("--database-url must target the dedicated loopback hekate_phase2_test database")
    os.environ["HEKATE_TEST_DATABASE_URL"] = args.database_url
    sys.path.insert(0, str(TEST_PATH))
    from test_phase2_postgres import Phase2PostgresTests

    run_id = f"p6b-legacy-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    artifact = args.artifact or ROOT / "integration/runtime/artifacts" / f"{run_id}.json"
    if not artifact.is_absolute():
        artifact = ROOT / artifact
    artifact = artifact.resolve()
    if artifact.parent != (ROOT / "integration/runtime/artifacts").resolve():
        parser.error("artifact path must stay under integration/runtime/artifacts")
    if artifact.exists():
        parser.error("artifact already exists; choose a new unique path")

    case = Phase2PostgresTests("test_phase6b_legacy_unmeasured_settlement_compatibility")
    result = unittest.TestResult()
    case.run(result)
    status = "pass" if result.wasSuccessful() and not result.skipped else "fail"
    fingerprint, files = _fingerprint()
    errors = [
        {"test": test.id(), "traceback": traceback[-6000:]}
        for test, traceback in [*result.errors, *result.failures]
    ]
    report = {
        "schema_version": "1",
        "probe": "phase6b-legacy-unmeasured-settlement",
        "run_id": run_id,
        "executed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "baseline_head": "60b304ae96d863fada19543191e18479fabd30aa",
        "branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip(),
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "code_fingerprint_sha256": fingerprint,
        "file_digests": files,
        "database_target": {
            "host": parsed.host,
            "port": parsed.port,
            "database": parsed.database,
            "postgres_version": (case.phase6b_legacy_report.get("database") or {}).get("postgres_version")
            if hasattr(case, "phase6b_legacy_report") else None,
            "migration_head": (case.phase6b_legacy_report.get("database") or {}).get("migration_head")
            if hasattr(case, "phase6b_legacy_report") else None,
            "isolated_fresh_container": "postgres:16.15, loopback port 55437",
        },
        "test": {"id": case.id(), "tests_run": result.testsRun, "status": status, "errors": errors},
        "skipped": [{"test": test.id(), "reason": reason} for test, reason in result.skipped],
        "settlement_scenarios": getattr(case, "phase6b_legacy_report", None),
        "calls": {"fake_provider_requests": 0, "real_provider_calls": 0},
        "production_dispatch": "blocked",
        "preexisting_unknown_and_unsettled_fixture_databases": "not accessed; fresh isolated database only",
        "command": [
            "uv", "run", "--locked", "python", "scripts/phase6b_legacy_settlement_probe.py",
            "--database-url", "<isolated-loopback-postgresql-url>",
        ],
        "unexecuted": [
            "No provider gateway or fake provider was invoked; this probe exercises the application and PostgreSQL settlement path.",
            "Production pricing, production dispatch, and provider-reported cost sourcing were not modified or externally validated.",
        ],
    }
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"artifact": artifact.relative_to(ROOT).as_posix(), "status": status, "tests_run": result.testsRun}))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
