from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from sqlalchemy.engine import make_url


ROOT = Path(__file__).resolve().parents[1]


def _record(name: str, display: str, command: list[str], env: dict[str, str], cwd: Path = ROOT) -> tuple[dict[str, object], subprocess.CompletedProcess[str]]:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=1800,
            check=False,
        )
        output = result.stdout + "\n" + result.stderr
        exit_code = result.returncode
    except subprocess.TimeoutExpired as error:
        output = str(error)
        exit_code = 124
    record: dict[str, object] = {
        "name": name,
        "command": display,
        "exit_code": exit_code,
        "status": "passed" if exit_code == 0 else "failed",
    }
    unittest = re.search(r"Ran (\d+) tests? in", output)
    tap = re.search(r"# tests (\d+).*?# pass (\d+).*?# fail (\d+)", output, re.S)
    schema = re.search(r"(\d+) schemas match generated output byte-for-byte", output)
    if unittest:
        count = int(unittest.group(1))
        record["tests_run"] = count
        record["tests_passed"] = count if exit_code == 0 else 0
        record["tests_failed"] = 0 if exit_code == 0 else count
    elif tap:
        record["tests_run"] = int(tap.group(1))
        record["tests_passed"] = int(tap.group(2))
        record["tests_failed"] = int(tap.group(3))
    elif schema:
        record["checks_run"] = int(schema.group(1))
        record["checks_passed"] = int(schema.group(1)) if exit_code == 0 else 0
        record["checks_failed"] = 0 if exit_code == 0 else int(schema.group(1))
    return record, result if 'result' in locals() else subprocess.CompletedProcess(command, exit_code, output, "")


def main() -> int:
    python = sys.executable
    phase3_database = os.environ.get("HEKATE_TEST_DATABASE_URL", "")
    phase2_database = os.environ.get("HEKATE_PHASE2_TEST_DATABASE_URL", "")
    node = os.environ.get("HEKATE_NODE_BIN", "")
    node_archive = os.environ.get("HEKATE_NODE_ARCHIVE", "")
    if not all((phase3_database, phase2_database, node, node_archive)):
        raise SystemExit("set HEKATE_TEST_DATABASE_URL, HEKATE_PHASE2_TEST_DATABASE_URL, HEKATE_NODE_BIN, and HEKATE_NODE_ARCHIVE")
    phase3_url = make_url(phase3_database)
    phase2_url = make_url(phase2_database)
    if phase3_url.database != "hekate_phase3_test" or phase3_url.host not in {"127.0.0.1", "localhost"}:
        raise SystemExit("HEKATE_TEST_DATABASE_URL must target the dedicated loopback hekate_phase3_test database")
    if phase2_url.database != "hekate_phase2_test" or phase2_url.host not in {"127.0.0.1", "localhost"}:
        raise SystemExit("HEKATE_PHASE2_TEST_DATABASE_URL must target the dedicated loopback hekate_phase2_test database")

    env = os.environ.copy()
    env["PATH"] = f"{Path(node).parent}:{env.get('PATH', '')}"
    records: list[dict[str, object]] = []

    def run(name: str, display: str, args: list[str], extra_env: dict[str, str] | None = None, cwd: Path = ROOT):
        command_env = env.copy()
        if extra_env:
            command_env.update(extra_env)
        record, result = _record(name, display, args, command_env, cwd)
        records.append(record)
        return record, result

    run("python_compile", "python -m compileall -q src scripts tests", [python, "-m", "compileall", "-q", "src", "scripts", "tests"])
    run(
        "phase3_contract_tests",
        "python -m unittest discover -s tests/contract -p test_phase3_bridge_and_gateway.py -v",
        [python, "-m", "unittest", "discover", "-s", "tests/contract", "-p", "test_phase3_bridge_and_gateway.py", "-v"],
    )
    run(
        "phase2_migration_upgrade",
        "HEKATE_DATABASE_URL=$HEKATE_PHASE2_TEST_DATABASE_URL python -m alembic upgrade head",
        [python, "-m", "alembic", "upgrade", "head"],
        {"HEKATE_DATABASE_URL": phase2_database},
    )
    run(
        "phase2_postgres_tests",
        "HEKATE_TEST_DATABASE_URL=$HEKATE_PHASE2_TEST_DATABASE_URL python -m unittest discover -s tests/persistence -v",
        [python, "-m", "unittest", "discover", "-s", "tests/persistence", "-v"],
        {"HEKATE_TEST_DATABASE_URL": phase2_database},
    )
    run("bridge_tests_and_build", "npm test (pinned Node.js 22.19.0)", ["npm", "test"], cwd=ROOT / "bridge/letta")

    schema_check = """from pathlib import Path
from tempfile import TemporaryDirectory
from scripts.export_contracts import write_schemas
checked = Path('contracts/generated')
with TemporaryDirectory() as temp:
    output = Path(temp)
    report = write_schemas(output)
    assert set(report.schemas) == {path.name for path in checked.glob('*.json')}
    assert all((output / name).read_bytes() == (checked / name).read_bytes() for name in report.schemas)
    print(f'{len(report.schemas)} schemas match generated output byte-for-byte')
"""
    run("schema_reproducibility", "python -c <generated schema byte comparison>", [python, "-c", schema_check])
    migration_env = {"HEKATE_DATABASE_URL": phase3_database}
    run("migration_upgrade", "HEKATE_DATABASE_URL=$HEKATE_TEST_DATABASE_URL python -m alembic upgrade head", [python, "-m", "alembic", "upgrade", "head"], migration_env)
    run("migration_check", "HEKATE_DATABASE_URL=$HEKATE_TEST_DATABASE_URL python -m alembic check", [python, "-m", "alembic", "check"], migration_env)
    run("diff_check", "git diff --check", ["git", "diff", "--check"])

    probe_env = {
        "HEKATE_TEST_DATABASE_URL": phase3_database,
        "HEKATE_NODE_BIN": node,
        "HEKATE_NODE_ARCHIVE": node_archive,
    }
    probe_record, probe_result = run(
        "pinned_runtime_probe",
        "python scripts/phase3_runtime_probe.py (dedicated loopback DB, pinned Node runtime)",
        [python, "scripts/phase3_runtime_probe.py"],
        probe_env,
    )
    artifact_path = None
    for line in probe_result.stdout.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and isinstance(value.get("artifact"), str):
            artifact_path = ROOT / value["artifact"]
    if artifact_path is None or not artifact_path.is_file():
        def redact(value: str) -> str:
            return value.replace(phase3_database, "<phase3-database-url>").replace(phase2_database, "<phase2-database-url>")[-2000:]
        print(json.dumps({
            "error": "runtime probe did not produce a readable artifact",
            "failed_commands": [record for record in records if record["status"] == "failed"],
            "probe_exit_code": probe_record["exit_code"],
            "probe_stdout_tail": redact(probe_result.stdout),
            "probe_stderr_tail": redact(probe_result.stderr),
        }, sort_keys=True))
        return 1

    probe3b_record, probe3b_result = run(
        "pinned_runtime_single_hekate_probe",
        "python scripts/phase3b_single_hekate_probe.py (dedicated loopback DB, pinned Node runtime)",
        [python, "scripts/phase3b_single_hekate_probe.py"],
        probe_env,
    )
    artifact3b_path = None
    for line in probe3b_result.stdout.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and isinstance(value.get("artifact"), str):
            artifact3b_path = ROOT / value["artifact"]
    if artifact3b_path is None or not artifact3b_path.is_file():
        def redact(value: str) -> str:
            return value.replace(phase3_database, "<phase3-database-url>").replace(phase2_database, "<phase2-database-url>")[-2000:]
        print(json.dumps({
            "error": "Phase 3B probe did not produce a readable artifact",
            "failed_commands": [record for record in records if record["status"] == "failed"],
            "probe_exit_code": probe3b_record["exit_code"],
            "probe_stdout_tail": redact(probe3b_result.stdout),
            "probe_stderr_tail": redact(probe3b_result.stderr),
        }, sort_keys=True))
        return 1

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    runtime_verification = artifact.get("verification", {})
    artifact3b = json.loads(artifact3b_path.read_text(encoding="utf-8"))
    runtime3b_results = artifact3b.get("results", {})
    command_passed = sum(record["status"] == "passed" for record in records)
    command_failed = len(records) - command_passed
    runtime_passed = artifact.get("overall_status") == "pass"
    artifact["verification"] = {
        "command": "uv run python scripts/phase3_verify.py",
        "status": "passed" if command_failed == 0 and runtime_passed else "failed",
        "phase3b_artifact": artifact3b_path.relative_to(ROOT).as_posix(),
        "commands_run": len(records),
        "commands_passed": command_passed,
        "commands_failed": command_failed,
        "commands": records,
        "runtime_scenarios": runtime_verification,
        "phase3b_runtime_scenarios": {
            "scenario_count": len(runtime3b_results),
            "passed_count": sum(value.get("passed") is True for value in runtime3b_results.values() if isinstance(value, dict)),
            "failed_count": sum(value.get("passed") is not True for value in runtime3b_results.values() if isinstance(value, dict)),
        },
    }
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    artifact3b["verification"] = artifact["verification"]
    artifact3b["verification"]["phase3a_artifact"] = artifact_path.relative_to(ROOT).as_posix()
    artifact3b_path.write_text(json.dumps(artifact3b, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({
        "artifact": artifact_path.relative_to(ROOT).as_posix(),
        "phase3b_artifact": artifact3b_path.relative_to(ROOT).as_posix(),
        "verification_status": artifact["verification"]["status"],
        "commands_run": len(records),
        "commands_passed": command_passed,
        "commands_failed": command_failed,
        "phase3_scenarios_passed": runtime_verification.get("passed_count", 0),
        "phase3_scenarios_failed": runtime_verification.get("failed_count", 0),
        "phase3b_scenarios_passed": artifact["verification"]["phase3b_runtime_scenarios"]["passed_count"],
        "phase3b_scenarios_failed": artifact["verification"]["phase3b_runtime_scenarios"]["failed_count"],
        "probe_exit_code": probe_record["exit_code"],
    }, sort_keys=True))
    return 0 if artifact["verification"]["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
