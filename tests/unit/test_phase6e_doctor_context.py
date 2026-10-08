from __future__ import annotations

import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from hekate import diagnostics
from hekate.infrastructure.letta.qwen_ollama import load_qwen_candidate_profile
from hekate.settings import load_settings


ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "config/local.example"


class Phase6EDoctorContextTests(unittest.TestCase):
    def _settings(self):
        return load_settings({
            "HEKATE_DATABASE_URL": "postgresql+psycopg://local:local@127.0.0.1/hekate",
            "HEKATE_LETTA_URL": "ws://127.0.0.1:8283",
            "HEKATE_RUNTIME_MODE": "local",
            "HEKATE_WORKER_ID": "phase6e-doctor-context-test",
            "HEKATE_PROJECT_DIR": str(ROOT),
        }, EXAMPLE)

    def _doctor_fixture(self, *, context, loaded, version_matches, manifest_matches):
        candidate = load_qwen_candidate_profile()
        requests: list[tuple[str, str, object]] = []

        def read_json(url: str, *, token=None, method="GET", body=None):
            requests.append((url, method, body))
            if url.endswith("/api/version"):
                return {"version": candidate.ollama_version if version_matches else "changed"}
            if url.endswith("/api/tags"):
                return {"models": [{
                    "name": candidate.model,
                    "digest": candidate.model_manifest_digest if manifest_matches else "0" * 64,
                }]}
            if url.endswith("/api/ps"):
                if not loaded:
                    return {"models": []}
                return {"models": [{
                    "name": candidate.model,
                    "digest": candidate.model_manifest_digest,
                    **({"context_length": context} if context is not None else {}),
                }]}
            raise AssertionError(f"unexpected doctor HTTP request: {url}")

        class ReadOnlyEngine:
            disposed = False

            async def dispose(self):
                self.disposed = True

        engine = ReadOnlyEngine()

        async def check_database(_engine):
            return SimpleNamespace(
                available=True,
                postgres_version="PostgreSQL fixture",
                migration_head="0014_local_dispatch_identity",
            )

        async def auth_status(_settings):
            return {"status": "READY", "active": True}

        def gateway_ready(_settings):
            return {"status": "READY"}

        async def runtime_ready(_settings):
            return {"status": "READY"}

        with (
            patch.object(diagnostics, "_read_json", side_effect=read_json),
            patch.object(diagnostics, "create_engine", return_value=engine),
            patch.object(diagnostics, "check_database", side_effect=check_database),
            patch.object(diagnostics, "_authorization_status", side_effect=auth_status),
            patch.object(diagnostics, "_gateway_status", side_effect=gateway_ready),
            patch.object(diagnostics, "_runtime_status", side_effect=runtime_ready),
            patch.object(diagnostics.subprocess, "run", return_value=SimpleNamespace(stdout="v22.19.0")),
        ):
            result = asyncio.run(diagnostics.doctor(self._settings()))
        return result, requests, engine, candidate

    def test_whole_doctor_distinguishes_runner_context_and_identity(self):
        cases = (
            (8192, True, True, True, "READY", "READY"),
            (8191, True, True, True, "CONTEXT_INSUFFICIENT", "NOT_READY"),
            (None, True, True, True, "CONTEXT_UNVERIFIED", "NOT_READY"),
            ("8192", True, True, True, "CONTEXT_UNVERIFIED", "NOT_READY"),
            (None, False, True, True, "NOT_LOADED", "PRESTART_OK"),
            (8192, True, False, True, "MODEL_MISSING_OR_CHANGED", "NOT_READY"),
            (8192, True, True, False, "MODEL_MISSING_OR_CHANGED", "NOT_READY"),
        )
        for context, loaded, version_matches, manifest_matches, expected_ollama, expected_doctor in cases:
            with self.subTest(
                context=context, loaded=loaded,
                version_matches=version_matches, manifest_matches=manifest_matches,
            ):
                result, requests, engine, candidate = self._doctor_fixture(
                    context=context, loaded=loaded,
                    version_matches=version_matches, manifest_matches=manifest_matches,
                )
                self.assertEqual(result["status"], expected_doctor)
                ollama = result["checks"]["ollama"]
                self.assertEqual(ollama["status"], expected_ollama)
                self.assertEqual(ollama["model_metadata_context_tokens"], candidate.model_metadata_context_tokens)
                self.assertEqual(ollama["required_context_tokens"], candidate.context_window_tokens)
                self.assertEqual(candidate.context_window_tokens, 8192)
                self.assertEqual(ollama["context_verified"], expected_ollama == "READY")
                self.assertFalse(result["inference_requested"])
                self.assertFalse(result["database_modified"])
                self.assertTrue(engine.disposed)
                self.assertEqual(requests, [
                    ("http://127.0.0.1:19191/api/version", "GET", None),
                    ("http://127.0.0.1:19191/api/tags", "GET", None),
                    ("http://127.0.0.1:19191/api/ps", "GET", None),
                ])


if __name__ == "__main__":
    unittest.main()
