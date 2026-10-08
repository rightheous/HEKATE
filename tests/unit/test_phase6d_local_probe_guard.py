from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.request import Request
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import phase6c_ollama_qwen_probe as probe
from hekate.infrastructure.letta.qwen_ollama import (
    load_qwen_candidate_profile,
    measure_qwen35_request,
    qwen35_native_json_schema_test_execution_profile,
)


class LocalProbePreSocketGuardTests(unittest.TestCase):
    def test_final_report_does_not_leave_pre_request_status_as_unexecuted(self):
        before_send = probe._local_unexecuted_boundaries(0)
        after_one_send = probe._local_unexecuted_boundaries(1)
        self.assertIn("did not reach the upstream", before_send[0])
        self.assertIn("attempt was used", after_one_send[0])
        self.assertIn("no retry", after_one_send[0])

    def test_output_observed_authorization_requires_the_current_adapter_preflight(self):
        failed = {
            "execution_mode": "one_local_ollama_smoke",
            "overall_status": "failed_after_single_upstream_attempt",
            "upstream_generation_attempts": 1,
            "actual_provider_calls": 1,
            "single_attempt_ledger": {"upstream_attempts": 1},
            "task_observation": {"state": "FAILED"},
            "results": {
                "pinned_runtime_turn": {
                    "result_and_schema_validation": {"processing_state": "REJECTED"},
                },
            },
            "database_target": {"database": "prior-isolated-db"},
        }

        def chain(size: int):
            content_hash = f"{size:064x}"
            adapter = {
                "directly_instrumented": True,
                "event_count": 265,
                "input_provider_content": {"utf8_bytes": size},
                "emitted_text_delta": {"utf8_bytes": size, "per_delta_matches_input": True},
            }
            return {
                "all_observed_assistant_content_matches_provider_fixture": True,
                "gateway_sse_wire_matches_fake_provider_wire": True,
                "assistant_content_stages": {"provider_adapter_internal_text_delta": adapter},
                "content_hash_for_test": content_hash,
            }

        fake = {
            "run_id": "fake-preflight",
            "execution_mode": "pinned_fake_provider_output_delivery",
            "overall_status": "pass",
            "fake_provider_requests": 2,
            "actual_provider_calls": 0,
            "upstream_generation_attempts": 0,
            "output_observation_diagnostics": {"enabled": True},
            "results": {
                "pinned_runtime_turn": {"fake_context_and_policy_verified": True},
                "output_delivery_reproduction": {
                    "complete_chain": chain(760),
                    "incomplete_chain": chain(759),
                    "terminal_content_comparison": {
                        "complete_task_accepted": True,
                        "incomplete_task_rejected": True,
                    },
                },
            },
        }
        with tempfile.TemporaryDirectory(prefix="phase6d-output-auth-") as temp_dir:
            prior_path = Path(temp_dir) / "failed.json"
            fake_path = Path(temp_dir) / "fake.json"
            prior_path.write_text(json.dumps(failed), encoding="utf-8")
            fake_path.write_text(json.dumps(fake), encoding="utf-8")
            with (
                patch.object(probe, "ROOT", Path(temp_dir)),
                patch.object(probe, "PREVIOUS_NATIVE_SCHEMA_FAILURE_ARTIFACT", prior_path),
                patch.object(probe, "OUTPUT_OBSERVED_PREFLIGHT_ARTIFACT", fake_path),
            ):
                authorization = probe._authorize_output_observed_independent_attempt()
                self.assertEqual(authorization["max_upstream_generation_attempts"], 1)
                self.assertEqual(authorization["prior_fake_run_id"], "fake-preflight")
                fake["results"]["output_delivery_reproduction"]["complete_chain"][
                    "assistant_content_stages"
                ]["provider_adapter_internal_text_delta"]["event_count"] = 264
                fake_path.write_text(json.dumps(fake), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "prior evidence does not prove"):
                    probe._authorize_output_observed_independent_attempt()

    def test_output_observed_followup_requires_consumed_prior_and_timeout_diagnosis(self):
        with tempfile.TemporaryDirectory(prefix="phase6d-output-followup-auth-") as temp_dir:
            root = Path(temp_dir)
            previous_path = root / "previous.json"
            diagnosis_path = root / "diagnosis.json"
            ledger_path = root / "previous-ledger.json"
            preflight_path = root / "preflight.json"
            previous = {
                "run_id": "prior-run",
                "execution_mode": "one_local_ollama_output_observation_diagnostic",
                "overall_status": "failed_after_single_upstream_attempt",
                "upstream_generation_attempts": 1,
                "actual_provider_calls": 1,
                "single_attempt_ledger": {"upstream_attempts": 1},
                "task_observation": {"state": "FAILED", "stop_reason": "POLICY"},
                "database_target": {"database": "prior-db"},
            }
            previous_path.write_text(json.dumps(previous), encoding="utf-8")
            boundary = lambda size: {"utf8_bytes": size}
            diagnosis = {
                "probe": "phase6d-one-authorized-qwen-output-diagnosis",
                "actual_run": {
                    "artifact": "previous.json",
                    "source_artifact_sha256": hashlib.sha256(previous_path.read_bytes()).hexdigest(),
                    "actual_provider_calls": 1,
                    "authorized_generation_attempts": 1,
                    "failure_diagnosis": {
                        "recomputed_first_divergence": "sdk_assistant_messages:content_mismatch",
                        "sdk_timeout_ms": 30_000,
                    },
                    "boundaries": {
                        "ollama_choice_0_content": boundary(763),
                        "gateway_received_and_yielded_choice_0": boundary(763),
                        "provider_adapter_received_content": boundary(763),
                        "provider_adapter_emitted_text_delta": boundary(763),
                        "app_server_assistant_stream": boundary(763),
                        "sdk_assistant_messages": boundary(669),
                        "postgresql_raw_output": boundary(669),
                    },
                    "actual_same_key_replay": {"no_new_upstream_request": True},
                },
                "fake_verification": {
                    "timeout_240ms": {
                        "probe_status": "pass",
                        "actual_provider_calls": 0,
                        "sdk_timeout_truncation_reproduced": False,
                        "stale_source_binding_rejected": True,
                        "transport_complete_through_postgresql": True,
                    },
                },
            }
            diagnosis_path.write_text(json.dumps(diagnosis), encoding="utf-8")
            ledger_path.write_text(json.dumps({
                "run_id": "prior-run", "state": "UPSTREAM_RESPONSE_READ", "upstream_attempts": 1,
            }), encoding="utf-8")
            def chain(size: int) -> dict[str, object]:
                return {
                    "all_observed_assistant_content_matches_provider_fixture": True,
                    "gateway_sse_wire_matches_fake_provider_wire": True,
                    "assistant_content_stages": {
                        "provider_adapter_internal_text_delta": {
                            "directly_instrumented": True,
                            "event_count": 265,
                            "input_provider_content": {"utf8_bytes": size},
                            "emitted_text_delta": {"utf8_bytes": size, "per_delta_matches_input": True},
                        },
                    },
                }

            preflight = {
                "execution_mode": "pinned_fake_provider_output_delivery",
                "overall_status": "pass",
                "fake_provider_requests": 2,
                "actual_provider_calls": 0,
                "upstream_generation_attempts": 0,
                "results": {
                    "output_delivery_reproduction": {
                        "complete_chain": chain(760),
                        "incomplete_chain": chain(759),
                        "terminal_content_comparison": {
                            "complete_task_accepted": True,
                            "incomplete_task_rejected": True,
                        },
                    },
                    "pinned_runtime_turn": {
                        "fake_context_and_policy_verified": True,
                        "same_key_replay": {"no_new_accounting_effect": True},
                    },
                },
            }
            preflight_path.write_text(json.dumps(preflight), encoding="utf-8")
            with (
                patch.object(probe, "ROOT", root),
                patch.object(probe, "PREVIOUS_OUTPUT_OBSERVED_ARTIFACT", previous_path),
                patch.object(probe, "OUTPUT_OBSERVED_DIAGNOSIS_ARTIFACT", diagnosis_path),
                patch.object(probe, "OUTPUT_OBSERVED_LOCAL_SMOKE_LEDGER", ledger_path),
                patch.object(probe, "OUTPUT_OBSERVED_FOLLOWUP_PREFLIGHT_ARTIFACT", preflight_path),
            ):
                authorization = probe._authorize_output_observed_followup_attempt()
                self.assertEqual(authorization["max_upstream_generation_attempts"], 1)
                self.assertEqual(authorization["prior_run_id"], "prior-run")
                self.assertEqual(authorization["prior_ledger_upstream_attempts"], 1)
                ledger_path.write_text(json.dumps({
                    "run_id": "prior-run", "state": "UPSTREAM_RESPONSE_READ", "upstream_attempts": 0,
                }), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "do not authorize"):
                    probe._authorize_output_observed_followup_attempt()

    def test_exact_request_and_execution_profile_pass_but_mismatches_never_open_transport(self):
        candidate = load_qwen_candidate_profile()
        execution_profile, _ = qwen35_native_json_schema_test_execution_profile(candidate)
        self.assertNotEqual(candidate.content_digest, execution_profile.content_digest)
        source_body = {
            "model": candidate.model,
            "messages": [{"role": "user", "content": "17 plus 25"}],
            "max_tokens": candidate.max_output_tokens,
            "reasoning_effort": "none",
            "tools": [],
        }
        _normalized, body, measurement = measure_qwen35_request(
            source_body, execution_profile, candidate.max_output_tokens,
            output_contract="hekate_turn_output_v1",
        )
        request_digest = hashlib.sha256(body).hexdigest()
        request = Request(
            "http://127.0.0.1:19191/v1/chat/completions",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        authorization = {
            "authorization": "permitted",
            "accounting_call_id": "call-1",
            "permit_id": "permit-1",
            "request_digest": request_digest,
            "profile_digest": execution_profile.content_digest,
            "measured_input_tokens": measurement.measured_input_tokens,
        }
        consumed = {
            "consumed": True,
            "accounting_call_id": "call-1",
            "permit_id": "permit-1",
            "request_digest": request_digest,
            "profile_digest": execution_profile.content_digest,
        }
        tampered_payload = json.loads(body.decode("utf-8"))
        tampered_payload["response_format"]["json_schema"]["schema"]["title"] = "untrusted-schema"
        tampered_body = json.dumps(
            tampered_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")
        tampered_digest = hashlib.sha256(tampered_body).hexdigest()
        tampered_request = Request(
            "http://127.0.0.1:19191/v1/chat/completions",
            data=tampered_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        tampered_authorization = {**authorization, "request_digest": tampered_digest}
        tampered_permit = {**consumed, "request_digest": tampered_digest}
        previous_ledger = probe.LOCAL_SMOKE_LEDGER
        with tempfile.TemporaryDirectory(prefix="phase6d-local-guard-") as temp_dir:
            try:
                probe.LOCAL_SMOKE_LEDGER = Path(temp_dir) / "single-attempt.json"
                for label, req, auth, permit, should_open in (
                    ("exact_execution_profile", request, authorization, consumed, True),
                    ("candidate_digest_substitution", request, {**authorization, "profile_digest": candidate.content_digest}, consumed, False),
                    ("changed_request_digest", request, {**authorization, "request_digest": "0" * 64}, consumed, False),
                    ("permit_digest_mismatch", request, authorization, {**consumed, "request_digest": "1" * 64}, False),
                    ("modified_native_schema", tampered_request, tampered_authorization, tampered_permit, False),
                ):
                    with self.subTest(label=label):
                        run_id = f"guard-{label}"
                        ledger_path = Path(temp_dir) / f"{label}.json"
                        probe.LOCAL_SMOKE_LEDGER = ledger_path
                        capture_directory = Path(temp_dir) / f"capture-{label}"
                        capture_directory.mkdir()
                        capture_paths = {
                            "provider_sse": capture_directory / "provider-response.sse",
                            "provider_choice_0": capture_directory / "provider-choice-0.txt",
                        }
                        for capture_path in capture_paths.values():
                            capture_path.touch()
                        probe._write_ledger_new({
                            "schema_version": "1",
                            "run_id": run_id,
                            "state": "MODEL_LOADED_CONTEXT_VERIFIED",
                            "model_load_attempts": 1,
                            "upstream_attempts": 0,
                        })
                        sent = []
                        responses = []
                        restore = probe._install_local_upstream_guard(
                            candidate,
                            execution_profile.content_digest,
                            run_id,
                            [auth],
                            [permit],
                            sent,
                            responses,
                            capture_paths,
                        )
                        try:
                            opener = probe.gateway_module._no_redirect_handler()

                            class Response:
                                status = 200
                                headers = {"Content-Type": "application/json"}

                                def read(self, *args, **kwargs):
                                    return b""

                                def close(self):
                                    return None

                            def counted_open(req, *args, **kwargs):
                                self.assertIs(req, req_expected)
                                return Response()

                            req_expected = req
                            opener._opener.open = counted_open
                            runner = {
                                "route": "GET /api/ps",
                                "model": candidate.model,
                                "manifest_digest": candidate.model_manifest_digest,
                                "context_length_tokens": candidate.context_window_tokens,
                                "minimum_required_tokens": candidate.context_window_tokens,
                                "context_verified_for_this_loaded_runner": True,
                                "loaded": True,
                            }
                            with patch.object(probe, "_observe_current_loaded_runner", return_value=runner):
                                if should_open:
                                    result = opener.open(req)
                                    self.assertEqual(result._request_digest, request_digest)
                                    self.assertEqual(sent[0]["request_sha256"], request_digest)
                                    self.assertEqual(sent[0]["profile_digest"], execution_profile.content_digest)
                                    self.assertEqual(sent[0]["live_loaded_runner_pre_send"], runner)
                                    self.assertEqual(json.loads(ledger_path.read_text())["upstream_attempts"], 1)
                                    result.close()
                                else:
                                    with self.assertRaisesRegex(RuntimeError, "not bound"):
                                        opener.open(req)
                                    self.assertEqual(sent, [])
                                    state = json.loads(ledger_path.read_text())
                                    self.assertEqual(state["upstream_attempts"], 0)
                                    self.assertEqual(state["state"], "PRE_SOCKET_GUARD_REJECTED")
                        finally:
                            restore()
            finally:
                probe.LOCAL_SMOKE_LEDGER = previous_ledger


if __name__ == "__main__":
    unittest.main()
