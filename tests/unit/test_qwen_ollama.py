from __future__ import annotations

import base64
import hashlib
import json
import unittest
from decimal import Decimal
from pathlib import Path

from pydantic import ValidationError

from hekate.infrastructure.letta.qwen_ollama import (
    hekate_turn_output_schema,
    load_qwen_candidate_profile,
    measure_qwen35_request,
    normalize_qwen35_request,
    qwen35_native_json_schema_test_execution_profile,
    qwen35_test_execution_profile,
    qwen35_tokenize,
    render_qwen35_messages,
    validate_qwen_candidate_profile,
    validate_qwen35_native_json_schema_profile,
)
from hekate.domain.capsules import parse_hekate_turn_output
from hekate.infrastructure.letta.token_accounting import validate_measurement, validate_profile
from hekate.domain.models import TaskExecutionConfig
from hekate.settings import execution_config_snapshot, restore_execution_config

ROOT = Path(__file__).resolve().parents[2]
REFERENCE = ROOT / "integration/runtime/fixtures/qwen35-renderer-tokenizer-v1.json"


class QwenOllamaProfileTests(unittest.TestCase):
    def setUp(self):
        self.candidate = load_qwen_candidate_profile()
        self.execution_profile, self.price_table = qwen35_test_execution_profile(self.candidate)

    def test_installed_tokenizer_and_pinned_renderer_goldens(self):
        fixture = json.loads(REFERENCE.read_text(encoding="utf-8"))
        self.assertEqual(fixture["identity"]["model"], self.candidate.model)
        for vector in fixture["raw_token_vectors"]:
            with self.subTest(vector=vector["name"]):
                self.assertEqual(qwen35_tokenize(vector["text"]), vector["token_ids"])
        for item in fixture["rendered_prompts"]:
            with self.subTest(prompt=item["name"]):
                rendered = render_qwen35_messages(item["messages"], think=False).encode("utf-8")
                self.assertEqual(rendered, base64.b64decode(item["rendered_utf8_base64"], validate=True))
                self.assertEqual(hashlib.sha256(rendered).hexdigest(), item["rendered_sha256"])
                self.assertEqual(qwen35_tokenize(rendered.decode("utf-8")), item["token_ids"])

    def test_normalizer_binds_forwarded_bytes_and_keeps_schema_out_of_prompt(self):
        body = {
            "model": self.candidate.model,
            "messages": [
                {"role": "system", "content": "trusted policy"},
                {"role": "developer", "content": "trusted core memory"},
                {"role": "user", "content": "Question with bounded task data."},
            ],
            "max_completion_tokens": 128,
            "reasoning_effort": "none",
            "store": False,
            "n": 1,
            "response_format": {"type": "json_schema", "json_schema": {
                "name": "turn-output", "strict": True,
                "schema": {"type": "object", "description": "SCHEMA_NOT_IN_PROMPT_UNIQUE"},
            }},
        }
        normalized, final_bytes, rendered = normalize_qwen35_request(body, self.candidate.model)
        self.assertEqual([message["role"] for message in normalized["messages"]], ["system", "system", "user"])
        self.assertEqual(normalized["max_tokens"], 128)
        self.assertNotIn("max_completion_tokens", normalized)
        self.assertNotIn("n", normalized)
        self.assertNotIn("store", normalized)
        self.assertEqual(normalized["reasoning_effort"], "none")
        self.assertNotIn("SCHEMA_NOT_IN_PROMPT_UNIQUE", rendered)
        expected_bytes = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        self.assertEqual(final_bytes, expected_bytes)
        self.assertEqual(hashlib.sha256(final_bytes).hexdigest(), hashlib.sha256(expected_bytes).hexdigest())

        changed_schema = json.loads(json.dumps(body))
        changed_schema["response_format"]["json_schema"]["schema"]["description"] = "different schema"
        _, changed_bytes, changed_render = normalize_qwen35_request(changed_schema, self.candidate.model)
        self.assertEqual(changed_render, rendered)
        self.assertNotEqual(changed_bytes, final_bytes)
        with self.assertRaisesRegex(ValueError, "store=false"):
            normalize_qwen35_request({**body, "store": True}, self.candidate.model)

    def test_native_json_schema_is_generated_trusted_measured_and_strictly_validated(self):
        old_profile, prices = qwen35_test_execution_profile(self.candidate)
        profile, native_prices = qwen35_native_json_schema_test_execution_profile(self.candidate)
        schema, schema_digest = hekate_turn_output_schema()
        checked_in_schema = json.loads(
            (ROOT / "contracts/generated/hekate-turn-output.v1.schema.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(schema, checked_in_schema)
        self.assertNotEqual(profile.profile_id, old_profile.profile_id)
        self.assertNotEqual(profile.content_digest, old_profile.content_digest)
        self.assertEqual(prices, native_prices)
        validate_qwen35_native_json_schema_profile(profile, native_prices)

        source = {
            "model": self.candidate.model,
            "messages": [{"role": "user", "content": "17 plus 25"}],
            "max_tokens": self.candidate.max_output_tokens,
            "reasoning_effort": "none",
            "tools": [],
        }
        normalized, final_bytes, measurement = measure_qwen35_request(
            source, profile, self.candidate.max_output_tokens,
            output_contract="hekate_turn_output_v1",
        )
        self.assertEqual(normalized["response_format"], {"type": "json_schema", "json_schema": {"schema": schema}})
        self.assertEqual(normalized["temperature"], 0)
        self.assertEqual(measurement.profile_digest, profile.content_digest)
        self.assertEqual(measurement.request_digest, hashlib.sha256(final_bytes).hexdigest())
        self.assertEqual(json.loads(final_bytes)["response_format"]["json_schema"]["schema"], schema)
        self.assertEqual(hashlib.sha256(json.dumps(schema, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest(), schema_digest)
        _final_body, remeasured_bytes, remeasured = measure_qwen35_request(
            normalized, profile, self.candidate.max_output_tokens,
            output_contract="hekate_turn_output_v1", native_schema_injected=True,
        )
        self.assertEqual(remeasured_bytes, final_bytes)
        self.assertEqual(remeasured.request_digest, measurement.request_digest)

        modified = json.loads(final_bytes)
        modified["response_format"]["json_schema"]["schema"]["title"] = "untrusted"
        with self.assertRaisesRegex(ValueError, "trusted generated schema"):
            measure_qwen35_request(
                modified, profile, self.candidate.max_output_tokens,
                output_contract="hekate_turn_output_v1", native_schema_injected=True,
            )
        with self.assertRaisesRegex(ValueError, "cannot select its own output schema"):
            measure_qwen35_request(
                normalized, profile, self.candidate.max_output_tokens,
                output_contract="hekate_turn_output_v1",
            )
        with self.assertRaisesRegex(ValueError, "admitted Hekate turn output contract"):
            measure_qwen35_request(source, profile, self.candidate.max_output_tokens, output_contract="critic_turn_output_v1")
        drifted = profile.model_copy(update={"renderer_sha256": "f" * 64})
        with self.assertRaisesRegex(ValueError, "profile or synthetic tariff changed"):
            validate_qwen35_native_json_schema_profile(drifted, native_prices)

    def test_strict_bridge_contract_accepts_complete_json_and_rejects_previous_fenced_truncation_shape(self):
        complete = {
            "schema_version": "1",
            "proposal": {"schema_version": "1", "action": "answer", "answer": "42"},
            "conclusion": {
                "schema_version": "1", "task_id": "task-demo", "attempt_id": "attempt-demo",
                "agent_id": "ha-demo", "status": "done",
                "assessment": {"statement": "17 plus 25 equals 42", "confidence": {"level": "high", "basis": ["addition"]}},
                "recommended_next_step": {"type": "answer"},
                "position_recommendation": {"action": "no_change", "summary": "No durable position is needed."},
            },
        }
        parsed = parse_hekate_turn_output(json.dumps(complete).encode("utf-8"))
        self.assertEqual(parsed.proposal.answer, "42")
        prior_incomplete_shape = (
            b'```json\n{"schema_version":"1","proposal":{"schema_version":"1","action":"answer","answer":"42"},'
            b'"conclusion":{"schema_version":"1","task_id":"task-demo","attempt_id":"attempt-demo",'
            b'"agent_id":"ha-demo","status":"done","assessment":{"statement":"x",'
            b'"confidence":{"level":"high","basis":[],"missing_evidence":'
        )
        with self.assertRaises((ValueError, ValidationError)):
            parse_hekate_turn_output(prior_incomplete_shape)

    def test_exact_input_and_combined_context_boundaries(self):
        empty_prompt = render_qwen35_messages([{"role": "user", "content": ""}], think=False)
        wrapper_tokens = len(qwen35_tokenize(empty_prompt))
        digit_count = self.candidate.max_input_tokens - wrapper_tokens
        body = {
            "model": self.candidate.model,
            "messages": [{"role": "user", "content": "1" * digit_count}],
            "max_tokens": self.candidate.max_output_tokens,
            "reasoning_effort": "none",
        }
        normalized, final_bytes, measurement = measure_qwen35_request(
            body, self.execution_profile, self.candidate.max_output_tokens,
        )
        self.assertEqual(measurement.measured_input_tokens, self.candidate.max_input_tokens)
        self.assertEqual(measurement.request_digest, hashlib.sha256(final_bytes).hexdigest())
        validate_measurement(
            measurement, self.execution_profile,
            plan_max_input=self.candidate.max_input_tokens,
            envelope_max_input=self.candidate.max_input_tokens,
            plan_max_output=self.candidate.max_output_tokens,
            envelope_max_output=self.candidate.max_output_tokens,
        )

        over_body = {**body, "messages": [{"role": "user", "content": "1" * (digit_count + 1)}]}
        _, _, over = measure_qwen35_request(over_body, self.execution_profile, self.candidate.max_output_tokens)
        with self.assertRaisesRegex(ValueError, "input-token ceiling"):
            validate_measurement(
                over, self.execution_profile,
                plan_max_input=self.candidate.max_input_tokens,
                envelope_max_input=self.candidate.max_input_tokens,
                plan_max_output=self.candidate.max_output_tokens,
                envelope_max_output=self.candidate.max_output_tokens,
            )

        over_output_body = {**body, "max_tokens": self.candidate.max_output_tokens + 1}
        _, _, over_output = measure_qwen35_request(
            over_output_body, self.execution_profile, self.candidate.max_output_tokens + 1,
        )
        with self.assertRaisesRegex(ValueError, "output ceiling"):
            validate_measurement(
                over_output, self.execution_profile,
                plan_max_input=self.candidate.max_input_tokens,
                envelope_max_input=self.candidate.max_input_tokens,
                plan_max_output=self.candidate.max_output_tokens + 1,
                envelope_max_output=self.candidate.max_output_tokens + 1,
            )

    def test_profile_drift_and_unsupported_requests_fail_closed(self):
        validate_qwen_candidate_profile(self.candidate)
        validate_profile(self.execution_profile, self.price_table, allow_test_profile=True)
        with self.assertRaisesRegex(ValueError, "explicit test-only gateway construction"):
            validate_profile(self.execution_profile, self.price_table, allow_test_profile=False)
        self.assertFalse(self.candidate.operational_dispatch_approved)
        self.assertFalse(self.candidate.runtime_context_verified)
        self.assertFalse(self.candidate.inference_usage_verified)
        changed = self.candidate.model_copy(update={"model_manifest_digest": "a" * 64})
        with self.assertRaisesRegex(ValueError, "identity or offline-only policy changed"):
            validate_qwen_candidate_profile(changed)

        base = {"model": self.candidate.model, "messages": [{"role": "user", "content": "safe"}], "max_tokens": 8}
        invalid = [
            {**base, "unknown_tokens": "blocked"},
            {**base, "n": 2},
            {**base, "tools": [{"type": "function"}]},
            {**base, "messages": [{"role": "user", "content": [{"type": "image_url", "image_url": "data:"}]}]},
            {**base, "messages": [{"role": "user", "content": "literal <|im_start|> control"}]},
            {**base, "messages": [{"role": "tool", "content": "tool output"}]},
        ]
        for item in invalid:
            with self.subTest(request=item):
                with self.assertRaises((ValueError, TypeError)):
                    normalize_qwen35_request(item, self.candidate.model)

    def test_qwen_runtime_settings_survive_workflow_snapshot_and_old_defaults(self):
        config = TaskExecutionConfig(
            task_budget_usd=Decimal("1.00"), system_daily_budget_usd=Decimal("10.00"), deadline_seconds=240,
            profile_id=self.execution_profile.profile_id,
            letta_model=f"openai-compatible/{self.candidate.model}", model=self.candidate.model,
            pricing_version=self.price_table.version,
            input_usd_per_million=self.price_table.input_usd_per_million,
            output_usd_per_million=self.price_table.output_usd_per_million,
            max_input_tokens=self.candidate.max_input_tokens,
            max_output_tokens=self.candidate.max_output_tokens,
            max_compaction_calls=0, context_window_tokens=self.candidate.context_window_tokens,
            model_revision=self.candidate.model_manifest_digest,
            profile_digest=self.execution_profile.content_digest,
            pricing_effective_at=self.price_table.effective_at,
            agent_system_prompt=self.candidate.agent_system_prompt,
            letta_context_estimator_tokens=self.candidate.letta_context_estimator_tokens,
            sdk_output_format=self.candidate.sdk_output_format,
        )
        snapshot = execution_config_snapshot(config)
        restored = restore_execution_config(snapshot)
        self.assertEqual(execution_config_snapshot(restored), snapshot)
        self.assertEqual(restored.letta_context_estimator_tokens, 16_384)
        self.assertFalse(restored.sdk_output_format)

        legacy_snapshot = dict(snapshot)
        legacy_snapshot.pop("letta_context_estimator_tokens")
        legacy_snapshot.pop("sdk_output_format")
        legacy_restored = restore_execution_config(legacy_snapshot)
        self.assertIsNone(legacy_restored.letta_context_estimator_tokens)
        self.assertTrue(legacy_restored.sdk_output_format)


if __name__ == "__main__":
    unittest.main()
