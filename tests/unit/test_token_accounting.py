from __future__ import annotations

import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from hekate.domain.budget_math import price_usage
from hekate.domain.errors import BudgetDenied
from hekate.domain.models import NormalizedUsage
from hekate.infrastructure.letta.token_accounting import (
    render_test_chat_input,
    test_execution_profile as _make_execution_profile,
    validate_measurement,
)


class TokenAccountingContractTests(unittest.TestCase):
    def setUp(self):
        self.profile, self.prices = _make_execution_profile(
            profile_id="test-accounting-v1", model="fake-model", model_revision="fixture-r1",
            context_window_tokens=160, max_input_tokens=120, max_output_tokens=50,
            pricing_version="test-prices-v1", input_usd_per_million=Decimal("1"),
            output_usd_per_million=Decimal("2"), pricing_effective_at="2026-10-01T00:00:00Z",
        )

    def test_renderer_counts_message_history_tools_and_structured_schema(self):
        body = {
            "model": "fake-model", "max_tokens": 32, "stream": False,
            "messages": [
                {"role": "system", "content": "policy and memory"},
                {"role": "assistant", "content": "history"},
                {"role": "user", "content": "current request"},
            ],
            "tools": [{"type": "function", "function": {
                "name": "lookup", "description": "Find a record",
                "parameters": {"type": "object", "properties": {"record_id": {"type": "string"}}},
            }}],
            "response_format": {"type": "json_schema", "json_schema": {
                "name": "reply", "strict": True,
                "schema": {"type": "object", "properties": {"answer": {"type": "string"}}},
            }},
        }
        rendered = render_test_chat_input(body)
        for fragment in ("policy and memory", "history", "current request", "Find a record", "record_id", "answer"):
            self.assertIn(fragment, rendered)
        body_with_store = {**body, "store": False}
        self.assertEqual(rendered, render_test_chat_input(body_with_store))
        with self.assertRaisesRegex(ValueError, "provider store control"):
            render_test_chat_input({**body, "store": "false"})

    def test_ceiling_boundaries_and_unsupported_inputs_fail_closed(self):
        from hekate.domain.models import ProviderRequestMeasurement

        def measurement(tokens: int, output: int) -> ProviderRequestMeasurement:
            return ProviderRequestMeasurement(
                request_digest="a" * 64, profile_digest=self.profile.content_digest,
                tokenizer_identity="test-tokenizer", renderer_identity="test-renderer",
                pricing_version=self.profile.pricing_version, pricing_digest=self.profile.pricing_digest,
                usage_semantics=self.profile.usage_semantics, measured_input_tokens=tokens,
                requested_output_tokens=output, context_window_tokens=self.profile.context_window_tokens,
                additional_reserved_tokens=0, verification_state="TEST_CONTRACT_VERIFIED",
                measured_at=datetime.now(UTC),
            )

        validate_measurement(measurement(120, 40), self.profile,
                             plan_max_input=120, envelope_max_input=120,
                             plan_max_output=50, envelope_max_output=50)
        with self.assertRaisesRegex(ValueError, "input-token ceiling"):
            validate_measurement(measurement(121, 1), self.profile,
                                 plan_max_input=120, envelope_max_input=120,
                                 plan_max_output=50, envelope_max_output=50)
        with self.assertRaisesRegex(ValueError, "context window"):
            validate_measurement(measurement(120, 41), self.profile,
                                 plan_max_input=120, envelope_max_input=120,
                                 plan_max_output=50, envelope_max_output=50)
        with self.assertRaisesRegex(ValueError, "unsupported token-bearing"):
            render_test_chat_input({"model": "fake-model", "messages": [{"role": "user", "content": "ok"}], "audio": {}})
        with self.assertRaises(BudgetDenied):
            price_usage(NormalizedUsage(
                completeness="COMPLETE", input_tokens=4, output_tokens=2, total_tokens=6, cache_tokens=1,
            ), self.prices)

    def test_tokenizer_asset_tampering_fails_closed(self):
        from hekate.infrastructure.letta import token_accounting

        with TemporaryDirectory() as directory:
            corrupted = Path(directory) / "cl100k_base.tiktoken"
            corrupted.write_bytes(b"not the pinned tokenizer asset")
            token_accounting._encoding.cache_clear()
            try:
                with patch.object(token_accounting, "_TOKENIZER_PATH", corrupted):
                    with self.assertRaisesRegex(ValueError, "asset digest mismatch"):
                        token_accounting._encoding()
            finally:
                token_accounting._encoding.cache_clear()
