from __future__ import annotations

import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from hekate.infrastructure.letta.bridge_protocol import decode_frame
from hekate.infrastructure.letta.provider_gateway import ProviderGatewayProfile
from hekate.domain.models import PriceTable
from hekate.settings import Settings, validate_settings


class Phase3BoundaryTests(unittest.TestCase):
    def test_bridge_replies_reject_duplicate_keys_unknown_fields_and_oversized_frames(self):
        valid = b'{"schema_version":"1","request_id":"r1","operation_id":"o1","command":"hello","status":"CONFIRMED"}'
        self.assertEqual(decode_frame(valid).status, "CONFIRMED")
        with self.assertRaisesRegex(ValueError, "invalid bridge reply"):
            decode_frame(b'{"schema_version":"1","request_id":"r1","request_id":"r2","operation_id":"o1","command":"hello","status":"CONFIRMED"}')
        with self.assertRaisesRegex(ValueError, "invalid bridge reply"):
            decode_frame(valid[:-1] + b',"extra":true}')
        with self.assertRaisesRegex(ValueError, "1 MiB"):
            decode_frame(b" " * 1_048_577)

    def test_test_provider_profile_is_explicit_and_cannot_route_external(self):
        profile = ProviderGatewayProfile(
            profile_id="fake-v1",
            price_table=PriceTable(
                model="fake-model",
                version="synthetic-v1",
                input_usd_per_million=Decimal("1"),
                output_usd_per_million=Decimal("2"),
                synthetic=True,
            ),
            upstream_base_url="http://127.0.0.1:9001",
            upstream_api_key="test-only",
            max_input_tokens=100,
            max_output_tokens=20,
            test_only=True,
        )
        with self.assertRaisesRegex(ValueError, "explicit test-mode"):
            profile.validate()
        profile.validate(allow_test_profile=True)
        external = replace(profile, upstream_base_url="https://provider.example/v1")
        with self.assertRaisesRegex(ValueError, "loopback"):
            external.validate(allow_test_profile=True)

    def test_production_settings_fail_closed_without_verified_model_and_pricing(self):
        settings = Settings(
            database_url="postgresql+psycopg://user:pass@127.0.0.1/hekate",
            node_bin="pinned-node",
            bridge_entry=Path(__file__).resolve().parents[2] / "src/hekate/__init__.py",
            letta_url="ws://127.0.0.1:4500",
            letta_token=None,
            worker_id="phase3-test",
            runtime_mode="production",
            config_dir=Path(__file__).resolve().parents[2] / "config",
            policy={},
            models={},
            pricing={},
        )
        with patch("hekate.settings.subprocess.run", return_value=SimpleNamespace(stdout="v22.19.0")):
            with self.assertRaisesRegex(ValueError, "production dispatch stays closed"):
                validate_settings(settings)


if __name__ == "__main__":
    unittest.main()
