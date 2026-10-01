from __future__ import annotations

import copy
import json
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from hekate.infrastructure.letta.bridge_protocol import decode_frame
from hekate.infrastructure.letta.provider_gateway import ProviderGatewayProfile
from hekate.application.results import supported_proposal_response, validate_turn_output
from hekate.domain.capsules import parse_hekate_turn_output
from hekate.domain.models import GuardBinding, PriceTable
from hekate.domain.types import (
    AttemptId, PrincipalId, ProviderAgentId, RegistryId, ScopeId, StopReason, TaskId,
)
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


class Phase3BResultContractTests(unittest.TestCase):
    def setUp(self):
        self.binding = GuardBinding(
            task_id=TaskId("task-1"), attempt_id=AttemptId("attempt-1"),
            agent_registry_id=RegistryId("registry-1"), provider_agent_id=ProviderAgentId("agent-1"),
            principal_id=PrincipalId("principal-1"), scope=ScopeId("scope-1"), input_revision=1,
            policy_version="policy-1", authz_epoch=1, fence=1, conversation_id="conversation-1",
        )
        self.output = {
            "schema_version": "1",
            "proposal": {"schema_version": "1", "action": "answer", "answer": "ok"},
            "conclusion": {
                "schema_version": "1", "task_id": "task-1", "attempt_id": "attempt-1",
                "agent_id": "registry-1", "status": "done",
                "assessment": {"statement": "synthetic", "confidence": {"level": "high", "basis": ["fixture"]}},
                "recommended_next_step": {"type": "none"},
                "position_recommendation": {"action": "maintain", "summary": "unchanged"},
            },
        }

    def test_result_rejection_boundaries(self):
        cases = [
            ("registry", lambda value: value["conclusion"].update(agent_id="registry-other"), "conclusion_binding_mismatch"),
            ("revision", lambda value: value["conclusion"].update(input_revision=2), "conclusion_binding_mismatch"),
            ("evidence", lambda value: value["conclusion"].update(evidence_used=["evidence-unverified"]), "evidence_not_supported"),
            ("unsupported action", lambda value: value.update(proposal={
                "schema_version": "1", "action": "continue", "unresolved_issue": "x",
                "next_action": "y", "expected_information_gain": "z", "decision_impact": "w",
            }), "unsupported_action"),
        ]
        for name, mutate, reason in cases:
            with self.subTest(name=name):
                value = copy.deepcopy(self.output)
                mutate(value)
                parsed = parse_hekate_turn_output(json.dumps(value).encode())
                with self.assertRaisesRegex(ValueError, reason):
                    validate_turn_output(parsed, self.binding)

        with self.assertRaises(ValueError):
            parse_hekate_turn_output(b"{not-json")

    def test_request_information_and_abstain_are_terminal_mappings(self):
        cases = [
            ("request_information", "what is missing?", "Additional information needed: what is missing?", "NEEDS_USER_INPUT", StopReason.NEEDS_USER_INPUT),
            ("abstain", "insufficient basis", "insufficient basis", "ABSTAINED", StopReason.POLICY),
        ]
        for action, reason, response, outcome, stop_reason in cases:
            with self.subTest(action=action):
                value = copy.deepcopy(self.output)
                value["proposal"] = {"schema_version": "1", "action": action, "reason": reason}
                proposal = parse_hekate_turn_output(json.dumps(value).encode()).proposal
                self.assertEqual(supported_proposal_response(proposal), (response, outcome, stop_reason))


if __name__ == "__main__":
    unittest.main()
