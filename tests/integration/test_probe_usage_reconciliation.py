from scripts.integration_probe import FAKE_USAGE, evaluate_g9, reconcile_fake_usage


def test_usage_reconciliation_counts_duplicates_after_the_first_record() -> None:
    usage = {
        "completeness": "COMPLETE",
        "accounting_call_id": "call-1",
        "provider_call_id": "response-1",
        "input_tokens": FAKE_USAGE["prompt_tokens"],
        "output_tokens": FAKE_USAGE["completion_tokens"],
        "total_tokens": FAKE_USAGE["total_tokens"],
    }
    snapshot = {
        "model_requests": [{
            "accounting_call_id": "call-1",
            "provider_response_id": "response-1",
            "fake_usage": FAKE_USAGE,
            "behavior": "normal",
        }],
        "checks": [{
            "authorized": True,
            "operation_id": "operation-1",
            "call_kind": "turn",
            "accounting_call_id": "call-1",
        }],
    }

    for events, expected_count, expected_duplicates in (
        ([{"usage": usage}], 1, 0),
        ([{"usage": usage}, {"usage": usage}], 2, 1),
    ):
        report = reconcile_fake_usage(snapshot, {"operation-1": {"usage_events": events}})
        call = report["calls"][0]
        assert call["bridge_usage_event_count"] == expected_count
        assert call["duplicate_usage_events"] == expected_duplicates
        assert report["usage_duplicate_counts"] == {"call-1": expected_duplicates}
        assert call["settlement"] == "MATCHED"


def test_g9_matches_all_forwarded_calls_and_keeps_expected_error_pending() -> None:
    def bridge_usage(call_id: str, response_id: str) -> dict[str, object]:
        return {
            "completeness": "COMPLETE",
            "accounting_call_id": call_id,
            "provider_call_id": response_id,
            "input_tokens": 37,
            "output_tokens": 2,
            "total_tokens": 39,
        }

    call_ids = ("turn-1", "compact-1", "compact-2", "error-1", "transport-1")
    checks = [
        {"authorized": True, "operation_id": op, "call_kind": kind, "accounting_call_id": call_id}
        for op, kind, call_id in (
            ("turn-op", "turn", call_ids[0]),
            ("compact-op-1", "compaction", call_ids[1]),
            ("compact-op-2", "compaction", call_ids[2]),
            ("error-op", "turn", call_ids[3]),
            ("transport-op", "turn", call_ids[4]),
        )
    ]
    checks.extend([
        {"authorized": False, "operation_id": "denied-op", "call_kind": "turn", "accounting_call_id": "denied-1"},
        {"authorized": False, "operation_id": "direct-test", "call_kind": "turn", "accounting_call_id": "direct-1"},
    ])
    snapshot = {
        "provider_request_count": 5,
        "permits_issued": 5,
        "permits_consumed": 5,
        "checks": checks,
        "model_requests": [
            {
                "accounting_call_id": call_id,
                "provider_response_id": f"response-{call_id}" if call_id not in {"error-1", "transport-1"} else None,
                "fake_usage": FAKE_USAGE if call_id not in {"error-1", "transport-1"} else None,
                "behavior": "error" if call_id == "error-1" else "disconnect" if call_id == "transport-1" else "normal",
            }
            for call_id in call_ids
        ],
    }
    turns = {
        op: {"usage_events": [{"event_type": "usage_statistics", "usage": bridge_usage(call_id, f"response-{call_id}")}]}
        for op, call_id in zip(("turn-op", "compact-op-1", "compact-op-2"), call_ids[:3])
    }
    turns["error-op"] = {"usage_events": [{
        "event_type": "usage_statistics",
        "usage": {"completeness": "UNKNOWN", "accounting_call_id": "error-1"},
    }]}
    turns["transport-op"] = {"state": "UNKNOWN", "usage_events": []}
    turns["denied-op"] = {"usage_events": []}

    report = reconcile_fake_usage(snapshot, turns, {"error-1", "transport-1"})
    assert evaluate_g9(report)
    assert report["counts"] == {
        "runtime_provider_requests": 5,
        "non_runtime_gateway_checks": 1,
        "direct_gateway_provider_requests": 0,
        "gateway_attempts": 7,
        "gateway_denials": 2,
        "permits_issued": 5,
        "permits_consumed": 5,
        "provider_endpoint_requests": 5,
    }
    assert {row["settlement"] for row in report["calls"]} == {
        "MATCHED", "EXPECTED_PENDING", "NOT_FORWARDED",
    }
    assert report["unsettled_call_count"] == 2
    assert report["blocking_call_count"] == 0

    missing_compaction = {**turns, "compact-op-2": {"usage_events": []}}
    missing_report = reconcile_fake_usage(snapshot, missing_compaction, {"error-1", "transport-1"})
    assert next(row for row in missing_report["calls"] if row["accounting_call_id"] == "compact-2")["settlement"] == "MISSING_USAGE"
    assert not evaluate_g9(missing_report)

    conflicting_turn = {
        **turns,
        "turn-op": {"usage_events": [{
            "event_type": "usage_statistics",
            "usage": {**bridge_usage("turn-1", "response-turn-1"), "input_tokens": 38},
        }]},
    }
    mismatch_report = reconcile_fake_usage(snapshot, conflicting_turn, {"error-1", "transport-1"})
    assert next(row for row in mismatch_report["calls"] if row["accounting_call_id"] == "turn-1")["settlement"] == "MISMATCH"
    assert not evaluate_g9(mismatch_report)
