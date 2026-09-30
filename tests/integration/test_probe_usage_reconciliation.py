from scripts.integration_probe import FAKE_USAGE, reconcile_fake_usage


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
