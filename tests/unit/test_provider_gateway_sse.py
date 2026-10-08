from __future__ import annotations

import hashlib
import json
import unittest

from hekate.infrastructure.letta.provider_gateway import _ProviderSSEObserver


class ProviderSSEObservationTests(unittest.TestCase):
    def test_delta_digest_survives_chunk_and_utf8_boundaries_without_retaining_text(self):
        frames = [
            {"id": "call-local", "choices": [{"index": 0, "delta": {"content": "응답: 한", "reasoning_content": "내부 검토"}, "finish_reason": None}]},
            {"id": "call-local", "choices": [{"index": 0, "delta": {"content": "국"}, "finish_reason": None}]},
            {"id": "call-local", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
            {"id": "call-local", "choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 3, "total_tokens": 13}},
        ]
        wire = b"".join(
            b"data: " + json.dumps(frame, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            + (b"\r\n\r\n" if index % 2 else b"\n\n")
            for index, frame in enumerate(frames)
        ) + b"data: [DONE]\r\n\r\n"
        split_at = wire.index("한".encode("utf-8")) + 1
        chunks = [wire[:split_at], wire[split_at:split_at + 7], wire[split_at + 7:]]

        observer = _ProviderSSEObserver()
        records = []
        for chunk in chunks:
            records.extend(observer.feed(chunk))
        records.extend(observer.finish())
        summary = observer.snapshot()
        choice = summary["choices"][0]

        self.assertEqual(len(records), 4)
        self.assertTrue(summary["done_seen"])
        self.assertEqual(summary["received_and_yielded_wire_utf8_bytes"], len(wire))
        self.assertEqual(summary["received_and_yielded_wire_sha256"], hashlib.sha256(wire).hexdigest())
        self.assertEqual(summary["malformed_event_count"], 0)
        self.assertEqual(choice["index"], 0)
        self.assertEqual(choice["content_delta_events"], 2)
        self.assertEqual(choice["content_utf8_bytes"], len("응답: 한국".encode("utf-8")))
        self.assertEqual(choice["content_sha256"], hashlib.sha256("응답: 한국".encode("utf-8")).hexdigest())
        self.assertEqual(choice["finish_reason"], "stop")
        self.assertEqual(
            [event["sequence"] for event in summary["event_order"]],
            [1, 2, 3, 4, 5],
        )
        self.assertEqual(summary["event_order"][-1]["event"], "done_marker")
        self.assertEqual(summary["event_order"][0]["choice_events"][0]["channel"], "assistant_and_reasoning")
        self.assertTrue(all(event["received_monotonic_ns"] > 0 for event in summary["event_order"]))
        self.assertEqual(
            choice["reasoning_channels"]["reasoning_content"],
            {
                "delta_events": 1,
                "utf8_bytes": len("내부 검토".encode("utf-8")),
                "sha256": hashlib.sha256("내부 검토".encode("utf-8")).hexdigest(),
            },
        )
        self.assertNotIn("응답", json.dumps(summary, ensure_ascii=False))
        self.assertNotIn("한국", json.dumps(summary, ensure_ascii=False))
        self.assertNotIn("내부 검토", json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
