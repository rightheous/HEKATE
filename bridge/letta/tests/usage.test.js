import assert from "node:assert/strict";
import test from "node:test";
import { mergeUsageUpdate, normalizeUsageStatistics } from "../dist/usage.js";

test("normalizes provider token values without inventing zeros or confusing IDs", () => {
  const usage = normalizeUsageStatistics({
    message_type: "usage_statistics",
    prompt_tokens: 17,
    completion_tokens: 3,
    total_tokens: 20,
    provider_call_id: "provider-response-7",
    accounting_call_id: "trusted-call-9",
    reasoning: "must not be copied",
  });
  assert.deepEqual(usage, {
    completeness: "COMPLETE",
    source: "runtime_reported",
    accounting_call_id: "trusted-call-9",
    provider_call_id: "provider-response-7",
    input_tokens: 17,
    output_tokens: 3,
    total_tokens: 20,
  });
});

test("marks unbound and partial usage honestly and rejects malformed quantities", () => {
  assert.deepEqual(normalizeUsageStatistics({
    message_type: "usage_statistics",
    prompt_tokens: 17,
  }), {
    completeness: "PARTIAL",
    source: "runtime_reported",
    input_tokens: 17,
  });
  assert.deepEqual(normalizeUsageStatistics({
    message_type: "usage_statistics",
    prompt_tokens: -1,
    completion_tokens: 1.5,
    context_tokens: 50,
  }), { completeness: "UNKNOWN" });
  assert.deepEqual(normalizeUsageStatistics({ message_type: "stop_reason" }), {
    completeness: "UNKNOWN",
  });
  assert.deepEqual(normalizeUsageStatistics({
    message_type: "usage_statistics",
    accounting_call_id: "failed-call-1",
  }), {
    completeness: "UNKNOWN",
    accounting_call_id: "failed-call-1",
  });
});

test("same-call updates replace duplicate usage and allow later fields to fill gaps", () => {
  const initial = normalizeUsageStatistics({
    message_type: "usage_statistics",
    accounting_call_id: "call-1",
    prompt_tokens: 17,
  });
  const duplicate = mergeUsageUpdate(initial, normalizeUsageStatistics({
    message_type: "usage_statistics",
    accounting_call_id: "call-1",
    prompt_tokens: 17,
  }));
  const completed = mergeUsageUpdate(duplicate, normalizeUsageStatistics({
    message_type: "usage_statistics",
    accounting_call_id: "call-1",
    completion_tokens: 3,
    total_tokens: 20,
  }));
  assert.equal(duplicate.input_tokens, 17);
  assert.equal(completed.input_tokens, 17);
  assert.equal(completed.output_tokens, 3);
  assert.equal(completed.total_tokens, 20);
  assert.equal(completed.completeness, "COMPLETE");
  assert.equal(mergeUsageUpdate(completed, normalizeUsageStatistics({
    message_type: "usage_statistics",
    accounting_call_id: "call-2",
    prompt_tokens: 99,
  })).accounting_call_id, "call-2");
});
