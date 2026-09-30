import assert from "node:assert/strict";
import test from "node:test";
import { assertBinding, decodeCommand, encodeEvent, negotiateVersion } from "../dist/protocol.js";

const binding = {
  task_id: "task-1",
  attempt_id: "attempt-1",
  agent_registry_id: "ha-1",
  provider_agent_id: "agent-1",
  input_revision: 3,
  fence: 9,
};

const frame = (value) => new TextEncoder().encode(JSON.stringify(value));

test("bridge.v1 accepts its hello and bound session commands", () => {
  assert.equal(decodeCommand(frame({
    schema_version: "1",
    request_id: "r1",
    operation_id: "o1",
    command: "hello",
  })).command, "hello");
  assert.equal(decodeCommand(frame({
    schema_version: "1",
    request_id: "r2",
    operation_id: "o2",
    command: "session.prepare",
    binding,
  })).command, "session.prepare");
});

test("bridge.v1 rejects an unsupported version, command, extra field, and missing binding", () => {
  for (const command of [
    { schema_version: "2", request_id: "r", operation_id: "o", command: "hello" },
    { schema_version: "1", request_id: "r", operation_id: "o", command: "agent.destroy" },
    { schema_version: "1", request_id: "r", operation_id: "o", command: "hello", debug: true },
    { schema_version: "1", request_id: "r", operation_id: "o", command: "session.prepare" },
  ]) {
    assert.throws(() => decodeCommand(frame(command)));
  }
});

test("bridge.v1 rejects a binding that differs from the prepared runtime", () => {
  const command = decodeCommand(frame({
    schema_version: "1",
    request_id: "r",
    operation_id: "o",
    command: "session.prepare",
    binding: { ...binding, attempt_id: "other-attempt" },
  }));
  assert.throws(() => assertBinding(command, binding), /binding mismatch: attempt_id/);
});

test("bridge transport caps an individual frame at one MiB", () => {
  assert.throws(() => decodeCommand(new Uint8Array(1_048_577)), /exceeds/);
});

test("App Server protocol_version is negotiated from the runtime's numeric field", () => {
  assert.equal(negotiateVersion({ protocol_version: 1 }).compatible, true);
  assert.equal(negotiateVersion({ protocol_version: 2 }).compatible, false);
});

test("bridge events accept only typed SDK error codes", () => {
  const event = {
    schema_version: "1",
    event_id: "op-1:result",
    operation_id: "op-1",
    binding: { ...binding, conversation_id: "conv-1" },
    event_type: "result",
    usage: { completeness: "UNKNOWN" },
    error_code: "approval_conflict",
    success: false,
  };
  assert.doesNotThrow(() => encodeEvent(event));
  assert.throws(() => encodeEvent({ ...event, error_code: "raw provider error" }));
});
