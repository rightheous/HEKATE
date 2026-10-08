import assert from "node:assert/strict";
import test from "node:test";

process.env.HEKATE_LETTA_URL ??= "http://127.0.0.1:1";
const { routeCommand } = await import("../dist/main.js");

const binding = {
  task_id: "task-1",
  attempt_id: "attempt-1",
  agent_registry_id: "ha-1",
  provider_agent_id: "agent-1",
  input_revision: 1,
  fence: 1,
};
const sessionBinding = { ...binding, conversation_id: "conversation-1" };
const command = (name, operationId, session = true) => ({
  schema_version: "1",
  request_id: `request-${operationId}`,
  operation_id: operationId,
  command: name,
  binding: session ? sessionBinding : binding,
  ...(name === "session.turn" ? { message: "continue" } : {}),
});
const entry = (turnState, operationId = "held-operation") => {
  const calls = { sends: 0, closes: 0 };
  return {
    calls,
    session: {
      async send() { calls.sends++; },
      close() { calls.closes++; },
      async *stream() {},
    },
    binding,
    conversationId: sessionBinding.conversation_id,
    appServerInfo: {},
    agentTools: [],
    turnOperationId: operationId,
    turnState,
    events: [],
    toolStats: { executorCalls: 0, blockedAttempts: 0 },
  };
};

for (const state of ["RUNNING", "UNKNOWN"]) {
  test(`route keeps ${state} execution held across new turn and session.prepare`, async () => {
    const running = entry(state);
    const sessions = new Map([[binding.provider_agent_id, running]]);
    const client = { createSession() { throw new Error("must not create SDK session"); } };

    const turn = await routeCommand(command("session.turn", "new-operation"), { client, sessions });
    assert.equal(turn.status, "UNKNOWN");
    assert.equal(turn.result.state, state);
    assert.equal(turn.result.unresolved_operation_id, "held-operation");
    assert.equal(running.calls.sends, 0);

    const prepare = await routeCommand(command("session.prepare", "prepare-new-fence", false), {
      client,
      sessions,
    });
    assert.equal(prepare.status, "UNKNOWN");
    assert.equal(prepare.result.unresolved_operation_id, "held-operation");
    assert.equal(running.calls.closes, 0);
    assert.equal(sessions.get(binding.provider_agent_id), running);
  });
}

test("events.collect exposes both immutable usage observations after a conflict", async () => {
  const observed = [
    {
      type: "stream_event",
      event: {
        message_type: "usage_statistics",
        accounting_call_id: "accounting-call-1",
        provider_call_id: "provider-call-1",
        prompt_tokens: 17,
      },
    },
    {
      type: "stream_event",
      event: {
        message_type: "usage_statistics",
        accounting_call_id: "accounting-call-1",
        provider_call_id: "provider-call-1",
        prompt_tokens: 18,
      },
    },
    { type: "result", success: true },
  ];
  const running = entry("IDLE", undefined);
  running.session.stream = async function* () { yield* observed; };
  const sessions = new Map([[binding.provider_agent_id, running]]);
  const deps = { client: {}, sessions };

  const dispatched = await routeCommand(command("session.turn", "usage-operation"), deps);
  assert.equal(dispatched.result.state, "DISPATCHED");
  const collected = await routeCommand(command("events.collect", "usage-operation"), deps);
  const callEvents = collected.result.events.filter(
    (event) => event.usage.accounting_call_id === "accounting-call-1",
  );

  assert.equal(collected.result.state, "COMPLETE");
  assert.deepEqual(callEvents.map((event) => event.event_type), ["usage_statistics", "usage_conflict"]);
  assert.deepEqual(callEvents.map((event) => event.usage.input_tokens), [17, 18]);
  assert.deepEqual(callEvents.map((event) => event.event_id), [
    "usage-operation:0:stream_event",
    "usage-operation:1:stream_event",
  ]);
  assert.deepEqual(callEvents.map((event) => event.usage.source), ["runtime_reported", "runtime_reported"]);
});
