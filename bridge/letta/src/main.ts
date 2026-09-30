import {
  LettaAgentClient,
  type LettaCodeSession,
  type SDKMessage,
} from "@letta-ai/letta-agent-sdk";
import { readFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import {
  assertBinding,
  decodeCommand,
  encodeEvent,
  encodeReply,
  negotiateVersion,
  type BridgeEvent,
  type Command,
  type Reply,
  type RuntimeBinding,
  type SessionBinding,
} from "./protocol.js";
import { mergeUsageUpdate, normalizeUsageStatistics, UsageConflictError } from "./usage.js";

interface SessionEntry {
  session: LettaCodeSession;
  binding: RuntimeBinding;
  conversationId: string;
  appServerInfo: Record<string, unknown>;
  agentTools: string[];
  turnOperationId?: string;
  turnState: "IDLE" | "RUNNING" | "COMPLETE" | "FAILED" | "UNKNOWN";
  events: BridgeEvent[];
  toolStats: { executorCalls: number; blockedAttempts: number };
  turnTask?: Promise<void>;
}

const MAX_FRAME_BYTES = 1_048_576;
const structuredOutputProbe = process.env.HEKATE_STRUCTURED_OUTPUT_PROBE === "1";
const sessions = new Map<string, SessionEntry>();
const appServerUrl = process.env.HEKATE_LETTA_URL;
if (!appServerUrl) throw new Error("HEKATE_LETTA_URL is required");

const client = new LettaAgentClient({
  backend: "remote",
  url: appServerUrl,
  ...(process.env.HEKATE_LETTA_TOKEN
    ? { authToken: process.env.HEKATE_LETTA_TOKEN }
    : {}),
  requestTimeoutMs: 30_000,
});

const positionCommitSchema = structuredOutputProbe
  ? JSON.parse(readFileSync("/workspace/contracts/generated/position-commit.v1.schema.json", "utf8"))
  : undefined;

function matchesTag(tags: unknown, tag: string): boolean {
  return Array.isArray(tags) && tags.some((candidate) => candidate === tag);
}

function cleanError(error: unknown): string {
  const message = error instanceof Error ? error.message : "bridge operation failed";
  const token = process.env.HEKATE_LETTA_TOKEN;
  return token ? message.replaceAll(token, "[redacted]") : message;
}

function isNotFoundError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const record = error as { status?: unknown; statusCode?: unknown; message?: unknown };
  return record.status === 404 || record.statusCode === 404 ||
    (typeof record.message === "string" && /\bnot found\b/i.test(record.message));
}

function reply(command: Command, status: Reply["status"], result?: Record<string, unknown>, error?: string): Reply {
  return {
    schema_version: "1",
    request_id: command.request_id,
    operation_id: command.operation_id,
    command: command.command,
    status,
    ...(result ? { result } : {}),
    ...(error ? { error } : {}),
  };
}

function summarizeUsage(message: SDKMessage): BridgeEvent["usage"] {
  if (message.type !== "stream_event" || !message.event || typeof message.event !== "object") {
    return { completeness: "UNKNOWN" };
  }
  return normalizeUsageStatistics(message.event);
}

function trustedOperationOtid(operationId: string, binding: SessionBinding): string {
  const envelope = Buffer.from(JSON.stringify({ version: 1, operation_id: operationId, ...binding }))
    .toString("base64url");
  return `hekate:v1:${envelope}`;
}

function summarizeSdkMessage(
  message: SDKMessage,
  operationId: string,
  binding: SessionBinding,
  index: number,
): BridgeEvent {
  const result = message.type === "result" ? message : undefined;
  const errorCode = message.type === "error" || message.type === "result"
    ? message.errorCode
    : undefined;
  const streamEvent = message.type === "stream_event" && message.event && typeof message.event === "object"
    ? message.event as Record<string, unknown>
    : undefined;
  const eventType = streamEvent?.message_type === "usage_statistics"
    ? "usage_statistics"
    : streamEvent?.message_type === "event_message" && streamEvent.event_type === "compaction"
      ? "compaction"
      : message.type;
  return {
    schema_version: "1",
    event_id: `${operationId}:${index}:${message.type}`,
    operation_id: operationId,
    binding,
    event_type: eventType,
    usage: summarizeUsage(message),
    ...(errorCode ? { error_code: errorCode } : {}),
    ...(result ? { success: result.success } : {}),
  };
}

async function runTurn(
  entry: SessionEntry,
  command: Extract<Command, { command: "session.turn" }>,
): Promise<void> {
  entry.turnState = "RUNNING";
  entry.events = [];
  let resultSeen = false;
  let dispatchAccepted = false;
  const consume = async (): Promise<void> => {
    let index = 0;
    for await (const message of entry.session.stream()) {
      const binding: SessionBinding = {
        ...entry.binding,
        conversation_id: entry.conversationId,
      };
      const event = summarizeSdkMessage(message, command.operation_id, binding, index++);
      encodeEvent(event);
      const callId = event.usage.accounting_call_id;
      const previousIndex = callId
        ? entry.events.findIndex((candidate) => candidate.usage.accounting_call_id === callId)
        : -1;
      if (previousIndex >= 0) {
        if (entry.events[previousIndex].event_type === "usage_conflict") continue;
        try {
          entry.events[previousIndex] = {
            ...event,
            usage: mergeUsageUpdate(entry.events[previousIndex].usage, event.usage),
          };
        } catch (error) {
          if (!(error instanceof UsageConflictError)) throw error;
          entry.events[previousIndex] = {
            ...event,
            event_type: "usage_conflict",
            usage: { completeness: "UNKNOWN", accounting_call_id: callId },
          };
        }
      } else {
        entry.events.push(event);
      }
      if (message.type === "result") {
        // SDK 0.8.25 emits trailing usage before result, or after its 100 ms
        // grace timeout when the App Server never sends usage.
        resultSeen = true;
        entry.turnState = message.errorCode === "stream_closed" || entry.turnState === "UNKNOWN"
          ? "UNKNOWN"
          : message.success ? "COMPLETE" : "FAILED";
        break;
      }
      if (message.type === "error" && message.errorCode === "stream_closed") {
        entry.turnState = "UNKNOWN";
      }
    }
  };

  try {
    const stream = consume();
    await entry.session.send(command.message, {
      otid: trustedOperationOtid(command.operation_id, command.binding),
    });
    dispatchAccepted = true;
    await stream;
    if (!resultSeen) entry.turnState = dispatchAccepted ? "UNKNOWN" : "FAILED";
  } catch (error) {
    const binding: SessionBinding = {
      ...entry.binding,
      conversation_id: entry.conversationId,
    };
    const event: BridgeEvent = {
      schema_version: "1",
      event_id: `${command.operation_id}:error`,
      operation_id: command.operation_id,
      binding,
      event_type: "bridge_error",
      usage: { completeness: "UNKNOWN" },
    };
    encodeEvent(event);
    entry.events.push(event);
    entry.turnState = dispatchAccepted ? "UNKNOWN" : "FAILED";
    process.stderr.write(`turn failed: ${cleanError(error)}\\n`);
  }
}

async function waitForTurn(entry: SessionEntry): Promise<void> {
  if (!entry.turnTask) return;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      entry.turnTask,
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, 120_000);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export async function routeCommand(command: Command): Promise<Reply> {
  switch (command.command) {
    case "hello":
      return reply(command, "CONFIRMED", {
        kind: "capabilities",
        sdk_version: "0.8.25",
        backend: "remote",
        protocol_version: "1",
        capabilities: {
          agent_create: true,
          agent_get: true,
          agent_list: true,
          agent_delete: true,
          session_prepare: true,
          session_turn: true,
          event_collection: true,
          pre_provider_call: false,
          provider_usage_by_call_id: false,
        },
        limitations: [
          "Provider-call authorization is enforced by the probe gateway using metadata emitted by its pinned runtime patch.",
          "Runtime usage fields are forwarded when present; the stock SDK contract does not guarantee physical provider-call identity.",
        ],
      });

    case "agent.create": {
      const tags = [
        `hekate-owner:${command.owner}`,
        `hekate-creation:${command.creation_tag}`,
        `hekate-role:${command.role}`,
      ];
      const providerAgentId = await client.createAgent({
        name: `hekate-probe-${command.creation_tag}`,
        description: "Ephemeral HEKATE Phase 1 integration probe agent",
        tags,
        baseTools: [],
        ...(command.model ? { model: command.model } : {}),
      });
      if (command.max_input_tokens !== undefined || command.max_output_tokens !== undefined) {
        await client.agents.update(providerAgentId, {
          modelSettings: {
            ...(command.max_input_tokens !== undefined
              ? { context_window_limit: command.max_input_tokens }
              : {}),
            ...(command.max_output_tokens !== undefined
              ? { max_tokens: command.max_output_tokens }
              : {}),
          } as never,
        });
      }
      return reply(command, "CONFIRMED", {
        kind: "agent",
        provider_agent_id: providerAgentId,
        present: true,
        owner: command.owner,
        creation_tag: command.creation_tag,
        tools: [],
      });
    }

    case "agent.get": {
      let agent;
      try {
        agent = await client.agents.retrieve(command.provider_agent_id);
      } catch (error) {
        if (!isNotFoundError(error)) throw error;
        return reply(command, "CONFIRMED", {
          kind: "agent",
          provider_agent_id: command.provider_agent_id,
          present: false,
        });
      }
      return reply(command, "CONFIRMED", {
        kind: "agent",
        provider_agent_id: agent.id,
        present: true,
        owner: Array.isArray(agent.tags)
          ? agent.tags.find((tag) => tag.startsWith("hekate-owner:"))?.slice("hekate-owner:".length) ?? null
          : null,
        creation_tag: Array.isArray(agent.tags)
          ? agent.tags.find((tag) => tag.startsWith("hekate-creation:"))?.slice("hekate-creation:".length) ?? null
          : null,
        tools: Array.isArray(agent.tools) ? agent.tools.map((tool) => tool.name) : [],
        model: agent.model ?? null,
        model_settings: agent.model_settings ?? {},
      });
    }

    case "agent.list": {
      const agents = await client.agents.list({ tags: [`hekate-owner:${command.owner}`], limit: 100 });
      const ids = agents
        .filter((agent) =>
          matchesTag(agent.tags, `hekate-owner:${command.owner}`) &&
          (command.creation_tag === undefined || command.creation_tag === null ||
            matchesTag(agent.tags, `hekate-creation:${command.creation_tag}`)),
        )
        .map((agent) => agent.id);
      return reply(command, "CONFIRMED", { kind: "agents", provider_agent_ids: ids });
    }

    case "agent.delete": {
      sessions.get(command.provider_agent_id)?.session.close();
      sessions.delete(command.provider_agent_id);
      try {
        await client.agents.delete(command.provider_agent_id);
      } catch (error) {
        if (!isNotFoundError(error)) throw error;
      }
      let present = true;
      try {
        await client.agents.retrieve(command.provider_agent_id);
      } catch (error) {
        if (!isNotFoundError(error)) throw error;
        present = false;
      }
      return reply(command, "CONFIRMED", {
        kind: "agent",
        provider_agent_id: command.provider_agent_id,
        present,
      });
    }

    case "session.prepare": {
      const previous = sessions.get(command.binding.provider_agent_id);
      previous?.session.close();
      const toolStats = { executorCalls: 0, blockedAttempts: 0 };
      const session = client.createSession(command.binding.provider_agent_id, {
        allowedTools: [],
        toolset: { base: "none" as const },
        tools: [],
        ...(positionCommitSchema
          ? {
              outputFormat: {
                type: "json_schema" as const,
                schema: positionCommitSchema,
                maxRetries: 0,
              },
            }
          : {}),
      });
      const ready = await session.ready();
      const rawInfo = await session.sendCommand(
        { type: "app_server_info" } as never,
        { responseType: "app_server_info_response", timeoutMs: 10_000 },
      );
      const info = rawInfo as unknown as Record<string, unknown>;
      const compatibility = negotiateVersion(info);
      const entry: SessionEntry = {
        session,
        binding: command.binding,
        conversationId: ready.conversationId,
        appServerInfo: info,
        agentTools: ready.tools ?? [],
        turnState: "IDLE",
        events: [],
        toolStats,
      };
      sessions.set(command.binding.provider_agent_id, entry);
      return reply(command, compatibility.compatible ? "CONFIRMED" : "REJECTED", {
        kind: "session",
        provider_agent_id: ready.agentId,
        conversation_id: ready.conversationId,
        model: ready.model ?? null,
        agent_tools: ready.tools ?? [],
        tool_executor_calls: toolStats.executorCalls,
        blocked_tool_attempts: toolStats.blockedAttempts,
        app_server_info: {
          protocol_version: compatibility.protocol_version,
          letta_code_version: info.letta_code_version ?? null,
          backend: info.backend ?? null,
          capabilities: info.capabilities ?? compatibility.capabilities,
          structured_outputs: info.structured_outputs ?? false,
        },
      }, compatibility.compatible ? undefined : "App Server protocol version mismatch");
    }

    case "session.turn": {
      const entry = sessions.get(command.binding.provider_agent_id);
      if (!entry) return reply(command, "REJECTED", undefined, "session is not prepared");
      assertBinding(command, entry.binding);
      if (command.binding.conversation_id !== entry.conversationId) {
        return reply(command, "REJECTED", undefined, "binding mismatch: conversation_id");
      }
      if (entry.turnState === "RUNNING") {
        return reply(command, "UNKNOWN", { kind: "turn", state: "UNKNOWN" }, "turn already running");
      }
      if (entry.turnOperationId === command.operation_id) {
        if (entry.turnState === "UNKNOWN") {
          return reply(command, "UNKNOWN", { kind: "turn", state: "UNKNOWN" }, "execution outcome is unknown");
        }
        return reply(command, "CONFIRMED", { kind: "turn", state: entry.turnState === "COMPLETE" ? "COMPLETED" : "FAILED" });
      }
      entry.turnOperationId = command.operation_id;
      entry.turnTask = runTurn(entry, command);
      return reply(command, "CONFIRMED", { kind: "turn", state: "DISPATCHED" });
    }

    case "events.collect": {
      const entry = sessions.get(command.binding.provider_agent_id);
      if (!entry) return reply(command, "REJECTED", undefined, "session is not prepared");
      assertBinding(command, entry.binding);
      if (command.binding.conversation_id !== entry.conversationId) {
        return reply(command, "REJECTED", undefined, "binding mismatch: conversation_id");
      }
      if (entry.turnOperationId !== command.operation_id) {
        return reply(command, "REJECTED", undefined, "operation_id does not match active turn");
      }
      await waitForTurn(entry);
      const state = entry.turnState === "RUNNING" ? "RUNNING"
        : entry.turnState === "COMPLETE" ? "COMPLETE"
          : entry.turnState === "FAILED" ? "FAILED"
            : entry.turnState === "UNKNOWN" ? "UNKNOWN" : "EMPTY";
      return reply(command, entry.turnState === "RUNNING" ? "UNKNOWN" : "CONFIRMED", {
        kind: "events",
        state,
        events: entry.events,
        tool_executor_calls: entry.toolStats.executorCalls,
        blocked_tool_attempts: entry.toolStats.blockedAttempts,
      });
    }
  }
}

function writeReply(value: Reply): void {
  process.stdout.write(Buffer.concat([Buffer.from(encodeReply(value)), Buffer.from("\n")]));
}

async function handleLine(line: Buffer): Promise<void> {
  try {
    const command = decodeCommand(line);
    try {
      writeReply(await routeCommand(command));
    } catch (error) {
      writeReply(reply(command, "REJECTED", undefined, cleanError(error)));
    }
  } catch (error) {
    const invalid: Reply = {
      schema_version: "1",
      request_id: "invalid",
      operation_id: "invalid",
      command: "unknown",
      status: "REJECTED",
      error: cleanError(error),
    };
    writeReply(invalid);
  }
}

export async function startBridge(): Promise<void> {
  const input = process.stdin;
  let pending = Buffer.alloc(0);
  for await (const chunk of input) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    let offset = 0;
    for (let index = 0; index < bytes.length; index++) {
      if (bytes[index] !== 10) continue;
      const segment = bytes.subarray(offset, index);
      const line = pending.length === 0 ? segment : Buffer.concat([pending, segment]);
      if (line.byteLength > MAX_FRAME_BYTES) {
        writeReply({
          schema_version: "1",
          request_id: "oversized",
          operation_id: "oversized",
          command: "unknown",
          status: "REJECTED",
          error: `bridge frame exceeds ${MAX_FRAME_BYTES} bytes`,
        });
      } else {
        await handleLine(line);
      }
      pending = Buffer.alloc(0);
      offset = index + 1;
    }
    if (offset < bytes.length) {
      const rest = bytes.subarray(offset);
      pending = pending.length === 0 ? Buffer.from(rest) : Buffer.concat([pending, rest]);
      if (pending.byteLength > MAX_FRAME_BYTES) {
        writeReply({
          schema_version: "1",
          request_id: "oversized",
          operation_id: "oversized",
          command: "unknown",
          status: "REJECTED",
          error: `bridge frame exceeds ${MAX_FRAME_BYTES} bytes`,
        });
        pending = Buffer.alloc(0);
        break;
      }
    }
  }
  if (pending.byteLength > 0) await handleLine(pending);
}

export async function shutdown(): Promise<void> {
  for (const entry of sessions.values()) entry.session.close();
  sessions.clear();
  await client.close();
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    await startBridge();
  } finally {
    await shutdown();
  }
}
