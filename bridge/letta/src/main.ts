import {
  LettaAgentClient,
  type LettaCodeRemoteClientOptions,
  type LettaCodeSession,
  type SDKMessage,
} from "@letta-ai/letta-agent-sdk";
import { Ajv2020 } from "ajv/dist/2020.js";
import { createHash } from "node:crypto";
import { appendFileSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
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
  outputContract?: "hekate_turn_output_v1" | "critic_turn_output_v1" | "position_commit_v1" | null;
  turnTask?: Promise<void>;
}

const MAX_FRAME_BYTES = 1_048_576;
const structuredOutputProbe = process.env.HEKATE_STRUCTURED_OUTPUT_PROBE === "1";
const MAX_BUSINESS_OUTPUT_BYTES = 65_536;
const outputObservationFile = process.env.HEKATE_OUTPUT_OBSERVATION_FILE;
const outputObservationRaw = outputObservationFile !== undefined && process.env.HEKATE_OUTPUT_OBSERVATION_RAW === "1";
const sessions = new Map<string, SessionEntry>();
const appServerUrl = process.env.HEKATE_LETTA_URL;
if (!appServerUrl) throw new Error("HEKATE_LETTA_URL is required");
const DEFAULT_SESSION_TURN_TIMEOUT_MS = 240_000;
const configuredTurnTimeout = process.env.HEKATE_LETTA_TURN_TIMEOUT_MS;
const sessionTurnTimeoutMs = configuredTurnTimeout === undefined ? DEFAULT_SESSION_TURN_TIMEOUT_MS : Number(configuredTurnTimeout);
if (!Number.isInteger(sessionTurnTimeoutMs) || sessionTurnTimeoutMs < 1 || sessionTurnTimeoutMs > 240_000) {
  throw new Error("HEKATE_LETTA_TURN_TIMEOUT_MS must be an integer from 1 through 240000");
}

type OutputChannelSummary = {
  event_count: number;
  utf8_bytes: number;
  hash: ReturnType<typeof createHash>;
  run_ids: Set<string>;
  message_ids: Set<string>;
  otids: Set<string>;
  events: Array<Record<string, unknown>>;
};

const appServerOutputByOperation = new Map<
  string,
  { assistant: OutputChannelSummary; reasoning: OutputChannelSummary }
>();
const appServerEventOrderByOperation = new Map<string, Array<Record<string, unknown>>>();
const operationByConversation = new Map<string, string>();
const operationByRunId = new Map<string, string>();
const appServerRawBytesByOperation = new Map<string, number>();

function emptyOutputChannelSummary(): OutputChannelSummary {
  return {
    event_count: 0,
    utf8_bytes: 0,
    hash: createHash("sha256"),
    run_ids: new Set(),
    message_ids: new Set(),
    otids: new Set(),
    events: [],
  };
}

function outputChannelSnapshot(value: OutputChannelSummary): Record<string, unknown> {
  return {
    event_count: value.event_count,
    utf8_bytes: value.utf8_bytes,
    sha256: value.hash.copy().digest("hex"),
    run_ids: [...value.run_ids].sort(),
    message_ids: [...value.message_ids].sort(),
    otids: [...value.otids].sort(),
    events: value.events,
  };
}

function appServerOutputSnapshot(operationId: string): Record<string, unknown> {
  const value = appServerOutputByOperation.get(operationId);
  return {
    kind: "app_server_stream_snapshot",
    operation_id: operationId,
    assistant: outputChannelSnapshot(value?.assistant ?? emptyOutputChannelSummary()),
    reasoning: outputChannelSnapshot(value?.reasoning ?? emptyOutputChannelSummary()),
    event_order: appServerEventOrderByOperation.get(operationId) ?? [],
  };
}

function appendOutputObservation(value: Record<string, unknown>): void {
  if (!outputObservationFile) return;
  try {
    appendFileSync(outputObservationFile, `${JSON.stringify(value)}\n`);
  } catch {
    // Diagnostic I/O must not change the runtime output or terminal decision.
  }
}

function boundedUtf8Prefix(value: string, limit: number): Buffer {
  let text = "";
  let bytes = 0;
  for (const point of Array.from(value)) {
    const length = Buffer.byteLength(point, "utf8");
    if (bytes + length > limit) break;
    text += point;
    bytes += length;
  }
  return Buffer.from(text, "utf8");
}

function protocolText(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) {
    return value.map((part) => {
      if (typeof part === "string") return part;
      if (!part || typeof part !== "object") return "";
      const record = part as Record<string, unknown>;
      return typeof record.text === "string" ? record.text : "";
    }).join("");
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return typeof record.text === "string" ? record.text : undefined;
  }
  return undefined;
}

function messageDataText(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (value instanceof ArrayBuffer) return Buffer.from(value).toString("utf8");
  if (value instanceof Uint8Array) return Buffer.from(value).toString("utf8");
  return undefined;
}

function observeAppServerMessage(data: unknown): void {
  if (!outputObservationFile) return;
  const raw = messageDataText(data);
  if (raw === undefined) return;
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return;
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return;
  const message = value as Record<string, unknown>;
  if (message.type !== "stream_delta" || !message.delta || typeof message.delta !== "object") return;
  const delta = message.delta as Record<string, unknown>;
  const messageType = delta.message_type;
  const channel = messageType === "assistant_message"
    ? "assistant"
    : messageType === "reasoning_message" ? "reasoning" : undefined;
  const runtime = message.runtime && typeof message.runtime === "object"
    ? message.runtime as Record<string, unknown>
    : undefined;
  const conversationId = typeof message.conversation_id === "string"
    ? message.conversation_id
    : typeof runtime?.conversation_id === "string" ? runtime.conversation_id : undefined;
  const runId = typeof delta.run_id === "string" ? delta.run_id : undefined;
  const operationId = (runId ? operationByRunId.get(runId) : undefined) ??
    (conversationId ? operationByConversation.get(conversationId) : undefined);
  if (!operationId) return;
  if (runId && !operationByRunId.has(runId)) operationByRunId.set(runId, operationId);
  const order = appServerEventOrderByOperation.get(operationId) ?? [];
  const sequence = order.length + 1;
  order.push({
    sequence,
    event_type: typeof messageType === "string" ? messageType : "unknown",
    run_id: runId ?? null,
    message_id: typeof delta.id === "string" ? delta.id : null,
    otid: typeof delta.otid === "string" ? delta.otid : null,
    received_at_unix_ms: Date.now(),
  });
  appServerEventOrderByOperation.set(operationId, order);
  if (!channel) {
    appendOutputObservation({
      kind: "app_server_stream_event",
      operation_id: operationId,
      ...order[order.length - 1],
    });
    return;
  }
  const content = channel === "assistant"
    ? protocolText(delta.content)
    : (typeof delta.reasoning === "string" ? delta.reasoning : protocolText(delta.content));
  if (!content) return;
  let summary = appServerOutputByOperation.get(operationId);
  if (!summary) {
    summary = { assistant: emptyOutputChannelSummary(), reasoning: emptyOutputChannelSummary() };
    appServerOutputByOperation.set(operationId, summary);
  }
  const channelSummary = summary[channel];
  const bytes = Buffer.from(content, "utf8");
  if (channel === "assistant" && outputObservationRaw) {
    const rawPath = process.env.HEKATE_OUTPUT_APP_SERVER_RAW_FILE;
    const previousBytes = appServerRawBytesByOperation.get(operationId) ?? 0;
    const retained = boundedUtf8Prefix(content, Math.max(0, MAX_BUSINESS_OUTPUT_BYTES - previousBytes));
    if (rawPath && retained.byteLength > 0) {
      try {
        appendFileSync(rawPath, retained);
        appServerRawBytesByOperation.set(operationId, previousBytes + retained.byteLength);
      } catch {
        // Diagnostic I/O must not change the runtime output or terminal decision.
      }
    }
  }
  channelSummary.event_count += 1;
  channelSummary.utf8_bytes += bytes.byteLength;
  channelSummary.hash.update(bytes);
  if (runId) channelSummary.run_ids.add(runId);
  if (typeof delta.id === "string") channelSummary.message_ids.add(delta.id);
  if (typeof delta.otid === "string") channelSummary.otids.add(delta.otid);
  channelSummary.events.push({
    sequence: channelSummary.event_count,
    channel,
    run_id: runId ?? null,
    message_id: typeof delta.id === "string" ? delta.id : null,
    otid: typeof delta.otid === "string" ? delta.otid : null,
    utf8_bytes: bytes.byteLength,
    sha256: createHash("sha256").update(bytes).digest("hex"),
  });
  appendOutputObservation({
    kind: "app_server_stream_event",
    operation_id: operationId,
    ...order[order.length - 1],
    channel,
    utf8_bytes: bytes.byteLength,
    sha256: createHash("sha256").update(bytes).digest("hex"),
  });
}

function outputObservationWebSocket(): NonNullable<LettaCodeRemoteClientOptions["WebSocket"]> {
  const BaseWebSocket = globalThis.WebSocket;
  if (!BaseWebSocket) throw new Error("output observation requires the pinned Node WebSocket runtime");
  class ObservingWebSocket extends BaseWebSocket {
    constructor(url: string | URL, options?: { headers?: Record<string, string> }) {
      super(url, options as unknown as string | string[] | undefined);
      this.addEventListener("message", (event) => observeAppServerMessage(event.data));
    }
  }
  return ObservingWebSocket as unknown as NonNullable<LettaCodeRemoteClientOptions["WebSocket"]>;
}

const client = new LettaAgentClient({
  backend: "remote",
  url: appServerUrl,
  ...(process.env.HEKATE_LETTA_TOKEN
    ? { authToken: process.env.HEKATE_LETTA_TOKEN }
    : {}),
  ...(outputObservationFile ? { WebSocket: outputObservationWebSocket() } : {}),
  requestTimeoutMs: sessionTurnTimeoutMs,
});

const positionCommitSchema = JSON.parse(
  readFileSync(resolve(import.meta.dirname, "../../../contracts/generated/position-commit.v1.schema.json"), "utf8"),
);
const hekateTurnOutputSchema = JSON.parse(
  readFileSync(resolve(import.meta.dirname, "../../../contracts/generated/hekate-turn-output.v1.schema.json"), "utf8"),
);
const criticTurnOutputSchema = JSON.parse(
  readFileSync(resolve(import.meta.dirname, "../../../contracts/generated/critic-turn-output.v1.schema.json"), "utf8"),
);
const outputValidator = new Ajv2020({ allErrors: true, strict: false });
const validateHekateTurnOutput = outputValidator.compile(hekateTurnOutputSchema);
const validateCriticTurnOutput = outputValidator.compile(criticTurnOutputSchema);

function sdkOutputSchema(schema: Record<string, unknown>): Record<string, unknown> {
  // SDK 0.8.25 compiles its outputFormat with draft-07 Ajv. The generated
  // contract is draft 2020-12, but this schema uses the shared subset; remove
  // dialect metadata only in the SDK copy. The bridge still validates against
  // the original generated schema with Ajv2020 before accepting the result.
  const compatible = { ...schema };
  delete compatible.$schema;
  delete compatible.$id;
  return compatible;
}

function parseHekateTurnOutput(raw: string): Record<string, unknown> | undefined {
  try {
    const value: unknown = JSON.parse(raw);
    return value && typeof value === "object" && !Array.isArray(value) && validateHekateTurnOutput(value)
      ? value as Record<string, unknown>
      : undefined;
  } catch {
    return undefined;
  }
}

function parseCriticTurnOutput(raw: string): Record<string, unknown> | undefined {
  try {
    const value: unknown = JSON.parse(raw);
    return value && typeof value === "object" && !Array.isArray(value) && validateCriticTurnOutput(value)
      ? value as Record<string, unknown>
      : undefined;
  } catch {
    return undefined;
  }
}

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

async function callMemoryEndpoint(
  command: Extract<Command, { command: "memory.read" | "memory.project" }>,
): Promise<Reply> {
  const token = process.env.HEKATE_LETTA_TOKEN;
  if (!token) return reply(command, "REJECTED", undefined, "memory projection requires the pinned App Server capability token");
  const endpoint = command.command === "memory.read" ? "/hekate-memory/v1/read" : "/hekate-memory/v1/project";
  const url = new URL(appServerUrl!);
  url.protocol = url.protocol === "wss:" ? "https:" : "http:";
  url.pathname = endpoint;
  url.search = "";
  let response: Response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: JSON.stringify(command),
      signal: AbortSignal.timeout(30_000),
    });
  } catch (error) {
    return reply(command, "UNKNOWN", undefined, cleanError(error));
  }
  let value: unknown;
  try {
    value = await response.json();
  } catch {
    return reply(command, "UNKNOWN", undefined, "pinned App Server returned an unreadable memory response");
  }
  if (!response.ok) {
    const detail = value && typeof value === "object" && typeof (value as { error?: unknown }).error === "string"
      ? (value as { error: string }).error
      : `memory endpoint returned HTTP ${response.status}`;
    return reply(command, response.status >= 500 ? "UNKNOWN" : "REJECTED", undefined, detail);
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return reply(command, "UNKNOWN", undefined, "pinned App Server returned an invalid memory observation");
  }
  return reply(command, "CONFIRMED", value as Record<string, unknown>);
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
  let finalAssistantOutput = "";
  let outputBytes = 0;
  let outputTruncated = false;
  let outputHash = createHash("sha256");
  let sdkAssistantEventCount = 0;
  let sdkAssistantBytes = 0;
  let sdkAssistantRawBytes = 0;
  let sdkAssistantRawTruncated = false;
  const sdkAssistantHash = createHash("sha256");
  let sdkReasoningEventCount = 0;
  let sdkReasoningBytes = 0;
  const sdkReasoningHash = createHash("sha256");
  const sdkMessageOrder: Array<Record<string, unknown>> = [];

  const appendBoundedRaw = (environmentName: string, text: string, alreadyWritten: number): { written: number; truncated: boolean } => {
    const path = process.env[environmentName];
    if (!path || !outputObservationRaw) return { written: alreadyWritten, truncated: false };
    const remaining = Math.max(0, MAX_BUSINESS_OUTPUT_BYTES - alreadyWritten);
    const bytes = Buffer.from(text, "utf8");
    const keep = boundedUtf8Prefix(text, remaining);
    try {
      if (keep.byteLength) appendFileSync(path, keep);
    } catch {
      return { written: alreadyWritten, truncated: true };
    }
    return { written: alreadyWritten + keep.byteLength, truncated: keep.byteLength < bytes.byteLength };
  };

  const resetOutput = (): void => {
    finalAssistantOutput = "";
    outputBytes = 0;
    outputTruncated = false;
    outputHash = createHash("sha256");
  };

  const captureOutput = (text: string): void => {
    const bytes = Buffer.from(text, "utf8");
    outputHash.update(bytes);
    const remaining = MAX_BUSINESS_OUTPUT_BYTES - outputBytes;
    if (remaining > 0) {
      const kept = bytes.subarray(0, remaining);
      finalAssistantOutput += kept.toString("utf8");
      outputBytes += kept.byteLength;
    }
    if (bytes.byteLength > remaining) outputTruncated = true;
  };

  const consume = async (): Promise<void> => {
    let index = 0;
    for await (const message of entry.session.stream()) {
      const binding: SessionBinding = {
        ...entry.binding,
        conversation_id: entry.conversationId,
      };
      const event = summarizeSdkMessage(message, command.operation_id, binding, index++);
      const sdkRecord = message as unknown as Record<string, unknown>;
      const streamRecord = sdkRecord.event && typeof sdkRecord.event === "object"
        ? sdkRecord.event as Record<string, unknown>
        : undefined;
      sdkMessageOrder.push({
        sequence: index,
        sdk_message_type: message.type,
        channel: message.type === "assistant" ? "assistant" : message.type === "reasoning" ? "reasoning" : "control",
        event_type: typeof streamRecord?.message_type === "string" ? streamRecord.message_type : null,
        stop_reason: typeof streamRecord?.stop_reason === "string" ? streamRecord.stop_reason : null,
        run_id: typeof sdkRecord.runId === "string" ? sdkRecord.runId : typeof streamRecord?.run_id === "string" ? streamRecord.run_id : null,
        message_id: typeof sdkRecord.id === "string" ? sdkRecord.id : typeof streamRecord?.id === "string" ? streamRecord.id : null,
        accounting_call_id: event.usage.accounting_call_id ?? null,
        provider_call_id: event.usage.provider_call_id ?? null,
        ...(message.type === "error" ? {
          error_code: message.errorCode ?? null,
          error_message_utf8_bytes: Buffer.byteLength(message.message, "utf8"),
          error_message_sha256: createHash("sha256").update(message.message, "utf8").digest("hex"),
          recoverable: message.recoverable ?? null,
        } : {}),
        ...(message.type === "result" ? {
          result_success: message.success,
          result_error_code: message.errorCode ?? null,
          result_stop_reason: message.stopReason ?? null,
          result_duration_ms: message.durationMs,
        } : {}),
      });
      if (message.type === "result" && message.errorCode === "structured_output_error") {
        process.stderr.write(`structured output rejected: ${(message.errorDetail ?? "unknown validation error").slice(0, 500)}\\n`);
      }
      if (message.type === "assistant") {
        const bytes = Buffer.from(message.content, "utf8");
        sdkAssistantEventCount += 1;
        sdkAssistantBytes += bytes.byteLength;
        sdkAssistantHash.update(bytes);
        captureOutput(message.content);
        const appended = appendBoundedRaw("HEKATE_OUTPUT_ASSISTANT_RAW_FILE", message.content, sdkAssistantRawBytes);
        sdkAssistantRawBytes = appended.written;
        sdkAssistantRawTruncated ||= appended.truncated;
      }
      if (message.type === "reasoning") {
        const bytes = Buffer.from(message.content, "utf8");
        sdkReasoningEventCount += 1;
        sdkReasoningBytes += bytes.byteLength;
        sdkReasoningHash.update(bytes);
      }
      if (message.type === "tool_call") resetOutput();
      const callId = event.usage.accounting_call_id;
      if (callId) {
        const previousIndex = entry.events.findIndex((candidate) =>
          candidate.usage.accounting_call_id === callId && candidate.event_type !== "usage_conflict",
        );
        const previous = previousIndex < 0 ? undefined : entry.events[previousIndex];
        const priorConflict = entry.events.some((candidate) =>
          candidate.usage.accounting_call_id === callId && candidate.event_type === "usage_conflict",
        );
        let projection: BridgeEvent["usage"] | undefined;
        let conflict = priorConflict;
        if (previous) projection = previous.usage;
        if (!conflict && projection) {
          try {
            projection = mergeUsageUpdate(projection, event.usage);
          } catch (error) {
            if (!(error instanceof UsageConflictError)) throw error;
            conflict = true;
          }
        }
        if (conflict) {
          if (!priorConflict) entry.events.push({
            ...event,
            event_id: event.event_id,
            event_type: "usage_conflict",
          });
        } else if (previousIndex < 0) {
          entry.events.push(event);
        } else {
          entry.events[previousIndex] = { ...event, event_id: entry.events[previousIndex].event_id, usage: projection ?? event.usage };
        }
      }
      if (message.type === "result") {
        // SDK 0.8.25 emits trailing usage before result, or after its 100 ms
        // grace timeout when the App Server never sends usage.
        resultSeen = true;
        if (entry.outputContract === "hekate_turn_output_v1" || entry.outputContract === "critic_turn_output_v1") {
          if (message.success && message.structuredOutput !== undefined) {
            // Portable StructuredOutput returns a validated value on the SDK result;
            // its tool call is not assistant prose and must become the bounded wire payload.
            resetOutput();
            captureOutput(JSON.stringify(message.structuredOutput));
          }
          const outputDigest = outputHash.digest("hex");
          const structured = entry.outputContract === "critic_turn_output_v1"
            ? parseCriticTurnOutput(finalAssistantOutput)
            : parseHekateTurnOutput(finalAssistantOutput);
          const structuredObject = structured && typeof structured === "object" && !Array.isArray(structured)
            ? structured as Record<string, unknown>
            : undefined;
          const structuredTooLarge = structuredObject !== undefined &&
            Buffer.byteLength(JSON.stringify(structuredObject), "utf8") > MAX_BUSINESS_OUTPUT_BYTES;
          const valid = message.success && structuredObject !== undefined && !outputTruncated && !structuredTooLarge;
          const bridgeRawBytes = Buffer.from(finalAssistantOutput, "utf8");
          const sdkResult = typeof message.result === "string" ? Buffer.from(message.result, "utf8") : undefined;
          if (outputObservationRaw) {
            for (const [environmentName, value] of [
              ["HEKATE_OUTPUT_SDK_RESULT_RAW_FILE", typeof message.result === "string" ? message.result : ""],
              ["HEKATE_OUTPUT_BRIDGE_RAW_FILE", finalAssistantOutput],
            ] as const) {
              appendBoundedRaw(environmentName, value, 0);
            }
          }
          if (outputObservationFile) {
            appendOutputObservation({
              kind: "sdk_turn_result",
              operation_id: command.operation_id,
              task_id: command.binding.task_id,
              attempt_id: command.binding.attempt_id,
              registry_id: command.binding.agent_registry_id,
              input_revision: command.binding.input_revision,
              conversation_id: entry.conversationId,
              app_server: appServerOutputSnapshot(command.operation_id),
              sdk_assistant: {
                event_count: sdkAssistantEventCount,
                utf8_bytes: sdkAssistantBytes,
                sha256: sdkAssistantHash.copy().digest("hex"),
              },
              sdk_reasoning: {
                event_count: sdkReasoningEventCount,
                utf8_bytes: sdkReasoningBytes,
                sha256: sdkReasoningHash.copy().digest("hex"),
              },
              sdk_result: sdkResult ? {
                event_count: 1,
                utf8_bytes: sdkResult.byteLength,
                sha256: createHash("sha256").update(sdkResult).digest("hex"),
              } : { event_count: 0, utf8_bytes: null, sha256: null },
              sdk_message_order: sdkMessageOrder,
              sdk_assistant_raw: {
                file: process.env.HEKATE_OUTPUT_ASSISTANT_RAW_FILE ?? null,
                captured_utf8_bytes: sdkAssistantRawBytes,
                truncated: sdkAssistantRawTruncated,
              },
              ...(outputObservationRaw ? {
                app_server_assistant_raw_file: process.env.HEKATE_OUTPUT_APP_SERVER_RAW_FILE ?? null,
                app_server_assistant_raw_captured_utf8_bytes: appServerRawBytesByOperation.get(command.operation_id) ?? 0,
              } : {}),
              ...(outputObservationRaw ? {
                sdk_result_text_raw_file: process.env.HEKATE_OUTPUT_SDK_RESULT_RAW_FILE ?? null,
                sdk_result_text_raw_truncated: Boolean(sdkResult && sdkResult.byteLength > MAX_BUSINESS_OUTPUT_BYTES),
                bridge_raw_output_text_file: process.env.HEKATE_OUTPUT_BRIDGE_RAW_FILE ?? null,
              } : {}),
              bridge_raw_output: {
                event_count: sdkAssistantEventCount,
                utf8_bytes: bridgeRawBytes.byteLength,
                sha256: createHash("sha256").update(bridgeRawBytes).digest("hex"),
                output_truncated: outputTruncated,
                raw_capture_file: process.env.HEKATE_OUTPUT_BRIDGE_RAW_FILE ?? null,
              },
            });
          }
          const event: BridgeEvent = {
            schema_version: "1",
            event_id: `${command.operation_id}:business-result`,
            operation_id: command.operation_id,
            binding,
            event_type: "business_result",
            usage: { completeness: "UNKNOWN" },
            ...(message.errorCode ? { error_code: message.errorCode } : {}),
            business_result: {
              state: valid ? "VALID" : finalAssistantOutput ? "INVALID" : "MISSING",
              raw_output: finalAssistantOutput,
              output_sha256: outputDigest,
              output_truncated: outputTruncated || structuredTooLarge,
              ...(valid ? { structured_output: structuredObject } : {}),
              ...(message.errorCode ? { failure_code: message.errorCode } : {}),
            },
          };
          encodeEvent(event);
          entry.events.push(event);
        }
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
    operationByConversation.set(entry.conversationId, command.operation_id);
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

async function waitForTurn(entry: SessionEntry, waitMs: number): Promise<void> {
  if (!entry.turnTask) return;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      entry.turnTask,
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, waitMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export async function routeCommand(
  command: Command,
  dependencies: { client: LettaAgentClient; sessions: Map<string, SessionEntry> } = { client, sessions },
): Promise<Reply> {
  const { client, sessions } = dependencies;
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
          memory_projection: true,
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
      const persistence = command.persistence ?? (command.role === "hekate" ? "persistent" : "ephemeral");
      const tags = [
        `hekate-owner:${command.owner}`,
        `hekate-creation:${command.creation_tag}`,
        `hekate-role:${command.role}`,
        `hekate-persistence:${persistence}`,
        ...(command.registry_id ? [`hekate-registry:${command.registry_id}`] : []),
      ];
      const providerAgentId = await client.createAgent({
        name: `hekate-${command.creation_tag}`,
        description: command.role === "hekate" ? "Persistent HEKATE reasoning agent" : "HEKATE Critic agent",
        tags,
        baseTools: [],
        ...(command.model ? { model: command.model } : {}),
        ...(command.system_prompt ? { systemPrompt: command.system_prompt } : {}),
      });
      if (
        command.context_window_tokens !== undefined || command.max_input_tokens !== undefined ||
        command.max_output_tokens !== undefined
      ) {
        await client.agents.update(providerAgentId, {
          modelSettings: {
            ...(command.context_window_tokens !== undefined || command.max_input_tokens !== undefined
              ? { context_window_limit: command.context_window_tokens ?? command.max_input_tokens }
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
        role: Array.isArray(agent.tags)
          ? agent.tags.find((tag) => tag.startsWith("hekate-role:"))?.slice("hekate-role:".length) ?? null
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
        .map((agent) => ({
          provider_agent_id: agent.id,
          owner: command.owner,
          creation_tag: Array.isArray(agent.tags)
            ? agent.tags.find((tag) => tag.startsWith("hekate-creation:"))?.slice("hekate-creation:".length) ?? null
            : null,
          role: Array.isArray(agent.tags)
            ? agent.tags.find((tag) => tag.startsWith("hekate-role:"))?.slice("hekate-role:".length) ?? null
            : null,
        }));
      return reply(command, "CONFIRMED", {
        kind: "agents",
        agents: ids,
        provider_agent_ids: ids.map((agent) => agent.provider_agent_id),
      });
    }

    case "agent.delete": {
      const previous = sessions.get(command.provider_agent_id);
      if (previous && (previous.turnState === "RUNNING" || previous.turnState === "UNKNOWN")) {
        return reply(command, "UNKNOWN", {
          kind: "agent",
          provider_agent_id: command.provider_agent_id,
          execution_state: previous.turnState,
          unresolved_operation_id: previous.turnOperationId ?? null,
        }, "agent has an unresolved execution");
      }
      previous?.session.close();
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

    case "memory.read":
    case "memory.project":
      return callMemoryEndpoint(command);

    case "session.prepare": {
      const previous = sessions.get(command.binding.provider_agent_id);
      if (previous && (previous.turnState === "RUNNING" || previous.turnState === "UNKNOWN")) {
        return reply(command, "UNKNOWN", {
          kind: "session",
          state: previous.turnState,
          unresolved_operation_id: previous.turnOperationId ?? null,
          conversation_id: previous.conversationId,
        }, "agent has an unresolved execution");
      }
      previous?.session.close();
      const toolStats = { executorCalls: 0, blockedAttempts: 0 };
      const selectedOutputSchema = command.output_contract === "position_commit_v1"
        ? positionCommitSchema
        : command.output_contract === "critic_turn_output_v1"
          ? criticTurnOutputSchema
          : command.output_contract === "hekate_turn_output_v1"
            ? hekateTurnOutputSchema
            : structuredOutputProbe && !command.output_contract
              ? positionCommitSchema
              : undefined;
      const sdkSchema = selectedOutputSchema && command.sdk_output_format !== false
        ? sdkOutputSchema(selectedOutputSchema)
        : undefined;
      const session = client.createSession(command.binding.provider_agent_id, {
        allowedTools: [],
        toolset: { base: "none" as const },
        tools: [],
        ...(sdkSchema
          ? {
              outputFormat: {
                type: "json_schema" as const,
                schema: sdkSchema,
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
        outputContract: command.output_contract ?? (structuredOutputProbe ? "position_commit_v1" : undefined),
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
      if (entry.turnState === "RUNNING" || entry.turnState === "UNKNOWN") {
        return reply(command, "UNKNOWN", {
          kind: "turn",
          state: entry.turnState,
          unresolved_operation_id: entry.turnOperationId ?? null,
        }, "agent has an unresolved execution");
      }
      if (entry.turnOperationId === command.operation_id) {
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
      await waitForTurn(entry, command.wait_ms ?? 0);
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
