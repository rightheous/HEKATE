import { Ajv2020, type ValidateFunction } from "ajv/dist/2020.js";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

export interface RuntimeBinding {
  task_id: string;
  attempt_id: string;
  agent_registry_id: string;
  provider_agent_id: string;
  input_revision: number;
  fence: number;
}

export interface SessionBinding extends RuntimeBinding {
  conversation_id: string;
}

interface CommandBase {
  schema_version: "1";
  request_id: string;
  operation_id: string;
}

export type Command =
  | (CommandBase & { command: "hello" })
  | (CommandBase & {
      command: "agent.create";
      owner: string;
      creation_tag: string;
      role: "hekate" | "critic";
      persistence?: "persistent" | "ephemeral" | null;
      registry_id?: string | null;
      model?: string | null;
      max_input_tokens?: number | null;
      max_output_tokens?: number | null;
    })
  | (CommandBase & { command: "agent.get"; provider_agent_id: string })
  | (CommandBase & {
      command: "agent.list";
      owner: string;
      creation_tag?: string | null;
    })
  | (CommandBase & { command: "agent.delete"; provider_agent_id: string })
  | (CommandBase & {
      command: "session.prepare";
      binding: RuntimeBinding;
      output_contract?: "hekate_turn_output_v1" | "critic_turn_output_v1" | "position_commit_v1" | null;
    })
  | (CommandBase & {
      command: "session.turn";
      binding: SessionBinding;
      message: string;
    })
  | (CommandBase & { command: "events.collect"; binding: SessionBinding; wait_ms?: number })
  | (CommandBase & { command: "memory.read"; identity: MemoryIdentity; topic_id: string })
  | (CommandBase & {
      command: "memory.project";
      identity: MemoryIdentity;
      topic_id: string;
      request_hash: string;
      source_version: number;
      format_version: 2;
      payload: string;
      payload_digest: string;
    });

export interface MemoryIdentity {
  owner: string;
  creation_tag: string;
  registry_id: string;
  provider_agent_id: string;
  authz_epoch: number;
  policy_version: string;
  principal_id: string;
  namespace: "hekate.position.v1";
  lease_owner: string;
  lease_fence: number;
  claim_owner: string | null;
  claim_fence: number | null;
}

export interface BridgeEvent {
  schema_version: "1";
  event_id: string;
  operation_id: string;
  binding: SessionBinding;
  event_type: string;
  usage: {
    completeness: "UNKNOWN" | "PARTIAL" | "COMPLETE";
    source?: "runtime_reported" | "provider_reported" | "runtime_estimated" | null;
    accounting_call_id?: string | null;
    provider_call_id?: string | null;
    input_tokens?: number | null;
    output_tokens?: number | null;
    total_tokens?: number | null;
    cost_usd?: string | null;
  };
  error_code?:
    | "approval_conflict"
    | "approval_conflict_terminal"
    | "protocol_error"
    | "error"
    | "llm_api_error"
    | "max_steps"
    | "interrupted"
    | "stream_closed"
    | "structured_output_error"
    | null;
  success?: boolean | null;
  business_result?: {
    state: "VALID" | "INVALID" | "MISSING";
    raw_output?: string | null;
    output_sha256?: string | null;
    output_truncated?: boolean;
    structured_output?: Record<string, unknown> | null;
    failure_code?: BridgeEvent["error_code"];
  } | null;
}

export type DomainEvent = BridgeEvent;

export interface Reply {
  schema_version: "1";
  request_id: string;
  operation_id: string;
  command: string;
  status: "CONFIRMED" | "REJECTED" | "UNKNOWN";
  result?: Record<string, unknown> | null;
  error?: string | null;
}

const MAX_FRAME_BYTES = 1_048_576;
const schema = JSON.parse(
  readFileSync(resolve(import.meta.dirname, "../../../contracts/generated/bridge.v1.schema.json"), "utf8"),
) as object;
const ajv = new Ajv2020({ allErrors: true, strict: false });
ajv.addSchema(schema);

function validator(name: string): ValidateFunction {
  const validate = ajv.getSchema(`urn:hekate:bridge:v1#/$defs/${name}`);
  if (!validate) throw new Error(`missing generated bridge schema: ${name}`);
  return validate;
}

const validateCommand = validator("BridgeCommand");
const validateReply = validator("BridgeReply");
const validateEvent = validator("BridgeEvent");

function assertValid(validate: ValidateFunction, value: unknown, label: string): void {
  if (!validate(value)) {
    const detail = (validate.errors ?? [])
      .map((error) => `${error.instancePath || "/"} ${error.message ?? "invalid"}`)
      .join("; ");
    throw new Error(`${label} rejected by bridge.v1 schema: ${detail}`);
  }
}

export function decodeCommand(frame: Uint8Array): Command {
  if (frame.byteLength > MAX_FRAME_BYTES) {
    throw new Error(`bridge frame exceeds ${MAX_FRAME_BYTES} bytes`);
  }
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(frame));
  } catch {
    throw new Error("invalid UTF-8 JSON bridge frame");
  }
  assertValid(validateCommand, value, "command");
  return value as Command;
}

export function encodeReply(reply: Reply): Uint8Array {
  assertValid(validateReply, reply, "reply");
  const encoded = new TextEncoder().encode(JSON.stringify(reply));
  if (encoded.byteLength > MAX_FRAME_BYTES) {
    throw new Error(`bridge frame exceeds ${MAX_FRAME_BYTES} bytes`);
  }
  return encoded;
}

export function encodeEvent(event: BridgeEvent): Uint8Array {
  assertValid(validateEvent, event, "event");
  const encoded = new TextEncoder().encode(JSON.stringify(event));
  if (encoded.byteLength > MAX_FRAME_BYTES) {
    throw new Error(`bridge frame exceeds ${MAX_FRAME_BYTES} bytes`);
  }
  return encoded;
}

export function assertBinding(command: Command, active: RuntimeBinding): void {
  if (!("binding" in command)) throw new Error("command is missing binding");
  const incoming = command.binding;
  for (const key of [
    "task_id",
    "attempt_id",
    "agent_registry_id",
    "provider_agent_id",
    "input_revision",
    "fence",
  ] as const) {
    if (incoming[key] !== active[key]) throw new Error(`binding mismatch: ${key}`);
  }
  if (
    "conversation_id" in incoming &&
    "conversation_id" in active &&
    incoming.conversation_id !== active.conversation_id
  ) {
    throw new Error("binding mismatch: conversation_id");
  }
}

export function negotiateVersion(info: unknown): {
  compatible: boolean;
  protocol_version: string;
  capabilities: Record<string, boolean>;
  limitations: string[];
} {
  if (!info || typeof info !== "object" || Array.isArray(info)) {
    throw new Error("invalid app_server_info response");
  }
  const record = info as Record<string, unknown>;
  const version = typeof record.protocol_version === "string"
    ? record.protocol_version
    : typeof record.protocol_version === "number" && Number.isInteger(record.protocol_version)
      ? String(record.protocol_version)
      : "unknown";
  return {
    compatible: version === "1",
    protocol_version: version,
    capabilities: {
      structured_outputs: record.structured_outputs === true,
      tools: record.tools === true,
      remote_agent_management: true,
    },
    limitations: [
      "The App Server info response is a capability advertisement, not proof of provider-call accounting.",
      "Provider-call IDs are present only when the selected runtime emits them; stock SDK stream events alone do not guarantee call identity.",
    ],
  };
}
