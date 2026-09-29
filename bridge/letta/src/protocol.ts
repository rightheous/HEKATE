export interface RuntimeBinding {
  task_id: string;
  attempt_id: string;
  agent_registry_id: string;
  provider_agent_id: string;
  conversation_id: string;
  input_revision: number;
  fence: number;
}

export interface Command {
  schema_version: "1";
  request_id: string;
  operation_id: string;
  command: string;
  binding?: RuntimeBinding;
  payload: Record<string, unknown>;
  envelope_id?: string;
}

export interface BridgeFrame {
  schema_version: "1";
  request_id: string;
  [key: string]: unknown;
}

export interface Reply {
  request_id: string;
  status: "CONFIRMED" | "REJECTED" | "UNKNOWN";
  payload?: Record<string, unknown>;
  error?: string;
}

export interface Compatibility {
  compatible: boolean;
  protocol_version: string;
  capabilities: Record<string, boolean>;
  limitations: string[];
}

export interface DomainEvent {
  schema_version: "1";
  binding: RuntimeBinding;
  stable_event_key: string;
  event_kind: string;
  payload: Record<string, unknown>;
}

export function decodeCommand(frame: Uint8Array): Command {
  throw new Error("Not implemented");
}

export function encodeEvent(event: DomainEvent): Uint8Array {
  throw new Error("Not implemented");
}

export function assertBinding(command: Command, active: RuntimeBinding): void {
  throw new Error("Not implemented");
}

export function negotiateVersion(hello: unknown): Compatibility {
  throw new Error("Not implemented");
}
