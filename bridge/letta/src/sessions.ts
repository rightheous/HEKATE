import type { RuntimeBinding } from "./protocol.js";

export interface RuntimePolicy {
  model_allowlist: string[];
  max_tool_calls: number;
  max_output_tokens: number;
}

export interface ReadySession {
  binding: RuntimeBinding;
  session_id: string;
  applied_policy: RuntimePolicy;
}

export interface DispatchObservation {
  state: "CONFIRMED" | "UNKNOWN";
  operation_id: string;
  turn_id?: string;
}

export async function prepareSession(binding: RuntimeBinding, policy: RuntimePolicy): Promise<ReadySession> {
  throw new Error("Not implemented");
}

export async function startTurn(
  binding: RuntimeBinding,
  input: unknown,
  envelope: Record<string, unknown>,
): Promise<DispatchObservation> {
  throw new Error("Not implemented");
}

export async function abortTurn(binding: RuntimeBinding): Promise<{ accepted: boolean; terminal: boolean }> {
  throw new Error("Not implemented");
}

export async function recoverTurn(binding: RuntimeBinding, operationId: string): Promise<unknown> {
  throw new Error("Not implemented");
}

export async function closeSession(binding: RuntimeBinding): Promise<void> {
  throw new Error("Not implemented");
}
