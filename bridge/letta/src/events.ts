import type { DomainEvent, RuntimeBinding } from "./protocol.js";

export interface UsageEvent {
  binding: RuntimeBinding;
  provider_call_id?: string;
  completeness: "COMPLETE" | "PARTIAL" | "UNKNOWN";
  quantities: Record<string, number | null>;
}

export function normalizeEvent(raw: unknown, binding: RuntimeBinding): DomainEvent | null {
  throw new Error("Not implemented");
}

export function normalizeUsage(raw: unknown, binding: RuntimeBinding): UsageEvent {
  throw new Error("Not implemented");
}

export function makeEventKey(raw: unknown, binding: RuntimeBinding): string {
  throw new Error("Not implemented");
}

export async function forwardWithAck(event: DomainEvent): Promise<void> {
  throw new Error("Not implemented");
}
