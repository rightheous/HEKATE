export interface GuardCapabilities {
  pre_provider_call: boolean;
  compaction: boolean;
  retry_suppression: boolean;
  usage_by_call_id: boolean;
}

export interface AppliedLimits {
  model: string;
  max_input_tokens: number;
  max_output_tokens: number;
  billable_call_slots: number;
}

export interface BillableCallIntent {
  operation_id: string;
  kind: "inference" | "compaction";
  model: string;
  max_input_tokens: number;
  max_output_tokens: number;
}

export async function probeGuardSupport(): Promise<GuardCapabilities> {
  throw new Error("Not implemented");
}

export async function applyRuntimeLimits(
  binding: unknown,
  envelope: Record<string, unknown>,
): Promise<AppliedLimits> {
  throw new Error("Not implemented");
}

export async function authorizeBillableCall(intent: BillableCallIntent): Promise<Record<string, unknown>> {
  throw new Error("Not implemented");
}

export async function recordCallUsage(record: Record<string, unknown>): Promise<void> {
  throw new Error("Not implemented");
}

export function verifyAppliedLimits(expected: AppliedLimits, observed: AppliedLimits): void {
  throw new Error("Not implemented");
}
