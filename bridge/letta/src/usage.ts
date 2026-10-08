import type { BridgeEvent } from "./protocol.js";

export class UsageConflictError extends Error {
  constructor() {
    super("conflicting usage fields for one accounting call");
    this.name = "UsageConflictError";
  }
}

function tokenCount(value: unknown): number | undefined {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0
    ? value
    : undefined;
}

export function normalizeUsageStatistics(payload: unknown): BridgeEvent["usage"] {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return { completeness: "UNKNOWN" };
  }
  const record = payload as Record<string, unknown>;
  if (record.message_type !== "usage_statistics") return { completeness: "UNKNOWN" };

  const inputTokens = tokenCount(record.prompt_tokens);
  const outputTokens = tokenCount(record.completion_tokens);
  const totalTokens = tokenCount(record.total_tokens);
  const cost = typeof record.cost_usd === "string" && /^\d+(\.\d+)?$/.test(record.cost_usd)
    ? record.cost_usd
    : undefined;
  const accountingCallId = typeof record.accounting_call_id === "string"
    ? record.accounting_call_id
    : undefined;
  const providerCallId = typeof record.provider_call_id === "string"
    ? record.provider_call_id
    : undefined;
  const hasUsage = inputTokens !== undefined || outputTokens !== undefined ||
    totalTokens !== undefined || cost !== undefined;

  return {
    completeness: !hasUsage
      ? "UNKNOWN"
      : accountingCallId && inputTokens !== undefined && outputTokens !== undefined && totalTokens !== undefined
        ? "COMPLETE"
        : "PARTIAL",
    ...(hasUsage ? { source: "runtime_reported" as const } : {}),
    ...(accountingCallId ? { accounting_call_id: accountingCallId } : {}),
    ...(providerCallId ? { provider_call_id: providerCallId } : {}),
    ...(inputTokens !== undefined ? { input_tokens: inputTokens } : {}),
    ...(outputTokens !== undefined ? { output_tokens: outputTokens } : {}),
    ...(totalTokens !== undefined ? { total_tokens: totalTokens } : {}),
    ...(cost !== undefined ? { cost_usd: cost } : {}),
  };
}

export function mergeUsageUpdate(
  previous: BridgeEvent["usage"],
  next: BridgeEvent["usage"],
): BridgeEvent["usage"] {
  if (!previous.accounting_call_id || previous.accounting_call_id !== next.accounting_call_id) {
    return next;
  }
  for (const key of [
    "provider_call_id", "input_tokens", "output_tokens", "total_tokens", "cost_usd", "source",
  ] as const) {
    if (previous[key] !== undefined && previous[key] !== null &&
        next[key] !== undefined && next[key] !== null && previous[key] !== next[key]) {
      throw new UsageConflictError();
    }
  }
  const merged = { ...previous, ...next };
  const updated = {
    ...merged,
    ...(next.input_tokens === undefined && previous.input_tokens !== undefined
      ? { input_tokens: previous.input_tokens }
      : {}),
    ...(next.output_tokens === undefined && previous.output_tokens !== undefined
      ? { output_tokens: previous.output_tokens }
      : {}),
    ...(next.total_tokens === undefined && previous.total_tokens !== undefined
      ? { total_tokens: previous.total_tokens }
      : {}),
    ...(next.cost_usd === undefined && previous.cost_usd !== undefined
      ? { cost_usd: previous.cost_usd }
      : {}),
  };
  return {
    ...updated,
    completeness: updated.accounting_call_id && updated.input_tokens !== undefined &&
      updated.output_tokens !== undefined && updated.total_tokens !== undefined
      ? "COMPLETE"
      : previous.completeness === "PARTIAL" || next.completeness === "PARTIAL"
        ? "PARTIAL"
        : "UNKNOWN",
  };
}
