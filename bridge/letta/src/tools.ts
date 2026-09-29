import type { RuntimeBinding } from "./protocol.js";

export interface ToolCall {
  tool_call_id: string;
  name: string;
  arguments: Record<string, unknown>;
}

export interface ToolResult {
  ok: boolean;
  content?: unknown;
  error?: string;
}

export interface AgentTool {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
  execute(input: Record<string, unknown>): Promise<ToolResult>;
}

export function makeToolDefinitions(binding: RuntimeBinding): AgentTool[] {
  throw new Error("Not implemented");
}

export async function forwardToolCall(binding: RuntimeBinding, call: ToolCall): Promise<ToolResult> {
  throw new Error("Not implemented");
}

export function denyUnexpectedTool(name: string, input: unknown): { block: true; reason: string } {
  throw new Error("Not implemented");
}
