export interface MemoryRef {
  path: string;
  content: string;
  version: string;
}

export interface ProjectionReceipt {
  applied_version: number;
  stale: boolean;
}

export async function readMemoryReference(agentId: string, path: string): Promise<MemoryRef> {
  throw new Error("Not implemented");
}

export async function writeProjection(
  agentId: string,
  projection: Record<string, unknown>,
): Promise<ProjectionReceipt> {
  throw new Error("Not implemented");
}

export function validateMemoryPath(path: string, scope: string): void {
  throw new Error("Not implemented");
}
