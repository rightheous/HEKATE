export interface AgentSpec {
  role: "hekate" | "critic";
  owner: string;
  creation_tag: string;
  configuration: Record<string, unknown>;
}

export interface CreateObservation {
  state: "CONFIRMED" | "UNKNOWN";
  provider_agent_id?: string;
}

export interface Observation {
  state: "PRESENT" | "ABSENT" | "RUNNING" | "STOPPED" | "UNKNOWN";
  observed_at: string;
}

export interface DeleteObservation {
  state: "DELETE_PENDING" | "DELETED" | "UNKNOWN";
}

export async function createAgent(spec: AgentSpec, operationId: string): Promise<CreateObservation> {
  throw new Error("Not implemented");
}

export async function findByCreationTag(owner: string, operationId: string | null): Promise<unknown[]> {
  throw new Error("Not implemented");
}

export async function observeAgent(providerId: string): Promise<Observation> {
  throw new Error("Not implemented");
}

export async function deleteAgent(providerId: string, operationId: string): Promise<DeleteObservation> {
  throw new Error("Not implemented");
}
