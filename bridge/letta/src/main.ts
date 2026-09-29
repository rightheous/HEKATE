import type { Command, Reply } from "./protocol.js";

export interface BridgeConfig {
  socket_path: string;
  protocol_version: string;
}

export interface BridgeServer {
  close(): Promise<void>;
}

export async function startBridge(config: BridgeConfig): Promise<BridgeServer> {
  throw new Error("Not implemented");
}

export async function routeCommand(command: Command): Promise<Reply> {
  throw new Error("Not implemented");
}

export async function shutdown(deadline: number): Promise<void> {
  throw new Error("Not implemented");
}
