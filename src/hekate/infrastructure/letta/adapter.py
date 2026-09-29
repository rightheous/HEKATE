from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

from hekate.domain.models import (
    AgentSpec, CancelObservation, CapabilityReport, CreateObservation, DeleteObservation,
    DispatchObservation, ExecutionEnvelope, MemoryProjection, ProviderAgent,
    ProviderCursor, ProviderObservation, RuntimeBinding, RuntimeCapabilities,
    RuntimeEvent, TurnInput,
)
from hekate.domain.types import DeploymentId, OperationId, ProviderAgentId
from hekate.ports.runtime import AgentRuntime


class LettaRuntimeAdapter(AgentRuntime):
    def __init__(self, bridge: object) -> None:
        self.bridge = bridge

    async def verify_compatibility(self) -> CapabilityReport:
        raise NotImplementedError

    async def capabilities(self) -> RuntimeCapabilities: raise NotImplementedError
    async def create_agent(self, spec: AgentSpec, operation_id: OperationId) -> CreateObservation: raise NotImplementedError
    async def list_owned_agents(self, owner: DeploymentId, creation_op: OperationId | None) -> Sequence[ProviderAgent]: raise NotImplementedError
    async def observe_agent(self, provider_id: ProviderAgentId) -> ProviderObservation: raise NotImplementedError
    async def start_turn(self, binding: RuntimeBinding, capsule: TurnInput, envelope: ExecutionEnvelope) -> DispatchObservation: raise NotImplementedError
    async def events(self, cursor: ProviderCursor | None) -> AsyncIterator[RuntimeEvent]: raise NotImplementedError
    async def abort(self, binding: RuntimeBinding, operation_id: OperationId) -> CancelObservation: raise NotImplementedError
    async def delete_agent(self, provider_id: ProviderAgentId, operation_id: OperationId) -> DeleteObservation: raise NotImplementedError
    async def recover_turn(self, binding: RuntimeBinding, otid: str) -> object: raise NotImplementedError
    async def project_memory(self, binding: RuntimeBinding, projection: MemoryProjection) -> object: raise NotImplementedError
