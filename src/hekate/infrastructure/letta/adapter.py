from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from pydantic import TypeAdapter

from hekate.domain.bridge_contracts import BridgeCommand, BridgeReply
from hekate.domain.models import (
    AgentSpec, CancelObservation, CapabilityReport, CreateObservation, DeleteObservation,
    DispatchObservation, ExecutionEnvelope, MemoryProjection, ProviderAgent,
    ProviderCursor, ProviderObservation, RuntimeBinding, RuntimeCapabilities,
    RuntimeEvent, TurnInput,
)
from hekate.domain.types import DeploymentId, ObservationState, OperationId, ProviderAgentId
from hekate.infrastructure.letta.bridge_protocol import BridgeClient, command_base
from hekate.ports.runtime import AgentRuntime

_COMMAND = TypeAdapter(BridgeCommand)


class LettaRuntimeAdapter(AgentRuntime):
    def __init__(self, bridge: BridgeClient) -> None:
        self.bridge = bridge
        self._capabilities: dict[str, object] | None = None

    async def _request(self, operation_id: str, command: str, **values: object) -> BridgeReply:
        request = _COMMAND.validate_python({**command_base(operation_id, command), **values}, strict=True)
        return await self.bridge.request(request)

    async def verify_compatibility(self) -> CapabilityReport:
        reply = await self._request("runtime:compatibility", "hello")
        if reply.status != "CONFIRMED" or reply.result is None or reply.result.kind != "capabilities":
            raise RuntimeError("Letta bridge capability negotiation failed")
        result = reply.result
        if result.protocol_version != "1" or result.sdk_version != "0.8.25":
            raise RuntimeError("Letta bridge version differs from the pinned contract")
        self._capabilities = {
            "sdk_version": result.sdk_version,
            "protocol_version": result.protocol_version,
            "capabilities": result.capabilities,
            "limitations": result.limitations,
        }
        return self._capabilities

    async def capabilities(self) -> RuntimeCapabilities:
        return await self.verify_compatibility()

    async def create_agent(self, spec: AgentSpec, operation_id: OperationId) -> CreateObservation:
        reply = await self._request(
            str(operation_id), "agent.create",
            owner=str(spec.get("owner", "")),
            creation_tag=str(spec.get("creation_tag", operation_id)),
            role=spec.get("role", "hekate"),
            **({"model": spec["model"]} if "model" in spec else {}),
            **({"max_input_tokens": spec["max_input_tokens"]} if "max_input_tokens" in spec else {}),
            **({"max_output_tokens": spec["max_output_tokens"]} if "max_output_tokens" in spec else {}),
        )
        if reply.status != "CONFIRMED" or reply.result is None or reply.result.kind != "agent":
            raise RuntimeError(f"Letta agent creation was not confirmed ({reply.status}): {reply.error or 'unexpected result'}")
        return reply.result.model_dump(mode="json")

    async def list_owned_agents(
        self, owner: DeploymentId, creation_op: OperationId | None,
    ) -> Sequence[ProviderAgent]:
        reply = await self._request(
            str(creation_op or f"agent-list:{owner}"), "agent.list", owner=str(owner),
            **({"creation_tag": str(creation_op)} if creation_op else {}),
        )
        if reply.status != "CONFIRMED" or reply.result is None or reply.result.kind != "agents":
            raise RuntimeError("Letta agent list was not confirmed")
        return tuple({"provider_agent_id": item} for item in reply.result.provider_agent_ids)

    async def observe_agent(self, provider_id: ProviderAgentId) -> ProviderObservation:
        reply = await self._request(f"agent-get:{provider_id}", "agent.get", provider_agent_id=str(provider_id))
        if reply.status != "CONFIRMED" or reply.result is None or reply.result.kind != "agent":
            raise RuntimeError("Letta agent observation was not confirmed")
        state = ObservationState.PRESENT if reply.result.present else ObservationState.ABSENT
        return ProviderObservation(state=state, evidence="agent.get", observed_at=datetime.now(UTC))

    async def prepare_session(self, binding: RuntimeBinding) -> tuple[RuntimeBinding, Mapping[str, object]]:
        reply = await self._request(
            f"session-prepare:{binding.attempt_id}", "session.prepare",
            binding={
                "task_id": str(binding.task_id), "attempt_id": str(binding.attempt_id),
                "agent_registry_id": str(binding.agent_registry_id),
                "provider_agent_id": str(binding.provider_agent_id),
                "input_revision": binding.input_revision, "fence": binding.fence,
            },
        )
        if reply.status != "CONFIRMED" or reply.result is None or reply.result.kind != "session":
            raise RuntimeError("Letta session preparation was not confirmed")
        session = reply.result
        trusted = binding.model_copy(update={"conversation_id": session.conversation_id})
        return trusted, session.model_dump(mode="json")

    async def start_turn(
        self, binding: RuntimeBinding, capsule: TurnInput, envelope: ExecutionEnvelope,
    ) -> DispatchObservation:
        message = capsule.get("message") or capsule.get("objective") or capsule.get("prompt")
        if not isinstance(message, str) or not message:
            raise ValueError("turn input must contain a non-empty message")
        reply = await self._request(
            str(envelope.operation_id), "session.turn",
            binding={
                "task_id": str(binding.task_id), "attempt_id": str(binding.attempt_id),
                "agent_registry_id": str(binding.agent_registry_id),
                "provider_agent_id": str(binding.provider_agent_id),
                "conversation_id": binding.conversation_id,
                "input_revision": binding.input_revision, "fence": binding.fence,
            },
            message=message,
        )
        if reply.result is None or reply.result.kind != "turn":
            raise RuntimeError("Letta turn reply did not include a typed turn result")
        if reply.status == "REJECTED":
            raise RuntimeError("Letta bridge rejected the turn")
        return {"state": reply.result.state, "accepted": reply.status == "CONFIRMED"}

    async def collect(self, binding: RuntimeBinding, operation_id: OperationId, wait_ms: int = 500) -> Mapping[str, object]:
        reply = await self._request(
            str(operation_id), "events.collect",
            binding={
                "task_id": str(binding.task_id), "attempt_id": str(binding.attempt_id),
                "agent_registry_id": str(binding.agent_registry_id),
                "provider_agent_id": str(binding.provider_agent_id),
                "conversation_id": binding.conversation_id,
                "input_revision": binding.input_revision, "fence": binding.fence,
            },
            wait_ms=wait_ms,
        )
        if reply.status == "REJECTED" or reply.result is None or reply.result.kind != "events":
            raise RuntimeError("Letta event collection was not confirmed")
        return {**reply.result.model_dump(mode="json"), "reply_status": reply.status}

    async def events(self, cursor: ProviderCursor | None) -> AsyncIterator[RuntimeEvent]:
        raise NotImplementedError("Letta events require the immutable runtime binding and operation id; use collect()")
        yield  # pragma: no cover

    async def abort(self, binding: RuntimeBinding, operation_id: OperationId) -> CancelObservation:
        raise NotImplementedError("the pinned bridge does not expose a verified abort operation")

    async def delete_agent(self, provider_id: ProviderAgentId, operation_id: OperationId) -> DeleteObservation:
        reply = await self._request(str(operation_id), "agent.delete", provider_agent_id=str(provider_id))
        if reply.status != "CONFIRMED" or reply.result is None or reply.result.kind != "agent":
            raise RuntimeError("Letta agent deletion was not confirmed")
        return reply.result.model_dump(mode="json")

    async def recover_turn(self, binding: RuntimeBinding, otid: str) -> object:
        raise NotImplementedError("same-execution resume is unsupported")

    async def project_memory(self, binding: RuntimeBinding, projection: MemoryProjection) -> object:
        raise NotImplementedError("memory projection is outside Phase 3")
