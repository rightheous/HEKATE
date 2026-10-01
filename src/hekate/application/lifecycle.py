from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from hekate.domain.contracts import canonical_json_hash
from hekate.domain.errors import Conflict, PolicyDenied, UnknownExecution
from hekate.domain.models import (
    AgentRecord, CreateObservation, DeleteObservation, RetirementReceipt,
    SpawnProposal, TaskExecutionConfig,
)
from hekate.domain.types import ActorContext, DeploymentId, OperationId, PrincipalId, ProviderAgentId, RegistryId, ScopeId, TaskId
from hekate.ports.runtime import AgentRuntime
from hekate.ports.store import UowFactory


async def ensure_hekate(
    factory: UowFactory,
    runtime: AgentRuntime,
    actor: ActorContext,
    config: TaskExecutionConfig,
) -> AgentRecord:
    creation_id = OperationId(str(uuid5(NAMESPACE_URL, f"hekate:create:{actor.scope}")))
    registry_id = RegistryId(str(uuid5(NAMESPACE_URL, f"hekate:registry:{actor.scope}")))
    created_now = False
    async with factory() as uow:
        authorization = await uow.tasks.lock_scope(actor.scope)
        if (authorization.principal_id, authorization.policy_version, authorization.authz_epoch) != (
            actor.principal_id, actor.policy_version, actor.authz_epoch,
        ):
            raise PolicyDenied("authorization snapshot changed")
        record = await uow.agents.get_persistent_scope(actor.scope, lock=True)
        if record is None:
            await uow.delivery.claim_lifecycle_operation(
                creation_id,
                actor.scope,
                "agent.create",
                canonical_json_hash({"scope": actor.scope, "role": "hekate", "persistence": "persistent"}),
            )
            record = AgentRecord(
                registry_id=registry_id,
                owner_scope=ScopeId(actor.scope),
                kind="hekate",
                persistence="persistent",
                creation_operation_id=creation_id,
                intended_state="CREATING",
                observation="UNKNOWN",
                policy_version=actor.policy_version,
            )
            await uow.agents.insert_intent(record)
            created_now = True
        await uow.commit()

    if record.provider_id is None:
        if created_now:
            observation = await runtime.create_agent({
                "owner": str(actor.scope),
                "creation_tag": str(record.creation_operation_id),
                "role": "hekate",
                "model": config.letta_model,
                "max_input_tokens": config.max_input_tokens,
                "max_output_tokens": config.max_output_tokens,
            }, record.creation_operation_id)
            provider_id = ProviderAgentId(str(observation["provider_agent_id"]))
        else:
            candidates = await runtime.list_owned_agents(
                DeploymentId(str(actor.scope)), record.creation_operation_id,
            )
            if len(candidates) != 1:
                raise UnknownExecution("persistent HEKATE create intent requires manual recovery")
            candidate = candidates[0]
            provider_id = ProviderAgentId(str(candidate["provider_agent_id"]))
        async with factory() as uow:
            current = await uow.agents.lock_registry(record.registry_id)
            if current.provider_id is not None and current.provider_id != provider_id:
                raise Conflict("persistent HEKATE provider binding changed")
            await uow.agents.bind_provider(record.registry_id, provider_id)
            await uow.commit()
    async with factory() as uow:
        record = await uow.agents.lock_registry(record.registry_id)
        if record.owner_scope != actor.scope or record.policy_version != actor.policy_version:
            raise PolicyDenied("persistent HEKATE scope or policy changed")
        if record.provider_id is None or record.intended_state != "READY":
            raise UnknownExecution("persistent HEKATE is not confirmed ready")
        await uow.commit()
        return record


async def request_critic(task_id: TaskId, proposal: SpawnProposal) -> AgentRecord:
    raise NotImplementedError


async def create_from_intent(operation_id: OperationId) -> CreateObservation:
    raise NotImplementedError


async def retire(registry_id: RegistryId) -> RetirementReceipt:
    raise NotImplementedError


async def confirm_deletion(operation_id: OperationId) -> DeleteObservation:
    raise NotImplementedError
