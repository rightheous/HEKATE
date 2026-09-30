from __future__ import annotations

from datetime import timedelta

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from hekate.domain.errors import Conflict, PolicyDenied, StaleInput
from hekate.domain.models import AgentRecord, Lease
from hekate.domain.types import (
    AgentState,
    OperationId,
    PrincipalId,
    ProviderAgentId,
    RegistryId,
    ScopeId,
    TaskId,
)

from . import tables
from .common import aware_now, new_key


class PostgresAgentRepository:
    def __init__(self, connection: AsyncSession) -> None:
        self.connection = connection

    async def get_persistent_owner(self, owner: PrincipalId) -> AgentRecord | None:
        row = (await self.connection.execute(select(tables.agent_registry).join(
            tables.authorization_scopes,
            tables.authorization_scopes.c.id == tables.agent_registry.c.owner_scope,
        ).where(
            tables.authorization_scopes.c.principal_id == owner,
            tables.agent_registry.c.role == "hekate",
            tables.agent_registry.c.persistence == "persistent",
        ))).mappings().one_or_none()
        return self._record(row) if row else None

    async def insert_intent(self, record: AgentRecord) -> None:
        if record.kind == "critic":
            if record.task_id is None:
                raise ValueError("critic registry requires task_id")
            task = (await self.connection.execute(select(tables.tasks).where(
                tables.tasks.c.id == record.task_id,
            ).with_for_update())).mappings().one_or_none()
            if task is None:
                raise StaleInput("critic task is unavailable")
            changed = await self.connection.execute(update(tables.tasks).where(
                tables.tasks.c.id == record.task_id,
                tables.tasks.c.critic_agents < 1,
            ).values(critic_agents=tables.tasks.c.critic_agents + 1))
            if changed.rowcount != 1:
                raise PolicyDenied("task critic-agent cap reached")
        await self.connection.execute(insert(tables.agent_registry).values(
            id=record.registry_id,
            owner_scope=record.owner_scope,
            task_id=record.task_id,
            role=record.kind,
            persistence=record.persistence,
            creation_operation_id=record.creation_operation_id,
            provider_agent_id=record.provider_id,
            intended_state=record.intended_state,
            observed_state=record.observation,
            observed_at=record.observed_at,
            active_attempt_id=record.active_attempt_id,
            policy_version=record.policy_version,
        ))

    @staticmethod
    def _record(row) -> AgentRecord:
        from hekate.domain.types import AttemptId

        return AgentRecord(
            registry_id=RegistryId(row["id"]),
            owner_scope=ScopeId(row["owner_scope"]),
            kind=row["role"],
            task_id=TaskId(row["task_id"]) if row["task_id"] else None,
            persistence=row["persistence"],
            creation_operation_id=OperationId(row["creation_operation_id"]),
            provider_id=ProviderAgentId(row["provider_agent_id"]) if row["provider_agent_id"] else None,
            intended_state=row["intended_state"],
            observation=row["observed_state"],
            observed_at=row["observed_at"],
            active_attempt_id=AttemptId(row["active_attempt_id"]) if row["active_attempt_id"] else None,
            policy_version=row["policy_version"],
        )

    async def lock_registry(self, registry_id: RegistryId) -> AgentRecord:
        row = (await self.connection.execute(select(tables.agent_registry).where(
            tables.agent_registry.c.id == registry_id,
        ).with_for_update())).mappings().one_or_none()
        if row is None:
            raise StaleInput("agent registry entry is unavailable")
        return self._record(row)

    async def bind_provider(self, registry_id: RegistryId, provider_id: ProviderAgentId) -> None:
        record = await self.lock_registry(registry_id)
        if record.provider_id is not None and record.provider_id != provider_id:
            raise Conflict("registry is already bound to a provider agent")
        await self.connection.execute(update(tables.agent_registry).where(
            tables.agent_registry.c.id == registry_id,
        ).values(provider_agent_id=provider_id))

    async def update_observation(self, registry_id: RegistryId, observation: object) -> None:
        await self.lock_registry(registry_id)
        state = getattr(observation, "state", None)
        observed_at = getattr(observation, "observed_at", None)
        if state is None or observed_at is None:
            raise TypeError("observation must provide state and observed_at")
        await self.connection.execute(update(tables.agent_registry).where(
            tables.agent_registry.c.id == registry_id,
        ).values(observed_state=str(getattr(state, "value", state)), observed_at=observed_at))

    async def acquire_lease(self, registry_id: RegistryId, owner: str, ttl: float) -> Lease | None:
        if ttl <= 0:
            raise ValueError("lease ttl must be positive")
        await self.lock_registry(registry_id)
        now = aware_now()
        row = (await self.connection.execute(select(tables.agent_leases).where(
            tables.agent_leases.c.registry_id == registry_id,
        ).with_for_update())).mappings().one_or_none()
        if row is not None and row["expires_at"] > now and row["owner_worker"] != owner:
            return None
        fence = row["fence"] if row and row["expires_at"] > now else (row["fence"] + 1 if row else 1)
        expires_at = now + timedelta(seconds=ttl)
        if row is None:
            await self.connection.execute(insert(tables.agent_leases).values(
                registry_id=registry_id,
                owner_worker=owner,
                fence=fence,
                expires_at=expires_at,
            ))
        else:
            await self.connection.execute(update(tables.agent_leases).where(
                tables.agent_leases.c.registry_id == registry_id,
            ).values(owner_worker=owner, fence=fence, expires_at=expires_at))
        return Lease(registry_id=registry_id, owner=owner, fence=fence, expires_at=expires_at)

    async def assert_current_lease(self, registry_id: RegistryId, owner: str, fence: int) -> Lease:
        row = (await self.connection.execute(select(tables.agent_leases).where(
            tables.agent_leases.c.registry_id == registry_id,
        ).with_for_update())).mappings().one_or_none()
        now = aware_now()
        if row is None or row["owner_worker"] != owner or row["fence"] != fence or row["expires_at"] <= now:
            raise StaleInput("agent lease or fence is stale")
        return Lease(registry_id=registry_id, owner=owner, fence=fence, expires_at=row["expires_at"])

    async def renew_lease(self, lease: Lease) -> Lease:
        await self.lock_registry(lease.registry_id)
        current = await self.assert_current_lease(lease.registry_id, lease.owner, lease.fence)
        if lease.expires_at <= current.expires_at:
            return current
        result = await self.connection.execute(update(tables.agent_leases).where(
            tables.agent_leases.c.registry_id == lease.registry_id,
            tables.agent_leases.c.owner_worker == lease.owner,
            tables.agent_leases.c.fence == lease.fence,
            tables.agent_leases.c.expires_at > aware_now(),
        ).values(expires_at=lease.expires_at))
        if result.rowcount != 1:
            raise StaleInput("agent lease changed")
        return lease

    async def release_lease(self, lease: Lease) -> None:
        await self.lock_registry(lease.registry_id)
        result = await self.connection.execute(update(tables.agent_leases).where(
            tables.agent_leases.c.registry_id == lease.registry_id,
            tables.agent_leases.c.owner_worker == lease.owner,
            tables.agent_leases.c.fence == lease.fence,
        ).values(expires_at=aware_now()))
        if result.rowcount != 1:
            raise StaleInput("agent lease changed")

    async def active_execution_hold(self, registry_id: RegistryId, *, lock: bool = False):
        query = select(tables.agent_execution_holds).where(
            tables.agent_execution_holds.c.registry_id == registry_id,
            tables.agent_execution_holds.c.quiescent_at.is_(None),
        )
        if lock:
            query = query.with_for_update()
        return (await self.connection.execute(query)).mappings().one_or_none()

    async def create_execution_hold(self, registry_id: RegistryId, operation_id: OperationId) -> None:
        if await self.active_execution_hold(registry_id, lock=True) is not None:
            raise Conflict("agent has an unresolved execution")
        await self.connection.execute(insert(tables.agent_execution_holds).values(
            id=new_key(),
            registry_id=registry_id,
            operation_id=operation_id,
            state="PENDING",
        ))

    async def set_registry_busy(self, registry_id: RegistryId, attempt_id: str) -> None:
        result = await self.connection.execute(update(tables.agent_registry).where(
            tables.agent_registry.c.id == registry_id,
            tables.agent_registry.c.intended_state.in_([AgentState.READY.value, AgentState.BUSY.value]),
        ).values(intended_state=AgentState.BUSY.value, active_attempt_id=attempt_id))
        if result.rowcount != 1:
            raise PolicyDenied("agent is not dispatchable")

    async def set_registry_ready(self, registry_id: RegistryId) -> None:
        result = await self.connection.execute(update(tables.agent_registry).where(
            tables.agent_registry.c.id == registry_id,
            tables.agent_registry.c.intended_state == AgentState.BUSY.value,
        ).values(intended_state=AgentState.READY.value, active_attempt_id=None))
        if result.rowcount != 1:
            raise Conflict("agent state changed before execution became quiescent")
