from __future__ import annotations

from hekate.domain.models import AgentRecord, Lease
from hekate.domain.types import OperationId, PrincipalId, ProviderAgentId, RegistryId


class PostgresAgentRepository:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def get_persistent_owner(self, owner: PrincipalId) -> AgentRecord | None: raise NotImplementedError
    async def insert_intent(self, record: AgentRecord) -> None: raise NotImplementedError
    async def bind_provider(self, registry_id: RegistryId, provider_id: ProviderAgentId) -> None: raise NotImplementedError
    async def update_observation(self, registry_id: RegistryId, observation: object) -> None: raise NotImplementedError
    async def acquire_lease(self, registry_id: RegistryId, owner: str, ttl: float) -> Lease | None: raise NotImplementedError
    async def renew_lease(self, lease: Lease) -> Lease: raise NotImplementedError
    async def release_lease(self, lease: Lease) -> None: raise NotImplementedError
