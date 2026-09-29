from __future__ import annotations

from hekate.domain.models import (
    MemoryRequest, MutationAuthorization, MutationIntent, RuntimeBinding,
    ToolRequest, ToolResult,
)
from hekate.domain.types import ActorContext


async def execute(binding: RuntimeBinding, request: ToolRequest) -> ToolResult:
    raise NotImplementedError


async def execute_memory_request(
    actor: ActorContext, request: MemoryRequest
) -> ToolResult:
    raise NotImplementedError


async def authorize_mutation(
    actor: ActorContext, intent: MutationIntent
) -> MutationAuthorization:
    raise NotImplementedError
