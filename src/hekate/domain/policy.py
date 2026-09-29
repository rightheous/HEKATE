from __future__ import annotations

from .models import (
    CapabilityGrant, ContextManifest, ModelPolicy, PolicyDecision, PolicySnapshot,
    ProposedAction, ReuseInputs, ScopeSnapshot, ToolDecision, ToolRequest,
)
from .types import Digest, Instant


def evaluate_action(
    snapshot: PolicySnapshot, action: ProposedAction, now: Instant
) -> PolicyDecision:
    raise NotImplementedError


def authorize_tool(
    grant: CapabilityGrant, request: ToolRequest, scope: ScopeSnapshot
) -> ToolDecision:
    raise NotImplementedError


def validate_data_egress(context: ContextManifest, model: ModelPolicy) -> None:
    raise NotImplementedError


def exact_reuse_key(inputs: ReuseInputs) -> Digest:
    raise NotImplementedError
