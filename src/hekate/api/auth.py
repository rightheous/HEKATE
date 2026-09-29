from __future__ import annotations

from fastapi import Request

from hekate.domain.types import ActorContext, ScopeId
from hekate.domain.models import Principal


def authenticate_user(request: Request) -> Principal:
    raise NotImplementedError


def authorize_scope(principal: Principal, scope: ScopeId) -> ActorContext:
    raise NotImplementedError
