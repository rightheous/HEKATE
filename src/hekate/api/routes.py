from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import APIRouter, Depends, Request

from hekate.api.auth import authenticate_user
from hekate.domain.models import (
    CancellationReceipt, InputChange, PositionView, PublicEvent, RevisionReceipt,
    Principal, TaskReceipt, TaskView, UserMessage,
)
from hekate.domain.types import EventId, TaskId, TopicId

router = APIRouter(prefix="/v1")


@router.post("/messages")
async def submit_message(
    body: UserMessage,
    request: Request,
    principal: Principal = Depends(authenticate_user),
) -> TaskReceipt:
    raise NotImplementedError


@router.patch("/tasks/{task_id}/input")
async def change_input(
    task_id: TaskId,
    body: InputChange,
    principal: Principal = Depends(authenticate_user),
) -> RevisionReceipt:
    raise NotImplementedError


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(
    task_id: TaskId,
    principal: Principal = Depends(authenticate_user),
) -> CancellationReceipt:
    raise NotImplementedError


@router.get("/tasks/{task_id}")
async def get_task(
    task_id: TaskId, principal: Principal = Depends(authenticate_user)
) -> TaskView:
    raise NotImplementedError


@router.get("/positions/{topic_id}")
async def get_position(
    topic_id: TopicId, principal: Principal = Depends(authenticate_user)
) -> PositionView:
    raise NotImplementedError


@router.get("/tasks/{task_id}/events", response_model=None)
async def stream_events(
    task_id: TaskId,
    after: EventId,
    principal: Principal = Depends(authenticate_user),
) -> AsyncIterator[PublicEvent]:
    raise NotImplementedError
