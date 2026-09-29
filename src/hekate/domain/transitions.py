from __future__ import annotations

from .types import (
    AgentEvent, AgentState, AttemptEvent, AttemptStatus, TaskEvent, TaskStatus,
)


def transition_task(current: TaskStatus, event: TaskEvent) -> TaskStatus:
    raise NotImplementedError


def transition_attempt(current: AttemptStatus, event: AttemptEvent) -> AttemptStatus:
    raise NotImplementedError


def transition_agent(current: AgentState, event: AgentEvent) -> AgentState:
    raise NotImplementedError
