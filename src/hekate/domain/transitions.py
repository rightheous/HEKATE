from __future__ import annotations

from hekate.domain.errors import Conflict
from .types import (
    AgentEvent, AgentState, AttemptEvent, AttemptStatus, TaskEvent, TaskStatus,
)


def transition_task(current: TaskStatus, event: TaskEvent) -> TaskStatus:
    allowed = {
        TaskStatus.QUEUED: {
            TaskEvent.START: TaskStatus.RUNNING,
            TaskEvent.CANCEL: TaskStatus.CANCELLED,
            TaskEvent.FAIL: TaskStatus.FAILED,
        },
        TaskStatus.RUNNING: {
            TaskEvent.WAIT: TaskStatus.WAITING,
            TaskEvent.STOP: TaskStatus.STOPPING,
            TaskEvent.COMPLETE: TaskStatus.COMPLETED,
            TaskEvent.CANCEL: TaskStatus.STOPPING,
            TaskEvent.FAIL: TaskStatus.FAILED,
        },
        TaskStatus.WAITING: {
            TaskEvent.RESUME: TaskStatus.RUNNING,
            TaskEvent.STOP: TaskStatus.STOPPING,
            TaskEvent.COMPLETE: TaskStatus.COMPLETED,
            TaskEvent.CANCEL: TaskStatus.STOPPING,
            TaskEvent.FAIL: TaskStatus.FAILED,
        },
        TaskStatus.STOPPING: {
            TaskEvent.COMPLETE: TaskStatus.COMPLETED,
            TaskEvent.CANCEL: TaskStatus.CANCELLED,
            TaskEvent.FAIL: TaskStatus.FAILED,
        },
    }
    try:
        return allowed[current][event]
    except KeyError as error:
        raise Conflict(f"invalid task transition: {current} + {event}") from error


def transition_attempt(current: AttemptStatus, event: AttemptEvent) -> AttemptStatus:
    allowed = {
        AttemptStatus.PENDING: {
            AttemptEvent.DISPATCH: AttemptStatus.DISPATCHED,
            AttemptEvent.FAIL: AttemptStatus.FAILED,
            AttemptEvent.CANCEL: AttemptStatus.CANCELLED,
        },
        AttemptStatus.DISPATCHED: {
            AttemptEvent.START: AttemptStatus.RUNNING,
            AttemptEvent.SUCCEED: AttemptStatus.SUCCEEDED,
            AttemptEvent.FAIL: AttemptStatus.FAILED,
            AttemptEvent.TIMEOUT: AttemptStatus.TIMED_OUT,
            AttemptEvent.CANCEL: AttemptStatus.CANCELLED,
        },
        AttemptStatus.RUNNING: {
            AttemptEvent.SUCCEED: AttemptStatus.SUCCEEDED,
            AttemptEvent.FAIL: AttemptStatus.FAILED,
            AttemptEvent.TIMEOUT: AttemptStatus.TIMED_OUT,
            AttemptEvent.CANCEL: AttemptStatus.CANCELLED,
        },
    }
    try:
        return allowed[current][event]
    except KeyError as error:
        raise Conflict(f"invalid attempt transition: {current} + {event}") from error


def transition_agent(current: AgentState, event: AgentEvent) -> AgentState:
    allowed = {
        AgentState.REQUESTED: {
            AgentEvent.CREATE: AgentState.CREATING,
            AgentEvent.FAIL: AgentState.FAILED,
        },
        AgentState.CREATING: {
            AgentEvent.CREATED: AgentState.READY,
            AgentEvent.FAIL: AgentState.FAILED,
        },
        AgentState.READY: {
            AgentEvent.USE: AgentState.BUSY,
            AgentEvent.RETIRE: AgentState.RETIRING,
            AgentEvent.FAIL: AgentState.FAILED,
        },
        AgentState.BUSY: {
            AgentEvent.IDLE: AgentState.READY,
            AgentEvent.RETIRE: AgentState.RETIRING,
            AgentEvent.FAIL: AgentState.FAILED,
        },
        AgentState.RETIRING: {
            AgentEvent.DELETE: AgentState.DELETE_PENDING,
            AgentEvent.FAIL: AgentState.DELETE_PENDING,
        },
        AgentState.DELETE_PENDING: {
            AgentEvent.DELETED: AgentState.DELETED,
            AgentEvent.FAIL: AgentState.DELETE_PENDING,
        },
    }
    try:
        return allowed[current][event]
    except KeyError as error:
        raise Conflict(f"invalid agent transition: {current} + {event}") from error
