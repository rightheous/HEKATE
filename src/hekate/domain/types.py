from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import NewType
from uuid import uuid4

DomainId = NewType("DomainId", str)
Digest = NewType("Digest", str)
TaskId = NewType("TaskId", str)
AttemptId = NewType("AttemptId", str)
RegistryId = NewType("RegistryId", str)
OperationId = NewType("OperationId", str)
EvidenceId = NewType("EvidenceId", str)
TopicId = NewType("TopicId", str)
ReservationId = NewType("ReservationId", str)
ProviderAgentId = NewType("ProviderAgentId", str)
ConversationId = NewType("ConversationId", str)
ProviderCallId = NewType("ProviderCallId", str)
AccountingCallId = NewType("AccountingCallId", str)
PermitId = NewType("PermitId", str)
ResultId = NewType("ResultId", str)
PrincipalId = NewType("PrincipalId", str)
ScopeId = NewType("ScopeId", str)
DeploymentId = NewType("DeploymentId", str)
InboxId = NewType("InboxId", str)
EventId = NewType("EventId", str)
RecoveryCaseId = NewType("RecoveryCaseId", str)
Revision = int
PositionVersion = int
Money = Decimal
Instant = datetime


class IdKind(StrEnum):
    TASK = "task"
    ATTEMPT = "attempt"
    REGISTRY = "registry"
    OPERATION = "operation"
    EVIDENCE = "evidence"
    TOPIC = "topic"
    RESERVATION = "reservation"
    ACCOUNTING_CALL = "accounting_call"
    PERMIT = "permit"
    RESULT = "result"


class TaskStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    STOPPING = "STOPPING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class AttemptStatus(StrEnum):
    PENDING = "PENDING"
    DISPATCHED = "DISPATCHED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"


class AgentState(StrEnum):
    REQUESTED = "REQUESTED"
    CREATING = "CREATING"
    READY = "READY"
    BUSY = "BUSY"
    RETIRING = "RETIRING"
    DELETE_PENDING = "DELETE_PENDING"
    DELETED = "DELETED"
    FAILED = "FAILED"


class ObservationState(StrEnum):
    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    UNKNOWN = "UNKNOWN"


class TaskEvent(StrEnum):
    START = "START"
    WAIT = "WAIT"
    RESUME = "RESUME"
    STOP = "STOP"
    COMPLETE = "COMPLETE"
    CANCEL = "CANCEL"
    FAIL = "FAIL"


class AttemptEvent(StrEnum):
    DISPATCH = "DISPATCH"
    START = "START"
    SUCCEED = "SUCCEED"
    FAIL = "FAIL"
    TIMEOUT = "TIMEOUT"
    CANCEL = "CANCEL"


class AgentEvent(StrEnum):
    CREATE = "CREATE"
    CREATED = "CREATED"
    READY = "READY"
    USE = "USE"
    IDLE = "IDLE"
    RETIRE = "RETIRE"
    DELETE = "DELETE"
    DELETED = "DELETED"
    FAIL = "FAIL"


class ReservationPurpose(StrEnum):
    OPERATION_ENVELOPE = "operation_envelope"
    FINAL_RESPONSE = "final_response"


class ExecutionState(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    UNKNOWN = "UNKNOWN"
    QUIESCENT = "QUIESCENT"


class UsageState(StrEnum):
    UNKNOWN = "UNKNOWN"
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"


class StopReason(StrEnum):
    USER_CANCELLED = "USER_CANCELLED"
    DEADLINE = "DEADLINE"
    BUDGET = "BUDGET"
    POLICY = "POLICY"
    COMPLETED = "COMPLETED"
    NEEDS_USER_INPUT = "NEEDS_USER_INPUT"
    ERROR = "ERROR"
    ROUND_LIMIT = "ROUND_LIMIT"
    NO_NEW_WORK = "NO_NEW_WORK"


def new_id(kind: IdKind) -> DomainId:
    return DomainId(str(uuid4()))


@dataclass(frozen=True, slots=True)
class ActorContext:
    principal_id: PrincipalId
    scope: ScopeId
    authenticated_agent_registry_id: RegistryId | None
    task_id: TaskId | None
    attempt_id: AttemptId | None
    input_revision: Revision | None
    policy_version: str
    authz_epoch: int
    fence: int
