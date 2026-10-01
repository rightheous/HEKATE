from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal, Mapping, TypeAlias, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from .contracts import MAX_CONTRACT_ITEMS
from .types import (
    AccountingCallId, AttemptId, AttemptStatus, DomainId, EvidenceId, OperationId, PermitId, ProviderAgentId,
    PrincipalId, ProviderCallId, RegistryId, ReservationId, Revision, ScopeId,
    ObservationState, TaskId, TaskStatus, TopicId,
)


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class TaskCounters(ContractModel):
    critic_agents: int = 0
    review_rounds: int = 0
    schema_repairs: int = 0
    transient_retries: int = 0
    tool_calls: int = 0
    provider_calls: int = 0


class Task(ContractModel):
    id: TaskId
    scope: ScopeId
    question: str
    input_revision: Revision
    constraints_hash: str
    status: TaskStatus
    topic_id: TopicId | None = None
    base_position_version: int = 0
    deadline: datetime
    counters: TaskCounters = Field(default_factory=TaskCounters)
    outcome: str | None = None
    stop_reason: str | None = None


class TaskSnapshot(ContractModel):
    task: Task
    policy_version: str
    model_version: str
    schema_version: str
    constraints_hash: str


def snapshot_task(task: Task) -> TaskSnapshot:
    raise NotImplementedError


class Attempt(ContractModel):
    id: AttemptId
    task_id: TaskId
    kind: str
    parent_attempt_id: AttemptId | None = None
    review_round: int = 0
    input_revision: Revision
    agent_registry_id: RegistryId
    status: AttemptStatus
    operation_id: OperationId
    reservation_id: ReservationId
    deadline: datetime


class AgentRecord(ContractModel):
    registry_id: RegistryId
    owner_scope: ScopeId
    kind: Literal["hekate", "critic"]
    task_id: TaskId | None = None
    persistence: Literal["persistent", "ephemeral"] = "ephemeral"
    creation_operation_id: OperationId
    provider_id: ProviderAgentId | None = None
    intended_state: str
    observation: str
    observed_at: datetime | None = None
    active_attempt_id: AttemptId | None = None
    policy_version: str


class Operation(ContractModel):
    id: OperationId
    kind: str
    request_hash: str
    owner_scope: ScopeId
    state: str
    observation: str
    provider_ids: tuple[str, ...] = ()
    retry_of: OperationId | None = None
    last_error: str | None = None
    next_check_at: datetime | None = None


class RuntimeBinding(ContractModel):
    task_id: TaskId
    attempt_id: AttemptId
    agent_registry_id: RegistryId
    provider_agent_id: ProviderAgentId
    conversation_id: str
    input_revision: Revision
    fence: int


class ExecutionEnvelope(ContractModel):
    task_id: TaskId
    attempt_id: AttemptId
    operation_id: OperationId
    principal_id: PrincipalId
    scope: ScopeId
    input_revision: Revision
    model_allowlist: tuple[str, ...]
    pricing_version: str
    deadline: datetime
    max_input_tokens: int
    max_output_tokens: int
    billable_call_slots: int
    max_tool_calls: int
    fence: int
    reservation_id: ReservationId


class ProviderCallPlan(ContractModel):
    """Immutable admission plan for physical provider requests."""
    profile_id: str = Field(min_length=1, max_length=128)
    model: str = Field(min_length=1, max_length=256)
    pricing_version: str = Field(min_length=1, max_length=128)
    max_input_tokens: int = Field(ge=0)
    max_output_tokens: int = Field(ge=1)
    main_turn_calls: int = Field(ge=0)
    compaction_calls: int = Field(ge=0)
    retry_calls: Literal[0] = 0


class UserMessage(ContractModel):
    text: str


class InputChange(ContractModel):
    text: str
    expected_revision: Revision
    constraints: Mapping[str, object] = Field(default_factory=dict)


class Confidence(ContractModel):
    level: str
    basis: tuple[str, ...] = Field(max_length=MAX_CONTRACT_ITEMS)
    missing_evidence: tuple[str, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)


class Assessment(ContractModel):
    statement: str
    confidence: Confidence


class Objection(ContractModel):
    id: str
    severity: str
    claim: str
    condition: str
    suggested_validation: str


class RecommendedNextStep(ContractModel):
    type: str


class PositionRecommendation(ContractModel):
    action: str
    summary: str


class ConclusionCapsule(ContractModel):
    schema_version: Literal["1"]
    task_id: TaskId
    attempt_id: AttemptId
    agent_id: RegistryId
    status: str
    input_revision: Revision | None = None
    assessment: Assessment
    evidence_used: tuple[EvidenceId, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    objections: tuple[Objection, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    assumptions: tuple[str, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    unresolved: tuple[str, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    recommended_next_step: RecommendedNextStep
    position_recommendation: PositionRecommendation


class Premise(ContractModel):
    id: str
    text: str
    kind: str


class TargetPosition(ContractModel):
    topic_id: TopicId
    version: int
    summary: str


class ExpectedOutput(ContractModel):
    schema_id: str = Field(alias="schema")


class RuntimeLimitsCapsule(ContractModel):
    max_output_tokens: int | None = None
    max_tool_calls: int | None = None
    deadline_at: str | None = None


class TaskCapsule(ContractModel):
    schema_version: Literal["1"]
    task_id: TaskId
    attempt_id: AttemptId
    input_revision: Revision
    objective: str
    reasoning_role: str
    mode: str
    premises: tuple[Premise, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    evidence_refs: tuple[EvidenceId, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    target_position: TargetPosition | None = None
    constraints: tuple[str, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    expected_output: ExpectedOutput
    runtime_limits: RuntimeLimitsCapsule = Field(default_factory=RuntimeLimitsCapsule)
    capability_profile: str


class HekateProposalModel(ContractModel):
    schema_version: Literal["1"]


class AnswerProposal(HekateProposalModel):
    action: Literal["answer"]
    answer: str


class SpawnProposal(HekateProposalModel):
    action: Literal["spawn"]
    role: Literal["critic"]
    purpose: str
    target_uncertainty: str
    expected_decision_impact: str
    task_id: TaskId


class ContinuationProposal(HekateProposalModel):
    action: Literal["continue"]
    unresolved_issue: str
    next_action: str
    expected_information_gain: str
    decision_impact: str


class SimpleProposal(HekateProposalModel):
    action: Literal["retrieve_evidence", "request_information", "wait", "stop", "abstain"]
    reason: str | None = None


class CommitProposal(HekateProposalModel):
    action: Literal["commit"]
    operation_id: OperationId
    task_id: TaskId
    topic_id: TopicId
    base_version: int
    input_revision: Revision
    proposed_position: PositionBody
    reason_for_change: str


HekateProposal: TypeAlias = Annotated[
    AnswerProposal | SpawnProposal | ContinuationProposal | SimpleProposal | CommitProposal,
    Field(discriminator="action"),
]
Capsule: TypeAlias = TaskCapsule | ConclusionCapsule


class PositionBody(ContractModel):
    """Wire body: applicability is content; authorization scope is supplied separately."""
    statement: str
    applicability: tuple[str, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    confidence: Confidence
    evidence_refs: tuple[EvidenceId, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    assumptions: tuple[str, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    dissent_refs: tuple[DomainId, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    uncertainty: str | None = None


class PositionCommitRequest(ContractModel):
    schema_version: Literal["1"] = "1"
    operation_id: OperationId
    task_id: TaskId
    topic_id: TopicId
    base_version: int
    input_revision: Revision
    proposed_position: PositionBody
    reason_for_change: str


class ToolRequest(ContractModel):
    tool_call_id: str
    name: str
    arguments: Mapping[str, object] = Field(default_factory=dict)


class ProviderObservation(ContractModel):
    state: ObservationState
    evidence: str | None = None
    observed_at: datetime


class RuntimeEvent(ContractModel):
    schema_version: Literal["1"] = "1"
    binding: RuntimeBinding
    stable_event_key: str
    event_kind: str
    provider_call_id: ProviderCallId | None = None
    payload: Mapping[str, object] = Field(default_factory=dict)


class UsageRecord(ContractModel):
    accounting_call_id: AccountingCallId
    binding: GuardBinding
    observation_identity: str
    source: str
    observed_at: datetime
    provider_call_id: ProviderCallId | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cache_tokens: int | None = None
    reasoning_tokens: int | None = None
    completeness: str
    pricing_version: str | None = None
    monetary_amount: Decimal | None = None


class BudgetReservation(ContractModel):
    id: ReservationId
    operation_id: OperationId
    purpose: str
    reserved_amount: Decimal
    settled_amount: Decimal | None = None
    status: str
    pricing_version: str


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    id: str
    scope_kind: str
    scope_ref: str
    period_id: str
    limit_amount: Decimal
    spent_amount: Decimal
    held_amount: Decimal

    @property
    def available(self) -> Decimal:
        return self.limit_amount - self.spent_amount - self.held_amount


@dataclass(frozen=True, slots=True)
class Account(AccountSnapshot):
    pass


@dataclass(frozen=True, slots=True)
class ReservationRequest:
    id: ReservationId
    operation_id: OperationId
    purpose: str
    amount: Decimal
    task_id: TaskId
    task_account_id: str
    system_account_id: str
    pricing_version: str
    system_period_id: str


@dataclass(frozen=True, slots=True)
class GuardBinding:
    task_id: TaskId
    attempt_id: AttemptId
    agent_registry_id: RegistryId
    provider_agent_id: ProviderAgentId
    principal_id: PrincipalId
    scope: ScopeId
    input_revision: Revision
    policy_version: str
    authz_epoch: int
    fence: int
    conversation_id: str | None = None


@dataclass(frozen=True, slots=True)
class RuntimeLimits:
    max_input_tokens: int
    max_output_tokens: int
    max_billable_calls: int
    deadline: datetime


@dataclass(frozen=True, slots=True)
class PriceTable:
    model: str
    version: str
    input_usd_per_million: Decimal
    output_usd_per_million: Decimal
    model_profile_verified: bool = False
    pricing_verified: bool = False
    tokenizer_verified: bool = False
    synthetic: bool = False


@dataclass(frozen=True, slots=True)
class AdmissionRequest:
    binding: GuardBinding
    reservation: ReservationRequest
    envelope: ExecutionEnvelope
    attempt_kind: str
    parent_attempt_id: AttemptId | None
    operation_kind: str
    payload: Mapping[str, object]
    lease_owner: str


@dataclass(frozen=True, slots=True)
class AuthorizationSnapshot:
    scope: ScopeId
    principal_id: PrincipalId
    policy_version: str
    authz_epoch: int


@dataclass(frozen=True, slots=True)
class OperationClaim:
    operation_id: OperationId
    owner_scope: ScopeId
    request_hash: str
    state: str
    receipt: AdmissionReceipt | None = None


@dataclass(frozen=True, slots=True)
class AdmissionReceipt:
    operation_id: OperationId
    attempt_id: AttemptId
    reservation_id: ReservationId
    state: str
    replayed: bool


@dataclass(frozen=True, slots=True)
class Lease:
    registry_id: RegistryId
    owner: str
    fence: int
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class BillableCallIntent:
    accounting_call_id: AccountingCallId
    permit_id: PermitId
    operation_id: OperationId
    call_kind: str
    slot_key: str
    binding: GuardBinding
    model: str
    allocation_amount: Decimal
    limits: RuntimeLimits
    price_table: PriceTable
    permit_expires_at: datetime
    lease_owner: str
    test_only: bool = False
    reservation_id: ReservationId | None = None


@dataclass(frozen=True, slots=True)
class CallPermit:
    permit_id: PermitId
    accounting_call_id: AccountingCallId
    operation_id: OperationId
    model: str
    max_input_tokens: int
    max_output_tokens: int
    expires_at: datetime
    consumed: bool
    test_only: bool


@dataclass(frozen=True, slots=True)
class NormalizedUsage:
    completeness: str
    input_tokens: int | None = None
    output_tokens: int | None = None
    cache_tokens: int | None = None
    reasoning_tokens: int | None = None
    total_tokens: int | None = None
    reported_cost_usd: Decimal | None = None


@dataclass(frozen=True, slots=True)
class UsageObservation:
    accounting_call_id: AccountingCallId
    source: str
    observation_identity: str
    usage: NormalizedUsage
    binding: GuardBinding
    observed_at: datetime
    provider_call_id: ProviderCallId | None = None


@dataclass(frozen=True, slots=True)
class CallObservation:
    accounting_call_id: AccountingCallId
    binding: GuardBinding
    state: str
    source: str
    observed_at: datetime
    lease_owner: str
    observer_fence: int
    provider_call_id: ProviderCallId | None = None


@dataclass(frozen=True, slots=True)
class UsageReceipt:
    accounting_call_id: AccountingCallId
    observation_id: str
    duplicate: bool
    conflict: bool
    completeness: str
    settlement_state: str
    accepted: bool = True
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class SettlementReceipt:
    accounting_call_id: AccountingCallId
    settled: bool
    pending_reason: str | None
    actual_cost: Decimal | None
    task_spent: Decimal
    task_held: Decimal
    system_spent: Decimal
    system_held: Decimal
    overrun: bool


@dataclass(frozen=True, slots=True)
class OutboxJob:
    id: str
    operation_id: OperationId
    kind: str
    generation: int
    payload: Mapping[str, object]
    status: str
    claim_owner: str | None = None
    claim_fence: int | None = None
    claim_expires_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class InboxReceipt:
    id: str
    duplicate: bool
    conflict: bool


@dataclass(frozen=True, slots=True)
class ExecutionObservation:
    operation_id: OperationId
    binding: GuardBinding
    lease_owner: str
    observer_fence: int
    state: str
    source: str
    observed_at: datetime
    outcome: str | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class DatabaseHealth:
    available: bool
    postgres_version: str | None
    migration_head: str | None


class PositionVersionRecord(ContractModel):
    scope: ScopeId
    topic_id: TopicId
    version: int
    base_version: int
    body: PositionBody
    operation_id: OperationId
    task_id: TaskId
    input_revision: Revision


class EvidenceInput(ContractModel):
    schema_version: Literal["1"] = "1"
    id: EvidenceId
    kind: str
    source_uri: str
    locator: str | None = None
    retrieved_at: datetime
    observed_at: datetime | None = None
    content_hash: str
    derived_from: tuple[EvidenceId, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    access_scope: str
    retention_class: str
    content_version: str | None = None
    availability: str | None = None
    access_epoch: int | None = None
    root_source_ids: tuple[EvidenceId, ...] = Field(default=(), max_length=MAX_CONTRACT_ITEMS)
    expiry_at: datetime | None = None


class EvidenceRecord(EvidenceInput):
    scope: ScopeId
    content_version: str
    availability: str
    access_epoch: int

    @classmethod
    def from_input(
        cls,
        source: EvidenceInput,
        scope_by_access_scope: Mapping[str, ScopeId],
        *,
        content_version: str,
        availability: str,
        access_epoch: int,
    ) -> EvidenceRecord:
        """Map the wire access-scope label to the store's trusted scope ID."""
        try:
            scope = scope_by_access_scope[source.access_scope]
        except KeyError as error:
            raise ValueError(f"unmapped access_scope: {source.access_scope}") from error
        values = source.model_dump()
        for field, value in (
            ("content_version", content_version),
            ("availability", availability),
            ("access_epoch", access_epoch),
        ):
            if values[field] is not None and values[field] != value:
                raise ValueError(f"conflicting evidence {field}")
            values[field] = value
        return cls(scope=scope, **values)


class StoredConclusion(ContractModel):
    id: DomainId
    attempt_id: AttemptId
    payload_hash: str
    capsule: ConclusionCapsule
    validation_status: str
    eligible: bool
    provider_provenance: Mapping[str, object] = Field(default_factory=dict)
    rejection_reason: str | None = None


class RecoveryCase(ContractModel):
    operation_id: OperationId
    observations: tuple[str, ...] = ()
    unknown_reason: str
    actions_tried: tuple[str, ...] = ()
    operator_decision: str | None = None


class PolicyDecision(ContractModel):
    allowed: bool
    reason: str | None = None


class ToolDecision(PolicyDecision):
    grant_id: str | None = None


class CostAssessment(ContractModel):
    amount: Decimal
    estimated: bool = False
    details: Mapping[str, object] = Field(default_factory=dict)


# Application views are refined as the API and persistence contracts are implemented.
_T = TypeVar("_T")
TaskReceipt: TypeAlias = Mapping[str, object]
RevisionReceipt: TypeAlias = Mapping[str, object]
CancellationReceipt: TypeAlias = Mapping[str, object]
TaskView: TypeAlias = Mapping[str, object]
PositionView: TypeAlias = Mapping[str, object]
PublicEvent: TypeAlias = Mapping[str, object]
ResponseRef: TypeAlias = str
TurnReceipt: TypeAlias = Mapping[str, object]
DecisionReceipt: TypeAlias = Mapping[str, object]
ScheduledDecision: TypeAlias = Mapping[str, object]
CreateObservation: TypeAlias = Mapping[str, object]
RetirementReceipt: TypeAlias = Mapping[str, object]
DeleteObservation: TypeAlias = Mapping[str, object]
IngestReceipt: TypeAlias = Mapping[str, object]
ValidationReport: TypeAlias = Mapping[str, object]
ResultDisposition: TypeAlias = Mapping[str, object]
RejectionReason: TypeAlias = str
AuditRef: TypeAlias = str
UsageCompleteness: TypeAlias = Mapping[str, object]
ReadLimits: TypeAlias = Mapping[str, object]
EvidenceView: TypeAlias = Mapping[str, object]
ReferenceValidation: TypeAlias = Mapping[str, object]
RetentionReport: TypeAlias = Mapping[str, object]
ReuseInputs: TypeAlias = Mapping[str, object]
ToolResult: TypeAlias = Mapping[str, object]
MemoryRequest: TypeAlias = Mapping[str, object]
MutationIntent: TypeAlias = Mapping[str, object]
MutationAuthorization: TypeAlias = Mapping[str, object]
RecoveryReport: TypeAlias = Mapping[str, object]
RecoveryDecision: TypeAlias = Mapping[str, object]
OrphanReport: TypeAlias = Mapping[str, object]
OperatorContext: TypeAlias = Mapping[str, object]
Resolution: TypeAlias = Mapping[str, object]
RecoveryReceipt: TypeAlias = Mapping[str, object]
ProjectionJob: TypeAlias = Mapping[str, object]
ProjectionReceipt: TypeAlias = Mapping[str, object]
ProjectionStatus: TypeAlias = Mapping[str, object]
Page: TypeAlias = Mapping[str, _T]
VersionCursor: TypeAlias = Mapping[str, object]
ArtifactRef: TypeAlias = str
DeletionReceipt: TypeAlias = Mapping[str, object]
AuditEvent: TypeAlias = Mapping[str, object]
RuntimeCapabilities: TypeAlias = Mapping[str, object]
CapabilityReport: TypeAlias = Mapping[str, object]
AgentSpec: TypeAlias = Mapping[str, object]
ProviderAgent: TypeAlias = Mapping[str, object]
DispatchObservation: TypeAlias = Mapping[str, object]
CancelObservation: TypeAlias = Mapping[str, object]
TurnInput: TypeAlias = Mapping[str, object]
ProviderCursor: TypeAlias = str
MemoryProjection: TypeAlias = Mapping[str, object]
Reservation: TypeAlias = Mapping[str, object]
Job: TypeAlias = Mapping[str, object]
HealthReport: TypeAlias = Mapping[str, object]
Principal: TypeAlias = Mapping[str, object]
SafeEvent: TypeAlias = Mapping[str, object]
EvalSpec: TypeAlias = Mapping[str, object]
EvaluationArtifact: TypeAlias = Mapping[str, object]
TrialResult: TypeAlias = Mapping[str, object]
GradingBatch: TypeAlias = Mapping[str, object]
QualityScore: TypeAlias = Mapping[str, object]
Comparison: TypeAlias = Mapping[str, object]
GateReport: TypeAlias = Mapping[str, object]
FailureClass: TypeAlias = str
ModelPolicy: TypeAlias = Mapping[str, object]
ContextManifest: TypeAlias = Mapping[str, object]
ScopeSnapshot: TypeAlias = Mapping[str, object]
CapabilityGrant: TypeAlias = Mapping[str, object]
ProposedAction: TypeAlias = Mapping[str, object]
PolicySnapshot: TypeAlias = Mapping[str, object]
SchemaFailure: TypeAlias = Mapping[str, object]
ApprovedAction: TypeAlias = Mapping[str, object]
CommitReceipt: TypeAlias = Mapping[str, object]
Command: TypeAlias = Mapping[str, object]
BridgeFrame: TypeAlias = Mapping[str, object]
BridgeReply: TypeAlias = Mapping[str, object]
TemplateResponse: TypeAlias = Mapping[str, object]
JsonSchema: TypeAlias = Mapping[str, object]
CapabilityReport: TypeAlias = Mapping[str, object]
DeploymentProfile: TypeAlias = Mapping[str, object]
