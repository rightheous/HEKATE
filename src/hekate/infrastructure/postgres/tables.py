from __future__ import annotations

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

_metadata = MetaData()
money = Numeric(asdecimal=True)
instant = DateTime(timezone=True)
json_default = text("'{}'::jsonb")

authorization_scopes = Table(
    "authorization_scopes", _metadata,
    Column("id", Text, primary_key=True),
    Column("principal_id", Text, nullable=False),
    Column("policy_version", Text, nullable=False),
    Column("authz_epoch", Integer, nullable=False, server_default="0"),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    CheckConstraint("authz_epoch >= 0", name="ck_authorization_scopes_epoch"),
)

tasks = Table(
    "tasks", _metadata,
    Column("id", Text, primary_key=True),
    Column("owner_scope", Text, ForeignKey("authorization_scopes.id"), nullable=False),
    Column("question", Text, nullable=False),
    Column("input_revision", Integer, nullable=False),
    Column("constraints_hash", String(64), nullable=False),
    Column("status", String(16), nullable=False),
    Column("topic_id", Text),
    Column("base_position_version", Integer, nullable=False, server_default="0"),
    Column("deadline", instant, nullable=False),
    Column("outcome", Text),
    Column("stop_reason", Text),
    Column("cancel_requested_at", instant),
    Column("critic_agents", Integer, nullable=False, server_default="0"),
    Column("review_rounds", Integer, nullable=False, server_default="0"),
    Column("schema_repairs", Integer, nullable=False, server_default="0"),
    Column("transient_retries", Integer, nullable=False, server_default="0"),
    Column("tool_calls", Integer, nullable=False, server_default="0"),
    Column("provider_calls", Integer, nullable=False, server_default="0"),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    CheckConstraint("input_revision >= 1", name="ck_tasks_revision"),
    CheckConstraint("base_position_version >= 0", name="ck_tasks_position_version"),
    CheckConstraint("status IN ('QUEUED','RUNNING','WAITING','STOPPING','COMPLETED','FAILED','CANCELLED')", name="ck_tasks_status"),
    CheckConstraint("critic_agents >= 0 AND review_rounds >= 0 AND schema_repairs >= 0 AND transient_retries >= 0 AND tool_calls >= 0 AND provider_calls >= 0", name="ck_tasks_counters"),
)

task_inputs = Table(
    "task_inputs", _metadata,
    Column("task_id", Text, ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("question", Text, nullable=False),
    Column("constraints", JSONB, nullable=False, server_default=json_default),
    Column("constraints_hash", String(64), nullable=False),
    Column("accepted_at", instant, nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("task_id", "revision", name="pk_task_inputs"),
    CheckConstraint("revision >= 1", name="ck_task_inputs_revision"),
)

operations = Table(
    "operations", _metadata,
    Column("id", Text, primary_key=True),
    Column("owner_scope", Text, ForeignKey("authorization_scopes.id"), nullable=False),
    Column("task_id", Text, ForeignKey("tasks.id")),
    Column("kind", Text, nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("state", String(20), nullable=False),
    Column("dispatch_state", String(24), nullable=False, server_default="NOT_STARTED"),
    Column("execution_state", String(16), nullable=False, server_default="PENDING"),
    Column("binding", JSONB, nullable=False),
    Column("envelope", JSONB, nullable=False),
    Column("receipt", JSONB),
    Column("observation", JSONB, nullable=False, server_default=json_default),
    Column("last_error", Text),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    Column("updated_at", instant, nullable=False, server_default=text("now()")),
    CheckConstraint("state IN ('CLAIMED','ADMITTED','COMPLETED','FAILED','UNKNOWN')", name="ck_operations_state"),
    CheckConstraint("dispatch_state IN ('NOT_STARTED','INTENT_RECORDED','SEND_INTENT','DISPATCHED','UNKNOWN','QUIESCENT')", name="ck_operations_dispatch_state"),
    CheckConstraint("execution_state IN ('PENDING','RUNNING','UNKNOWN','QUIESCENT')", name="ck_operations_execution_state"),
)

agent_registry = Table(
    "agent_registry", _metadata,
    Column("id", Text, primary_key=True),
    Column("owner_scope", Text, ForeignKey("authorization_scopes.id"), nullable=False),
    Column("task_id", Text, ForeignKey("tasks.id")),
    Column("role", String(12), nullable=False),
    Column("persistence", String(12), nullable=False, server_default="ephemeral"),
    Column("creation_operation_id", Text, ForeignKey("operations.id"), nullable=False),
    Column("provider_agent_id", Text, unique=True),
    Column("intended_state", String(20), nullable=False),
    Column("observed_state", String(16), nullable=False, server_default="UNKNOWN"),
    Column("observed_at", instant),
    Column("active_attempt_id", Text),
    Column("policy_version", Text, nullable=False),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    CheckConstraint("role IN ('hekate','critic')", name="ck_agent_registry_role"),
    CheckConstraint("persistence IN ('persistent','ephemeral')", name="ck_agent_registry_persistence"),
    CheckConstraint("observed_state IN ('PRESENT','ABSENT','RUNNING','STOPPED','UNKNOWN')", name="ck_agent_registry_observation"),
)
Index(
    "uq_agent_registry_persistent_hekate",
    agent_registry.c.owner_scope,
    unique=True,
    postgresql_where=(agent_registry.c.role == "hekate") & (agent_registry.c.persistence == "persistent"),
)
Index(
    "uq_agent_registry_critic_per_task",
    agent_registry.c.task_id,
    unique=True,
    postgresql_where=(agent_registry.c.role == "critic") & (agent_registry.c.task_id.is_not(None)),
)

agent_leases = Table(
    "agent_leases", _metadata,
    Column("registry_id", Text, ForeignKey("agent_registry.id", ondelete="CASCADE"), primary_key=True),
    Column("owner_worker", Text, nullable=False),
    Column("fence", Integer, nullable=False),
    Column("expires_at", instant, nullable=False),
    CheckConstraint("fence >= 1", name="ck_agent_leases_fence"),
)

attempts = Table(
    "attempts", _metadata,
    Column("id", Text, primary_key=True),
    Column("task_id", Text, ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False),
    Column("kind", Text, nullable=False),
    Column("parent_attempt_id", Text, ForeignKey("attempts.id", ondelete="RESTRICT")),
    Column("review_round", Integer, nullable=False, server_default="0"),
    Column("input_revision", Integer, nullable=False),
    Column("agent_registry_id", Text, ForeignKey("agent_registry.id", ondelete="RESTRICT"), nullable=False),
    Column("status", String(16), nullable=False),
    Column("operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False, unique=True),
    Column("reservation_id", Text),
    Column("deadline", instant, nullable=False),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    CheckConstraint("input_revision >= 1 AND review_round >= 0", name="ck_attempts_revision_round"),
    CheckConstraint("status IN ('PENDING','DISPATCHED','RUNNING','SUCCEEDED','FAILED','TIMED_OUT','CANCELLED')", name="ck_attempts_status"),
)
agent_registry.append_constraint(ForeignKeyConstraint(
    ["active_attempt_id"], ["attempts.id"], ondelete="SET NULL",
    name="fk_agent_registry_active_attempt", use_alter=True,
))
Index("ix_attempts_task_status", attempts.c.task_id, attempts.c.status)

agent_execution_holds = Table(
    "agent_execution_holds", _metadata,
    Column("id", Text, primary_key=True),
    Column("registry_id", Text, ForeignKey("agent_registry.id", ondelete="RESTRICT"), nullable=False),
    Column("operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False, unique=True),
    Column("state", String(16), nullable=False),
    Column("reason", Text),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    Column("quiescent_at", instant),
    CheckConstraint("state IN ('PENDING','RUNNING','UNKNOWN','QUIESCENT')", name="ck_agent_execution_holds_state"),
)
Index(
    "uq_agent_execution_hold_active_registry",
    agent_execution_holds.c.registry_id,
    unique=True,
    postgresql_where=agent_execution_holds.c.quiescent_at.is_(None),
)

outbox = Table(
    "outbox", _metadata,
    Column("id", Text, primary_key=True),
    Column("operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False),
    Column("kind", Text, nullable=False),
    Column("generation", Integer, nullable=False, server_default="0"),
    Column("payload", JSONB, nullable=False, server_default=json_default),
    Column("status", String(12), nullable=False, server_default="PENDING"),
    Column("available_at", instant, nullable=False, server_default=text("now()")),
    Column("claim_owner", Text),
    Column("claim_fence", Integer),
    Column("claim_expires_at", instant),
    Column("send_intent_at", instant),
    Column("send_intent_owner", Text),
    Column("send_intent_fence", Integer),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    Column("acked_at", instant),
    UniqueConstraint("operation_id", "kind", "generation", name="uq_outbox_generation"),
    CheckConstraint("generation >= 0", name="ck_outbox_generation"),
    CheckConstraint("status IN ('PENDING','CLAIMED','ACKED','FAILED')", name="ck_outbox_status"),
)
Index("ix_outbox_due", outbox.c.status, outbox.c.available_at)

inbox = Table(
    "inbox", _metadata,
    Column("id", Text, primary_key=True),
    Column("provider_scope", Text, nullable=False),
    Column("stable_event_key", Text, nullable=False),
    Column("payload_hash", String(64), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("received_at", instant, nullable=False, server_default=text("now()")),
    Column("processed_at", instant),
    UniqueConstraint("provider_scope", "stable_event_key", "payload_hash", name="uq_inbox_same_observation"),
)
Index("ix_inbox_event_identity", inbox.c.provider_scope, inbox.c.stable_event_key)

audit_events = Table(
    "audit_events", _metadata,
    Column("id", Text, primary_key=True),
    Column("owner_scope", Text, ForeignKey("authorization_scopes.id")),
    Column("task_id", Text, ForeignKey("tasks.id")),
    Column("attempt_id", Text, ForeignKey("attempts.id")),
    Column("operation_id", Text, ForeignKey("operations.id")),
    Column("registry_id", Text, ForeignKey("agent_registry.id")),
    Column("event_kind", Text, nullable=False),
    Column("safe_payload", JSONB, nullable=False, server_default=json_default),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
)

recovery_cases = Table(
    "recovery_cases", _metadata,
    Column("operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), primary_key=True),
    Column("reason", Text, nullable=False),
    Column("observations", JSONB, nullable=False, server_default=json_default),
    Column("resolution", Text),
    Column("updated_at", instant, nullable=False, server_default=text("now()")),
)

budget_accounts = Table(
    "budget_accounts", _metadata,
    Column("id", Text, primary_key=True),
    Column("scope_kind", String(8), nullable=False),
    Column("scope_ref", Text, nullable=False),
    Column("period_id", Text, nullable=False),
    Column("limit_amount", money, nullable=False),
    Column("spent_amount", money, nullable=False, server_default="0"),
    Column("held_amount", money, nullable=False, server_default="0"),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    UniqueConstraint("scope_kind", "scope_ref", "period_id", name="uq_budget_account_period"),
    CheckConstraint("scope_kind IN ('TASK','SYSTEM')", name="ck_budget_account_scope_kind"),
    CheckConstraint("limit_amount >= 0 AND spent_amount >= 0 AND held_amount >= 0", name="ck_budget_account_nonnegative"),
    CheckConstraint("limit_amount < 'Infinity'::numeric AND limit_amount > '-Infinity'::numeric AND spent_amount < 'Infinity'::numeric AND spent_amount > '-Infinity'::numeric AND held_amount < 'Infinity'::numeric AND held_amount > '-Infinity'::numeric", name="ck_budget_account_finite"),
)

budget_reservations = Table(
    "budget_reservations", _metadata,
    Column("id", Text, primary_key=True),
    Column("operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False),
    Column("purpose", Text, nullable=False),
    Column("amount", money, nullable=False),
    Column("status", String(24), nullable=False),
    Column("pricing_version", Text, nullable=False),
    Column("system_period_id", Text, nullable=False),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    Column("settled_at", instant),
    UniqueConstraint("operation_id", "purpose", name="uq_budget_reservation_purpose"),
    CheckConstraint("purpose IN ('operation_envelope','final_response')", name="ck_budget_reservation_purpose"),
    CheckConstraint("status IN ('RESERVED','PENDING_SETTLEMENT','SETTLED','RELEASED')", name="ck_budget_reservation_status"),
    CheckConstraint("amount >= 0 AND amount < 'Infinity'::numeric AND amount > '-Infinity'::numeric", name="ck_budget_reservation_finite"),
)
attempts.append_constraint(ForeignKeyConstraint(
    ["reservation_id"], ["budget_reservations.id"], ondelete="RESTRICT",
    name="fk_attempts_reservation", use_alter=True,
))

reservation_accounts = Table(
    "reservation_accounts", _metadata,
    Column("reservation_id", Text, ForeignKey("budget_reservations.id", ondelete="RESTRICT"), nullable=False),
    Column("account_id", Text, ForeignKey("budget_accounts.id", ondelete="RESTRICT"), nullable=False),
    Column("held_amount", money, nullable=False),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("reservation_id", "account_id", name="pk_reservation_accounts"),
    CheckConstraint("held_amount >= 0 AND held_amount < 'Infinity'::numeric AND held_amount > '-Infinity'::numeric", name="ck_reservation_accounts_finite"),
)

provider_calls = Table(
    "provider_calls", _metadata,
    Column("accounting_call_id", Text, primary_key=True),
    Column("permit_id", Text, nullable=False, unique=True),
    Column("operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False),
    Column("attempt_id", Text, ForeignKey("attempts.id", ondelete="RESTRICT"), nullable=False),
    Column("reservation_id", Text, ForeignKey("budget_reservations.id", ondelete="RESTRICT"), nullable=False),
    Column("registry_id", Text, ForeignKey("agent_registry.id", ondelete="RESTRICT"), nullable=False),
    Column("call_kind", Text, nullable=False),
    Column("slot_key", Text, nullable=False),
    Column("intent_hash", String(64), nullable=False),
    Column("model", Text, nullable=False),
    Column("status", String(16), nullable=False),
    Column("max_input_tokens", Integer, nullable=False),
    Column("max_output_tokens", Integer, nullable=False),
    Column("allocation_amount", money, nullable=False),
    Column("pricing_version", Text, nullable=False),
    Column("input_usd_per_million", money, nullable=False),
    Column("output_usd_per_million", money, nullable=False),
    Column("model_profile_verified", Boolean, nullable=False, server_default=text("false")),
    Column("pricing_verified", Boolean, nullable=False, server_default=text("false")),
    Column("tokenizer_verified", Boolean, nullable=False, server_default=text("false")),
    Column("test_only", Boolean, nullable=False, server_default=text("false")),
    Column("input_revision", Integer, nullable=False),
    Column("fence", Integer, nullable=False),
    Column("conversation_id", Text),
    Column("lease_owner", Text, nullable=False),
    Column("expires_at", instant, nullable=False),
    Column("consumed_at", instant),
    Column("dispatched_at", instant),
    Column("provider_call_id", Text),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    UniqueConstraint("operation_id", "slot_key", name="uq_provider_call_slot"),
    CheckConstraint("status IN ('ALLOCATED','CONSUMED','DISPATCHED','RUNNING','UNKNOWN','QUIESCENT','EXPIRED','REVOKED')", name="ck_provider_calls_status"),
    CheckConstraint("max_input_tokens >= 0 AND max_output_tokens >= 0 AND input_revision >= 1 AND fence >= 1", name="ck_provider_calls_limits"),
    CheckConstraint("allocation_amount >= 0 AND input_usd_per_million >= 0 AND output_usd_per_million >= 0", name="ck_provider_calls_amounts"),
)
Index("ix_provider_calls_operation", provider_calls.c.operation_id)

call_permits = Table(
    "call_permits", _metadata,
    Column("permit_id", Text, primary_key=True),
    Column("accounting_call_id", Text, ForeignKey("provider_calls.accounting_call_id", ondelete="RESTRICT"), nullable=False, unique=True),
    Column("state", String(12), nullable=False),
    Column("issued_at", instant, nullable=False, server_default=text("now()")),
    Column("expires_at", instant, nullable=False),
    Column("consumed_at", instant),
    Column("revoked_at", instant),
    CheckConstraint("state IN ('ISSUED','CONSUMED','REVOKED','EXPIRED')", name="ck_call_permits_state"),
)
provider_calls.append_constraint(ForeignKeyConstraint(
    ["permit_id"], ["call_permits.permit_id"], name="fk_provider_calls_permit_id",
    use_alter=True, deferrable=True, initially="DEFERRED",
))

call_allocations = Table(
    "call_allocations", _metadata,
    Column("accounting_call_id", Text, ForeignKey("provider_calls.accounting_call_id", ondelete="RESTRICT"), nullable=False),
    Column("reservation_id", Text, nullable=False),
    Column("account_id", Text, nullable=False),
    Column("amount", money, nullable=False),
    PrimaryKeyConstraint("accounting_call_id", "account_id", name="pk_call_allocations"),
    ForeignKeyConstraint(["reservation_id", "account_id"], ["reservation_accounts.reservation_id", "reservation_accounts.account_id"], ondelete="RESTRICT", name="fk_call_allocations_reservation_account"),
    CheckConstraint("amount >= 0 AND amount < 'Infinity'::numeric AND amount > '-Infinity'::numeric", name="ck_call_allocations_finite"),
)

usage_observations = Table(
    "usage_observations", _metadata,
    Column("id", Text, primary_key=True),
    Column("accounting_call_id", Text, ForeignKey("provider_calls.accounting_call_id", ondelete="RESTRICT"), nullable=False),
    Column("provider_call_id", Text),
    Column("source", Text, nullable=False),
    Column("observation_identity", Text, nullable=False),
    Column("payload_hash", String(64), nullable=False),
    Column("completeness", String(12), nullable=False),
    Column("input_tokens", Integer),
    Column("output_tokens", Integer),
    Column("cache_tokens", Integer),
    Column("reasoning_tokens", Integer),
    Column("total_tokens", Integer),
    Column("reported_cost_usd", money),
    Column("observed_at", instant, nullable=False),
    Column("received_at", instant, nullable=False, server_default=text("now()")),
    UniqueConstraint("accounting_call_id", "source", "payload_hash", name="uq_usage_observation_payload"),
    CheckConstraint("completeness IN ('UNKNOWN','PARTIAL','COMPLETE')", name="ck_usage_observation_completeness"),
    CheckConstraint("(input_tokens IS NULL OR input_tokens >= 0) AND (output_tokens IS NULL OR output_tokens >= 0) AND (cache_tokens IS NULL OR cache_tokens >= 0) AND (reasoning_tokens IS NULL OR reasoning_tokens >= 0) AND (total_tokens IS NULL OR total_tokens >= 0)", name="ck_usage_observation_tokens"),
)
Index("ix_usage_observations_identity", usage_observations.c.accounting_call_id, usage_observations.c.observation_identity)

usage_projections = Table(
    "usage_projections", _metadata,
    Column("accounting_call_id", Text, ForeignKey("provider_calls.accounting_call_id", ondelete="RESTRICT"), primary_key=True),
    Column("completeness", String(12), nullable=False),
    Column("has_conflict", Boolean, nullable=False, server_default=text("false")),
    Column("settlement_state", String(12), nullable=False, server_default="PENDING"),
    Column("input_tokens", Integer),
    Column("output_tokens", Integer),
    Column("cache_tokens", Integer),
    Column("reasoning_tokens", Integer),
    Column("total_tokens", Integer),
    Column("reported_cost_usd", money),
    Column("reported_cost_source", Text),
    Column("evaluated_cost_usd", money),
    Column("overrun", Boolean, nullable=False, server_default=text("false")),
    Column("updated_at", instant, nullable=False, server_default=text("now()")),
    CheckConstraint("completeness IN ('UNKNOWN','PARTIAL','COMPLETE')", name="ck_usage_projection_completeness"),
    CheckConstraint("settlement_state IN ('PENDING','SETTLED','CONFLICT')", name="ck_usage_projection_settlement"),
)

budget_ledger = Table(
    "budget_ledger", _metadata,
    Column("id", Text, primary_key=True),
    Column("account_id", Text, ForeignKey("budget_accounts.id", ondelete="RESTRICT"), nullable=False),
    Column("reservation_id", Text, ForeignKey("budget_reservations.id", ondelete="RESTRICT")),
    Column("accounting_call_id", Text, ForeignKey("provider_calls.accounting_call_id", ondelete="RESTRICT")),
    Column("effect_key", Text, nullable=False),
    Column("effect_type", String(16), nullable=False),
    Column("amount", money, nullable=False),
    Column("held_delta", money, nullable=False, server_default="0"),
    Column("spent_delta", money, nullable=False, server_default="0"),
    Column("created_at", instant, nullable=False, server_default=text("now()")),
    UniqueConstraint("account_id", "effect_key", name="uq_budget_ledger_effect"),
    CheckConstraint("effect_type IN ('HOLD','SETTLE','RELEASE','ADJUSTMENT')", name="ck_budget_ledger_effect_type"),
    CheckConstraint("amount > '-Infinity'::numeric AND amount < 'Infinity'::numeric AND held_delta > '-Infinity'::numeric AND held_delta < 'Infinity'::numeric AND spent_delta > '-Infinity'::numeric AND spent_delta < 'Infinity'::numeric", name="ck_budget_ledger_finite"),
)


def metadata() -> MetaData:
    return _metadata
