"""Fence and reconcile external Position memory writes.

Revision ID: 0012_projection_write_guard
Revises: 0011_phase6a_memory_projection
"""

from alembic import context, op
from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKeyConstraint, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB

revision = "0012_projection_write_guard"
down_revision = "0011_phase6a_memory_projection"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("ck_memory_projection_claim_format", "memory_projections", type_="check")
    op.create_check_constraint(
        "ck_memory_projection_claim_format", "memory_projections",
        "claim_fence >= 0 AND attempt_count >= 0 AND projection_format_version IN (1,2)",
    )
    op.drop_constraint("ck_projection_operation_version", "memory_projection_operations", type_="check")
    op.create_check_constraint(
        "ck_projection_operation_version", "memory_projection_operations",
        "source_version >= 1 AND base_applied_version >= 0 AND format_version IN (1,2)",
    )
    op.create_table(
        "memory_projection_write_guards",
        Column("id", Text, primary_key=True),
        Column("operation_id", Text, nullable=False),
        Column("scope", Text, nullable=False),
        Column("registry_id", Text, nullable=False),
        Column("topic_id", Text, nullable=False),
        Column("request_hash", String(64), nullable=False),
        Column("principal_id", Text, nullable=False),
        Column("policy_version", Text, nullable=False),
        Column("authz_epoch", Integer, nullable=False),
        Column("lease_owner", Text, nullable=False),
        Column("lease_fence", Integer, nullable=False),
        Column("claim_owner", Text, nullable=False),
        Column("claim_fence", Integer, nullable=False),
        Column("source_version", Integer, nullable=False),
        Column("payload_digest", String(64), nullable=False),
        Column("state", Text, nullable=False),
        Column("observation", JSONB),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        Column("resolved_at", DateTime(timezone=True)),
        ForeignKeyConstraint(["operation_id"], ["memory_projection_operations.id"], ondelete="RESTRICT"),
        ForeignKeyConstraint(["scope"], ["authorization_scopes.id"], ondelete="RESTRICT"),
        ForeignKeyConstraint(["registry_id"], ["agent_registry.id"], ondelete="RESTRICT"),
        CheckConstraint(
            "authz_epoch >= 0 AND lease_fence >= 1 AND claim_fence >= 1 AND source_version >= 1",
            name="ck_projection_write_guard_identity",
        ),
        CheckConstraint("state IN ('AUTHORIZED','EFFECT_CONFIRMED','NO_EFFECT')", name="ck_projection_write_guard_state"),
    )
    op.create_index(
        "ix_projection_write_guard_active", "memory_projection_write_guards", ["registry_id", "state"],
    )


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError("projection write guard downgrade requires an online safety check")
    connection = op.get_bind()
    if connection.execute(text("SELECT EXISTS(SELECT 1 FROM memory_projection_write_guards)")).scalar_one():
        raise RuntimeError("refusing to discard projection authorization and effect observations")
    op.drop_index("ix_projection_write_guard_active", table_name="memory_projection_write_guards")
    op.drop_table("memory_projection_write_guards")
    if connection.execute(text("SELECT EXISTS(SELECT 1 FROM memory_projection_operations WHERE format_version <> 1)")).scalar_one():
        raise RuntimeError("refusing to downgrade memory projection format while v2 operations exist")
    if connection.execute(text("SELECT EXISTS(SELECT 1 FROM memory_projections WHERE projection_format_version <> 1)")).scalar_one():
        raise RuntimeError("refusing to downgrade memory projection format while v2 topic state exists")
    op.drop_constraint("ck_projection_operation_version", "memory_projection_operations", type_="check")
    op.create_check_constraint(
        "ck_projection_operation_version", "memory_projection_operations",
        "source_version >= 1 AND base_applied_version >= 0 AND format_version = 1",
    )
    op.drop_constraint("ck_memory_projection_claim_format", "memory_projections", type_="check")
    op.create_check_constraint(
        "ck_memory_projection_claim_format", "memory_projections",
        "claim_fence >= 0 AND attempt_count >= 0 AND projection_format_version = 1",
    )
