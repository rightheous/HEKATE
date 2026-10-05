"""Durable Letta Position projection operations and observed runtime memory."""

from alembic import context, op
from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKeyConstraint, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB

revision = "0011_phase6a_memory_projection"
down_revision = "0010_p5b_delib_maint"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("ck_memory_projection_state", "memory_projections", type_="check")
    op.create_check_constraint(
        "ck_memory_projection_state",
        "memory_projections",
        "state IN ('PENDING_UNSUPPORTED','PENDING','CLAIMED','UNKNOWN','APPLIED','DRIFT','CONFLICT')",
    )
    for name, column in (
        ("observed_memory_version", Column("observed_memory_version", Integer)),
        ("observed_payload_digest", Column("observed_payload_digest", String(64))),
        ("projection_format_version", Column("projection_format_version", Integer, nullable=False, server_default="1")),
        ("payload_digest", Column("payload_digest", String(64))),
        ("operation_id", Column("operation_id", Text)),
        ("request_hash", Column("request_hash", String(64))),
        ("next_retry_at", Column("next_retry_at", DateTime(timezone=True))),
        ("claim_owner", Column("claim_owner", Text)),
        ("claim_expires_at", Column("claim_expires_at", DateTime(timezone=True))),
        ("claim_fence", Column("claim_fence", Integer, nullable=False, server_default="0")),
        ("attempt_count", Column("attempt_count", Integer, nullable=False, server_default="0")),
        ("last_attempt_at", Column("last_attempt_at", DateTime(timezone=True))),
    ):
        op.add_column("memory_projections", column)
    op.create_check_constraint(
        "ck_memory_projection_claim_format",
        "memory_projections",
        "claim_fence >= 0 AND attempt_count >= 0 AND projection_format_version = 1",
    )
    op.create_index(
        "ix_memory_projection_due",
        "memory_projections",
        ["state", "next_retry_at", "updated_at"],
    )
    op.create_table(
        "memory_projection_operations",
        Column("id", Text, primary_key=True),
        Column("scope", Text, nullable=False),
        Column("topic_id", Text, nullable=False),
        Column("target_registry_id", Text, nullable=False),
        Column("source_version", Integer, nullable=False),
        Column("base_applied_version", Integer, nullable=False),
        Column("format_version", Integer, nullable=False),
        Column("payload_digest", String(64), nullable=False),
        Column("request_hash", String(64), nullable=False),
        Column("original_operation_id", Text, nullable=False),
        Column("state", Text, nullable=False),
        Column("observation", JSONB),
        Column("failure_reason", Text),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        Column("completed_at", DateTime(timezone=True)),
        ForeignKeyConstraint(["scope"], ["authorization_scopes.id"], ondelete="RESTRICT"),
        ForeignKeyConstraint(["target_registry_id"], ["agent_registry.id"], ondelete="RESTRICT"),
        ForeignKeyConstraint(["original_operation_id"], ["operations.id"], ondelete="RESTRICT"),
        ForeignKeyConstraint(
            ["scope", "topic_id"], ["position_topics.scope", "position_topics.topic_id"],
            ondelete="RESTRICT", name="fk_projection_operation_topic",
        ),
        UniqueConstraint(
            "scope", "topic_id", "target_registry_id", "source_version", "request_hash",
            name="uq_projection_operation_request",
        ),
        CheckConstraint("source_version >= 1 AND base_applied_version >= 0 AND format_version = 1", name="ck_projection_operation_version"),
        CheckConstraint(
            "state IN ('PENDING','STARTED','UNKNOWN','COMPLETED','SUPERSEDED','DRIFT','CONFLICT')",
            name="ck_projection_operation_state",
        ),
    )
    op.create_index("ix_projection_operation_pending", "memory_projection_operations", ["state", "updated_at"])


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError("Phase 6A memory projection downgrade requires an online safety check")
    connection = op.get_bind()
    operations_exist = connection.execute(text("SELECT EXISTS(SELECT 1 FROM memory_projection_operations)")).scalar_one()
    projection_progress_exists = connection.execute(text("""
        SELECT EXISTS(
            SELECT 1 FROM memory_projections
            WHERE applied_version > 0
               OR state NOT IN ('PENDING_UNSUPPORTED','APPLIED')
               OR observed_memory_version IS NOT NULL
               OR payload_digest IS NOT NULL
               OR operation_id IS NOT NULL
        )
    """)).scalar_one()
    if operations_exist or projection_progress_exists:
        raise RuntimeError("refusing to discard persisted Position-to-Letta memory projection progress")
    op.drop_index("ix_projection_operation_pending", table_name="memory_projection_operations")
    op.drop_table("memory_projection_operations")
    op.drop_index("ix_memory_projection_due", table_name="memory_projections")
    op.drop_constraint("ck_memory_projection_claim_format", "memory_projections", type_="check")
    op.drop_constraint("ck_memory_projection_state", "memory_projections", type_="check")
    for name in (
        "last_attempt_at", "attempt_count", "claim_fence", "claim_expires_at", "claim_owner", "next_retry_at",
        "request_hash", "operation_id", "payload_digest", "projection_format_version",
        "observed_payload_digest", "observed_memory_version",
    ):
        op.drop_column("memory_projections", name)
    op.create_check_constraint(
        "ck_memory_projection_state", "memory_projections",
        "state IN ('PENDING_UNSUPPORTED','APPLIED')",
    )
