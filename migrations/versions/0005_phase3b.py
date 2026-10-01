"""Persist single-HEKATE task submissions and turn results."""

from alembic import op
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

revision = "0005_phase3b"
down_revision = "0004_terminal_inbox"
branch_labels = None
depends_on = None
def upgrade() -> None:
    op.create_table(
        "task_preparations",
        Column("task_id", Text, ForeignKey("tasks.id", ondelete="RESTRICT"), primary_key=True),
        Column("input_revision", Integer, primary_key=True),
        Column("operation_id", Text, nullable=False, unique=True),
        Column("attempt_id", Text, nullable=False, unique=True),
        Column("reservation_id", Text, nullable=False, unique=True),
        Column("claim_owner", Text),
        Column("claim_expires_at", DateTime(timezone=True)),
        Column("conversation_id", Text),
        Column("fence", Integer),
        Column("state", String(16), nullable=False, server_default="PREPARING"),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        CheckConstraint("input_revision >= 1 AND (fence IS NULL OR fence >= 1)", name="ck_task_preparations_identity"),
        CheckConstraint("state IN ('PREPARING','ADMITTED')", name="ck_task_preparations_state"),
    )
    op.create_index("ix_task_preparations_claim", "task_preparations", ["state", "claim_expires_at"])
    op.create_table(
        "task_submissions",
        Column("owner_scope", Text, ForeignKey("authorization_scopes.id"), primary_key=True),
        Column("request_key", String(128), primary_key=True),
        Column("request_hash", String(64), nullable=False),
        Column("task_id", Text, ForeignKey("tasks.id", ondelete="RESTRICT"), unique=True),
        Column("receipt", JSONB),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    )
    op.create_table(
        "conclusions",
        Column("id", Text, primary_key=True),
        Column("task_id", Text, ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False),
        Column("attempt_id", Text, ForeignKey("attempts.id", ondelete="RESTRICT"), nullable=False),
        Column("operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False),
        Column("registry_id", Text, ForeignKey("agent_registry.id", ondelete="RESTRICT"), nullable=False),
        Column("input_revision", Integer, nullable=False),
        Column("payload_hash", String(64), nullable=False),
        Column("capsule", JSONB, nullable=False),
        Column("validation_status", String(24), nullable=False),
        Column("eligible", Boolean, nullable=False),
        Column("provider_provenance", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
        Column("rejection_reason", Text),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        UniqueConstraint("attempt_id", "payload_hash", name="uq_conclusions_attempt_payload"),
        CheckConstraint("input_revision >= 1", name="ck_conclusions_revision"),
    )
    op.create_table(
        "turn_results",
        Column("inbox_id", Text, ForeignKey("inbox.id", ondelete="RESTRICT"), primary_key=True),
        Column("task_id", Text, ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False),
        Column("attempt_id", Text, ForeignKey("attempts.id", ondelete="RESTRICT"), nullable=False),
        Column("operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False),
        Column("registry_id", Text, ForeignKey("agent_registry.id", ondelete="RESTRICT"), nullable=False),
        Column("input_revision", Integer, nullable=False),
        Column("output_hash", String(64), nullable=False),
        Column("raw_output", Text),
        Column("structured_output", JSONB),
        Column("proposal", JSONB),
        Column("conclusion_id", Text, ForeignKey("conclusions.id", ondelete="RESTRICT")),
        Column("processing_state", String(24), nullable=False),
        Column("next_attempt_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        Column("rejection_reason", Text),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        CheckConstraint("input_revision >= 1", name="ck_turn_results_revision"),
        CheckConstraint("processing_state IN ('WAITING_EXECUTION','VALIDATED','REJECTED','ACCEPTED','LATE')", name="ck_turn_results_state"),
    )
    op.create_index("ix_turn_results_pending", "turn_results", ["processing_state", "next_attempt_at"])
    op.create_table(
        "task_responses",
        Column("task_id", Text, ForeignKey("tasks.id", ondelete="RESTRICT"), primary_key=True),
        Column("input_revision", Integer, nullable=False),
        Column("operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False),
        Column("attempt_id", Text, ForeignKey("attempts.id", ondelete="RESTRICT"), nullable=False),
        Column("registry_id", Text, ForeignKey("agent_registry.id", ondelete="RESTRICT"), nullable=False),
        Column("source_inbox_id", Text, ForeignKey("inbox.id", ondelete="RESTRICT"), nullable=False, unique=True),
        Column("proposal", JSONB, nullable=False),
        Column("response_text", Text, nullable=False),
        Column("outcome", String(32), nullable=False),
        Column("stop_reason", String(32), nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        CheckConstraint("input_revision >= 1", name="ck_task_responses_revision"),
    )


def downgrade() -> None:
    op.drop_table("task_responses")
    op.drop_index("ix_turn_results_pending", table_name="turn_results")
    op.drop_table("turn_results")
    op.drop_table("conclusions")
    op.drop_table("task_submissions")
    op.drop_index("ix_task_preparations_claim", table_name="task_preparations")
    op.drop_table("task_preparations")
