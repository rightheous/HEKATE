"""Durable first Critic review and synthesis workflow."""

from alembic import context, op
from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKey, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB

revision = "0007_phase5a_critic_workflows"
down_revision = "0006_phase4_knowledge"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "critic_workflows",
        Column("task_id", Text, ForeignKey("tasks.id", ondelete="RESTRICT"), primary_key=True),
        Column("owner_scope", Text, ForeignKey("authorization_scopes.id", ondelete="RESTRICT"), nullable=False),
        Column("input_revision", Integer, nullable=False),
        Column("stage", Text, nullable=False),
        Column("spawn_request_hash", String(64), nullable=False),
        Column("proposal", JSONB, nullable=False),
        Column("critic_profile", JSONB, nullable=False),
        Column("hekate_profile", JSONB, nullable=False),
        Column("parent_attempt_id", Text, ForeignKey("attempts.id", ondelete="RESTRICT"), nullable=False),
        Column("planning_operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False),
        Column("planning_conclusion_id", Text, ForeignKey("conclusions.id", ondelete="RESTRICT"), nullable=False),
        Column("critic_registry_id", Text, ForeignKey("agent_registry.id", ondelete="RESTRICT"), nullable=False, unique=True),
        Column("create_operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False, unique=True),
        Column("review_attempt_id", Text, nullable=False, unique=True),
        Column("review_operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False, unique=True),
        Column("review_reservation_id", Text, ForeignKey("budget_reservations.id", ondelete="RESTRICT"), nullable=False, unique=True),
        Column("synthesis_attempt_id", Text, nullable=False, unique=True),
        Column("synthesis_operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False, unique=True),
        Column("synthesis_reservation_id", Text, ForeignKey("budget_reservations.id", ondelete="RESTRICT"), nullable=False, unique=True),
        Column("critic_conclusion_id", Text, ForeignKey("conclusions.id", ondelete="RESTRICT"), unique=True),
        Column("delete_operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), unique=True),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        CheckConstraint("input_revision >= 1", name="ck_critic_workflows_revision"),
        CheckConstraint(
            "stage IN ('CREATE_PENDING','CREATE_UNKNOWN','CRITIC_READY','REVIEW_ADMITTED','SYNTHESIS_PENDING','SYNTHESIS_ADMITTED','DELETE_PENDING','COMPLETE','FAILED')",
            name="ck_critic_workflows_stage",
        ),
    )
    op.create_index("ix_critic_workflows_stage", "critic_workflows", ["stage", "updated_at"])


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError("Phase 5A downgrade requires an online database safety check")
    connection = op.get_bind()
    populated = connection.execute(text("SELECT EXISTS(SELECT 1 FROM critic_workflows)")).scalar_one()
    if populated:
        raise RuntimeError("refusing destructive Phase 5A downgrade while Critic workflow records exist")
    op.drop_index("ix_critic_workflows_stage", table_name="critic_workflows")
    op.drop_table("critic_workflows")
