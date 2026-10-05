"""Durable bounded deliberation steps."""

from alembic import context, op
from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB

revision = "0008_phase5b_deliberation_steps"
down_revision = "0007_phase5a_critic_workflows"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", Column("hekate_continuations", Integer, nullable=False, server_default="0"))
    op.create_check_constraint(
        "ck_tasks_hekate_continuations", "tasks", "hekate_continuations >= 0",
    )
    op.create_table(
        "deliberation_steps",
        Column("id", Text, primary_key=True),
        Column("task_id", Text, ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False),
        Column("owner_scope", Text, ForeignKey("authorization_scopes.id", ondelete="RESTRICT"), nullable=False),
        Column("input_revision", Integer, nullable=False),
        Column("step_order", Integer, nullable=False),
        Column("step_kind", String(24), nullable=False),
        Column("step_slot", String(40), nullable=False),
        Column("review_round", Integer, nullable=False, server_default="0"),
        Column("parent_attempt_id", Text, ForeignKey("attempts.id", ondelete="RESTRICT"), nullable=False),
        Column("parent_operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False),
        Column("parent_conclusion_id", Text, ForeignKey("conclusions.id", ondelete="RESTRICT"), nullable=False),
        Column("previous_step_id", Text, ForeignKey("deliberation_steps.id", ondelete="RESTRICT")),
        Column("proposal", JSONB, nullable=False),
        Column("proposal_hash", String(64), nullable=False),
        Column("request_hash", String(64), nullable=False),
        Column("work_fingerprint", String(64)),
        Column("state", String(24), nullable=False),
        Column("attempt_id", Text, nullable=False, unique=True),
        Column("operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), nullable=False, unique=True),
        Column("reservation_id", Text, ForeignKey("budget_reservations.id", ondelete="RESTRICT"), nullable=False, unique=True),
        Column("registry_id", Text, ForeignKey("agent_registry.id", ondelete="RESTRICT"), nullable=False),
        Column("profile", JSONB, nullable=False),
        Column("context", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
        Column("conclusion_id", Text, ForeignKey("conclusions.id", ondelete="RESTRICT"), unique=True),
        Column("stop_reason", Text),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        UniqueConstraint("task_id", "step_slot", name="uq_deliberation_task_slot"),
        UniqueConstraint("task_id", "parent_operation_id", "proposal_hash", name="uq_deliberation_parent_proposal"),
        UniqueConstraint("task_id", "work_fingerprint", name="uq_deliberation_task_work_fingerprint"),
        CheckConstraint("input_revision >= 1 AND step_order >= 1 AND review_round >= 0", name="ck_deliberation_step_identity"),
        CheckConstraint("step_kind IN ('hekate_reasoning','critic_review','synthesis')", name="ck_deliberation_step_kind"),
        CheckConstraint("state IN ('READY','WAITING_PARENT','ADMITTED','RESULT_ACCEPTED','COMPLETE','STOPPED','REJECTED')", name="ck_deliberation_step_state"),
    )
    op.create_index("ix_deliberation_steps_ready", "deliberation_steps", ["state", "created_at"])


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError("Phase 5B downgrade requires an online database safety check")
    connection = op.get_bind()
    populated = connection.execute(text("SELECT EXISTS(SELECT 1 FROM deliberation_steps)" )).scalar_one()
    if populated:
        raise RuntimeError("refusing destructive Phase 5B downgrade while deliberation steps exist")
    op.drop_index("ix_deliberation_steps_ready", table_name="deliberation_steps")
    op.drop_table("deliberation_steps")
    op.drop_constraint("ck_tasks_hekate_continuations", "tasks", type_="check")
    op.drop_column("tasks", "hekate_continuations")
