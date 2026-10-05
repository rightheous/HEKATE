"""Track completed and deferred standalone deliberation maintenance."""

from alembic import context, op
from sqlalchemy import Column, DateTime, text

revision = "0010_p5b_delib_maint"
down_revision = "0009_p5b_parent_attempt_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "deliberation_steps",
        Column("maintenance_completed_at", DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "deliberation_steps",
        Column("maintenance_retry_after", DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_delib_maintenance_due",
        "deliberation_steps",
        ["maintenance_retry_after", "created_at"],
        postgresql_where=text("maintenance_completed_at IS NULL"),
    )


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError("Phase 5B maintenance downgrade requires an online safety check")
    connection = op.get_bind()
    progress_exists = connection.execute(text("""
        SELECT EXISTS(
            SELECT 1 FROM deliberation_steps
            WHERE maintenance_completed_at IS NOT NULL
               OR maintenance_retry_after IS NOT NULL
        )
    """)).scalar_one()
    if progress_exists:
        raise RuntimeError("refusing to discard persisted deliberation maintenance progress")
    op.drop_index("ix_delib_maintenance_due", table_name="deliberation_steps")
    op.drop_column("deliberation_steps", "maintenance_retry_after")
    op.drop_column("deliberation_steps", "maintenance_completed_at")
