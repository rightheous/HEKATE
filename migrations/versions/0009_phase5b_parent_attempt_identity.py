"""Allow approved child steps to reference a not-yet-admitted parent attempt."""

from alembic import context, op
from sqlalchemy import text

revision = "0009_p5b_parent_attempt_id"
down_revision = "0008_phase5b_deliberation_steps"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "deliberation_steps_parent_attempt_id_fkey",
        "deliberation_steps",
        type_="foreignkey",
    )


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError("Phase 5B parent-attempt downgrade requires an online safety check")
    connection = op.get_bind()
    pending = connection.execute(text("""
        SELECT EXISTS(
            SELECT 1 FROM deliberation_steps d
            LEFT JOIN attempts a ON a.id=d.parent_attempt_id
            WHERE a.id IS NULL
        )
    """)).scalar_one()
    if pending:
        raise RuntimeError("refusing to restore parent-attempt FK while deferred identities are unadmitted")
    op.create_foreign_key(
        "deliberation_steps_parent_attempt_id_fkey",
        "deliberation_steps", "attempts", ["parent_attempt_id"], ["id"],
        ondelete="RESTRICT",
    )
