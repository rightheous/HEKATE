"""Keep terminal runtime observations durable while individual calls settle."""

from alembic import op
from sqlalchemy import Column, DateTime, Text, text

revision = "0004_terminal_inbox"
down_revision = "0003_runtime_dispatch"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("inbox", Column("next_attempt_at", DateTime(timezone=True), nullable=False, server_default=text("now()")))
    op.add_column("inbox", Column("pending_reason", Text()))
    op.add_column("inbox", Column("rejection_reason", Text()))
    op.create_index(
        "ix_inbox_pending",
        "inbox",
        ["next_attempt_at", "received_at"],
        postgresql_where=text("processed_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_inbox_pending", table_name="inbox")
    op.drop_column("inbox", "rejection_reason")
    op.drop_column("inbox", "pending_reason")
    op.drop_column("inbox", "next_attempt_at")
