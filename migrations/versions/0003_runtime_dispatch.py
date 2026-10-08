"""Persist the external-send boundary for outbox dispatch."""

from alembic import op
from sqlalchemy import Column, DateTime, Integer, Text

revision = "0003_runtime_dispatch"
down_revision = "0002_budget"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("ck_operations_dispatch_state", "operations", type_="check")
    op.create_check_constraint(
        "ck_operations_dispatch_state",
        "operations",
        "dispatch_state IN ('NOT_STARTED','INTENT_RECORDED','SEND_INTENT','DISPATCHED','UNKNOWN','QUIESCENT')",
    )
    op.add_column("outbox", Column("send_intent_at", DateTime(timezone=True)))
    op.add_column("outbox", Column("send_intent_owner", Text()))
    op.add_column("outbox", Column("send_intent_fence", Integer()))


def downgrade() -> None:
    op.drop_column("outbox", "send_intent_fence")
    op.drop_column("outbox", "send_intent_owner")
    op.drop_column("outbox", "send_intent_at")
    op.drop_constraint("ck_operations_dispatch_state", "operations", type_="check")
    op.create_check_constraint(
        "ck_operations_dispatch_state",
        "operations",
        "dispatch_state IN ('NOT_STARTED','INTENT_RECORDED','DISPATCHED','UNKNOWN','QUIESCENT')",
    )
