"""Create reservation, provider-call, observation, and ledger tables."""

from pathlib import Path

from alembic import op

revision = "0002_budget"
down_revision = "0001_operations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in Path(__file__).with_suffix(".sql").read_text(encoding="utf-8").split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    op.drop_constraint("fk_provider_calls_permit_id", "provider_calls", type_="foreignkey")
    op.drop_constraint("fk_attempts_reservation", "attempts", type_="foreignkey")
    for table in (
        "budget_ledger", "usage_projections", "usage_observations", "call_allocations",
        "call_permits", "provider_calls", "reservation_accounts", "budget_reservations",
        "budget_accounts",
    ):
        op.drop_table(table)
