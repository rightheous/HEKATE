"""Budget accounts, reservations, provider calls, usage, and ledger tables."""

revision = "0002_budget"
down_revision = "0001_operations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    raise NotImplementedError


def downgrade() -> None:
    raise NotImplementedError
