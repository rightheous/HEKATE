"""Create operation, task, registry, lease, and durable-delivery tables."""

from pathlib import Path

from alembic import op

revision = "0001_operations"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in Path(__file__).with_suffix(".sql").read_text(encoding="utf-8").split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    op.drop_constraint("fk_agent_registry_active_attempt", "agent_registry", type_="foreignkey")
    for table in (
        "recovery_cases", "audit_events", "inbox", "outbox", "agent_execution_holds",
        "attempts", "agent_leases", "agent_registry", "operations", "task_inputs", "tasks",
        "authorization_scopes",
    ):
        op.drop_table(table)
