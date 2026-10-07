"""Persist the personal-local physical call cap on each Task.

Revision ID: 0015_personal_task_cap
Revises: 0014_local_dispatch_identity
"""

from alembic import context, op
from sqlalchemy import Column, Integer, text

revision = "0015_personal_task_cap"
down_revision = "0014_local_dispatch_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", Column("max_provider_calls", Integer(), nullable=False, server_default="0"))
    op.create_check_constraint("ck_tasks_max_provider_calls", "tasks", "max_provider_calls >= 0")


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError("personal Task generation-cap downgrade requires an online safety check")
    if op.get_bind().execute(text(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE max_provider_calls > 0)"
    )).scalar_one():
        raise RuntimeError("refusing to discard personal Task generation-cap history")
    op.drop_constraint("ck_tasks_max_provider_calls", "tasks", type_="check")
    op.drop_column("tasks", "max_provider_calls")
