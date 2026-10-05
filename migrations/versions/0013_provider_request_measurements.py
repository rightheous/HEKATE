"""Bind final provider request token measurements to calls and permits.

Revision ID: 0013_provider_token_measurement
Revises: 0012_projection_write_guard
"""

from alembic import context, op
from sqlalchemy import Column, String, text
from sqlalchemy.dialects.postgresql import JSONB

revision = "0013_provider_token_measurement"
down_revision = "0012_projection_write_guard"
branch_labels = None
depends_on = None


def _measurement_constraints(table: str) -> None:
    op.create_check_constraint(
        f"ck_{table}_measurement_status", table,
        "measurement_status IN ('LEGACY_UNMEASURED','MEASURED')",
    )
    op.create_check_constraint(
        f"ck_{table}_measurement_data", table,
        "(measurement_status = 'LEGACY_UNMEASURED' AND request_digest IS NULL AND profile_digest IS NULL AND measurement_data IS NULL) "
        "OR (measurement_status = 'MEASURED' AND length(request_digest) = 64 AND length(profile_digest) = 64 AND measurement_data IS NOT NULL)",
    )


def upgrade() -> None:
    for table in ("provider_calls", "call_permits"):
        op.add_column(table, Column("measurement_status", String(24), nullable=False, server_default="LEGACY_UNMEASURED"))
        op.add_column(table, Column("request_digest", String(64)))
        op.add_column(table, Column("profile_digest", String(64)))
        op.add_column(table, Column("measurement_data", JSONB))
        _measurement_constraints(table)


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError("provider measurement downgrade requires an online safety check")
    connection = op.get_bind()
    for table in ("call_permits", "provider_calls"):
        if connection.execute(text(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE measurement_status = 'MEASURED')")).scalar_one():
            raise RuntimeError(f"refusing to discard measured provider request history in {table}")
    for table in ("call_permits", "provider_calls"):
        op.drop_constraint(f"ck_{table}_measurement_data", table, type_="check")
        op.drop_constraint(f"ck_{table}_measurement_status", table, type_="check")
        op.drop_column(table, "measurement_data")
        op.drop_column(table, "profile_digest")
        op.drop_column(table, "request_digest")
        op.drop_column(table, "measurement_status")
