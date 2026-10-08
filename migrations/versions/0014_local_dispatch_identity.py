"""Distinguish local candidate calls from synthetic fixtures and revocable scopes.

Revision ID: 0014_local_dispatch_identity
Revises: 0013_provider_token_measurement
"""

from alembic import context, op
from sqlalchemy import Boolean, Column, String, text

revision = "0014_local_dispatch_identity"
down_revision = "0013_provider_token_measurement"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("authorization_scopes", Column("active", Boolean(), nullable=False, server_default=text("true")))
    op.add_column("provider_calls", Column("execution_mode", String(24), nullable=False, server_default=text("'synthetic_test'")))
    op.add_column("provider_calls", Column("price_synthetic", Boolean(), nullable=False, server_default=text("true")))
    op.execute("UPDATE provider_calls SET price_synthetic = test_only")
    op.execute(
        "UPDATE provider_calls SET execution_mode = "
        "CASE WHEN test_only THEN 'synthetic_test' ELSE 'production' END"
    )
    op.create_check_constraint(
        "ck_provider_calls_execution_mode", "provider_calls",
        "execution_mode IN ('synthetic_test','local_candidate','production')",
    )


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError("local dispatch identity downgrade requires an online safety check")
    connection = op.get_bind()
    if connection.execute(text(
        "SELECT EXISTS(SELECT 1 FROM provider_calls WHERE execution_mode = 'local_candidate')"
    )).scalar_one():
        raise RuntimeError("refusing to discard local candidate call history")
    if connection.execute(text(
        "SELECT EXISTS(SELECT 1 FROM authorization_scopes WHERE active = false)"
    )).scalar_one():
        raise RuntimeError("refusing to discard authorization revocation state")
    op.drop_constraint("ck_provider_calls_execution_mode", "provider_calls", type_="check")
    op.drop_column("provider_calls", "price_synthetic")
    op.drop_column("provider_calls", "execution_mode")
    op.drop_column("authorization_scopes", "active")
