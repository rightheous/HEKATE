"""Evidence archive, immutable Positions, commit receipts, and projection intents."""

from alembic import op
from sqlalchemy import (
    CheckConstraint, Column, DateTime, ForeignKey, ForeignKeyConstraint, Integer,
    PrimaryKeyConstraint, String, Table, Text, UniqueConstraint, text,
)
from sqlalchemy.dialects.postgresql import JSONB

revision = "0006_phase4_knowledge"
down_revision = "0005_phase3b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", Column("evidence_refs", JSONB, nullable=False, server_default=text("'[]'::jsonb")))
    op.add_column("task_inputs", Column("topic_id", Text))
    op.add_column("task_inputs", Column("evidence_refs", JSONB, nullable=False, server_default=text("'[]'::jsonb")))
    op.create_table(
        "artifacts",
        Column("artifact_ref", Text, primary_key=True),
        Column("content_hash", String(64), nullable=False),
        Column("byte_size", Integer, nullable=False),
        Column("retention_class", Text, nullable=False),
        Column("storage_state", Text, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        Column("deleted_at", DateTime(timezone=True)),
        CheckConstraint("byte_size >= 0", name="ck_artifacts_size"),
        CheckConstraint("storage_state IN ('STAGED','AVAILABLE','DELETE_PENDING','DELETE_FAILED','DELETED')", name="ck_artifacts_state"),
    )
    op.create_table(
        "evidence",
        Column("id", Text, primary_key=True),
        Column("scope", Text, ForeignKey("authorization_scopes.id", ondelete="RESTRICT"), nullable=False),
        Column("kind", Text, nullable=False), Column("source_uri", Text, nullable=False), Column("locator", Text),
        Column("retrieved_at", DateTime(timezone=True), nullable=False), Column("observed_at", DateTime(timezone=True)),
        Column("content_hash", String(64), nullable=False), Column("derived_from", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
        Column("root_source_ids", JSONB, nullable=False, server_default=text("'[]'::jsonb")), Column("access_scope", Text, nullable=False),
        Column("retention_class", Text, nullable=False), Column("content_version", Text, nullable=False),
        Column("availability", Text, nullable=False), Column("access_epoch", Integer, nullable=False),
        Column("expiry_at", DateTime(timezone=True)),
        Column("artifact_ref", Text, ForeignKey("artifacts.artifact_ref", ondelete="RESTRICT")),
        Column("registered_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        CheckConstraint("access_epoch >= 0", name="ck_evidence_access_epoch"),
        CheckConstraint("availability IN ('STAGED','AVAILABLE','EXPIRED','REVOKED','UNAVAILABLE')", name="ck_evidence_availability"),
    )
    op.create_index("ix_evidence_scope_expiry", "evidence", ["scope", "expiry_at"])
    op.create_table(
        "evidence_imports",
        Column("scope", Text, ForeignKey("authorization_scopes.id", ondelete="RESTRICT"), primary_key=True),
        Column("request_key", String(128), primary_key=True), Column("request_hash", String(64), nullable=False),
        Column("evidence_id", Text, ForeignKey("evidence.id", ondelete="RESTRICT"), nullable=False, unique=True),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    )
    op.create_table(
        "evidence_edges",
        Column("derived_id", Text, ForeignKey("evidence.id", ondelete="RESTRICT"), nullable=False),
        Column("source_id", Text, ForeignKey("evidence.id", ondelete="RESTRICT"), nullable=False),
        PrimaryKeyConstraint("derived_id", "source_id", name="pk_evidence_edges"),
        CheckConstraint("derived_id <> source_id", name="ck_evidence_edge_not_self"),
    )
    op.create_table(
        "position_topics",
        Column("scope", Text, ForeignKey("authorization_scopes.id", ondelete="RESTRICT"), nullable=False),
        Column("topic_id", Text, nullable=False), Column("current_version", Integer, nullable=False, server_default="0"),
        PrimaryKeyConstraint("scope", "topic_id", name="pk_position_topics"),
        CheckConstraint("current_version >= 0", name="ck_position_topic_version"),
    )
    op.create_table(
        "position_versions",
        Column("scope", Text, nullable=False), Column("topic_id", Text, nullable=False), Column("version", Integer, nullable=False),
        Column("base_version", Integer, nullable=False), Column("body", JSONB, nullable=False),
        Column("operation_id", Text, nullable=False, unique=True),
        Column("task_id", Text, ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False),
        Column("input_revision", Integer, nullable=False),
        Column("registry_id", Text, ForeignKey("agent_registry.id", ondelete="RESTRICT"), nullable=False),
        Column("conclusion_id", Text, ForeignKey("conclusions.id", ondelete="RESTRICT"), nullable=False),
        Column("reason_for_change", Text, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        ForeignKeyConstraint(["scope", "topic_id"], ["position_topics.scope", "position_topics.topic_id"], ondelete="RESTRICT", name="fk_position_version_topic"),
        PrimaryKeyConstraint("scope", "topic_id", "version", name="pk_position_versions"),
        CheckConstraint("version > 0 AND base_version >= 0 AND version = base_version + 1 AND input_revision >= 1", name="ck_position_version_order"),
    )
    op.create_table(
        "dissent",
        Column("id", Text, primary_key=True), Column("scope", Text, ForeignKey("authorization_scopes.id", ondelete="RESTRICT"), nullable=False),
        Column("conclusion_id", Text, ForeignKey("conclusions.id", ondelete="RESTRICT"), nullable=False),
        Column("local_objection_id", Text, nullable=False), Column("body", JSONB, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        UniqueConstraint("conclusion_id", "local_objection_id", name="uq_dissent_local_id"),
    )
    for name, table, reference, pk in (
        ("position_evidence", "position_versions", "evidence", ["scope", "topic_id", "version", "evidence_id"]),
        ("position_dissent", "position_versions", "dissent", ["scope", "topic_id", "version", "dissent_id"]),
    ):
        ref_column = "evidence_id" if reference == "evidence" else "dissent_id"
        op.create_table(
            name,
            Column("scope", Text, nullable=False), Column("topic_id", Text, nullable=False), Column("version", Integer, nullable=False),
            Column(ref_column, Text, ForeignKey(f"{reference}.id", ondelete="RESTRICT"), nullable=False),
            ForeignKeyConstraint(["scope", "topic_id", "version"], ["position_versions.scope", "position_versions.topic_id", "position_versions.version"], ondelete="RESTRICT", name=f"fk_{name}_version"),
            PrimaryKeyConstraint(*pk, name=f"pk_{name}"),
        )
    op.create_table(
        "position_commit_receipts",
        Column("operation_id", Text, primary_key=True), Column("request_hash", String(64), nullable=False),
        Column("scope", Text, ForeignKey("authorization_scopes.id", ondelete="RESTRICT"), nullable=False),
        Column("registry_id", Text, ForeignKey("agent_registry.id", ondelete="RESTRICT"), nullable=False),
        Column("receipt", JSONB, nullable=False), Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    )
    op.create_table(
        "memory_projections",
        Column("scope", Text, nullable=False), Column("topic_id", Text, nullable=False),
        Column("target_registry_id", Text, ForeignKey("agent_registry.id", ondelete="RESTRICT"), nullable=False),
        Column("desired_version", Integer, nullable=False), Column("applied_version", Integer, nullable=False, server_default="0"),
        Column("state", Text, nullable=False), Column("pending_reason", Text),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        ForeignKeyConstraint(["scope", "topic_id"], ["position_topics.scope", "position_topics.topic_id"], ondelete="RESTRICT", name="fk_memory_projection_topic"),
        PrimaryKeyConstraint("scope", "topic_id", "target_registry_id", name="pk_memory_projections"),
        CheckConstraint("desired_version >= 0 AND applied_version >= 0 AND applied_version <= desired_version", name="ck_memory_projection_versions"),
        CheckConstraint("state IN ('PENDING_UNSUPPORTED','APPLIED')", name="ck_memory_projection_state"),
    )
    op.create_table(
        "context_manifests",
        Column("operation_id", Text, ForeignKey("operations.id", ondelete="RESTRICT"), primary_key=True),
        Column("task_id", Text, ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False),
        Column("input_revision", Integer, nullable=False), Column("manifest_hash", String(64), nullable=False),
        Column("manifest", JSONB, nullable=False), Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        CheckConstraint("input_revision >= 1", name="ck_context_manifest_revision"),
    )
    op.execute("""
        CREATE FUNCTION reject_immutable_knowledge_change() RETURNS trigger AS $$
        BEGIN RAISE EXCEPTION 'immutable knowledge rows cannot be changed'; END;
        $$ LANGUAGE plpgsql;
        CREATE TRIGGER immutable_position_versions BEFORE UPDATE OR DELETE ON position_versions
        FOR EACH ROW EXECUTE FUNCTION reject_immutable_knowledge_change();
        CREATE TRIGGER immutable_position_evidence BEFORE UPDATE OR DELETE ON position_evidence
        FOR EACH ROW EXECUTE FUNCTION reject_immutable_knowledge_change();
        CREATE TRIGGER immutable_position_dissent BEFORE UPDATE OR DELETE ON position_dissent
        FOR EACH ROW EXECUTE FUNCTION reject_immutable_knowledge_change();
        CREATE TRIGGER immutable_dissent BEFORE UPDATE OR DELETE ON dissent
        FOR EACH ROW EXECUTE FUNCTION reject_immutable_knowledge_change();
    """)


def downgrade() -> None:
    from sqlalchemy import text as sql_text
    from alembic import context

    connection = op.get_bind()
    if context.is_offline_mode():
        raise RuntimeError("Phase 4 downgrade requires an online database safety check")
    populated = connection.execute(sql_text(
        "SELECT EXISTS(SELECT 1 FROM evidence) "
        "OR EXISTS(SELECT 1 FROM artifacts) "
        "OR EXISTS(SELECT 1 FROM evidence_imports) "
        "OR EXISTS(SELECT 1 FROM evidence_edges) "
        "OR EXISTS(SELECT 1 FROM position_topics) "
        "OR EXISTS(SELECT 1 FROM position_versions) "
        "OR EXISTS(SELECT 1 FROM position_evidence) "
        "OR EXISTS(SELECT 1 FROM position_dissent) "
        "OR EXISTS(SELECT 1 FROM dissent) "
        "OR EXISTS(SELECT 1 FROM position_commit_receipts) "
        "OR EXISTS(SELECT 1 FROM memory_projections) "
        "OR EXISTS(SELECT 1 FROM context_manifests) "
        "OR EXISTS(SELECT 1 FROM outbox WHERE kind IN ('position_projection','archive_delete')) "
        "OR EXISTS(SELECT 1 FROM task_inputs WHERE topic_id IS NOT NULL OR evidence_refs <> '[]'::jsonb) "
        "OR EXISTS(SELECT 1 FROM tasks WHERE topic_id IS NOT NULL OR evidence_refs <> '[]'::jsonb OR base_position_version > 0)"
    )).scalar_one()
    if populated:
        raise RuntimeError("refusing destructive Phase 4 downgrade while Phase 4 knowledge or Task context exists")
    op.execute("DROP TRIGGER immutable_dissent ON dissent")
    op.execute("DROP TRIGGER immutable_position_dissent ON position_dissent")
    op.execute("DROP TRIGGER immutable_position_evidence ON position_evidence")
    op.execute("DROP TRIGGER immutable_position_versions ON position_versions")
    op.execute("DROP FUNCTION reject_immutable_knowledge_change()")
    op.drop_table("context_manifests")
    op.drop_table("memory_projections")
    op.drop_table("position_commit_receipts")
    op.drop_table("position_dissent")
    op.drop_table("position_evidence")
    op.drop_table("dissent")
    op.drop_table("position_versions")
    op.drop_table("position_topics")
    op.drop_table("evidence_edges")
    op.drop_table("evidence_imports")
    op.drop_index("ix_evidence_scope_expiry", table_name="evidence")
    op.drop_table("evidence")
    op.drop_table("artifacts")
    op.drop_column("task_inputs", "evidence_refs")
    op.drop_column("task_inputs", "topic_id")
    op.drop_column("tasks", "evidence_refs")
