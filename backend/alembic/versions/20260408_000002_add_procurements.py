"""Add procurements table.

Revision ID: 20260408_000002
Revises: 20260408_000001
Create Date: 2026-04-08 00:00:02
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260408_000002"
down_revision: str | None = "20260408_000001"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    op.create_table(
        "procurements",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("procurement_id", sa.String(64), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("buyer", sa.Text(), nullable=True),
        sa.Column("region", sa.Text(), nullable=True),
        sa.Column("cpv_main", sa.String(256), nullable=True),
        sa.Column("estimated_value_eur", sa.Float(), nullable=True),
        sa.Column("publication_date", sa.String(32), nullable=True),
        sa.Column("submission_deadline", sa.String(32), nullable=True),
        sa.Column("status", sa.String(256), nullable=True),
        sa.Column("procedure_type", sa.String(256), nullable=True),
        sa.Column("eis_url", sa.Text(), nullable=True),
        sa.Column("synced_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_procurements_procurement_id", "procurements", ["procurement_id"], unique=True)
    op.create_index("ix_procurements_cpv_main", "procurements", ["cpv_main"])
    op.create_index("ix_procurements_submission_deadline", "procurements", ["submission_deadline"])
    op.create_index("ix_procurements_status", "procurements", ["status"])
    op.execute(
        "CREATE INDEX ix_procurements_title_trgm ON procurements USING GIN (title gin_trgm_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_procurements_title_trgm")
    op.drop_index("ix_procurements_status", table_name="procurements")
    op.drop_index("ix_procurements_submission_deadline", table_name="procurements")
    op.drop_index("ix_procurements_cpv_main", table_name="procurements")
    op.drop_index("ix_procurements_procurement_id", table_name="procurements")
    op.drop_table("procurements")
