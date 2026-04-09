"""Initial schema.

Revision ID: 20260408_000001
Revises:
Create Date: 2026-04-08 00:00:01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260408_000001"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "documents",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("storage_path", sa.String(length=512), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("extracted_text", sa.Text(), nullable=True),
    )
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("hashed_password", sa.String(length=255), nullable=False),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_users_email", table_name="users")
    op.drop_table("users")
    op.drop_table("documents")
