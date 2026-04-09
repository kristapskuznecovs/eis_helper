"""Expand procurements table to store all CKAN fields.

Revision ID: 20260408_000003
Revises: 20260408_000002
Create Date: 2026-04-08 00:00:03
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260408_000003"
down_revision: str | None = "20260408_000002"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("procurements", sa.Column("identification_number", sa.String(128), nullable=True))

    # Buyer extended
    op.add_column("procurements", sa.Column("buyer_reg_number", sa.String(64), nullable=True))
    op.add_column("procurements", sa.Column("buyer_reg_number_type", sa.String(128), nullable=True))
    op.add_column("procurements", sa.Column("buyer_pvs_id", sa.Integer(), nullable=True))
    op.add_column("procurements", sa.Column("parent_organization", sa.Text(), nullable=True))
    op.add_column("procurements", sa.Column("for_other_buyers", sa.String(8), nullable=True))
    op.add_column("procurements", sa.Column("actual_recipient", sa.Text(), nullable=True))

    # Procurement details
    op.add_column("procurements", sa.Column("subject_type", sa.String(64), nullable=True))
    op.add_column("procurements", sa.Column("cpv_additional", sa.Text(), nullable=True))
    op.add_column("procurements", sa.Column("governing_law", sa.String(256), nullable=True))
    op.add_column("procurements", sa.Column("winner_selection_method", sa.String(256), nullable=True))
    op.add_column("procurements", sa.Column("submission_language", sa.String(64), nullable=True))
    op.add_column("procurements", sa.Column("eu_project_reference", sa.Text(), nullable=True))
    op.add_column("procurements", sa.Column("variants_allowed", sa.String(8), nullable=True))
    op.add_column("procurements", sa.Column("submission_place", sa.Text(), nullable=True))
    op.add_column("procurements", sa.Column("interested_parties_meeting", sa.Text(), nullable=True))
    op.add_column("procurements", sa.Column("contact_person", sa.Text(), nullable=True))

    # Dates
    op.add_column("procurements", sa.Column("submission_deadline_time", sa.String(16), nullable=True))

    # Contract duration
    op.add_column("procurements", sa.Column("contract_duration_type", sa.String(128), nullable=True))
    op.add_column("procurements", sa.Column("contract_duration", sa.Integer(), nullable=True))
    op.add_column("procurements", sa.Column("contract_duration_unit", sa.String(32), nullable=True))
    op.add_column("procurements", sa.Column("contract_start_date", sa.String(32), nullable=True))
    op.add_column("procurements", sa.Column("contract_end_date", sa.String(32), nullable=True))

    # Contract value extended
    op.add_column("procurements", sa.Column("value_type", sa.String(128), nullable=True))
    op.add_column("procurements", sa.Column("value_min_eur", sa.Float(), nullable=True))
    op.add_column("procurements", sa.Column("value_max_eur", sa.Float(), nullable=True))
    op.add_column("procurements", sa.Column("value_currency", sa.String(8), nullable=True))

    # URLs
    op.add_column("procurements", sa.Column("iub_url", sa.Text(), nullable=True))

    # Lots
    op.add_column("procurements", sa.Column("has_lots", sa.String(8), nullable=True))
    op.add_column("procurements", sa.Column("lot_submission_conditions", sa.Text(), nullable=True))
    op.add_column("procurements", sa.Column("lot_number", sa.Integer(), nullable=True))
    op.add_column("procurements", sa.Column("lot_name", sa.Text(), nullable=True))
    op.add_column("procurements", sa.Column("lot_status", sa.String(256), nullable=True))
    op.add_column("procurements", sa.Column("lot_contract_duration_type", sa.String(128), nullable=True))
    op.add_column("procurements", sa.Column("lot_contract_duration", sa.Integer(), nullable=True))
    op.add_column("procurements", sa.Column("lot_contract_duration_unit", sa.String(32), nullable=True))
    op.add_column("procurements", sa.Column("lot_contract_start_date", sa.String(32), nullable=True))
    op.add_column("procurements", sa.Column("lot_contract_end_date", sa.String(32), nullable=True))
    op.add_column("procurements", sa.Column("lot_value_type", sa.String(128), nullable=True))
    op.add_column("procurements", sa.Column("lot_estimated_value_eur", sa.Float(), nullable=True))
    op.add_column("procurements", sa.Column("lot_value_min_eur", sa.Float(), nullable=True))
    op.add_column("procurements", sa.Column("lot_value_max_eur", sa.Float(), nullable=True))
    op.add_column("procurements", sa.Column("lot_value_currency", sa.String(8), nullable=True))

    # Index subject_type for filtering
    op.create_index("ix_procurements_subject_type", "procurements", ["subject_type"])


def downgrade() -> None:
    op.drop_index("ix_procurements_subject_type", table_name="procurements")

    for col in [
        "identification_number", "buyer_reg_number", "buyer_reg_number_type", "buyer_pvs_id",
        "parent_organization", "for_other_buyers", "actual_recipient", "subject_type",
        "cpv_additional", "governing_law", "winner_selection_method", "submission_language",
        "eu_project_reference", "variants_allowed", "submission_place",
        "interested_parties_meeting", "contact_person", "submission_deadline_time",
        "contract_duration_type", "contract_duration", "contract_duration_unit",
        "contract_start_date", "contract_end_date", "value_type", "value_min_eur",
        "value_max_eur", "value_currency", "iub_url", "has_lots", "lot_submission_conditions",
        "lot_number", "lot_name", "lot_status", "lot_contract_duration_type",
        "lot_contract_duration", "lot_contract_duration_unit", "lot_contract_start_date",
        "lot_contract_end_date", "lot_value_type", "lot_estimated_value_eur",
        "lot_value_min_eur", "lot_value_max_eur", "lot_value_currency",
    ]:
        op.drop_column("procurements", col)
