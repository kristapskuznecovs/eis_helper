"""Add raw CKAN tables: results, participants, amendments, purchase_orders, deliveries, buyers.

Revision ID: 20260408_000004
Revises: 20260408_000003
Create Date: 2026-04-08 00:00:04
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260408_000004"
down_revision: str | None = "20260408_000003"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. iepirkumu-rezultatu-datu-grupa  — contract results / winners
    # ------------------------------------------------------------------
    op.create_table(
        "ckan_results",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("procurement_id", sa.String(64), nullable=False),          # Iepirkuma_ID
        sa.Column("procurement_title", sa.Text(), nullable=True),             # Iepirkuma_nosaukums
        sa.Column("buyer", sa.Text(), nullable=True),                         # Pasutitaja_nosaukums
        sa.Column("buyer_reg_number", sa.String(64), nullable=True),          # Pasutitaja_registracijas_numurs
        sa.Column("winner_name", sa.Text(), nullable=True),                   # Uzvaretaja_nosaukums
        sa.Column("winner_reg_number", sa.String(64), nullable=True),         # Uzvaretaja_registracijas_numurs
        sa.Column("contract_value_eur", sa.Float(), nullable=True),           # Aktuala_liguma_summa (current, may be amended)
        sa.Column("contract_value_original_eur", sa.Float(), nullable=True),  # Sakotneja_liguma_summa
        sa.Column("contract_signed_date", sa.String(32), nullable=True),      # Liguma_dok_noslegsanas_datums
        sa.Column("source_year", sa.Integer(), nullable=True),
        sa.Column("synced_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_ckan_results_procurement_id", "ckan_results", ["procurement_id"])
    op.create_index("ix_ckan_results_winner_reg_number", "ckan_results", ["winner_reg_number"])
    op.create_index("ix_ckan_results_source_year", "ckan_results", ["source_year"])
    # Unique per (procurement_id, winner_reg_number) — one row per winner per procurement
    op.create_index(
        "ix_ckan_results_unique",
        "ckan_results",
        ["procurement_id", "winner_reg_number"],
        unique=True,
    )

    # ------------------------------------------------------------------
    # 2. iepirkumu-piedavajumu-atversanu-datu-grupa  — offer openings / participants
    # ------------------------------------------------------------------
    op.create_table(
        "ckan_participants",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("procurement_id", sa.String(64), nullable=False),           # Iepirkuma_ID
        sa.Column("procurement_title", sa.Text(), nullable=True),             # Iepirkuma_nosaukums
        sa.Column("identification_number", sa.String(128), nullable=True),    # Iepirkuma_identifikacijas_numurs
        sa.Column("buyer", sa.Text(), nullable=True),                         # Pasutitaja_nosaukums
        sa.Column("buyer_reg_number", sa.String(64), nullable=True),          # Pasutitaja_registracijas_numurs
        sa.Column("buyer_pvs_id", sa.Integer(), nullable=True),               # Pasutitaja_PVS_ID
        sa.Column("parent_organization", sa.Text(), nullable=True),           # Augstak_stavosa_organizacija
        sa.Column("status", sa.String(256), nullable=True),                   # Iepirkuma_statuss
        sa.Column("cpv_main", sa.String(256), nullable=True),                 # CPV_kods_galvenais_prieksmets
        sa.Column("subject_type", sa.String(64), nullable=True),              # Iepirkuma_prieksmeta_veids
        sa.Column("estimated_value_eur", sa.Float(), nullable=True),          # Planota_ligumcena
        sa.Column("currency", sa.String(8), nullable=True),                   # Ligumcenas_valuta
        sa.Column("contract_duration_months", sa.Integer(), nullable=True),   # Planotais_liguma_darbibas_termins
        sa.Column("submission_deadline", sa.String(32), nullable=True),       # Piedavajumu_iesniegsanas_termins_datums
        sa.Column("participant_name", sa.Text(), nullable=True),              # Pretendenta_nosaukums
        sa.Column("participant_reg_number", sa.String(64), nullable=True),    # Pretendenta_registracijas_numurs
        sa.Column("participant_submitted_at", sa.String(32), nullable=True),  # Pretendenta_piedavajuma_iesniegsanas_datums
        sa.Column("source_year", sa.Integer(), nullable=True),
        sa.Column("synced_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_ckan_participants_procurement_id", "ckan_participants", ["procurement_id"])
    op.create_index("ix_ckan_participants_participant_reg_number", "ckan_participants", ["participant_reg_number"])
    op.create_index("ix_ckan_participants_source_year", "ckan_participants", ["source_year"])
    op.create_index("ix_ckan_participants_cpv_main", "ckan_participants", ["cpv_main"])
    op.create_index(
        "ix_ckan_participants_unique",
        "ckan_participants",
        ["procurement_id", "participant_reg_number"],
        unique=True,
    )

    # ------------------------------------------------------------------
    # 3. iepirkumu-grozijumu-datu-grupa  — amendments
    # ------------------------------------------------------------------
    op.create_table(
        "ckan_amendments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("procurement_id", sa.String(64), nullable=False),           # Iepirkuma_ID
        sa.Column("procurement_title", sa.Text(), nullable=True),             # Iepirkuma_nosaukums
        sa.Column("identification_number", sa.String(128), nullable=True),    # Iepirkuma_identifikacijas_numurs
        sa.Column("buyer", sa.Text(), nullable=True),                         # Pasutitaja_nosaukums
        sa.Column("buyer_reg_number", sa.String(64), nullable=True),          # Pasutitaja_registracijas_numurs
        sa.Column("buyer_reg_number_type", sa.String(128), nullable=True),    # Pasutitaja_registracijas_numura_veids
        sa.Column("buyer_pvs_id", sa.Integer(), nullable=True),               # Pasutitaja_PVS_ID
        sa.Column("parent_organization", sa.Text(), nullable=True),           # Augstak_stavosa_organizacija
        sa.Column("status", sa.String(256), nullable=True),                   # Iepirkuma_statuss
        sa.Column("announcement_date", sa.String(32), nullable=True),         # Iepirkuma_izsludinasanas_datums
        sa.Column("amendment_date", sa.String(32), nullable=True),            # Grozijumu_datums
        sa.Column("submission_deadline", sa.String(64), nullable=True),       # Piedavajumu_iesniegsanas_datumlaiks
        sa.Column("interested_parties_meeting", sa.Text(), nullable=True),    # Ieintereseto_personu_sanaksmes
        sa.Column("eu_project_reference", sa.Text(), nullable=True),          # Atsauce_uz_ES_projektiem_un_programmam
        sa.Column("eis_url", sa.Text(), nullable=True),                       # Hipersaite_EIS_kura_pieejams_zinojums
        sa.Column("iub_url", sa.Text(), nullable=True),                       # Hipersaite_uz_IUB_publikaciju
        sa.Column("has_lots", sa.String(8), nullable=True),                   # Ir_dalijums_dalas
        sa.Column("lot_submission_conditions", sa.Text(), nullable=True),     # Dalu_iesniegsanas_nosacijumi
        sa.Column("lot_number", sa.Integer(), nullable=True),                 # Iepirkuma_dalas_nr
        sa.Column("lot_name", sa.Text(), nullable=True),                      # Iepirkuma_dalas_nosaukums
        sa.Column("lot_status", sa.String(256), nullable=True),               # Iepirkuma_dalas_statuss
        sa.Column("source_year", sa.Integer(), nullable=True),
        sa.Column("synced_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_ckan_amendments_procurement_id", "ckan_amendments", ["procurement_id"])
    op.create_index("ix_ckan_amendments_amendment_date", "ckan_amendments", ["amendment_date"])
    op.create_index("ix_ckan_amendments_source_year", "ckan_amendments", ["source_year"])

    # ------------------------------------------------------------------
    # 4. pirkuma-pasutijumu-datu-grupa  — purchase orders (catalog orders via EIS)
    # ------------------------------------------------------------------
    op.create_table(
        "ckan_purchase_orders",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("order_number", sa.String(128), nullable=True),             # Pasutijuma_Nr
        sa.Column("order_status", sa.String(64), nullable=True),              # Pasutijuma_statuss
        sa.Column("order_confirmed_date", sa.String(32), nullable=True),      # Pasutijuma_apstiprinasanas_datums
        sa.Column("buyer", sa.Text(), nullable=True),                         # Pasutitajs
        sa.Column("buyer_reg_number", sa.String(64), nullable=True),          # Pasutitaja_registracijas_numurs
        sa.Column("buyer_reg_number_type", sa.String(128), nullable=True),    # Pasutitaja_registracijas_numura_veids
        sa.Column("parent_organization", sa.Text(), nullable=True),           # Augstak_stavosa_organizacija
        sa.Column("supplier", sa.Text(), nullable=True),                      # Piegadatajs
        sa.Column("supplier_reg_number", sa.String(64), nullable=True),       # Piegadataja_registracijas_numurs
        sa.Column("supplier_reg_number_type", sa.String(128), nullable=True), # Piegadataja_registracijas_numura_veids
        sa.Column("catalog_number", sa.String(128), nullable=True),           # Kataloga_numurs
        sa.Column("catalog_name", sa.Text(), nullable=True),                  # Kataloga_nosaukums
        sa.Column("item_name", sa.Text(), nullable=True),                     # Pasutitas_preces_nosaukums
        sa.Column("item_manufacturer", sa.Text(), nullable=True),             # Pasutitas_preces_razotajs
        sa.Column("max_delivery_date", sa.String(32), nullable=True),         # Max_piegades_datums
        sa.Column("amount_eur_ex_vat", sa.Float(), nullable=True),            # Summa_bez_PVN
        sa.Column("vat_pct", sa.Float(), nullable=True),                      # PVN_proc
        sa.Column("quantity", sa.Float(), nullable=True),                     # Pasutitas_skaits
        sa.Column("source_year", sa.Integer(), nullable=True),
        sa.Column("synced_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_ckan_purchase_orders_buyer_reg_number", "ckan_purchase_orders", ["buyer_reg_number"])
    op.create_index("ix_ckan_purchase_orders_supplier_reg_number", "ckan_purchase_orders", ["supplier_reg_number"])
    op.create_index("ix_ckan_purchase_orders_source_year", "ckan_purchase_orders", ["source_year"])

    # ------------------------------------------------------------------
    # 5. piegazu-datu-grupa  — deliveries (fulfilled purchase orders)
    # ------------------------------------------------------------------
    op.create_table(
        "ckan_deliveries",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("order_number", sa.String(128), nullable=True),             # PasutijumaNr
        sa.Column("waybill_number", sa.String(128), nullable=True),           # Pavadzimes_nr
        sa.Column("delivery_status", sa.String(64), nullable=True),           # Piegades_statuss
        sa.Column("buyer", sa.Text(), nullable=True),                         # Pasutitajs
        sa.Column("buyer_reg_number", sa.String(64), nullable=True),          # Pasut_orgnr
        sa.Column("buyer_reg_number_type", sa.String(128), nullable=True),    # Pasut_org_reg_nr_veids
        sa.Column("parent_organization", sa.Text(), nullable=True),           # Augstak_stavosa_organizacija
        sa.Column("supplier", sa.Text(), nullable=True),                      # Piegadatajs
        sa.Column("supplier_reg_number", sa.String(64), nullable=True),       # Piegnr
        sa.Column("supplier_reg_number_type", sa.String(128), nullable=True), # Pieg_org_nr_veids
        sa.Column("catalog_number", sa.String(128), nullable=True),           # Kataloga_nr
        sa.Column("catalog_name", sa.Text(), nullable=True),                  # Kataloga_nos
        sa.Column("item_name", sa.Text(), nullable=True),                     # Preces_nosaukums
        sa.Column("item_manufacturer", sa.Text(), nullable=True),             # Piegadatas_preces_razotajs
        sa.Column("purchase_created_date", sa.String(32), nullable=True),     # Pirkuma_izveid_dat
        sa.Column("quality_approved_date", sa.String(32), nullable=True),     # Kval_apstipr_dat
        sa.Column("delivery_address", sa.Text(), nullable=True),              # Piegades_adrese
        sa.Column("amount_eur_ex_vat", sa.Float(), nullable=True),            # Summa_bez_PVN
        sa.Column("vat_pct", sa.Float(), nullable=True),                      # PVN_proc
        sa.Column("quantity", sa.Float(), nullable=True),                     # Kval_skaits
        sa.Column("source_year", sa.Integer(), nullable=True),
        sa.Column("synced_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_ckan_deliveries_buyer_reg_number", "ckan_deliveries", ["buyer_reg_number"])
    op.create_index("ix_ckan_deliveries_supplier_reg_number", "ckan_deliveries", ["supplier_reg_number"])
    op.create_index("ix_ckan_deliveries_source_year", "ckan_deliveries", ["source_year"])

    # ------------------------------------------------------------------
    # 6. pasutitaju-datu-grupa  — buyers registry
    # ------------------------------------------------------------------
    op.create_table(
        "ckan_buyers",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("organization", sa.Text(), nullable=False),                 # Organizacija
        sa.Column("reg_number", sa.String(64), nullable=True),                # RegNr
        sa.Column("reg_number_type", sa.String(128), nullable=True),          # RegNrVeids
        sa.Column("parent_organization", sa.Text(), nullable=True),           # Augstak_stavosa_organizacija
        sa.Column("registered_date", sa.String(32), nullable=True),           # RegDatums
        sa.Column("is_blocked", sa.Boolean(), nullable=True),                 # Blokets
        sa.Column("is_deleted", sa.Boolean(), nullable=True),                 # Dzests
        sa.Column("synced_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_ckan_buyers_reg_number", "ckan_buyers", ["reg_number"], unique=True)

    # ------------------------------------------------------------------
    # Views
    # ------------------------------------------------------------------
    # View: procurement with its winner (most recent contract value per winner)
    op.execute("""
        CREATE VIEW v_procurement_winners AS
        SELECT
            p.procurement_id,
            p.title,
            p.buyer,
            p.cpv_main,
            p.status,
            p.submission_deadline,
            p.estimated_value_eur,
            p.eis_url,
            r.winner_name,
            r.winner_reg_number,
            r.contract_value_eur,
            r.contract_signed_date,
            r.source_year AS result_year
        FROM procurements p
        JOIN ckan_results r ON r.procurement_id = p.procurement_id
    """)

    # View: procurement with participants (bidders)
    op.execute("""
        CREATE VIEW v_procurement_participants AS
        SELECT
            p.procurement_id,
            p.title,
            p.buyer,
            p.cpv_main,
            p.status,
            p.submission_deadline,
            p.estimated_value_eur,
            p.eis_url,
            pt.participant_name,
            pt.participant_reg_number,
            pt.participant_submitted_at,
            pt.source_year AS participant_year
        FROM procurements p
        JOIN ckan_participants pt ON pt.procurement_id = p.procurement_id
    """)

    # View: full picture — procurement + all participants + winner flag
    op.execute("""
        CREATE VIEW v_procurement_full AS
        SELECT
            pt.procurement_id,
            pt.procurement_title         AS title,
            pt.buyer,
            pt.cpv_main,
            pt.status,
            pt.submission_deadline,
            pt.estimated_value_eur,
            pt.participant_name,
            pt.participant_reg_number,
            pt.participant_submitted_at,
            r.winner_name,
            r.winner_reg_number,
            (pt.participant_reg_number IS NOT NULL
             AND pt.participant_reg_number = r.winner_reg_number) AS is_winner,
            r.contract_value_eur,
            r.contract_signed_date
        FROM ckan_participants pt
        LEFT JOIN ckan_results r
            ON r.procurement_id = pt.procurement_id
           AND r.winner_reg_number = pt.participant_reg_number
    """)

    # View: org activity — total won + total participated per org
    op.execute("""
        CREATE VIEW v_org_activity AS
        SELECT
            COALESCE(r.winner_reg_number, pt.participant_reg_number) AS reg_number,
            COALESCE(r.winner_name, pt.participant_name)             AS org_name,
            COUNT(DISTINCT r.procurement_id)                          AS procurements_won,
            SUM(r.contract_value_eur)                                 AS total_won_eur,
            COUNT(DISTINCT pt.procurement_id)                         AS procurements_participated,
            MIN(COALESCE(r.contract_signed_date, pt.submission_deadline)) AS first_activity,
            MAX(COALESCE(r.contract_signed_date, pt.submission_deadline)) AS last_activity
        FROM ckan_participants pt
        FULL OUTER JOIN ckan_results r
            ON r.winner_reg_number = pt.participant_reg_number
           AND r.procurement_id = pt.procurement_id
        GROUP BY 1, 2
    """)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS v_org_activity")
    op.execute("DROP VIEW IF EXISTS v_procurement_full")
    op.execute("DROP VIEW IF EXISTS v_procurement_participants")
    op.execute("DROP VIEW IF EXISTS v_procurement_winners")

    op.drop_index("ix_ckan_buyers_reg_number", table_name="ckan_buyers")
    op.drop_table("ckan_buyers")

    op.drop_index("ix_ckan_deliveries_source_year", table_name="ckan_deliveries")
    op.drop_index("ix_ckan_deliveries_supplier_reg_number", table_name="ckan_deliveries")
    op.drop_index("ix_ckan_deliveries_buyer_reg_number", table_name="ckan_deliveries")
    op.drop_table("ckan_deliveries")

    op.drop_index("ix_ckan_purchase_orders_source_year", table_name="ckan_purchase_orders")
    op.drop_index("ix_ckan_purchase_orders_supplier_reg_number", table_name="ckan_purchase_orders")
    op.drop_index("ix_ckan_purchase_orders_buyer_reg_number", table_name="ckan_purchase_orders")
    op.drop_table("ckan_purchase_orders")

    op.drop_index("ix_ckan_amendments_source_year", table_name="ckan_amendments")
    op.drop_index("ix_ckan_amendments_amendment_date", table_name="ckan_amendments")
    op.drop_index("ix_ckan_amendments_procurement_id", table_name="ckan_amendments")
    op.drop_table("ckan_amendments")

    op.drop_index("ix_ckan_participants_unique", table_name="ckan_participants")
    op.drop_index("ix_ckan_participants_cpv_main", table_name="ckan_participants")
    op.drop_index("ix_ckan_participants_source_year", table_name="ckan_participants")
    op.drop_index("ix_ckan_participants_participant_reg_number", table_name="ckan_participants")
    op.drop_index("ix_ckan_participants_procurement_id", table_name="ckan_participants")
    op.drop_table("ckan_participants")

    op.drop_index("ix_ckan_results_unique", table_name="ckan_results")
    op.drop_index("ix_ckan_results_source_year", table_name="ckan_results")
    op.drop_index("ix_ckan_results_winner_reg_number", table_name="ckan_results")
    op.drop_index("ix_ckan_results_procurement_id", table_name="ckan_results")
    op.drop_table("ckan_results")
