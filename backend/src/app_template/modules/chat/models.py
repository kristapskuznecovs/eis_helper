from datetime import datetime

from sqlalchemy import Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app_template.shared.db.base import Base


class Procurement(Base):
    __tablename__ = "procurements"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Core identification
    procurement_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    identification_number: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # Buyer
    buyer: Mapped[str | None] = mapped_column(Text, nullable=True)
    buyer_reg_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    buyer_reg_number_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    buyer_pvs_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parent_organization: Mapped[str | None] = mapped_column(Text, nullable=True)
    for_other_buyers: Mapped[str | None] = mapped_column(String(8), nullable=True)
    actual_recipient: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Procurement details
    title: Mapped[str] = mapped_column(Text)
    subject_type: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    cpv_main: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)
    cpv_additional: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)
    governing_law: Mapped[str | None] = mapped_column(String(256), nullable=True)
    procedure_type: Mapped[str | None] = mapped_column(String(256), nullable=True)
    winner_selection_method: Mapped[str | None] = mapped_column(String(256), nullable=True)
    submission_language: Mapped[str | None] = mapped_column(String(64), nullable=True)
    eu_project_reference: Mapped[str | None] = mapped_column(Text, nullable=True)
    variants_allowed: Mapped[str | None] = mapped_column(String(8), nullable=True)
    submission_place: Mapped[str | None] = mapped_column(Text, nullable=True)
    interested_parties_meeting: Mapped[str | None] = mapped_column(Text, nullable=True)
    contact_person: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Dates
    publication_date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    submission_deadline: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    submission_deadline_time: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # Location
    region: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Contract duration
    contract_duration_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    contract_duration: Mapped[int | None] = mapped_column(Integer, nullable=True)
    contract_duration_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    contract_start_date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    contract_end_date: Mapped[str | None] = mapped_column(String(32), nullable=True)

    # Contract value
    value_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    estimated_value_eur: Mapped[float | None] = mapped_column(Float, nullable=True)
    value_min_eur: Mapped[float | None] = mapped_column(Float, nullable=True)
    value_max_eur: Mapped[float | None] = mapped_column(Float, nullable=True)
    value_currency: Mapped[str | None] = mapped_column(String(8), nullable=True)

    # URLs
    eis_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    iub_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Lots
    has_lots: Mapped[str | None] = mapped_column(String(8), nullable=True)
    lot_submission_conditions: Mapped[str | None] = mapped_column(Text, nullable=True)
    lot_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lot_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    lot_status: Mapped[str | None] = mapped_column(String(256), nullable=True)
    lot_contract_duration_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lot_contract_duration: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lot_contract_duration_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lot_contract_start_date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lot_contract_end_date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lot_value_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lot_estimated_value_eur: Mapped[float | None] = mapped_column(Float, nullable=True)
    lot_value_min_eur: Mapped[float | None] = mapped_column(Float, nullable=True)
    lot_value_max_eur: Mapped[float | None] = mapped_column(Float, nullable=True)
    lot_value_currency: Mapped[str | None] = mapped_column(String(8), nullable=True)

    synced_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
