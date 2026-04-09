from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app_template.shared.db.base import Base


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(String(255))
    storage_path: Mapped[str] = mapped_column(String(512))
    status: Mapped[str] = mapped_column(String(64), default="uploaded")
    extracted_text: Mapped[str | None] = mapped_column(Text(), nullable=True)
