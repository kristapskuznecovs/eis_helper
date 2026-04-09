from pathlib import Path

from fastapi import UploadFile
from sqlalchemy.orm import Session

from app_template.modules.documents.models import Document
from app_template.shared.storage import service as storage_service


def create_document(db: Session, file: UploadFile) -> Document:
    content = file.file.read()
    filename = Path(file.filename or "upload.bin").name
    relative_path = str(Path("documents") / filename)
    stored_file = storage_service.upload(
        key=relative_path,
        data=content,
        content_type=file.content_type or "application/octet-stream",
    )

    document = Document(filename=filename, storage_path=stored_file.key, status="uploaded")
    db.add(document)
    db.commit()
    db.refresh(document)
    return document


def mark_document_processed(db: Session, document_id: int) -> Document | None:
    document = db.query(Document).filter(Document.id == document_id).first()
    if document is None:
        return None
    document.status = "processed"
    db.commit()
    db.refresh(document)
    return document
