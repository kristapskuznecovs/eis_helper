from typing import Any

from fastapi import APIRouter, Depends, File, UploadFile
from sqlalchemy.orm import Session

from app_template.deps import get_db
from app_template.modules.documents.schemas import DocumentRead
from app_template.modules.documents.service import create_document
from app_template.shared.auth.dependencies import get_current_user

router = APIRouter(prefix="/api/documents", tags=["documents"])


@router.post("/upload", response_model=DocumentRead)
def upload_document(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> DocumentRead:
    document = create_document(db, file)
    return DocumentRead.model_validate(document)
