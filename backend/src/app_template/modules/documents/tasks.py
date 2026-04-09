import uuid

from sqlalchemy.orm import Session
from structlog.contextvars import bind_contextvars, clear_contextvars

from app_template.modules.documents.service import mark_document_processed
from app_template.shared.logging.config import get_logger

logger = get_logger(__name__)


def process_document(db: Session, document_id: int):
    clear_contextvars()
    bind_contextvars(
        request_id=str(uuid.uuid4()),
        task_name="process_document",
        document_id=document_id,
    )
    logger.info("task started")
    document = mark_document_processed(db, document_id)
    logger.info("task completed", found=document is not None)
    return document
