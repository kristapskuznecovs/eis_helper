from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from structlog.contextvars import bind_contextvars

from app_template.deps import get_db
from app_template.modules.users.models import User
from app_template.shared.auth.jwt import decode_access_token
from app_template.shared.errors.exceptions import AppError

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    try:
        payload = decode_access_token(token)
        subject = payload.get("sub")
        if not subject:
            raise ValueError("missing subject")
    except Exception as exc:  # pragma: no cover
        raise AppError(code="auth.invalid_token", status_code=401) from exc

    user = db.query(User).filter(User.email == subject).first()
    if user is None:
        raise AppError(code="auth.user_not_found", status_code=401)
    bind_contextvars(user_id=str(user.id))
    return user
