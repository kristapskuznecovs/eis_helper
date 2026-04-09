from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app_template.deps import get_db
from app_template.modules.auth.schemas import LoginRequest, RegisterRequest, TokenResponse
from app_template.modules.auth.service import login_user, register_user
from app_template.shared.errors.exceptions import AppError

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/register", response_model=TokenResponse)
def register(payload: RegisterRequest, db: Session = Depends(get_db)) -> TokenResponse:
    try:
        register_user(db, email=payload.email, password=payload.password)
        token = login_user(db, email=payload.email, password=payload.password)
    except ValueError as exc:
        if str(exc) == "User already exists":
            raise AppError(code="auth.email_already_exists", status_code=409) from exc
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return TokenResponse(access_token=token)


@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)) -> TokenResponse:
    try:
        token = login_user(db, email=payload.email, password=payload.password)
    except ValueError as exc:
        if str(exc) == "Invalid credentials":
            raise AppError(code="auth.invalid_credentials", status_code=401) from exc
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    return TokenResponse(access_token=token)
