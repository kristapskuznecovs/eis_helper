from sqlalchemy.orm import Session

from app_template.modules.users.models import User
from app_template.modules.users.service import get_user_by_email
from app_template.shared.auth.service import authenticate_user, build_password_hash


def register_user(db: Session, *, email: str, password: str) -> User:
    existing = get_user_by_email(db, email)
    if existing is not None:
        raise ValueError("User already exists")

    user = User(email=email, hashed_password=build_password_hash(password))
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def login_user(db: Session, *, email: str, password: str) -> str:
    user = get_user_by_email(db, email)
    token = authenticate_user(user, password)
    if token is None:
        raise ValueError("Invalid credentials")
    return token
