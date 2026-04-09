from app_template.modules.users.models import User
from app_template.shared.auth.hashing import hash_password, verify_password
from app_template.shared.auth.jwt import create_access_token


def build_password_hash(password: str) -> str:
    return hash_password(password)


def authenticate_user(user: User | None, password: str) -> str | None:
    if user is None:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return create_access_token(user.email)
