from sqlalchemy.orm import Session

from app_template.modules.users.models import User


def get_user_by_email(db: Session, email: str) -> User | None:
    return db.query(User).filter(User.email == email).first()
