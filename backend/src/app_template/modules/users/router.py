from fastapi import APIRouter, Depends

from app_template.modules.users.schemas import UserRead
from app_template.shared.auth.dependencies import get_current_user

router = APIRouter(prefix="/api/users", tags=["users"])


@router.get("/me", response_model=UserRead)
def read_current_user(current_user=Depends(get_current_user)) -> UserRead:
    return UserRead.model_validate(current_user)
