from pydantic import BaseModel, EmailStr


class UserRead(BaseModel):
    id: int
    email: EmailStr

    model_config = {"from_attributes": True}
