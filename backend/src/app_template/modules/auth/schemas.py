import re

from pydantic import BaseModel, EmailStr, field_validator

from app_template.settings import get_settings


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str

    @field_validator("password")
    @classmethod
    def validate_password_strength(cls, value: str) -> str:
        minimum = get_settings().auth_password_min_length
        if len(value) < minimum:
            raise ValueError(f"Password must be at least {minimum} characters")
        if not re.search(r"[A-Z]", value):
            raise ValueError("Password must contain an uppercase letter")
        if not re.search(r"[0-9]", value):
            raise ValueError("Password must contain a digit")
        return value


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
