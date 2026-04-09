import pytest

from app_template.modules.auth.schemas import RegisterRequest


def test_register_password_requires_minimum_length() -> None:
    with pytest.raises(ValueError):
        RegisterRequest(email="user@example.com", password="Abc12")


def test_register_password_requires_uppercase() -> None:
    with pytest.raises(ValueError):
        RegisterRequest(email="user@example.com", password="secure123")


def test_register_password_requires_digit() -> None:
    with pytest.raises(ValueError):
        RegisterRequest(email="user@example.com", password="SecurePass")


def test_register_password_accepts_strong_password() -> None:
    payload = RegisterRequest(email="user@example.com", password="Secure123")
    assert payload.email == "user@example.com"
