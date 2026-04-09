from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    project_name: str = "app_template"
    environment: str = "development"
    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/app_template"
    web_origin: str = "http://localhost:3000"
    jwt_secret_key: str = "change-me"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 60
    auth_password_min_length: int = 8
    storage_backend: str = "local"
    storage_local_root: str = "./data/uploads"
    s3_bucket: str = "app-template"
    s3_endpoint_url: str | None = "http://localhost:9000"
    aws_access_key_id: str | None = "minioadmin"
    aws_secret_access_key: str | None = "minioadmin"
    ai_provider: str = "openai"
    ai_model: str = "gpt-4o-mini"
    openai_api_key: str | None = None
    jobs_backend: str = "inline"
    redis_url: str = "redis://localhost:6379/0"

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
