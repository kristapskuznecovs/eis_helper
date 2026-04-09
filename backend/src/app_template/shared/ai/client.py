from dataclasses import dataclass

from app_template.settings import get_settings


@dataclass
class AIClientConfig:
    provider: str
    model: str


def get_ai_client_config() -> AIClientConfig:
    settings = get_settings()
    return AIClientConfig(provider=settings.ai_provider, model=settings.ai_model)
