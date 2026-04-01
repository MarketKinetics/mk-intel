from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    # API
    app_name: str = "MK Intel API"
    app_version: str = "0.1.0"
    debug: bool = False

    # Redis + Celery
    redis_url: str = "redis://localhost:6379/0"

    # Anthropic
    anthropic_api_key: str

    # Admin
    admin_key: str = "change-me-in-env"

    # Paths
    project_root: Path = Path(__file__).resolve().parents[1]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
