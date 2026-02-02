from __future__ import annotations

from typing import List, Any

from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

PROJECT_ROOT = Path(__file__).parent.parent.parent


class Settings(BaseSettings):
    """
    Central configuration.

    Mirrors the style of the user_management service:
    - Values loaded from `.env`
    - Comma-separated lists for multi-value settings like CORS_ORIGINS
    """

    # ----------------------------
    # Service
    # ----------------------------
    SERVICE_NAME: str = "labelling-task-service"
    ENVIRONMENT: str = "development"

    # ----------------------------
    # Mongo
    # ----------------------------
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "test"

    # ----------------------------
    # Redis
    # ----------------------------
    redis_url: str = "redis://localhost:6379/0"
    redis_stream_tasks: str = "lt:stream:tasks"
    redis_stream_reviews: str = "lt:stream:reviews"

    # ----------------------------
    # OAuth / JWKS
    # ----------------------------
    JWKS_URL: str = "http://localhost:5055/oauth2/jwks"
    JWKS_CACHE_TTL: int = 300  # 5 minutes
    CLOCK_SKEW_SECONDS: int = 60  # industry standard (1 min)

    # ----------------------------
    # CORS
    # ----------------------------
    # store as raw string list from env; we will normalize in code
    CORS_ORIGINS: Any = Field(default_factory=list)

    # ----------------------------
    # S3
    # ----------------------------
    s3_bucket: str = "labelling-task"
    s3_region: str = "us-east-1"

    # ----------------------------
    # JWT
    # ----------------------------
    jwt_alg: str = "HS256"  # HS256 (secret) or RS256 (JWKS)
    jwt_secret: str = "change-me"
    jwt_audience: str | None = None
    jwt_issuer: str | None = None
    jwt_jwks_url: str | None = None  # if set, fetch keys (not enabled in sandbox)

    # ----------------------------
    # Retention
    # ----------------------------
    deleted_retention_days: int = 90
    
    # Pydantic settings config (v2 style)
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
