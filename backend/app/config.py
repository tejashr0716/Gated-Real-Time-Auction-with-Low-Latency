import os
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    APP_NAME: str = "Gated Real-Time Auction Engine"
    DEBUG: bool = False
    PORT: int = 8000
    HOST: str = "0.0.0.0"

    # PostgreSQL Connection
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "auction_db")

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @property
    def ASYNC_DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    # Redis Connection
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD", None)
    REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "2000"))

    # Security & Gated Auction Token
    DEMO_MODE: bool = os.getenv("DEMO_MODE", "true").lower() in ("true", "1")
    SECRET_KEY: str = os.getenv("SECRET_KEY", "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440 # 24 hours

    DEFAULT_INSECURE_SECRET: str = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"

    def validate_security_configuration(self):
        """
        Enforces secure cryptographic signing secrets in non-demo / production environments.
        """
        if not self.DEMO_MODE:
            if self.SECRET_KEY == self.DEFAULT_INSECURE_SECRET or len(self.SECRET_KEY) < 32:
                raise RuntimeError(
                    "INSECURE_CONFIGURATION: In production mode (DEMO_MODE=false), a securely configured "
                    "SECRET_KEY (min 32 characters) must be provided via environment variable."
                )

    # Telemetry Defaults
    TELEMETRY_SAMPLE_RATE_MS: int = 1000
    MAX_WS_CLIENTS_PER_NODE: int = 5000

    model_config = {
        "env_file": ".env",
        "extra": "allow"
    }

settings = Settings()
