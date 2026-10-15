from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# BASE_DIR = Path(__file__).resolve().parent


class EmptyBaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class AppSettings(EmptyBaseSettings):
    SERVICE_NAME: str = "Aggregators"
    ROOT_PATH: str = ""
    ALLOW_ORIGINS: str | None = None

    DEBUG: bool = False

    @property
    def allow_origins_list(self) -> list[str]:
        return self.ALLOW_ORIGINS.split(",") if self.ALLOW_ORIGINS else []


app_settings = AppSettings()
