"""Configuration de l'application, chargee depuis les variables d'environnement."""

from functools import lru_cache
from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    """Parametres de l'application, miroir de `.env.example`."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://user:pass@localhost:5432/budgetcitoyen"
    redis_url: str = "redis://localhost:6379/0"
    data_economie_base_url: str = "https://www.data.economie.gouv.fr"
    data_gouv_base_url: str = "https://www.data.gouv.fr"
    # NoDecode: empeche pydantic-settings de tenter un decodage JSON de la variable
    # d'environnement avant validation, afin de pouvoir parser un simple CSV.
    cors_origins: Annotated[list[str], NoDecode] = ["http://localhost:5173"]
    env: str = "development"

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _parse_cors_origins(cls, value: object) -> object:
        """Autorise la valeur `.env` sous forme de CSV: "a,b,c"."""
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value


@lru_cache
def get_settings() -> Settings:
    """Retourne une instance mise en cache des parametres de l'application."""
    return Settings()
