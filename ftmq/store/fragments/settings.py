from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_uri: str = Field(
        default="sqlite:///:memory:",
        validation_alias=AliasChoices("ftm_store_uri", "fragments_uri"),
    )
