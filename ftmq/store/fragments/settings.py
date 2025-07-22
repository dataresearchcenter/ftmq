from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_uri: str = Field(
        default="sqlite:///ftm_fragments.db", alias="ftm_fragments_uri"
    )
