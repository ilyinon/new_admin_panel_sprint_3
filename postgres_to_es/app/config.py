import logging
import os

from pydantic_settings import BaseSettings, SettingsConfigDict

DOTENV = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))


logging.basicConfig(level=logging.INFO)


class EtlSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=DOTENV)

    BATCH_SIZE: int
    DELAY_BETWEEN_LOADS: int

    ELASTIC_SCHEMA_PATH: str
    ELASTIC_HOST: str
    ELASTIC_PORT: int
    ELASTIC_SHEMA: str
    ELASTIC_INDEX: str

    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int

    REDIS_HOST: str
    REDIS_PORT: int

    @property
    def elastic_url(self):
        return f'http://{self.ELASTIC_HOST}:{self.ELASTIC_PORT}'


settings = EtlSettings()
