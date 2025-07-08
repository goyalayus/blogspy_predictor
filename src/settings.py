from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Loads configuration from environment variables."""
    DATABASE_URL: str

    model_config = SettingsConfigDict(env_file='../.env', env_file_encoding='utf-8', extra='ignore')

settings = Settings()
