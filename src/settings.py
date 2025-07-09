# src/settings.py
import pathlib
from pydantic_settings import BaseSettings, SettingsConfigDict

# Define the root of your project
# __file__ -> /path/to/project/src/settings.py
# .parent -> /path/to/project/src
# .parent -> /path/to/project
PROJECT_ROOT = pathlib.Path(__file__).parent.parent


class Settings(BaseSettings):
    """Loads configuration from environment variables."""
    DATABASE_URL: str

    model_config = SettingsConfigDict(
        # Use the absolute path to the .env file
        env_file=PROJECT_ROOT / '.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )


settings = Settings()
