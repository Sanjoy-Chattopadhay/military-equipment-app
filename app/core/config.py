"""Configuration settings for the application"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database settings
    DB_NAME: str = "neondb"
    DB_USER: str = "neondb_owner"
    DB_PASSWORD: str = "npg_t70imvFJbTOW"
    DB_HOST: str = "ep-wild-glade-adu0fglb-pooler.c-2.us-east-1.aws.neon.tech"
    DB_PORT: str = "5432"
    DB_SSL_MODE: str = "require"

    # API settings
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Vehicle Management System"

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}?sslmode={self.DB_SSL_MODE}"

    class Config:
        env_file = ".env"


settings = Settings()
