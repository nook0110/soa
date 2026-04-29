from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    kafka_topic: str = "movie-events"
    generator_interval_seconds: float = 2.0
    generator_enabled: bool = True

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
