from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_db: str = "cinema"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    postgres_dsn: str = "postgresql://cinema:cinema_pass@localhost:5432/cinema_metrics"

    minio_endpoint: str = "localhost:9002"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "movie-analytics"

    aggregation_schedule_seconds: int = 300
    export_schedule_seconds: int = 3600

    class Config:
        env_file = ".env"


settings = Settings()
