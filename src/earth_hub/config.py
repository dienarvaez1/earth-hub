"""Typed application settings loaded from the environment and an optional .env file."""

import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "Earth-Hub"
    app_env: Literal["development", "test", "production"] = "development"
    api_port: int = Field(default=8000, ge=1, le=65535)

    postgres_host: str = "localhost"
    postgres_port: int = Field(default=5433, ge=1, le=65535)
    postgres_db: str = Field(min_length=1)
    postgres_user: str = Field(min_length=1)
    postgres_password: SecretStr

    kafka_bootstrap_servers: str = Field(default="127.0.0.1:29092", min_length=1)
    kafka_topic_raw: str = Field(default="earthquakes.raw", min_length=1)

    usgs_feed_url: HttpUrl = HttpUrl(
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
    )
    poll_interval_seconds: int = Field(default=60, ge=10)
    poller_heartbeat_path: Path = Path(tempfile.gettempdir()) / "earth-hub-poller.heartbeat"
    consumer_heartbeat_path: Path = Path(tempfile.gettempdir()) / "earth-hub-consumer.heartbeat"
    kafka_consumer_group: str = Field(default="earth-hub-consumer", min_length=1)
    # Which topic this consumer process reads: the live topic, or the backfill topic for the
    # separate backfill consumer, so live events never queue behind historical ones.
    kafka_consumer_topic: str | None = None
    consumer_batch_size: int = Field(default=500, ge=1, le=5000)

    # Backfill and catch-up publish here; the live poller never reads it back.
    kafka_topic_backfill: str = Field(default="earthquakes.backfill", min_length=1)
    kafka_backfill_consumer_group: str = Field(default="earth-hub-consumer-backfill", min_length=1)
    fdsn_url: HttpUrl = HttpUrl("https://earthquake.usgs.gov/fdsnws/event/1")
    usgs_month_feed_url: HttpUrl = HttpUrl(
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    )
    fdsn_pause_seconds: float = Field(default=1.0, ge=0.0, le=60.0)
    catchup_interval_seconds: int = Field(default=3600, ge=300)
    deleted_check_interval_seconds: int = Field(default=86400, ge=3600)
    catchup_heartbeat_path: Path = Path(tempfile.gettempdir()) / "earth-hub-catchup.heartbeat"
    # Optional contact sent in the User-Agent so USGS can reach the operator, e.g. a repo URL.
    usgs_contact: str = Field(default="", max_length=200)

    @property
    def consumer_topic(self) -> str:
        return self.kafka_consumer_topic or self.kafka_topic_raw

    def database_url(self, database: str | None = None) -> URL:
        """SQLAlchemy URL built from the POSTGRES_* parts. str() masks the password."""
        return URL.create(
            "postgresql+psycopg",
            username=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            host=self.postgres_host,
            port=self.postgres_port,
            database=database or self.postgres_db,
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
