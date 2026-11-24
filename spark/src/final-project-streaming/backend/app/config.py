"""Configuration read from environment; importing the app never requires secrets."""
from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str | None
    cors_origins: tuple[str, ...]

    @classmethod
    def from_env(cls) -> "Settings":
        settings = cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "trama-commerce-final-events"),
            db_host=os.getenv("DB_HOST", "trama-postgres-final"),
            db_port=int(os.getenv("DB_PORT", "5432")),
            db_name=os.getenv("DB_NAME", "trama_commerce_final"),
            db_user=os.getenv("DB_USER", "trama_final_user"),
            db_password=os.getenv("DB_PASSWORD") or None,
            cors_origins=tuple(
                item.strip() for item in os.getenv(
                    "CORS_ORIGINS", "http://127.0.0.1:5173,http://localhost:5173"
                ).split(",") if item.strip()
            ),
        )
        if (settings.kafka_topic == "ecommerce-events" or
                settings.db_host == "postgres-iteso" or
                settings.db_name == "streaming_project"):
            raise ValueError("Historical Commerce Lab targets are not allowed in the final API")
        return settings
