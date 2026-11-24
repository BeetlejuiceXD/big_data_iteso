"""Kafka writer. Construction is lazy; GET requests never contact Kafka."""
import json
import threading

from kafka import KafkaProducer

from .config import Settings
from .models import EventRecord


class KafkaPublisher:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._producer = None
        self._lock = threading.Lock()

    def _get_producer(self):
        if self._producer is None:
            with self._lock:
                if self._producer is None:
                    self._producer = KafkaProducer(
                        bootstrap_servers=self.settings.kafka_bootstrap_servers,
                        acks="all",
                        retries=0,
                        max_block_ms=5000,
                    )
        return self._producer

    def publish(self, event: EventRecord):
        payload = event.model_dump(mode="json")
        # Spark expects a JSON number for its DoubleType price field.
        payload["price"] = float(event.price)
        value = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        future = self._get_producer().send(
            self.settings.kafka_topic,
            key=event.user_id.encode("utf-8"),
            value=value,
        )
        return future.get(timeout=10)

    def close(self):
        if self._producer is not None:
            self._producer.close(timeout=5)
