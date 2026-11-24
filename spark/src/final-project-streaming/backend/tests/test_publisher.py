import json
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from app.config import Settings
from app.models import EventRecord
from app.publisher import KafkaPublisher


def test_kafka_wire_payload_uses_numeric_price_and_producer_fields(monkeypatch):
    captured = {}

    class FakeFuture:
        def get(self, timeout):
            captured["timeout"] = timeout
            return object()

    class FakeKafkaProducer:
        def __init__(self, **kwargs):
            captured["config"] = kwargs

        def send(self, topic, *, key, value):
            captured.update(topic=topic, key=key, value=value)
            return FakeFuture()

        def close(self, timeout):
            pass

    monkeypatch.setattr("app.publisher.KafkaProducer", FakeKafkaProducer)
    settings = Settings("localhost:9092", "ecommerce-events", "unused", 5432,
                        "unused", "unused", None, ())
    publisher = KafkaPublisher(settings)
    event = EventRecord(event_id=uuid4(), user_id="user-001", product_id="P001",
                        category="Electronics", event_type="purchase",
                        price=Decimal("799.00"), quantity=2,
                        timestamp=datetime.now(timezone.utc))
    publisher.publish(event)
    payload = json.loads(captured["value"])
    assert captured["topic"] == "ecommerce-events"
    assert captured["key"] == b"user-001"
    assert set(payload) == {"event_id", "user_id", "product_id", "category",
                            "event_type", "price", "quantity", "timestamp"}
    assert payload["price"] == 799.0
    assert isinstance(payload["price"], float)
    assert payload["timestamp"].endswith("Z")
    assert captured["config"]["acks"] == "all"
