from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app, get_publisher, get_reader


class FakePublisher:
    def __init__(self):
        self.events = []
        self.fail = False

    def publish(self, event):
        if self.fail:
            raise OSError("broker unavailable")
        self.events.append(event)
        return SimpleNamespace(partition=0, offset=42)


class FakeReader:
    def __init__(self):
        self.calls = []
        self.fail = False

    def _check(self):
        if self.fail:
            raise OSError("database unavailable")

    def events(self, limit, offset, event_type, category):
        self._check()
        self.calls.append((limit, offset, event_type, category))
        return [{
            "event_id": "15bb8b7a-d3b6-49b3-8891-bd4214d8c247",
            "user_id": "user-001", "product_id": "P001", "category": "Electronics",
            "event_type": "purchase", "price": Decimal("799.00"), "quantity": 2,
            "event_timestamp": datetime(2026, 11, 20, tzinfo=timezone.utc),
            "total_amount": Decimal("1598.00"),
        }]

    def window_stats(self, limit, offset, category):
        self._check()
        self.calls.append((limit, offset, category))
        return [{
            "window_start": datetime(2026, 11, 20, tzinfo=timezone.utc),
            "window_end": datetime(2026, 11, 20, 0, 5, tzinfo=timezone.utc),
            "category": "Electronics", "total_events": 2,
            "purchases": 1, "revenue": Decimal("1598.00"),
        }]

    def metrics(self):
        self._check()
        return {"total_events": 2, "purchases": 1, "revenue": Decimal("1598.00"),
                "categories": [{"category": "Electronics", "total_events": 2,
                                "purchases": 1, "revenue": Decimal("1598.00")}],
                "event_types": [{"event_type": "purchase", "total_events": 1}]}

    def consistency(self):
        self._check()
        return {"consistent": True, "events_total": 2, "windows_total": 2,
                "events_revenue": Decimal("1598.00"), "windows_revenue": Decimal("1598.00"),
                "duplicate_event_ids": 0, "event_count_difference": 0,
                "revenue_difference": Decimal("0.00")}


@pytest.fixture
def api():
    settings = Settings("kafka:9093", "ecommerce-events", "postgres-iteso", 5432,
                        "streaming_project", "streaming_user", None,
                        ("http://127.0.0.1:5173",))
    app = create_app(settings)
    publisher, reader = FakePublisher(), FakeReader()
    app.dependency_overrides[get_publisher] = lambda: publisher
    app.dependency_overrides[get_reader] = lambda: reader
    with TestClient(app) as client:
        yield client, publisher, reader


def test_health_is_liveness_only(api):
    client, publisher, reader = api
    assert client.get("/health").json() == {"status": "ok", "dependencies": "not_checked"}
    assert publisher.events == []
    assert reader.calls == []


def test_publish_queues_compatible_event_without_db_read(api):
    client, publisher, reader = api
    response = client.post("/api/v1/events", json={
        "user_id": "user-007", "product_id": "P001",
        "event_type": "purchase", "quantity": 2,
    })
    assert response.status_code == 202
    assert response.json()["status"] == "queued"
    assert response.json()["offset"] == 42
    event = publisher.events[0]
    assert UUID(str(event.event_id)).version == 4
    assert event.timestamp.tzinfo == timezone.utc
    assert (event.category, event.price, event.quantity) == ("Electronics", Decimal("799.00"), 2)
    assert reader.calls == []


def test_idempotency_key_reuses_uuid(api):
    client, publisher, _ = api
    body = {"user_id": "user-007", "product_id": "P001", "event_type": "view", "quantity": 1}
    headers = {"Idempotency-Key": "click-123"}
    a = client.post("/api/v1/events", json=body, headers=headers)
    b = client.post("/api/v1/events", json=body, headers=headers)
    assert a.status_code == b.status_code == 202
    assert a.json()["event_id"] == b.json()["event_id"]
    assert UUID(a.json()["event_id"]).version == 5
    assert len(publisher.events) == 2


@pytest.mark.parametrize("body", [
    {"user_id": "user-1", "product_id": "P999", "event_type": "view", "quantity": 1},
    {"user_id": "user-1", "product_id": "P001", "event_type": "view", "quantity": 2},
    {"user_id": "user-1", "product_id": "P001", "event_type": "purchase", "quantity": 0},
    {"user_id": "user-1", "product_id": "P001", "event_type": "refund", "quantity": 1},
    {"user_id": "bad user", "product_id": "P001", "event_type": "view", "quantity": 1},
])
def test_invalid_publish_never_calls_kafka(api, body):
    client, publisher, _ = api
    assert client.post("/api/v1/events", json=body).status_code == 422
    assert publisher.events == []


def test_kafka_failure_is_503_without_credential_leak(api):
    client, publisher, _ = api
    publisher.fail = True
    response = client.post("/api/v1/events", json={
        "user_id": "user-1", "product_id": "P001", "event_type": "view", "quantity": 1,
    })
    assert response.status_code == 503
    assert "broker unavailable" not in response.text


def test_reads_are_from_reader_only(api):
    client, publisher, reader = api
    events = client.get("/api/v1/events?limit=10&offset=3&event_type=purchase&category=Electronics")
    assert events.status_code == 200
    assert events.json()[0]["total_amount"] == "1598.00"
    assert reader.calls[-1] == (10, 3, "purchase", "Electronics")
    windows = client.get("/api/v1/window-stats?limit=5&category=Home")
    assert windows.status_code == 200
    assert reader.calls[-1] == (5, 0, "Home")
    metrics = client.get("/api/v1/metrics")
    assert metrics.status_code == 200
    assert metrics.json()["revenue"] == "1598.00"
    consistency = client.get("/api/v1/consistency")
    assert consistency.status_code == 200
    assert consistency.json()["consistent"] is True
    assert publisher.events == []


def test_db_failure_and_bad_pagination(api):
    client, _, reader = api
    reader.fail = True
    assert client.get("/api/v1/metrics").status_code == 503
    assert client.get("/api/v1/events?limit=501").status_code == 422


@pytest.mark.parametrize("name,value", [
    ("KAFKA_TOPIC", "ecommerce-events"),
    ("DB_HOST", "postgres-iteso"),
    ("DB_NAME", "streaming_project"),
])
def test_environment_refuses_historical_destinations(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match="Historical"):
        Settings.from_env()
