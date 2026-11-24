"""Exercise real FastAPI handlers against the disposable Kafka and PostgreSQL."""
import sys

from fastapi.testclient import TestClient

from backend.app.config import Settings
from backend.app.main import create_app


def main(mode):
    app = create_app(Settings.from_env())
    with TestClient(app) as client:
        if mode == "purchase":
            payload = {"user_id": "react-contract-test", "product_id": "P001",
                       "event_type": "purchase", "quantity": 1}
            headers = {"Idempotency-Key": "migration-react-purchase-001"}
            first = client.post("/api/v1/events", json=payload, headers=headers)
            second = client.post("/api/v1/events", json=payload, headers=headers)
            assert first.status_code == second.status_code == 202, (first.text, second.text)
            assert first.json()["event_id"] == second.json()["event_id"]
            assert first.json()["status"] == "queued"
            print("API_PURCHASE_UUID=" + first.json()["event_id"])
        else:
            metrics = client.get("/api/v1/metrics")
            events = client.get("/api/v1/events?limit=100")
            windows = client.get("/api/v1/window-stats?limit=100")
            consistency = client.get("/api/v1/consistency")
            assert all(r.status_code == 200 for r in (metrics, events, windows, consistency))
            assert metrics.json()["total_events"] == 4, metrics.json()
            assert metrics.json()["purchases"] == 2, metrics.json()
            assert metrics.json()["revenue"] == "1248.00", metrics.json()
            assert len(events.json()) == 4
            assert sum(row["total_events"] for row in windows.json()) == 4
            assert consistency.json()["consistent"] is True, consistency.json()
            print("API_FINAL=" + str(metrics.json()) + " CONSISTENCY=" + str(consistency.json()))


if __name__ == "__main__":
    main(sys.argv[1])
