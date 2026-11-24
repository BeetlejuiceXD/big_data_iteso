"""Generate deterministic counts with unique UUIDs for an isolated Kafka topic."""

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from uuid import uuid4


def main(directory):
    destination = Path(directory)
    destination.mkdir(parents=True, exist_ok=True)
    # The same event-time window is used before and after the forced restart.
    timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
    rows = [
        ("P001", "Electronics", "view", 799.0, 1),
        ("P001", "Electronics", "add_to_cart", 799.0, 1),
        ("P001", "Electronics", "purchase", 799.0, 2),
        ("P006", "Home", "view", 449.0, 1),
        ("P006", "Home", "add_to_cart", 449.0, 1),
        ("P006", "Home", "purchase", 449.0, 1),
        ("P001", "Electronics", "view", 799.0, 1),
        ("P006", "Home", "add_to_cart", 449.0, 1),
        ("P006", "Home", "purchase", 449.0, 2),
    ]
    for name, selected in (("initial.jsonl", rows[:6]), ("after-restart.jsonl", rows[6:])):
        with (destination / name).open("w") as output:
            for index, (product, category, event_type, price, quantity) in enumerate(selected):
                record = {
                    "event_id": str(uuid4()), "user_id": f"jdbc-e2e-{name[0]}-{index}",
                    "product_id": product, "category": category,
                    "event_type": event_type, "price": price, "quantity": quantity,
                    "timestamp": timestamp,
                }
                output.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    main(sys.argv[1])
