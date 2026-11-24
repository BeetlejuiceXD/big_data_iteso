"""Synthetic event-time windows for an isolated old-to-JDBC handoff."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from uuid import uuid4


def main(destination):
    directory = Path(destination)
    directory.mkdir(parents=True, exist_ok=True)
    boundary = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    boundary -= timedelta(minutes=boundary.minute % 5 + 20)
    original_id, new_id = str(uuid4()), str(uuid4())

    def event(event_id, minute, kind, product="P001", category="Electronics", price=799, quantity=1):
        return {"event_id": event_id, "user_id": "migration-test-user", "product_id": product,
                "category": category, "event_type": kind, "price": price,
                "quantity": quantity, "timestamp": (boundary + timedelta(minutes=minute)).isoformat()}

    groups = {
        "old-first": [event(original_id, 0, "purchase"), event(str(uuid4()), 6, "view")],
        "old-late": [event(str(uuid4()), 1, "add_to_cart")],
        "new-first": [event(new_id, 10, "purchase", "P006", "Home", 449),
                      event(str(uuid4()), 11, "view", "P006", "Home", 449)],
        # The same UUID is re-published with a newer timestamp, as FastAPI may do on retry.
        "new-retry": [event(new_id, 15, "purchase", "P006", "Home", 449)],
        # A genuinely new late event can change a historical window.
        "new-late": [event(str(uuid4()), 1, "view")],
    }
    for name, events in groups.items():
        (directory / f"{name}.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    (directory / "expected.json").write_text(json.dumps({"old_id": original_id, "new_id": new_id,
                                                          "boundary": boundary.isoformat()}))


if __name__ == "__main__":
    main(sys.argv[1])
