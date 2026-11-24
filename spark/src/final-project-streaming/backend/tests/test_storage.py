from app.config import Settings
from app.storage import PostgresReader


def test_reader_uses_read_only_transaction_and_closes_connection(monkeypatch):
    calls = []

    class Cursor:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            calls.append("cursor_closed")
        def execute(self, query, params):
            assert query.lstrip().startswith("SELECT")
            assert params == (None, None, None, None, 5, 0)
            calls.append("select")
        def fetchall(self):
            return []

    class Connection:
        def set_session(self, **kwargs):
            calls.append(("session", kwargs))
        def cursor(self, **kwargs):
            return Cursor()
        def rollback(self):
            calls.append("rollback")
        def close(self):
            calls.append("close")

    monkeypatch.setattr("app.storage.psycopg2.connect", lambda **kwargs: Connection())
    settings = Settings("unused", "unused", "postgres-iteso", 5432,
                        "streaming_project", "streaming_user", "test-only", ())
    assert PostgresReader(settings).events(5, 0, None, None) == []
    assert calls == [
        ("session", {"readonly": True, "isolation_level": "REPEATABLE READ"}),
        "select", "cursor_closed", "rollback", "close",
    ]
