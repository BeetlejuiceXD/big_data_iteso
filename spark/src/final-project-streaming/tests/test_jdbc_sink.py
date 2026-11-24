"""No Kafka, Docker, Spark, or PostgreSQL connection is made by these tests."""

from pathlib import Path
import os
import sys
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import jdbc_sink


def config():
    return jdbc_sink.JdbcConfig(
        "jdbc-test-db", 5432, "trama_jdbc_test", "test_user", "test-only",
        "isolated-run-001", "/tmp/trama-jdbc-checkpoints", "test-kafka:9093",
        "ecommerce-jdbc-test", 2, 25,
    )


class FakeCursor:
    def __init__(self, *, committed=False, conflicting=False, fail_merge=False, lease_granted=True, changed=False):
        self.statements = []
        self.committed = committed
        self.conflicting = conflicting
        self.fail_merge = fail_merge
        self.lease_granted = lease_granted
        self.changed = changed
        self.last_sql = ""

    def __enter__(self): return self
    def __exit__(self, *_): pass

    def execute(self, sql, params):
        self.last_sql = sql
        self.statements.append((sql, params))
        if self.fail_merge and "INSERT INTO events (" in sql:
            raise RuntimeError("injected failure before commit")

    def fetchone(self):
        if "pg_try_advisory_lock" in self.last_sql:
            return (self.lease_granted,)
        if "FROM jdbc_batch_commits" in self.last_sql:
            return (1, "old-digest" if self.changed else "digest") if self.committed else None
        if "AS distinct_rows" in self.last_sql:
            return ("duplicate key",) if self.conflicting else None
        if "AS d" in self.last_sql:
            return (1, "digest")
        return None


class FakeConnection:
    def __init__(self, cursor):
        self.fake_cursor = cursor
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def __enter__(self): return self
    def __exit__(self, exc_type, *_):
        if exc_type: self.rollbacks += 1
        else: self.commits += 1

    def cursor(self): return self.fake_cursor
    def close(self): self.closed = True


class FakeWriter:
    def __init__(self): self.calls = []
    def format(self, value): self.calls.append(("format", value)); return self
    def mode(self, value): self.calls.append(("mode", value)); return self
    def option(self, key, value): self.calls.append((key, value)); return self
    def save(self): self.calls.append(("save", None))


class FakeFrame:
    def __init__(self): self.write = FakeWriter(); self.columns = []
    def select(self, *columns): self.columns = columns; return self
    def withColumn(self, *_): return self


class SinkTests(unittest.TestCase):
    def test_rejects_historical_targets_and_original_checkpoints(self):
        values = {
            "JDBC_DB_HOST": "jdbc-test-db", "JDBC_DB_NAME": "streaming_project",
            "JDBC_DB_USER": "test_user", "JDBC_DB_PASSWORD": "test-only",
            "JDBC_STREAM_NAMESPACE": "isolated-run", "JDBC_CHECKPOINT_ROOT": "/tmp/jdbc",
            "JDBC_KAFKA_BROKER": "test-kafka:9093", "JDBC_KAFKA_TOPIC": "test-topic",
        }
        with patch.dict(os.environ, values, clear=True):
            with self.assertRaisesRegex(ValueError, "historical"):
                jdbc_sink.JdbcConfig.from_env()
            os.environ["JDBC_DB_NAME"] = "trama_jdbc_test"
            os.environ["JDBC_KAFKA_TOPIC"] = "ecommerce-events"
            with self.assertRaisesRegex(ValueError, "historical"):
                jdbc_sink.JdbcConfig.from_env()
            os.environ["JDBC_KAFKA_TOPIC"] = "test-topic"
            os.environ["JDBC_DB_HOST"] = "postgres-iteso"
            with self.assertRaisesRegex(ValueError, "historical"):
                jdbc_sink.JdbcConfig.from_env()
            os.environ["JDBC_DB_HOST"] = "jdbc-test-db"
            os.environ["JDBC_CHECKPOINT_ROOT"] = "/opt/project/checkpoints/events"
            with self.assertRaisesRegex(ValueError, "checkpoint"):
                jdbc_sink.JdbcConfig.from_env()

    def test_final_target_requires_explicit_opt_in_and_exact_offsets(self):
        values = {
            "JDBC_DB_HOST": "trama-postgres-final", "JDBC_DB_NAME": "trama_commerce_final",
            "JDBC_DB_USER": "trama_final_user", "JDBC_DB_PASSWORD": "test-only",
            "JDBC_STREAM_NAMESPACE": "new-cutover", "JDBC_CHECKPOINT_ROOT": "/tmp/jdbc-new",
            "JDBC_KAFKA_BROKER": "kafka:9093", "JDBC_KAFKA_TOPIC": "trama-commerce-final-events",
        }
        with patch.dict(os.environ, values, clear=True):
            with self.assertRaisesRegex(ValueError, "explicit opt-in"):
                jdbc_sink.JdbcConfig.from_env()
            os.environ["JDBC_ALLOW_FINAL_DEMO"] = "I_ACCEPT_NEW_DATABASE_WRITES"
            with self.assertRaisesRegex(ValueError, "exact offsets"):
                jdbc_sink.JdbcConfig.from_env()
            for invalid in ('latest', 'earliest', '{"trama-commerce-final-events":{"0":-1}}',
                            '{"other":{"0":7}}'):
                os.environ["JDBC_STARTING_OFFSETS"] = invalid
                with self.assertRaises(ValueError):
                    jdbc_sink.JdbcConfig.from_env()
            os.environ["JDBC_STARTING_OFFSETS"] = '{"trama-commerce-final-events":{"0":7,"1":12}}'
            self.assertEqual(jdbc_sink.JdbcConfig.from_env().starting_offsets,
                             os.environ["JDBC_STARTING_OFFSETS"])
            os.environ["JDBC_DB_NAME"] = "streaming_project"
            with self.assertRaisesRegex(ValueError, "historical"):
                jdbc_sink.JdbcConfig.from_env()

    def test_windows_reconcile_from_canonical_events_and_retries_allow_new_timestamp(self):
        self.assertIn("FROM affected a JOIN events e", jdbc_sink.WINDOW_FROM_EVENTS)
        self.assertIn("FROM inserted", jdbc_sink.EVENT_MERGE)
        self.assertIn("ON CONFLICT (event_id) DO NOTHING", jdbc_sink.EVENT_MERGE)
        self.assertNotIn("event_timestamp", jdbc_sink.EVENT_CONFLICT)
        self.assertIn("IS DISTINCT FROM", jdbc_sink.EVENT_EXISTING_CONFLICT)
        for sink in ("events", "windows"):
            cursor = FakeCursor()
            jdbc_sink.consolidate_batch(7, config(), sink, lambda **_: FakeConnection(cursor))
            statements = "\n".join(sql for sql, _ in cursor.statements)
            self.assertIn(":window-reconcile", str(cursor.statements))
            self.assertIn("FROM affected a JOIN events e", statements)

    def test_spark_writer_uses_jdbc_without_collecting_rows(self):
        fake_functions = types.SimpleNamespace(lit=lambda value: types.SimpleNamespace(cast=lambda _: value))
        with patch.dict(sys.modules, {"pyspark": types.ModuleType("pyspark"),
                                      "pyspark.sql": types.SimpleNamespace(functions=fake_functions)}):
            frame = FakeFrame()
            stream = jdbc_sink.stage_batch(frame, 7, config(), "events")
        self.assertEqual(stream, "isolated-run-001:events")
        self.assertEqual(frame.columns, jdbc_sink.EVENT_COLUMNS)
        self.assertIn(("format", "jdbc"), frame.write.calls)
        self.assertIn(("dbtable", "jdbc_stage_events"), frame.write.calls)
        self.assertIn(("numPartitions", "2"), frame.write.calls)
        self.assertIn(("batchsize", "25"), frame.write.calls)
        self.assertIn(("save", None), frame.write.calls)

    def test_only_one_consumer_can_hold_database_lease(self):
        granted = FakeConnection(FakeCursor())
        self.assertIs(jdbc_sink.acquire_writer_lease(config(), lambda **_: granted), granted)
        self.assertTrue(granted.autocommit)
        denied = FakeConnection(FakeCursor(lease_granted=False))
        with self.assertRaisesRegex(RuntimeError, "already holds"):
            jdbc_sink.acquire_writer_lease(config(), lambda **_: denied)
        self.assertTrue(denied.closed)

    def test_commit_is_atomic_and_second_attempt_skips_merge(self):
        for already_committed, expected in [(False, True), (True, False)]:
            cursor = FakeCursor(committed=already_committed)
            conn = FakeConnection(cursor)
            result = jdbc_sink.consolidate_batch(7, config(), "events", lambda **_: conn)
            sql = "\n".join(statement for statement, _ in cursor.statements)
            self.assertEqual(result, expected)
            self.assertEqual(conn.commits, 1)
            self.assertEqual(conn.rollbacks, 0)
            self.assertTrue(conn.closed)
            self.assertIn("DELETE FROM jdbc_stage_events", sql)
            self.assertEqual("INSERT INTO events (" in sql, not already_committed)
            self.assertEqual("INSERT INTO jdbc_batch_commits" in sql, not already_committed)

    def test_conflicting_copies_abort_before_final_insert(self):
        cursor = FakeCursor(conflicting=True)
        conn = FakeConnection(cursor)
        with self.assertRaisesRegex(ValueError, "Conflicting payloads"):
            jdbc_sink.consolidate_batch(7, config(), "windows", lambda **_: conn)
        self.assertEqual(conn.rollbacks, 1)
        self.assertNotIn("INSERT INTO window_stats", "\n".join(s for s, _ in cursor.statements))

    def test_committed_batch_with_changed_payload_is_rejected(self):
        cursor = FakeCursor(committed=True, changed=True)
        conn = FakeConnection(cursor)
        with self.assertRaisesRegex(ValueError, "Changed payload"):
            jdbc_sink.consolidate_batch(7, config(), "events", lambda **_: conn)
        self.assertEqual(conn.rollbacks, 1)
        self.assertNotIn("DELETE FROM jdbc_stage_events", "\n".join(s for s, _ in cursor.statements))

    def test_partial_stage_or_merge_failure_cannot_mark_batch_complete(self):
        with patch.object(jdbc_sink, "stage_batch", side_effect=RuntimeError("partial JDBC write")):
            with patch.object(jdbc_sink, "consolidate_batch") as merge:
                with self.assertRaisesRegex(RuntimeError, "partial JDBC"):
                    jdbc_sink.write_batch(FakeFrame(), 7, config(), "events")
                merge.assert_not_called()
        cursor = FakeCursor(fail_merge=True)
        conn = FakeConnection(cursor)
        with self.assertRaisesRegex(RuntimeError, "injected failure"):
            jdbc_sink.consolidate_batch(7, config(), "events", lambda **_: conn)
        self.assertEqual(conn.rollbacks, 1)
        self.assertNotIn("INSERT INTO jdbc_batch_commits", "\n".join(s for s, _ in cursor.statements))

    def test_pipeline_transformations_match_original(self):
        root = Path(__file__).resolve().parents[1]
        original = (root / "tests/fixtures/consumer_psycopg2_original.py").read_text()
        alternative = (root / "consumer.py").read_text()
        start, end = "# 4. Interpretar los eventos JSON", "# 9. Guardar eventos individuales"
        self.assertEqual(original.split(start, 1)[1].split(end, 1)[0],
                         alternative.split(start, 1)[1].split(end, 1)[0])


if __name__ == "__main__":
    unittest.main()
