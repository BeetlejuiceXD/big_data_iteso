"""Isolated batch-level JDBC test; run only on trama-jdbc-isolated_default."""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, LongType,
)

from jdbc_sink import JdbcConfig, acquire_writer_lease, stage_batch, consolidate_batch


def main():
    config = JdbcConfig.from_env()
    assert config.database == "trama_jdbc_test"
    assert config.host == "db"
    lease = acquire_writer_lease(config)
    try:
        acquire_writer_lease(config)
        raise AssertionError("A second consumer acquired the writer lease")
    except RuntimeError as exc:
        assert "already holds" in str(exc)
    spark = SparkSession.builder.appName("Trama JDBC isolated integration") \
        .master("local[2]").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    event_id = str(uuid4())
    when = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("total_amount", DoubleType(), False),
    ])
    event_row = (event_id, "test-user", "P001", "Electronics", "purchase", 799.0, 1, when, 799.0)
    events = spark.createDataFrame([event_row], event_schema)

    # A failed partition write can leave a subset committed in staging. Retry
    # appends it again; the final INSERT must still create one event.
    stage_batch(events, 1, config, "events")
    stage_batch(events, 1, config, "events")
    assert consolidate_batch(1, config, "events") is True
    stage_batch(events, 1, config, "events")
    assert consolidate_batch(1, config, "events") is False
    changed_event = spark.createDataFrame([(event_id, "different-user", "P001", "Electronics", "purchase", 799.0, 1, when, 799.0)], event_schema)
    stage_batch(changed_event, 1, config, "events")
    try:
        consolidate_batch(1, config, "events")
        raise AssertionError("Changed committed event batch was accepted")
    except ValueError as exc:
        assert "previously stored" in str(exc)
    stage_batch(events, 2, config, "events")
    stage_batch(changed_event, 2, config, "events")
    try:
        consolidate_batch(2, config, "events")
        raise AssertionError("Conflicting event copies were accepted")
    except ValueError as exc:
        assert "Conflicting payloads" in str(exc)
    # A new microbatch may repeat the same Kafka event UUID. The final primary
    # key and ON CONFLICT DO NOTHING still keep exactly one persisted event.
    stage_batch(events, 4, config, "events")
    assert consolidate_batch(4, config, "events") is True
    stage_batch(changed_event, 5, config, "events")
    try:
        consolidate_batch(5, config, "events")
        raise AssertionError("Changed event_id in a later microbatch was accepted")
    except ValueError as exc:
        assert "previously stored" in str(exc)

    window_schema = StructType([
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("category", StringType(), False),
        StructField("total_events", LongType(), False),
        StructField("purchases", LongType(), False),
        StructField("revenue", DoubleType(), False),
    ])
    end = datetime(2026, 11, 20, 12, 5, tzinfo=timezone.utc)
    first = spark.createDataFrame([(when, end, "Electronics", 1, 1, 799.0)], window_schema)
    updated = spark.createDataFrame([(when, end, "Electronics", 2, 1, 799.0)], window_schema)
    stage_batch(first, 1, config, "windows")
    assert consolidate_batch(1, config, "windows") is True
    stage_batch(updated, 2, config, "windows")
    assert consolidate_batch(2, config, "windows") is True
    stage_batch(updated, 2, config, "windows")
    assert consolidate_batch(2, config, "windows") is False

    # Differing copies of one window key must roll back without changing it.
    stage_batch(first, 3, config, "windows")
    stage_batch(updated, 3, config, "windows")
    try:
        consolidate_batch(3, config, "windows")
        raise AssertionError("Conflicting window copies were accepted")
    except ValueError as exc:
        assert "Conflicting payloads" in str(exc)

    import psycopg2
    with psycopg2.connect(**config.connection_kwargs()) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT count(*), sum(total_amount) FROM events WHERE event_id = %s", (event_id,))
            assert cursor.fetchone() == (1, Decimal("799.00"))
            cursor.execute("SELECT total_events, purchases, revenue FROM window_stats WHERE category = 'Electronics'")
            # The staged Spark aggregate may count a repeated UUID; canonical
            # events remain the source of truth for the persisted window.
            assert cursor.fetchone() == (1, 1, Decimal("799.00"))
            cursor.execute("SELECT count(*) FROM jdbc_batch_commits WHERE stream_id LIKE %s", (config.namespace + ':%',))
            assert cursor.fetchone()[0] == 4
            cursor.execute("SELECT count(*) FROM jdbc_stage_events WHERE stream_id = %s AND batch_id = 1", (config.namespace + ':events',))
            assert cursor.fetchone()[0] == 1
            cursor.execute("SELECT count(*) FROM jdbc_stage_windows WHERE stream_id = %s AND batch_id = 3", (config.namespace + ':windows',))
            assert cursor.fetchone()[0] == 2
    print("PASS: Spark JDBC inserts, UPSERT, partial retry, committed-batch digest, conflict rollback, zero duplicate events")
    lease.close()
    spark.stop()


if __name__ == "__main__":
    main()
