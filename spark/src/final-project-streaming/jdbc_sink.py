"""Distributed Spark JDBC staging and transactional PostgreSQL consolidation.

No DataFrame rows are collected in Python. psycopg2 executes only bounded
control queries and set-based SQL after Spark JDBC finishes a batch write.
"""

from contextlib import closing
from dataclasses import dataclass
import json
import os
from pathlib import Path
import time

EVENT_COLUMNS = (
    "event_id", "user_id", "product_id", "category", "event_type", "price",
    "quantity", "event_timestamp", "total_amount",
)
WINDOW_COLUMNS = (
    "window_start", "window_end", "category", "total_events", "purchases", "revenue",
)

EVENT_CONFLICT = """
    SELECT event_id FROM (
        SELECT DISTINCT event_id::uuid AS event_id, user_id, product_id,
            category, event_type, price, quantity, total_amount
        FROM jdbc_stage_events WHERE stream_id = %s AND batch_id = %s
    ) AS distinct_rows
    GROUP BY event_id HAVING COUNT(*) > 1 LIMIT 1
"""
EVENT_EXISTING_CONFLICT = """
    SELECT s.event_id FROM jdbc_stage_events s
    JOIN events e ON e.event_id = s.event_id::uuid
    WHERE s.stream_id = %s AND s.batch_id = %s
      AND (e.user_id, e.product_id, e.category, e.event_type,
           e.price, e.quantity, e.total_amount)
          IS DISTINCT FROM
          (s.user_id, s.product_id, s.category, s.event_type,
           s.price, s.quantity, s.total_amount)
    LIMIT 1
"""
WINDOW_CONFLICT = """
    SELECT window_start, window_end, category FROM (
        SELECT DISTINCT window_start, window_end, category,
            total_events, purchases, revenue
        FROM jdbc_stage_windows WHERE stream_id = %s AND batch_id = %s
    ) AS distinct_rows
    GROUP BY window_start, window_end, category
    HAVING COUNT(*) > 1 LIMIT 1
"""
EVENT_DIGEST = """
    SELECT COUNT(*), md5(COALESCE(
        string_agg(md5(row_to_json(d)::text), '' ORDER BY event_id), ''))
    FROM (
        SELECT DISTINCT event_id::uuid AS event_id, user_id, product_id,
            category, event_type, price, quantity, event_timestamp, total_amount
        FROM jdbc_stage_events WHERE stream_id = %s AND batch_id = %s
    ) AS d
"""
WINDOW_DIGEST = """
    SELECT COUNT(*), md5(COALESCE(
        string_agg(md5(row_to_json(d)::text), ''
            ORDER BY window_start, window_end, category), ''))
    FROM (
        SELECT DISTINCT window_start, window_end, category,
            total_events, purchases, revenue
        FROM jdbc_stage_windows WHERE stream_id = %s AND batch_id = %s
    ) AS d
"""
EVENT_MERGE = """
    CREATE TEMP TABLE jdbc_affected_windows ON COMMIT DROP AS
    WITH inserted AS (
        INSERT INTO events (event_id, user_id, product_id, category, event_type,
            price, quantity, event_timestamp, total_amount)
        SELECT DISTINCT ON (event_id::uuid) event_id::uuid, user_id, product_id,
            category, event_type, price, quantity, event_timestamp, total_amount
        FROM jdbc_stage_events WHERE stream_id = %s AND batch_id = %s
        ORDER BY event_id::uuid, event_timestamp
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_timestamp, category
    )
    SELECT DISTINCT date_bin('5 minutes', event_timestamp,
        TIMESTAMPTZ '1970-01-01 00:00:00+00') AS window_start, category
    FROM inserted
"""
WINDOW_FROM_EVENTS = """
    WITH affected AS (
        SELECT DISTINCT window_start, window_start + INTERVAL '5 minutes' AS window_end,
            category FROM jdbc_affected_windows
    ), totals AS (
        SELECT a.window_start, a.window_end, a.category,
            COUNT(*) AS total_events,
            COUNT(*) FILTER (WHERE e.event_type = 'purchase') AS purchases,
            COALESCE(SUM(e.total_amount), 0) AS revenue
        FROM affected a JOIN events e ON e.category = a.category
            AND e.event_timestamp >= a.window_start
            AND e.event_timestamp < a.window_end
        GROUP BY a.window_start, a.window_end, a.category
    )
    INSERT INTO window_stats (window_start, window_end, category,
        total_events, purchases, revenue)
    SELECT window_start, window_end, category, total_events, purchases, revenue
    FROM totals
    ON CONFLICT (window_start, window_end, category) DO UPDATE SET
        total_events = EXCLUDED.total_events,
        purchases = EXCLUDED.purchases,
        revenue = EXCLUDED.revenue
"""
WINDOW_MERGE = """
    CREATE TEMP TABLE jdbc_affected_windows ON COMMIT DROP AS
    SELECT DISTINCT window_start, category
    FROM jdbc_stage_windows WHERE stream_id = %s AND batch_id = %s
"""


@dataclass(frozen=True)
class JdbcConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    namespace: str
    checkpoint_root: str
    kafka_broker: str
    kafka_topic: str
    max_partitions: int = 4
    batch_size: int = 500
    starting_offsets: str = "earliest"

    @classmethod
    def from_env(cls):
        required = (
            "JDBC_DB_HOST", "JDBC_DB_NAME", "JDBC_DB_USER", "JDBC_DB_PASSWORD",
            "JDBC_STREAM_NAMESPACE", "JDBC_CHECKPOINT_ROOT", "JDBC_KAFKA_BROKER",
            "JDBC_KAFKA_TOPIC",
        )
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise ValueError("Missing JDBC configuration: " + ", ".join(missing))
        starting_offsets = os.getenv("JDBC_STARTING_OFFSETS", "earliest")
        try:
            offsets = json.loads(starting_offsets)
        except json.JSONDecodeError:
            offsets = None
        if offsets is not None:
            if not isinstance(offsets, dict) or not offsets or any(
                not isinstance(partitions, dict) or not partitions or
                any(not str(partition).isdigit() or type(offset) is not int or offset < 0
                    for partition, offset in partitions.items())
                for partitions in offsets.values()
            ):
                raise ValueError("JDBC_STARTING_OFFSETS requires exact nonnegative per-partition offsets")
        elif starting_offsets not in ("earliest", "latest"):
            raise ValueError("Invalid JDBC_STARTING_OFFSETS")
        config = cls(
            host=os.environ["JDBC_DB_HOST"],
            port=int(os.getenv("JDBC_DB_PORT", "5432")),
            database=os.environ["JDBC_DB_NAME"],
            user=os.environ["JDBC_DB_USER"],
            password=os.environ["JDBC_DB_PASSWORD"],
            namespace=os.environ["JDBC_STREAM_NAMESPACE"],
            checkpoint_root=os.environ["JDBC_CHECKPOINT_ROOT"].rstrip("/"),
            kafka_broker=os.environ["JDBC_KAFKA_BROKER"],
            kafka_topic=os.environ["JDBC_KAFKA_TOPIC"],
            max_partitions=int(os.getenv("JDBC_MAX_PARTITIONS", "4")),
            batch_size=int(os.getenv("JDBC_BATCH_SIZE", "500")),
            starting_offsets=starting_offsets,
        )
        if (config.database == "streaming_project" or config.host == "postgres-iteso" or
                config.kafka_topic == "ecommerce-events"):
            raise ValueError("The historical Commerce Lab database is permanently refused")
        final_target = (config.host == "trama-postgres-final" or
                        config.database == "trama_commerce_final" or
                        config.kafka_topic == "trama-commerce-final-events")
        if final_target and not (
            (config.host, config.database, config.user, config.kafka_topic) ==
            ("trama-postgres-final", "trama_commerce_final", "trama_final_user", "trama-commerce-final-events")
            and os.getenv("JDBC_ALLOW_FINAL_DEMO") == "I_ACCEPT_NEW_DATABASE_WRITES"
            and isinstance(offsets, dict)
            and list(offsets) == [config.kafka_topic]
        ):
            raise ValueError("The final demo requires its own database and topic, explicit opt-in and exact offsets")
        if config.checkpoint_root.endswith("/checkpoints") or "/checkpoints/" in config.checkpoint_root:
            raise ValueError("Use a new JDBC checkpoint root outside the original checkpoints directory")
        if not config.namespace.strip() or not config.checkpoint_root or config.max_partitions < 1 or config.batch_size < 1:
            raise ValueError("Invalid JDBC stream namespace, checkpoint root, or write limits")
        return config

    @property
    def url(self):
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def connection_kwargs(self):
        return dict(host=self.host, port=self.port, dbname=self.database,
                    user=self.user, password=self.password)


def stage_batch(batch_df, batch_id, config, sink):
    """Spark performs the complete distributed DataFrame write through JDBC."""
    from pyspark.sql import functions as F

    if sink not in ("events", "windows"):
        raise ValueError("Unknown JDBC sink")
    columns = EVENT_COLUMNS if sink == "events" else WINDOW_COLUMNS
    table = "jdbc_stage_events" if sink == "events" else "jdbc_stage_windows"
    stream_id = f"{config.namespace}:{sink}"
    staged = batch_df.select(*columns).withColumn("stream_id", F.lit(stream_id)) \
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
    (staged.write.format("jdbc").mode("append")
        .option("url", config.url).option("dbtable", table)
        .option("driver", "org.postgresql.Driver")
        .option("user", config.user).option("password", config.password)
        .option("numPartitions", str(config.max_partitions))
        .option("batchsize", str(config.batch_size))
        .option("isolationLevel", "READ_COMMITTED")
        .save())
    return stream_id


def acquire_writer_lease(config, connect=None):
    """Keep one JDBC consumer process per database; close on process exit."""
    if connect is None:
        import psycopg2
        connect = psycopg2.connect
    conn = connect(**config.connection_kwargs())
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", ("trama-jdbc-consumer",))
            if not cursor.fetchone()[0]:
                raise RuntimeError("Another JDBC consumer already holds the database writer lease")
        return conn
    except Exception:
        conn.close()
        raise


def consolidate_batch(batch_id, config, sink, connect=None):
    """One PostgreSQL transaction merges, records completion, and cleans staging."""
    if sink not in ("events", "windows"):
        raise ValueError("Unknown JDBC sink")
    stream_id = f"{config.namespace}:{sink}"
    table = "jdbc_stage_events" if sink == "events" else "jdbc_stage_windows"
    conflict_sql = EVENT_CONFLICT if sink == "events" else WINDOW_CONFLICT
    digest_sql = EVENT_DIGEST if sink == "events" else WINDOW_DIGEST
    merge_sql = EVENT_MERGE if sink == "events" else WINDOW_MERGE
    if connect is None:
        import psycopg2
        connect = psycopg2.connect
    with closing(connect(**config.connection_kwargs())) as conn:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("SET LOCAL TIME ZONE 'UTC'", ())
                cursor.execute("INSERT INTO jdbc_stream_locks (stream_id) VALUES (%s) ON CONFLICT DO NOTHING", (stream_id,))
                cursor.execute("SELECT stream_id FROM jdbc_stream_locks WHERE stream_id = %s FOR UPDATE", (stream_id,))
                cursor.execute(conflict_sql, (stream_id, batch_id))
                if cursor.fetchone() is not None:
                    raise ValueError(f"Conflicting payloads in {sink} batch {batch_id}; no final rows were changed")
                if sink == "events":
                    cursor.execute(EVENT_EXISTING_CONFLICT, (stream_id, batch_id))
                    if cursor.fetchone() is not None:
                        raise ValueError(f"Conflicting event_id in previously stored events for batch {batch_id}")
                cursor.execute(digest_sql, (stream_id, batch_id))
                row_count, payload_digest = cursor.fetchone()
                cursor.execute("SELECT row_count, payload_digest FROM jdbc_batch_commits WHERE stream_id = %s AND batch_id = %s", (stream_id, batch_id))
                previous = cursor.fetchone()
                already_committed = previous is not None
                if already_committed and previous != (row_count, payload_digest):
                    raise ValueError(f"Changed payload in committed {sink} batch {batch_id}")
                if not already_committed:
                    # Both independent queries serialize their canonical window refresh.
                    cursor.execute("INSERT INTO jdbc_stream_locks (stream_id) VALUES (%s) ON CONFLICT DO NOTHING", (config.namespace + ':window-reconcile',))
                    cursor.execute("SELECT stream_id FROM jdbc_stream_locks WHERE stream_id = %s FOR UPDATE", (config.namespace + ':window-reconcile',))
                    cursor.execute(merge_sql, (stream_id, batch_id))
                    cursor.execute(WINDOW_FROM_EVENTS, ())
                    cursor.execute("INSERT INTO jdbc_batch_commits (stream_id, batch_id, row_count, payload_digest) VALUES (%s, %s, %s, %s)", (stream_id, batch_id, row_count, payload_digest))
                cursor.execute(f"DELETE FROM {table} WHERE stream_id = %s AND batch_id = %s", (stream_id, batch_id))
    return not already_committed


def write_batch(batch_df, batch_id, config, sink):
    # If Spark's partition writes fail, no merge or ledger entry occurs.
    stage_batch(batch_df, batch_id, config, sink)
    # Explicit fault-injection hook for the isolated Kafka end-to-end test.
    # It is inactive unless the test runner sets a marker path.
    marker = os.getenv("JDBC_TEST_PAUSE_AFTER_STAGE_MARKER")
    if marker and sink == "events" and batch_id == 0:
        Path(marker).write_text("Spark JDBC staging complete; waiting for test interruption\n")
        while Path(marker).exists():
            time.sleep(0.1)
    committed = consolidate_batch(batch_id, config, sink)
    print(f"{sink} batch {batch_id}: {'committed' if committed else 'already committed'}")
