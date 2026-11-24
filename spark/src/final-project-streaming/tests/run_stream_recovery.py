"""Checkpoint restart test with a file stream; never connects to Kafka."""

from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import psycopg2
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType,
)

from jdbc_sink import JdbcConfig, write_batch


def main():
    base_config = JdbcConfig.from_env()
    assert base_config.database == "trama_jdbc_test" and base_config.host == "db"
    config = replace(base_config, namespace="isolated-recovery-001")
    spark = SparkSession.builder.appName("Trama JDBC checkpoint recovery") \
        .master("local[2]").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    schema = StructType([
        StructField("event_id", StringType()), StructField("user_id", StringType()),
        StructField("product_id", StringType()), StructField("category", StringType()),
        StructField("event_type", StringType()), StructField("price", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("event_timestamp", TimestampType()),
        StructField("total_amount", DoubleType()),
    ])
    with TemporaryDirectory(prefix="trama-jdbc-recovery-") as temporary:
        source = Path(temporary) / "input"
        source.mkdir()
        checkpoint = Path(temporary) / "checkpoints"
        fixed_time = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)

        def add_file(index):
            row = {
                "event_id": str(uuid4()), "user_id": "recovery-test",
                "product_id": "P006", "category": "Home", "event_type": "purchase",
                "price": 449.0, "quantity": 1,
                "event_timestamp": fixed_time.isoformat(), "total_amount": 449.0,
            }
            (source / f"event-{index}.json").write_text(json.dumps(row) + "\n")

        def run_once(sink):
            events = spark.readStream.schema(schema).json(str(source))
            if sink == "windows":
                output = (events.withWatermark("event_timestamp", "10 minutes")
                    .groupBy(F.window("event_timestamp", "5 minutes"), F.col("category"))
                    .agg(F.count("*").alias("total_events"),
                         F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
                         F.round(F.sum("total_amount"), 2).alias("revenue"))
                    .select(F.col("window.start").alias("window_start"),
                            F.col("window.end").alias("window_end"), "category",
                            "total_events", "purchases", "revenue"))
                mode = "update"
            else:
                output = events
                mode = "append"
            query = (output.writeStream
                .foreachBatch(lambda df, batch_id: write_batch(df, batch_id, config, sink))
                .outputMode(mode).option("checkpointLocation", str(checkpoint / sink))
                .trigger(availableNow=True).start())
            query.awaitTermination(90)
            if query.exception():
                raise RuntimeError(str(query.exception()))

        add_file(1)
        run_once("events")
        run_once("windows")
        # Starting from the same checkpoint without new input must not replay.
        run_once("events")
        run_once("windows")
        add_file(2)
        run_once("events")
        run_once("windows")

        with psycopg2.connect(**config.connection_kwargs()) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT count(*), sum(total_amount) FROM events WHERE user_id = 'recovery-test'")
                assert cursor.fetchone() == (2, 898)
                cursor.execute("SELECT total_events, purchases, revenue FROM window_stats WHERE category = 'Home'")
                assert cursor.fetchone() == (2, 2, 898)
                cursor.execute("SELECT count(*) FROM (SELECT event_id FROM events GROUP BY event_id HAVING count(*) > 1) duplicates")
                assert cursor.fetchone()[0] == 0
    print("PASS: isolated file stream resumed both separate checkpoints, processed new input once, and preserved window UPSERT")
    spark.stop()


if __name__ == "__main__":
    main()
