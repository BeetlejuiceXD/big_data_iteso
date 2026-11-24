# 1. Imports y configuración
import os
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)


KAFKA_BROKER = "kafka:9093"
KAFKA_TOPIC = "ecommerce-events"

# Esquema de los eventos de E-commerce

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])


# Guardar eventos en PostgreSQL
def save_events_to_postgres(batch_df, batch_id):

    rows = [
        (
            row.event_id,
            row.user_id,
            row.product_id,
            row.category,
            row.event_type,
            row.price,
            row.quantity,
            row.event_timestamp,
            row.total_amount
        )
        for row in batch_df.toLocalIterator()
    ]

    if not rows:
        return

    with psycopg2.connect(
        host="postgres-iteso",
        port=5432,
        dbname="streaming_project",
        user="streaming_user",
        password=os.environ["DB_PASSWORD"]
    ) as conn:

        with conn.cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO events (
                    event_id,
                    user_id,
                    product_id,
                    category,
                    event_type,
                    price,
                    quantity,
                    event_timestamp,
                    total_amount
                )
                VALUES %s
                ON CONFLICT (event_id) DO NOTHING
                """,
                rows
            )

    print(f"Batch {batch_id}: {len(rows)} eventos recibidos")


# Guardar estadísticas por ventanas en PostgreSQL
def save_window_stats_to_postgres(batch_df, batch_id):

    rows = [
        (
            row.window_start,
            row.window_end,
            row.category,
            row.total_events,
            row.purchases,
            row.revenue
        )
        for row in batch_df.toLocalIterator()
    ]

    if not rows:
        return

    with psycopg2.connect(
        host="postgres-iteso",
        port=5432,
        dbname="streaming_project",
        user="streaming_user",
        password=os.environ["DB_PASSWORD"]
    ) as conn:

        with conn.cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO window_stats (
                    window_start,
                    window_end,
                    category,
                    total_events,
                    purchases,
                    revenue
                )
                VALUES %s
                ON CONFLICT (
                    window_start,
                    window_end,
                    category
                )
                DO UPDATE SET
                    total_events = EXCLUDED.total_events,
                    purchases = EXCLUDED.purchases,
                    revenue = EXCLUDED.revenue
                """,
                rows
            )

    print(f"Batch {batch_id}: {len(rows)} ventanas actualizadas")

def main():
    # 2. Crear sesión de Spark
    spark = (
        SparkSession.builder
        .appName("Ecommerce Streaming Consumer")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # 3. Leer eventos de Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )


    # 4. Interpretar los eventos JSON

    events_df = (
        kafka_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                EVENT_SCHEMA
            ).alias("event")
        )
        .select("event.*")
    )

    # 5. Validación de eventos

    valid_events_df = (
        events_df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("category").isNotNull())
        .filter(F.col("event_type").isin(
            "view", "add_to_cart", "purchase"
        ))
        .filter(F.col("price").isNotNull() & (F.col("price") > 0))
        .filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))
        .filter(F.col("timestamp").isNotNull())
    )

    # 6. Transformación y enriquecimiento

    processed_events_df = (
        valid_events_df
        .withColumn(
            "event_timestamp",
            F.to_timestamp("timestamp")
        )
        .filter(F.col("event_timestamp").isNotNull())
        .withColumn(
            "total_amount",
            F.when(
                F.col("event_type") == "purchase",
                F.round(F.col("price") * F.col("quantity"), 2)
            ).otherwise(F.lit(0.0))
        )
        .drop("timestamp")
    )

    # 7. Agregaciones por categoría

    category_stats_df = (
        processed_events_df
        .groupBy("category")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(
                F.when(F.col("event_type") == "view", 1).otherwise(0)
            ).alias("views"),
            F.sum(
                F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)
            ).alias("add_to_cart"),
            F.sum(
                F.when(F.col("event_type") == "purchase", 1).otherwise(0)
            ).alias("purchases"),
            F.round(F.sum("total_amount"), 2).alias("revenue")
        )
    )

    # 8. Agregaciones por ventanas de tiempo

    window_stats_df = (
        processed_events_df
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            F.window("event_timestamp", "5 minutes"),
            F.col("category")
        )
        .agg(
            F.count("*").alias("total_events"),
            F.sum(
                F.when(F.col("event_type") == "purchase", 1)
                .otherwise(0)
            ).alias("purchases"),
            F.round(F.sum("total_amount"), 2).alias("revenue")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "category",
            "total_events",
            "purchases",
            "revenue"
        )
    )

    # 9. Guardar eventos individuales
    events_query = (
        processed_events_df.writeStream
        .foreachBatch(save_events_to_postgres)
        .outputMode("append")
        .option(
            "checkpointLocation",
            "/opt/spark/work-dir/src/final-project-streaming/checkpoints/events"
        )
        .trigger(processingTime="10 seconds")
        .start()
    )

    # 10. Guardar estadísticas por ventanas
    windows_query = (
        window_stats_df.writeStream
        .foreachBatch(save_window_stats_to_postgres)
        .outputMode("update")
        .option(
            "checkpointLocation",
            "/opt/spark/work-dir/src/final-project-streaming/checkpoints/windows"
        )
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Mantener ambos flujos ejecutándose
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()