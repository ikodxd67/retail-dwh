"""Кликстрим: Kafka -> Delta (bronze) -> сессии (silver) -> воронка по дням.

Функции получают готовую SparkSession и не знают, откуда она: Airflow
передаёт удалённую (Spark Connect), а `python web_events.py stream` на
сервере Spark запускает тот же код как постоянный поток.

Слои в MinIO (бакет lake):
  delta/web/events             bronze: разобранные события, без дублей
  delta/web/events_quarantine  битые сообщения как есть, для разбора
  delta/web/sessions           silver: одна строка на сессию
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

LAKE = "s3a://lake"
EVENTS = f"{LAKE}/delta/web/events"
QUARANTINE = f"{LAKE}/delta/web/events_quarantine"
SESSIONS = f"{LAKE}/delta/web/sessions"
CHECKPOINTS = f"{LAKE}/_checkpoints/web"
KAFKA = "kafka:19092"  # адрес, видимый с сервера Spark
TOPIC = "web.events"

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_ts", StringType()),
        StructField("session_id", StringType()),
        StructField("customer_id", LongType()),
        StructField("device", StringType()),
        StructField("traffic_source", StringType()),
        StructField("sku", StringType()),
        StructField("price", DoubleType()),
        StructField("qty", IntegerType()),
        StructField("order_value", DoubleType()),
    ]
)


def _kafka_stream(spark: SparkSession, max_offsets: int) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        # сообщения старше retention Kafka уже удалены — не падать, а ехать дальше
        .option("failOnDataLoss", "false")
        .load()
    )
    return raw.select(
        F.col("value").cast("string").alias("raw"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
    )


def ingest_events(
    spark: SparkSession,
    *,
    processing_time: str | None = None,
    max_offsets: int = 500_000,
) -> dict:
    """Забрать из Kafka всё новое (или работать постоянно, если задан processing_time).

    Прочитанные смещения хранятся в чекпоинте, поэтому повторный запуск
    продолжает с места остановки, а падение посреди записи не даст дублей:
    Delta-приёмник фиксирует партию атомарно вместе с чекпоинтом.
    """
    parsed = _kafka_stream(spark, max_offsets)
    good = (
        parsed.where(F.col("e.event_id").isNotNull() & F.col("e.event_ts").isNotNull())
        .select("e.*", "partition", "offset", "kafka_ts")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("ingested_at", F.current_timestamp())
        # SDK повторяет отправку при сбоях сети — один event_id может прийти
        # дважды. Состояние дедупликации хранится 30 минут по времени события.
        .withWatermark("event_ts", "30 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    bad = parsed.where(F.col("e.event_id").isNull() | F.col("e.event_ts").isNull()).select(
        "raw", "partition", "offset", "kafka_ts", F.current_timestamp().alias("ingested_at")
    )

    def start(df: DataFrame, path: str, name: str, partition_by: list[str]):
        writer = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINTS}/{name}")
            .queryName(name)
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer = (
            writer.trigger(processingTime=processing_time)
            if processing_time
            else writer.trigger(availableNow=True)
        )
        return writer.start(path)

    queries = [
        start(good, EVENTS, "web_events", ["event_date"]),
        start(bad, QUARANTINE, "web_events_quarantine", []),
    ]
    if processing_time:
        spark.streams.awaitAnyTermination()
    stats = {}
    for q in queries:
        q.awaitTermination()
        # в Spark Connect прогресс приходит JSON-строками, в обычном Spark — словарями
        progress = [json.loads(p) if isinstance(p, str) else p for p in q.recentProgress]
        # numInputRows на верхнем уровне есть не во всех версиях клиента, в sources — всегда
        stats[q.name] = sum(src.get("numInputRows", 0) for p in progress for src in p.get("sources", []))
    return stats


def build_sessions(spark: SparkSession, days_back: int = 2) -> DataFrame:
    """Пересобрать сессии за последние дни и вернуть воронку по дням.

    Сессия может начаться до полуночи и закончиться после, поэтому берём
    события на день раньше окна, а в silver перезаписываем только окно
    (replaceWhere) — перезапуск безопасен.
    """
    start = date.today() - timedelta(days=days_back)
    events = (
        spark.read.format("delta").load(EVENTS).where(F.col("event_date") >= F.lit(start - timedelta(days=1)))
    )

    ordered = Window.partitionBy("session_id").orderBy("event_ts")
    per_event = events.withColumn("step", F.row_number().over(ordered)).withColumn(
        # пауза с предыдущим событием сессии — для средней «задумчивости»
        "gap_sec",
        F.col("event_ts").cast("long") - F.lag(F.col("event_ts").cast("long")).over(ordered),
    )
    sessions = (
        per_event.groupBy("session_id")
        .agg(
            F.min("event_ts").alias("started_at"),
            F.max("event_ts").alias("ended_at"),
            F.max("customer_id").alias("customer_id"),
            F.first("device", ignorenulls=True).alias("device"),
            F.first("traffic_source", ignorenulls=True).alias("traffic_source"),
            F.count("*").alias("events"),
            F.countDistinct(F.when(F.col("event_type") == "product_view", F.col("sku"))).alias(
                "products_viewed"
            ),
            F.max((F.col("event_type") == "add_to_cart").cast("int")).alias("has_cart"),
            F.max((F.col("event_type") == "checkout_start").cast("int")).alias("has_checkout"),
            F.max((F.col("event_type") == "purchase").cast("int")).alias("has_purchase"),
            F.sum("order_value").alias("order_value"),
            F.avg("gap_sec").alias("avg_gap_sec"),
        )
        .withColumn("session_date", F.to_date("started_at"))
        .withColumn("duration_sec", F.col("ended_at").cast("long") - F.col("started_at").cast("long"))
        .where(F.col("session_date") >= F.lit(start))
    )
    # номер визита клиента: сколько сессий у него было до этой (для «новый/вернувшийся»)
    visits = Window.partitionBy("customer_id").orderBy("started_at")
    sessions = sessions.withColumn(
        "visit_no", F.when(F.col("customer_id").isNotNull(), F.row_number().over(visits))
    )

    (
        sessions.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"session_date >= '{start.isoformat()}'")
        .partitionBy("session_date")
        .save(SESSIONS)
    )

    return (
        spark.read.format("delta")
        .load(SESSIONS)
        .where(F.col("session_date") >= F.lit(start))
        .groupBy("session_date", "device", "traffic_source")
        .agg(
            F.count("*").alias("sessions"),
            F.sum("has_cart").alias("with_cart"),
            F.sum("has_checkout").alias("with_checkout"),
            F.sum("has_purchase").alias("with_purchase"),
            F.coalesce(F.sum("order_value"), F.lit(0.0)).alias("revenue"),
            F.avg("duration_sec").alias("avg_duration_sec"),
        )
    )


if __name__ == "__main__":
    # постоянный поток прямо на сервере Spark:
    #   spark-submit ... web_events.py stream
    session = SparkSession.builder.getOrCreate()
    if sys.argv[1:] == ["stream"]:
        ingest_events(session, processing_time="30 seconds")
    else:
        print(ingest_events(session))
