"""Structured Streaming job: Kafka `server-logs` -> windowed anomaly detection -> Elasticsearch + Kafka `anomalies`.

Parsing runs inside Spark (a Python UDF around `log_parser.parse_line`, so JSON and Apache lines
both work). Each micro-batch of parsed rows is then handed to the stateful `WindowAnalyzer`.
`maxOffsetsPerTrigger` keeps those micro-batches small enough for the driver to hold in memory.
"""

from __future__ import annotations

import json
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from spark_jobs.analyzer import WindowAnalyzer
from spark_jobs.log_parser import LogRecord, parse_line
from spark_jobs.rules import Thresholds
from spark_jobs.sinks import bulk_index, make_es_client, make_kafka_producer, publish_findings

PARSED_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("client_ip", StringType(), False),
    StructField("method", StringType(), False),
    StructField("url", StringType(), False),
    StructField("status_code", IntegerType(), False),
    StructField("latency_ms", DoubleType(), False),
    StructField("user_id", StringType(), True),
])


def _parse_to_row(raw: str | None):
    record = parse_line(raw)
    if record is None:
        return None
    return (record.timestamp, record.client_ip, record.method, record.url, record.status_code, record.latency_ms, record.user_id)


parse_udf = F.udf(_parse_to_row, PARSED_SCHEMA)


def build_session() -> SparkSession:
    builder = SparkSession.builder.appName(os.getenv("SPARK_APP_NAME", "log-sentinel"))
    if os.getenv("SPARK_GPU_ENABLED", "false").lower() == "true":
        builder = (
            builder.config("spark.plugins", "com.nvidia.spark.SQLPlugin")
            .config("spark.rapids.sql.enabled", "true")
            .config("spark.driver.resource.gpu.amount", os.getenv("SPARK_GPU_AMOUNT", "1"))
            .config("spark.task.resource.gpu.amount", os.getenv("SPARK_GPU_TASK_AMOUNT", "0.25"))
        )
        print("RAPIDS GPU acceleration enabled", flush=True)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def kafka_options() -> dict[str, str]:
    options = {
        "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "subscribe": os.getenv("KAFKA_TOPIC_SERVER_LOGS", "server-logs"),
        "startingOffsets": os.getenv("KAFKA_STARTING_OFFSETS", "latest"),
        "maxOffsetsPerTrigger": os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "50000"),
        "failOnDataLoss": "false",
    }
    if os.getenv("KAFKA_SECURITY_PROTOCOL"):
        login = "org.apache.kafka.common.security.plain.PlainLoginModule"
        options.update({
            "kafka.security.protocol": os.environ["KAFKA_SECURITY_PROTOCOL"],
            "kafka.sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            "kafka.sasl.jaas.config": (
                f'{login} required username="{os.environ["KAFKA_SASL_USERNAME"]}" '
                f'password="{os.environ["KAFKA_SASL_PASSWORD"]}";'
            ),
        })
    return options


def parse_stream(raw: DataFrame) -> DataFrame:
    parsed = raw.select(parse_udf(F.col("value").cast("string")).alias("r")).where(F.col("r").isNotNull())
    return parsed.select("r.*").withWatermark("timestamp", "2 minutes")


def main() -> None:
    spark = build_session()
    raw = spark.readStream.format("kafka").options(**kafka_options()).load()
    logs = parse_stream(raw)

    window_seconds = int(os.getenv("WINDOW_SECONDS", "60"))
    thresholds = Thresholds(
        unauthorized_per_ip=int(os.getenv("THRESHOLD_UNAUTHORIZED", "100")),
        error_rate=float(os.getenv("THRESHOLD_ERROR_RATE", "0.01")),
        surge_multiplier=float(os.getenv("THRESHOLD_SURGE_MULTIPLIER", "5")),
    )
    analyzer = WindowAnalyzer(window_seconds=window_seconds, thresholds=thresholds)

    write_to_es = os.getenv("SPARK_ELASTICSEARCH_ENABLED", "true").lower() == "true"
    es = make_es_client() if write_to_es else None
    anomaly_topic = os.getenv("KAFKA_TOPIC_ANOMALIES", "anomalies")
    producer = make_kafka_producer(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))

    def process_batch(batch: DataFrame, batch_id: int) -> None:
        rows = batch.collect()
        if not rows:
            return
        records = [
            LogRecord(r.timestamp, r.client_ip, r.method, r.url, r.status_code, r.latency_ms, r.user_id)
            for r in rows
        ]
        findings = analyzer.ingest(records)
        if es is not None:
            bulk_index(es, (rec.to_document() for rec in records), "server-logs")
            if findings:
                bulk_index(es, findings, "anomalies")
        publish_findings(producer, anomaly_topic, findings)
        print(f"batch={batch_id} records={len(records)} findings={len(findings)}", flush=True)
        for finding in findings:
            print(json.dumps(finding), flush=True)

    checkpoint = os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/log-sentinel-checkpoint")
    query = logs.writeStream.foreachBatch(process_batch).option("checkpointLocation", checkpoint).start()
    try:
        query.awaitTermination()
    finally:
        producer.flush()
        if es is not None:
            es.close()


if __name__ == "__main__":
    main()
