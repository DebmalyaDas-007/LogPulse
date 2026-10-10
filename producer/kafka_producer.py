"""Publish generated (or piped-in) access logs to Kafka."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Callable, Iterable, TextIO

try:
    from .log_generator import FORMATS, SCENARIOS, LogGenerator
except ImportError:  # run as a plain script: python producer/kafka_producer.py
    from log_generator import FORMATS, SCENARIOS, LogGenerator


def build_producer(bootstrap_servers: str) -> Any:
    from kafka import KafkaProducer

    options: dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers.split(","),
        "acks": "all",
        "retries": 5,
        "linger_ms": 20,
    }
    if os.getenv("KAFKA_SECURITY_PROTOCOL"):
        options.update(
            security_protocol=os.environ["KAFKA_SECURITY_PROTOCOL"],
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_plain_username=os.getenv("KAFKA_SASL_USERNAME"),
            sasl_plain_password=os.getenv("KAFKA_SASL_PASSWORD"),
        )
        if os.getenv("KAFKA_SSL_CA_FILE"):
            options["ssl_cafile"] = os.environ["KAFKA_SSL_CA_FILE"]
    return KafkaProducer(**options)


def publish_generated(
    producer: Any,
    topic: str,
    generator: LogGenerator,
    count: int | None,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    sent = 0
    for record in generator.records(count):
        producer.send(topic, value=generator.render(record).encode("utf-8"))
        sent += 1
        sleep(generator.delay())
    producer.flush()
    return sent


def publish_stdin(producer: Any, topic: str, stream: TextIO) -> int:
    sent = 0
    for number, line in enumerate(stream, start=1):
        line = line.strip()
        if not line:
            continue
        if line.startswith("{"):
            try:
                json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"line {number} is not valid JSON: {exc}") from exc
        producer.send(topic, value=line.encode("utf-8"))
        sent += 1
    producer.flush()
    return sent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=float, default=10.0, help="records per second")
    parser.add_argument("--anomaly-rate", type=float, default=0.01)
    parser.add_argument("--scenario", choices=SCENARIOS)
    parser.add_argument("--format", dest="output_format", choices=FORMATS, default="json")
    parser.add_argument("--count", type=int, help="stop after N records (default: run until Ctrl+C)")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--stdin", action="store_true", help="forward lines from stdin instead of generating")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_SERVER_LOGS", "server-logs"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    producer = build_producer(args.bootstrap_servers)
    sent = 0
    try:
        if args.stdin:
            sent = publish_stdin(producer, args.topic, sys.stdin)
        else:
            generator = LogGenerator(args.rate, args.anomaly_rate, args.scenario, args.output_format, args.seed)
            sent = publish_generated(producer, args.topic, generator, args.count)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
    print(f"Published {sent} records to {args.topic}", file=sys.stderr)


if __name__ == "__main__":
    main()
