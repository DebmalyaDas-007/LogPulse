"""Elasticsearch and Kafka output helpers. Heavy client libraries are imported lazily."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Iterable


def index_name(prefix: str, timestamp: str | datetime | None = None) -> str:
    """Daily index name such as `server-logs-2026.10.10`."""
    if timestamp is None:
        day = datetime.now(timezone.utc).date()
    elif isinstance(timestamp, datetime):
        day = timestamp.date()
    else:
        day = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).date()
    return f"{prefix}-{day:%Y.%m.%d}"


def make_es_client() -> Any:
    from elasticsearch import Elasticsearch

    url = os.getenv("ELASTICSEARCH_URL") or (
        f"http://{os.getenv('ELASTICSEARCH_HOST', 'localhost')}:{os.getenv('ELASTICSEARCH_PORT', '9200')}"
    )
    options: dict[str, Any] = {
        "request_timeout": 60,
        "max_retries": 5,
        "retry_on_timeout": True,
        "verify_certs": os.getenv("ELASTICSEARCH_VERIFY_CERTS", "true").lower() == "true",
    }
    if os.getenv("ELASTICSEARCH_API_KEY"):
        options["api_key"] = os.environ["ELASTICSEARCH_API_KEY"]
    elif os.getenv("ELASTICSEARCH_USERNAME") and os.getenv("ELASTICSEARCH_PASSWORD"):
        options["basic_auth"] = (os.environ["ELASTICSEARCH_USERNAME"], os.environ["ELASTICSEARCH_PASSWORD"])
    return Elasticsearch(url, **options)


def bulk_index(client: Any, documents: Iterable[dict[str, Any]], prefix: str) -> int:
    from elasticsearch.helpers import bulk

    actions = ({"_index": index_name(prefix, doc.get("timestamp")), "_source": doc} for doc in documents)
    indexed, _ = bulk(client, actions, raise_on_error=True)
    return int(indexed)


def make_kafka_producer(bootstrap_servers: str) -> Any:
    from kafka import KafkaProducer

    options: dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers.split(","),
        "acks": "all",
        "retries": 5,
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


def publish_findings(producer: Any, topic: str, findings: Iterable[dict[str, Any]]) -> int:
    sent = 0
    for finding in findings:
        producer.send(topic, value=json.dumps(finding, separators=(",", ":")).encode("utf-8"))
        sent += 1
    producer.flush()
    return sent
