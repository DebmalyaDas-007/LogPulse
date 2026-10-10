import json
from datetime import timezone

from spark_jobs.log_parser import parse_line

JSON_LINE = json.dumps({
    "timestamp": "2026-10-10T06:00:00Z", "client_ip": "1.2.3.4", "method": "GET", "url": "/",
    "status_code": 200, "latency_ms": 12.5, "user_id": "user-1",
})
APACHE_LINE = '9.8.7.6 - alice [10/Oct/2026:11:30:00 +0530] "POST /login HTTP/1.1" 401 512 "-" "agent"'


def test_json_line_is_parsed():
    record = parse_line(JSON_LINE)
    assert record.client_ip == "1.2.3.4"
    assert record.status_code == 200
    assert record.latency_ms == 12.5
    assert record.timestamp.tzinfo is not None


def test_apache_line_is_parsed_and_converted_to_utc():
    record = parse_line(APACHE_LINE)
    assert (record.method, record.url, record.status_code, record.user_id) == ("POST", "/login", 401, "alice")
    assert record.latency_ms == 0.0
    assert record.timestamp.utcoffset().total_seconds() == 0
    assert record.timestamp.astimezone(timezone.utc).hour == 6  # 11:30 +05:30


def test_dash_user_becomes_none():
    assert parse_line(APACHE_LINE.replace("alice", "-")).user_id is None


def test_malformed_input_returns_none_instead_of_raising():
    for bad in ("", "   ", "garbage", "{not json", '{"timestamp": "nope"}', None, b"\xff\xfe"):
        assert parse_line(bad) is None


def test_document_matches_strict_mapping_fields():
    doc = parse_line(JSON_LINE).to_document()
    assert set(doc) == {"timestamp", "client_ip", "method", "url", "status_code", "latency_ms", "user_id"}
    assert doc["timestamp"].endswith("Z")
