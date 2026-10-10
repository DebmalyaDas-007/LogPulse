import json
from io import StringIO

import pytest

from producer.kafka_producer import publish_generated, publish_stdin
from producer.log_generator import LogGenerator
from spark_jobs.analyzer import WindowAnalyzer
from spark_jobs.log_parser import parse_line
from spark_jobs.sinks import index_name

FIELDS = {"timestamp", "client_ip", "method", "url", "status_code", "latency_ms", "user_id"}


def test_normal_records_have_expected_shape():
    for record in LogGenerator(anomaly_rate=0, seed=1).records(30):
        assert FIELDS == record.keys()
        assert isinstance(record["status_code"], int) and isinstance(record["latency_ms"], float)


def test_same_seed_gives_same_ips_and_statuses():
    def sample(seed):
        return [(r["client_ip"], r["status_code"]) for r in LogGenerator(anomaly_rate=0, seed=seed).records(20)]

    assert sample(5) == sample(5)


def test_latency_scenario_is_far_above_normal():
    assert next(LogGenerator(scenario="latency", seed=4).records(1))["latency_ms"] > 2000


def test_brute_force_scenario_shape():
    records = list(LogGenerator(scenario="brute-force", seed=11).records(110))
    assert {r["client_ip"] for r in records} == {records[0]["client_ip"]}
    assert all(r["url"] == "/login" and r["status_code"] == 401 for r in records)


def test_error_burst_and_ddos_shapes():
    errors = list(LogGenerator(scenario="error-burst", seed=2).records(25))
    assert all(r["status_code"] == 500 for r in errors)
    assert len({r["client_ip"] for r in LogGenerator(scenario="ddos", seed=2).records(100)}) <= 5


def test_ddos_runs_faster_than_normal_traffic():
    gen = LogGenerator(rate=10, scenario="ddos", seed=1)
    normal_delay = LogGenerator(rate=10, anomaly_rate=0).delay()
    gen.next_record()
    assert gen.delay() < normal_delay


def test_invalid_arguments_are_rejected():
    for kwargs in ({"rate": 0}, {"anomaly_rate": 2}, {"scenario": "nope"}, {"output_format": "xml"}):
        with pytest.raises(ValueError):
            LogGenerator(**kwargs)


def test_run_writes_json_lines_and_paces_output():
    out, pauses = StringIO(), []
    LogGenerator(rate=4, anomaly_rate=0, seed=3).run(out, count=3, sleep=pauses.append)
    lines = out.getvalue().splitlines()
    assert len(lines) == 3 and all(FIELDS <= json.loads(line).keys() for line in lines)
    assert pauses == [0.25, 0.25, 0.25]


def test_apache_output_round_trips_through_the_parser():
    out = StringIO()
    LogGenerator(anomaly_rate=0, output_format="apache", seed=3).run(out, count=5, sleep=lambda _: None)
    parsed = [parse_line(line) for line in out.getvalue().splitlines()]
    assert all(p is not None for p in parsed)


def test_generated_attack_is_caught_end_to_end():
    gen = LogGenerator(scenario="brute-force", seed=8)
    records = [parse_line(gen.render(r)) for r in gen.records(140)]
    kinds = {f["anomaly_type"] for f in WindowAnalyzer().ingest(records)}
    assert "brute_force" in kinds


class FakeProducer:
    def __init__(self):
        self.sent, self.flushed = [], 0

    def send(self, topic, value):
        self.sent.append((topic, value))

    def flush(self):
        self.flushed += 1


def test_publish_generated_sends_each_record_and_flushes():
    producer = FakeProducer()
    sent = publish_generated(producer, "server-logs", LogGenerator(anomaly_rate=0, seed=1), 4, sleep=lambda _: None)
    assert sent == 4 and len(producer.sent) == 4 and producer.flushed == 1
    assert all(topic == "server-logs" for topic, _ in producer.sent)


def test_publish_stdin_skips_blank_lines_and_rejects_bad_json():
    producer = FakeProducer()
    assert publish_stdin(producer, "t", StringIO('{"a":1}\n\n1.1.1.1 - - [x] "GET / HTTP/1.1" 200 1\n')) == 2
    with pytest.raises(ValueError):
        publish_stdin(producer, "t", StringIO("{broken\n"))


def test_index_name_uses_event_date():
    assert index_name("anomalies", "2026-10-10T23:59:59Z") == "anomalies-2026.10.10"
