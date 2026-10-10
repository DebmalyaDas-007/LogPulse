from datetime import datetime, timedelta, timezone

from spark_jobs.analyzer import WindowAnalyzer
from spark_jobs.log_parser import LogRecord

T0 = datetime(2026, 10, 10, 6, 0, 0, tzinfo=timezone.utc)  # aligned to a minute boundary


def rec(offset_s, status=200, ip="10.0.0.1", latency=120.0):
    return LogRecord(T0 + timedelta(seconds=offset_s), ip, "GET", "/", status, latency, None)


def by_type(findings, kind):
    return [f for f in findings if f["anomaly_type"] == kind]


def test_quiet_traffic_produces_no_findings():
    assert WindowAnalyzer().ingest([rec(i * 0.5, latency=100 + i % 7) for i in range(100)]) == []


def test_brute_force_fires_once_even_across_batches():
    analyzer = WindowAnalyzer()
    first = analyzer.ingest([rec(i * 0.1, 401, "6.6.6.6") for i in range(60)])
    second = analyzer.ingest([rec(10 + i * 0.1, 401, "6.6.6.6") for i in range(60)])
    assert first == []  # 60 failures is not enough yet
    hits = by_type(second, "brute_force")
    assert len(hits) == 1 and hits[0]["client_ip"] == "6.6.6.6" and hits[0]["unauthorized_count"] == 101
    assert analyzer.ingest([rec(20 + i * 0.1, 401, "6.6.6.6") for i in range(50)]) == []  # not repeated


def test_brute_force_is_tracked_per_ip():
    records = [rec(i * 0.1, 401, f"7.7.7.{i % 3}") for i in range(240)]  # 80 per IP
    assert by_type(WindowAnalyzer().ingest(records), "brute_force") == []


def test_error_burst_needs_minimum_requests():
    few = [rec(0, 500), rec(1, 500), rec(2, 200)]
    assert WindowAnalyzer().ingest(few) == []
    many = [rec(i * 0.1, 500 if i % 10 == 0 else 200) for i in range(50)]  # 10% errors
    hits = by_type(WindowAnalyzer().ingest(many), "error_burst")
    assert len(hits) == 1 and hits[0]["error_rate"] > 0.01


def test_latency_spike_detected_after_warmup_and_excluded_from_baseline():
    warmup = [rec(i * 0.2, latency=100 + (i % 5)) for i in range(40)]
    analyzer = WindowAnalyzer()
    assert analyzer.ingest(warmup) == []
    findings = analyzer.ingest([rec(9, latency=9000.0, ip="3.3.3.3")])
    hit = by_type(findings, "latency_spike")[0]
    assert hit["client_ip"] == "3.3.3.3" and hit["latency_ms"] == 9000.0
    assert hit["mean_latency_ms"] < 110  # the spike did not drag the baseline up


def test_latency_not_judged_before_enough_samples():
    assert WindowAnalyzer().ingest([rec(0, latency=100), rec(1, latency=99999)]) == []


def test_traffic_surge_uses_previous_windows_as_baseline():
    steady = [rec(w * 60 + i * 6, ) for w in range(3) for i in range(10)]  # 10 req/min
    surge = [rec(180 + i * 0.5) for i in range(60)]  # 60 req in the next minute
    findings = WindowAnalyzer().ingest(steady + surge)
    hits = by_type(findings, "traffic_surge")
    assert len(hits) == 1 and hits[0]["window_start"] == "2026-10-10T06:03:00Z"


def test_no_surge_without_baseline():
    assert by_type(WindowAnalyzer().ingest([rec(i * 0.1) for i in range(500)]), "traffic_surge") == []


def test_findings_use_documented_fields_only():
    allowed = {
        "anomaly_type", "severity", "message", "timestamp", "window_start", "window_end", "client_ip",
        "latency_ms", "mean_latency_ms", "stddev_latency_ms", "request_count", "error_count",
        "unauthorized_count", "error_rate",
    }
    records = [rec(i * 0.1, 401, "6.6.6.6") for i in range(150)] + [rec(30 + i * 0.1, 500) for i in range(40)]
    for finding in WindowAnalyzer().ingest(records):
        assert set(finding) <= allowed
        assert finding["window_end"] > finding["window_start"]


def test_old_windows_are_evicted_and_late_records_ignored():
    analyzer = WindowAnalyzer(retain_windows=2)
    analyzer.ingest([rec(0), rec(600)])
    assert len(analyzer._windows) == 1
    assert analyzer.ingest([rec(1, 401, "1.1.1.1")] * 200) == []  # far too late
