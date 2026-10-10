"""Pure anomaly rules. No Spark, Kafka or Elasticsearch imports, so they are trivial to unit test."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Thresholds:
    sigma: float = 3.0                    # latency spike: value > mean + sigma * stddev
    min_latency_samples: int = 30         # don't judge latency until the window has this many samples
    unauthorized_per_ip: int = 100        # brute force: 401/403 count per IP per window
    error_rate: float = 0.01              # error burst: share of 5xx responses in a window
    min_requests_for_error_rate: int = 20 # avoid flagging 1 error out of 3 requests
    surge_multiplier: float = 5.0         # traffic surge: window count > multiplier * trailing baseline


def _finding(anomaly_type: str, severity: str, message: str) -> dict[str, str]:
    return {"anomaly_type": anomaly_type, "severity": severity, "message": message}


def latency_spike(latency_ms: float, mean_ms: float, stddev_ms: float, sigma: float = 3.0) -> dict[str, str] | None:
    limit = mean_ms + sigma * stddev_ms
    if latency_ms > limit:
        return _finding(
            "latency_spike",
            "high",
            f"latency {latency_ms:.1f}ms is above the {sigma:g}-sigma limit of {limit:.1f}ms",
        )
    return None


def brute_force(unauthorized_count: int, limit: int = 100) -> dict[str, str] | None:
    if unauthorized_count > limit:
        return _finding(
            "brute_force",
            "critical",
            f"{unauthorized_count} HTTP 401/403 responses from a single IP in one window",
        )
    return None


def error_burst(error_count: int, request_count: int, rate_limit: float = 0.01, min_requests: int = 1) -> dict[str, str] | None:
    if request_count < max(1, min_requests):
        return None
    rate = error_count / request_count
    if rate > rate_limit:
        return _finding("error_burst", "high", f"{rate:.1%} of requests returned HTTP 5xx ({error_count}/{request_count})")
    return None


def traffic_surge(request_count: int, baseline: float, multiplier: float = 5.0) -> dict[str, str] | None:
    if baseline > 0 and request_count > multiplier * baseline:
        return _finding(
            "traffic_surge",
            "critical",
            f"{request_count} requests in one window is more than {multiplier:g}x the baseline of {baseline:.0f}",
        )
    return None
