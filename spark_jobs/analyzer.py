"""Stateful, event-time window analyzer.

Spark delivers records in micro-batches, but a one-minute window usually spans several
batches. This analyzer keeps running statistics per window across batches and emits each
finding once, the moment a window crosses a threshold.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from .log_parser import LogRecord
from .rules import Thresholds, brute_force, error_burst, latency_spike, traffic_surge


@dataclass
class WindowStats:
    count: int = 0
    errors: int = 0
    unauthorized: Counter = field(default_factory=Counter)
    # Welford running mean / variance over latencies that were not flagged as spikes
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def add_latency(self, value: float) -> None:
        self.n += 1
        delta = value - self.mean
        self.mean += delta / self.n
        self.m2 += delta * (value - self.mean)

    @property
    def stddev(self) -> float:
        return math.sqrt(self.m2 / self.n) if self.n > 1 else 0.0


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class WindowAnalyzer:
    def __init__(self, window_seconds: int = 60, thresholds: Thresholds | None = None, baseline_windows: int = 5, retain_windows: int = 15):
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window_seconds = window_seconds
        self.thresholds = thresholds or Thresholds()
        self.baseline_windows = baseline_windows
        self.retain_windows = retain_windows
        self._windows: dict[datetime, WindowStats] = {}
        self._emitted: set[tuple] = set()
        self._latest: datetime | None = None

    # -- helpers ---------------------------------------------------------------------------------
    def _window_start(self, moment: datetime) -> datetime:
        epoch = int(moment.timestamp())
        return datetime.fromtimestamp(epoch - epoch % self.window_seconds, tz=timezone.utc)

    def _baseline(self, start: datetime) -> float:
        earlier = sorted((s for s in self._windows if s < start), reverse=True)[: self.baseline_windows]
        counts = [self._windows[s].count for s in earlier]
        return sum(counts) / len(counts) if counts else 0.0

    def _evict(self) -> None:
        if self._latest is None:
            return
        cutoff = self._latest - timedelta(seconds=self.window_seconds * self.retain_windows)
        for start in [s for s in self._windows if s < cutoff]:
            del self._windows[start]
        self._emitted = {key for key in self._emitted if key[1] >= cutoff}

    def _emit(self, findings: list[dict[str, Any]], key: tuple, start: datetime, result: dict[str, str], **extra: Any) -> None:
        if key in self._emitted:
            return
        self._emitted.add(key)
        end = start + timedelta(seconds=self.window_seconds)
        findings.append({
            **result,
            "timestamp": extra.pop("timestamp", _iso(start)),
            "window_start": _iso(start),
            "window_end": _iso(end),
            **extra,
        })

    # -- main entry ------------------------------------------------------------------------------
    def ingest(self, records: Iterable[LogRecord]) -> list[dict[str, Any]]:
        t = self.thresholds
        findings: list[dict[str, Any]] = []

        for record in sorted(records, key=lambda r: r.timestamp):
            start = self._window_start(record.timestamp)
            if self._latest is not None and start < self._latest - timedelta(seconds=self.window_seconds * self.retain_windows):
                continue  # too late, its window has already been evicted
            if self._latest is None or start > self._latest:
                self._latest = start
            stats = self._windows.setdefault(start, WindowStats())

            # Latency is judged against what the window looked like *before* this record.
            flagged = False
            if stats.n >= t.min_latency_samples:
                spike = latency_spike(record.latency_ms, stats.mean, stats.stddev, t.sigma)
                if spike:
                    flagged = True
                    findings_key = ("latency", start, record.timestamp, record.client_ip, record.latency_ms)
                    self._emit(
                        findings, findings_key, start, spike,
                        timestamp=_iso(record.timestamp),
                        client_ip=record.client_ip,
                        latency_ms=record.latency_ms,
                        mean_latency_ms=round(stats.mean, 3),
                        stddev_latency_ms=round(stats.stddev, 3),
                    )
            if not flagged and record.latency_ms > 0:
                stats.add_latency(record.latency_ms)

            stats.count += 1
            if record.status_code >= 500:
                stats.errors += 1
            if record.status_code in (401, 403):
                stats.unauthorized[record.client_ip] += 1
                result = brute_force(stats.unauthorized[record.client_ip], t.unauthorized_per_ip)
                if result:
                    self._emit(
                        findings, ("brute_force", start, record.client_ip), start, result,
                        client_ip=record.client_ip,
                        unauthorized_count=stats.unauthorized[record.client_ip],
                    )

            result = error_burst(stats.errors, stats.count, t.error_rate, t.min_requests_for_error_rate)
            if result:
                self._emit(
                    findings, ("error_burst", start), start, result,
                    request_count=stats.count,
                    error_count=stats.errors,
                    error_rate=round(stats.errors / stats.count, 5),
                )

            result = traffic_surge(stats.count, self._baseline(start), t.surge_multiplier)
            if result:
                self._emit(findings, ("traffic_surge", start), start, result, request_count=stats.count)

        self._evict()
        return findings
