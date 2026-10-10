"""Synthetic web access logs with injectable anomaly scenarios.

Scenarios (each runs as a "burst" of consecutive anomalous records):
  latency      one record with 50-100x normal latency
  error-burst  a run of HTTP 500s from one IP
  brute-force  110-140 failed POST /login (401) from one IP
  ddos         a flood from 5 IPs, emitted ~20x faster than normal so window counts jump
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from typing import Callable, Iterator, TextIO

METHODS = ("GET", "POST", "PUT", "DELETE")
URLS = ("/", "/products", "/search", "/api/orders", "/health")
SCENARIOS = ("latency", "error-burst", "brute-force", "ddos")
FORMATS = ("json", "apache")
DDOS_SPEEDUP = 20


class LogGenerator:
    def __init__(
        self,
        rate: float = 10.0,
        anomaly_rate: float = 0.01,
        scenario: str | None = None,
        output_format: str = "json",
        seed: int | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be greater than zero")
        if not 0 <= anomaly_rate <= 1:
            raise ValueError("anomaly_rate must be between 0 and 1")
        if scenario is not None and scenario not in SCENARIOS:
            raise ValueError(f"unsupported scenario: {scenario}")
        if output_format not in FORMATS:
            raise ValueError(f"unsupported output format: {output_format}")
        self.rate = rate
        self.anomaly_rate = anomaly_rate
        self.scenario = scenario
        self.output_format = output_format
        self._rng = random.Random(seed)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._burst_left = 0
        self._burst_kind: str | None = None
        self._burst_ip = ""
        self._ddos_ips = tuple(self._ip() for _ in range(5))

    # -- record construction ---------------------------------------------------------------------
    def _ip(self) -> str:
        return ".".join(str(self._rng.randint(1, 254)) for _ in range(4))

    def _normal(self) -> dict[str, object]:
        return {
            "timestamp": self._clock().isoformat().replace("+00:00", "Z"),
            "client_ip": self._ip(),
            "method": self._rng.choice(METHODS),
            "url": self._rng.choice(URLS),
            "status_code": self._rng.choices((200, 201, 204, 301, 404), weights=(70, 10, 5, 5, 10))[0],
            "latency_ms": round(max(1.0, self._rng.gauss(120, 25)), 2),
            "user_id": f"user-{self._rng.randint(1000, 9999)}",
        }

    def _burst_length(self, kind: str) -> int:
        if kind == "brute-force":
            return self._rng.randint(110, 140)
        if kind == "error-burst":
            return self._rng.randint(30, 50)
        if kind == "ddos":
            return max(600, int(self.rate * 60 * 6))
        return 1

    def _begin_burst(self, kind: str) -> None:
        self._burst_kind = kind
        self._burst_ip = self._ip()
        self._burst_left = self._burst_length(kind)

    def _inject(self, record: dict[str, object], kind: str) -> dict[str, object]:
        if kind == "latency":
            record["latency_ms"] = round(self._rng.uniform(50, 100) * float(record["latency_ms"]), 2)
        elif kind == "error-burst":
            record.update(client_ip=self._burst_ip, status_code=500)
        elif kind == "brute-force":
            record.update(
                client_ip=self._burst_ip, method="POST", url="/login", status_code=401,
                latency_ms=round(self._rng.uniform(40, 100), 2),
            )
        elif kind == "ddos":
            record.update(client_ip=self._rng.choice(self._ddos_ips), url=self._rng.choice(("/", "/api/search", "/login")))
        return record

    def next_record(self) -> dict[str, object]:
        if self._burst_left == 0:
            if self.scenario is not None:
                self._begin_burst(self.scenario)
            elif self._rng.random() < self.anomaly_rate:
                self._begin_burst(self._rng.choice(SCENARIOS))
        record = self._normal()
        if self._burst_left > 0 and self._burst_kind:
            record = self._inject(record, self._burst_kind)
            self._burst_left -= 1
        return record

    # -- output ----------------------------------------------------------------------------------
    def delay(self) -> float:
        """Seconds to wait before the next record (shorter while a ddos burst is running)."""
        speedup = DDOS_SPEEDUP if self._burst_kind == "ddos" and self._burst_left > 0 else 1
        return 1.0 / (self.rate * speedup)

    def records(self, count: int | None = None) -> Iterator[dict[str, object]]:
        produced = 0
        while count is None or produced < count:
            yield self.next_record()
            produced += 1

    def render(self, record: dict[str, object]) -> str:
        if self.output_format == "json":
            return json.dumps(record, separators=(",", ":"))
        moment = datetime.fromisoformat(str(record["timestamp"]).replace("Z", "+00:00"))
        return (
            f'{record["client_ip"]} - {record["user_id"]} [{moment:%d/%b/%Y:%H:%M:%S %z}] '
            f'"{record["method"]} {record["url"]} HTTP/1.1" {record["status_code"]} '
            f'{self._rng.randint(256, 8192)} "-" "log-sentinel-generator/1.0"'
        )

    def run(self, out: TextIO, count: int | None = None, sleep: Callable[[float], None] = time.sleep) -> None:
        for record in self.records(count):
            out.write(self.render(record) + "\n")
            out.flush()
            sleep(self.delay())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rate", type=float, default=10.0, help="normal records per second (default 10)")
    parser.add_argument("--anomaly-rate", type=float, default=0.01, help="chance of starting a random burst per record")
    parser.add_argument("--scenario", choices=SCENARIOS, help="repeat one specific anomaly instead of random ones")
    parser.add_argument("--format", dest="output_format", choices=FORMATS, default="json")
    parser.add_argument("--count", type=int, help="stop after N records (default: run forever)")
    parser.add_argument("--seed", type=int, help="seed for repeatable output")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    generator = LogGenerator(args.rate, args.anomaly_rate, args.scenario, args.output_format, args.seed)
    try:
        generator.run(sys.stdout, args.count)
    except (KeyboardInterrupt, BrokenPipeError):
        pass


if __name__ == "__main__":
    main()
