"""Parse one raw log line (JSON or Apache Combined Log Format) into a LogRecord."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone

APACHE_RE = re.compile(
    r'^(?P<ip>\S+) \S+ (?P<user>\S+) \[(?P<ts>[^\]]+)\] '
    r'"(?P<method>[A-Z]+) (?P<url>\S+)[^"]*" (?P<status>\d{3}) '
)


@dataclass(frozen=True)
class LogRecord:
    timestamp: datetime  # timezone-aware, UTC
    client_ip: str
    method: str
    url: str
    status_code: int
    latency_ms: float
    user_id: str | None

    def to_document(self) -> dict[str, object]:
        """Shape matching the strict `server-logs-*` Elasticsearch mapping."""
        return {
            "timestamp": self.timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "client_ip": self.client_ip,
            "method": self.method,
            "url": self.url,
            "status_code": self.status_code,
            "latency_ms": self.latency_ms,
            "user_id": self.user_id,
        }


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _parse_json(line: str) -> LogRecord | None:
    try:
        data = json.loads(line)
        return LogRecord(
            timestamp=_parse_iso(str(data["timestamp"])),
            client_ip=str(data["client_ip"]),
            method=str(data["method"]),
            url=str(data["url"]),
            status_code=int(data["status_code"]),
            latency_ms=float(data["latency_ms"]),
            user_id=(str(data["user_id"]) if data.get("user_id") not in (None, "", "-") else None),
        )
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


def _parse_apache(line: str) -> LogRecord | None:
    match = APACHE_RE.match(line)
    if not match:
        return None
    try:
        timestamp = datetime.strptime(match["ts"], "%d/%b/%Y:%H:%M:%S %z")
    except ValueError:
        return None
    user = match["user"]
    return LogRecord(
        timestamp=timestamp.astimezone(timezone.utc),
        client_ip=match["ip"],
        method=match["method"],
        url=match["url"],
        status_code=int(match["status"]),
        latency_ms=0.0,  # Combined Log Format carries no response time
        user_id=None if user == "-" else user,
    )


def parse_line(line: str | bytes | None) -> LogRecord | None:
    """Return a LogRecord, or None when the line is malformed. Never raises."""
    if line is None:
        return None
    if isinstance(line, bytes):
        line = line.decode("utf-8", errors="replace")
    line = line.strip()
    if not line:
        return None
    return _parse_json(line) if line.startswith("{") else _parse_apache(line)
