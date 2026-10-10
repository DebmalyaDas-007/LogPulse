"""Create the two Kibana data views (server-logs-*, anomalies-*). Safe to re-run."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

KIBANA = os.getenv("KIBANA_URL", "http://localhost:5601").rstrip("/")
VIEWS = [
    {"id": "sentinel-server-logs", "name": "server-logs-*", "title": "server-logs-*", "timeFieldName": "timestamp"},
    {"id": "sentinel-anomalies", "name": "anomalies-*", "title": "anomalies-*", "timeFieldName": "window_start"},
]


def create_view(view: dict) -> str:
    request = urllib.request.Request(
        f"{KIBANA}/api/data_views/data_view",
        data=json.dumps({"data_view": view, "override": True}).encode(),
        headers={"Content-Type": "application/json", "kbn-xsrf": "true"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return f"ok ({response.status})"
    except urllib.error.HTTPError as exc:
        return f"HTTP {exc.code}: {exc.read().decode()[:200]}"
    except urllib.error.URLError as exc:
        return f"unreachable: {exc.reason}"


def main() -> int:
    failed = False
    for view in VIEWS:
        status = create_view(view)
        print(f"{view['title']:<14} {status}")
        failed |= not status.startswith("ok")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
