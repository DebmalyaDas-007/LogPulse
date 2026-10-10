// Aggregates Elasticsearch data for the web dashboard. Works as a plain function (server.js) and
// as a Vercel serverless handler (module.exports). Requires Node 18+ for the global fetch.

const { demoData } = require("./demo");

function esConfig() {
  const headers = { "content-type": "application/json" };
  if (process.env.ELASTICSEARCH_API_KEY) headers.authorization = `ApiKey ${process.env.ELASTICSEARCH_API_KEY}`;
  else if (process.env.ELASTICSEARCH_USERNAME) {
    const pair = `${process.env.ELASTICSEARCH_USERNAME}:${process.env.ELASTICSEARCH_PASSWORD || ""}`;
    headers.authorization = `Basic ${Buffer.from(pair).toString("base64")}`;
  }
  return { url: (process.env.ELASTICSEARCH_URL || "http://localhost:9200").replace(/\/$/, ""), headers };
}

async function search(index, body) {
  const { url, headers } = esConfig();
  const response = await fetch(`${url}/${index}/_search?ignore_unavailable=true&allow_no_indices=true`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(8000),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Elasticsearch ${response.status}: ${text.slice(0, 200)}`);
  return JSON.parse(text);
}

const topN = (field, size = 8) => ({ terms: { field, size } });
const buckets = (result, name) => (result.aggregations?.[name]?.buckets || []).map((b) => ({ label: String(b.key), value: b.doc_count }));

async function getDashboardData() {
  if (process.env.DEMO_MODE === "true") return demoData();

  const lastHour = { range: { timestamp: { gte: "now-1h" } } };
  const [logs, anomalies, recent, trend] = await Promise.all([
    search("server-logs-*", {
      size: 0,
      track_total_hits: true,
      aggs: {
        status: topN("status_code", 10),
        ips: topN("client_ip", 8),
        unauthorized: { filter: { terms: { status_code: [401, 403] } } },
        latency: { avg: { field: "latency_ms" } },
        pct: { percentiles: { field: "latency_ms", percents: [50, 95, 99] } },
      },
    }),
    search("anomalies-*", {
      size: 0,
      track_total_hits: true,
      aggs: { types: topN("anomaly_type"), severity: topN("severity") },
    }),
    search("anomalies-*", {
      size: 12,
      sort: [{ window_start: { order: "desc" } }],
      _source: ["window_start", "window_end", "anomaly_type", "severity", "client_ip", "message"],
    }),
    search("server-logs-*", {
      size: 0,
      query: lastHour,
      aggs: { t: { date_histogram: { field: "timestamp", fixed_interval: "1m", min_doc_count: 0 } } },
    }),
  ]);

  const pct = logs.aggregations?.pct?.values || {};
  return {
    mode: "live",
    kpis: {
      totalRequests: logs.hits.total.value,
      totalAnomalies: anomalies.hits.total.value,
      unauthorizedAttempts: logs.aggregations?.unauthorized?.doc_count || 0,
      avgLatency: logs.aggregations?.latency?.value || 0,
      p50: pct["50.0"] || 0,
      p95: pct["95.0"] || 0,
      p99: pct["99.0"] || 0,
    },
    requestsOverTime: (trend.aggregations?.t?.buckets || []).map((b) => ({
      label: new Date(b.key).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
      value: b.doc_count,
    })),
    statusDistribution: buckets(logs, "status"),
    topClientIps: buckets(logs, "ips"),
    anomalyTypes: buckets(anomalies, "types"),
    severity: buckets(anomalies, "severity"),
    recentAnomalies: recent.hits.hits.map((h) => h._source),
  };
}

async function handler(req, res) {
  try {
    const data = await getDashboardData();
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Cache-Control", "no-store");
    res.end(JSON.stringify(data));
  } catch (error) {
    res.statusCode = 502;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: error.message }));
  }
}

module.exports = handler;
module.exports.getDashboardData = getDashboardData;
