// Believable sample data so the dashboard can be previewed without Elasticsearch.

function wobble(base, spread) {
  return Math.max(0, Math.round(base + (Math.random() - 0.5) * spread));
}

function demoData() {
  const now = Date.now();
  const requestsOverTime = Array.from({ length: 60 }, (_, i) => {
    const t = new Date(now - (59 - i) * 60000);
    const spike = i === 41 || i === 42 ? 4 : 1;
    return { label: t.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }), value: wobble(600, 120) * spike };
  });
  const anomalyTypes = ["brute_force", "traffic_surge", "error_burst", "latency_spike"];
  const severities = { brute_force: "critical", traffic_surge: "critical", error_burst: "high", latency_spike: "high" };
  const recentAnomalies = Array.from({ length: 12 }, (_, i) => {
    const type = anomalyTypes[i % 4];
    const start = new Date(Math.floor((now - i * 95000) / 60000) * 60000);
    return {
      window_start: start.toISOString(),
      window_end: new Date(start.getTime() + 60000).toISOString(),
      anomaly_type: type,
      severity: severities[type],
      client_ip: type === "traffic_surge" || type === "error_burst" ? null : `203.0.113.${20 + i}`,
      message: `${type.replace("_", " ")} detected in window (demo data)`,
    };
  });
  const total = requestsOverTime.reduce((sum, p) => sum + p.value, 0);
  return {
    mode: "demo",
    kpis: { totalRequests: total, totalAnomalies: 37, unauthorizedAttempts: Math.round(total * 0.04), avgLatency: 124.6, p50: 118, p95: 171, p99: 240 },
    requestsOverTime,
    statusDistribution: [["200", 0.7], ["201", 0.1], ["404", 0.1], ["204", 0.05], ["301", 0.05]].map(([label, share]) => ({ label, value: Math.round(total * share) })),
    anomalyTypes: [["brute_force", 11], ["error_burst", 9], ["traffic_surge", 5], ["latency_spike", 12]].map(([label, value]) => ({ label, value })),
    severity: [["critical", 16], ["high", 21]].map(([label, value]) => ({ label, value })),
    topClientIps: Array.from({ length: 6 }, (_, i) => ({ label: `203.0.113.${20 + i}`, value: 900 - i * 120 })),
    recentAnomalies,
  };
}

module.exports = { demoData };
