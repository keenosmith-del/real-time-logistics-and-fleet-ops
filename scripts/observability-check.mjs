import assert from "node:assert/strict";
const p = await (
  await fetch("http://localhost:9090/api/v1/query?query=up")
).json();
assert.equal(p.status, "success");
assert(
  p.data.result.some(
    (x) => x.metric.job === "fleet-backend" && x.value[1] === "1",
  ),
);
assert(
  p.data.result.some((x) => x.metric.job === "redpanda" && x.value[1] === "1"),
);
const g = await (await fetch("http://localhost:3000/api/health")).json();
assert.equal(g.database, "ok");
const auth = Buffer.from(
  `admin:${process.env.GRAFANA_PASSWORD || "fleet-demo"}`,
).toString("base64");
const dashboard = await (
  await fetch("http://localhost:3000/api/dashboards/uid/fleet-operations", {
    headers: { Authorization: "Basic " + auth },
  })
).json();
assert(dashboard.dashboard.panels.length >= 10);
const metrics = await (await fetch("http://localhost:8080/metrics")).text();
assert(metrics.includes('fleet_consumer_lag{consumer="history-v1"} 0'));
assert(metrics.includes('fleet_consumer_lag{consumer="live-v1"} 0'));
assert(/^fleet_completed_deliveries \d+/m.test(metrics));
console.log(
  "PASS Prometheus backend/broker scrapes, broker-derived zero consumer lag, operational gauges, Grafana health and provisioned dashboard",
);
