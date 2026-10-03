import assert from "node:assert/strict";
import fs from "node:fs";
const base = process.env.API_URL || "http://localhost:8080";
async function call(path, body, method = "POST") {
  const r = await fetch(
    base + "/api" + path,
    body === undefined
      ? {}
      : {
          method,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
  );
  const d = await r.json();
  if (!r.ok) throw new Error(`${path}: ${r.status} ${JSON.stringify(d)}`);
  return d;
}
async function until(fn, description, timeout = 60000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    const value = await fn();
    if (value) return value;
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error("Timed out: " + description);
}
if (process.argv.includes("--verify")) {
  await until(async () => {
    try {
      return Object.values(await call("/health")).every(Boolean);
    } catch {
      return false;
    }
  }, "restarted dependencies healthy");
  const s = await call("/snapshot");
  const saved = JSON.parse(
    fs.readFileSync("docs/smoke-checkpoint.json", "utf8"),
  );
  assert(
    s.deliveries.some(
      (d) => d.id === saved.delivery && d.status === "completed",
    ),
  );
  assert(s.alerts.some((a) => a.id === saved.alert && a.status === "resolved"));
  assert(s.assignments.filter((a) => a.delivery_id === "DEL-001").length >= 2);
  const h = await call(`/vehicles/VH-001/history?journey_id=${saved.journey}`);
  assert(h.events.length >= 2);
  assert(s.metrics.telemetry_total >= saved.telemetry);
  console.log(
    "PASS durable deliveries, alert history, assignments and telemetry after restart",
  );
  process.exit(0);
}
let socket;
try {
  const health = await call("/health");
  assert(Object.values(health).every(Boolean));
  await call("/seed", {});
  let s = await call("/snapshot");
  assert(s.vehicles.length >= 120);
  assert(s.drivers.length >= 120);
  assert(
    s.routes.filter((r) => r.status === "assigned").length >= 100,
    "Run smoke on a fresh demonstration seed",
  );
  let live = new Set();
  socket = new WebSocket(base.replace("http", "ws") + "/api/ws");
  await new Promise((resolve, reject) => {
    socket.onopen = resolve;
    socket.onerror = reject;
  });
  socket.onmessage = (m) => {
    const event = JSON.parse(m.data);
    if (event.type === "position") live.add(event.data.vehicle_id);
  };
  await call("/simulation/configure", { speed: 1, stop_seconds: 2 });
  await call("/simulation/start", { count: 100, speed: 1 });
  await until(() => live.size >= 100, "100 live vehicles");
  s = await call("/snapshot");
  assert(s.metrics.running_journeys >= 100);
  console.log("PASS 100 concurrent GPS streams through Redpanda and WebSocket");
  await until(async () => {
    const v = (await call("/snapshot")).vehicles.find((v) => v.id === "VH-001");
    return v.last_position?.seq >= 3;
  }, "durable GPS samples");
  await call("/vehicles/VH-001/scenario", { scenario: "breakdown" });
  await call("/vehicles/VH-002/scenario", { scenario: "deviation" });
  await call("/vehicles/VH-003/scenario", { scenario: "delay" });
  await call("/vehicles/VH-004/scenario", { scenario: "stop" });
  const alert = await until(async () => {
    const snap = await call("/snapshot");
    return snap.alerts.find(
      (a) => a.vehicle_id === "VH-001" && a.type === "breakdown",
    );
  }, "persistent breakdown");
  await until(async () => {
    const x = (await call("/snapshot")).alerts;
    return ["route_deviation", "delay", "prolonged_stop"].every((t) =>
      x.some((a) => a.type === t),
    );
  }, "all exception detectors");
  console.log(
    "PASS breakdown, geographic deviation, deadline and stop detectors",
  );
  await call(`/alerts/${alert.id}/acknowledge`, {
    note: "Smoke: dispatcher accepted",
  });
  await call("/deliveries/DEL-001/reassign", {
    vehicle_id: "VH-101",
    driver_id: "DR-101",
    reason: "Smoke: recovery vehicle dispatched",
  });
  await until(async () => {
    const d = (await call("/snapshot")).deliveries.find(
      (d) => d.id === "DEL-001",
    );
    return d.status === "in_transit";
  }, "reassigned delivery continues");
  await call("/deliveries/DEL-001/complete", {
    outcome: "Smoke: handed to recipient",
  });
  await call(`/alerts/${alert.id}/resolve`, {
    note: "Smoke: delivery recovered; vehicle requires repair",
  });
  const history = await call(`/alerts/${alert.id}/history`);
  assert.deepEqual(
    history.map((h) => h.action),
    ["detected", "acknowledge", "resolve"],
  );
  const stamp = Date.now();
  const del = await call("/deliveries", {
    reference: "SMOKE-" + stamp,
    customer: "Smoke customer",
    address: "18 Market Street Johannesburg",
    lat: -26.2041,
    lon: 28.0483,
    priority: "high",
    scheduled_at: new Date(Date.now() - 60000).toISOString(),
    due_at: new Date(Date.now() + 3600000).toISOString(),
  });
  await call(
    `/deliveries/${del.id}`,
    {
      reference: "SMOKE-" + stamp,
      customer: "Updated smoke customer",
      address: "18 Market Street Johannesburg",
      lat: -26.2041,
      lon: 28.0483,
      priority: "urgent",
      scheduled_at: new Date(Date.now() - 60000).toISOString(),
      due_at: new Date(Date.now() + 3600000).toISOString(),
    },
    "PATCH",
  );
  const route = await call("/routes", {
    name: "Smoke short local route",
    depot_id: "depot-jhb",
    delivery_ids: [del.id],
  });
  await call(`/routes/${route.id}/assign`, {
    vehicle_id: "VH-102",
    driver_id: "DR-102",
  });
  let conflict = false;
  try {
    await call("/deliveries/DEL-002/reassign", {
      vehicle_id: "VH-102",
      driver_id: "DR-102",
      reason: "Must reject occupied vehicle",
    });
  } catch {
    conflict = true;
  }
  assert(conflict);
  await call(`/routes/${route.id}/dispatch`, {});
  await call("/simulation/pause", {});
  s = await call("/snapshot");
  assert.equal(s.settings[0].sim_state, "paused");
  await call("/simulation/resume", {});
  await call("/simulation/configure", { speed: 25 });
  await until(async () => {
    const x = (await call("/snapshot")).deliveries.find((d) => d.id === del.id);
    return x.status === "completed";
  }, "automatic arrival completion");
  await call("/simulation/pause", {});
  s = await call("/snapshot");
  const journeys = await call("/vehicles/VH-001/journeys");
  const h = await call(`/vehicles/VH-001/history?journey_id=${journeys[0].id}`);
  assert(h.events.length >= 3);
  assert(
    h.events.every(
      (e, i) => i === 0 || e.timestamp >= h.events[i - 1].timestamp,
    ),
  );
  assert(
    s.assignments
      .filter((a) => a.delivery_id === "DEL-001")
      .some((a) => a.ended_at),
  );
  assert(
    s.assignments
      .filter((a) => a.delivery_id === "DEL-001")
      .some((a) => a.vehicle_id === "VH-101"),
  );
  fs.writeFileSync(
    "docs/smoke-checkpoint.json",
    JSON.stringify(
      {
        delivery: del.id,
        alert: alert.id,
        journey: journeys[0].id,
        telemetry: s.metrics.telemetry_total,
        verified_at: new Date().toISOString(),
      },
      null,
      2,
    ),
  );
  console.log(
    "PASS create/edit/route/assign/dispatch, occupied-resource rejection, alert acknowledge/reassign/resolve, pause/resume, automatic completion and historical replay",
  );
  console.log("Focused smoke test passed; platform left paused.");
} finally {
  socket?.close();
}
