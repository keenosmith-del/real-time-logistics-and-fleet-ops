import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
const base = process.env.API_URL || "http://localhost:8080";
const get = async (p) => {
  const r = await fetch(base + p);
  assert(r.ok);
  return r.json();
};
const wait = async (fn) => {
  for (let i = 0; i < 40; i++) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error("Stream assertion timed out");
};
function publish(value, key) {
  const p = spawnSync(
    "docker",
    [
      "compose",
      "exec",
      "-T",
      "redpanda",
      "rpk",
      "topic",
      "produce",
      "fleet.telemetry.v1",
      "-k",
      key,
    ],
    { input: JSON.stringify(value) + "\n", encoding: "utf8" },
  );
  assert.equal(p.status, 0, p.stderr);
}
const s = await get("/api/snapshot");
assert.equal(
  s.settings[0].sim_state,
  "paused",
  "Pause the demonstration first",
);
const e = s.vehicles.find((v) => v.id === "VH-002").last_position;
assert(e, "Run the main smoke first");
const history = async () =>
  get(`/api/vehicles/${e.vehicle_id}/history?journey_id=${e.journey_id}`);
const before = (await history()).events.length;
publish(e, e.vehicle_id);
await new Promise((r) => setTimeout(r, 1500));
assert.equal(
  (await history()).events.length,
  before,
  "Duplicate event produced another telemetry row",
);
const late = {
  ...e,
  event_id: "late-test-" + Date.now(),
  timestamp: new Date(new Date(e.timestamp).getTime() - 120000).toISOString(),
  seq: Math.max(1, e.seq - 1),
};
publish(late, e.vehicle_id);
await wait(async () =>
  (await history()).events.some((p) => p.event_id === late.event_id),
);
const after = await get("/api/snapshot");
assert.equal(
  after.vehicles.find((v) => v.id === e.vehicle_id).last_position.event_id,
  e.event_id,
  "Late GPS rewound durable current position",
);
assert.equal(
  after.positions.find((v) => v.vehicle_id === e.vehicle_id).event_id,
  e.event_id,
  "Late GPS rewound Redis current position",
);
publish({ ...e, event_id: "bad-test-" + Date.now(), lat: 100 }, e.vehicle_id);
publish(
  { ...e, event_id: "unknown-test-" + Date.now(), journey_id: "unknown" },
  e.vehicle_id,
);
await wait(async () => {
  const text = await (await fetch(base + "/metrics")).text();
  return ["history-v1", "live-v1"].every((group) => {
    const line = text
      .split("\n")
      .find(
        (l) =>
          l.startsWith("fleet_events_total{") &&
          l.includes(`consumer="${group}"`) &&
          l.includes('result="dead_letter"'),
      );
    return Number(line?.split(" ").at(-1) || 0) >= 2;
  });
});
console.log(
  "PASS duplicate idempotency, stale GPS ordering, schema DLQ and invalid-context DLQ in both consumer groups",
);
