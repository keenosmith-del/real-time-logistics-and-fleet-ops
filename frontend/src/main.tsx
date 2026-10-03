import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { FleetMap } from "./Map";
import { api, type Snapshot, type Position, type Journey } from "./types";
import "./style.css";
const empty: Snapshot = {
  depots: [],
  vehicles: [],
  drivers: [],
  deliveries: [],
  routes: [],
  alerts: [],
  assignments: [],
  positions: [],
  settings: [],
  metrics: {},
};
const date = (s: string | null) => (s ? new Date(s).toLocaleString() : "—");
const localDate = (d: Date) =>
  new Date(d.getTime() - d.getTimezoneOffset() * 60000)
    .toISOString()
    .slice(0, 16);
function Badge({ value }: { value: string }) {
  return <span className={"badge " + value}>{value.replaceAll("_", " ")}</span>;
}
function App() {
  const [data, setData] = useState(empty),
    [positions, setPositions] = useState<Record<string, Position>>({}),
    [tab, setTab] = useState("Live operations"),
    [vehicle, setVehicle] = useState(""),
    [online, setOnline] = useState(false),
    [error, setError] = useState(""),
    [notice, setNotice] = useState(""),
    [busy, setBusy] = useState(false),
    [filter, setFilter] = useState(""),
    [statusFilter, setStatusFilter] = useState("all");
  const [speed, setSpeed] = useState(1),
    [count, setCount] = useState(100),
    [deviation, setDeviation] = useState(350),
    [stopSeconds, setStopSeconds] = useState(20),
    [delaySeconds, setDelaySeconds] = useState(0);
  const [deliveryForm, setDeliveryForm] = useState({
      reference: "",
      customer: "",
      address: "",
      lat: -26.19,
      lon: 28.06,
      priority: "normal",
      scheduled_at: localDate(new Date()),
      due_at: localDate(new Date(Date.now() + 7200000)),
    }),
    [editing, setEditing] = useState("");
  const [routeName, setRouteName] = useState(""),
    [depot, setDepot] = useState("depot-jhb"),
    [stops, setStops] = useState<string[]>([]),
    [route, setRoute] = useState(""),
    [assignVehicle, setAssignVehicle] = useState(""),
    [driver, setDriver] = useState("");
  const [reassignDelivery, setReassignDelivery] = useState(""),
    [replacement, setReplacement] = useState(""),
    [replacementDriver, setReplacementDriver] = useState(""),
    [reason, setReason] = useState("Recovery after operational disruption");
  const [note, setNote] = useState("Dispatcher reviewed"),
    [alertHistory, setAlertHistory] = useState<
      { action: string; note: string; at: string }[]
    >([]),
    [historyTitle, setHistoryTitle] = useState("");
  const [journeys, setJourneys] = useState<Journey[]>([]),
    [journey, setJourney] = useState(""),
    [replay, setReplay] = useState<Position[]>([]),
    [frame, setFrame] = useState(0),
    [playing, setPlaying] = useState(false),
    [playSpeed, setPlaySpeed] = useState(1),
    [replayBusy, setReplayBusy] = useState(false),
    [health, setHealth] = useState<Record<string, boolean>>({}),
    [prom, setProm] = useState("");
  const [registryKind, setRegistryKind] = useState("vehicle"),
    [registryID, setRegistryID] = useState(""),
    [registryName, setRegistryName] = useState(""),
    [registryDepot, setRegistryDepot] = useState("depot-jhb"),
    [registryCapacity, setRegistryCapacity] = useState(3500);
  const refresh = async () => {
    const s = await api<Snapshot>("/snapshot");
    setData(s);
    setPositions((prev) => {
      const next = { ...prev };
      for (const p of s.positions) {
        if (
          !next[p.vehicle_id] ||
          new Date(next[p.vehicle_id].timestamp) <= new Date(p.timestamp)
        )
          next[p.vehicle_id] = p;
      }
      return next;
    });
  };
  const refreshRef = useRef(refresh);
  refreshRef.current = refresh;
  useEffect(() => {
    let alive = true,
      socket: WebSocket | null = null,
      reconnect: ReturnType<typeof setTimeout>,
      change: ReturnType<typeof setTimeout> | undefined;
    let backoff = 500;
    const load = () =>
      refreshRef.current().catch((e) => alive && setError(e.message));
    load();
    const poll = setInterval(load, 10000);
    const connect = () => {
      if (!alive) return;
      socket = new WebSocket(
        `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/api/ws`,
      );
      socket.onopen = () => {
        setOnline(true);
        backoff = 500;
        load();
      };
      socket.onmessage = (event) => {
        try {
          const m = JSON.parse(event.data);
          if (m.type === "position") {
            const p: Position = m.data;
            setPositions((prev) =>
              !prev[p.vehicle_id] ||
              new Date(p.timestamp) > new Date(prev[p.vehicle_id].timestamp)
                ? { ...prev, [p.vehicle_id]: p }
                : prev,
            );
          } else if (m.type === "change" && !change) {
            change = setTimeout(() => {
              change = undefined;
              load();
            }, 500);
          }
        } catch {
          /* malformed messages do not interrupt the connection */
        }
      };
      socket.onclose = () => {
        setOnline(false);
        if (alive) {
          reconnect = setTimeout(connect, backoff);
          backoff = Math.min(backoff * 2, 10000);
        }
      };
      socket.onerror = () => socket?.close();
    };
    connect();
    return () => {
      alive = false;
      clearInterval(poll);
      clearTimeout(reconnect);
      clearTimeout(change);
      socket?.close();
    };
  }, []);
  useEffect(() => {
    const s = data.settings[0];
    if (s) {
      setSpeed(s.speed);
      setDeviation(s.deviation_m);
      setStopSeconds(s.stop_seconds);
      setDelaySeconds(s.delay_seconds);
    }
  }, [data.settings]);
  const act = async (work: () => Promise<unknown>, message = "Saved") => {
    if (busy) return;
    setBusy(true);
    setError("");
    try {
      await work();
      await refresh();
      setNotice(message);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  const current = positions[vehicle],
    selectedVehicle = data.vehicles.find((v) => v.id === vehicle),
    selectedRoute =
      data.routes.find((r) => r.id === current?.route_id) ||
      data.routes.find(
        (r) =>
          r.vehicle_id === vehicle &&
          ["assigned", "active", "paused", "disrupted"].includes(r.status),
      );
  const visibleDeliveries = data.deliveries.filter(
    (d) =>
      (statusFilter === "all" || d.status === statusFilter) &&
      `${d.reference} ${d.customer} ${d.id}`
        .toLowerCase()
        .includes(filter.toLowerCase()),
  );
  const activeAlerts = data.alerts.filter((a) => a.status !== "resolved");
  const availableVehicles = data.vehicles.filter(
      (v) => v.status === "available",
    ),
    availableDrivers = data.drivers.filter((v) => v.status === "available");
  const mapPositions = useMemo(() => Object.values(positions), [positions]);
  useEffect(() => {
    let live = true;
    setJourneys([]);
    setJourney("");
    setReplay([]);
    setPlaying(false);
    if (vehicle)
      api<Journey[]>(`/vehicles/${vehicle}/journeys`)
        .then((j) => {
          if (live) setJourneys(j);
        })
        .catch((e) => setError(e.message));
    return () => {
      live = false;
    };
  }, [vehicle, tab]);
  useEffect(() => {
    let alive = true;
    setPlaying(false);
    setFrame(0);
    setReplay([]);
    if (!vehicle || !journey) return;
    setReplayBusy(true);
    (async () => {
      let all: Position[] = [];
      let after = "";
      for (let page = 0; page < 1000; page++) {
        const d = await api<{ events: Position[]; has_more: boolean }>(
          `/vehicles/${vehicle}/history?journey_id=${encodeURIComponent(journey)}${after ? "&after=" + encodeURIComponent(after) : ""}`,
        );
        if (!alive) return;
        all = all.concat(d.events);
        if (!d.has_more || !d.events.length) break;
        after = d.events[d.events.length - 1].timestamp;
      }
      if (alive) setReplay(all);
    })()
      .catch((e) => alive && setError(e.message))
      .finally(() => alive && setReplayBusy(false));
    return () => {
      alive = false;
    };
  }, [journey, vehicle]);
  useEffect(() => {
    if (!playing || replay.length < 2) return;
    let last = performance.now();
    let elapsed = 0;
    const timer = setInterval(() => {
      const now = performance.now();
      elapsed += (now - last) * playSpeed;
      last = now;
      setFrame((previous) => {
        let n = previous;
        while (
          n < replay.length - 1 &&
          elapsed >=
            new Date(replay[n + 1].timestamp).getTime() -
              new Date(replay[n].timestamp).getTime()
        ) {
          elapsed -= Math.max(
            1,
            new Date(replay[n + 1].timestamp).getTime() -
              new Date(replay[n].timestamp).getTime(),
          );
          n++;
        }
        if (n === replay.length - 1) setPlaying(false);
        return n;
      });
    }, 100);
    return () => clearInterval(timer);
  }, [playing, playSpeed, replay]);
  useEffect(() => {
    if (tab !== "Analytics") return;
    api<Record<string, boolean>>("/health")
      .then(setHealth)
      .catch(() =>
        setHealth({ database: false, redis: false, redpanda: false }),
      );
    fetch("/metrics")
      .then((r) => r.text())
      .then(setProm)
      .catch((e) => setError(e.message));
  }, [tab, data.metrics]);
  const rates = useMemo(() => {
    const value = (name: string) =>
      prom
        .split("\n")
        .filter((l) => l.startsWith(name) && !l.startsWith(name + "_"))
        .reduce((n, l) => n + Number(l.split(" ").at(-1) || 0), 0);
    const sum = Number(
        prom.match(/^fleet_processing_seconds_sum (.+)$/m)?.[1] || 0,
      ),
      n = Number(
        prom.match(/^fleet_processing_seconds_count (.+)$/m)?.[1] || 0,
      );
    return {
      lag: value("fleet_consumer_lag"),
      latency: n ? sum / n : 0,
      retries: prom
        .split("\n")
        .filter(
          (l) =>
            l.startsWith("fleet_events_total") && l.includes('result="retry"'),
        )
        .reduce((n, l) => n + Number(l.split(" ").at(-1)), 0),
    };
  }, [prom]);
  return (
    <div className="shell">
      <header>
        <div>
          <p className="eyebrow">PROJECT 06 · GAUTENG LOGISTICS</p>
          <h1>Fleet Operations Control</h1>
        </div>
        <div className="connection">
          <span className={online ? "dot online" : "dot"} />
          {online ? "Live stream connected" : "Reconnecting stream"}
          <small>Simulation: {data.settings[0]?.sim_state || "loading"}</small>
        </div>
      </header>
      <nav>
        {[
          "Live operations",
          "Dispatch",
          "Fleet registry",
          "Exceptions",
          "Journey replay",
          "Analytics",
        ].map((t) => (
          <button
            className={tab === t ? "selected" : ""}
            key={t}
            onClick={() => setTab(t)}
          >
            {t}
            {t === "Exceptions" && activeAlerts.length > 0 && (
              <b>{activeAlerts.length}</b>
            )}
          </button>
        ))}
      </nav>
      {error && (
        <div role="alert" className="error">
          {error}
          <button onClick={() => setError("")}>Dismiss</button>
        </div>
      )}
      {notice && (
        <div role="status" className="notice">
          {notice}
          <button onClick={() => setNotice("")}>Dismiss</button>
        </div>
      )}
      <div className="metrics">
        {Object.entries(data.metrics)
          .slice(0, 6)
          .map(([k, v]) => (
            <div key={k}>
              <strong>{v}</strong>
              <span>{k.replaceAll("_", " ")}</span>
            </div>
          ))}
      </div>
      {tab === "Live operations" && (
        <>
          <section className="panel">
            <h2>Simulation control</h2>
            <div className="toolbar">
              <label>
                Vehicles
                <input
                  type="number"
                  min="1"
                  max="1000"
                  value={count}
                  onChange={(e) => setCount(+e.target.value)}
                />
              </label>
              <label>
                Simulation speed
                <select
                  value={speed}
                  onChange={(e) => setSpeed(+e.target.value)}
                >
                  {[0.1, 1, 2, 5, 10, 25, 50, 100].map((n) => (
                    <option key={n} value={n}>
                      {n}×
                    </option>
                  ))}
                </select>
              </label>
              {["start", "pause", "resume", "stop"].map((a) => (
                <button
                  disabled={busy}
                  key={a}
                  onClick={() =>
                    act(
                      () => api(`/simulation/${a}`, { speed, count }),
                      `Simulation ${a}`,
                    )
                  }
                >
                  {a}
                </button>
              ))}
              <button
                disabled={busy}
                onClick={() =>
                  act(
                    () => api("/simulation/configure", { speed }),
                    "Speed updated",
                  )
                }
              >
                Apply speed
              </button>
            </div>
            <p className="muted">
              Start dispatches assigned routes. Pause preserves progress; resume
              continues it. Stop ends journeys and returns outstanding
              deliveries to assigned routes. Physical vehicle speed remains
              realistic while simulation time advances faster.
            </p>
          </section>
          <div className="split">
            <section className="panel">
              <div className="panel-title">
                <h2>Live fleet map</h2>
                <select
                  value={vehicle}
                  onChange={(e) => setVehicle(e.target.value)}
                >
                  <option value="">All vehicles</option>
                  {data.vehicles.map((v) => (
                    <option key={v.id} value={v.id}>
                      {v.id} · {v.status}
                    </option>
                  ))}
                </select>
              </div>
              <FleetMap
                positions={mapPositions}
                routes={data.routes}
                depots={data.depots}
                selected={vehicle}
                onSelect={setVehicle}
              />
              <p className="muted">
                Green: vehicle · red: injected scenario · slate: depot · blue:
                planned fixture route. Basemap requires internet; telemetry and
                route geometry work locally.
              </p>
            </section>
            <section className="panel">
              <h2>{vehicle || "Vehicle details"}</h2>
              {selectedVehicle ? (
                <>
                  <Badge value={selectedVehicle.status} />
                  <dl>
                    <dt>Registration</dt>
                    <dd>{selectedVehicle.registration}</dd>
                    <dt>Route</dt>
                    <dd>{selectedRoute?.name || "Unassigned"}</dd>
                    <dt>Driver</dt>
                    <dd>
                      {data.drivers.find(
                        (d) => d.id === selectedRoute?.driver_id,
                      )?.name || "—"}
                    </dd>
                    <dt>Speed / heading</dt>
                    <dd>
                      {current
                        ? `${current.speed.toFixed(1)} km/h · ${current.heading.toFixed(0)}°`
                        : "No GPS yet"}
                    </dd>
                    <dt>Progress</dt>
                    <dd>
                      {current
                        ? `${(current.progress * 100).toFixed(1)}%`
                        : "—"}
                    </dd>
                    <dt>Last GPS</dt>
                    <dd>
                      {date(current?.timestamp || null)}{" "}
                      {current &&
                        Date.now() - new Date(current.timestamp).getTime() >
                          15000 && <Badge value="stale" />}
                    </dd>
                    <dt>Coordinates</dt>
                    <dd>
                      {current
                        ? `${current.lat.toFixed(5)}, ${current.lon.toFixed(5)}`
                        : "—"}
                    </dd>
                  </dl>
                  <h3>Inject scenario</h3>
                  <div className="buttons">
                    {["deviation", "delay", "stop", "breakdown", "clear"].map(
                      (s) => (
                        <button
                          disabled={busy}
                          key={s}
                          onClick={() =>
                            act(
                              () =>
                                api(`/vehicles/${vehicle}/scenario`, {
                                  scenario: s,
                                }),
                              `Scenario: ${s}`,
                            )
                          }
                        >
                          {s}
                        </button>
                      ),
                    )}
                  </div>
                  <h3>Assigned deliveries</h3>
                  {data.deliveries
                    .filter((d) => d.route_id === selectedRoute?.id)
                    .map((d) => (
                      <div className="item" key={d.id}>
                        <b>{d.reference}</b>
                        <Badge value={d.status} />
                        <p>{d.address}</p>
                        <button
                          onClick={() => {
                            setReassignDelivery(d.id);
                            setTab("Dispatch");
                          }}
                        >
                          Intervene / reassign
                        </button>
                      </div>
                    ))}
                </>
              ) : (
                <p className="muted">
                  Select a map marker or vehicle to inspect its position,
                  assignment and exception controls.
                </p>
              )}
            </section>
          </div>
        </>
      )}
      {tab === "Dispatch" && (
        <>
          <div className="three">
            <section className="panel">
              <h2>{editing ? "Edit delivery" : "Create delivery"}</h2>
              <form
                onSubmit={(e) => {
                  e.preventDefault();
                  act(async () => {
                    await api(
                      editing ? `/deliveries/${editing}` : "/deliveries",
                      {
                        ...deliveryForm,
                        scheduled_at: new Date(
                          deliveryForm.scheduled_at,
                        ).toISOString(),
                        due_at: new Date(deliveryForm.due_at).toISOString(),
                      },
                      editing ? "PATCH" : "POST",
                    );
                    setEditing("");
                    setDeliveryForm({
                      ...deliveryForm,
                      reference: "",
                      customer: "",
                      address: "",
                    });
                  }, "Delivery saved");
                }}
              >
                <label>
                  Reference
                  <input
                    required
                    value={deliveryForm.reference}
                    onChange={(e) =>
                      setDeliveryForm({
                        ...deliveryForm,
                        reference: e.target.value,
                      })
                    }
                  />
                </label>
                <label>
                  Customer
                  <input
                    required
                    value={deliveryForm.customer}
                    onChange={(e) =>
                      setDeliveryForm({
                        ...deliveryForm,
                        customer: e.target.value,
                      })
                    }
                  />
                </label>
                <label>
                  Address
                  <input
                    required
                    value={deliveryForm.address}
                    onChange={(e) =>
                      setDeliveryForm({
                        ...deliveryForm,
                        address: e.target.value,
                      })
                    }
                  />
                </label>
                <div className="row">
                  <label>
                    Latitude
                    <input
                      required
                      type="number"
                      step="any"
                      min="-90"
                      max="90"
                      value={deliveryForm.lat}
                      onChange={(e) =>
                        setDeliveryForm({
                          ...deliveryForm,
                          lat: +e.target.value,
                        })
                      }
                    />
                  </label>
                  <label>
                    Longitude
                    <input
                      required
                      type="number"
                      step="any"
                      min="-180"
                      max="180"
                      value={deliveryForm.lon}
                      onChange={(e) =>
                        setDeliveryForm({
                          ...deliveryForm,
                          lon: +e.target.value,
                        })
                      }
                    />
                  </label>
                </div>
                <label>
                  Priority
                  <select
                    value={deliveryForm.priority}
                    onChange={(e) =>
                      setDeliveryForm({
                        ...deliveryForm,
                        priority: e.target.value,
                      })
                    }
                  >
                    {["low", "normal", "high", "urgent"].map((p) => (
                      <option key={p}>{p}</option>
                    ))}
                  </select>
                </label>
                <label>
                  Scheduled
                  <input
                    required
                    type="datetime-local"
                    value={deliveryForm.scheduled_at}
                    onChange={(e) =>
                      setDeliveryForm({
                        ...deliveryForm,
                        scheduled_at: e.target.value,
                      })
                    }
                  />
                </label>
                <label>
                  Due
                  <input
                    required
                    type="datetime-local"
                    value={deliveryForm.due_at}
                    onChange={(e) =>
                      setDeliveryForm({
                        ...deliveryForm,
                        due_at: e.target.value,
                      })
                    }
                  />
                </label>
                <button disabled={busy} className="primary">
                  Save delivery
                </button>
                {editing && (
                  <button type="button" onClick={() => setEditing("")}>
                    Cancel edit
                  </button>
                )}
              </form>
            </section>
            <section className="panel">
              <h2>Plan and sequence a route</h2>
              <label>
                Route name
                <input
                  value={routeName}
                  onChange={(e) => setRouteName(e.target.value)}
                />
              </label>
              <label>
                Starting depot
                <select
                  value={depot}
                  onChange={(e) => setDepot(e.target.value)}
                >
                  {data.depots.map((d) => (
                    <option value={d.id} key={d.id}>
                      {d.name}
                    </option>
                  ))}
                </select>
              </label>
              <p className="muted">
                Select unrouted deliveries below. Stops follow the selected
                order; use arrows to resequence. Local fixture geometry follows
                orthogonal street legs.
              </p>
              <ol>
                {stops.map((id, i) => (
                  <li key={id}>
                    {data.deliveries.find((d) => d.id === id)?.reference}
                    <button
                      aria-label="Move stop up"
                      disabled={i === 0}
                      onClick={() =>
                        setStops((s) => {
                          const n = [...s];
                          [n[i - 1], n[i]] = [n[i], n[i - 1]];
                          return n;
                        })
                      }
                    >
                      ↑
                    </button>
                    <button
                      aria-label="Move stop down"
                      disabled={i === stops.length - 1}
                      onClick={() =>
                        setStops((s) => {
                          const n = [...s];
                          [n[i + 1], n[i]] = [n[i], n[i + 1]];
                          return n;
                        })
                      }
                    >
                      ↓
                    </button>
                    <button
                      onClick={() => setStops((s) => s.filter((x) => x !== id))}
                    >
                      Remove
                    </button>
                  </li>
                ))}
              </ol>
              <button
                className="primary"
                disabled={busy || !stops.length || !routeName}
                onClick={() =>
                  act(async () => {
                    const x = await api<{ id: string }>("/routes", {
                      name: routeName,
                      depot_id: depot,
                      delivery_ids: stops,
                    });
                    setRoute(x.id);
                    setStops([]);
                    setRouteName("");
                  }, "Route created")
                }
              >
                Create planned route
              </button>
              <h3>Assign and dispatch</h3>
              <label>
                Route
                <select
                  value={route}
                  onChange={(e) => setRoute(e.target.value)}
                >
                  <option value="">Choose route</option>
                  {data.routes
                    .filter((r) => ["planned", "assigned"].includes(r.status))
                    .map((r) => (
                      <option key={r.id} value={r.id}>
                        {r.name} · {r.status}
                      </option>
                    ))}
                </select>
              </label>
              <label>
                Available vehicle
                <select
                  value={assignVehicle}
                  onChange={(e) => setAssignVehicle(e.target.value)}
                >
                  <option value="">Choose vehicle</option>
                  {availableVehicles.map((v) => (
                    <option key={v.id}>{v.id}</option>
                  ))}
                </select>
              </label>
              <label>
                Available driver
                <select
                  value={driver}
                  onChange={(e) => setDriver(e.target.value)}
                >
                  <option value="">Choose driver</option>
                  {availableDrivers.map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.name}
                    </option>
                  ))}
                </select>
              </label>
              <div className="buttons">
                <button
                  disabled={busy || !route || !assignVehicle || !driver}
                  onClick={() =>
                    act(
                      () =>
                        api(`/routes/${route}/assign`, {
                          vehicle_id: assignVehicle,
                          driver_id: driver,
                        }),
                      "Route assigned",
                    )
                  }
                >
                  Assign
                </button>
                <button
                  disabled={busy || !route}
                  onClick={() =>
                    act(
                      () => api(`/routes/${route}/dispatch`, {}),
                      "Journey dispatched",
                    )
                  }
                >
                  Dispatch
                </button>
              </div>
            </section>
            <section className="panel">
              <h2>Dispatch intervention</h2>
              <label>
                Affected delivery
                <select
                  value={reassignDelivery}
                  onChange={(e) => setReassignDelivery(e.target.value)}
                >
                  <option value="">Choose delivery</option>
                  {data.deliveries
                    .filter(
                      (d) =>
                        d.route_id &&
                        ![
                          "completed",
                          "cancelled",
                          "scheduled",
                          "pending",
                        ].includes(d.status),
                    )
                    .map((d) => (
                      <option key={d.id} value={d.id}>
                        {d.reference} · {d.status}
                      </option>
                    ))}
                </select>
              </label>
              <label>
                Replacement vehicle
                <select
                  value={replacement}
                  onChange={(e) => setReplacement(e.target.value)}
                >
                  <option value="">Choose available vehicle</option>
                  {availableVehicles.map((v) => (
                    <option key={v.id}>{v.id}</option>
                  ))}
                </select>
              </label>
              <label>
                Replacement driver
                <select
                  value={replacementDriver}
                  onChange={(e) => setReplacementDriver(e.target.value)}
                >
                  <option value="">Choose available driver</option>
                  {availableDrivers.map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.name}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Reason
                <textarea
                  value={reason}
                  onChange={(e) => setReason(e.target.value)}
                />
              </label>
              <button
                disabled={
                  busy ||
                  !reassignDelivery ||
                  !replacement ||
                  !replacementDriver ||
                  !reason
                }
                className="primary"
                onClick={() =>
                  act(
                    () =>
                      api(`/deliveries/${reassignDelivery}/reassign`, {
                        vehicle_id: replacement,
                        driver_id: replacementDriver,
                        reason,
                      }),
                    "Delivery reassigned and recovery journey started",
                  )
                }
              >
                Reassign and continue
              </button>
              <h3>Record delivery outcome</h3>
              <p className="muted">
                Choose an in-transit delivery above and record its completion.
                Simulated arrivals also complete deliveries automatically.
              </p>
              <button
                disabled={busy || !reassignDelivery}
                onClick={() =>
                  act(
                    () =>
                      api(`/deliveries/${reassignDelivery}/complete`, {
                        outcome: reason || "Delivered by dispatcher",
                      }),
                    "Delivery completed",
                  )
                }
              >
                Complete selected delivery
              </button>
              <h3>Assignment history</h3>
              {data.assignments
                .filter((a) => a.delivery_id === reassignDelivery)
                .map((a) => (
                  <div className="item" key={a.id}>
                    <b>
                      {a.vehicle_id} · {a.driver_id}
                    </b>
                    <p>{a.reason}</p>
                    <small>
                      {date(a.assigned_at)} → {date(a.ended_at)}
                    </small>
                  </div>
                ))}
            </section>
          </div>
          <section className="panel">
            <h2>Delivery register</h2>
            <div className="toolbar">
              <input
                placeholder="Search reference / customer"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
              />
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
              >
                {[
                  "all",
                  "pending",
                  "scheduled",
                  "assigned",
                  "in_transit",
                  "delayed",
                  "completed",
                ].map((s) => (
                  <option key={s}>{s}</option>
                ))}
              </select>
              <span>{visibleDeliveries.length} deliveries</span>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Plan</th>
                    <th>Reference / customer</th>
                    <th>Destination</th>
                    <th>Priority</th>
                    <th>Status</th>
                    <th>Due / completed</th>
                    <th>Route</th>
                    <th>Action / outcome</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleDeliveries.map((d) => (
                    <tr key={d.id}>
                      <td>
                        <input
                          aria-label={`Select ${d.reference}`}
                          type="checkbox"
                          disabled={!!d.route_id}
                          checked={stops.includes(d.id)}
                          onChange={(e) =>
                            setStops((s) =>
                              e.target.checked
                                ? [...s, d.id]
                                : s.filter((id) => id !== d.id),
                            )
                          }
                        />
                      </td>
                      <td>
                        <b>{d.reference}</b>
                        <small>{d.customer}</small>
                      </td>
                      <td>
                        {d.address}
                        <small>
                          {d.lat.toFixed(4)}, {d.lon.toFixed(4)}
                        </small>
                      </td>
                      <td>
                        <Badge value={d.priority} />
                      </td>
                      <td>
                        <Badge value={d.status} />
                      </td>
                      <td>
                        {date(d.due_at)}
                        <small>{date(d.completed_at)}</small>
                      </td>
                      <td>
                        {data.routes.find((r) => r.id === d.route_id)?.name ||
                          "Unrouted"}
                        <small>Stop {d.sequence || "—"}</small>
                      </td>
                      <td>
                        {!d.route_id ? (
                          <button
                            onClick={() => {
                              setEditing(d.id);
                              setDeliveryForm({
                                reference: d.reference,
                                customer: d.customer,
                                address: d.address,
                                lat: d.lat,
                                lon: d.lon,
                                priority: d.priority,
                                scheduled_at: localDate(
                                  new Date(d.scheduled_at),
                                ),
                                due_at: localDate(new Date(d.due_at)),
                              });
                              window.scrollTo({ top: 0, behavior: "smooth" });
                            }}
                          >
                            Edit
                          </button>
                        ) : d.status !== "completed" ? (
                          <button onClick={() => setReassignDelivery(d.id)}>
                            Select intervention
                          </button>
                        ) : (
                          d.outcome
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
          <section className="panel">
            <h2>Route register</h2>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Route</th>
                    <th>Status</th>
                    <th>Vehicle</th>
                    <th>Driver</th>
                    <th>Stops</th>
                  </tr>
                </thead>
                <tbody>
                  {data.routes.map((r) => (
                    <tr key={r.id}>
                      <td>{r.name}</td>
                      <td>
                        <Badge value={r.status} />
                      </td>
                      <td>{r.vehicle_id || "—"}</td>
                      <td>{r.driver_id || "—"}</td>
                      <td>
                        {data.deliveries
                          .filter((d) => d.route_id === r.id)
                          .sort((a, b) => (a.sequence || 0) - (b.sequence || 0))
                          .map((d) => d.reference)
                          .join(" → ")}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      )}
      {tab === "Fleet registry" && (
        <>
          <section className="panel">
            <h2>Add fleet resources</h2>
            <form
              className="toolbar"
              onSubmit={(e) => {
                e.preventDefault();
                act(
                  () =>
                    api("/registry/" + registryKind, {
                      id: registryID,
                      name: registryName,
                      depot_id: registryDepot,
                      capacity: registryCapacity,
                    }),
                  "Registry record created",
                );
              }}
            >
              <label>
                Resource
                <select
                  value={registryKind}
                  onChange={(e) => setRegistryKind(e.target.value)}
                >
                  <option value="vehicle">Vehicle</option>
                  <option value="driver">Driver</option>
                </select>
              </label>
              <label>
                ID
                <input
                  required
                  value={registryID}
                  onChange={(e) => setRegistryID(e.target.value)}
                />
              </label>
              <label>
                {registryKind === "vehicle" ? "Registration" : "Driver name"}
                <input
                  required
                  value={registryName}
                  onChange={(e) => setRegistryName(e.target.value)}
                />
              </label>
              <label>
                Depot
                <select
                  value={registryDepot}
                  onChange={(e) => setRegistryDepot(e.target.value)}
                >
                  {data.depots.map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.name}
                    </option>
                  ))}
                </select>
              </label>
              {registryKind === "vehicle" && (
                <label>
                  Capacity kg
                  <input
                    min="1"
                    type="number"
                    value={registryCapacity}
                    onChange={(e) => setRegistryCapacity(+e.target.value)}
                  />
                </label>
              )}
              <button disabled={busy}>Add resource</button>
            </form>
          </section>
          <div className="split">
            <section className="panel">
              <h2>Vehicles ({data.vehicles.length})</h2>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Vehicle / registration</th>
                      <th>Depot</th>
                      <th>Capacity</th>
                      <th>Status</th>
                      <th>Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.vehicles.map((v) => (
                      <tr key={v.id}>
                        <td>
                          {v.id}
                          <small>{v.registration}</small>
                        </td>
                        <td>
                          {data.depots.find((d) => d.id === v.depot_id)?.name}
                        </td>
                        <td>{v.capacity} kg</td>
                        <td>
                          <Badge value={v.status} />
                        </td>
                        <td>
                          <button
                            onClick={() => {
                              setVehicle(v.id);
                              setTab("Live operations");
                            }}
                          >
                            Inspect
                          </button>
                          {v.status === "breakdown" && (
                            <button
                              disabled={busy}
                              onClick={() =>
                                act(
                                  () =>
                                    api(
                                      `/registry/vehicle/${v.id}`,
                                      {
                                        status: "available",
                                        depot_id: v.depot_id,
                                      },
                                      "PATCH",
                                    ),
                                  "Vehicle restored to availability",
                                )
                              }
                            >
                              Mark repaired
                            </button>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
            <section className="panel">
              <h2>Drivers ({data.drivers.length})</h2>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Driver / license</th>
                      <th>Depot</th>
                      <th>Status</th>
                      <th>Availability</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.drivers.map((d) => (
                      <tr key={d.id}>
                        <td>
                          {d.name}
                          <small>{d.license}</small>
                        </td>
                        <td>
                          {data.depots.find((p) => p.id === d.depot_id)?.name}
                        </td>
                        <td>
                          <Badge value={d.status} />
                        </td>
                        <td>
                          {["available", "unavailable"].includes(d.status) && (
                            <button
                              disabled={busy}
                              onClick={() =>
                                act(
                                  () =>
                                    api(
                                      `/registry/driver/${d.id}`,
                                      {
                                        status:
                                          d.status === "available"
                                            ? "unavailable"
                                            : "available",
                                        depot_id: d.depot_id,
                                      },
                                      "PATCH",
                                    ),
                                  "Driver availability updated",
                                )
                              }
                            >
                              {d.status === "available"
                                ? "Off duty"
                                : "Available"}
                            </button>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          </div>
          <section className="panel">
            <h2>Depots</h2>
            <div className="toolbar">
              {data.depots.map((d) => (
                <div className="item" key={d.id}>
                  <b>{d.name}</b>
                  <small>
                    {d.lat}, {d.lon}
                  </small>
                </div>
              ))}
            </div>
          </section>
        </>
      )}
      {tab === "Exceptions" && (
        <>
          <section className="panel">
            <h2>Detection configuration</h2>
            <div className="toolbar">
              <label>
                Route deviation (m)
                <input
                  min="10"
                  type="number"
                  value={deviation}
                  onChange={(e) => setDeviation(+e.target.value)}
                />
              </label>
              <label>
                Prolonged stop (seconds)
                <input
                  min="2"
                  type="number"
                  value={stopSeconds}
                  onChange={(e) => setStopSeconds(+e.target.value)}
                />
              </label>
              <label>
                Delay grace (seconds)
                <input
                  min="0"
                  type="number"
                  value={delaySeconds}
                  onChange={(e) => setDelaySeconds(+e.target.value)}
                />
              </label>
              <button
                disabled={busy}
                onClick={() =>
                  act(
                    () =>
                      api("/simulation/configure", {
                        deviation_m: deviation,
                        stop_seconds: stopSeconds,
                        delay_seconds: delaySeconds,
                      }),
                    "Detection rules saved",
                  )
                }
              >
                Save thresholds
              </button>
            </div>
          </section>
          <section className="panel">
            <h2>Operational exceptions and history</h2>
            <label>
              Action note
              <input value={note} onChange={(e) => setNote(e.target.value)} />
            </label>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Severity / type</th>
                    <th>Vehicle</th>
                    <th>Message</th>
                    <th>Status</th>
                    <th>Detected</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {data.alerts.map((a) => (
                    <tr key={a.id}>
                      <td>
                        <Badge value={a.severity} />
                        <small>{a.type.replaceAll("_", " ")}</small>
                      </td>
                      <td>{a.vehicle_id}</td>
                      <td>{a.message}</td>
                      <td>
                        <Badge value={a.status} />
                      </td>
                      <td>{date(a.created_at)}</td>
                      <td>
                        {a.status === "open" && (
                          <button
                            disabled={busy || !note}
                            onClick={() =>
                              act(
                                () =>
                                  api(`/alerts/${a.id}/acknowledge`, { note }),
                                "Alert acknowledged",
                              )
                            }
                          >
                            Acknowledge
                          </button>
                        )}
                        {a.status !== "resolved" && (
                          <button
                            disabled={busy || !note}
                            onClick={() =>
                              act(
                                () => api(`/alerts/${a.id}/resolve`, { note }),
                                "Alert resolved",
                              )
                            }
                          >
                            Resolve
                          </button>
                        )}
                        <button
                          onClick={() => {
                            setVehicle(a.vehicle_id);
                            setReassignDelivery(
                              data.deliveries.find(
                                (d) =>
                                  d.route_id === a.route_id &&
                                  !["completed", "cancelled"].includes(
                                    d.status,
                                  ),
                              )?.id || "",
                            );
                            setTab("Dispatch");
                          }}
                        >
                          Intervene
                        </button>
                        <button
                          onClick={() =>
                            act(async () => {
                              setAlertHistory(
                                await api(`/alerts/${a.id}/history`),
                              );
                              setHistoryTitle(`${a.vehicle_id} / ${a.type}`);
                            }, "Alert history loaded")
                          }
                        >
                          History
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {!data.alerts.length && (
              <p className="muted">
                No alerts. Start the simulation and inject a scenario from
                vehicle details.
              </p>
            )}
            {historyTitle && (
              <>
                <h3>{historyTitle}</h3>
                {alertHistory.map((h, i) => (
                  <div className="item" key={i}>
                    <b>{h.action}</b> · {h.note}
                    <small>{date(h.at)}</small>
                  </div>
                ))}
              </>
            )}
          </section>
        </>
      )}
      {tab === "Journey replay" && (
        <section className="panel">
          <h2>Historical journey replay</h2>
          <div className="toolbar">
            <label>
              Vehicle
              <select
                value={vehicle}
                onChange={(e) => setVehicle(e.target.value)}
              >
                <option value="">Choose vehicle</option>
                {data.vehicles.map((v) => (
                  <option key={v.id}>{v.id}</option>
                ))}
              </select>
            </label>
            <label>
              Stored journey
              <select
                value={journey}
                onChange={(e) => setJourney(e.target.value)}
              >
                <option value="">Choose journey</option>
                {journeys.map((j) => (
                  <option key={j.id} value={j.id}>
                    {date(j.started_at)} · {j.status} · {j.id.slice(-6)}
                  </option>
                ))}
              </select>
            </label>
            <button
              disabled={!replay.length}
              onClick={() => {
                if (frame === replay.length - 1) setFrame(0);
                setPlaying((p) => !p);
              }}
            >
              {playing ? "Pause replay" : "Play replay"}
            </button>
            <button
              disabled={!replay.length}
              onClick={() => {
                setPlaying(false);
                setFrame(0);
              }}
            >
              Reset
            </button>
            <label>
              Playback speed
              <select
                value={playSpeed}
                onChange={(e) => setPlaySpeed(+e.target.value)}
              >
                {[1, 2, 5, 10, 25, 100].map((n) => (
                  <option key={n} value={n}>
                    {n}×
                  </option>
                ))}
              </select>
            </label>
            <span>
              {replayBusy
                ? "Loading durable telemetry…"
                : `${replay.length} stored GPS samples`}
            </span>
          </div>
          <input
            aria-label="Replay timeline"
            className="timeline"
            type="range"
            min="0"
            max={Math.max(0, replay.length - 1)}
            value={frame}
            disabled={!replay.length}
            onChange={(e) => {
              setPlaying(false);
              setFrame(+e.target.value);
            }}
          />
          <p>
            {replay[frame]
              ? `${date(replay[frame].timestamp)} · ${replay[frame].speed} km/h · sample ${frame + 1}/${replay.length}`
              : "Select a vehicle and journey to query TimescaleDB."}
          </p>
          <FleetMap
            positions={replay[frame] ? [replay[frame]] : []}
            routes={data.routes.filter(
              (r) => r.id === journeys.find((j) => j.id === journey)?.route_id,
            )}
            depots={data.depots}
            selected={vehicle}
            onSelect={setVehicle}
          />
        </section>
      )}
      {tab === "Analytics" && (
        <>
          <section className="panel">
            <h2>Streaming and service health</h2>
            <div className="metrics">
              <div>
                <strong>
                  {((data.metrics.telemetry_last_minute || 0) / 60).toFixed(1)}
                </strong>
                <span>durable events / second (60s)</span>
              </div>
              <div>
                <strong>{rates.lag}</strong>
                <span>consumer lag (broker offsets)</span>
              </div>
              <div>
                <strong>{(rates.latency * 1000).toFixed(0)} ms</strong>
                <span>mean processing latency</span>
              </div>
              <div>
                <strong>{rates.retries}</strong>
                <span>consumer retries since boot</span>
              </div>
            </div>
            <div className="toolbar">
              {Object.entries(health).map(([k, v]) => (
                <div className="item" key={k}>
                  {k}: <Badge value={v ? "healthy" : "unavailable"} />
                </div>
              ))}
              <a href="http://localhost:3000" target="_blank" rel="noreferrer">
                Open Grafana
              </a>
              <a href="http://localhost:9090" target="_blank" rel="noreferrer">
                Open Prometheus
              </a>
            </div>
            <p className="muted">
              Metrics reflect actual persisted records and backend
              instrumentation. Grafana provides time-series rates, lag, latency
              and process health.
            </p>
          </section>
          <div className="split">
            <section className="panel">
              <h2>Delivery performance</h2>
              {[
                "pending",
                "scheduled",
                "assigned",
                "in_transit",
                "delayed",
                "completed",
              ].map((s) => (
                <div className="stat-line" key={s}>
                  <Badge value={s} />
                  <strong>
                    {data.deliveries.filter((d) => d.status === s).length}
                  </strong>
                </div>
              ))}
              <div className="stat-line">
                Completed on time
                <strong>
                  {
                    data.deliveries.filter(
                      (d) =>
                        d.completed_at &&
                        new Date(d.completed_at) <= new Date(d.due_at),
                    ).length
                  }
                </strong>
              </div>
            </section>
            <section className="panel">
              <h2>Exception performance</h2>
              {["open", "acknowledged", "resolved"].map((s) => (
                <div className="stat-line" key={s}>
                  <Badge value={s} />
                  <strong>
                    {data.alerts.filter((a) => a.status === s).length}
                  </strong>
                </div>
              ))}
              <div className="stat-line">
                Critical exceptions
                <strong>
                  {data.alerts.filter((a) => a.severity === "critical").length}
                </strong>
              </div>
            </section>
          </div>
        </>
      )}
      <footer>
        Gauteng Logistics · fictional operator · local route fixtures · durable
        operations platform
      </footer>
    </div>
  );
}
createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
