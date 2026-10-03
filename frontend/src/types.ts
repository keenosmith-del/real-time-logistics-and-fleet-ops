export type Position = {
  event_id: string;
  timestamp: string;
  vehicle_id: string;
  journey_id: string;
  route_id: string;
  lat: number;
  lon: number;
  speed: number;
  heading: number;
  progress: number;
  seq: number;
  scenario: string;
};
export type Depot = { id: string; name: string; lat: number; lon: number };
export type Vehicle = {
  id: string;
  registration: string;
  depot_id: string;
  status: string;
  capacity: number;
  last_ts: string | null;
  last_position: Position | null;
};
export type Driver = {
  id: string;
  name: string;
  license: string;
  depot_id: string;
  status: string;
};
export type Delivery = {
  id: string;
  reference: string;
  customer: string;
  address: string;
  lat: number;
  lon: number;
  priority: string;
  scheduled_at: string;
  due_at: string;
  status: string;
  route_id: string | null;
  sequence: number | null;
  completed_at: string | null;
  outcome: string | null;
};
export type Route = {
  id: string;
  name: string;
  geometry: number[][];
  status: string;
  vehicle_id: string | null;
  driver_id: string | null;
};
export type Alert = {
  id: string;
  vehicle_id: string;
  journey_id: string;
  route_id: string;
  type: string;
  severity: string;
  message: string;
  status: string;
  created_at: string;
  acknowledged_at: string | null;
  resolved_at: string | null;
};
export type Assignment = {
  id: string;
  delivery_id: string;
  route_id: string;
  vehicle_id: string;
  driver_id: string;
  assigned_at: string;
  ended_at: string | null;
  reason: string;
};
export type Settings = {
  speed: number;
  deviation_m: number;
  stop_seconds: number;
  delay_seconds: number;
  sim_state: string;
};
export type Snapshot = {
  depots: Depot[];
  vehicles: Vehicle[];
  drivers: Driver[];
  deliveries: Delivery[];
  routes: Route[];
  alerts: Alert[];
  assignments: Assignment[];
  positions: Position[];
  settings: Settings[];
  metrics: Record<string, number>;
};
export type Journey = {
  id: string;
  route_id: string;
  status: string;
  started_at: string;
  ended_at: string | null;
  progress: number;
};
export async function api<T = unknown>(
  path: string,
  body?: unknown,
  method = "POST",
): Promise<T> {
  const res = await fetch(
    "/api" + path,
    body === undefined
      ? {}
      : {
          method,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
  );
  const data = await res.json();
  if (!res.ok) throw new Error(data.error ?? `Request failed (${res.status})`);
  return data as T;
}
