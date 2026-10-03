# REST and WebSocket API

Base `/api`. JSON responses; errors have `{ "error": "message" }`. 400 means invalid input, 409 means unavailable state/resource or relational conflict, 503 means a dependency failed. UI calls these endpoints directly.

| Method | Endpoint | Body / behavior |
|---|---|---|
| GET | `/health` | PostgreSQL, Redis and Redpanda connectivity; 503 when degraded |
| GET | `/snapshot` | Fleet, depots, drivers, deliveries, routes, alerts, assignment history, positions, metrics, settings |
| POST | `/seed` | `{}`; idempotent fictional company seed |
| POST | `/registry/vehicle` | `id,name,depot_id,capacity`; name is registration |
| POST | `/registry/driver` | `id,name,depot_id`; license derived from ID |
| PATCH | `/registry/{vehicle\|driver}/{id}` | `status,depot_id`; cannot release assigned resources |
| POST | `/deliveries` | `reference,customer,address,lat,lon,priority,scheduled_at,due_at` |
| PATCH | `/deliveries/{id}` | Same fields; only unrouted pending/scheduled deliveries |
| POST | `/routes` | `name,depot_id,delivery_ids`; ordered stops and local fixture geometry |
| POST | `/routes/{id}/assign` | `vehicle_id,driver_id,reason?`; both resources must be available |
| POST | `/routes/{id}/dispatch` | `{}`; assigned route, scheduled time reached; starts journey |
| POST | `/deliveries/{id}/reassign` | `vehicle_id,driver_id,reason`; atomic assignment history and recovery route/dispatch |
| POST | `/deliveries/{id}/complete` | `outcome`; in-transit/delayed delivery only |
| POST | `/simulation/{start\|pause\|resume\|stop\|configure}` | `speed?,count?,deviation_m?,stop_seconds?,delay_seconds?` |
| POST | `/vehicles/{id}/scenario` | `scenario`: deviation, delay, stop, breakdown, clear |
| POST | `/alerts/{id}/acknowledge` | `note`; open alert only |
| POST | `/alerts/{id}/resolve` | `note`; open/acknowledged alert |
| GET | `/alerts/{id}/history` | Ordered detection/actions with notes |
| GET | `/vehicles/{id}/journeys` | Most recent 100 journeys |
| GET | `/vehicles/{id}/history?journey_id=...&after=...` | Chronological events, has_more; after is exclusive RFC3339 timestamp |
| GET / upgrade | `/ws` | `position` or `change` messages; ping/pong |
| GET | `/metrics` (outside `/api`) | Prometheus exposition |

Example create:

```sh
curl -fsS http://localhost:8080/api/deliveries -H 'Content-Type: application/json' -d '{"reference":"CUSTOM-001","customer":"Example retailer","address":"22 Market Street","lat":-26.19,"lon":28.06,"priority":"high"}'
curl -fsS http://localhost:8080/api/simulation/start -H 'Content-Type: application/json' -d '{"count":100,"speed":1}'
```

WebSocket position data contains `event_id,timestamp,vehicle_id,journey_id,route_id,lat,lon,speed,heading,seq,progress,scenario`. A `change` notification asks the client to refetch its snapshot. Snapshot recovery also handles reconnects and slow clients. `GET /snapshot` includes at most 500 alerts and 1,000 assignment rows; historical telemetry is paginated independently.
