# Project 6 — Real-Time Logistics & Fleet Operations Intelligence

An end-to-end dispatcher platform for **Gauteng Logistics**, a fictional South African operator. React/TypeScript/Vite/Tailwind and MapLibre connect to a Go REST/WebSocket service. Redpanda carries GPS events to independent consumers. PostgreSQL/TimescaleDB holds operational records and historical GPS; Redis serves current positions. Prometheus and Grafana expose live pipeline metrics.

## Start locally

Requirements: Docker Desktop / Docker Engine with Compose v2, about 5 GB available memory, and internet for the first image/dependency download. No paid service, API key or local Go installation is required.

```sh
cp .env.example .env
docker compose up -d --build
docker compose ps
```

Open [dispatcher dashboard](http://localhost:5173), [API health](http://localhost:8080/api/health), [Grafana](http://localhost:3000) (`admin` / `fleet-demo`), and [Prometheus](http://localhost:9090). Ports bind to loopback. The OSM basemap uses public map tiles and requires internet; local route overlays, positions and operational APIs do not.

Migrations and an idempotent seed run automatically. The database contains 3 depots, 120 drivers, 120 vehicles, and 100 deliveries with assigned fixture routes. Twenty spare vehicles/drivers support recovery operations. `POST /api/seed` is idempotent and never resets existing work. To start without a demonstration company set `AUTO_SEED=false`; create depots using SQL before adding resources.

## Dispatcher demonstration

1. Inspect **Fleet registry** and the **Live operations** readiness metrics.
2. In **Dispatch**, create a delivery with customer, coordinates, scheduled time, deadline and priority. Select unrouted deliveries in the register, enter a route name and depot, resequence stops, and create the route. Assign an available vehicle and driver; dispatch it.
3. In **Live operations**, start 100 vehicles at 1×. The seed's assigned routes become active journeys. Click moving map markers to see speed, heading, timestamp, progress, driver and deliveries.
4. Select `VH-001` and inject **breakdown**, or use deviation/delay/stop on another active vehicle. Deviation is calculated against every geographic route segment; stop detection requires the configured real elapsed seconds. Delay injection makes outstanding deadlines overdue.
5. In **Exceptions**, acknowledge the persisted alert with a note. Choose **Intervene**, select an outstanding delivery, a spare vehicle and driver, and a recovery reason. **Reassign and continue** atomically closes the previous assignment, creates a new route from the replacement position/depot, and starts the recovery journey.
6. Complete a delivery with a recorded outcome, or let simulation reach its stop for automatic proof of delivery. Resolve alerts separately with a note; clearing a scenario does not erase alert history.
7. In **Journey replay**, select a vehicle and its stored journey, then play, pause, seek or change playback speed. Replay fetches actual TimescaleDB samples in chronological pages.
8. Inspect completed outcomes, assignment history, exception history and **Analytics**. Grafana includes event rate, consumer lag, processing latency, retries and service health.
9. Restart services. Records and consumer offsets survive in named volumes. Backend restart pauses journeys for explicit operator review; **Resume** continues stored progress.

```sh
docker compose restart backend redis postgres
# wait for API health, then reopen the dashboard and Resume
curl -fsS http://localhost:8080/api/health
```

`docker compose down` preserves data; deleting Compose volumes resets the demonstration and is destructive.

## Targeted verification

```sh
# Fresh seeded stack; requires Node 22+ for built-in WebSocket:
node scripts/smoke.mjs
node scripts/stream-check.mjs
node scripts/observability-check.mjs
# Restart relevant services and verify the checkpoint:
docker compose restart backend redis postgres
node scripts/smoke.mjs --verify
# Targeted Go tests/build inside the Go image:
docker run --rm -v "$PWD/backend:/src" -w /src golang:1.24-alpine sh -c 'go test ./internal/platform && go build ./cmd/server'
# Frontend checks:
cd frontend
npm ci
npm run build
```

The focused smoke covers 100 WebSocket positions, all four exception detectors, creation/editing/planning/assignment/dispatch, resource conflict rejection, acknowledgement/reassignment/resolution, pause/resume, automatic completion and historical replay. It leaves the fleet paused and writes `docs/smoke-checkpoint.json` for restart verification. Run it once on a fresh seed; subsequent runs intentionally fail the readiness precondition instead of mutating an already-used company.

See [architecture](docs/architecture.md), [API reference](docs/api.md), [event schema](docs/event-schema.json), [verification](docs/verification.md), and [Kubernetes deployment](infra/k8s/README.md).

## Development

With infrastructure running through Compose:

```sh
# Requires Go 1.24+; stop the Compose backend to avoid two simulator owners.
docker compose stop backend
cd backend
go mod download
DATABASE_URL='postgres://fleet:fleet@localhost:5433/fleet?sslmode=disable' REDIS_ADDR=localhost:6379 KAFKA_BROKER=localhost:19092 go run ./cmd/server
# Another terminal:
cd frontend
npm ci
npm run dev
```

The Vite proxy routes `/api`, WebSockets and `/metrics` to localhost:8080. Docker uses Nginx as the same-origin proxy. Source is split into `backend/internal/platform` (operations, registry, stream, geometry), `backend/migrations`, `frontend/src`, and `infra`.

## Scope and remaining deployment work

This checkpoint is a complete locally executable functional demonstration, ready for frontend redesign. Fixture routing is deterministic local geometry rather than road-network optimization. Records are real, but seed destinations are fictional. There is no identity provider or tenant isolation; keep the default loopback deployment private. Production rollout needs authentication/authorization, TLS, HA/replication, capacity/load testing, backup policies, and event/telemetry retention appropriate to the operator. Kubernetes manifests are provided but are not required for the local demonstration.
