# Verified functional checkpoint

Verified 03 October 2026 at 20:26 (Africa/Johannesburg) on the real local Docker Compose stack.

| Check | Result |
|---|---|
| Go compilation, targeted geometry/schema tests and go vet | Passed |
| TypeScript and Vite production build; Nginx serving | Passed |
| Frontend dependency audit | Zero reported vulnerabilities |
| All core Compose service health checks | Passed |
| 100 concurrent simulated vehicles → Redpanda → WebSocket | Passed |
| Actual TimescaleDB telemetry hypertable | Confirmed through timescaledb_information.hypertables |
| Delivery create/edit, route planning, assignment and dispatch | Passed |
| Occupied-resource rejection and transactional recovery reassignment | Passed |
| Deviation, overdue delivery, prolonged stop and breakdown alerts | Passed |
| Persistent alert acknowledgement, resolution and action history | Passed |
| Pause/resume and automatic arrival completion | Passed |
| Duplicate idempotency and stale GPS persistence without current-position rewind | Passed |
| Invalid schema and invalid journey context DLQ in both groups | Passed |
| Durable records after backend, Redis and PostgreSQL restart | Passed |
| Browser rendering, live stream connection and real historical playback | Passed; saved replay screenshot |
| Prometheus backend/Redpanda scrapes and broker-derived lag | Passed; both groups caught up |
| Grafana health and provisioned operational dashboard | Passed |
| Kubernetes manifests | 18 resource documents validated structurally; cluster deployment unverified |

The checkpoint contains 120 vehicles, 120 drivers, 3 depots, 101 deliveries and 2200 durable processed events. The demonstration is left paused. Two deliveries were completed during the smoke: one through dispatcher intervention and one through simulated arrival. Demonstration exception records remain available for inspection. Resume from the dashboard to continue; choose 1× for ordinary observation.

## Reproduce

Start a fresh seeded stack with `docker compose up -d --build`, then run `node scripts/smoke.mjs` and `node scripts/stream-check.mjs`. Restart the backend, Redis and PostgreSQL and run `node scripts/smoke.mjs --verify`. Run `node scripts/observability-check.mjs` after Prometheus has scraped both targets. Preserve named volumes to retain records. See README for targeted build commands and infrastructure requirements.

The initial integration run exposed a telemetry INSERT placeholder mismatch; it was fixed, retained events were recovered, and the complete focused smoke passed. Browser verification also found a JavaScript-module MIME type/cache issue; the Nginx configuration was fixed and actual replay playback was verified. These failures are resolved, not outstanding limitations.

## Practical limits

Routing uses fictional local street-leg fixtures, not an optimized road network. OSM basemap tiles require internet. The local topology has one backend/simulator owner and one broker/database; it is not an HA or authenticated public deployment. Retention/backup, identity/RBAC, TLS and production scale testing are deployment work outside this private demonstration. Kubernetes rollout was not tested because no cluster was used.

## UI redesign boundary

Fleet readiness, dispatch, route planning/sequencing, alerts, intervention, simulation and replay already use actual APIs and persisted data. The redesign phase can focus on layout, visual hierarchy, styling, responsive behavior and accessibility polish. Keep `frontend/src/types.ts` contracts and REST/WebSocket integration intact. [Browser replay evidence](replay-screenshot.jpg).
