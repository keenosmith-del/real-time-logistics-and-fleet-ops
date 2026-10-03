# Architecture and consistency

## Components

React's dispatcher screens use REST mutations and a reconnecting WebSocket. A ten-second snapshot refresh reconciles dropped notifications and disconnected clients. MapLibre draws positions and route geometry from actual backend responses. The only external request is the free OSM raster basemap.

One Go process owns REST operations, the bounded concurrent simulator (32 workers, 100+ active journeys), WebSocket clients, and two independent Kafka consumer groups. It is deliberately a modular service rather than a network of microservices. PostgreSQL is authoritative; Redis is a current-position projection and can be rebuilt by stream replay or supplemented from durable vehicle positions.

## Relational model

Depots own drivers and vehicles. Routes contain GeoJSON-compatible coordinate arrays and delivery stop sequence. Deliveries have scheduling, deadlines, priority, operational status, completion timestamp and outcome. Assignment rows preserve every vehicle/driver change with reason and time range. Journeys preserve simulation progress, monotonic sequence, scenario and lifecycle. Alerts and append-only action history preserve detection, acknowledgement and resolution. Settings persist simulation speed, state and detection thresholds.

Foreign keys, status CHECK constraints, geographic bounds, due/scheduled-time validation and partial unique indexes enforce one active route per resource, one current assignment per delivery and one running/paused journey per vehicle. Assignment and reassignment lock the relevant rows and commit all state changes transactionally. An occupied resource produces HTTP 409. A recovery route starts from the replacement's last durable position or its depot. Completed deliveries remain on their original route; only the affected outstanding delivery is moved. The old route is cancelled if no outstanding deliveries remain. Broken vehicles stay unavailable until explicitly repaired.

TimescaleDB's `telemetry` hypertable partitions by event time with indexes on `(vehicle_id, ts)` and `(journey_id, ts)`. The `processed_events` ledger deduplicates globally by `event_id` inside the same transaction as telemetry/history changes. Telemetry's `(ts,event_id)` uniqueness satisfies hypertable requirements. The ledger is deliberately unpruned in this demonstration; production retention must coordinate ledger, Kafka and telemetry retention.

## Stream contract

* `fleet.telemetry.v1`: 12 partitions, vehicle ID as the partition key, JSON schema in `event-schema.json`.
* `fleet.telemetry.dlq.v1`: 3 partitions, invalid events with original bytes, consumer, reason, partition and offset.
* `history-v1`: validates, deduplicates, stores telemetry, updates durable current positions, detects exceptions and advances delivery outcomes.
* `live-v1`: validates and atomically updates the Redis hash with a numeric event timestamp; broadcasts only newer positions.

Consumers manually commit offsets after successful effects. Invalid schemas go to DLQ before offset commit. Dependency failures use bounded exponential-like backoff (1–10 seconds) and retry without discarding records. This is at-least-once delivery with idempotent effects. Hash partitioning preserves per-vehicle Kafka order. Durable current positions only advance for newer timestamps; old GPS is still stored for forensic replay, but does not trigger backward operational transitions. Event IDs use journey ID plus sequence.

Simulator progress and sequence are checkpointed after an acknowledged Kafka publish. A crash between publish and checkpoint may repeat an event ID; the ledger suppresses duplicated durable effects. Redpanda has a named durable volume. Producer waits for all configured replica acknowledgements (the local broker has one replica). Consumers start from retained history when a new group is created. Redis and history processing are independent: cache failure retries the live consumer while historical persistence can continue.

## Exceptions and interventions

Geographic deviation computes the shortest distance to every route segment in a local tangent-plane approximation, using haversine distances. It is suitable for Gauteng fixtures, not polar or antimeridian routing. Deadline alerts compare actual GPS event time with due time plus a configurable grace. Stop detection records the first stationary timestamp and tests elapsed real seconds. Breakdown injection produces a critical alert, pauses that journey and marks its route disrupted and vehicle broken. Active alerts are unique by journey/type. Resolving an ongoing condition permits a new detection, preserving a separate history of repeated exceptions.

## Controls, recovery and replay

Simulation speed changes distance progression per wall-clock tick; reported physical speed stays in km/h. Pause preserves journey progress. Stop closes journey history and returns routes with outstanding work to assigned status; a subsequent start is a new journey. Backend restart pauses active operations intentionally. Resume explicitly continues them. Automatic stop arrival completes associated deliveries in sequence and releases resources when the route has no outstanding deliveries. Manual completion records a dispatcher-provided outcome.

Historical replay requests one vehicle/journey and at most 5,000 samples per page, ordered by timestamp and ID. The frontend follows pages and plays actual timestamp intervals at a user-selected multiplier. Live operations are unaffected by replay.

WebSocket clients have bounded queues, read/write deadlines, ping/pong, reconnect backoff and snapshot reconciliation. Slow clients can miss notifications and recover via snapshots. REST request bodies are bounded and reject unknown fields. Structured JSON logs expose retry and startup failures. Prometheus counters, latency histogram, broker-derived consumer lag and standard Go process metrics are scraped every five seconds.

## Deployment boundary

Local Compose health gates dependencies and persists all state in named volumes. Backend must remain a single replica because simulator ownership and hub fanout are in-process. The single broker/database are demonstration durability, not HA. The APIs are private trusted-operator endpoints without authentication. Kubernetes deploys the same complete topology with persistent volumes and secret references; production hardening is documented separately.
