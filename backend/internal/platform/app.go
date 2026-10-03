package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

type App struct {
	DB      *pgxpool.Pool
	Redis   *redis.Client
	Writer  *kafka.Writer
	Broker  string
	Hub     *Hub
	Logger  *slog.Logger
	control sync.Mutex
	Events  *prometheus.CounterVec
	Latency prometheus.Histogram
	Lag     *prometheus.GaugeVec
}

func Env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func New(ctx context.Context) (*App, error) {
	db, err := pgxpool.New(ctx, Env("DATABASE_URL", "postgres://fleet:fleet@localhost:5433/fleet?sslmode=disable"))
	if err != nil {
		return nil, err
	}
	if err = db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	a := &App{DB: db, Redis: redis.NewClient(&redis.Options{Addr: Env("REDIS_ADDR", "localhost:6379")}), Broker: Env("KAFKA_BROKER", "localhost:19092"), Hub: &Hub{clients: map[*websocket.Conn]chan []byte{}}, Logger: slog.New(slog.NewJSONHandler(os.Stdout, nil))}
	a.Writer = &kafka.Writer{Addr: kafka.TCP(a.Broker), Topic: "fleet.telemetry.v1", Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, WriteTimeout: 10 * time.Second}
	a.Events = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fleet_events_total", Help: "Telemetry processing results"}, []string{"consumer", "result"})
	a.Latency = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "fleet_processing_seconds", Help: "Event end to end latency", Buckets: prometheus.ExponentialBuckets(.01, 2, 16)})
	a.Lag = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "fleet_consumer_lag", Help: "Broker high watermark minus committed group offsets"}, []string{"consumer"})
	prometheus.MustRegister(a.Events, a.Latency, a.Lag)
	a.registerOperationalMetrics()
	return a, nil
}
func (a *App) Migrate(ctx context.Context, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	tx, err := a.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(43); CREATE TABLE IF NOT EXISTS schema_migrations(version text PRIMARY KEY,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	var applied bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, path).Scan(&applied); err != nil {
		return err
	}
	if !applied {
		if _, err = tx.Exec(ctx, string(b)); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES($1)`, path); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (a *App) Start(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", a.Broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err = conn.CreateTopics(kafka.TopicConfig{Topic: "fleet.telemetry.v1", NumPartitions: 12, ReplicationFactor: 1}, kafka.TopicConfig{Topic: "fleet.telemetry.dlq.v1", NumPartitions: 3, ReplicationFactor: 1}); err != nil {
		return err
	}
	// Resume requires an explicit operator action after a service restart.
	_, err = a.DB.Exec(ctx, `UPDATE settings SET sim_state='paused' WHERE sim_state='running'; UPDATE journeys SET status='paused' WHERE status='running'; UPDATE routes SET status='paused' WHERE status='active'; UPDATE vehicles SET status='paused' WHERE status='moving'`)
	if err != nil {
		return err
	}
	go a.consume(ctx, "history-v1", a.persist)
	go a.consume(ctx, "live-v1", a.live)
	go a.simulate(ctx)
	go a.monitor(ctx)
	return nil
}
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/registry/{kind}", a.createRegistry)
	mux.HandleFunc("PATCH /api/registry/{kind}/{id}", a.editRegistry)
	mux.HandleFunc("GET /api/health", a.health)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /api/snapshot", a.snapshot)
	mux.HandleFunc("POST /api/seed", a.seedHTTP)
	mux.HandleFunc("POST /api/deliveries", a.createDelivery)
	mux.HandleFunc("PATCH /api/deliveries/{id}", a.editDelivery)
	mux.HandleFunc("POST /api/deliveries/{id}/complete", a.completeDelivery)
	mux.HandleFunc("POST /api/routes", a.createRoute)
	mux.HandleFunc("POST /api/routes/{id}/assign", a.assign)
	mux.HandleFunc("POST /api/routes/{id}/dispatch", a.dispatch)
	mux.HandleFunc("POST /api/deliveries/{id}/reassign", a.reassign)
	mux.HandleFunc("POST /api/alerts/{id}/{action}", a.alertAction)
	mux.HandleFunc("GET /api/alerts/{id}/history", a.alertHistory)
	mux.HandleFunc("POST /api/simulation/{action}", a.simControl)
	mux.HandleFunc("POST /api/vehicles/{id}/scenario", a.scenario)
	mux.HandleFunc("GET /api/vehicles/{id}/history", a.history)
	mux.HandleFunc("GET /api/vehicles/{id}/journeys", a.journeys)
	mux.HandleFunc("GET /api/ws", a.ws)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", Env("CORS_ORIGIN", "http://localhost:5173"))
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PATCH,OPTIONS")
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		defer func() {
			if e := recover(); e != nil {
				a.Logger.Error("request panic", "error", fmt.Sprint(e))
				fail(w, 500, "internal error")
			}
		}()
		mux.ServeHTTP(w, r)
	})
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, s string) {
	respond(w, status, map[string]string{"error": s})
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		fail(w, 400, "invalid JSON: "+err.Error())
		return false
	}
	return true
}
func (a *App) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	db := a.DB.Ping(ctx) == nil
	cache := a.Redis.Ping(ctx).Err() == nil
	conn, err := kafka.DialContext(ctx, "tcp", a.Broker)
	broker := err == nil
	if conn != nil {
		conn.Close()
	}
	status := 200
	if !db || !cache || !broker {
		status = 503
	}
	respond(w, status, map[string]any{"database": db, "redis": cache, "redpanda": broker})
}
func (a *App) rows(ctx context.Context, q string, args ...any) ([]json.RawMessage, error) {
	rows, err := a.DB.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		out = append(out, json.RawMessage(b))
	}
	return out, rows.Err()
}
func (a *App) snapshot(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	out := map[string]any{}
	queries := map[string]string{"depots": "SELECT row_to_json(t) FROM depots t ORDER BY name", "vehicles": "SELECT row_to_json(t) FROM vehicles t ORDER BY id", "drivers": "SELECT row_to_json(t) FROM drivers t ORDER BY id", "deliveries": "SELECT row_to_json(t) FROM deliveries t ORDER BY created_at DESC", "routes": "SELECT row_to_json(t) FROM routes t ORDER BY created_at DESC", "alerts": "SELECT row_to_json(t) FROM alerts t ORDER BY created_at DESC LIMIT 500", "assignments": "SELECT row_to_json(t) FROM assignments t ORDER BY assigned_at DESC LIMIT 1000", "settings": "SELECT row_to_json(t) FROM settings t"}
	for k, q := range queries {
		v, err := a.rows(ctx, q)
		if err != nil {
			fail(w, 503, err.Error())
			return
		}
		out[k] = v
	}
	positions := []json.RawMessage{}
	cached, err := a.Redis.HGetAll(ctx, "fleet:positions").Result()
	if err == nil {
		for _, v := range cached {
			positions = append(positions, json.RawMessage(v))
		}
	}
	// Fill cache misses from the durable current-position projection.
	var vehicles []json.RawMessage
	vehicles = out["vehicles"].([]json.RawMessage)
	seen := map[string]bool{}
	for _, p := range positions {
		var e Event
		_ = json.Unmarshal(p, &e)
		seen[e.VehicleID] = true
	}
	for _, v := range vehicles {
		var x struct {
			ID       string          `json:"id"`
			Position json.RawMessage `json:"last_position"`
		}
		_ = json.Unmarshal(v, &x)
		if !seen[x.ID] && len(x.Position) > 0 && string(x.Position) != "null" {
			positions = append(positions, x.Position)
		}
	}
	out["positions"] = positions
	var metrics []byte
	err = a.DB.QueryRow(ctx, `SELECT json_build_object('active_vehicles',(SELECT count(*) FROM vehicles WHERE status='moving'),'completed_deliveries',(SELECT count(*) FROM deliveries WHERE status='completed'),'pending_deliveries',(SELECT count(*) FROM deliveries WHERE status NOT IN ('completed','cancelled')),'delayed_deliveries',(SELECT count(*) FROM deliveries WHERE status='delayed'),'active_exceptions',(SELECT count(*) FROM alerts WHERE status<>'resolved'),'telemetry_last_minute',(SELECT count(*) FROM telemetry WHERE ts>now()-interval '1 minute'),'telemetry_total',(SELECT count(*) FROM processed_events),'running_journeys',(SELECT count(*) FROM journeys WHERE status='running'))`).Scan(&metrics)
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	out["metrics"] = json.RawMessage(metrics)
	respond(w, 200, out)
}
