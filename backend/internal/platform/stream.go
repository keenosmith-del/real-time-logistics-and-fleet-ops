package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
)

type permanentError struct{ error }

type Hub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]chan []byte
}

func (h *Hub) Broadcast(kind string, data any) {
	b, _ := json.Marshal(map[string]any{"type": kind, "data": data})
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.clients {
		select {
		case ch <- b:
		default: /* Bounded queues avoid a slow client blocking telemetry. */
		}
	}
}
func (a *App) ws(w http.ResponseWriter, r *http.Request) {
	u := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool {
		o := r.Header.Get("Origin")
		return o == "" || o == Env("CORS_ORIGIN", "http://localhost:5173") || o == "http://"+r.Host || o == "https://"+r.Host
	}}
	conn, err := u.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	ch := make(chan []byte, 256)
	a.Hub.mu.Lock()
	a.Hub.clients[conn] = ch
	a.Hub.mu.Unlock()
	defer func() { a.Hub.mu.Lock(); delete(a.Hub.clients, conn); a.Hub.mu.Unlock(); conn.Close() }()
	conn.SetReadLimit(1024)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(60 * time.Second)) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case b := <-ch:
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err = conn.WriteMessage(websocket.TextMessage, b); err != nil {
				return
			}
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err = conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
func validEvent(e Event) error {
	if e.EventID == "" || e.VehicleID == "" || e.JourneyID == "" || e.RouteID == "" || e.Timestamp.IsZero() || e.Seq < 1 {
		return errors.New("required telemetry identity missing")
	}
	if e.Lat < -90 || e.Lat > 90 || e.Lon < -180 || e.Lon > 180 || e.Speed < 0 || e.Speed > 300 || e.Heading < 0 || e.Heading >= 360 || e.Progress < 0 || e.Progress > 1 || math.IsNaN(e.Lat) || math.IsNaN(e.Lon) {
		return errors.New("telemetry outside schema bounds")
	}
	switch e.Scenario {
	case "", "deviation", "delay", "stop", "breakdown":
	default:
		return errors.New("unknown scenario")
	}
	if e.Timestamp.After(time.Now().Add(30 * time.Second)) {
		return errors.New("future GPS timestamp")
	}
	return nil
}
func (a *App) consume(ctx context.Context, group string, process func(context.Context, Event) error) {
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{a.Broker}, Topic: "fleet.telemetry.v1", GroupID: group, MinBytes: 1, MaxBytes: 10e6, CommitInterval: 0, StartOffset: kafka.FirstOffset, ReadLagInterval: 5 * time.Second})
	defer reader.Close()
	dlq := &kafka.Writer{Addr: kafka.TCP(a.Broker), Topic: "fleet.telemetry.dlq.v1", Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}
	defer dlq.Close()
	for ctx.Err() == nil {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() == nil {
				a.Logger.Error("consumer fetch", "group", group, "error", err)
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
			continue
		}

		var e Event
		err = json.Unmarshal(m.Value, &e)
		if err == nil {
			err = validEvent(e)
		}
		if err != nil {
			b, _ := json.Marshal(map[string]any{"consumer": group, "reason": err.Error(), "original": string(m.Value), "partition": m.Partition, "offset": m.Offset})
			for ctx.Err() == nil {
				if err = dlq.WriteMessages(ctx, kafka.Message{Key: m.Key, Value: b}); err == nil {
					break
				}
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
			if ctx.Err() != nil {
				return
			}
			a.Events.WithLabelValues(group, "dead_letter").Inc()
		} else {
			attempt := 0
			for ctx.Err() == nil {
				err = process(ctx, e)
				if err == nil {
					a.Events.WithLabelValues(group, "processed").Inc()
					break
				}
				var invalid permanentError
				if errors.As(err, &invalid) {
					payload, _ := json.Marshal(map[string]any{"consumer": group, "reason": err.Error(), "original": string(m.Value), "partition": m.Partition, "offset": m.Offset})
					for ctx.Err() == nil {
						if err = dlq.WriteMessages(ctx, kafka.Message{Key: m.Key, Value: payload}); err == nil {
							break
						}
						select {
						case <-ctx.Done():
						case <-time.After(time.Second):
						}
					}
					if ctx.Err() != nil {
						return
					}
					a.Events.WithLabelValues(group, "dead_letter").Inc()
					break
				}
				attempt++
				a.Events.WithLabelValues(group, "retry").Inc()
				a.Logger.Error("event retry", "consumer", group, "event_id", e.EventID, "attempt", attempt, "error", err)
				delay := time.Duration(min(attempt, 10)) * time.Second
				select {
				case <-ctx.Done():
				case <-time.After(delay):
				}
			}
			if ctx.Err() != nil {
				return
			}
		}
		for ctx.Err() == nil {
			if err = reader.CommitMessages(ctx, m); err == nil {
				break
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}
func (a *App) live(ctx context.Context, e Event) error {
	var exists bool
	if err := a.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM journeys WHERE id=$1 AND vehicle_id=$2 AND route_id=$3)`, e.JourneyID, e.VehicleID, e.RouteID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return permanentError{errors.New("unknown or mismatched journey context")}
	}
	b, _ := json.Marshal(e)
	// Compare millisecond timestamps and sequence atomically; late/duplicate events never rewind the live map.
	// Numeric epoch microseconds avoid lexical ordering errors in RFC3339 timestamps.
	const ordered = `local prev=redis.call('HGET',KEYS[2],ARGV[1]); local stamp=tonumber(ARGV[2]); if prev and tonumber(prev)>=stamp then return 0 end; redis.call('HSET',KEYS[2],ARGV[1],ARGV[2]); redis.call('HSET',KEYS[1],ARGV[1],ARGV[3]); return 1`
	n, err := a.Redis.Eval(ctx, ordered, []string{"fleet:positions", "fleet:position-times"}, e.VehicleID, e.Timestamp.UnixMicro(), string(b)).Int()
	if err != nil {
		return err
	}
	if n == 1 {
		a.Hub.Broadcast("position", e)
	}
	return nil
}
func (a *App) persist(ctx context.Context, e Event) error {
	tx, err := a.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING`, e.EventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		a.Events.WithLabelValues("history-v1", "duplicate").Inc()
		return tx.Commit(ctx)
	}
	var gjson []byte
	var journeyStatus string
	err = tx.QueryRow(ctx, `SELECT r.geometry,j.status FROM journeys j JOIN routes r ON r.id=j.route_id WHERE j.id=$1 AND j.vehicle_id=$2 AND j.route_id=$3 FOR UPDATE OF j,r`, e.JourneyID, e.VehicleID, e.RouteID).Scan(&gjson, &journeyStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return permanentError{errors.New("unknown or mismatched journey context")}
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO telemetry(ts,event_id,vehicle_id,journey_id,route_id,lat,lon,speed,heading,seq,progress,scenario) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING`, e.Timestamp, e.EventID, e.VehicleID, e.JourneyID, e.RouteID, e.Lat, e.Lon, e.Speed, e.Heading, e.Seq, e.Progress, e.Scenario)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(e)
	tag, err = tx.Exec(ctx, `UPDATE vehicles SET last_ts=$2,last_position=$3 WHERE id=$1 AND (last_ts IS NULL OR last_ts<$2)`, e.VehicleID, e.Timestamp, b)
	if err != nil {
		return err
	}
	changed := false
	if tag.RowsAffected() > 0 && (journeyStatus == "running" || journeyStatus == "paused") {
		var g [][]float64
		if err = json.Unmarshal(gjson, &g); err != nil {
			return err
		}
		var deviation float64
		var stopSeconds, delaySeconds int
		err = tx.QueryRow(ctx, `SELECT deviation_m,stop_seconds,delay_seconds FROM settings WHERE id=1`).Scan(&deviation, &stopSeconds, &delaySeconds)
		if err != nil {
			return err
		}
		alert := func(kind, severity, message string) error {
			var inserted string
			err := tx.QueryRow(ctx, `INSERT INTO alerts(id,vehicle_id,journey_id,route_id,type,severity,message) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (journey_id,type) WHERE status<>'resolved' DO NOTHING RETURNING id`, ID("alert-"), e.VehicleID, e.JourneyID, e.RouteID, kind, severity, message).Scan(&inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			changed = true
			_, err = tx.Exec(ctx, `INSERT INTO alert_history(alert_id,action,note) VALUES($1,'detected',$2)`, inserted, message)
			return err
		}
		if dist := routeDistance([]float64{e.Lon, e.Lat}, g); dist > deviation {
			if err = alert("route_deviation", "warning", fmt.Sprintf("Vehicle is %.0f m from its planned route", dist)); err != nil {
				return err
			}
		}
		if e.Speed < 1 {
			_, err = tx.Exec(ctx, `INSERT INTO stop_tracking(vehicle_id,since) VALUES($1,$2) ON CONFLICT DO NOTHING`, e.VehicleID, e.Timestamp)
			if err != nil {
				return err
			}
			var since time.Time
			if err = tx.QueryRow(ctx, `SELECT since FROM stop_tracking WHERE vehicle_id=$1`, e.VehicleID).Scan(&since); err != nil {
				return err
			}
			if e.Timestamp.Sub(since).Seconds() >= float64(stopSeconds) {
				if err = alert("prolonged_stop", "warning", fmt.Sprintf("Vehicle stopped for at least %d seconds", stopSeconds)); err != nil {
					return err
				}
			}
		} else {
			if _, err = tx.Exec(ctx, `DELETE FROM stop_tracking WHERE vehicle_id=$1`, e.VehicleID); err != nil {
				return err
			}
		}
		var late int
		err = tx.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE route_id=$1 AND status IN ('in_transit','delayed') AND due_at+($2*interval '1 second')<$3`, e.RouteID, delaySeconds, e.Timestamp).Scan(&late)
		if err != nil {
			return err
		}
		if late > 0 {
			if _, err = tx.Exec(ctx, `UPDATE deliveries SET status='delayed' WHERE route_id=$1 AND status='in_transit' AND due_at+($2*interval '1 second')<$3`, e.RouteID, delaySeconds, e.Timestamp); err != nil {
				return err
			}
			if err = alert("delay", "warning", "Delivery deadline exceeded"); err != nil {
				return err
			}
		}
		if e.Scenario == "breakdown" {
			if err = alert("breakdown", "critical", "Simulated mechanical breakdown; dispatch intervention required"); err != nil {
				return err
			}
			for _, q := range []struct {
				sql string
				id  string
			}{{`UPDATE vehicles SET status='breakdown' WHERE id=$1`, e.VehicleID}, {`UPDATE journeys SET status='paused' WHERE id=$1`, e.JourneyID}, {`UPDATE routes SET status='disrupted' WHERE id=$1`, e.RouteID}} {
				if _, err = tx.Exec(ctx, q.sql, q.id); err != nil {
					return err
				}
			}
			changed = true
		} else {
			total := 0.0
			for i := 1; i < len(g); i++ {
				total += distance(g[i-1], g[i])
			}
			cumulative := 0.0
			thresholds := map[int]float64{}
			for i := 1; i < len(g); i++ {
				cumulative += distance(g[i-1], g[i])
				if i%2 == 0 {
					thresholds[i/2] = cumulative / math.Max(total, 1)
				}
			}
			rows, err := tx.Query(ctx, `SELECT id,sequence FROM deliveries WHERE route_id=$1 AND status IN ('in_transit','delayed') ORDER BY sequence FOR UPDATE`, e.RouteID)
			if err != nil {
				return err
			}
			ids := []string{}
			for rows.Next() {
				var id string
				var sequence int
				if err = rows.Scan(&id, &sequence); err != nil {
					rows.Close()
					return err
				}
				if threshold, ok := thresholds[sequence]; ok && e.Progress >= threshold-.00001 && e.Scenario == "" {
					ids = append(ids, id)
				}
			}
			rows.Close()
			if err = rows.Err(); err != nil {
				return err
			}
			for _, id := range ids {
				if _, err = tx.Exec(ctx, `UPDATE deliveries SET status='completed',completed_at=$2,outcome='Simulated proof of delivery' WHERE id=$1`, id, e.Timestamp); err != nil {
					return err
				}
				if _, err = tx.Exec(ctx, `UPDATE assignments SET ended_at=$2 WHERE delivery_id=$1 AND ended_at IS NULL`, id, e.Timestamp); err != nil {
					return err
				}
				changed = true
			}
			if len(ids) > 0 {
				if err = finishRoute(ctx, tx, e.RouteID); err != nil {
					return err
				}
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	a.Latency.Observe(math.Max(0, time.Since(e.Timestamp).Seconds()))
	if changed {
		a.Hub.Broadcast("change", nil)
	}
	return nil
}
func (a *App) simControl(w http.ResponseWriter, r *http.Request) {
	a.control.Lock()
	defer a.control.Unlock()
	var d struct {
		Speed        float64 `json:"speed"`
		Count        int     `json:"count"`
		DeviationM   float64 `json:"deviation_m"`
		StopSeconds  int     `json:"stop_seconds"`
		DelaySeconds *int    `json:"delay_seconds"`
	}
	if !decode(w, r, &d) {
		return
	}
	action := r.PathValue("action")
	ctx := r.Context()
	tx, err := a.DB.Begin(ctx)
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	defer tx.Rollback(ctx)
	if d.Speed != 0 {
		if d.Speed < .1 || d.Speed > 100 {
			fail(w, 400, "speed must be 0.1-100")
			return
		}
		_, err = tx.Exec(ctx, `UPDATE settings SET speed=$1`, d.Speed)
	}
	if err == nil && d.DeviationM != 0 {
		if d.DeviationM < 10 {
			fail(w, 400, "deviation threshold must be >=10 m")
			return
		}
		_, err = tx.Exec(ctx, `UPDATE settings SET deviation_m=$1`, d.DeviationM)
	}
	if err == nil && d.StopSeconds != 0 {
		if d.StopSeconds < 2 {
			fail(w, 400, "stop threshold must be >=2 seconds")
			return
		}
		_, err = tx.Exec(ctx, `UPDATE settings SET stop_seconds=$1`, d.StopSeconds)
	}
	if err == nil && d.DelaySeconds != nil {
		if *d.DelaySeconds < 0 {
			fail(w, 400, "delay grace must be >=0")
			return
		}
		_, err = tx.Exec(ctx, `UPDATE settings SET delay_seconds=$1`, *d.DelaySeconds)
	}
	if err == nil {
		switch action {
		case "start":
			if d.Count == 0 {
				d.Count = 100
			}
			if d.Count < 1 || d.Count > 1000 {
				fail(w, 400, "count must be 1-1000")
				return
			}
			rows, e := tx.Query(ctx, `SELECT id FROM routes WHERE status='assigned' ORDER BY id LIMIT $1`, d.Count)
			if e != nil {
				err = e
				break
			}
			ids := []string{}
			for rows.Next() {
				var id string
				if e = rows.Scan(&id); e != nil {
					err = e
					break
				}
				ids = append(ids, id)
			}
			rows.Close()
			if err == nil {
				err = rows.Err()
			}
			for _, id := range ids {
				if err != nil {
					break
				}
				_, err = dispatchRoute(ctx, tx, id)
			}
			if err == nil {
				_, err = tx.Exec(ctx, `UPDATE settings SET sim_state='running'`)
			}
		case "pause":
			_, err = tx.Exec(ctx, `UPDATE settings SET sim_state='paused'; UPDATE journeys SET status='paused' WHERE status='running'; UPDATE routes SET status='paused' WHERE status='active'; UPDATE vehicles SET status='paused' WHERE status='moving'`)
		case "resume":
			_, err = tx.Exec(ctx, `UPDATE settings SET sim_state='running'; UPDATE journeys SET status='running' WHERE status='paused' AND scenario<>'breakdown'; UPDATE routes SET status='active' WHERE status='paused'; UPDATE vehicles v SET status='moving' FROM journeys j WHERE j.vehicle_id=v.id AND j.status='running' AND v.status='paused'`)
		case "stop":
			_, err = tx.Exec(ctx, `UPDATE settings SET sim_state='stopped'; UPDATE journeys SET status='stopped',ended_at=now() WHERE status IN ('running','paused'); UPDATE routes SET status='assigned' WHERE status IN ('active','paused','disrupted'); UPDATE vehicles SET status='assigned' WHERE status IN ('moving','paused'); UPDATE drivers SET status='assigned' WHERE status='driving'; UPDATE deliveries SET status='assigned' WHERE status IN ('in_transit','delayed'); DELETE FROM stop_tracking`)
		case "configure":
		default:
			err = errors.New("unknown simulation action")
		}
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		fail(w, 409, err.Error())
		return
	}
	a.Hub.Broadcast("change", nil)
	respond(w, 200, map[string]bool{"ok": true})
}
func (a *App) scenario(w http.ResponseWriter, r *http.Request) {
	var d struct {
		Scenario string `json:"scenario"`
	}
	if !decode(w, r, &d) {
		return
	}
	if !strings.Contains("|deviation|delay|stop|breakdown|clear|", "|"+d.Scenario+"|") || d.Scenario == "" {
		fail(w, 400, "scenario must be deviation, delay, stop, breakdown or clear")
		return
	}
	ctx := r.Context()
	tx, err := a.DB.Begin(ctx)
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	defer tx.Rollback(ctx)
	var journey, route string
	err = tx.QueryRow(ctx, `SELECT id,route_id FROM journeys WHERE vehicle_id=$1 AND status IN ('running','paused') FOR UPDATE`, r.PathValue("id")).Scan(&journey, &route)
	if err != nil {
		fail(w, 409, "vehicle has no active journey")
		return
	}
	scenario := d.Scenario
	if scenario == "clear" {
		scenario = ""
	}
	if _, err = tx.Exec(ctx, `UPDATE journeys SET scenario=$2 WHERE id=$1`, journey, scenario); err == nil && d.Scenario == "delay" {
		_, err = tx.Exec(ctx, `UPDATE deliveries SET scheduled_at=least(scheduled_at,now()-interval '1 hour'),due_at=now()-interval '10 minutes' WHERE route_id=$1 AND status NOT IN ('completed','cancelled')`, route)
	}
	if err == nil && d.Scenario == "clear" {
		_, err = tx.Exec(ctx, `UPDATE journeys SET status='running' WHERE id=$1`, journey)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE routes SET status='active' WHERE id=$1`, route)
		}
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE vehicles SET status='moving' WHERE id=$1`, r.PathValue("id"))
		}
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE settings SET sim_state='running'`)
		}
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		fail(w, 409, err.Error())
		return
	}
	a.Hub.Broadcast("change", nil)
	respond(w, 200, map[string]bool{"ok": true})
}
func (a *App) simulate(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	sem := make(chan struct{}, 32)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var state string
			var speed float64
			if err := a.DB.QueryRow(ctx, `SELECT sim_state,speed FROM settings`).Scan(&state, &speed); err != nil || state != "running" {
				continue
			}
			rows, err := a.DB.Query(ctx, `SELECT j.id,j.vehicle_id,j.route_id,j.progress,j.seq,j.scenario,r.geometry FROM journeys j JOIN routes r ON r.id=j.route_id WHERE j.status='running'`)
			if err != nil {
				a.Logger.Error("simulation query", "error", err)
				continue
			}
			type job struct {
				id, v, r, scenario string
				p                  float64
				seq                int64
				g                  []byte
			}
			jobs := []job{}
			for rows.Next() {
				var j job
				if err = rows.Scan(&j.id, &j.v, &j.r, &j.p, &j.seq, &j.scenario, &j.g); err != nil {
					break
				}
				jobs = append(jobs, j)
			}
			rows.Close()
			if err != nil {
				a.Logger.Error("simulation scan", "error", err)
				continue
			}
			var wg sync.WaitGroup
			for _, j := range jobs {
				wg.Add(1)
				sem <- struct{}{}
				go func(j job) {
					defer wg.Done()
					defer func() { <-sem }()
					var g [][]float64
					if json.Unmarshal(j.g, &g) != nil || len(g) < 2 {
						return
					}
					total := 0.0
					for i := 1; i < len(g); i++ {
						total += distance(g[i-1], g[i])
					}
					velocity := 40.0
					p := j.p
					switch j.scenario {
					case "stop", "breakdown", "delay":
						velocity = 0
					default:
						p = math.Min(1, p+speed*(velocity/3.6)/math.Max(total, 1))
					}
					lat, lon, heading := pointAt(g, p)
					if j.scenario == "deviation" {
						lat += .015
						lon += .015
					}
					e := Event{EventID: fmt.Sprintf("%s:%d", j.id, j.seq+1), Timestamp: time.Now().UTC(), VehicleID: j.v, JourneyID: j.id, RouteID: j.r, Lat: lat, Lon: lon, Speed: velocity, Heading: heading, Seq: j.seq + 1, Progress: p, Scenario: j.scenario}
					b, _ := json.Marshal(e)
					writeCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
					defer cancel()
					if err := a.Writer.WriteMessages(writeCtx, kafka.Message{Key: []byte(j.v), Value: b}); err != nil {
						a.Logger.Error("produce", "vehicle", j.v, "error", err)
						return
					}
					_, err := a.DB.Exec(ctx, `UPDATE journeys SET progress=$2,seq=$3 WHERE id=$1 AND seq=$4`, j.id, p, j.seq+1, j.seq)
					if err != nil {
						a.Logger.Error("checkpoint", "error", err)
					}
				}(j)
			}
			wg.Wait()
		}
	}
}
