package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"net/http"
	"time"
)

type DeliveryInput struct {
	Reference   string    `json:"reference"`
	Customer    string    `json:"customer"`
	Address     string    `json:"address"`
	Lat         float64   `json:"lat"`
	Lon         float64   `json:"lon"`
	Priority    string    `json:"priority"`
	ScheduledAt time.Time `json:"scheduled_at"`
	DueAt       time.Time `json:"due_at"`
}

func (d *DeliveryInput) validate() error {
	if d.Reference == "" || d.Customer == "" || d.Address == "" {
		return errors.New("reference, customer and address are required")
	}
	if d.Lat < -90 || d.Lat > 90 || d.Lon < -180 || d.Lon > 180 {
		return errors.New("invalid coordinates")
	}
	if d.Priority == "" {
		d.Priority = "normal"
	}
	switch d.Priority {
	case "low", "normal", "high", "urgent":
	default:
		return errors.New("invalid priority")
	}
	if d.ScheduledAt.IsZero() {
		d.ScheduledAt = time.Now().UTC()
	}
	if d.DueAt.IsZero() {
		d.DueAt = d.ScheduledAt.Add(2 * time.Hour)
	}
	if d.DueAt.Before(d.ScheduledAt) {
		return errors.New("due time precedes schedule")
	}
	return nil
}
func (a *App) createDelivery(w http.ResponseWriter, r *http.Request) {
	var d DeliveryInput
	if !decode(w, r, &d) {
		return
	}
	if err := d.validate(); err != nil {
		fail(w, 400, err.Error())
		return
	}
	id := ID("del-")
	_, err := a.DB.Exec(r.Context(), `INSERT INTO deliveries(id,reference,customer,address,lat,lon,priority,scheduled_at,due_at,status) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'scheduled')`, id, d.Reference, d.Customer, d.Address, d.Lat, d.Lon, d.Priority, d.ScheduledAt, d.DueAt)
	if err != nil {
		fail(w, 409, err.Error())
		return
	}
	a.Hub.Broadcast("change", map[string]string{"entity": "delivery"})
	respond(w, 201, map[string]string{"id": id})
}
func (a *App) editDelivery(w http.ResponseWriter, r *http.Request) {
	var d DeliveryInput
	if !decode(w, r, &d) {
		return
	}
	if err := d.validate(); err != nil {
		fail(w, 400, err.Error())
		return
	}
	tag, err := a.DB.Exec(r.Context(), `UPDATE deliveries SET reference=$2,customer=$3,address=$4,lat=$5,lon=$6,priority=$7,scheduled_at=$8,due_at=$9 WHERE id=$1 AND status IN ('pending','scheduled') AND route_id IS NULL`, r.PathValue("id"), d.Reference, d.Customer, d.Address, d.Lat, d.Lon, d.Priority, d.ScheduledAt, d.DueAt)
	if err != nil {
		fail(w, 409, err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		fail(w, 409, "delivery does not exist or is already routed")
		return
	}
	a.Hub.Broadcast("change", nil)
	respond(w, 200, map[string]bool{"ok": true})
}

// Fixtures use short orthogonal street legs, avoiding a mandatory external routing API.
func geometry(start []float64, stops [][]float64) [][]float64 {
	g := [][]float64{start}
	for _, p := range stops {
		last := g[len(g)-1]
		g = append(g, []float64{p[0], last[1]}, p)
	}
	return g
}

type RouteInput struct {
	Name        string   `json:"name"`
	DepotID     string   `json:"depot_id"`
	DeliveryIDs []string `json:"delivery_ids"`
}

func (a *App) createRoute(w http.ResponseWriter, r *http.Request) {
	var d RouteInput
	if !decode(w, r, &d) {
		return
	}
	if d.Name == "" || len(d.DeliveryIDs) == 0 || len(d.DeliveryIDs) > 100 {
		fail(w, 400, "name and 1-100 delivery IDs required")
		return
	}
	tx, err := a.DB.Begin(r.Context())
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	var lat, lon float64
	if err = tx.QueryRow(r.Context(), `SELECT lat,lon FROM depots WHERE id=$1`, d.DepotID).Scan(&lat, &lon); err != nil {
		fail(w, 400, "unknown depot")
		return
	}
	stops := [][]float64{}
	seen := map[string]bool{}
	for _, id := range d.DeliveryIDs {
		if seen[id] {
			fail(w, 400, "duplicate delivery")
			return
		}
		seen[id] = true
		var status string
		var route *string
		var la, lo float64
		err = tx.QueryRow(r.Context(), `SELECT lat,lon,status,route_id FROM deliveries WHERE id=$1 FOR UPDATE`, id).Scan(&la, &lo, &status, &route)
		if err != nil || route != nil || (status != "pending" && status != "scheduled") {
			fail(w, 409, "delivery unavailable: "+id)
			return
		}
		stops = append(stops, []float64{lo, la})
	}
	id := ID("route-")
	g, _ := json.Marshal(geometry([]float64{lon, lat}, stops))
	if _, err = tx.Exec(r.Context(), `INSERT INTO routes(id,name,geometry) VALUES($1,$2,$3)`, id, d.Name, g); err != nil {
		fail(w, 409, err.Error())
		return
	}
	for i, del := range d.DeliveryIDs {
		_, err = tx.Exec(r.Context(), `UPDATE deliveries SET route_id=$2,sequence=$3 WHERE id=$1`, del, id, i+1)
		if err != nil {
			fail(w, 409, err.Error())
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		fail(w, 409, err.Error())
		return
	}
	a.Hub.Broadcast("change", nil)
	respond(w, 201, map[string]string{"id": id})
}

type AssignmentInput struct {
	VehicleID string `json:"vehicle_id"`
	DriverID  string `json:"driver_id"`
	Reason    string `json:"reason"`
}

func assignRoute(ctx context.Context, tx pgx.Tx, route string, d AssignmentInput) error {
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM routes WHERE id=$1 FOR UPDATE`, route).Scan(&status); err != nil {
		return err
	}
	if status != "planned" {
		return errors.New("route must be planned")
	}
	var vs, ds string
	if err := tx.QueryRow(ctx, `SELECT status FROM vehicles WHERE id=$1 FOR UPDATE`, d.VehicleID).Scan(&vs); err != nil {
		return errors.New("unknown vehicle")
	}
	if err := tx.QueryRow(ctx, `SELECT status FROM drivers WHERE id=$1 FOR UPDATE`, d.DriverID).Scan(&ds); err != nil {
		return errors.New("unknown driver")
	}
	if vs != "available" || ds != "available" {
		return errors.New("vehicle and driver must be available")
	}
	if d.Reason == "" {
		d.Reason = "initial assignment"
	}
	_, err := tx.Exec(ctx, `UPDATE routes SET vehicle_id=$2,driver_id=$3,status='assigned' WHERE id=$1;`, route, d.VehicleID, d.DriverID)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE vehicles SET status='assigned' WHERE id=$1`, d.VehicleID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE drivers SET status='assigned' WHERE id=$1`, d.DriverID); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT id FROM deliveries WHERE route_id=$1 AND status NOT IN ('completed','cancelled') ORDER BY sequence`, route)
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err = tx.Exec(ctx, `UPDATE deliveries SET status='assigned' WHERE id=$1`, id); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO assignments(id,delivery_id,route_id,vehicle_id,driver_id,reason) VALUES($1,$2,$3,$4,$5,$6)`, ID("assignment-"), id, route, d.VehicleID, d.DriverID, d.Reason); err != nil {
			return err
		}
	}
	return nil
}
func (a *App) assign(w http.ResponseWriter, r *http.Request) {
	var d AssignmentInput
	if !decode(w, r, &d) {
		return
	}
	tx, err := a.DB.Begin(r.Context())
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	if err = assignRoute(r.Context(), tx, r.PathValue("id"), d); err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		fail(w, 409, err.Error())
		return
	}
	a.Hub.Broadcast("change", nil)
	respond(w, 200, map[string]bool{"ok": true})
}
func dispatchRoute(ctx context.Context, tx pgx.Tx, id string) (string, error) {
	var status, vehicle, driver string
	err := tx.QueryRow(ctx, `SELECT status,vehicle_id,driver_id FROM routes WHERE id=$1 FOR UPDATE`, id).Scan(&status, &vehicle, &driver)
	if err != nil {
		return "", err
	}
	if status != "assigned" {
		return "", errors.New("route must be assigned")
	}
	var future int
	err = tx.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE route_id=$1 AND scheduled_at>now()`, id).Scan(&future)
	if err != nil {
		return "", err
	}
	if future > 0 {
		return "", errors.New("deliveries are scheduled in the future")
	}
	journey := ID("journey-")
	if _, err = tx.Exec(ctx, `INSERT INTO journeys(id,route_id,vehicle_id,driver_id,status) VALUES($1,$2,$3,$4,'running')`, journey, id, vehicle, driver); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `UPDATE routes SET status='active' WHERE id=$1`, id); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `UPDATE vehicles SET status='moving' WHERE id=$1`, vehicle); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `UPDATE drivers SET status='driving' WHERE id=$1`, driver); err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, `UPDATE deliveries SET status='in_transit' WHERE route_id=$1 AND status='assigned'`, id)
	return journey, err
}
func (a *App) dispatch(w http.ResponseWriter, r *http.Request) {
	tx, err := a.DB.Begin(r.Context())
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	id, err := dispatchRoute(r.Context(), tx, r.PathValue("id"))
	if err == nil {
		_, err = tx.Exec(r.Context(), `UPDATE settings SET sim_state='running'`)
	}
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		fail(w, 409, err.Error())
		return
	}
	a.Hub.Broadcast("change", nil)
	respond(w, 201, map[string]string{"journey_id": id})
}
func (a *App) reassign(w http.ResponseWriter, r *http.Request) {
	var d AssignmentInput
	if !decode(w, r, &d) {
		return
	}
	if d.Reason == "" {
		fail(w, 400, "reassignment reason required")
		return
	}
	ctx := r.Context()
	tx, err := a.DB.Begin(ctx)
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	defer tx.Rollback(ctx)
	var old, status string
	var lat, lon float64
	err = tx.QueryRow(ctx, `SELECT route_id,status,lat,lon FROM deliveries WHERE id=$1 FOR UPDATE`, r.PathValue("id")).Scan(&old, &status, &lat, &lon)
	if err != nil || status == "completed" || status == "cancelled" {
		fail(w, 409, "delivery cannot be reassigned")
		return
	}
	var oldVehicle, oldDriver string
	if err = tx.QueryRow(ctx, `SELECT vehicle_id,driver_id FROM routes WHERE id=$1 FOR UPDATE`, old).Scan(&oldVehicle, &oldDriver); err != nil {
		fail(w, 409, "route has no assignment")
		return
	}
	var startLat, startLon float64
	if err = tx.QueryRow(ctx, `SELECT coalesce((v.last_position->>'lat')::float8,d.lat),coalesce((v.last_position->>'lon')::float8,d.lon) FROM vehicles v JOIN depots d ON d.id=v.depot_id WHERE v.id=$1`, d.VehicleID).Scan(&startLat, &startLon); err != nil {
		fail(w, 400, "unknown replacement vehicle")
		return
	}
	newRoute := ID("route-")
	g, _ := json.Marshal(geometry([]float64{startLon, startLat}, [][]float64{{lon, lat}}))
	_, err = tx.Exec(ctx, `INSERT INTO routes(id,name,geometry) VALUES($1,$2,$3)`, newRoute, "Recovery "+r.PathValue("id"), g)
	if err == nil {
		_, err = tx.Exec(ctx, `UPDATE assignments SET ended_at=now() WHERE delivery_id=$1 AND ended_at IS NULL`, r.PathValue("id"))
	}
	if err == nil {
		_, err = tx.Exec(ctx, `UPDATE deliveries SET route_id=$2,sequence=1,status='scheduled' WHERE id=$1`, r.PathValue("id"), newRoute)
	}
	if err == nil {
		err = assignRoute(ctx, tx, newRoute, d)
	}
	var remaining int
	if err == nil {
		err = tx.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE route_id=$1 AND status NOT IN ('completed','cancelled')`, old).Scan(&remaining)
	}
	if err == nil && remaining == 0 {
		_, err = tx.Exec(ctx, `UPDATE journeys SET status='stopped',ended_at=now() WHERE route_id=$1 AND status IN ('running','paused')`, old)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE routes SET status='cancelled' WHERE id=$1`, old)
		}
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE vehicles SET status='available' WHERE id=$1 AND status<>'breakdown'`, oldVehicle)
		}
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE drivers SET status='available' WHERE id=$1`, oldDriver)
		}
	}
	if err == nil {
		_, err = dispatchRoute(ctx, tx, newRoute)
	}
	if err == nil {
		_, err = tx.Exec(ctx, `UPDATE settings SET sim_state='running'`)
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		fail(w, 409, err.Error())
		return
	}
	a.Hub.Broadcast("change", nil)
	respond(w, 200, map[string]string{"route_id": newRoute})
}
func finishRoute(ctx context.Context, tx pgx.Tx, route string) error {
	var remaining int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE route_id=$1 AND status NOT IN ('completed','cancelled')`, route).Scan(&remaining); err != nil {
		return err
	}
	if remaining > 0 {
		return nil
	}
	var v, d string
	if err := tx.QueryRow(ctx, `SELECT vehicle_id,driver_id FROM routes WHERE id=$1 FOR UPDATE`, route).Scan(&v, &d); err != nil {
		return err
	}
	for _, q := range []struct {
		sql string
		arg string
	}{{`UPDATE routes SET status='completed' WHERE id=$1`, route}, {`UPDATE journeys SET status='completed',ended_at=now() WHERE route_id=$1 AND status IN ('running','paused')`, route}, {`UPDATE vehicles SET status='available' WHERE id=$1 AND status<>'breakdown'`, v}, {`UPDATE drivers SET status='available' WHERE id=$1`, d}, {`UPDATE assignments SET ended_at=now() WHERE route_id=$1 AND ended_at IS NULL`, route}} {
		if _, err := tx.Exec(ctx, q.sql, q.arg); err != nil {
			return err
		}
	}
	return nil
}
func (a *App) completeDelivery(w http.ResponseWriter, r *http.Request) {
	var d struct {
		Outcome string `json:"outcome"`
	}
	if !decode(w, r, &d) {
		return
	}
	if d.Outcome == "" {
		fail(w, 400, "outcome required")
		return
	}
	tx, err := a.DB.Begin(r.Context())
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	var route string
	err = tx.QueryRow(r.Context(), `UPDATE deliveries SET status='completed',completed_at=now(),outcome=$2 WHERE id=$1 AND status IN ('in_transit','delayed') RETURNING route_id`, r.PathValue("id"), d.Outcome).Scan(&route)
	if err == nil {
		err = finishRoute(r.Context(), tx, route)
	}
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		fail(w, 409, "delivery must be in transit: "+err.Error())
		return
	}
	a.Hub.Broadcast("change", nil)
	respond(w, 200, map[string]bool{"ok": true})
}
func (a *App) alertAction(w http.ResponseWriter, r *http.Request) {
	action := r.PathValue("action")
	if action != "acknowledge" && action != "resolve" {
		fail(w, 400, "unknown alert action")
		return
	}
	var d struct {
		Note string `json:"note"`
	}
	if !decode(w, r, &d) {
		return
	}
	if d.Note == "" {
		fail(w, 400, "note required")
		return
	}
	tx, err := a.DB.Begin(r.Context())
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	query := `UPDATE alerts SET status='acknowledged',acknowledged_at=now() WHERE id=$1 AND status='open'`
	if action == "resolve" {
		query = `UPDATE alerts SET status='resolved',resolved_at=now() WHERE id=$1 AND status IN ('open','acknowledged')`
	}
	tag, err := tx.Exec(r.Context(), query, r.PathValue("id"))
	if err == nil && tag.RowsAffected() == 0 {
		err = errors.New("alert transition unavailable")
	}
	if err == nil {
		_, err = tx.Exec(r.Context(), `INSERT INTO alert_history(alert_id,action,note) VALUES($1,$2,$3)`, r.PathValue("id"), action, d.Note)
	}
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		fail(w, 409, err.Error())
		return
	}
	a.Hub.Broadcast("change", nil)
	respond(w, 200, map[string]bool{"ok": true})
}
func (a *App) alertHistory(w http.ResponseWriter, r *http.Request) {
	v, err := a.rows(r.Context(), `SELECT row_to_json(t) FROM alert_history t WHERE alert_id=$1 ORDER BY at,id`, r.PathValue("id"))
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	respond(w, 200, v)
}
func (a *App) journeys(w http.ResponseWriter, r *http.Request) {
	v, err := a.rows(r.Context(), `SELECT row_to_json(t) FROM journeys t WHERE vehicle_id=$1 ORDER BY started_at DESC LIMIT 100`, r.PathValue("id"))
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	respond(w, 200, v)
}
func (a *App) history(w http.ResponseWriter, r *http.Request) {
	journey := r.URL.Query().Get("journey_id")
	if journey == "" {
		fail(w, 400, "journey_id required")
		return
	}
	after := r.URL.Query().Get("after")
	if after == "" {
		after = "1970-01-01T00:00:00Z"
	}
	t, err := time.Parse(time.RFC3339Nano, after)
	if err != nil {
		fail(w, 400, "invalid after timestamp")
		return
	}
	v, err := a.rows(r.Context(), `SELECT row_to_json(t) FROM (SELECT ts AS timestamp,event_id,vehicle_id,journey_id,route_id,lat,lon,speed,heading,seq,progress,scenario FROM telemetry WHERE vehicle_id=$1 AND journey_id=$2 AND ts>$3 ORDER BY ts,event_id LIMIT 5000) t`, r.PathValue("id"), journey, t)
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	respond(w, 200, map[string]any{"events": v, "has_more": len(v) == 5000})
}
func (a *App) seedHTTP(w http.ResponseWriter, r *http.Request) {
	if err := a.Seed(r.Context()); err != nil {
		fail(w, 409, err.Error())
		return
	}
	a.Hub.Broadcast("change", nil)
	respond(w, 200, map[string]bool{"ok": true})
}
func (a *App) Seed(ctx context.Context) error {
	tx, err := a.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(42)`); err != nil {
		return err
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM depots`).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return tx.Commit(ctx)
	}
	depots := []struct {
		id, name string
		lat, lon float64
	}{{"depot-jhb", "Johannesburg Central", -26.2041, 28.0473}, {"depot-mid", "Midrand Distribution", -25.9973, 28.1263}, {"depot-pta", "Pretoria North", -25.7479, 28.2293}}
	for _, d := range depots {
		if _, err = tx.Exec(ctx, `INSERT INTO depots VALUES($1,$2,$3,$4)`, d.id, d.name, d.lat, d.lon); err != nil {
			return err
		}
	}
	names := []string{"Sipho", "Thandi", "Lerato", "Michael", "Naledi", "David", "Nomsa", "Zanele", "Kabelo", "Sarah"}
	for i := 1; i <= 120; i++ {
		d := depots[(i-1)%3]
		v, driver := fmt.Sprintf("VH-%03d", i), fmt.Sprintf("DR-%03d", i)
		if _, err = tx.Exec(ctx, `INSERT INTO vehicles(id,registration,depot_id,capacity) VALUES($1,$2,$3,3500)`, v, fmt.Sprintf("GP %03d FLEET", i), d.id); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO drivers(id,name,license,depot_id) VALUES($1,$2,$3,$4)`, driver, fmt.Sprintf("%s %03d", names[(i-1)%len(names)], i), fmt.Sprintf("ZA-CDL-%05d", i), d.id); err != nil {
			return err
		}
		if i <= 100 {
			la := d.lat + .012 + float64(i%10)*.004
			lo := d.lon - .035 + float64(i%9)*.008
			del, route := fmt.Sprintf("DEL-%03d", i), fmt.Sprintf("RT-%03d", i)
			if _, err = tx.Exec(ctx, `INSERT INTO deliveries(id,reference,customer,address,lat,lon,priority,scheduled_at,due_at,status) VALUES($1,$2,$3,$4,$5,$6,'normal',now()-interval '1 minute',now()+interval '2 hours','scheduled')`, del, fmt.Sprintf("DEMO-%04d", i), fmt.Sprintf("Gauteng Retail %03d", i), fmt.Sprintf("%d Market Street, Gauteng", 10+i), la, lo); err != nil {
				return err
			}
			g, _ := json.Marshal(geometry([]float64{d.lon, d.lat}, [][]float64{{lo, la}}))
			if _, err = tx.Exec(ctx, `INSERT INTO routes(id,name,geometry) VALUES($1,$2,$3)`, route, fmt.Sprintf("Gauteng run %03d", i), g); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `UPDATE deliveries SET route_id=$2,sequence=1 WHERE id=$1`, del, route); err != nil {
				return err
			}
			if err = assignRoute(ctx, tx, route, AssignmentInput{VehicleID: v, DriverID: driver, Reason: "demo seed"}); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}
