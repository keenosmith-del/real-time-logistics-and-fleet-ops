package platform

import (
	"errors"
	"net/http"
)

func (a *App) createRegistry(w http.ResponseWriter, r *http.Request) {
	var d struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		DepotID  string `json:"depot_id"`
		Capacity int    `json:"capacity"`
	}
	if !decode(w, r, &d) {
		return
	}
	if d.ID == "" || d.Name == "" || d.DepotID == "" {
		fail(w, 400, "ID, name and depot required")
		return
	}
	var err error
	switch r.PathValue("kind") {
	case "vehicle":
		if d.Capacity <= 0 {
			fail(w, 400, "positive capacity required")
			return
		}
		_, err = a.DB.Exec(r.Context(), `INSERT INTO vehicles(id,registration,depot_id,capacity) VALUES($1,$2,$3,$4)`, d.ID, d.Name, d.DepotID, d.Capacity)
	case "driver":
		_, err = a.DB.Exec(r.Context(), `INSERT INTO drivers(id,name,license,depot_id) VALUES($1,$2,$3,$4)`, d.ID, d.Name, "ZA-CDL-"+d.ID, d.DepotID)
	default:
		fail(w, 400, "unknown resource")
		return
	}
	if err != nil {
		fail(w, 409, err.Error())
		return
	}
	a.Hub.Broadcast("change", nil)
	respond(w, 201, map[string]string{"id": d.ID})
}
func (a *App) editRegistry(w http.ResponseWriter, r *http.Request) {
	var d struct {
		Status  string `json:"status"`
		DepotID string `json:"depot_id"`
	}
	if !decode(w, r, &d) {
		return
	}
	ctx := r.Context()
	tx, err := a.DB.Begin(ctx)
	if err != nil {
		fail(w, 503, err.Error())
		return
	}
	defer tx.Rollback(ctx)
	kind := r.PathValue("kind")
	table := "vehicles"
	if kind == "driver" {
		table = "drivers"
	} else if kind != "vehicle" {
		fail(w, 400, "unknown resource")
		return
	}
	var old string
	err = tx.QueryRow(ctx, "SELECT status FROM "+table+" WHERE id=$1 FOR UPDATE", r.PathValue("id")).Scan(&old)
	if err == nil {
		var active int
		column := "vehicle_id"
		if kind == "driver" {
			column = "driver_id"
		}
		err = tx.QueryRow(ctx, "SELECT count(*) FROM routes WHERE "+column+"=$1 AND status IN ('assigned','active','paused','disrupted')", r.PathValue("id")).Scan(&active)
		if active > 0 {
			err = errors.New("resource still assigned to an operational route")
		}
	}
	if err == nil {
		if kind == "vehicle" {
			if d.Status != "available" && d.Status != "breakdown" {
				err = errors.New("invalid vehicle availability")
			}
		} else if d.Status != "available" && d.Status != "unavailable" {
			err = errors.New("invalid driver availability")
		}
	}
	if err == nil {
		_, err = tx.Exec(ctx, "UPDATE "+table+" SET status=$2,depot_id=$3 WHERE id=$1", r.PathValue("id"), d.Status, d.DepotID)
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
