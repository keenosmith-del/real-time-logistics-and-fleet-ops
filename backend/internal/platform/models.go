package platform

import (
	"crypto/rand"
	"encoding/hex"
	"math"
	"time"
)

type Event struct {
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
	VehicleID string    `json:"vehicle_id"`
	JourneyID string    `json:"journey_id"`
	RouteID   string    `json:"route_id"`
	Lat       float64   `json:"lat"`
	Lon       float64   `json:"lon"`
	Speed     float64   `json:"speed"`
	Heading   float64   `json:"heading"`
	Seq       int64     `json:"seq"`
	Progress  float64   `json:"progress"`
	Scenario  string    `json:"scenario"`
}

func ID(prefix string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b)
}
func distance(a, b []float64) float64 {
	const r = 6371000.0
	p1, p2 := a[1]*math.Pi/180, b[1]*math.Pi/180
	dp, dl := (b[1]-a[1])*math.Pi/180, (b[0]-a[0])*math.Pi/180
	h := math.Sin(dp/2)*math.Sin(dp/2) + math.Cos(p1)*math.Cos(p2)*math.Sin(dl/2)*math.Sin(dl/2)
	return 2 * r * math.Asin(math.Sqrt(math.Min(1, h)))
}
func routeDistance(p []float64, g [][]float64) float64 {
	best := math.MaxFloat64
	for i := 1; i < len(g); i++ {
		scale := math.Cos(p[1] * math.Pi / 180)
		ax, ay := (g[i-1][0]-p[0])*scale, g[i-1][1]-p[1]
		bx, by := (g[i][0]-p[0])*scale, g[i][1]-p[1]
		dx, dy := bx-ax, by-ay
		t := 0.0
		if d := dx*dx + dy*dy; d > 0 {
			t = math.Max(0, math.Min(1, -(ax*dx+ay*dy)/d))
		}
		q := []float64{p[0] + (ax+t*dx)/scale, p[1] + ay + t*dy}
		best = math.Min(best, distance(p, q))
	}
	return best
}
func pointAt(g [][]float64, p float64) (float64, float64, float64) {
	lengths := make([]float64, len(g)-1)
	total := 0.0
	for i := range lengths {
		lengths[i] = distance(g[i], g[i+1])
		total += lengths[i]
	}
	target := math.Min(1, math.Max(0, p)) * total
	for i, l := range lengths {
		if target <= l || i == len(lengths)-1 {
			t := 0.0
			if l > 0 {
				t = target / l
			}
			a, b := g[i], g[i+1]
			heading := math.Mod(math.Atan2((b[0]-a[0])*math.Cos(a[1]*math.Pi/180), b[1]-a[1])*180/math.Pi+360, 360)
			return a[1] + (b[1]-a[1])*t, a[0] + (b[0]-a[0])*t, heading
		}
		target -= l
	}
	return g[0][1], g[0][0], 0
}
