package platform

import (
	"math"
	"testing"
	"time"
)

func TestRouteGeometry(t *testing.T) {
	g := geometry([]float64{28, -26}, [][]float64{{28.02, -26.02}, {28.03, -26.04}})
	if len(g) != 5 {
		t.Fatal("fixture stops lost")
	}
	if routeDistance([]float64{28.01, -26}, g) > 1 {
		t.Fatal("point on route deviates")
	}
	if routeDistance([]float64{28.2, -26.2}, g) < 1000 {
		t.Fatal("off-route point undetected")
	}
	la, lo, h := pointAt(g, 1)
	if math.Abs(la+26.04) > .000001 || math.Abs(lo-28.03) > .000001 || h < 0 || h >= 360 {
		t.Fatal("invalid route endpoint")
	}
}
func TestEventValidation(t *testing.T) {
	e := Event{EventID: "x", VehicleID: "v", JourneyID: "j", RouteID: "r", Timestamp: time.Now(), Seq: 1, Lat: -26, Lon: 28, Speed: 40, Heading: 120, Progress: .3}
	if err := validEvent(e); err != nil {
		t.Fatal(err)
	}
	e.Timestamp = time.Now().Add(time.Hour)
	if validEvent(e) == nil {
		t.Fatal("accepted future position")
	}
	e.Timestamp = time.Now()
	e.Lat = 100
	if validEvent(e) == nil {
		t.Fatal("accepted invalid latitude")
	}
}
