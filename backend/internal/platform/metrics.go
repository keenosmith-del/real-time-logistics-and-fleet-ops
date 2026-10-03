package platform

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"math"
	"time"
)

func (a *App) monitor(ctx context.Context) {
	client := &kafka.Client{Addr: kafka.TCP(a.Broker), Timeout: 4 * time.Second}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reqs := make([]kafka.OffsetRequest, 12)
			parts := make([]int, 12)
			for i := range reqs {
				reqs[i] = kafka.LastOffsetOf(i)
				parts[i] = i
			}
			check, cancel := context.WithTimeout(ctx, 4*time.Second)
			end, err := client.ListOffsets(check, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{"fleet.telemetry.v1": reqs}})
			if err == nil {
				for _, group := range []string{"history-v1", "live-v1"} {
					committed, e := client.OffsetFetch(check, &kafka.OffsetFetchRequest{GroupID: group, Topics: map[string][]int{"fleet.telemetry.v1": parts}})
					if e != nil || committed.Error != nil {
						continue
					}
					offsets := map[int]int64{}
					for _, p := range committed.Topics["fleet.telemetry.v1"] {
						if p.Error == nil {
							offsets[p.Partition] = max(0, p.CommittedOffset)
						}
					}
					lag := 0.0
					valid := true
					for _, p := range end.Topics["fleet.telemetry.v1"] {
						if p.Error != nil {
							valid = false
							break
						}
						lag += math.Max(0, float64(p.LastOffset-offsets[p.Partition]))
					}
					if valid {
						a.Lag.WithLabelValues(group).Set(lag)
					}
				}
			}
			cancel()
		}
	}
}
func (a *App) registerOperationalMetrics() {
	for name, q := range map[string]string{
		"fleet_active_vehicles":      "SELECT count(*) FROM vehicles WHERE status='moving'",
		"fleet_completed_deliveries": "SELECT count(*) FROM deliveries WHERE status='completed'",
		"fleet_pending_deliveries":   "SELECT count(*) FROM deliveries WHERE status NOT IN ('completed','cancelled')",
		"fleet_delayed_deliveries":   "SELECT count(*) FROM deliveries WHERE status='delayed'",
		"fleet_active_exceptions":    "SELECT count(*) FROM alerts WHERE status<>'resolved'",
		"fleet_persisted_events":     "SELECT count(*) FROM processed_events",
	} {
		query := q
		prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: "Durable operational count"}, func() float64 {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var n float64
			if a.DB.QueryRow(ctx, query).Scan(&n) != nil {
				return math.NaN()
			}
			return n
		}))
	}
}
