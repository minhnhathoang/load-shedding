// Package simulate drives synthetic open-model HTTP traffic through a load
// shedder so the different strategies can be compared and self-tested before
// being wired into a real service.
//
// It depends only on a shedder exposing the common Handler middleware shape
// (func(http.Handler) http.Handler), which every shedder in this repository
// satisfies — that is the pluggability contract a host service relies on.
package simulate

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Shedder is the pluggable contract: a middleware that may shed requests with
// 503. gozero.Shedder, quarkus.Shedder, queue.Pool and queue.CodelPool all
// satisfy it.
type Shedder interface {
	Handler(next http.Handler) http.Handler
}

// Scenario describes a synthetic load profile (open model — arrivals are issued
// at a fixed rate regardless of how fast the service responds).
type Scenario struct {
	Duration    time.Duration // how long to issue traffic
	RPS         int           // arrival rate (requests per second)
	ServiceTime time.Duration // synthetic work each admitted request performs
}

// Result is the outcome of a simulation run.
type Result struct {
	Name   string
	Sent   int64
	Served int64         // 2xx
	Shed   int64         // 503
	Other  int64         // anything else (e.g. 499)
	P50    time.Duration // latency percentiles of served requests
	P95    time.Duration
	P99    time.Duration
	Wall   time.Duration
}

// ShedRate is the fraction of requests rejected.
func (r Result) ShedRate() float64 {
	if r.Sent == 0 {
		return 0
	}
	return float64(r.Shed) / float64(r.Sent)
}

// Run drives the scenario through s wrapping a synthetic backend and returns the
// observed Result. It blocks until all issued requests complete.
func Run(name string, s Shedder, sc Scenario) Result {
	if sc.RPS < 1 {
		sc.RPS = 1
	}

	backend := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if sc.ServiceTime > 0 {
			time.Sleep(sc.ServiceTime)
		}
		w.WriteHeader(http.StatusOK)
	})
	h := s.Handler(backend)

	var sent, served, shed, other int64
	var mu sync.Mutex
	var latencies []time.Duration

	interval := time.Second / time.Duration(sc.RPS)
	if interval <= 0 {
		interval = time.Microsecond
	}

	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(sc.Duration)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for now := range ticker.C {
		if now.After(deadline) {
			break
		}
		atomic.AddInt64(&sent, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			t0 := time.Now()
			h.ServeHTTP(rec, req)
			d := time.Since(t0)

			switch rec.Code {
			case http.StatusOK:
				atomic.AddInt64(&served, 1)
				mu.Lock()
				latencies = append(latencies, d)
				mu.Unlock()
			case http.StatusServiceUnavailable:
				atomic.AddInt64(&shed, 1)
			default:
				atomic.AddInt64(&other, 1)
			}
		}()
	}
	wg.Wait()
	wall := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return Result{
		Name:   name,
		Sent:   sent,
		Served: served,
		Shed:   shed,
		Other:  other,
		P50:    percentile(latencies, 0.50),
		P95:    percentile(latencies, 0.95),
		P99:    percentile(latencies, 0.99),
		Wall:   wall,
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// FormatTable renders results as an aligned text table for quick comparison.
func FormatTable(results []Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-22s %7s %7s %7s %8s %8s %8s %8s\n",
		"strategy", "sent", "served", "shed", "shed%", "p50", "p95", "p99")
	for _, r := range results {
		fmt.Fprintf(&b, "%-22s %7d %7d %7d %7.1f%% %8s %8s %8s\n",
			r.Name, r.Sent, r.Served, r.Shed, r.ShedRate()*100,
			roundMs(r.P50), roundMs(r.P95), roundMs(r.P99))
	}
	return b.String()
}

func roundMs(d time.Duration) time.Duration {
	return d.Round(time.Millisecond)
}
