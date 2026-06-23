// Package sentinel adapts Alibaba Sentinel's system adaptive protection
// (https://github.com/alibaba/sentinel-golang) into the same shedder shape used
// by the sibling load-shedding packages.
//
// Sentinel sheds inbound load using the BBR adaptive strategy driven by system
// metrics (CPU usage, system load1, concurrency, average RT, inbound QPS). It is
// a global, process-wide mechanism: rules apply to all inbound Sentinel entries,
// not per-Shedder instance.
//
// This is an isolated nested module; pull it in explicitly with:
//
//	go get github.com/minhnhathoang/load-shedding/sentinel
package sentinel

import (
	"net/http"
	"sync"

	sentinel "github.com/alibaba/sentinel-golang/api"
	"github.com/alibaba/sentinel-golang/core/base"
	"github.com/alibaba/sentinel-golang/core/system"
)

const defaultResource = "go-zero-inbound"

var (
	initOnce sync.Once
	initErr  error
)

// Config configures the Sentinel-backed shedder. Each threshold is the BBR
// trigger count for its metric; a non-positive value disables that rule.
type Config struct {
	// Resource is the Sentinel resource name used for entries.
	Resource string
	// CpuThreshold triggers BBR on CPU usage. Range (0, 1], e.g. 0.8 = 80%.
	CpuThreshold float64
	// MaxConcurrency triggers BBR on inbound concurrency.
	MaxConcurrency float64
	// MaxLoad triggers BBR on system load1 (Linux/Unix).
	MaxLoad float64
	// AvgRtMillis triggers BBR on average inbound response time (milliseconds).
	AvgRtMillis float64
}

// DefaultConfig returns a config that sheds based on CPU usage at 80%.
func DefaultConfig() Config {
	return Config{
		Resource:     defaultResource,
		CpuThreshold: 0.8,
	}
}

// Shedder sheds inbound load using Sentinel system adaptive protection.
type Shedder struct {
	resource string
}

// New initializes Sentinel (once per process), loads the system rules derived
// from cfg, and returns a Shedder.
//
// Note: Sentinel system rules are global. Constructing multiple Shedders (or
// calling New repeatedly) replaces the previously loaded system rules.
func New(cfg Config) (*Shedder, error) {
	initOnce.Do(func() {
		initErr = sentinel.InitDefault()
	})
	if initErr != nil {
		return nil, initErr
	}

	rules := buildRules(cfg)
	if len(rules) > 0 {
		if _, err := system.LoadRules(rules); err != nil {
			return nil, err
		}
	}

	resource := cfg.Resource
	if resource == "" {
		resource = defaultResource
	}
	return &Shedder{resource: resource}, nil
}

func buildRules(cfg Config) []*system.Rule {
	var rules []*system.Rule
	add := func(mt system.MetricType, trigger float64) {
		if trigger > 0 {
			rules = append(rules, &system.Rule{
				MetricType:   mt,
				TriggerCount: trigger,
				Strategy:     system.BBR,
			})
		}
	}
	add(system.CpuUsage, cfg.CpuThreshold)
	add(system.Concurrency, cfg.MaxConcurrency)
	add(system.Load, cfg.MaxLoad)
	add(system.AvgRT, cfg.AvgRtMillis)
	return rules
}

// Allow asks Sentinel whether an inbound request may proceed. When ok is true
// the caller must invoke the returned done func exactly once on completion; when
// ok is false the request is shed and done is nil.
func (s *Shedder) Allow() (done func(), ok bool) {
	e, b := sentinel.Entry(s.resource, sentinel.WithTrafficType(base.Inbound))
	if b != nil {
		return nil, false
	}
	return func() { e.Exit() }, true
}

// Handler returns a middleware that sheds inbound requests with 503 Service
// Unavailable when Sentinel blocks them.
func (s *Shedder) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		done, ok := s.Allow()
		if !ok {
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		defer done()
		next.ServeHTTP(w, r)
	})
}
