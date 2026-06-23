package quarkus

import (
	"math"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/stat"
)

// Cohort bounds. There are 128 statically defined cohorts, inclusive.
const (
	MinCohort = 1
	MaxCohort = 128
)

// RequestPriority is the priority assigned to a request. Lower ordinal = higher
// priority (less likely to be shed).
type RequestPriority int

const (
	// PriorityCritical requests should almost never be rejected.
	PriorityCritical RequestPriority = iota
	// PriorityImportant requests should only be rejected under high load.
	PriorityImportant
	// PriorityNormal is a normal request.
	PriorityNormal
	// PriorityBackground requests may be rejected if needed.
	PriorityBackground
	// PriorityDegraded requests may be rejected freely.
	PriorityDegraded

	numPriorities = iota
)

// cohortBaseline is the per-priority score offset: ordinal * MaxCohort.
func (p RequestPriority) cohortBaseline() int64 {
	return int64(p) * MaxCohort
}

// RequestClassifier assigns a cohort number to a request. All classifiers are
// inspected and the first whose AppliesTo returns true is used.
type RequestClassifier interface {
	AppliesTo(request any) bool
	Cohort(request any) int
}

// RequestPrioritizer assigns a priority to a request. All prioritizers are
// inspected and the first whose AppliesTo returns true is used.
type RequestPrioritizer interface {
	AppliesTo(request any) bool
	Priority(request any) RequestPriority
}

// PriorityLoadShedding decides which requests to shed once the system is known
// to be overloaded, scoring each request's priority and cohort against a
// threshold derived from CPU load.
type PriorityLoadShedding struct {
	enabled      bool
	max          int64
	cpuLoad      func() float64
	now          func() time.Time
	prioritizers []RequestPrioritizer
	classifiers  []RequestClassifier

	mu                sync.Mutex
	lastThreshold     float64
	lastThresholdTime int64 // unix milliseconds
}

// PriorityOption customizes a PriorityLoadShedding.
type PriorityOption func(*PriorityLoadShedding)

// WithCpuLoad overrides the CPU-load source. The function must return a value in
// [0, 1], or a negative value to signal "shed everything".
func WithCpuLoad(f func() float64) PriorityOption {
	return func(p *PriorityLoadShedding) { p.cpuLoad = f }
}

// WithClock overrides the clock, primarily for tests.
func WithClock(f func() time.Time) PriorityOption {
	return func(p *PriorityLoadShedding) { p.now = f }
}

// WithPrioritizers sets the request prioritizers, inspected in order.
func WithPrioritizers(prioritizers ...RequestPrioritizer) PriorityOption {
	return func(p *PriorityLoadShedding) { p.prioritizers = prioritizers }
}

// WithClassifiers sets the request classifiers, inspected in order.
func WithClassifiers(classifiers ...RequestClassifier) PriorityOption {
	return func(p *PriorityLoadShedding) { p.classifiers = classifiers }
}

// NewPriorityLoadShedding returns a PriorityLoadShedding configured from cfg.
func NewPriorityLoadShedding(cfg Config, opts ...PriorityOption) *PriorityLoadShedding {
	p := &PriorityLoadShedding{
		enabled: cfg.PriorityEnabled,
		max:     int64(numPriorities) * MaxCohort,
		cpuLoad: defaultCpuLoad,
		now:     time.Now,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// ShedLoad reports whether the given request should be shed. The caller must
// only invoke it when the system is already known to be overloaded.
func (p *PriorityLoadShedding) ShedLoad(request any) bool {
	if !p.enabled {
		return true
	}

	now := p.now().UnixMilli()
	p.mu.Lock()
	// Recompute the threshold at most once per second.
	if now-p.lastThresholdTime > 1000 {
		load := p.cpuLoad()
		if load < 0 {
			p.lastThreshold = -1
		} else {
			// Cubic in load: as load approaches 1, the threshold collapses,
			// shedding progressively more (lower-priority, higher-cohort) requests.
			p.lastThreshold = float64(p.max) * (1.0 - load*load*load)
		}
		p.lastThresholdTime = now
	}
	threshold := p.lastThreshold
	p.mu.Unlock()

	if threshold < 0 {
		return true
	}

	priority := PriorityNormal
	for _, prioritizer := range p.prioritizers {
		if prioritizer.AppliesTo(request) {
			priority = prioritizer.Priority(request)
			break
		}
	}

	cohort := 64 // middle of the [1, 128] interval
	for _, classifier := range p.classifiers {
		if classifier.AppliesTo(request) {
			cohort = classifier.Cohort(request)
			break
		}
	}
	cohort = normalizeCohort(cohort)

	return float64(priority.cohortBaseline()+int64(cohort)) > threshold
}

func normalizeCohort(cohort int) int {
	switch {
	case cohort == math.MinInt:
		return MaxCohort
	case cohort < 0:
		return (-cohort)%MaxCohort + 1
	case cohort == 0:
		return MinCohort
	case cohort > MaxCohort:
		return cohort%MaxCohort + 1
	default:
		return cohort
	}
}

// defaultCpuLoad maps go-zero's cgroup-aware CPU usage (millicpu, 0-1000) into
// the [0, 1] range expected by the threshold formula.
func defaultCpuLoad() float64 {
	return float64(stat.CpuUsage()) / 1000.0
}
