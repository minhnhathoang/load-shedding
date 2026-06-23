package quarkus

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCohortBaseline(t *testing.T) {
	assert.Equal(t, int64(0), PriorityCritical.cohortBaseline())
	assert.Equal(t, int64(128), PriorityImportant.cohortBaseline())
	assert.Equal(t, int64(256), PriorityNormal.cohortBaseline())
	assert.Equal(t, int64(384), PriorityBackground.cohortBaseline())
	assert.Equal(t, int64(512), PriorityDegraded.cohortBaseline())
}

func TestNormalizeCohort(t *testing.T) {
	tests := []struct {
		in   int
		want int
	}{
		{math.MinInt, MaxCohort},
		{-1, 2},   // (-(-1))%128 + 1 = 1%128 + 1 = 2
		{-128, 1}, // 128%128 + 1 = 1
		{0, MinCohort},
		{1, 1},
		{128, 128},
		{129, 2}, // 129%128 + 1 = 1 + 1 = 2
		{256, 1}, // 256%128 + 1 = 0 + 1 = 1
	}
	for _, tt := range tests {
		assert.Equalf(t, tt.want, normalizeCohort(tt.in), "normalizeCohort(%d)", tt.in)
	}
}

// fixedPrioritizer/fixedClassifier are test stubs.
type fixedPrioritizer struct{ p RequestPriority }

func (f fixedPrioritizer) AppliesTo(any) bool           { return true }
func (f fixedPrioritizer) Priority(any) RequestPriority { return f.p }

type fixedClassifier struct{ c int }

func (f fixedClassifier) AppliesTo(any) bool { return true }
func (f fixedClassifier) Cohort(any) int     { return f.c }

func newPriority(load float64, opts ...PriorityOption) *PriorityLoadShedding {
	base := []PriorityOption{
		WithCpuLoad(func() float64 { return load }),
		// fixed clock (>1s past epoch) so the threshold is computed on first call.
		WithClock(func() time.Time { return time.Unix(10, 0) }),
	}
	return NewPriorityLoadShedding(DefaultConfig(), append(base, opts...)...)
}

func TestShedLoadDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PriorityEnabled = false
	p := NewPriorityLoadShedding(cfg)
	assert.True(t, p.ShedLoad("req")) // disabled => always shed
}

func TestShedLoadNegativeLoadShedsAll(t *testing.T) {
	p := newPriority(-1)
	assert.True(t, p.ShedLoad("req"))
}

func TestShedLoadNoLoadKeepsEverything(t *testing.T) {
	// load=0 => threshold = max (640). Even the worst score (degraded, max
	// cohort) = 512 + 128 = 640, which is not > 640, so nothing is shed.
	p := newPriority(0,
		WithPrioritizers(fixedPrioritizer{PriorityDegraded}),
		WithClassifiers(fixedClassifier{MaxCohort}),
	)
	assert.False(t, p.ShedLoad("req"))
}

func TestShedLoadHighLoadShedsLowPriorityKeepsCritical(t *testing.T) {
	// load=0.9 => threshold = 640 * (1 - 0.729) = 173.44
	// NORMAL, cohort 64 => 256 + 64 = 320 > 173 => shed
	shedNormal := newPriority(0.9,
		WithPrioritizers(fixedPrioritizer{PriorityNormal}),
		WithClassifiers(fixedClassifier{64}),
	)
	assert.True(t, shedNormal.ShedLoad("req"))

	// CRITICAL, cohort 64 => 0 + 64 = 64, not > 173 => keep
	keepCritical := newPriority(0.9,
		WithPrioritizers(fixedPrioritizer{PriorityCritical}),
		WithClassifiers(fixedClassifier{64}),
	)
	assert.False(t, keepCritical.ShedLoad("req"))
}

func TestShedLoadExtremeLoadShedsHighCohortCritical(t *testing.T) {
	// load=0.99 => threshold = 640 * (1 - 0.970299) = ~19.0
	// CRITICAL, cohort 1 => 0 + 1 = 1, not > 19 => keep
	keep := newPriority(0.99,
		WithPrioritizers(fixedPrioritizer{PriorityCritical}),
		WithClassifiers(fixedClassifier{1}),
	)
	assert.False(t, keep.ShedLoad("req"))

	// CRITICAL, cohort 64 => 64 > 19 => shed
	shed := newPriority(0.99,
		WithPrioritizers(fixedPrioritizer{PriorityCritical}),
		WithClassifiers(fixedClassifier{64}),
	)
	assert.True(t, shed.ShedLoad("req"))
}

func TestShedLoadDefaultsToNormalAndMidCohort(t *testing.T) {
	// No prioritizers/classifiers => NORMAL priority, cohort 64 => score 320.
	// load=0.5 => threshold = 640 * (1 - 0.125) = 560 => 320 not > 560 => keep.
	keep := newPriority(0.5)
	assert.False(t, keep.ShedLoad("req"))

	// load=0.95 => threshold = 640 * (1 - 0.857375) = 91.28 => 320 > 91 => shed.
	shed := newPriority(0.95)
	assert.True(t, shed.ShedLoad("req"))
}
