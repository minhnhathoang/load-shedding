package simulate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/minhnhathoang/load-shedding/gozero"
	"github.com/minhnhathoang/load-shedding/quarkus"
	"github.com/minhnhathoang/load-shedding/queue"
	"github.com/zeromicro/go-zero/core/load"
)

func init() { logx.Disable() }

// overloadScenario issues far more work than the service can absorb in the
// window, so capacity-bounded shedders must shed.
func overloadScenario() Scenario {
	return Scenario{
		Duration:    300 * time.Millisecond,
		RPS:         500,
		ServiceTime: 20 * time.Millisecond,
	}
}

func assertConserved(t *testing.T, r Result) {
	t.Helper()
	assert.Equal(t, r.Sent, r.Served+r.Shed+r.Other,
		"%s: every request must be served, shed, or other", r.Name)
}

func TestSimulateQueueLengthSheds(t *testing.T) {
	p := queue.New(queue.Config{Workers: 2, QueueCapacity: 4})
	defer p.Stop()

	r := Run("queue-length", p, overloadScenario())
	t.Log("\n" + FormatTable([]Result{r}))

	assertConserved(t, r)
	assert.Positive(t, r.Served, "some requests must get through")
	assert.Positive(t, r.Shed, "a small bounded queue under overload must shed")
}

func TestSimulateQueueTimeoutSheds(t *testing.T) {
	p := queue.New(queue.Config{Workers: 2, QueueCapacity: 4, MaxWait: 10 * time.Millisecond})
	defer p.Stop()

	r := Run("queue-timeout", p, overloadScenario())
	t.Log("\n" + FormatTable([]Result{r}))

	assertConserved(t, r)
	assert.Positive(t, r.Shed)
}

func TestSimulateCodelSheds(t *testing.T) {
	p := queue.NewCodel(queue.CodelConfig{
		Workers:  2,
		Capacity: 1000,
		Target:   time.Millisecond,
		Interval: 20 * time.Millisecond,
	})
	defer p.Stop()

	r := Run("queue-codel", p, overloadScenario())
	t.Log("\n" + FormatTable([]Result{r}))

	assertConserved(t, r)
	assert.Positive(t, r.Served)
	assert.Positive(t, r.Shed, "sustained overload must trigger CoDel drops")
}

func TestSimulateAdaptiveLIFO(t *testing.T) {
	p := queue.NewCodel(queue.CodelConfig{
		Workers:      2,
		Capacity:     1000,
		Target:       time.Millisecond,
		Interval:     20 * time.Millisecond,
		AdaptiveLIFO: true,
	})
	defer p.Stop()

	r := Run("queue-adaptive-lifo", p, overloadScenario())
	t.Log("\n" + FormatTable([]Result{r}))

	assertConserved(t, r)
	assert.Positive(t, r.Served)
}

func TestSimulateCpuGateShedsWhenHigh(t *testing.T) {
	// A CPU gate fed a saturated reading must reject (almost) everything up
	// front, before requests reach the queue.
	gate := queue.NewCPUThresholdGateFunc(900, func() int64 { return 1000 })
	p := queue.New(queue.Config{Workers: 2, QueueCapacity: 8, Gate: gate})
	defer p.Stop()

	r := Run("cpu-gate-high", p, Scenario{Duration: 200 * time.Millisecond, RPS: 200, ServiceTime: time.Millisecond})
	t.Log("\n" + FormatTable([]Result{r}))

	assertConserved(t, r)
	assert.Zero(t, r.Served, "saturated CPU gate must admit nothing")
	assert.Equal(t, r.Sent, r.Shed)
}

func TestSimulateCpuGateOffAdmits(t *testing.T) {
	// Same saturated CPU, but the gate is toggled off → everything is admitted.
	gate := queue.NewCPUThresholdGateFunc(900, func() int64 { return 1000 })
	p := queue.New(queue.Config{Workers: 4, QueueCapacity: 64, Gate: gate})
	defer p.Stop()
	p.SetGateEnabled(false)

	r := Run("cpu-gate-off", p, Scenario{Duration: 200 * time.Millisecond, RPS: 200, ServiceTime: time.Millisecond})
	t.Log("\n" + FormatTable([]Result{r}))

	assertConserved(t, r)
	assert.Positive(t, r.Served, "a disabled gate admits despite high CPU")
}

func TestSimulateQuarkusConserves(t *testing.T) {
	s := quarkus.New(quarkus.DefaultConfig())
	r := Run("quarkus", s, overloadScenario())
	t.Log("\n" + FormatTable([]Result{r}))

	assertConserved(t, r)
	assert.Positive(t, r.Served)
}

func TestSimulateGozeroConserves(t *testing.T) {
	// CPU won't reliably hit the threshold in a unit test, so we only assert
	// conservation and no hang/panic — the shedder must remain pluggable.
	s := gozero.New(load.WithCpuThreshold(900))
	r := Run("gozero", s, overloadScenario())
	t.Log("\n" + FormatTable([]Result{r}))

	assertConserved(t, r)
	assert.Positive(t, r.Served)
}
