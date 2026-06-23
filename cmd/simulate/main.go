// Command simulate runs every load-shedding strategy through the same synthetic
// overload scenario and prints a comparison table.
//
//	go run ./cmd/simulate
package main

import (
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/load"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/minhnhathoang/load-shedding/gozero"
	"github.com/minhnhathoang/load-shedding/quarkus"
	"github.com/minhnhathoang/load-shedding/queue"
	"github.com/minhnhathoang/load-shedding/simulate"
)

func main() {
	logx.Disable()

	sc := simulate.Scenario{
		Duration:    2 * time.Second,
		RPS:         800,
		ServiceTime: 20 * time.Millisecond, // ~50 rps capacity per worker
	}

	fmt.Printf("scenario: %ds @ %d rps, service %s\n\n",
		int(sc.Duration.Seconds()), sc.RPS, sc.ServiceTime)

	results := []simulate.Result{
		runQueueLength(sc),
		runQueueTimeout(sc),
		runCodel(sc, false),
		runCodel(sc, true),
		runGozero(sc),
		runQuarkus(sc),
	}

	fmt.Print(simulate.FormatTable(results))
}

func runQueueLength(sc simulate.Scenario) simulate.Result {
	p := queue.New(queue.Config{Workers: 4, QueueCapacity: 16})
	defer p.Stop()
	return simulate.Run("queue-length", p, sc)
}

func runQueueTimeout(sc simulate.Scenario) simulate.Result {
	p := queue.New(queue.Config{Workers: 4, QueueCapacity: 16, MaxWait: 50 * time.Millisecond})
	defer p.Stop()
	return simulate.Run("queue-timeout", p, sc)
}

func runCodel(sc simulate.Scenario, lifo bool) simulate.Result {
	p := queue.NewCodel(queue.CodelConfig{
		Workers:      4,
		Capacity:     10000,
		Target:       5 * time.Millisecond,
		Interval:     100 * time.Millisecond,
		AdaptiveLIFO: lifo,
	})
	defer p.Stop()
	name := "queue-codel"
	if lifo {
		name = "queue-adaptive-lifo"
	}
	return simulate.Run(name, p, sc)
}

func runGozero(sc simulate.Scenario) simulate.Result {
	return simulate.Run("gozero", gozero.New(load.WithCpuThreshold(900)), sc)
}

func runQuarkus(sc simulate.Scenario) simulate.Result {
	return simulate.Run("quarkus", quarkus.New(quarkus.DefaultConfig()), sc)
}
