# load-shedding

A collection of interchangeable load-shedding strategies for Go services, each in
its own package behind a small, consistent surface (a programmatic entry point
plus an `http.Handler` middleware).

| Package | Strategy | Signal | Dependency footprint |
|---------|----------|--------|----------------------|
| [`gozero`](./gozero) | Adaptive (Little's Law) | CPU + in-flight concurrency | go-zero |
| [`quarkus`](./quarkus) | TCP Vegas + priority/cohort (Quarkus port) | RTT gradient + CPU | go-zero (CPU only) |
| [`concurrencylimits`](./concurrencylimits) | Vegas / Gradient / Gradient2 + LIFO queue (Netflix port) | RTT gradient | none (stdlib) |
| [`queue`](./queue) | Fixed worker pool + bounded queue (Java threadpool) | concurrency (+ optional CPU gate) | none (stdlib) |
| [`sentinel`](./sentinel) | Sentinel BBR system adaptive protection | CPU / load / RT / concurrency / QPS | **separate module** (sentinel-golang) |

## Install

The core strategies live in the main module:

```bash
go get github.com/minhnhathoang/load-shedding
```

The `sentinel` strategy is an **isolated nested module** so its heavy dependency
tree (gopsutil, prometheus client, …) is only pulled in if you use it:

```bash
go get github.com/minhnhathoang/load-shedding/sentinel
```

## Usage

### gozero — adaptive (CPU + Little's Law)

```
import (
    "github.com/minhnhathoang/load-shedding/gozero"
    "github.com/zeromicro/go-zero/core/load"
)

s := gozero.New(load.WithCpuThreshold(900)) // 90% of CPU limit
mux.Handle("/", s.Handler(next))
```

### quarkus — Vegas concurrency limiter + priority shedding

```
import "github.com/minhnhathoang/load-shedding/quarkus"

s := quarkus.New(quarkus.DefaultConfig(),
    quarkus.WithPrioritizers(myPrioritizer),
    quarkus.WithClassifiers(myClassifier),
)
mux.Handle("/", s.Handler(next))
```

### concurrencylimits — Netflix adaptive concurrency limits

```
import "github.com/minhnhathoang/load-shedding/concurrencylimits"

// fail-fast: reject when in-flight exceeds the adaptive limit
s := concurrencylimits.NewSimpleLimiter(concurrencylimits.NewGradient2Limit())
mux.Handle("/", s.Handler(next))

// or wrap it in a bounded LIFO backlog queue (wait instead of reject)
q := concurrencylimits.NewLifoBlockingLimiter(s, 100, 200*time.Millisecond)
mux.Handle("/", q.Handler(next))
```

### queue — fixed pool + bounded queue (reject when full)

```
import "github.com/minhnhathoang/load-shedding/queue"

p := queue.New(queue.Config{Workers: 8, QueueCapacity: 64})
defer p.Stop()
mux.Handle("/", p.Handler(next)) // 503 when the queue is full
```

### sentinel — BBR system adaptive protection

```
import "github.com/minhnhathoang/load-shedding/sentinel"

s, err := sentinel.New(sentinel.Config{CpuThreshold: 0.8})
if err != nil { /* ... */ }
mux.Handle("/", s.Handler(next))
```

## Choosing a strategy

- **gozero** — general-purpose adaptive shedding tied to the CPU limit; self-calibrating, zero tuning.
- **quarkus** — when you want graded, priority-aware shedding (shed low-priority/high-cohort traffic first) driven by latency gradients.
- **concurrencylimits** — pure latency-driven adaptive concurrency (no CPU signal needed), with a choice of fail-fast or LIFO-queue admission; zero external deps.
- **queue** — when capacity is a hard, known concurrency budget and you want deterministic reject-when-full behavior.
- **sentinel** — when you're already in the Sentinel ecosystem or want multi-metric (load/RT/QPS) BBR protection.

## Attribution

- `gozero` wraps [go-zero](https://github.com/zeromicro/go-zero) `core/load`.
- `quarkus` is a Go port of the [Quarkus load-shedding extension](https://github.com/quarkusio/quarkus/tree/main/extensions/load-shedding) (itself a simplified Vegas from Netflix concurrency-limits).
- `concurrencylimits` is a Go port of [Netflix concurrency-limits](https://github.com/Netflix/concurrency-limits) (Vegas/Gradient/Gradient2 + SimpleLimiter + LifoBlockingLimiter).
- `queue` models a Java `ThreadPoolExecutor` with a bounded queue and `AbortPolicy`.
- `sentinel` wraps [sentinel-golang](https://github.com/alibaba/sentinel-golang) system adaptive protection.
