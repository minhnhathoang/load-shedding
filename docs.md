# Load Testing

# Load Shedding

## go-zero

> go-zero uses an adaptive load shedder based on CPU utilization and in-flight request count. When the system is overloaded, new requests are rejected to protect existing in-flight work.

https://github.com/zeromicro/go-zero
>

### How it works

The shedder combines two signals to decide whether to accept a request:

- **CPU usage** - sampled every 250 ms via `/proc/stat`. Shedding activates once CPU exceeds the configured threshold (default 90%).
- **Pass rate** - the rolling ratio of completed requests to total attempted requests in the last sliding window. If the pass rate drops below a calculated floor, new arrivals are shed.

This double-gate ensures you never shed when the CPU is healthy, and always shed when it is saturated, regardless of the request volume.

```
  request
     │
     ▼
  Allow()
     │
     ▼
  Gate 1: pressure?               ── no ──┐
  (CPU >= threshold OR stillHot)          │
     │ yes                                │
     ▼                                    ▼
  Gate 2: over capacity?          ── no ──▶ admit (++flying, return promise)
  (avgFlying AND flying > limit)           │
     │ yes                                 ▼ handle
     ▼                                   --flying
  return 503
```

### Technical Implementation

[https://github.com/zeromicro/go-zero/blob/1540bdc/core/load/adaptiveshedder.go](https://github.com/zeromicro/go-zero/blob/1540bdc/core/load/adaptiveshedder.go)

#### **Rejection Decision Logic**

```
func shouldDrop() bool {
    if systemOverloaded() || stillHot() {   // Check 1
        if highThru() {                     // Check 2
            // log + stat
            return true
        }
    }
    return false
}
```

**Gate 1: CPU pressure with hysteresis**

- `systemOverloaded()` : Check `cpuUsage() > cpuThreshold` and records `overloadTime` when true

    ```
    // Default cpuThreshold = 900 (90%)
    
    // Refresh CPU usage every 250ms 
    // (cgroup-aware, % of CPU limit not CPU request, millicpu 0-1000)
    curCpuUsage = internal.RefreshCpu() 
    
    // CPU usage is smoothed with EWMA (Exponentially Weighted Moving Average)
    cpuUsage = preCpuUsage * beta + curCpuUsage * (1 - beta) // beta = 0.95
    ```

- `stillHot()` : After dropping a request, shedder enters a `1s` cool-off via `droppedRecently`. CPU can recover immediately, but shedding pressure remains active until the cool-off expires.

**Gate 2: In-flight request count**

- `overloadFactor()` : Linear interpolation of CPU headroom remaining.

    ```
    func overloadFactor() float64 {
    	// cpuThreshold must be less than cpuMax (cpuMax = 1000)
    	factor := (cpuMax - cpuUsage()) / (cpuMax - cpuThreshold)
    	
    	// at least accept 10% of acceptable requests, even cpu is highly overloaded.
    	// (overloadFactorLowerBound = 0.1)
    	return mathx.Between(factor, overloadFactorLowerBound, 1)
    }
    ```

- `maxFlight()` : Maximum in-flight requests, estimated via [Little's Law](https://en.wikipedia.org/wiki/Little%27s_law). It is the best-case capacity, derived from **peak throughput** and **minimum latency**. But at the moment we decide whether to shed, the service is under stress (`systemOverloaded()` or `stillHot()`),  so its real safe concurrency right now is **lower** than that optimistic `maxFlight`.

    ```
    func maxFlight() float64 {
    	// Little's Law: allowedFlying = maxQPS * minRt_in_seconds
    	// 
    	// bucketsPerSecond = defaultBuckets / defaultWindows = 50 / 5s = 10
    	// maxQPS = maxPass * bucketsPerSecond
    	// minRT  = min avg response time in milliseconds
    	// allowedFlying = maxPass * bucketsPerSecond * minRT / millisecondsPerSecond
    	//               = maxPass * minRT * windowScale
    	// windowScale = bucketsPerSecond / 1e3 = 10 / 1e3 = 0.01
    	maxFlight := maxPass() * minRt() * windowScale
    	return mathx.AtLeast(maxFlight, 1)
    }
    ```

- `highThru()` : Requires the EWMA (`avgFlying`) **and** the **instantaneous** count (`flying`) to exceed `limit`. This avoids dropping on a brief spike that the service can absorb, while still reacting to sustained overload.

    ```
    func addFlying(delta int64) {
    	flying := atomic.AddInt64(&flying, delta)
    	// update avgFlying when the request is finished.
    	// this strategy makes avgFlying have a little bit of lag against flying, and smoother.
    	// when the flying requests increase rapidly, avgFlying increase slower, accept more requests.
    	// when the flying requests drop rapidly, avgFlying drop slower, accept fewer requests.
    	// it makes the service to serve as many requests as possible.
    	// 
    	// moving average hyperparameter beta for calculating requests on the fly
    	// flyingBeta = 0.9
    	if delta < 0 {
    		avgFlyingLock.Lock()
    		avgFlying = avgFlying*flyingBeta + flying*(1-flyingBeta)
    		avgFlyingLock.Unlock()
    	}
    }
    
    func highThru() bool {
        limit := maxFlight() * overloadFactor()
        return avgFlying > limit && flying > limit
    }
    ```


## sentinel-golang

> Sentinel sheds inbound load using **system adaptive protection**. Each rule watches one system metric (CPU usage, load1, average RT, concurrency, or inbound QPS). With the **BBR** strategy, crossing the metric trigger only *arms* shedding; the actual decision comes from a Little's-Law concurrency check, so it won't shed while CPU is high but the service is still keeping up.

https://github.com/alibaba/sentinel-golang
>

### How it works

Two families of rule:

- **Hard threshold** (`InboundQPS`, `Concurrency`, `AvgRT`): block as soon as the metric reaches `TriggerCount`. No adaptivity.
- **Adaptive / BBR** (`CpuUsage`, `Load`): the metric crossing `TriggerCount` arms shedding, but the block is decided by a BBR concurrency check — the same Little's Law idea as go-zero's `maxFlight`:

```
estimatedCapacity = maxCompleteQPS * minRT_seconds
block when  currentConcurrency > estimatedCapacity   (and concurrency > 1)
```

```
  Inbound request
       │
       ▼
  for each system Rule:
       │
       ▼
  metric > TriggerCount ?  ─── no ──▶ pass
       │ yes
       ▼
  Strategy == BBR ?        ─── no ──▶ BLOCK   (hard threshold)
       │ yes
       ▼
  concurrency > maxCompleteQPS*minRT ?  ── no ──▶ pass
       │ yes
       ▼
  BLOCK (ResourceExhausted / 503)
```

### Technical Implementation

[core/system/slot.go](https://github.com/alibaba/sentinel-golang/blob/master/core/system/slot.go)

```
// CpuUsage rule (Load is identical):
c := system_metric.CurrentCpuUsage()
if c > threshold {                         // metric armed
    if rule.Strategy != BBR || !checkBbrSimple() {
        return false                       // BLOCK
    }
}
return true                                // pass

func checkBbrSimple() bool {
    concurrency := InboundNode().CurrentConcurrency()
    minRt       := InboundNode().MinRT()
    maxComplete := InboundNode().GetMaxAvg(Complete)    // max completed QPS
    // Little's Law: capacity = maxCompleteQPS * minRT(seconds)
    if concurrency > 1 && float64(concurrency) > maxComplete*minRt/1000.0 {
        return false                       // over capacity → block
    }
    return true
}
```

- CPU usage / load1 are sampled by a background collector (`system_metric`).
- Rules are **process-global** and apply to every inbound Sentinel entry, not per-resource.

## concurrency-limits

> Netflix's library adaptively *discovers* a service's concurrency limit using algorithms from TCP congestion control (Vegas, Gradient, AIMD). It watches request latency and moves an in-flight limit up or down by the latency gradient — there is no fixed capacity to configure.

https://github.com/Netflix/concurrency-limits
>

### How it works

The library ships three limit algorithms — **Vegas**, **Gradient**, **Gradient2** — all of which:

- keep `currentLimit` and `inflight`, and reject when `inflight >= currentLimit`;
- on each completion measure RTT and adjust the limit from a **latency gradient**;
- differ only in **which RTT they compare the current sample against**.

Vegas, Gradient and Gradient2 can all be read as one update per completion:

```
newLimit = limit * gradient + queueSize
```

- `gradient ∈ [0.5, 1.0]` — latency health. ≈1.0 ⇒ no congestion (grow); <1.0 ⇒ latency rising (shrink).
- `queueSize` — burst headroom; the "+room" that lets the limit climb while healthy.
- It stops growing at the fixed point `limit = queueSize / (1 - gradient)`.

```
  request → inflight >= limit ? ─── yes ──▶ reject
       │ no
       ▼ admit, inflight++
     handle, measure RTT
       │ on complete:
       ▼
  adjust limit from the RTT gradient (Vegas / Gradient / Gradient2)
  clamp [minLimit, maxLimit]
```

### Vegas

The textbook version. Reference RTT is `minRTT` (the best seen = an uncongested baseline); it estimates a **queue length** from the RTT ratio and applies a **log-scaled tiered AIMD**:

```
func update(rtt, inflight):
    if probeFactor*probeJitter*limit <= ++probeCount:   // periodic re-probe
        resetProbe(); minRTT = rtt; return
    if rtt < minRTT: minRTT = rtt; return
    if 2*inflight < limit: return                        // only react when loaded
    queue := ceil(limit * (1 - minRTT/rtt))
    g := log10(limit);  α := 3g;  β := 6g                // alphaFactor=3, betaFactor=6
    switch:
      queue <= g  → limit += 6g    (grow fast, queue tiny)
      queue <  α  → limit += g     (grow slow)
      queue >  β  → limit -= g     (shrink)
      else        → unchanged
    limit = clamp(limit, 1, maxLimit)
```

- Periodically **probes** by resetting `minRTT` so a drifting baseline (caches warming, GC) gets re-measured.

#### Ported by the `quarkus` package

The [`quarkus`](./quarkus) package ports this Vegas variant as `OverloadDetector`. Quarkus's port is a **simplified** Netflix `VegasLimit`, not a line-for-line copy — the core limit search is identical, but it drops two things and tweaks the threshold:

| | Netflix `VegasLimit` | Quarkus `OverloadDetector` (= the `quarkus` Go port) |
|---|---|---|
| `didDrop` handling | yes → multiplicative backoff on a dropped request | **removed** (detector gets no drop signal) |
| smoothing | `(1-s)·limit + s·newLimit` (configurable) | **none** |
| fast-grow threshold | `log10(limit)` | `log10(limit) + 1` (precomputed `LOG10_PLUS_1` table) |
| grow/shrink step | `±log10(limit)` | `±(log10(limit)+1)` |
| default initial limit | 20 | 100 |

Identical in both: `queue = ceil(limit·(1−minRTT/RTT))`, `α=3·log10`, `β=6·log10`, the tiered grow/shrink, the `2·inflight < limit` guard, the probe (`probeFactor/probeMultiplier = 30`), and the `[1, maxLimit]` clamp. The full Netflix VegasLimit (with drop backoff + smoothing) and Gradient/Gradient2 are **not** ported.

### Gradient — reference = absolute minimum (no-load) RTT

```
rttNoLoad = running MINIMUM of all observed RTT          // MinimumMeasurement
gradient  = clamp(rttTolerance * rttNoLoad / rtt, 0.5, 1.0)   // tolerance = 2.0

if didDrop:               newLimit = limit * backoffRatio      // 0.9, multiplicative cut
elif inflight < limit/2:  return limit                          // too idle to judge
else:                     newLimit = limit * gradient + queueSize

if newLimit < limit:      newLimit = limit*(1-smoothing) + smoothing*newLimit  // smooth ONLY on decrease
newLimit = clamp(newLimit, queueSize, maxLimit)
```

- `gradient = 1` while `rtt ≤ 2×rttNoLoad` (tolerance 2.0) → it keeps growing until latency exceeds 2× the no-load baseline, then shrinks.
- `queueSize = max(4, sqrt(limit))` (default) — headroom scales with the limit.
- **Probing:** every ~`probeInterval` (1000) samples it *resets* `limit = queueSize` and clears `rttNoLoad` to re-measure true no-load latency — because an absolute running minimum can get stuck stale and starve the limit.
- Defaults: init 50, min 1, max 1000, smoothing 0.2.
- **Weakness:** the absolute minimum is fragile — one freak-fast sample pins `rttNoLoad` low forever, and a legitimately shifted baseline only recovers via the periodic probe (a sawtooth).

### Gradient2 — reference = long-term moving average

```
shortRtt = rtt                                      // this sample
longRtt  = EWMA of rtt over ~longWindow samples     // ExpAvgMeasurement(600)

if longRtt/shortRtt > 2:  longRtt *= 0.95           // got much faster → let baseline fall faster
if inflight < limit/2:    return limit

gradient = clamp(rttTolerance * longRtt / shortRtt, 0.5, 1.0)   // tolerance = 1.5
newLimit = limit * gradient + queueSize
newLimit = limit*(1-smoothing) + smoothing*newLimit             // ALWAYS smoothed
newLimit = clamp(newLimit, minLimit, maxLimit)
```

- Compares **recent latency to a slowly-adapting "normal"** instead of an all-time best. A rising `shortRtt` against a stable `longRtt` is precisely the signal of *emerging* congestion.
- The `longRtt *= 0.95` rule replaces Gradient's probe: when things get much faster it decays the baseline downward continuously — no sawtooth reset.
- No `didDrop` branch — relies purely on the latency gradient.
- `queueSize = 4` constant (default). Defaults: init/min 20, max 200, tolerance 1.5, longWindow 600.
- **Why it's the default choice:** robust to baseline drift (no probe artifacts), smoother (damped both directions), and short-vs-long detects a latency *trend* rather than deviation from an unrepresentative minimum.

### At a glance

| | Vegas | Gradient | Gradient2 |
|---|---|---|---|
| Reference RTT | min RTT (probe-reset) | absolute running min | long-window EWMA |
| Congestion signal | `queue = limit·(1−minRTT/RTT)` | `tol·minRTT/RTT` | `tol·longRTT/shortRTT` |
| Baseline-drift fix | periodic probe | periodic probe | continuous `×0.95` decay |
| Drop handling | — | `×0.9` backoff | — |
| Smoothing | configurable | decrease only | both directions |
| Tolerance (default) | — | 2.0 | 1.5 |
| init / max (default) | 20 / 1000 | 50 / 1000 | 20 / 200 |

The `quarkus` package ports the **Vegas** variant (simplified — see above).

## quarkus

> The Quarkus load-shedding extension separates *whether* to shed from *what* to shed. A Vegas **OverloadDetector** (from concurrency-limits) answers the first; a **priority + cohort** scorer answers the second, shedding the least important traffic first and progressively more as CPU climbs.

https://github.com/quarkusio/quarkus/tree/main/extensions/load-shedding
>

### How it works

**Stage 1 — OverloadDetector (Vegas):** `isOverloaded = inflight >= adaptiveLimit` (adaptive limit per concurrency-limits above).

**Stage 2 — PriorityLoadShedding** (consulted only when overloaded):

- Each request scores `score = priority.baseline + cohort`, where priority ∈ {CRITICAL…DEGRADED} (baseline = ordinal × 128) and cohort ∈ [1,128] (e.g. a hashed user id).
- The threshold falls **cubically** with CPU: `threshold = maxScore * (1 - cpuLoad³)`.
- Shed when `score > threshold`. Low CPU ⇒ high threshold ⇒ nothing shed; high CPU ⇒ threshold collapses ⇒ shed degraded/background first, then normal, eventually all but critical.

```
  request
     │
     ▼
  inflight >= vegasLimit ? ─── no ──▶ admit
     │ yes (overloaded)
     ▼
  threshold = maxScore * (1 - cpuLoad³)
  score     = priority.baseline + cohort
     │
     ▼
  score > threshold ? ─── no ──▶ admit
     │ yes
     ▼
  SHED (503)
```

### Technical Implementation

[OverloadDetector.java](https://github.com/quarkusio/quarkus/blob/main/extensions/load-shedding/runtime/src/main/java/io/quarkus/load/shedding/runtime/OverloadDetector.java) · [PriorityLoadShedding.java](https://github.com/quarkusio/quarkus/blob/main/extensions/load-shedding/runtime/src/main/java/io/quarkus/load/shedding/runtime/PriorityLoadShedding.java)

```
// when overloaded:
threshold = max * (1 - load*load*load)        // max = 5 priorities * 128 cohorts = 640
priority  = first matching prioritizer        (default NORMAL)
cohort    = first matching classifier         (default 64), normalized into [1,128]
return priority.cohortBaseline() + cohort > threshold   // true → shed
```

The detector's adaptive `limit` uses the Vegas `update` shown under concurrency-limits.

## queue

> A fixed worker pool with a bounded queue — the Go analogue of a Java `ThreadPoolExecutor` + bounded `BlockingQueue`. `Workers` goroutines pull tasks from a queue; the **shedding policy decides what happens when the queue can't keep up**. There are four common policies, from a simple length cap to latency-aware dropping.

### How it works

All policies share the same skeleton — workers pulling from a queue — and differ only in **when a task is rejected (at submit) or dropped (at dequeue)**:

```
  request ──▶ [ admission policy ] ──▶ queue ──▶ worker runs
                     │                   │
              reject at submit     drop at dequeue
              (length / wait)      (sojourn time: CoDel / LIFO)
```

| Policy | Sheds on | Bounds | Adaptive | Used by |
|---|---|---|---|---|
| **1. Length** | queue full | concurrency budget | no | `ThreadPoolExecutor` AbortPolicy · this `queue` pkg |
| **2. Enqueue timeout** | time spent *waiting to enter* | admission block time | no | Resilience4j `ThreadPoolBulkhead.maxWaitDuration` |
| **3. CoDel** | time spent *in* the queue (sojourn) | tail latency | yes | folly (Facebook) · Envoy |
| **4. Adaptive LIFO + CoDel** | sojourn + serves newest first | tail latency + freshness | yes | Facebook Wangle/folly |

### 1. Length-based (reject when full)

Hard budget of `Workers + QueueCapacity` in-flight tasks; reject the instant it's exhausted.

```
select {
case sem <- token:          // acquire in-flight slot (running + waiting)
    tasks <- task           // cap(tasks) == cap(sem) ⇒ never blocks
    return accepted
default:
    return ErrQueueFull     // budget exhausted → shed
}
// worker: run(task); <-sem  // release slot on completion
```

- **Pro:** deterministic, trivial, zero latency cost.
- **Con:** queue *length* doesn't bound *latency* — a 64-slot queue behind a slow service can still mean seconds of wait. This is what the `queue` package implements today.

### 2. Enqueue-wait timeout

Block the submitter for up to `maxWait` to get into the queue, then reject:

```
if !queue.offer(task, maxWait) {   // Java: workQueue.offer(t, timeout, unit)
    return ErrRejected
}
```

- Bounds how long *admission* blocks — useful to absorb micro-bursts without unbounded caller blocking.
- **Does not** bound time spent *in* the queue once admitted. (Resilience4j `ThreadPoolBulkhead`.)

### 3. CoDel — Controlled Delay (sojourn-time shedding)

Stamp each task on enqueue; when a worker dequeues it, look at its **sojourn time** `now - enqueuedAt`. Keep a rolling **minimum** sojourn over an `interval`; if it stays above `target`, enter dropping mode and drop stale tasks at an increasing rate:

```
target   = 5ms      // acceptable standing queue delay
interval = 100ms    // window to confirm a *standing* (not transient) queue

on dequeue(task):
    sojourn = now - task.enqueuedAt
    if sojourn < target or queueEmpty:
        dropping = false; firstAboveTime = 0          // queue drained → stop
    else:
        if firstAboveTime == 0:
            firstAboveTime = now + interval           // start the timer
        elif now >= firstAboveTime:
            dropping = true
    if dropping and now >= dropNext:
        drop(task); count++
        dropNext = now + interval/sqrt(count)         // control law: drop faster the longer it persists
        return                                        // fail fast, pick next task
    run(task)
```

- Sheds on **queue time, not length**, so it directly bounds tail latency and tolerates short bursts (a transient queue under `interval` is never dropped).
- Drops *expired* work (clients likely already gave up) instead of running dead work.

### 4. Adaptive LIFO + CoDel

Same CoDel trigger, but flip the queue discipline under load:

```
normal load   → FIFO   (fair, preserves order)
CoDel dropping → LIFO   (serve newest first)
```

- Under a standing queue, FIFO serves the **oldest** (most likely already-expired) request first → everyone is slow. **LIFO** serves the **freshest** request first, so recent requests meet their deadline while the stale tail is drained/dropped by CoDel.
- Facebook's production pattern (Wangle/folly Thrift servers).

### CPU gate (reject high CPU, else enqueue)

Both pools accept an optional admission **`Gate`** placed in front of the queue.
A request the gate rejects is shed with `ErrOverloaded` **before it reserves a
slot or enters the queue**; otherwise it is enqueued as usual:

```
  request
     │
     ▼
  Gate.Allow() ?  ── reject ──▶ ErrOverloaded (503)
     │ admit
     ▼
  enqueue (length / timeout / CoDel)
```

Two gate implementations are provided (or supply your own `Gate`):

- **`NewCPUThresholdGate(threshold)`** — static: reject when CPU usage (millicpu,
  0-1000, from go-zero's cgroup-aware `stat.CpuUsage`) ≥ threshold. Shorthand:
  set `Config.CpuThreshold` and a static gate is installed automatically.
- **`NewGozeroGate(load.WithCpuThreshold(...))`** — uses go-zero's **adaptive
  shedder** (CPU + Little's Law + cool-off hysteresis); it learns capacity from
  live traffic and only sheds when CPU is saturated *and* concurrency exceeds the
  learned limit. The gate feeds Pass/Fail back to the shedder on completion.

The gate is **toggleable at runtime** — `pool.SetGateEnabled(false)` (or
`pool.Gate().SetEnabled(false)`) turns it off; a disabled gate admits everything.

### Note for this repo

The `queue` package implements all four policies plus the CPU gate:

- **`Pool`** — policy 1 (length, `ErrQueueFull`) and policy 2 (`MaxWait` enqueue timeout, `ErrQueueTimeout`).
- **`CodelPool`** — policy 3 (CoDel sojourn-time dropping, `ErrDropped`) and policy 4 (`AdaptiveLIFO`).
- Both — optional admission `Gate` (`ErrOverloaded`): static CPU threshold or go-zero adaptive shedder, toggleable on/off at runtime.

Prefer **sojourn-time (CoDel)** when you care about latency SLOs rather than a raw concurrency cap.

# Autoscaling

