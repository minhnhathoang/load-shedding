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

Load shedding and autoscaling (HPA) solve the **same problem at different time
scales**: shedding protects a pod *now* (microseconds), autoscaling adds capacity
*later* (tens of seconds to minutes). Shedding exists to survive the **scale-up
lag** — metric window + scrape interval + pod scheduling + container start +
readiness — during which load already exceeds capacity but new pods aren't ready.

## The core tension: shedding can blind the autoscaler

If the shedder caps the very signal the HPA scales on, the HPA never fires.

> **CPU-based shedding + CPU-based HPA can deadlock.** A CPU shedder rejects
> requests to hold CPU at its threshold. If the HPA's target CPU is at or above
> that threshold, CPU never climbs past it → HPA never scales up → the shedder
> sheds forever instead of getting more pods.

The same blindness appears with **queues**: a queue absorbs a burst by making
requests *wait*, not compute, so **CPU stays low while latency explodes**. A
CPU-target HPA sees a healthy-looking pod and does nothing.

## Two rules that fix it

1. **Order the thresholds: `HPA target` < `shed threshold`.** Leave a band where
   load raises the metric enough to trigger the HPA *before* shedding engages.
   Example: HPA target CPU 65%, shed at 90%. The HPA reacts first; shedding is the
   last-resort safety net for the lag.
2. **Scale on a signal the shedder doesn't suppress.** The truest "need capacity"
   signal is the **shed/drop rate** itself, or **RPS / queue depth / p99 latency**
   — emit these and scale on them (custom metrics / KEDA), not just CPU.

## Per-strategy interaction

| Strategy | What it caps | Safe HPA signal | Watch-out |
|---|---|---|---|
| **gozero** (CPU + Little's Law) | CPU near threshold | CPU **target well below** shed threshold; or RPS | If HPA target ≈ shed threshold, CPU is pinned and the HPA stalls |
| **quarkus** (Vegas + priority) | concurrency via latency gradient | RPS / inflight-limit / reject rate | Keeps the box from saturating → CPU-target HPA under-scales |
| **queue (length/timeout)** | in-flight concurrency budget | **queue depth / reject rate / RPS** | Waiting ≠ CPU → CPU-target HPA is blind to backpressure |
| **queue (CoDel/LIFO)** | queue *sojourn time* | **drop rate / p99 latency** | Best autoscaling signal of all — drop rate rises exactly when capacity is short |
| **sentinel** (BBR system) | CPU/load/RT/QPS/concurrency | the same metric the rule watches | Global rules; scale on the BBR-triggering metric |
| **CPU gate** (this repo) | CPU at the gate threshold | CPU target **below** gate threshold; or gate `off` + scale on queue | Same pin-the-CPU trap as gozero if thresholds collide |

## Practical guidance

- **Pick the gate vs HPA roles deliberately.** Treat the HPA as the *primary*
  capacity controller and the shedder as the *overload backstop*. Their thresholds
  must not overlap (rule 1).
- **For queue/latency strategies, scale on latency or queue/drop rate**, never CPU
  alone. CoDel's drop rate is an almost-ideal scaling trigger.
- **Size `minReplicas` + shed budget for the lag.** A spike must be survivable on
  the current replicas (with shedding) for the whole time-to-Ready of new pods.
- **Align cooldowns.** Match the shedder's hysteresis (e.g. go-zero's 1s cool-off,
  CoDel's interval) against the HPA stabilization window so the two controllers
  don't oscillate / churn replicas.
- **Scale-down is an overload event too.** Removing pods raises per-pod load; the
  shedder must engage to protect the survivors during the transient.
- **The `off` toggle is an autoscaling tool.** Disabling the CPU gate
  (`SetGateEnabled(false)`) lets CPU rise so a CPU-target HPA can see real demand —
  useful when you want the HPA, not the shedder, to absorb a known ramp.

This is exercised by **Load Testing → Scenario 6 (Autoscaling interaction)** below.

## Building triggers in practice (Netflix, KEDA, Envoy)

What real systems actually do when load shedding and autoscaling coexist.

### Netflix

- Scales on **RPS, not CPU** — RPS is a cleaner, earlier signal for the scaling
  action than CPU (which their shedder/concurrency-limiter suppresses).
- Tracks **SPS (Starts Per Second)** as the demand metric, and uses an **"RPS
  Hammer" step-scaling** policy to add a chunk of capacity *fast* on a spike.
- Uses **5-second high-resolution metrics** to detect spikes sooner than the
  default ~60s.
- **Prioritized load shedding only begins after the target CPU is reached** —
  the shed point sits *above* the autoscale target (rule 1), then progressively
  drops less-critical traffic while autoscaling catches up.
- Per-node **adaptive concurrency limits** (Little's Law, `Limit = RPS × latency`)
  are the in-process backstop.

### KEDA

- Event/metric-driven scaling on **queue depth** (RabbitMQ/Redis/SQS), **RPS or
  concurrent connections** (HTTP add-on), **latency**, or any **Prometheus /
  Datadog** custom metric — "scale on what matters, not just CPU" — plus
  scale-to-zero.
- **Caveat:** Prometheus-scraped metrics are typically **30–90s stale**, so for a
  15–30s spike the HPA reacts *after* it's over. This is exactly why the
  in-process shedder must cover the lag; mitigate with higher-resolution / push
  (OTel) metrics and step scaling.

### Envoy / mesh

- Adaptive concurrency (gradient) sheds at the proxy; the same min-RTT /
  concurrency signals can drive scaling.

### Recommended trigger stack (three layers)

1. **Proactive — scale on demand** (RPS or in-flight concurrency) *before*
   saturation. KEDA HTTP add-on or Prometheus scaler.
2. **Reactive — scale on the shed signal.** The rate of `503/429` or the
   shedder's reject/drop counter is the *truest* "out of capacity now" trigger;
   wire it as a step-scaler for speed.
3. **Instant — the in-process shedder** protects each pod during the unavoidable
   metric + scheduling lag.

### Concrete KEDA triggers

Reactive, on **shed rate** (router-service already emits a reject counter via
`metrics.CountLoadShedderReject`):

```yaml
triggers:
- type: prometheus
  metadata:
    serverAddress: http://prometheus:9090
    query: sum(rate(router_load_shedder_reject_total[1m]))
    threshold: "1"        # >1 sustained reject/s ⇒ add capacity
```

Proactive, **Netflix-style RPS** (note the 30s window for faster reaction):

```yaml
- type: prometheus
  metadata:
    query: sum(rate(http_requests_total{job="router"}[30s]))
    threshold: "500"      # ~500 rps per pod
```

Backpressure, **queue depth / CoDel drop rate** (export `Stats()`):

```yaml
- type: prometheus
  metadata:
    query: avg(queue_depth{job="router"})     # or rate(codel_dropped_total[1m])
    threshold: "32"
```

### What to export from these shedders

`Pool.Stats()` / `CodelPool.Stats()` already expose `Accepted / Rejected /
Dropped / Queued / Active`. Publish them as Prometheus metrics and scale on:

| Metric | Trigger role |
|---|---|
| `rate(rejected) + rate(dropped)` | reactive shed-rate (layer 2) |
| `Queued` gauge | backpressure for queue/CoDel strategies |
| request RPS | proactive demand (layer 1) |

Always combine with **rule 1** (autoscale target below the shed threshold) so the
scaler fires *before* the shedder — the shedder then only has to cover the lag.

Sources: Netflix — *Performance Under Load* and *Service-Level Prioritized Load
Shedding* (netflixtechblog); KEDA docs (keda.sh) and the Prometheus-staleness
caveat (kedify.io); Netflix `concurrency-limits` (github.com/Netflix/concurrency-limits).

# Load Testing

## Goal

Validate that, under overload, each shedder **protects the service** (bounded tail latency, bounded CPU, no death spiral) while **maximizing goodput** (admitted-and-successful rps), and that it **recovers quickly** once load normalizes. Compare the four strategies (go-zero, sentinel, quarkus, queue) under identical conditions.

## System under test

- The target service behind one shedder middleware, deployed with an HPA.
- A controllable downstream dependency (so we can inject latency independently of CPU).
- A load generator (k6 / vegeta / wrk2 — prefer **wrk2/k6 for open-model, constant-arrival-rate** load; closed-model tools hide overload because clients self-throttle).

## Inputs (test matrix)

| Dimension | Values |
|---|---|
| Shedder | go-zero · sentinel · quarkus · queue |
| Min replicas | 1 · 3 |
| HPA target CPU % | 75 · 85 |
| Shed threshold (% of CPU/limit) | 85 · 95 · 105 |
| Traffic source | single `/route` · prod sample · generated mix |
| Baseline rps | ~50% of measured single-replica capacity |
| Spike rps | 3× · 6× · 10× baseline |

> Calibrate "capacity" first: ramp a single replica with **no** shedder to find the knee (rps where p99 latency or CPU breaks SLO). All multipliers are relative to that.

## Metrics to capture

| Metric | Why |
|---|---|
| **Goodput** (successful rps, excl. 503-shed) | the number to maximize |
| **Shed rate** (503/s) | how much is rejected |
| **Error rate** (non-shed 5xx, timeouts) | death-spiral / leakage signal |
| **Admitted latency** p50 / p95 / p99 | tail protection (shed requests excluded) |
| **CPU %** (vs limit) | did the shedder hold the ceiling |
| **In-flight / queue depth** | concurrency behaviour |
| **Replica count over time** | HPA interaction |
| **Time-to-shed** / **time-to-recover** | responsiveness & hysteresis |

## Scenarios

1. **Baseline (sanity)** — steady baseline rps, no overload.
   *Expect:* ~0 shed, latency nominal, CPU below threshold. Shedder must not shed a healthy service.

2. **Gradual ramp** — linearly ramp baseline → 10× over N minutes.
   *Expect:* shedding engages near the knee; **goodput plateaus** at capacity; p99 stays bounded; CPU pinned near threshold, not 100%.

3. **Step spike** — instant jump baseline → {3×,6×,10×}, hold, then drop back.
   *Expect:* brief latency blip, excess shed within seconds, CPU bounded, **goodput stays at capacity** (not collapsing toward 0). Measure time-to-shed.

4. **Sustained overload** — hold 6× for 10+ min.
   *Expect:* steady-state shed rate, goodput flat at capacity, no upward latency/CPU drift, no OOM/goroutine leak.

5. **Slow dependency (RT, not CPU)** — inject +200ms downstream latency at fixed rps; CPU stays low but concurrency/RT climb.
   *Expect:* RT/concurrency-aware shedders (**quarkus, sentinel, queue**) engage; **CPU-only (go-zero)** may under-protect — document the gap.

6. **Autoscaling interaction** — overload with HPA enabled (min replicas 1 vs 3; target 75 vs 85).
   *Expect:* shed covers the scale-up lag, then shed rate decays as replicas come online; verify no oscillation between HPA and shedder.

7. **Recovery / hysteresis** — drop 10× → baseline instantly.
   *Expect:* shedding clears within the cool-off window; latency normalizes; no prolonged over-rejection.

8. **Priority fairness (quarkus only)** — mixed CRITICAL/NORMAL/DEGRADED + cohort traffic under overload.
   *Expect:* CRITICAL preserved longest, DEGRADED shed first; shed fraction tracks `1 - cpuLoad³`.

9. **Mixed route cost** — cheap + expensive endpoints together under overload.
   *Expect:* expensive endpoints don't starve cheap ones; goodput-weighting is acceptable.

## Pass / fail criteria (per overload scenario)

- **Tail latency:** p99 of *admitted* requests ≤ SLO (e.g. ≤ 2× baseline p99).
- **CPU ceiling:** sustained CPU ≤ shed threshold + small margin (e.g. ≤ threshold + 5%).
- **Goodput floor:** ≥ X% of single-replica capacity per replica (no death spiral — goodput must not trend to 0).
- **Recovery:** shed rate → ~0 within T seconds after load returns to baseline.
- **No leakage:** non-shed 5xx / timeout rate ≈ 0.

## Comparison report

Run scenarios 2–7 across all four shedders with one fixed config (e.g. min replicas 3, HPA 85, threshold 95, generated mix) and tabulate goodput, p99, CPU, shed rate, time-to-shed, time-to-recover. Expected qualitative shape:

| Shedder | Overload signal | Strength | Watch-out |
|---|---|---|---|
| go-zero | CPU + concurrency | self-tuning, cheap | blind to non-CPU stalls (scenario 5) |
| sentinel | CPU/load/RT/QPS + BBR | multi-metric | global rules, heavier deps |
| quarkus | RTT gradient + priority | graded, priority-aware | needs a CPU source for priority |
| queue | fixed budget | deterministic, predictable | must size Workers+Queue correctly; not adaptive |