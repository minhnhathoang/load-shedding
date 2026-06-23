// Package quarkus is a Go port of the Quarkus load-shedding extension.
//
// It combines two independent mechanisms:
//
//   - OverloadDetector: an adaptive concurrency limiter based on TCP Vegas, as
//     implemented by Netflix concurrency-limits. It learns the maximum number of
//     concurrent in-flight requests by observing response-time gradients.
//   - PriorityLoadShedding: once overloaded, decides which requests to shed based
//     on a per-request priority and cohort scored against a CPU-load threshold.
//
// Ported from:
// https://github.com/quarkusio/quarkus/tree/main/extensions/load-shedding
package quarkus

// Config holds the tunables for the load shedder. Defaults mirror the Quarkus
// extension's runtime configuration.
type Config struct {
	// MaxLimit is the maximum number of concurrent requests allowed.
	MaxLimit int64
	// AlphaFactor is the alpha factor of the Vegas overload detection algorithm.
	AlphaFactor int64
	// BetaFactor is the beta factor of the Vegas overload detection algorithm.
	BetaFactor int64
	// ProbeFactor is the probe factor of the Vegas overload detection algorithm.
	ProbeFactor float64
	// InitialLimit is the initial concurrency limit.
	InitialLimit int64
	// PriorityEnabled enables priority-based shedding once overloaded.
	PriorityEnabled bool
}

// DefaultConfig returns the Quarkus default configuration.
func DefaultConfig() Config {
	return Config{
		MaxLimit:        1000,
		AlphaFactor:     3,
		BetaFactor:      6,
		ProbeFactor:     30.0,
		InitialLimit:    100,
		PriorityEnabled: true,
	}
}
