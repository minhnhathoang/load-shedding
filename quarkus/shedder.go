package quarkus

import (
	"net/http"
	"time"
)

// Shedder ties the OverloadDetector and PriorityLoadShedding together, mirroring
// the orchestration of the Quarkus HttpLoadShedding handler.
type Shedder struct {
	detector *OverloadDetector
	priority *PriorityLoadShedding
}

// New returns a Shedder built from cfg. opts customize the priority component.
func New(cfg Config, opts ...PriorityOption) *Shedder {
	return &Shedder{
		detector: NewOverloadDetector(cfg),
		priority: NewPriorityLoadShedding(cfg, opts...),
	}
}

// Detector exposes the underlying overload detector (for metrics/inspection).
func (s *Shedder) Detector() *OverloadDetector { return s.detector }

// Admit decides whether to accept request. When ok is true the caller must
// invoke the returned done func exactly once when the request finishes. When ok
// is false the request should be rejected and done is nil.
func (s *Shedder) Admit(request any) (done func(), ok bool) {
	if s.detector.IsOverloaded() && s.priority.ShedLoad(request) {
		return nil, false
	}

	s.detector.RequestBegin()
	start := time.Now()
	return func() {
		s.detector.RequestEnd(time.Since(start))
	}, true
}

// Handler returns a middleware that sheds load using the HTTP request as the
// classification subject. Shed requests receive 503 Service Unavailable.
func (s *Shedder) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		done, ok := s.Admit(r)
		if !ok {
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		defer done()
		next.ServeHTTP(w, r)
	})
}
