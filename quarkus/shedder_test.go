package quarkus

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShedderAdmitNotOverloaded(t *testing.T) {
	s := New(DefaultConfig())
	done, ok := s.Admit("req")
	assert.True(t, ok)
	assert.NotNil(t, done)
	assert.Equal(t, int64(1), s.Detector().CurrentRequests())
	done()
	assert.Equal(t, int64(0), s.Detector().CurrentRequests())
}

func TestShedderAdmitOverloadedSheds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialLimit = 1
	s := New(cfg,
		WithCpuLoad(func() float64 { return 0.99 }),
		WithClock(func() time.Time { return time.Unix(10, 0) }),
		WithPrioritizers(fixedPrioritizer{PriorityNormal}),
		WithClassifiers(fixedClassifier{64}),
	)

	// Fill the single slot so the detector reports overloaded.
	done, ok := s.Admit("req1")
	assert.True(t, ok)
	assert.True(t, s.Detector().IsOverloaded())

	// Second request is shed (overloaded AND priority decides to shed).
	done2, ok2 := s.Admit("req2")
	assert.False(t, ok2)
	assert.Nil(t, done2)

	done()
}

func TestShedderHandlerServesAndSheds(t *testing.T) {
	cfg := DefaultConfig()
	s := New(cfg)

	h := s.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, int64(0), s.Detector().CurrentRequests())
}

func TestShedderHandlerShedsWhenOverloaded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialLimit = 1
	s := New(cfg,
		WithCpuLoad(func() float64 { return 0.99 }),
		WithClock(func() time.Time { return time.Unix(10, 0) }),
	)

	// Occupy the only slot.
	done, ok := s.Admit("occupied")
	assert.True(t, ok)
	defer done()

	h := s.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
