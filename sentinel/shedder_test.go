package sentinel

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alibaba/sentinel-golang/core/system"
	"github.com/stretchr/testify/assert"
)

func TestBuildRules(t *testing.T) {
	rules := buildRules(Config{
		CpuThreshold:   0.8,
		MaxConcurrency: 100,
		MaxLoad:        8,
		AvgRtMillis:    50,
	})
	assert.Len(t, rules, 4)
	for _, r := range rules {
		assert.Equal(t, system.BBR, r.Strategy)
		assert.Greater(t, r.TriggerCount, 0.0)
	}

	// Non-positive thresholds are skipped.
	assert.Empty(t, buildRules(Config{}))
	assert.Len(t, buildRules(Config{CpuThreshold: 0.9}), 1)
}

func TestNewDefault(t *testing.T) {
	s, err := New(DefaultConfig())
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.Equal(t, defaultResource, s.resource)
}

func TestNewEmptyResourceFallsBack(t *testing.T) {
	s, err := New(Config{CpuThreshold: 0.9})
	assert.NoError(t, err)
	assert.Equal(t, defaultResource, s.resource)
}

func TestAllowHappyPath(t *testing.T) {
	// A very high CPU trigger (>1.0 is never reached) means BBR stays inactive,
	// so requests are admitted.
	s, err := New(Config{Resource: "test-allow", CpuThreshold: 2.0})
	assert.NoError(t, err)

	done, ok := s.Allow()
	assert.True(t, ok)
	assert.NotNil(t, done)
	done() // must not panic
}

func TestHandlerServesWhenAllowed(t *testing.T) {
	s, err := New(Config{Resource: "test-handler", CpuThreshold: 2.0})
	assert.NoError(t, err)

	h := s.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestNoRulesStillAllows(t *testing.T) {
	// No thresholds => no system rules loaded => everything is admitted.
	s, err := New(Config{Resource: "test-norules"})
	assert.NoError(t, err)

	_, ok := s.Allow()
	assert.True(t, ok)
}
