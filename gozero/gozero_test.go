package gozero

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/load"
)

func TestAllowAdmitsAndPasses(t *testing.T) {
	s := New(load.WithCpuThreshold(900))
	p, err := s.Allow()
	assert.NoError(t, err)
	assert.NotNil(t, p)
	p.Pass()
}

func TestHandlerServes(t *testing.T) {
	s := New()
	h := s.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCodeWriterCapturesStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	cw := &codeWriter{ResponseWriter: rec, code: http.StatusOK}
	cw.WriteHeader(http.StatusServiceUnavailable)
	cw.WriteHeader(http.StatusOK) // second call ignored
	assert.Equal(t, http.StatusServiceUnavailable, cw.code)

	// Write without explicit WriteHeader defaults to 200.
	rec2 := httptest.NewRecorder()
	cw2 := &codeWriter{ResponseWriter: rec2, code: 0}
	_, _ = cw2.Write([]byte("ok"))
	assert.Equal(t, http.StatusOK, cw2.code)
}
