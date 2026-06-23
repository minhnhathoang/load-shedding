// Package gozero adapts go-zero's adaptive load shedder (core/load) to the
// common shape used across this repository: a programmatic Allow plus an
// http.Handler middleware.
//
// go-zero's shedder is adaptive: it estimates capacity from peak throughput and
// minimum latency (Little's Law) and sheds when CPU is saturated AND in-flight
// concurrency exceeds that estimate.
package gozero

import (
	"net/http"

	"github.com/zeromicro/go-zero/core/load"
)

// Promise mirrors load.Promise: the caller reports the outcome of an admitted
// request so the shedder can update its capacity model.
type Promise = load.Promise

// Shedder wraps a go-zero adaptive shedder.
type Shedder struct {
	inner load.Shedder
}

// New returns a Shedder backed by go-zero's adaptive shedder. opts are forwarded
// to load.NewAdaptiveShedder (e.g. load.WithCpuThreshold, load.WithWindow).
func New(opts ...load.ShedderOption) *Shedder {
	return &Shedder{inner: load.NewAdaptiveShedder(opts...)}
}

// Allow reports whether a request may proceed. On success it returns a Promise
// whose Pass/Fail must be called on completion; otherwise it returns
// load.ErrServiceOverloaded.
func (s *Shedder) Allow() (Promise, error) {
	return s.inner.Allow()
}

// Handler returns a middleware that sheds load, responding with 503 Service
// Unavailable when overloaded. A downstream 503 is reported as Fail so it does
// not inflate the measured capacity.
func (s *Shedder) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		promise, err := s.inner.Allow()
		if err != nil {
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		cw := &codeWriter{ResponseWriter: w, code: http.StatusOK}
		defer func() {
			if cw.code == http.StatusServiceUnavailable {
				promise.Fail()
			} else {
				promise.Pass()
			}
		}()
		next.ServeHTTP(cw, r)
	})
}

// codeWriter captures the response status code so the handler can distinguish a
// successful request from a 503.
type codeWriter struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (w *codeWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *codeWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}
