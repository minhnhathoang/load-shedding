package gozero

import (
	"net/http"

	"github.com/zeromicro/go-zero/core/load"
)

type Promise = load.Promise

type Shedder struct {
	inner load.Shedder
}

func New(opts ...load.ShedderOption) *Shedder {
	return &Shedder{inner: load.NewAdaptiveShedder(opts...)}
}

func (s *Shedder) Allow() (Promise, error) {
	return s.inner.Allow()
}

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
			if p := recover(); p != nil {
				promise.Fail()
				panic(p)
			}
			if cw.code >= http.StatusInternalServerError {
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

func (w *codeWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
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
