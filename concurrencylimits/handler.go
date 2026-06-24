package concurrencylimits

import "net/http"

// Handler wraps next, gating admission through limiter. Rejected requests get
// 503 Service Unavailable. A downstream 5xx is reported as OnDropped so it does
// not inflate the measured capacity; everything else is OnSuccess.
func Handler(limiter Limiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		listener, ok := limiter.Acquire()
		if !ok {
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		cw := &codeWriter{ResponseWriter: w, code: http.StatusOK}
		defer func() {
			if p := recover(); p != nil {
				listener.OnDropped()
				panic(p)
			}
			if cw.code >= 500 {
				listener.OnDropped()
			} else {
				listener.OnSuccess()
			}
		}()
		next.ServeHTTP(cw, r)
	})
}

// Handler returns a middleware gating admission via this SimpleLimiter.
func (l *SimpleLimiter) Handler(next http.Handler) http.Handler {
	return Handler(l, next)
}

// codeWriter captures the response status code so the handler can distinguish a
// success from a 5xx/503.
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
