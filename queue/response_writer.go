package queue

import "net/http"

// codeWriter captures the response status code while preserving access to the
// underlying writer through http.ResponseController.
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
