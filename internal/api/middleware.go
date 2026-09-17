package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// maxRequestBody caps request bodies. Workflow specs and task payloads are
// small; a bound keeps a malformed or hostile client from exhausting memory.
const maxRequestBody = 1 << 20 // 1 MiB

type contextKey string

const requestIDKey contextKey = "chronos.requestId"

// requestIDFrom returns the request ID attached by withRequestID.
func requestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// statusRecorder captures the response status for access logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// withRequestID attaches a request ID, honouring a client-supplied one so a
// trace can be followed across the worker/API boundary. This is the seam
// OpenTelemetry propagation replaces in Phase 4.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if id == "" || len(id) > 128 {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

// withRecovery converts a panic into a 500 so one bad request cannot take the
// process down and drop every in-flight worker poll with it.
func withRecovery(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if recovered := recover(); recovered != nil {
					// A closed connection surfaces as a panic from net/http and
					// is not an application fault.
					if errors.Is(r.Context().Err(), context.Canceled) {
						return
					}
					logger.Error("panic recovered",
						"method", r.Method, "path", r.URL.Path,
						"requestId", requestIDFrom(r.Context()),
						"panic", recovered, "stack", string(debug.Stack()))
					writeError(w, r, logger, fmt.Errorf("unhandled panic: %v", recovered))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// withAccessLog emits one structured line per request.
func withAccessLog(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Health and poll traffic is high-volume and uninteresting at info
			// level; log it at debug so production logs stay readable.
			chatty := r.URL.Path == "/healthz" || r.URL.Path == "/readyz" ||
				r.URL.Path == "/v1/tasks/poll"

			started := time.Now()
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r)

			level := slog.LevelInfo
			if chatty {
				level = slog.LevelDebug
			}
			if rec.status >= http.StatusInternalServerError {
				level = slog.LevelError
			}

			logger.Log(r.Context(), level, "http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.bytes,
				"durationMs", time.Since(started).Milliseconds(),
				"requestId", requestIDFrom(r.Context()))
		})
	}
}

// withBodyLimit bounds request bodies before any handler reads them.
func withBodyLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
		}
		next.ServeHTTP(w, r)
	})
}

// chain applies middleware so the first argument is the outermost layer.
func chain(h http.Handler, middleware ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middleware) - 1; i >= 0; i-- {
		h = middleware[i](h)
	}
	return h
}

// decodeJSON strictly decodes a JSON request body.
//
// Unknown fields are rejected: silently ignoring a misspelled "maxAttemps" would
// let a client believe it configured a retry policy that was never applied.
func decodeJSON(r *http.Request, dst any) error {
	if r.Body == nil {
		return badRequest("request body is required")
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return decodeError(err)
	}
	// Reject trailing content so "{} {}" is an error rather than a silent
	// partial read.
	if dec.More() {
		return badRequest("request body must contain exactly one JSON object")
	}
	return nil
}

// decodeError turns a json decode failure into a message that points at the
// actual problem, since encoding/json's raw errors are hard to act on.
func decodeError(err error) error {
	var (
		maxErr    *http.MaxBytesError
		syntaxErr *json.SyntaxError
		typeErr   *json.UnmarshalTypeError
	)
	switch {
	case errors.As(err, &maxErr):
		return payloadTooLarge(fmt.Sprintf("request body exceeds %d bytes", maxRequestBody))
	case errors.Is(err, io.EOF):
		return badRequest("request body is required")
	case errors.Is(err, io.ErrUnexpectedEOF):
		return badRequest("request body contains truncated JSON")
	case errors.As(err, &syntaxErr):
		return badRequest(fmt.Sprintf("malformed JSON at byte offset %d", syntaxErr.Offset))
	case errors.As(err, &typeErr):
		return badRequest(fmt.Sprintf("field %q must be of type %s", typeErr.Field, typeErr.Type))
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		return badRequest(fmt.Sprintf("unrecognized field %s",
			strings.TrimPrefix(err.Error(), "json: unknown field ")))
	default:
		return badRequest("request body could not be parsed as JSON")
	}
}

// queryInt reads a bounded integer query parameter.
func queryInt(r *http.Request, name string, def, min, max int) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, badRequest(fmt.Sprintf("%s must be an integer", name))
	}
	if v < min || v > max {
		return 0, badRequest(fmt.Sprintf("%s must be between %d and %d", name, min, max))
	}
	return v, nil
}

// queryInt64 reads a non-negative int64 query parameter.
func queryInt64(r *http.Request, name string, def int64) (int64, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 0, badRequest(fmt.Sprintf("%s must be a non-negative integer", name))
	}
	return v, nil
}

// pathUUID parses a UUID path parameter.
func pathUUID(r *http.Request, name string) (uuid.UUID, error) {
	raw := r.PathValue(name)
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, badRequest(fmt.Sprintf("%s must be a UUID, got %q", name, raw))
	}
	return id, nil
}
