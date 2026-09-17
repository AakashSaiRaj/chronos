package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

// errorCode is a stable, machine-readable error identifier. Clients branch on
// these rather than on HTTP status or message text.
type errorCode string

const (
	codeValidation   errorCode = "validation_error"
	codeNotFound     errorCode = "not_found"
	codeConflict     errorCode = "conflict"
	codeStaleClaim   errorCode = "stale_claim"
	codeBadRequest   errorCode = "bad_request"
	codeUnavailable  errorCode = "unavailable"
	codeInternal     errorCode = "internal_error"
	codeNotAllowed   errorCode = "method_not_allowed"
	codePayloadLarge errorCode = "payload_too_large"
)

// errorBody is the single error envelope every failing endpoint returns.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code      errorCode `json:"code"`
	Message   string    `json:"message"`
	RequestID string    `json:"requestId,omitempty"`
}

// httpError carries an explicit status for errors raised inside handlers, where
// the domain sentinels are not expressive enough.
type httpError struct {
	status  int
	code    errorCode
	message string
}

func (e *httpError) Error() string { return e.message }

func badRequest(message string) error {
	return &httpError{status: http.StatusBadRequest, code: codeBadRequest, message: message}
}

func payloadTooLarge(message string) error {
	return &httpError{status: http.StatusRequestEntityTooLarge, code: codePayloadLarge, message: message}
}

// classify maps an error onto the status and code the client should see.
//
// This is the only place transport semantics are attached to an error, which is
// what lets the engine and store speak purely in domain terms.
func classify(err error) (int, errorCode) {
	var he *httpError
	if errors.As(err, &he) {
		return he.status, he.code
	}

	switch {
	case errors.Is(err, domain.ErrValidation):
		return http.StatusBadRequest, codeValidation
	case errors.Is(err, domain.ErrNotFound):
		return http.StatusNotFound, codeNotFound
	case errors.Is(err, domain.ErrAlreadyExists):
		return http.StatusConflict, codeConflict
	case errors.Is(err, domain.ErrConflict):
		return http.StatusConflict, codeConflict
	case errors.Is(err, domain.ErrInvalidStateTransition):
		// The caller's view of state is stale rather than malformed, so this is
		// a conflict to be retried after re-reading, not a 400.
		return http.StatusConflict, codeConflict
	case errors.Is(err, domain.ErrStaleClaim):
		// 409 tells the worker its lease is gone and it must stop working on the
		// task; retrying the same report will never succeed.
		return http.StatusConflict, codeStaleClaim
	default:
		return http.StatusInternalServerError, codeInternal
	}
}

// writeError renders an error response, logging server-side faults with full
// detail while returning a generic message so internals never leak to clients.
func writeError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	status, code := classify(err)
	requestID := requestIDFrom(r.Context())

	message := err.Error()
	if status >= http.StatusInternalServerError {
		logger.Error("request failed",
			"method", r.Method, "path", r.URL.Path,
			"status", status, "requestId", requestID, "error", err)
		message = "internal server error"
	} else {
		logger.Debug("request rejected",
			"method", r.Method, "path", r.URL.Path,
			"status", status, "code", code, "requestId", requestID, "error", err)
	}

	writeJSON(w, r, logger, status, errorBody{Error: errorDetail{
		Code:      code,
		Message:   message,
		RequestID: requestID,
	}})
}

// writeJSON renders a success payload.
func writeJSON(w http.ResponseWriter, r *http.Request, logger *slog.Logger, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if payload == nil || status == http.StatusNoContent {
		return
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		// The status line is already committed, so the only useful action left
		// is to record that the body was truncated.
		logger.Error("encode response failed",
			"method", r.Method, "path", r.URL.Path, "error", err)
	}
}
