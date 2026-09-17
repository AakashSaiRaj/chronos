package domain

import "errors"

// Sentinel errors. The API layer maps these onto HTTP status codes in exactly
// one place (internal/api/errors.go) so transport concerns never leak into the
// engine or store.
var (
	// ErrNotFound means the requested entity does not exist.
	ErrNotFound = errors.New("not found")

	// ErrAlreadyExists means a uniqueness constraint rejected the write.
	ErrAlreadyExists = errors.New("already exists")

	// ErrConflict means the request collided with existing durable state in a
	// way the caller must resolve, e.g. re-registering an immutable workflow
	// version with a different body.
	ErrConflict = errors.New("conflict")

	// ErrInvalidStateTransition means a state machine transition was rejected.
	ErrInvalidStateTransition = errors.New("invalid state transition")

	// ErrValidation means caller-supplied input was malformed.
	ErrValidation = errors.New("validation failed")

	// ErrStaleClaim means a worker tried to report on a task whose lease it no
	// longer holds. This is the guard that makes task completion safe when a
	// slow worker returns after its task has been reassigned.
	ErrStaleClaim = errors.New("stale task claim")
)
