package testsupport

import (
	"io"
	"log/slog"
)

// discardLogger returns a logger that drops everything, keeping test output
// focused on assertion failures.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Logger is the exported form of discardLogger for tests that must pass a
// logger into production constructors.
func Logger() *slog.Logger { return discardLogger() }
