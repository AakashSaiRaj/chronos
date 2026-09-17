package client_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/client"
	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/testsupport"
)

func newClient(t *testing.T, handler http.Handler, maxRetries int) *client.Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	c, err := client.New(client.Config{
		BaseURL:    srv.URL,
		Timeout:    5 * time.Second,
		MaxRetries: maxRetries,
		Logger:     testsupport.Logger(),
	})
	require.NoError(t, err)
	return c
}

func TestNewRequiresBaseURL(t *testing.T) {
	_, err := client.New(client.Config{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "BaseURL")
}

func TestNewTrimsTrailingSlash(t *testing.T) {
	var gotPath string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	c, err := client.New(client.Config{BaseURL: srv.URL + "/", Logger: testsupport.Logger()})
	require.NoError(t, err)
	require.NoError(t, c.Ready(context.Background()))
	require.Equal(t, "/readyz", gotPath, "paths must not double up on the slash")
}

// TestPollTaskTreatsNoContentAsEmptyQueue: an idle queue is the normal steady
// state, so it must not surface as an error to the worker's poll loop.
func TestPollTaskTreatsNoContentAsEmptyQueue(t *testing.T) {
	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), 0)

	_, err := c.PollTask(context.Background(), client.PollRequest{WorkerID: "w1"})
	require.ErrorIs(t, err, client.ErrNoTask)
}

func TestPollTaskDecodesTask(t *testing.T) {
	taskID := uuid.New()
	token := uuid.New()

	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/tasks/poll", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var req client.PollRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, "w1", req.WorkerID)
		require.Equal(t, 30, req.LeaseSeconds)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": taskID, "executionId": uuid.New(), "name": "task_a",
			"activity": "noop", "state": "RUNNING", "attempt": 1, "maxAttempts": 1,
			"claimToken": token.String(), "input": json.RawMessage(`{"k":1}`),
			"taskQueue": "default", "createdAt": time.Now(), "updatedAt": time.Now(),
		})
	}), 0)

	task, err := c.PollTask(context.Background(), client.PollRequest{
		WorkerID: "w1", TaskQueue: "default", LeaseSeconds: 30,
	})
	require.NoError(t, err)
	require.Equal(t, taskID, task.ID)
	require.Equal(t, "task_a", task.Name)
	require.Equal(t, token.String(), task.ClaimToken)
	require.Equal(t, domain.TaskRunning, task.State)
	require.JSONEq(t, `{"k":1}`, string(task.Input))
}

// TestRetriesTransientServerErrors is only safe because every mutating endpoint
// is idempotent; this test pins that the client actually relies on it.
func TestRetriesTransientServerErrors(t *testing.T) {
	var attempts int32
	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&attempts, 1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":"unavailable","message":"rolling"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"` + uuid.NewString() + `","state":"COMPLETED","name":"task_a"}`))
	}), 3)

	task, err := c.CompleteTask(context.Background(), uuid.New(), uuid.NewString(), json.RawMessage(`{}`))
	require.NoError(t, err)
	require.Equal(t, domain.TaskCompleted, task.State)
	require.Equal(t, int32(3), atomic.LoadInt32(&attempts), "the client must retry until it succeeds")
}

func TestGivesUpAfterMaxRetries(t *testing.T) {
	var attempts int32
	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"broken"}}`))
	}), 2)

	_, err := c.CompleteTask(context.Background(), uuid.New(), uuid.NewString(), nil)
	require.Error(t, err)
	require.Equal(t, int32(3), atomic.LoadInt32(&attempts),
		"the initial attempt plus MaxRetries retries, then stop")

	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusInternalServerError, apiErr.StatusCode)
	require.True(t, apiErr.Retryable())
}

// TestDoesNotRetryClientErrors matters because retrying a 4xx wastes the
// server's time and can never succeed.
func TestDoesNotRetryClientErrors(t *testing.T) {
	var attempts int32
	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":"validation_error","message":"bad input"}}`))
	}), 3)

	_, err := c.CompleteTask(context.Background(), uuid.New(), uuid.NewString(), nil)
	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&attempts), "a 4xx must not be retried")

	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "validation_error", apiErr.Code)
	require.False(t, apiErr.Retryable())
	require.Contains(t, apiErr.Error(), "bad input")
}

// TestStaleClaimIsASentinel lets the worker distinguish "your lease is gone,
// abandon this task" from a retryable failure.
func TestStaleClaimIsASentinel(t *testing.T) {
	var attempts int32
	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"code":"stale_claim","message":"task reassigned"}}`))
	}), 3)

	_, err := c.CompleteTask(context.Background(), uuid.New(), uuid.NewString(), nil)
	require.ErrorIs(t, err, client.ErrStaleClaim)
	require.Contains(t, err.Error(), "task reassigned")
	require.Equal(t, int32(1), atomic.LoadInt32(&attempts),
		"a lost lease can never be resolved by retrying")
}

func TestStartExecutionSendsIdempotencyHeader(t *testing.T) {
	var gotHeader string
	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("Idempotency-Key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"` + uuid.NewString() + `","state":"PENDING","workflowName":"w"}`))
	}), 0)

	exec, err := c.StartExecution(context.Background(), client.StartExecutionRequest{
		WorkflowName:   "w",
		IdempotencyKey: "order-1",
	})
	require.NoError(t, err)
	require.Equal(t, domain.WorkflowPending, exec.State)
	require.Equal(t, "order-1", gotHeader,
		"the key must travel as a header so proxies and retries can see it")
}

func TestRegisterWorkflowRoundTrip(t *testing.T) {
	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/workflows", r.URL.Path)

		var spec domain.WorkflowSpec
		require.NoError(t, json.NewDecoder(r.Body).Decode(&spec))
		require.Equal(t, "pipeline", spec.Name)
		require.Len(t, spec.Tasks, 2)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": uuid.New(), "name": spec.Name, "version": spec.Version,
			"taskQueue": "default", "tasks": spec.Tasks, "specHash": "abc",
			"createdAt": time.Now(),
		})
	}), 0)

	out, err := c.RegisterWorkflow(context.Background(), domain.WorkflowSpec{
		Name: "pipeline", Version: 1,
		Tasks: []domain.TaskSpec{
			{Name: "task_a", Activity: "noop"},
			{Name: "task_b", Activity: "noop", DependsOn: []string{"task_a"}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "pipeline", out.Name)
	require.Equal(t, "abc", out.SpecHash)
	require.Len(t, out.Tasks, 2)
}

func TestGetWorkflowUsesVersionedPathWhenPinned(t *testing.T) {
	var paths []string
	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"` + uuid.NewString() + `","name":"w","version":1}`))
	}), 0)

	_, err := c.GetWorkflow(context.Background(), "w", 0)
	require.NoError(t, err)
	_, err = c.GetWorkflow(context.Background(), "w", 3)
	require.NoError(t, err)

	require.Equal(t, []string{"/v1/workflows/w", "/v1/workflows/w/versions/3"}, paths)
}

func TestGetHistoryUsesCursor(t *testing.T) {
	var gotQuery string
	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"executionId":"` + uuid.NewString() + `","events":[]}`))
	}), 0)

	_, err := c.GetHistory(context.Background(), uuid.New(), 42)
	require.NoError(t, err)
	require.Equal(t, "afterId=42", gotQuery)
}

func TestHeartbeatAndReadyTolerateEmptyBodies(t *testing.T) {
	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), 0)

	require.NoError(t, c.Heartbeat(context.Background(), uuid.New()))
	require.NoError(t, c.Ready(context.Background()))
}

// TestRetriesTransportFailures covers a rolling deployment: the connection dies
// mid-request and the next attempt lands on a healthy replica.
func TestRetriesTransportFailures(t *testing.T) {
	var attempts int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&attempts, 1) == 1 {
			// Abort the connection without a response.
			if hijacker, ok := w.(http.Hijacker); ok {
				conn, _, err := hijacker.Hijack()
				if err == nil {
					_ = conn.Close()
					return
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	c := newClient(t, handler, 3)

	require.NoError(t, c.Ready(context.Background()))
	require.GreaterOrEqual(t, atomic.LoadInt32(&attempts), int32(2),
		"a dropped connection must be retried")
}

func TestContextCancellationStopsRetrying(t *testing.T) {
	c := newClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}), 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.PollTask(ctx, client.PollRequest{WorkerID: "w1"})
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAPIErrorRetryableClassification(t *testing.T) {
	for status, retryable := range map[int]bool{
		http.StatusBadRequest:          false,
		http.StatusNotFound:            false,
		http.StatusConflict:            false,
		http.StatusTooManyRequests:     true,
		http.StatusInternalServerError: true,
		http.StatusBadGateway:          true,
		http.StatusServiceUnavailable:  true,
	} {
		err := &client.APIError{StatusCode: status}
		require.Equal(t, retryable, err.Retryable(), "status %d", status)
	}
}
