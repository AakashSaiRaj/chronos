package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/api"
	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/engine"
	"github.com/AakashSaiRaj/chronos/internal/store"
	"github.com/AakashSaiRaj/chronos/internal/testsupport"
)

// stubService is a scriptable api.Service. The HTTP layer's job is routing,
// parsing, and status mapping, so testing it against a stub keeps these tests
// fast and focused on transport behaviour rather than on the engine.
type stubService struct {
	pingErr error

	registerWorkflow func(context.Context, domain.WorkflowSpec) (*domain.WorkflowDefinition, bool, error)
	getWorkflow      func(context.Context, string, int) (*domain.WorkflowDefinition, error)
	listWorkflows    func(context.Context, string, int) ([]domain.WorkflowDefinition, error)

	startExecution func(context.Context, engine.StartExecutionRequest) (*domain.WorkflowExecution, bool, error)
	getExecution   func(context.Context, uuid.UUID) (*engine.ExecutionDetail, error)
	listExecutions func(context.Context, store.ExecutionFilter) ([]domain.WorkflowExecution, error)
	getHistory     func(context.Context, uuid.UUID, int64, int) ([]domain.HistoryEvent, error)
	cancel         func(context.Context, uuid.UUID, string) (*domain.WorkflowExecution, error)

	registerWorker func(context.Context, store.RegisterWorkerParams) (*domain.Worker, error)
	heartbeat      func(context.Context, uuid.UUID) (*domain.Worker, error)
	listWorkers    func(context.Context, string, int) ([]domain.Worker, error)

	pollTask     func(context.Context, engine.PollRequest) (*domain.Task, error)
	completeTask func(context.Context, uuid.UUID, uuid.UUID, json.RawMessage) (*domain.Task, error)
	failTask     func(context.Context, uuid.UUID, uuid.UUID, store.FailParams) (*domain.Task, error)

	heartbeatTask  func(context.Context, uuid.UUID, uuid.UUID, time.Duration) (*engine.LeaseHeartbeat, error)
	listDeadLetter func(context.Context, store.DeadLetterFilter) ([]domain.Task, error)
	countDeadLtr   func(context.Context) (int, error)
	replayTask     func(context.Context, uuid.UUID, int) (*domain.Task, error)

	// lastStart records what the handler passed down, so header/body precedence
	// can be asserted.
	lastStart      engine.StartExecutionRequest
	lastPoll       engine.PollRequest
	lastFiler      store.ExecutionFilter
	lastFail       store.FailParams
	lastDLQFilter  store.DeadLetterFilter
	lastReplayN    int
	lastHeartbeatD time.Duration
}

func (s *stubService) Ping(context.Context) error { return s.pingErr }

func (s *stubService) RegisterWorkflow(ctx context.Context, spec domain.WorkflowSpec) (*domain.WorkflowDefinition, bool, error) {
	if s.registerWorkflow != nil {
		return s.registerWorkflow(ctx, spec)
	}
	return &domain.WorkflowDefinition{
		ID: uuid.New(), Name: spec.Name, Version: spec.Version,
		TaskQueue: spec.TaskQueue, Spec: spec, SpecHash: "hash", CreatedAt: time.Now(),
	}, true, nil
}

func (s *stubService) GetWorkflow(ctx context.Context, name string, version int) (*domain.WorkflowDefinition, error) {
	if s.getWorkflow != nil {
		return s.getWorkflow(ctx, name, version)
	}
	return &domain.WorkflowDefinition{ID: uuid.New(), Name: name, Version: max(version, 1)}, nil
}

func (s *stubService) ListWorkflows(ctx context.Context, name string, limit int) ([]domain.WorkflowDefinition, error) {
	if s.listWorkflows != nil {
		return s.listWorkflows(ctx, name, limit)
	}
	return nil, nil
}

func (s *stubService) StartExecution(ctx context.Context, req engine.StartExecutionRequest) (*domain.WorkflowExecution, bool, error) {
	s.lastStart = req
	if s.startExecution != nil {
		return s.startExecution(ctx, req)
	}
	return &domain.WorkflowExecution{
		ID: uuid.New(), WorkflowName: req.WorkflowName, State: domain.WorkflowPending,
		Input: json.RawMessage(`{}`), IdempotencyKey: req.IdempotencyKey,
	}, true, nil
}

func (s *stubService) GetExecution(ctx context.Context, id uuid.UUID) (*engine.ExecutionDetail, error) {
	if s.getExecution != nil {
		return s.getExecution(ctx, id)
	}
	return &engine.ExecutionDetail{
		Execution: &domain.WorkflowExecution{ID: id, State: domain.WorkflowRunning, Input: json.RawMessage(`{}`)},
	}, nil
}

func (s *stubService) ListExecutions(ctx context.Context, f store.ExecutionFilter) ([]domain.WorkflowExecution, error) {
	s.lastFiler = f
	if s.listExecutions != nil {
		return s.listExecutions(ctx, f)
	}
	return nil, nil
}

func (s *stubService) GetHistory(ctx context.Context, id uuid.UUID, afterID int64, limit int) ([]domain.HistoryEvent, error) {
	if s.getHistory != nil {
		return s.getHistory(ctx, id, afterID, limit)
	}
	return nil, nil
}

func (s *stubService) CancelExecution(ctx context.Context, id uuid.UUID, reason string) (*domain.WorkflowExecution, error) {
	if s.cancel != nil {
		return s.cancel(ctx, id, reason)
	}
	return &domain.WorkflowExecution{ID: id, State: domain.WorkflowCanceled, Error: reason, Input: json.RawMessage(`{}`)}, nil
}

func (s *stubService) RegisterWorker(ctx context.Context, p store.RegisterWorkerParams) (*domain.Worker, error) {
	if s.registerWorker != nil {
		return s.registerWorker(ctx, p)
	}
	return &domain.Worker{ID: uuid.New(), Name: p.Name, TaskQueue: p.TaskQueue,
		Activities: p.Activities, State: domain.WorkerActive}, nil
}

func (s *stubService) Heartbeat(ctx context.Context, id uuid.UUID) (*domain.Worker, error) {
	if s.heartbeat != nil {
		return s.heartbeat(ctx, id)
	}
	return &domain.Worker{ID: id, State: domain.WorkerActive}, nil
}

func (s *stubService) ListWorkers(ctx context.Context, queue string, limit int) ([]domain.Worker, error) {
	if s.listWorkers != nil {
		return s.listWorkers(ctx, queue, limit)
	}
	return nil, nil
}

func (s *stubService) PollTask(ctx context.Context, req engine.PollRequest) (*domain.Task, error) {
	s.lastPoll = req
	if s.pollTask != nil {
		return s.pollTask(ctx, req)
	}
	return nil, domain.ErrNotFound
}

func (s *stubService) CompleteTask(ctx context.Context, taskID, token uuid.UUID, output json.RawMessage) (*domain.Task, error) {
	if s.completeTask != nil {
		return s.completeTask(ctx, taskID, token, output)
	}
	return &domain.Task{ID: taskID, State: domain.TaskCompleted, Output: output, Input: json.RawMessage(`{}`)}, nil
}

func (s *stubService) FailTask(ctx context.Context, taskID, token uuid.UUID, p store.FailParams) (*domain.Task, error) {
	s.lastFail = p
	if s.failTask != nil {
		return s.failTask(ctx, taskID, token, p)
	}
	return &domain.Task{
		ID: taskID, State: domain.TaskFailed, Error: p.Error,
		Retryable: p.Retryable, LastFailureReason: string(p.Reason),
		Input: json.RawMessage(`{}`),
	}, nil
}

func (s *stubService) HeartbeatTask(ctx context.Context, taskID, token uuid.UUID, extendBy time.Duration) (*engine.LeaseHeartbeat, error) {
	s.lastHeartbeatD = extendBy
	if s.heartbeatTask != nil {
		return s.heartbeatTask(ctx, taskID, token, extendBy)
	}
	expiry := time.Now().Add(extendBy)
	return &engine.LeaseHeartbeat{
		Task:           &domain.Task{ID: taskID, State: domain.TaskRunning, Attempt: 1},
		LeaseExpiresAt: &expiry,
	}, nil
}

func (s *stubService) ListDeadLetterTasks(ctx context.Context, f store.DeadLetterFilter) ([]domain.Task, error) {
	s.lastDLQFilter = f
	if s.listDeadLetter != nil {
		return s.listDeadLetter(ctx, f)
	}
	return nil, nil
}

func (s *stubService) CountDeadLetterTasks(ctx context.Context) (int, error) {
	if s.countDeadLtr != nil {
		return s.countDeadLtr(ctx)
	}
	return 0, nil
}

func (s *stubService) ReplayTask(ctx context.Context, taskID uuid.UUID, extraAttempts int) (*domain.Task, error) {
	s.lastReplayN = extraAttempts
	if s.replayTask != nil {
		return s.replayTask(ctx, taskID, extraAttempts)
	}
	return &domain.Task{
		ID: taskID, State: domain.TaskScheduled, MaxAttempts: 1 + extraAttempts,
		Input: json.RawMessage(`{}`),
	}, nil
}

// newTestServer wires a stub behind the real router and middleware stack.
func newTestServer(t *testing.T, svc *stubService) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(api.NewServer(svc, api.Options{
		Logger:  testsupport.Logger(),
		Version: "test",
	}))
	t.Cleanup(srv.Close)
	return srv
}

func doJSON(t *testing.T, srv *httptest.Server, method, path, body string, headers map[string]string) (*http.Response, []byte) {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, reader)
	require.NoError(t, err)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, raw
}

func errorCode(t *testing.T, raw []byte) string {
	t.Helper()
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"requestId"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(raw, &envelope), "error body must be the standard envelope")
	require.NotEmpty(t, envelope.Error.Message, "errors must carry a message")
	require.NotEmpty(t, envelope.Error.RequestID, "errors must carry a request id for correlation")
	return envelope.Error.Code
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

// TestHealthEndpointsSeparateLivenessFromReadiness pins the operational contract:
// a database outage must not make the process look dead, or an orchestrator would
// pointlessly restart it.
func TestHealthEndpointsSeparateLivenessFromReadiness(t *testing.T) {
	svc := &stubService{pingErr: fmt.Errorf("database is down")}
	srv := newTestServer(t, svc)

	resp, body := doJSON(t, srv, http.MethodGet, "/healthz", "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"liveness must not depend on the database")
	require.Contains(t, string(body), `"status":"ok"`)

	resp, body = doJSON(t, srv, http.MethodGet, "/readyz", "", nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"readiness must fail when the database is unreachable")
	require.Contains(t, string(body), "unreachable")

	svc.pingErr = nil
	resp, _ = doJSON(t, srv, http.MethodGet, "/readyz", "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRequestIDIsEchoedAndGenerated(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	resp, _ := doJSON(t, srv, http.MethodGet, "/healthz", "", map[string]string{"X-Request-Id": "trace-abc"})
	require.Equal(t, "trace-abc", resp.Header.Get("X-Request-Id"),
		"a client-supplied request id must be preserved for cross-service tracing")

	resp, _ = doJSON(t, srv, http.MethodGet, "/healthz", "", nil)
	require.NotEmpty(t, resp.Header.Get("X-Request-Id"),
		"a request id must be generated when the client supplies none")
}

// ---------------------------------------------------------------------------
// Workflow registration
// ---------------------------------------------------------------------------

func TestRegisterWorkflowStatusReflectsCreation(t *testing.T) {
	created := true
	svc := &stubService{
		registerWorkflow: func(_ context.Context, spec domain.WorkflowSpec) (*domain.WorkflowDefinition, bool, error) {
			return &domain.WorkflowDefinition{
				ID: uuid.New(), Name: spec.Name, Version: spec.Version, Spec: spec,
			}, created, nil
		},
	}
	srv := newTestServer(t, svc)
	body := `{"name":"order_pipeline","version":1,"tasks":[{"name":"task_a","activity":"noop"}]}`

	resp, _ := doJSON(t, srv, http.MethodPost, "/v1/workflows", body, nil)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "/v1/workflows/order_pipeline/versions/1", resp.Header.Get("Location"))

	// An idempotent re-registration is a 200, not a 201 or a conflict.
	created = false
	resp, _ = doJSON(t, srv, http.MethodPost, "/v1/workflows", body, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Location"))
}

func TestRegisterWorkflowPassesSpecThrough(t *testing.T) {
	var captured domain.WorkflowSpec
	svc := &stubService{
		registerWorkflow: func(_ context.Context, spec domain.WorkflowSpec) (*domain.WorkflowDefinition, bool, error) {
			captured = spec
			return &domain.WorkflowDefinition{ID: uuid.New(), Name: spec.Name, Version: spec.Version, Spec: spec}, true, nil
		},
	}
	srv := newTestServer(t, svc)

	resp, _ := doJSON(t, srv, http.MethodPost, "/v1/workflows", `{
		"name":"pipeline","version":2,"description":"d","taskQueue":"q",
		"tasks":[
			{"name":"task_a","activity":"noop","maxAttempts":3,"timeoutSeconds":30,"input":{"k":"v"}},
			{"name":"task_b","activity":"noop","dependsOn":["task_a"]}
		]}`, nil)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	require.Equal(t, "pipeline", captured.Name)
	require.Equal(t, 2, captured.Version)
	require.Equal(t, "q", captured.TaskQueue)
	require.Len(t, captured.Tasks, 2)
	require.Equal(t, 3, captured.Tasks[0].MaxAttempts)
	require.Equal(t, 30, captured.Tasks[0].TimeoutSeconds)
	require.JSONEq(t, `{"k":"v"}`, string(captured.Tasks[0].Input))
	require.Equal(t, []string{"task_a"}, captured.Tasks[1].DependsOn)
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

// TestDomainErrorsMapToStatusCodes is the contract clients depend on, and the
// reason the engine never needs to know about HTTP.
func TestDomainErrorsMapToStatusCodes(t *testing.T) {
	tests := map[string]struct {
		err        error
		wantStatus int
		wantCode   string
	}{
		"validation":       {domain.ErrValidation, http.StatusBadRequest, "validation_error"},
		"not found":        {domain.ErrNotFound, http.StatusNotFound, "not_found"},
		"already exists":   {domain.ErrAlreadyExists, http.StatusConflict, "conflict"},
		"conflict":         {domain.ErrConflict, http.StatusConflict, "conflict"},
		"state transition": {domain.ErrInvalidStateTransition, http.StatusConflict, "conflict"},
		"stale claim":      {domain.ErrStaleClaim, http.StatusConflict, "stale_claim"},
		"unexpected error": {fmt.Errorf("kaboom"), http.StatusInternalServerError, "internal_error"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			svc := &stubService{
				getWorkflow: func(context.Context, string, int) (*domain.WorkflowDefinition, error) {
					return nil, fmt.Errorf("wrapped: %w", tc.err)
				},
			}
			srv := newTestServer(t, svc)

			resp, raw := doJSON(t, srv, http.MethodGet, "/v1/workflows/whatever", "", nil)
			require.Equal(t, tc.wantStatus, resp.StatusCode)
			require.Equal(t, tc.wantCode, errorCode(t, raw))
		})
	}
}

// TestInternalErrorsDoNotLeakDetails matters because store errors can contain
// SQL fragments and constraint names.
func TestInternalErrorsDoNotLeakDetails(t *testing.T) {
	svc := &stubService{
		getWorkflow: func(context.Context, string, int) (*domain.WorkflowDefinition, error) {
			return nil, fmt.Errorf("pq: relation \"secret_table\" does not exist")
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodGet, "/v1/workflows/x", "", nil)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.NotContains(t, string(raw), "secret_table",
		"server-side failure detail must stay in the logs, not the response")
	require.Contains(t, string(raw), "internal server error")
}

// ---------------------------------------------------------------------------
// Request decoding
// ---------------------------------------------------------------------------

func TestRequestBodyValidation(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	tests := map[string]struct {
		body     string
		wantCode string
	}{
		"missing body":     {"", "bad_request"},
		"malformed json":   {`{"name":`, "bad_request"},
		"wrong field type": {`{"name":"x","version":"not-a-number","tasks":[]}`, "bad_request"},
		"unknown field":    {`{"name":"x","version":1,"tasks":[],"maxAttemps":3}`, "bad_request"},
		"trailing content": {`{"name":"x","version":1,"tasks":[]} {}`, "bad_request"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			resp, raw := doJSON(t, srv, http.MethodPost, "/v1/workflows", tc.body, nil)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.Equal(t, tc.wantCode, errorCode(t, raw))
		})
	}
}

// TestUnknownFieldsAreRejected guards a subtle failure mode: silently ignoring a
// misspelled field would let a client believe it configured something it did not.
func TestUnknownFieldsAreRejected(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/workflows",
		`{"name":"x","version":1,"tasks":[{"name":"a","activity":"noop","maxAttemps":5}]}`, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(raw), "maxAttemps",
		"the rejection must name the offending field so the client can fix it")
	_ = errorCode(t, raw)
}

func TestOversizedBodyIsRejected(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	huge := `{"name":"x","version":1,"description":"` + strings.Repeat("a", 2<<20) + `","tasks":[]}`
	resp, _ := doJSON(t, srv, http.MethodPost, "/v1/workflows", huge, nil)
	require.Contains(t,
		[]int{http.StatusRequestEntityTooLarge, http.StatusBadRequest},
		resp.StatusCode, "an oversized body must be refused, not buffered")
}

func TestInvalidPathParametersAreRejected(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	for _, path := range []string{
		"/v1/executions/not-a-uuid",
		"/v1/executions/not-a-uuid/history",
	} {
		resp, raw := doJSON(t, srv, http.MethodGet, path, "", nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, path)
		require.Equal(t, "bad_request", errorCode(t, raw))
	}

	resp, raw := doJSON(t, srv, http.MethodGet, "/v1/workflows/name/versions/zero", "", nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "bad_request", errorCode(t, raw))
}

func TestQueryParameterBounds(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	for _, q := range []string{"limit=0", "limit=99999", "limit=abc", "offset=-1"} {
		resp, raw := doJSON(t, srv, http.MethodGet, "/v1/executions?"+q, "", nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, q)
		require.Equal(t, "bad_request", errorCode(t, raw))
	}
}

func TestListExecutionsPassesFilters(t *testing.T) {
	svc := &stubService{}
	srv := newTestServer(t, svc)

	resp, _ := doJSON(t, srv, http.MethodGet,
		"/v1/executions?workflowName=order_pipeline&state=running&limit=10&offset=5", "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.Equal(t, "order_pipeline", svc.lastFiler.WorkflowName)
	require.Equal(t, domain.WorkflowRunning, svc.lastFiler.State,
		"a lowercase state filter must be normalized")
	require.Equal(t, 10, svc.lastFiler.Limit)
	require.Equal(t, 5, svc.lastFiler.Offset)
}

func TestListEndpointsReturnEmptyArrays(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	for _, path := range []string{"/v1/workflows", "/v1/executions", "/v1/workers"} {
		resp, raw := doJSON(t, srv, http.MethodGet, path, "", nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, path)

		var list struct {
			Items []json.RawMessage `json:"items"`
			Count int               `json:"count"`
		}
		require.NoError(t, json.Unmarshal(raw, &list))
		require.NotNil(t, list.Items, "%s must return [] rather than null", path)
		require.Zero(t, list.Count)
	}
}

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

// TestIdempotencyKeyHeaderTakesPrecedence lets infrastructure inject a retry key
// without having to rewrite request bodies.
func TestIdempotencyKeyHeaderTakesPrecedence(t *testing.T) {
	svc := &stubService{}
	srv := newTestServer(t, svc)

	_, _ = doJSON(t, srv, http.MethodPost, "/v1/executions",
		`{"workflowName":"w","idempotencyKey":"from-body"}`,
		map[string]string{"Idempotency-Key": "from-header"})
	require.Equal(t, "from-header", svc.lastStart.IdempotencyKey)

	_, _ = doJSON(t, srv, http.MethodPost, "/v1/executions",
		`{"workflowName":"w","idempotencyKey":"from-body"}`, nil)
	require.Equal(t, "from-body", svc.lastStart.IdempotencyKey,
		"the body key must be used when no header is present")

	_, _ = doJSON(t, srv, http.MethodPost, "/v1/executions", `{"workflowName":"w"}`, nil)
	require.Empty(t, svc.lastStart.IdempotencyKey)
}

func TestStartExecutionStatusReflectsCreation(t *testing.T) {
	created := true
	id := uuid.New()
	svc := &stubService{
		startExecution: func(_ context.Context, req engine.StartExecutionRequest) (*domain.WorkflowExecution, bool, error) {
			return &domain.WorkflowExecution{
				ID: id, WorkflowName: req.WorkflowName,
				State: domain.WorkflowPending, Input: json.RawMessage(`{}`),
			}, created, nil
		},
	}
	srv := newTestServer(t, svc)
	body := `{"workflowName":"w","input":{"a":1}}`

	resp, _ := doJSON(t, srv, http.MethodPost, "/v1/executions", body, nil)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "/v1/executions/"+id.String(), resp.Header.Get("Location"))

	// A deduplicated replay must be a success, so a retrying client converges.
	created = false
	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/executions", body, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(raw), id.String())
}

func TestIdempotencyKeyLengthIsBounded(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/executions", `{"workflowName":"w"}`,
		map[string]string{"Idempotency-Key": strings.Repeat("k", 256)})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "bad_request", errorCode(t, raw))
}

// ---------------------------------------------------------------------------
// Task dispatch
// ---------------------------------------------------------------------------

// TestPollTaskReturnsNoContentWhenIdle keeps the steady state of an idle worker
// out of the error path, so "no work" never inflates error metrics.
func TestPollTaskReturnsNoContentWhenIdle(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/tasks/poll", `{"workerId":"w1"}`, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.Empty(t, raw, "204 must carry no body")
}

// TestPollTaskExposesClaimToken checks the one place the token is disclosed:
// holding it is what authorizes reporting a result.
func TestPollTaskExposesClaimToken(t *testing.T) {
	token := uuid.New()
	taskID := uuid.New()
	svc := &stubService{
		pollTask: func(context.Context, engine.PollRequest) (*domain.Task, error) {
			return &domain.Task{
				ID: taskID, ExecutionID: uuid.New(), Name: "task_a", Activity: "noop",
				State: domain.TaskRunning, Input: json.RawMessage(`{"k":1}`),
				Attempt: 1, MaxAttempts: 1, ClaimToken: &token,
			}, nil
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/tasks/poll",
		`{"workerId":"w1","taskQueue":"default","activities":["noop"],"leaseSeconds":30}`, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var task struct {
		ID         string `json:"id"`
		ClaimToken string `json:"claimToken"`
	}
	require.NoError(t, json.Unmarshal(raw, &task))
	require.Equal(t, taskID.String(), task.ID)
	require.Equal(t, token.String(), task.ClaimToken)

	require.Equal(t, "w1", svc.lastPoll.WorkerID)
	require.Equal(t, "default", svc.lastPoll.TaskQueue)
	require.Equal(t, []string{"noop"}, svc.lastPoll.Activities)
	require.Equal(t, 30*time.Second, svc.lastPoll.LeaseDuration)
}

// TestOtherEndpointsNeverExposeClaimToken: leaking a token would let any reader
// of the API report results for someone else's task.
func TestOtherEndpointsNeverExposeClaimToken(t *testing.T) {
	token := uuid.New()
	id := uuid.New()
	svc := &stubService{
		getExecution: func(context.Context, uuid.UUID) (*engine.ExecutionDetail, error) {
			return &engine.ExecutionDetail{
				Execution: &domain.WorkflowExecution{ID: id, State: domain.WorkflowRunning, Input: json.RawMessage(`{}`)},
				Tasks: []domain.Task{{
					ID: uuid.New(), Name: "task_a", State: domain.TaskRunning,
					Input: json.RawMessage(`{}`), ClaimToken: &token,
				}},
			}, nil
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodGet, "/v1/executions/"+id.String(), "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(raw), token.String(),
		"the execution view must never disclose a task's claim token")
	require.Contains(t, string(raw), "task_a")
}

func TestPollTaskValidation(t *testing.T) {
	svc := &stubService{
		pollTask: func(_ context.Context, req engine.PollRequest) (*domain.Task, error) {
			if req.WorkerID == "" {
				return nil, fmt.Errorf("%w: workerId is required", domain.ErrValidation)
			}
			return nil, domain.ErrNotFound
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/tasks/poll", `{"workerId":""}`, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "validation_error", errorCode(t, raw))

	resp, raw = doJSON(t, srv, http.MethodPost, "/v1/tasks/poll", `{"workerId":"w","leaseSeconds":99999}`, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "bad_request", errorCode(t, raw))
}

func TestTaskReportRequiresValidClaimToken(t *testing.T) {
	srv := newTestServer(t, &stubService{})
	taskID := uuid.New()

	for _, path := range []string{
		"/v1/tasks/" + taskID.String() + "/complete",
		"/v1/tasks/" + taskID.String() + "/fail",
	} {
		resp, raw := doJSON(t, srv, http.MethodPost, path, `{"claimToken":""}`, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, path)
		require.Equal(t, "bad_request", errorCode(t, raw))

		resp, raw = doJSON(t, srv, http.MethodPost, path, `{"claimToken":"not-a-uuid"}`, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, path)
		require.Equal(t, "bad_request", errorCode(t, raw))
	}
}

// TestStaleClaimIsReportedDistinctly lets a worker tell "lease lost, give up"
// apart from "transient error, retry".
func TestStaleClaimIsReportedDistinctly(t *testing.T) {
	svc := &stubService{
		completeTask: func(context.Context, uuid.UUID, uuid.UUID, json.RawMessage) (*domain.Task, error) {
			return nil, fmt.Errorf("task reassigned: %w", domain.ErrStaleClaim)
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodPost,
		"/v1/tasks/"+uuid.New().String()+"/complete",
		fmt.Sprintf(`{"claimToken":%q,"output":{"ok":true}}`, uuid.New()), nil)
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	require.Equal(t, "stale_claim", errorCode(t, raw))
}

func TestCompleteAndFailTaskHappyPath(t *testing.T) {
	srv := newTestServer(t, &stubService{})
	taskID := uuid.New()
	token := uuid.New()

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/tasks/"+taskID.String()+"/complete",
		fmt.Sprintf(`{"claimToken":%q,"output":{"receipt":"r1"}}`, token), nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(raw), `"state":"COMPLETED"`)
	require.Contains(t, string(raw), `"receipt":"r1"`)

	resp, raw = doJSON(t, srv, http.MethodPost, "/v1/tasks/"+taskID.String()+"/fail",
		fmt.Sprintf(`{"claimToken":%q,"error":"upstream timeout"}`, token), nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(raw), `"state":"FAILED"`)
	require.Contains(t, string(raw), "upstream timeout")
}

// ---------------------------------------------------------------------------
// Cancellation and routing
// ---------------------------------------------------------------------------

func TestCancelExecutionAcceptsOptionalBody(t *testing.T) {
	var captured string
	svc := &stubService{
		cancel: func(_ context.Context, id uuid.UUID, reason string) (*domain.WorkflowExecution, error) {
			captured = reason
			return &domain.WorkflowExecution{ID: id, State: domain.WorkflowCanceled, Input: json.RawMessage(`{}`)}, nil
		},
	}
	srv := newTestServer(t, svc)
	path := "/v1/executions/" + uuid.New().String() + "/cancel"

	resp, _ := doJSON(t, srv, http.MethodPost, path, `{"reason":"operator request"}`, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "operator request", captured)

	// A bodyless cancel is valid; the service supplies the default reason.
	resp, _ = doJSON(t, srv, http.MethodPost, path, "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, captured)
}

func TestMethodAndRouteMismatchesAreRejected(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	resp, _ := doJSON(t, srv, http.MethodDelete, "/v1/workflows", "", nil)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)

	resp, _ = doJSON(t, srv, http.MethodGet, "/v1/tasks/poll", "", nil)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)

	resp, _ = doJSON(t, srv, http.MethodGet, "/v1/nonexistent", "", nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestWorkerRegistrationAndHeartbeat(t *testing.T) {
	var captured store.RegisterWorkerParams
	svc := &stubService{
		registerWorker: func(_ context.Context, p store.RegisterWorkerParams) (*domain.Worker, error) {
			captured = p
			return &domain.Worker{ID: uuid.New(), Name: p.Name, TaskQueue: p.TaskQueue,
				Activities: p.Activities, State: domain.WorkerActive}, nil
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/workers",
		`{"name":"  worker-1  ","taskQueue":" default ","activities":["noop","charge_payment"]}`, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "worker-1", captured.Name, "worker names must be trimmed")
	require.Equal(t, "default", captured.TaskQueue)
	require.Len(t, captured.Activities, 2)
	require.Contains(t, string(raw), "worker-1")

	resp, _ = doJSON(t, srv, http.MethodPost, "/v1/workers/"+uuid.New().String()+"/heartbeat", "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, raw = doJSON(t, srv, http.MethodPost, "/v1/workers/not-a-uuid/heartbeat", "", nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "bad_request", errorCode(t, raw))
}

func TestHistoryEndpointReturnsEnvelope(t *testing.T) {
	id := uuid.New()
	var capturedAfter int64
	svc := &stubService{
		getHistory: func(_ context.Context, _ uuid.UUID, afterID int64, _ int) ([]domain.HistoryEvent, error) {
			capturedAfter = afterID
			return []domain.HistoryEvent{
				{ID: 7, ExecutionID: id, EventType: domain.EventWorkflowStarted},
			}, nil
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodGet, "/v1/executions/"+id.String()+"/history?afterId=6", "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, int64(6), capturedAfter, "the tailing cursor must be forwarded")

	var body struct {
		ExecutionID string                `json:"executionId"`
		Events      []domain.HistoryEvent `json:"events"`
	}
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Equal(t, id.String(), body.ExecutionID)
	require.Len(t, body.Events, 1)
	require.Equal(t, domain.EventWorkflowStarted, body.Events[0].EventType)
}

// TestPanicIsContained matters for a control plane: one malformed request must
// not take down the process and drop every worker's in-flight poll.
func TestPanicIsContained(t *testing.T) {
	svc := &stubService{
		listWorkflows: func(context.Context, string, int) ([]domain.WorkflowDefinition, error) {
			panic("boom")
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodGet, "/v1/workflows", "", nil)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Equal(t, "internal_error", errorCode(t, raw))

	// The server must still be serving afterwards.
	resp, _ = doJSON(t, srv, http.MethodGet, "/healthz", "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestResponsesAreJSON(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	resp, _ := doJSON(t, srv, http.MethodGet, "/healthz", "", nil)
	require.Contains(t, resp.Header.Get("Content-Type"), "application/json")

	resp, _ = doJSON(t, srv, http.MethodGet, "/v1/executions/bad", "", nil)
	require.Contains(t, resp.Header.Get("Content-Type"), "application/json",
		"error responses must be JSON too")
}
