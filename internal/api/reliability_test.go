package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/engine"
	"github.com/AakashSaiRaj/chronos/internal/store"
)

// ---------------------------------------------------------------------------
// Retryable failures
// ---------------------------------------------------------------------------

// TestFailTaskDefaultsToRetryable is the safe default: assuming a failure is
// permanent would silently skip the retry policy the workflow author configured.
func TestFailTaskDefaultsToRetryable(t *testing.T) {
	svc := &stubService{}
	srv := newTestServer(t, svc)

	resp, _ := doJSON(t, srv, http.MethodPost, "/v1/tasks/"+uuid.New().String()+"/fail",
		fmt.Sprintf(`{"claimToken":%q,"error":"transient"}`, uuid.New()), nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.True(t, svc.lastFail.Retryable,
		"omitting retryable must mean retryable, not permanent")
	require.Equal(t, domain.FailureActivityError, svc.lastFail.Reason)
	require.Equal(t, "transient", svc.lastFail.Error)
}

func TestFailTaskHonoursExplicitPermanence(t *testing.T) {
	svc := &stubService{}
	srv := newTestServer(t, svc)

	resp, _ := doJSON(t, srv, http.MethodPost, "/v1/tasks/"+uuid.New().String()+"/fail",
		fmt.Sprintf(`{"claimToken":%q,"error":"bad input","retryable":false}`, uuid.New()), nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.False(t, svc.lastFail.Retryable)
}

func TestFailTaskAcceptsTimeoutReason(t *testing.T) {
	svc := &stubService{}
	srv := newTestServer(t, svc)

	resp, _ := doJSON(t, srv, http.MethodPost, "/v1/tasks/"+uuid.New().String()+"/fail",
		fmt.Sprintf(`{"claimToken":%q,"error":"too slow","reason":"timeout"}`, uuid.New()), nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, domain.FailureTimeout, svc.lastFail.Reason,
		"a lowercase reason must be normalized")
}

// TestFailTaskRejectsEngineOwnedReasons: a worker claiming its own lease expired
// would be reporting on its own death, which is a conclusion only the control
// plane can draw.
func TestFailTaskRejectsEngineOwnedReasons(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	for _, reason := range []string{"LEASE_EXPIRED", "WORKER_DEAD"} {
		resp, raw := doJSON(t, srv, http.MethodPost, "/v1/tasks/"+uuid.New().String()+"/fail",
			fmt.Sprintf(`{"claimToken":%q,"error":"x","reason":%q}`, uuid.New(), reason), nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, reason)
		require.Equal(t, "bad_request", errorCode(t, raw))
	}

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/tasks/"+uuid.New().String()+"/fail",
		fmt.Sprintf(`{"claimToken":%q,"error":"x","reason":"NONSENSE"}`, uuid.New()), nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(raw), "ACTIVITY_ERROR")
	_ = errorCode(t, raw)
}

// ---------------------------------------------------------------------------
// Task lease heartbeat
// ---------------------------------------------------------------------------

func TestHeartbeatTaskRenewsLease(t *testing.T) {
	expiry := time.Now().Add(45 * time.Second)
	taskID := uuid.New()
	svc := &stubService{
		heartbeatTask: func(context.Context, uuid.UUID, uuid.UUID, time.Duration) (*engine.LeaseHeartbeat, error) {
			return &engine.LeaseHeartbeat{
				Task:           &domain.Task{ID: taskID, State: domain.TaskRunning, Attempt: 2},
				LeaseExpiresAt: &expiry,
			}, nil
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/tasks/"+taskID.String()+"/heartbeat",
		fmt.Sprintf(`{"claimToken":%q,"leaseSeconds":45}`, uuid.New()), nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		TaskID          string     `json:"taskId"`
		CancelRequested bool       `json:"cancelRequested"`
		LeaseExpiresAt  *time.Time `json:"leaseExpiresAt"`
		Attempt         int        `json:"attempt"`
	}
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Equal(t, taskID.String(), body.TaskID)
	require.False(t, body.CancelRequested)
	require.NotNil(t, body.LeaseExpiresAt)
	require.Equal(t, 2, body.Attempt)
	require.Equal(t, 45*time.Second, svc.lastHeartbeatD)
}

// TestHeartbeatRelaysCancellation is the cooperative-cancellation channel: the
// worker is already checking in, so telling it to stop costs no extra round trip.
func TestHeartbeatRelaysCancellation(t *testing.T) {
	svc := &stubService{
		heartbeatTask: func(context.Context, uuid.UUID, uuid.UUID, time.Duration) (*engine.LeaseHeartbeat, error) {
			return &engine.LeaseHeartbeat{
				Task:            &domain.Task{ID: uuid.New(), State: domain.TaskRunning, Attempt: 1},
				CancelRequested: true,
			}, nil
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/tasks/"+uuid.New().String()+"/heartbeat",
		fmt.Sprintf(`{"claimToken":%q}`, uuid.New()), nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(raw), `"cancelRequested":true`)
}

// TestHeartbeatReportsLostLeaseAsStaleClaim tells a worker to abandon the task
// rather than finish work whose result will be refused.
func TestHeartbeatReportsLostLeaseAsStaleClaim(t *testing.T) {
	svc := &stubService{
		heartbeatTask: func(context.Context, uuid.UUID, uuid.UUID, time.Duration) (*engine.LeaseHeartbeat, error) {
			return nil, fmt.Errorf("reaped: %w", domain.ErrStaleClaim)
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/tasks/"+uuid.New().String()+"/heartbeat",
		fmt.Sprintf(`{"claimToken":%q}`, uuid.New()), nil)
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	require.Equal(t, "stale_claim", errorCode(t, raw))
}

func TestHeartbeatValidation(t *testing.T) {
	srv := newTestServer(t, &stubService{})
	path := "/v1/tasks/" + uuid.New().String() + "/heartbeat"

	resp, raw := doJSON(t, srv, http.MethodPost, path, `{"claimToken":""}`, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "bad_request", errorCode(t, raw))

	resp, raw = doJSON(t, srv, http.MethodPost, path,
		fmt.Sprintf(`{"claimToken":%q,"leaseSeconds":99999}`, uuid.New()), nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "bad_request", errorCode(t, raw))

	resp, raw = doJSON(t, srv, http.MethodPost, "/v1/tasks/not-a-uuid/heartbeat",
		fmt.Sprintf(`{"claimToken":%q}`, uuid.New()), nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "bad_request", errorCode(t, raw))
}

// ---------------------------------------------------------------------------
// Dead letter queue
// ---------------------------------------------------------------------------

func TestListDeadLetterReportsPageAndTotal(t *testing.T) {
	parkedAt := time.Now()
	svc := &stubService{
		listDeadLetter: func(_ context.Context, f store.DeadLetterFilter) ([]domain.Task, error) {
			return []domain.Task{{
				ID: uuid.New(), Name: "task_b", Activity: "reserve_inventory",
				State: domain.TaskDeadLetter, Input: json.RawMessage(`{}`),
				Error: "inventory service down", LastFailureReason: "ACTIVITY_ERROR",
				DeadLetteredAt: &parkedAt, LeaseExpiryCount: 2,
				Attempt: 3, MaxAttempts: 3,
			}}, nil
		},
		countDeadLtr: func(context.Context) (int, error) { return 17, nil },
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodGet,
		"/v1/dead-letter?workflowName=order_pipeline&activity=reserve_inventory&limit=10&offset=5", "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Items []struct {
			Task struct {
				Name  string `json:"name"`
				State string `json:"state"`
			} `json:"task"`
			FailureReason    string `json:"failureReason"`
			LeaseExpiryCount int    `json:"leaseExpiryCount"`
		} `json:"items"`
		Count int `json:"count"`
		Total int `json:"total"`
	}
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Len(t, body.Items, 1)
	require.Equal(t, 1, body.Count)
	require.Equal(t, 17, body.Total,
		"the total backlog must be visible without paging through it")
	require.Equal(t, "task_b", body.Items[0].Task.Name)
	require.Equal(t, "DEAD_LETTER", body.Items[0].Task.State)
	require.Equal(t, "ACTIVITY_ERROR", body.Items[0].FailureReason)
	require.Equal(t, 2, body.Items[0].LeaseExpiryCount)

	require.Equal(t, "order_pipeline", svc.lastDLQFilter.WorkflowName)
	require.Equal(t, "reserve_inventory", svc.lastDLQFilter.Activity)
	require.Equal(t, 10, svc.lastDLQFilter.Limit)
	require.Equal(t, 5, svc.lastDLQFilter.Offset)
}

func TestListDeadLetterReturnsEmptyArray(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	resp, raw := doJSON(t, srv, http.MethodGet, "/v1/dead-letter", "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Items []json.RawMessage `json:"items"`
		Count int               `json:"count"`
		Total int               `json:"total"`
	}
	require.NoError(t, json.Unmarshal(raw, &body))
	require.NotNil(t, body.Items, "an empty queue must return [] rather than null")
	require.Zero(t, body.Count)
}

func TestListDeadLetterBoundsQueryParameters(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	for _, q := range []string{"limit=0", "limit=9999", "offset=-1", "limit=abc"} {
		resp, raw := doJSON(t, srv, http.MethodGet, "/v1/dead-letter?"+q, "", nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, q)
		require.Equal(t, "bad_request", errorCode(t, raw))
	}
}

// TestDeadLetterViewNeverExposesClaimToken: a token in a widely-read operator view
// would let any reader report results for someone else's task.
func TestDeadLetterViewNeverExposesClaimToken(t *testing.T) {
	token := uuid.New()
	parkedAt := time.Now()
	svc := &stubService{
		listDeadLetter: func(context.Context, store.DeadLetterFilter) ([]domain.Task, error) {
			return []domain.Task{{
				ID: uuid.New(), Name: "task_a", State: domain.TaskDeadLetter,
				Input: json.RawMessage(`{}`), ClaimToken: &token, DeadLetteredAt: &parkedAt,
			}}, nil
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodGet, "/v1/dead-letter", "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(raw), token.String())
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

func TestReplayTaskDefaultsAndOverrides(t *testing.T) {
	svc := &stubService{}
	srv := newTestServer(t, svc)
	path := "/v1/tasks/" + uuid.New().String() + "/replay"

	// A bodyless replay is valid and uses the default grant.
	resp, _ := doJSON(t, srv, http.MethodPost, path, "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Zero(t, svc.lastReplayN, "the service applies the default, not the handler")

	resp, raw := doJSON(t, srv, http.MethodPost, path, `{"extraAttempts":3}`, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 3, svc.lastReplayN)
	require.Contains(t, string(raw), `"state":"SCHEDULED"`)
}

func TestReplayTaskValidation(t *testing.T) {
	srv := newTestServer(t, &stubService{})
	path := "/v1/tasks/" + uuid.New().String() + "/replay"

	for _, body := range []string{`{"extraAttempts":-1}`, `{"extraAttempts":9999}`} {
		resp, raw := doJSON(t, srv, http.MethodPost, path, body, nil)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, body)
		require.Equal(t, "bad_request", errorCode(t, raw))
	}

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/tasks/nope/replay", "", nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "bad_request", errorCode(t, raw))
}

// TestReplayRejectsNonParkedTaskAsConflict tells the caller the task's state is
// wrong, rather than that the request was malformed.
func TestReplayRejectsNonParkedTaskAsConflict(t *testing.T) {
	svc := &stubService{
		replayTask: func(context.Context, uuid.UUID, int) (*domain.Task, error) {
			return nil, fmt.Errorf("task is RUNNING: %w", domain.ErrConflict)
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodPost,
		"/v1/tasks/"+uuid.New().String()+"/replay", "", nil)
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	require.Equal(t, "conflict", errorCode(t, raw))
}

// ---------------------------------------------------------------------------
// Task representation
// ---------------------------------------------------------------------------

// TestTaskResponseCarriesReliabilityFields: an operator diagnosing a workflow
// needs to tell a lost worker apart from a failing activity, which the error
// string alone does not convey.
func TestTaskResponseCarriesReliabilityFields(t *testing.T) {
	id := uuid.New()
	parkedAt := time.Now()
	svc := &stubService{
		getExecution: func(context.Context, uuid.UUID) (*engine.ExecutionDetail, error) {
			return &engine.ExecutionDetail{
				Execution: &domain.WorkflowExecution{
					ID: id, State: domain.WorkflowFailed, Input: json.RawMessage(`{}`),
				},
				Tasks: []domain.Task{{
					ID: uuid.New(), Name: "task_a", State: domain.TaskDeadLetter,
					Input: json.RawMessage(`{}`), Attempt: 3, MaxAttempts: 3,
					Retryable: false, LeaseExpiryCount: 2, MaxLeaseExpiries: 3,
					LastFailureReason: "WORKER_DEAD", DeadLetteredAt: &parkedAt,
				}},
			}, nil
		},
	}
	srv := newTestServer(t, svc)

	resp, raw := doJSON(t, srv, http.MethodGet, "/v1/executions/"+id.String(), "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := string(raw)
	require.Contains(t, body, `"state":"DEAD_LETTER"`)
	require.Contains(t, body, `"retryable":false`)
	require.Contains(t, body, `"leaseExpiryCount":2`)
	require.Contains(t, body, `"maxLeaseExpiries":3`)
	require.Contains(t, body, `"lastFailureReason":"WORKER_DEAD"`)
	require.Contains(t, body, `"deadLetteredAt"`)
}

// TestRegisterWorkflowAcceptsRetryPolicy pins the wire contract for configuring
// reliability on a workflow.
func TestRegisterWorkflowAcceptsRetryPolicy(t *testing.T) {
	var captured domain.WorkflowSpec
	svc := &stubService{
		registerWorkflow: func(_ context.Context, spec domain.WorkflowSpec) (*domain.WorkflowDefinition, bool, error) {
			captured = spec
			return &domain.WorkflowDefinition{
				ID: uuid.New(), Name: spec.Name, Version: spec.Version, Spec: spec,
			}, true, nil
		},
	}
	srv := newTestServer(t, svc)

	resp, _ := doJSON(t, srv, http.MethodPost, "/v1/workflows", `{
		"name":"reliable","version":1,
		"tasks":[{
			"name":"task_a","activity":"charge_payment",
			"maxAttempts":5,"timeoutSeconds":30,"maxLeaseExpiries":2,
			"retryPolicy":{
				"initialIntervalMs":500,"backoffCoefficient":3,
				"maxIntervalMs":30000,"jitterPercent":0
			}
		}]}`, nil)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	require.Len(t, captured.Tasks, 1)
	task := captured.Tasks[0]
	require.Equal(t, 5, task.MaxAttempts)
	require.Equal(t, 30, task.TimeoutSeconds)
	require.Equal(t, 2, task.MaxLeaseExpiries)
	require.NotNil(t, task.RetryPolicy)
	require.Equal(t, 500, task.RetryPolicy.InitialIntervalMS)
	require.Equal(t, 3.0, task.RetryPolicy.BackoffCoefficient)
	require.Equal(t, 30_000, task.RetryPolicy.MaxIntervalMS)
	require.NotNil(t, task.RetryPolicy.JitterPercent)
	require.Zero(t, *task.RetryPolicy.JitterPercent,
		"an explicit zero jitter must survive the wire, not be treated as unset")
}

func TestRegisterWorkflowRejectsUnknownRetryFields(t *testing.T) {
	srv := newTestServer(t, &stubService{})

	resp, raw := doJSON(t, srv, http.MethodPost, "/v1/workflows", `{
		"name":"typo","version":1,
		"tasks":[{"name":"task_a","activity":"noop",
			"retryPolicy":{"initialIntervalMillis":500}}]}`, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(raw), "initialIntervalMillis",
		"a misspelled retry field must be named in the rejection")
	_ = errorCode(t, raw)
}
