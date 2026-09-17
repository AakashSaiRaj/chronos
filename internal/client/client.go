// Package client is the Go SDK for the Chronos control plane.
//
// Workers never touch PostgreSQL directly; they reach the engine only through
// this client. Keeping the data plane behind the API means task leasing,
// idempotency, and history stay enforced in one place, and a worker needs no
// database credentials.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/domain"
)

// Errors returned by the client.
var (
	// ErrNoTask means the queue had nothing eligible. It is the normal reply to
	// a poll on an idle queue, not a failure.
	ErrNoTask = errors.New("no task available")
	// ErrStaleClaim means the server rejected a report because the worker no
	// longer holds the task's lease. The worker must abandon the task.
	ErrStaleClaim = errors.New("stale task claim")
)

// APIError is a structured non-2xx response.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
	RequestID  string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("chronos api: %d %s: %s", e.StatusCode, e.Code, e.Message)
}

// Retryable reports whether repeating the request could plausibly succeed.
func (e *APIError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests ||
		e.StatusCode >= http.StatusInternalServerError
}

// Config configures a Client.
type Config struct {
	BaseURL string
	// Timeout bounds a single HTTP attempt.
	Timeout time.Duration
	// MaxRetries bounds retries of retryable failures. Every retried operation
	// is idempotent by design, which is what makes blind retries safe.
	MaxRetries int
	Logger     *slog.Logger
	HTTPClient *http.Client
}

// Client talks to a Chronos server.
type Client struct {
	baseURL    string
	http       *http.Client
	maxRetries int
	logger     *slog.Logger
}

// New constructs a Client.
func New(cfg Config) (*Client, error) {
	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		return nil, errors.New("chronos client: BaseURL is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				// Workers poll continuously, so connection reuse matters more
				// here than in a typical client.
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     90 * time.Second,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
			},
		}
	}

	return &Client{
		baseURL:    base,
		http:       httpClient,
		maxRetries: cfg.MaxRetries,
		logger:     cfg.Logger,
	}, nil
}

// ---------------------------------------------------------------------------
// Workflows and executions
// ---------------------------------------------------------------------------

// WorkflowResponse mirrors the API's workflow representation.
type WorkflowResponse struct {
	ID          uuid.UUID         `json:"id"`
	Name        string            `json:"name"`
	Version     int               `json:"version"`
	Description string            `json:"description,omitempty"`
	TaskQueue   string            `json:"taskQueue"`
	Tasks       []domain.TaskSpec `json:"tasks"`
	SpecHash    string            `json:"specHash"`
	CreatedAt   time.Time         `json:"createdAt"`
}

// RegisterWorkflow registers a workflow version. Safe to call on every startup:
// re-registering an identical spec is a no-op server-side.
func (c *Client) RegisterWorkflow(ctx context.Context, spec domain.WorkflowSpec) (*WorkflowResponse, error) {
	var out WorkflowResponse
	if err := c.do(ctx, http.MethodPost, "/v1/workflows", nil, spec, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetWorkflow fetches a workflow version, or the latest when version <= 0.
func (c *Client) GetWorkflow(ctx context.Context, name string, version int) (*WorkflowResponse, error) {
	path := "/v1/workflows/" + name
	if version > 0 {
		path = fmt.Sprintf("/v1/workflows/%s/versions/%d", name, version)
	}
	var out WorkflowResponse
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// StartExecutionRequest starts a workflow run.
type StartExecutionRequest struct {
	WorkflowName    string          `json:"workflowName"`
	WorkflowVersion int             `json:"workflowVersion,omitempty"`
	Input           json.RawMessage `json:"input,omitempty"`
	IdempotencyKey  string          `json:"idempotencyKey,omitempty"`
}

// ExecutionResponse mirrors the API's execution representation.
type ExecutionResponse struct {
	ID              uuid.UUID            `json:"id"`
	WorkflowName    string               `json:"workflowName"`
	WorkflowVersion int                  `json:"workflowVersion"`
	State           domain.WorkflowState `json:"state"`
	TaskQueue       string               `json:"taskQueue"`
	Input           json.RawMessage      `json:"input"`
	Output          json.RawMessage      `json:"output,omitempty"`
	Error           string               `json:"error,omitempty"`
	IdempotencyKey  string               `json:"idempotencyKey,omitempty"`
	CreatedAt       time.Time            `json:"createdAt"`
	UpdatedAt       time.Time            `json:"updatedAt"`
	StartedAt       *time.Time           `json:"startedAt,omitempty"`
	CompletedAt     *time.Time           `json:"completedAt,omitempty"`
	Tasks           []TaskResponse       `json:"tasks,omitempty"`
}

// StartExecution durably enqueues a workflow run.
func (c *Client) StartExecution(ctx context.Context, req StartExecutionRequest) (*ExecutionResponse, error) {
	headers := map[string]string{}
	if req.IdempotencyKey != "" {
		headers["Idempotency-Key"] = req.IdempotencyKey
	}
	var out ExecutionResponse
	if err := c.do(ctx, http.MethodPost, "/v1/executions", headers, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetExecution fetches an execution and its tasks.
func (c *Client) GetExecution(ctx context.Context, id uuid.UUID) (*ExecutionResponse, error) {
	var out ExecutionResponse
	if err := c.do(ctx, http.MethodGet, "/v1/executions/"+id.String(), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CancelExecution stops an execution.
func (c *Client) CancelExecution(ctx context.Context, id uuid.UUID, reason string) (*ExecutionResponse, error) {
	var out ExecutionResponse
	body := map[string]string{"reason": reason}
	if err := c.do(ctx, http.MethodPost, "/v1/executions/"+id.String()+"/cancel", nil, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// HistoryResponse is an execution's append-only history.
type HistoryResponse struct {
	ExecutionID uuid.UUID             `json:"executionId"`
	Events      []domain.HistoryEvent `json:"events"`
}

// GetHistory fetches execution history after the given event id cursor.
func (c *Client) GetHistory(ctx context.Context, id uuid.UUID, afterID int64) (*HistoryResponse, error) {
	path := fmt.Sprintf("/v1/executions/%s/history?afterId=%d", id, afterID)
	var out HistoryResponse
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ---------------------------------------------------------------------------
// Worker registration and task dispatch
// ---------------------------------------------------------------------------

// RegisterWorker announces a worker and the activities it serves.
func (c *Client) RegisterWorker(ctx context.Context, name, taskQueue string, activities []string) (*domain.Worker, error) {
	body := map[string]any{"name": name, "taskQueue": taskQueue, "activities": activities}
	var out domain.Worker
	if err := c.do(ctx, http.MethodPost, "/v1/workers", nil, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Heartbeat refreshes a worker's liveness.
func (c *Client) Heartbeat(ctx context.Context, workerID uuid.UUID) error {
	return c.do(ctx, http.MethodPost, "/v1/workers/"+workerID.String()+"/heartbeat", nil, nil, nil)
}

// TaskResponse is a task as returned by the API. ClaimToken is populated only on
// the poll response.
type TaskResponse struct {
	ID             uuid.UUID        `json:"id"`
	ExecutionID    uuid.UUID        `json:"executionId"`
	Name           string           `json:"name"`
	Activity       string           `json:"activity"`
	State          domain.TaskState `json:"state"`
	DependsOn      []string         `json:"dependsOn,omitempty"`
	Input          json.RawMessage  `json:"input"`
	Output         json.RawMessage  `json:"output,omitempty"`
	Error          string           `json:"error,omitempty"`
	Attempt        int              `json:"attempt"`
	MaxAttempts    int              `json:"maxAttempts"`
	TimeoutSeconds int              `json:"timeoutSeconds,omitempty"`
	TaskQueue      string           `json:"taskQueue"`
	WorkerID       string           `json:"workerId,omitempty"`
	ClaimToken     string           `json:"claimToken,omitempty"`
	LeaseExpiresAt *time.Time       `json:"leaseExpiresAt,omitempty"`
	ScheduledAt    *time.Time       `json:"scheduledAt,omitempty"`
	CreatedAt      time.Time        `json:"createdAt"`
	UpdatedAt      time.Time        `json:"updatedAt"`
	StartedAt      *time.Time       `json:"startedAt,omitempty"`
	CompletedAt    *time.Time       `json:"completedAt,omitempty"`
}

// PollRequest asks for the next task.
type PollRequest struct {
	WorkerID     string   `json:"workerId"`
	TaskQueue    string   `json:"taskQueue,omitempty"`
	Activities   []string `json:"activities,omitempty"`
	LeaseSeconds int      `json:"leaseSeconds,omitempty"`
}

// PollTask leases the next eligible task, or returns ErrNoTask when the queue is
// empty. An empty queue is signalled by 204 No Content.
func (c *Client) PollTask(ctx context.Context, req PollRequest) (*TaskResponse, error) {
	var out TaskResponse
	status, err := c.doRaw(ctx, http.MethodPost, "/v1/tasks/poll", nil, req, &out)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNoContent {
		return nil, ErrNoTask
	}
	return &out, nil
}

// CompleteTask reports a successful attempt. The claim token makes this
// idempotent, so a retry after a lost response is safe.
func (c *Client) CompleteTask(ctx context.Context, taskID uuid.UUID, claimToken string, output json.RawMessage) (*TaskResponse, error) {
	body := map[string]any{"claimToken": claimToken, "output": output}
	var out TaskResponse
	if err := c.do(ctx, http.MethodPost, "/v1/tasks/"+taskID.String()+"/complete", nil, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FailRequest reports a failed attempt.
type FailRequest struct {
	ClaimToken string `json:"claimToken"`
	Error      string `json:"error"`
	// Retryable, when set to false, sends the task straight to the dead letter
	// queue instead of consuming its remaining attempts on a failure the worker
	// already knows is permanent.
	Retryable *bool `json:"retryable,omitempty"`
	// Reason categorizes the failure: ACTIVITY_ERROR (default) or TIMEOUT.
	Reason string `json:"reason,omitempty"`
}

// FailTask reports a failed attempt.
func (c *Client) FailTask(ctx context.Context, taskID uuid.UUID, req FailRequest) (*TaskResponse, error) {
	var out TaskResponse
	if err := c.do(ctx, http.MethodPost, "/v1/tasks/"+taskID.String()+"/fail", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// HeartbeatTaskResponse is the control plane's answer to a lease renewal.
type HeartbeatTaskResponse struct {
	TaskID uuid.UUID `json:"taskId"`
	// CancelRequested means the worker should abandon the task. Its result would
	// be rejected anyway, so continuing only wastes effort.
	CancelRequested bool       `json:"cancelRequested"`
	LeaseExpiresAt  *time.Time `json:"leaseExpiresAt,omitempty"`
	Attempt         int        `json:"attempt"`
}

// HeartbeatTask renews a task lease while the activity runs, and learns whether
// cancellation has been requested.
//
// Returns ErrStaleClaim once the lease is gone — reaped and reassigned, or
// canceled. That is a terminal answer: the worker must stop, because anything it
// reports will be refused.
func (c *Client) HeartbeatTask(ctx context.Context, taskID uuid.UUID, claimToken string, leaseSeconds int) (*HeartbeatTaskResponse, error) {
	body := map[string]any{"claimToken": claimToken, "leaseSeconds": leaseSeconds}
	var out HeartbeatTaskResponse
	if err := c.do(ctx, http.MethodPost, "/v1/tasks/"+taskID.String()+"/heartbeat", nil, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeadLetterItem is a parked task plus triage context.
type DeadLetterItem struct {
	Task             TaskResponse `json:"task"`
	FailureReason    string       `json:"failureReason,omitempty"`
	DeadLetteredAt   *time.Time   `json:"deadLetteredAt,omitempty"`
	LeaseExpiryCount int          `json:"leaseExpiryCount"`
}

// DeadLetterResponse is the dead letter queue listing.
type DeadLetterResponse struct {
	Items []DeadLetterItem `json:"items"`
	Count int              `json:"count"`
	Total int              `json:"total"`
}

// ListDeadLetter returns tasks parked for operator attention.
func (c *Client) ListDeadLetter(ctx context.Context, workflowName string, limit int) (*DeadLetterResponse, error) {
	path := "/v1/dead-letter"
	query := url.Values{}
	if workflowName != "" {
		query.Set("workflowName", workflowName)
	}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if len(query) > 0 {
		path += "?" + query.Encode()
	}

	var out DeadLetterResponse
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ReplayTask returns a dead-lettered task to the queue, reviving its workflow.
func (c *Client) ReplayTask(ctx context.Context, taskID uuid.UUID, extraAttempts int) (*TaskResponse, error) {
	body := map[string]any{"extraAttempts": extraAttempts}
	var out TaskResponse
	if err := c.do(ctx, http.MethodPost, "/v1/tasks/"+taskID.String()+"/replay", nil, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Ready reports whether the server is ready to serve traffic.
func (c *Client) Ready(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/readyz", nil, nil, nil)
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

func (c *Client) do(ctx context.Context, method, path string, headers map[string]string, in, out any) error {
	_, err := c.doRaw(ctx, method, path, headers, in, out)
	return err
}

// doRaw performs a request with bounded retries on transient failures.
//
// Retrying is only safe because every mutating endpoint is idempotent: workflow
// registration keys on the spec hash, execution starts key on the idempotency
// key, and task reports key on the claim token. That property is what lets the
// client recover from a lost response without risking a duplicate side effect.
func (c *Client) doRaw(ctx context.Context, method, path string, headers map[string]string, in, out any) (int, error) {
	var body []byte
	if in != nil {
		encoded, err := json.Marshal(in)
		if err != nil {
			return 0, fmt.Errorf("chronos client: encode request: %w", err)
		}
		body = encoded
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			if err := sleepWithContext(ctx, backoff(attempt)); err != nil {
				return 0, err
			}
		}

		status, err := c.attempt(ctx, method, path, headers, body, out)
		if err == nil {
			return status, nil
		}
		lastErr = err

		if !isRetryable(err) || ctx.Err() != nil {
			return status, err
		}
		c.logger.Debug("retrying chronos request",
			"method", method, "path", path, "attempt", attempt+1, "error", err)
	}
	return 0, lastErr
}

func (c *Client) attempt(ctx context.Context, method, path string, headers map[string]string, body []byte, out any) (int, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, fmt.Errorf("chronos client: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("chronos client: %s %s: %w", method, path, err)
	}
	defer func() {
		// Drain before closing so the connection can be reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 400 {
		return resp.StatusCode, parseAPIError(resp)
	}
	if resp.StatusCode == http.StatusNoContent || out == nil {
		return resp.StatusCode, nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return resp.StatusCode, fmt.Errorf("chronos client: decode %s %s response: %w", method, path, err)
	}
	return resp.StatusCode, nil
}

func parseAPIError(resp *http.Response) error {
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"requestId"`
		} `json:"error"`
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	_ = json.Unmarshal(raw, &envelope)

	apiErr := &APIError{
		StatusCode: resp.StatusCode,
		Code:       envelope.Error.Code,
		Message:    envelope.Error.Message,
		RequestID:  envelope.Error.RequestID,
	}
	if apiErr.Message == "" {
		apiErr.Message = strings.TrimSpace(string(raw))
		if apiErr.Message == "" {
			apiErr.Message = resp.Status
		}
	}

	// Surface a lost lease as a distinct sentinel so a worker can stop working
	// on the task instead of retrying a report that can never succeed.
	if apiErr.Code == "stale_claim" {
		return fmt.Errorf("%w: %s", ErrStaleClaim, apiErr.Message)
	}
	return apiErr
}

func isRetryable(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Retryable()
	}
	if errors.Is(err, ErrStaleClaim) || errors.Is(err, ErrNoTask) {
		return false
	}
	// Transport-level failures (connection refused, reset, timeout) are worth
	// another attempt; the server may simply be rolling.
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		isNetworkError(err)
}

func isNetworkError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

// backoff returns an exponentially increasing delay with jitter, so a fleet of
// workers that all lose the server does not reconnect in lockstep.
func backoff(attempt int) time.Duration {
	const (
		base = 100 * time.Millisecond
		max  = 5 * time.Second
	)
	d := time.Duration(math.Pow(2, float64(attempt-1))) * base
	if d > max {
		d = max
	}
	// Full jitter over [d/2, d].
	jitter := time.Duration(rand.Int64N(int64(d/2) + 1))
	return d/2 + jitter
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
