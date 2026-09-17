package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/domain"
	"github.com/AakashSaiRaj/chronos/internal/engine"
	"github.com/AakashSaiRaj/chronos/internal/store"
)

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

// handleLive reports process liveness. It deliberately does not touch the
// database: a database outage must not cause orchestrators to kill otherwise
// healthy pods, which would only add churn to an already degraded system.
func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, s.logger, http.StatusOK, healthResponse{Status: "ok", Detail: s.version})
}

// handleReady reports whether this instance can serve traffic, which requires a
// reachable database.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	// Bound the probe so a hung database turns into a fast "not ready" rather
	// than a stuck request that the orchestrator times out on its own terms.
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.svc.Ping(ctx); err != nil {
		s.logger.Warn("readiness check failed", "error", err)
		writeJSON(w, r, s.logger, http.StatusServiceUnavailable, healthResponse{
			Status:   "unavailable",
			Database: "unreachable",
			Detail:   "database is not reachable",
		})
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, healthResponse{
		Status: "ok", Database: "reachable", Detail: s.version,
	})
}

// ---------------------------------------------------------------------------
// Workflow definitions
// ---------------------------------------------------------------------------

// handleRegisterWorkflow registers a workflow version.
//
// Returns 201 when the version is newly created and 200 when an identical spec
// was already registered, so a client that retries a registration cannot tell
// the difference in outcome but can still see whether it was the creator.
func (s *Server) handleRegisterWorkflow(w http.ResponseWriter, r *http.Request) {
	var req RegisterWorkflowRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	def, created, err := s.svc.RegisterWorkflow(r.Context(), req.toDomain())
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
		w.Header().Set("Location", "/v1/workflows/"+def.Name+"/versions/"+strconv.Itoa(def.Version))
	}
	writeJSON(w, r, s.logger, status, newWorkflowResponse(def))
}

func (s *Server) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	limit, err := queryInt(r, "limit", 100, 1, 1000)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	name := strings.TrimSpace(r.URL.Query().Get("name"))

	defs, err := s.svc.ListWorkflows(r.Context(), name, limit)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	items := make([]WorkflowResponse, 0, len(defs))
	for i := range defs {
		items = append(items, newWorkflowResponse(&defs[i]))
	}
	writeJSON(w, r, s.logger, http.StatusOK, newListResponse(items))
}

// handleGetLatestWorkflow returns the highest registered version of a workflow.
func (s *Server) handleGetLatestWorkflow(w http.ResponseWriter, r *http.Request) {
	def, err := s.svc.GetWorkflow(r.Context(), r.PathValue("name"), 0)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, newWorkflowResponse(def))
}

func (s *Server) handleGetWorkflowVersion(w http.ResponseWriter, r *http.Request) {
	version, err := strconv.Atoi(r.PathValue("version"))
	if err != nil || version < 1 {
		writeError(w, r, s.logger, badRequest("version must be a positive integer"))
		return
	}

	def, err := s.svc.GetWorkflow(r.Context(), r.PathValue("name"), version)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, newWorkflowResponse(def))
}

// ---------------------------------------------------------------------------
// Executions
// ---------------------------------------------------------------------------

// handleStartExecution durably accepts a workflow run.
//
// The response is 202-like in spirit but uses 201 Created, because a durable
// execution resource genuinely exists at the returned location by the time the
// client sees the response; only its *scheduling* is asynchronous.
func (s *Server) handleStartExecution(w http.ResponseWriter, r *http.Request) {
	var req StartExecutionRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	// The header wins over the body so infrastructure that injects retry keys
	// does not have to rewrite payloads.
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		key = strings.TrimSpace(req.IdempotencyKey)
	}
	if len(key) > 255 {
		writeError(w, r, s.logger, badRequest("idempotency key must be at most 255 characters"))
		return
	}

	exec, created, err := s.svc.StartExecution(r.Context(), engine.StartExecutionRequest{
		WorkflowName:    req.WorkflowName,
		WorkflowVersion: req.WorkflowVersion,
		Input:           req.Input,
		IdempotencyKey:  key,
	})
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
		w.Header().Set("Location", "/v1/executions/"+exec.ID.String())
	}
	writeJSON(w, r, s.logger, status, newExecutionResponse(exec, nil))
}

func (s *Server) handleListExecutions(w http.ResponseWriter, r *http.Request) {
	limit, err := queryInt(r, "limit", 50, 1, 500)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	offset, err := queryInt(r, "offset", 0, 0, 1_000_000)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	filter := store.ExecutionFilter{
		WorkflowName: strings.TrimSpace(r.URL.Query().Get("workflowName")),
		State:        domain.WorkflowState(strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("state")))),
		Limit:        limit,
		Offset:       offset,
	}

	execs, err := s.svc.ListExecutions(r.Context(), filter)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	items := make([]ExecutionResponse, 0, len(execs))
	for i := range execs {
		items = append(items, newExecutionResponse(&execs[i], nil))
	}
	writeJSON(w, r, s.logger, http.StatusOK, newListResponse(items))
}

// handleGetExecution returns an execution together with its task graph, which is
// the view an operator needs to see where a workflow actually is.
func (s *Server) handleGetExecution(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	detail, err := s.svc.GetExecution(r.Context(), id)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, newExecutionResponse(detail.Execution, detail.Tasks))
}

// handleGetHistory returns the append-only execution history. The afterId cursor
// lets a client tail a long-running execution incrementally by passing back the
// id of the last event it saw.
func (s *Server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	after, err := queryInt64(r, "afterId", 0)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	limit, err := queryInt(r, "limit", 500, 1, 2000)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	events, err := s.svc.GetHistory(r.Context(), id, after, limit)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	if events == nil {
		events = []domain.HistoryEvent{}
	}
	writeJSON(w, r, s.logger, http.StatusOK, HistoryResponse{ExecutionID: id, Events: events})
}

func (s *Server) handleCancelExecution(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	// A body is optional for cancel; only a malformed one is an error.
	var req CancelExecutionRequest
	if r.ContentLength != 0 {
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, r, s.logger, err)
			return
		}
	}

	exec, err := s.svc.CancelExecution(r.Context(), id, strings.TrimSpace(req.Reason))
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, newExecutionResponse(exec, nil))
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------

func (s *Server) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req RegisterWorkerRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	worker, err := s.svc.RegisterWorker(r.Context(), store.RegisterWorkerParams{
		Name:       strings.TrimSpace(req.Name),
		TaskQueue:  strings.TrimSpace(req.TaskQueue),
		Activities: req.Activities,
	})
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, worker)
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	limit, err := queryInt(r, "limit", 100, 1, 1000)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	workers, err := s.svc.ListWorkers(r.Context(),
		strings.TrimSpace(r.URL.Query().Get("taskQueue")), limit)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, newListResponse(workers))
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	worker, err := s.svc.Heartbeat(r.Context(), id)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, worker)
}

// ---------------------------------------------------------------------------
// Task dispatch
// ---------------------------------------------------------------------------

// handlePollTask leases the next task to a worker.
//
// An empty queue returns 204 No Content rather than 404: "nothing to do right
// now" is the normal steady state for a polling worker, not an error, and
// keeping it out of the error path stops it from polluting error metrics.
func (s *Server) handlePollTask(w http.ResponseWriter, r *http.Request) {
	var req PollTaskRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	if req.LeaseSeconds < 0 || req.LeaseSeconds > 3600 {
		writeError(w, r, s.logger, badRequest("leaseSeconds must be between 0 and 3600"))
		return
	}

	task, err := s.svc.PollTask(r.Context(), engine.PollRequest{
		WorkerID:      strings.TrimSpace(req.WorkerID),
		TaskQueue:     strings.TrimSpace(req.TaskQueue),
		Activities:    req.Activities,
		LeaseDuration: time.Duration(req.LeaseSeconds) * time.Second,
	})
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeJSON(w, r, s.logger, http.StatusNoContent, nil)
			return
		}
		writeError(w, r, s.logger, err)
		return
	}

	// The claim token is only ever disclosed here, to the worker that won the
	// lease, because possessing it is what authorizes reporting a result.
	writeJSON(w, r, s.logger, http.StatusOK, newTaskResponse(task, true))
}

func (s *Server) handleCompleteTask(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	var req CompleteTaskRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	token, err := parseClaimToken(req.ClaimToken)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	task, err := s.svc.CompleteTask(r.Context(), id, token, req.Output)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, newTaskResponse(task, false))
}

func (s *Server) handleFailTask(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	var req FailTaskRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	token, err := parseClaimToken(req.ClaimToken)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	// Absent means retryable: assuming a failure is permanent would silently skip
	// the retry policy the workflow author configured.
	retryable := true
	if req.Retryable != nil {
		retryable = *req.Retryable
	}
	reason, err := parseFailureReason(req.Reason)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	task, err := s.svc.FailTask(r.Context(), id, token, store.FailParams{
		Error:     strings.TrimSpace(req.Error),
		Reason:    reason,
		Retryable: retryable,
	})
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, newTaskResponse(task, false))
}

// handleHeartbeatTask renews a task lease and relays cancellation.
//
// This is the highest-frequency write in the system for long-running tasks, and
// it doubles as the cooperative-cancellation channel: the worker is already
// checking in, so telling it to stop costs no extra round trip.
func (s *Server) handleHeartbeatTask(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	var req HeartbeatTaskRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	token, err := parseClaimToken(req.ClaimToken)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	if req.LeaseSeconds < 0 || req.LeaseSeconds > 3600 {
		writeError(w, r, s.logger, badRequest("leaseSeconds must be between 0 and 3600"))
		return
	}

	beat, err := s.svc.HeartbeatTask(r.Context(), id, token,
		time.Duration(req.LeaseSeconds)*time.Second)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, HeartbeatTaskResponse{
		TaskID:          beat.Task.ID,
		CancelRequested: beat.CancelRequested,
		LeaseExpiresAt:  beat.LeaseExpiresAt,
		Attempt:         beat.Task.Attempt,
	})
}

// handleListDeadLetter serves the operator's triage view.
func (s *Server) handleListDeadLetter(w http.ResponseWriter, r *http.Request) {
	limit, err := queryInt(r, "limit", 50, 1, 500)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	offset, err := queryInt(r, "offset", 0, 0, 1_000_000)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	tasks, err := s.svc.ListDeadLetterTasks(r.Context(), store.DeadLetterFilter{
		WorkflowName: strings.TrimSpace(r.URL.Query().Get("workflowName")),
		Activity:     strings.TrimSpace(r.URL.Query().Get("activity")),
		Limit:        limit,
		Offset:       offset,
	})
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	total, err := s.svc.CountDeadLetterTasks(r.Context())
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	items := make([]DeadLetterItem, 0, len(tasks))
	for i := range tasks {
		items = append(items, newDeadLetterItem(&tasks[i]))
	}
	writeJSON(w, r, s.logger, http.StatusOK, DeadLetterResponse{
		Items: items, Count: len(items), Total: total,
	})
}

// handleReplayTask returns a dead-lettered task to the queue.
func (s *Server) handleReplayTask(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "id")
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}

	// A body is optional; only a malformed one is an error.
	var req ReplayTaskRequest
	if r.ContentLength != 0 {
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, r, s.logger, err)
			return
		}
	}
	if req.ExtraAttempts < 0 || req.ExtraAttempts > 100 {
		writeError(w, r, s.logger, badRequest("extraAttempts must be between 0 and 100"))
		return
	}

	task, err := s.svc.ReplayTask(r.Context(), id, req.ExtraAttempts)
	if err != nil {
		writeError(w, r, s.logger, err)
		return
	}
	writeJSON(w, r, s.logger, http.StatusOK, newTaskResponse(task, false))
}

// parseFailureReason validates a caller-supplied failure category.
func parseFailureReason(raw string) (domain.FailureReason, error) {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	switch domain.FailureReason(raw) {
	case "":
		return domain.FailureActivityError, nil
	case domain.FailureActivityError, domain.FailureTimeout:
		return domain.FailureReason(raw), nil
	case domain.FailureLeaseExpired, domain.FailureWorkerDead:
		// These are conclusions the control plane draws from silence. A worker
		// claiming them would be reporting on its own death.
		return "", badRequest(fmt.Sprintf("reason %q is set by the engine, not by workers", raw))
	default:
		return "", badRequest(fmt.Sprintf("unknown reason %q; use ACTIVITY_ERROR or TIMEOUT", raw))
	}
}

func parseClaimToken(raw string) (uuid.UUID, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return uuid.Nil, badRequest("claimToken is required")
	}
	token, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, badRequest("claimToken must be a UUID")
	}
	return token, nil
}
