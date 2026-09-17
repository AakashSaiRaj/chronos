// Package telemetry holds Chronos's metrics and tracing.
//
// It is deliberately a leaf package: nothing here imports the engine, the store,
// or the API. Instrumentation therefore cannot create an import cycle, and the
// metric names live in one file where they can be reviewed as a set rather than
// discovered scattered through the code.
package telemetry

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Namespace prefixes every metric. A single prefix keeps Chronos's series
// separable from everything else scraped by the same Prometheus.
const Namespace = "chronos"

// Latency bucket choices matter more than they look, because a histogram's
// buckets fix what questions it can answer later. Once data is recorded you
// cannot re-bucket it retrospectively.
var (
	// taskLatencyBuckets span 5ms to ~2 minutes. Activities in the example set
	// finish in tens of milliseconds; real ones make network calls. The dense
	// region is 10ms-1s because that is where an alert threshold would sit.
	taskLatencyBuckets = []float64{
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120,
	}

	// workflowLatencyBuckets extend to an hour: a workflow's duration is the sum
	// of its tasks plus every scheduling gap and retry backoff between them, so it
	// is legitimately orders of magnitude longer than any single task.
	workflowLatencyBuckets = []float64{
		0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600,
	}

	// queueWaitBuckets are the ones that matter for saturation. Sub-millisecond
	// resolution at the bottom because on an idle system a task is claimed within
	// one engine sweep; anything above a second means workers are the constraint.
	queueWaitBuckets = []float64{
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
	}

	// dbBuckets are tight. Chronos uses PostgreSQL as its task queue, so claim
	// latency *is* commit latency: single-digit milliseconds is healthy and 100ms
	// is a problem, so the resolution belongs down there.
	dbBuckets = []float64{
		0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5,
	}
)

// Metrics is the full set of Chronos series.
//
// Cardinality is controlled deliberately. Labels are workflow name, activity,
// state, and task queue — all bounded by the number of registered workflows. What
// is *not* a label: execution ID, task ID, worker name. Those are unbounded, and
// putting an ID in a label is the standard way to take Prometheus down.
type Metrics struct {
	registry *prometheus.Registry

	// --- Workflow lifecycle -------------------------------------------------

	WorkflowsStarted  *prometheus.CounterVec
	WorkflowsFinished *prometheus.CounterVec
	WorkflowDuration  *prometheus.HistogramVec

	// --- Task lifecycle -----------------------------------------------------

	TasksScheduled *prometheus.CounterVec
	TasksStarted   *prometheus.CounterVec
	TasksFinished  *prometheus.CounterVec
	TaskDuration   *prometheus.HistogramVec

	// TaskQueueWait is the interval between a task becoming claimable and a
	// worker claiming it. This is the queue's own latency, and the single most
	// useful signal for deciding whether the worker pool is undersized: it rises
	// when workers are the constraint and stays flat when they are not.
	TaskQueueWait *prometheus.HistogramVec

	// --- Reliability --------------------------------------------------------

	TaskRetries          *prometheus.CounterVec
	TasksDeadLettered    *prometheus.CounterVec
	TaskLeaseExpiries    *prometheus.CounterVec
	TaskLeaseRenewals    *prometheus.CounterVec
	WorkersDeclaredDead  prometheus.Counter
	StaleClaimRejections *prometheus.CounterVec

	// --- Queue and backlog gauges (populated by the DB collector) -----------

	QueueDepth         *prometheus.GaugeVec
	QueueBackoffDepth  *prometheus.GaugeVec
	TasksByState       *prometheus.GaugeVec
	ExecutionsByState  *prometheus.GaugeVec
	DeadLetterDepth    prometheus.Gauge
	OldestClaimableAge *prometheus.GaugeVec
	RunningTaskCount   *prometheus.GaugeVec

	// --- Workers ------------------------------------------------------------

	WorkersRegistered *prometheus.GaugeVec
	WorkerSlotsTotal  prometheus.Gauge
	WorkerSlotsBusy   prometheus.Gauge
	WorkerPolls       *prometheus.CounterVec

	// --- Engine and reaper internals ----------------------------------------

	EngineSweepDuration   prometheus.Histogram
	EngineSweepExecutions prometheus.Counter
	EngineSweepErrors     prometheus.Counter
	ReaperSweepDuration   prometheus.Histogram
	ReaperReclaims        *prometheus.CounterVec

	// --- Database -----------------------------------------------------------

	DBQueryDuration *prometheus.HistogramVec
	DBQueryErrors   *prometheus.CounterVec
}

// NewMetrics builds and registers the metric set on its own registry.
//
// A dedicated registry rather than prometheus.DefaultRegisterer: it makes the
// exposed series explicit, keeps tests independent of global state, and means two
// instances in one process (as tests create) do not collide on registration.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{registry: reg}

	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: name, Help: help,
		}, labels)
	}
	gauge := func(name, help string, labels ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: name, Help: help,
		}, labels)
	}
	histogram := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: name, Help: help, Buckets: buckets,
		}, labels)
	}

	m.WorkflowsStarted = counter("workflow_executions_started_total",
		"Workflow executions accepted and durably persisted.", "workflow")
	m.WorkflowsFinished = counter("workflow_executions_finished_total",
		"Workflow executions that reached a terminal state.", "workflow", "state")
	m.WorkflowDuration = histogram("workflow_duration_seconds",
		"End-to-end workflow execution duration, from acceptance to terminal state.",
		workflowLatencyBuckets, "workflow", "state")

	m.TasksScheduled = counter("tasks_scheduled_total",
		"Tasks enqueued, including retries and requeues.", "workflow", "activity")
	m.TasksStarted = counter("tasks_started_total",
		"Task attempts claimed by a worker.", "workflow", "activity")
	m.TasksFinished = counter("tasks_finished_total",
		"Task attempts that reported an outcome.", "workflow", "activity", "state")
	m.TaskDuration = histogram("task_duration_seconds",
		"Task attempt execution time, from claim to reported outcome.",
		taskLatencyBuckets, "workflow", "activity", "state")
	m.TaskQueueWait = histogram("task_queue_wait_seconds",
		"Time a task spent claimable before a worker took it. The queue's own latency.",
		queueWaitBuckets, "workflow", "activity")

	m.TaskRetries = counter("task_retries_total",
		"Retries scheduled after a failed attempt.", "workflow", "activity", "reason")
	m.TasksDeadLettered = counter("tasks_dead_lettered_total",
		"Tasks parked for operator attention.", "workflow", "activity", "reason")
	m.TaskLeaseExpiries = counter("task_lease_expiries_total",
		"Leases reclaimed because the holding worker stopped reporting.",
		"activity", "cause")
	m.TaskLeaseRenewals = counter("task_lease_renewals_total",
		"Lease renewals served to workers holding long-running tasks.", "outcome")
	m.WorkersDeclaredDead = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Name: "workers_declared_dead_total",
		Help: "Workers marked dead because their heartbeat lapsed.",
	})
	m.StaleClaimRejections = counter("stale_claim_rejections_total",
		"Task reports refused because the caller no longer held the lease. "+
			"Non-zero is normal after a reclaim; a sustained rise means leases are too short.",
		"operation")

	m.QueueDepth = gauge("queue_depth",
		"Tasks claimable right now. The primary backlog signal.", "task_queue", "activity")
	m.QueueBackoffDepth = gauge("queue_backoff_depth",
		"Tasks scheduled for the future, waiting out a retry backoff.", "task_queue")
	m.TasksByState = gauge("tasks", "Tasks by state.", "state")
	m.ExecutionsByState = gauge("workflow_executions", "Workflow executions by state.", "state")
	m.DeadLetterDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace, Name: "dead_letter_depth",
		Help: "Tasks in the dead letter queue awaiting an operator.",
	})
	m.OldestClaimableAge = gauge("queue_oldest_claimable_age_seconds",
		"Age of the longest-waiting claimable task. Rises when workers cannot keep up, "+
			"and unlike queue_depth it distinguishes a large fast-draining queue from a stuck one.",
		"task_queue")
	m.RunningTaskCount = gauge("tasks_running",
		"Tasks currently leased to a worker.", "task_queue")

	m.WorkersRegistered = gauge("workers_registered",
		"Registered workers by queue and state.", "task_queue", "state")
	m.WorkerSlotsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace, Name: "worker_slots_total",
		Help: "Concurrency slots this worker process has.",
	})
	m.WorkerSlotsBusy = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace, Name: "worker_slots_busy",
		Help: "Concurrency slots currently executing an activity. " +
			"Divided by worker_slots_total this is worker utilization.",
	})
	m.WorkerPolls = counter("worker_polls_total",
		"Poll attempts made by this worker, by outcome.", "outcome")

	m.EngineSweepDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: Namespace, Name: "engine_sweep_duration_seconds",
		Help:    "Duration of one engine scheduling pass.",
		Buckets: dbBuckets,
	})
	m.EngineSweepExecutions = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Name: "engine_sweep_executions_total",
		Help: "Executions the engine advanced.",
	})
	m.EngineSweepErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Name: "engine_sweep_errors_total",
		Help: "Engine passes that failed. Sustained non-zero means the database is unhappy.",
	})
	m.ReaperSweepDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: Namespace, Name: "reaper_sweep_duration_seconds",
		Help:    "Duration of one failure-detection pass.",
		Buckets: dbBuckets,
	})
	m.ReaperReclaims = counter("reaper_reclaims_total",
		"Tasks reclaimed by failure detection, by outcome.", "outcome")

	m.DBQueryDuration = histogram("db_query_duration_seconds",
		"Store operation latency. Chronos uses PostgreSQL as its queue, so this is "+
			"the throughput ceiling made visible.",
		dbBuckets, "operation")
	m.DBQueryErrors = counter("db_query_errors_total",
		"Store operations that returned an error.", "operation")

	reg.MustRegister(
		m.WorkflowsStarted, m.WorkflowsFinished, m.WorkflowDuration,
		m.TasksScheduled, m.TasksStarted, m.TasksFinished, m.TaskDuration, m.TaskQueueWait,
		m.TaskRetries, m.TasksDeadLettered, m.TaskLeaseExpiries, m.TaskLeaseRenewals,
		m.WorkersDeclaredDead, m.StaleClaimRejections,
		m.QueueDepth, m.QueueBackoffDepth, m.TasksByState, m.ExecutionsByState,
		m.DeadLetterDepth, m.OldestClaimableAge, m.RunningTaskCount,
		m.WorkersRegistered, m.WorkerSlotsTotal, m.WorkerSlotsBusy, m.WorkerPolls,
		m.EngineSweepDuration, m.EngineSweepExecutions, m.EngineSweepErrors,
		m.ReaperSweepDuration, m.ReaperReclaims,
		m.DBQueryDuration, m.DBQueryErrors,
	)

	// Go runtime and process metrics: memory, goroutines, GC, file descriptors.
	// Cheap, and the first thing to check when latency rises for no visible reason.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

// Registry exposes the registry for the /metrics handler.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// ObserveDBQuery records a store operation's latency and outcome.
//
// Called from the store via a callback rather than the store importing telemetry,
// which keeps the dependency pointing one way.
func (m *Metrics) ObserveDBQuery(operation string, d time.Duration, err error) {
	if m == nil {
		return
	}
	m.DBQueryDuration.WithLabelValues(operation).Observe(d.Seconds())
	if err != nil {
		m.DBQueryErrors.WithLabelValues(operation).Inc()
	}
}

// SetWorkerSlots records this worker's concurrency, so utilization is a ratio of
// two exported series rather than a number computed in the worker.
func (m *Metrics) SetWorkerSlots(total, busy int) {
	if m == nil {
		return
	}
	m.WorkerSlotsTotal.Set(float64(total))
	m.WorkerSlotsBusy.Set(float64(busy))
}

// attemptLabel bounds attempt-number cardinality. Attempts beyond a handful are
// rare and interesting only as "many", so they collapse into one bucket rather
// than growing a new series per attempt.
func attemptLabel(attempt int) string {
	if attempt > 5 {
		return "6+"
	}
	return strconv.Itoa(attempt)
}
