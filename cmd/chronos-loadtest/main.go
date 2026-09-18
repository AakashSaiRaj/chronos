// Command chronos-loadtest drives load through the Chronos control plane and
// reports throughput and latency.
//
// It is a client, not a harness: it talks to the same REST API a real caller would
// and never touches the database. That matters because the numbers are meant to
// describe the system as deployed, including the HTTP layer and the connection
// pool, rather than the engine in isolation.
//
// What it measures, and why each is separate:
//
//   - Submission latency: how long POST /v1/executions takes. This is the only
//     latency a caller experiences synchronously, and it should stay flat as the
//     backlog grows, because accepting a workflow is one insert and is deliberately
//     decoupled from running it.
//   - End-to-end workflow latency: acceptance to terminal state, computed from the
//     server's own timestamps rather than the load generator's clock, so a slow or
//     descheduled load generator cannot inflate it.
//   - Throughput: completed workflows and tasks per second over the drain window,
//     which is the figure that actually reflects capacity.
//
// Latency and throughput are collected in one run but reported separately, because
// a system can be fast and low-throughput or slow and high-throughput, and the
// interesting failure is when queue wait dominates end-to-end latency while
// submission stays fast.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/AakashSaiRaj/chronos/internal/client"
	"github.com/AakashSaiRaj/chronos/internal/domain"
)

type options struct {
	serverURL   string
	executions  int
	concurrency int
	rate        float64
	workflow    string
	tasks       int
	timeout     time.Duration
	jsonOut     string
	label       string
	warmup      int
}

func main() {
	opts := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, opts); err != nil {
		fmt.Fprintf(os.Stderr, "loadtest: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() options {
	var opts options
	flag.StringVar(&opts.serverURL, "server", "http://127.0.0.1:8088", "Chronos API base URL")
	flag.IntVar(&opts.executions, "executions", 200, "number of workflow executions to start")
	flag.IntVar(&opts.concurrency, "concurrency", 16, "concurrent submitters")
	flag.Float64Var(&opts.rate, "rate", 0,
		"target submissions per second (0 = as fast as concurrency allows)")
	flag.StringVar(&opts.workflow, "workflow", "loadtest_pipeline", "workflow name to register and run")
	flag.IntVar(&opts.tasks, "tasks", 3, "tasks per workflow, chained sequentially")
	flag.DurationVar(&opts.timeout, "timeout", 5*time.Minute, "overall budget for the run")
	flag.StringVar(&opts.jsonOut, "json", "", "also write the report as JSON to this path")
	flag.StringVar(&opts.label, "label", "", "label recorded in the report, e.g. the worker count")
	flag.IntVar(&opts.warmup, "warmup", 5,
		"executions to run and discard before measuring, so first-request costs do not skew percentiles")
	flag.Parse()
	return opts
}

func run(ctx context.Context, opts options) error {
	ctx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()

	c, err := client.New(client.Config{
		BaseURL: opts.serverURL,
		Timeout: 30 * time.Second,
		// Retries off: a retried submission would be counted once but timed twice,
		// and idempotency keys would hide a real failure as a success.
		MaxRetries: 0,
	})
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}

	if err := c.Ready(ctx); err != nil {
		return fmt.Errorf("control plane is not ready at %s: %w", opts.serverURL, err)
	}

	spec := buildSpec(opts)
	if _, err := c.RegisterWorkflow(ctx, spec); err != nil {
		return fmt.Errorf("register workflow: %w", err)
	}

	if err := requireWorkers(ctx, c, spec.TaskQueue); err != nil {
		return err
	}

	// Warm-up is discarded. The first requests pay for connection setup, prepared
	// statement caching, and the engine's first sweep, all of which are one-time
	// costs that would otherwise land in the tail percentiles.
	//
	// Drained rather than merely submitted: leaving warm-up workflows in flight
	// would have them competing for the same worker slots as the measured run, so
	// the run would be sharing capacity with load it does not account for.
	if opts.warmup > 0 {
		fmt.Printf("warming up with %d executions...\n", opts.warmup)
		warm := opts
		// Its own workflow name, so the measured run's filtered execution list
		// contains only measured executions.
		warm.workflow = opts.workflow + "_warmup"
		warm.executions = opts.warmup
		warm.concurrency = min(opts.warmup, opts.concurrency)
		warm.rate = 0

		warmSpec := buildSpec(warm)
		if _, err := c.RegisterWorkflow(ctx, warmSpec); err != nil {
			return fmt.Errorf("register warmup workflow: %w", err)
		}
		if _, err := submit(ctx, c, warm, "warmup"); err != nil {
			return fmt.Errorf("warmup: %w", err)
		}
		if _, err := drain(ctx, c, warm.workflow, warm.executions); err != nil {
			return fmt.Errorf("warmup drain: %w", err)
		}
	}

	fmt.Printf("submitting %d executions of %s (%d tasks each) at concurrency %d",
		opts.executions, opts.workflow, opts.tasks, opts.concurrency)
	if opts.rate > 0 {
		fmt.Printf(", target %.0f/s", opts.rate)
	}
	fmt.Println()

	started := time.Now()
	sub, err := submit(ctx, c, opts, "run")
	if err != nil {
		return err
	}
	submitDone := time.Now()

	fmt.Printf("submitted %d in %s (%.0f/s accepted); draining...\n",
		len(sub.ids), submitDone.Sub(started).Round(time.Millisecond),
		float64(len(sub.ids))/submitDone.Sub(started).Seconds())

	drained, err := drain(ctx, c, opts.workflow, len(sub.ids))
	if err != nil {
		return err
	}
	// Measured with Go's monotonic clock, which does not advance while the host is
	// suspended. That property is what makes it usable as a cross-check against the
	// database's wall-clock timestamps; see report.ClockSuspect.
	wallDuration := time.Since(started)

	report := buildReport(opts, sub, drained, wallDuration)
	report.print()

	if opts.jsonOut != "" {
		if err := report.writeJSON(opts.jsonOut); err != nil {
			return fmt.Errorf("write json report: %w", err)
		}
		fmt.Printf("\nreport written to %s\n", opts.jsonOut)
	}

	if report.Failed > 0 {
		return fmt.Errorf("%d executions did not complete successfully", report.Failed)
	}
	return nil
}

// buildSpec creates a linear chain of tasks. Sequential rather than parallel on
// purpose: a chain forces the engine to make a scheduling decision between every
// task, so the run measures the scheduling loop and not just the claim path.
//
// Every task runs `noop`. The order-pipeline activities would work for a
// three-task chain, but they validate their input and read named upstream outputs,
// so they would cap the chain at three tasks and fold JSON validation into the
// measurement. The point here is to measure Chronos -- scheduling, queueing,
// claiming, committing -- so the activity is deliberately as close to free as
// possible, and whatever is left is the engine's own cost.
func buildSpec(opts options) domain.WorkflowSpec {
	spec := domain.WorkflowSpec{
		Name:        opts.workflow,
		Version:     1,
		Description: "Chronos load test: sequential no-op chain",
		TaskQueue:   "default",
	}

	for i := range opts.tasks {
		task := domain.TaskSpec{
			Name:     fmt.Sprintf("task_%d", i+1),
			Activity: "noop",
		}
		if i > 0 {
			task.DependsOn = []string{fmt.Sprintf("task_%d", i)}
		}
		spec.Tasks = append(spec.Tasks, task)
	}
	spec.Normalize()
	return spec
}

// requireWorkers refuses to run without a worker on the queue.
//
// Worth failing loudly: with no worker every workflow would simply sit in the
// queue, the run would time out, and the resulting "0 workflows/sec" would look
// like a performance finding rather than a missing process.
func requireWorkers(ctx context.Context, c *client.Client, queue string) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		workers, err := c.ListWorkers(ctx, queue)
		if err == nil {
			for _, w := range workers {
				if w.State == domain.WorkerActive {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no active worker on queue %q: start chronos-worker first", queue)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

type submission struct {
	ids       []uuid.UUID
	latencies []time.Duration
	errors    int
}

// submit starts executions and records how long each acceptance took.
func submit(ctx context.Context, c *client.Client, opts options, phase string) (*submission, error) {
	var (
		mu        sync.Mutex
		ids       = make([]uuid.UUID, 0, opts.executions)
		latencies = make([]time.Duration, 0, opts.executions)
		errCount  atomic.Int64
	)

	work := make(chan int)
	var wg sync.WaitGroup

	// A rate limiter only when a target rate is set. Open-loop submission is the
	// honest way to measure latency under a known offered load; closed-loop
	// (concurrency-bound) submission instead measures maximum throughput, because
	// the generator slows down exactly when the system does.
	var ticker *time.Ticker
	if opts.rate > 0 {
		ticker = time.NewTicker(time.Duration(float64(time.Second) / opts.rate))
		defer ticker.Stop()
	}

	for range opts.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				started := time.Now()
				exec, err := c.StartExecution(ctx, client.StartExecutionRequest{
					WorkflowName: opts.workflow,
					Input:        payload(i),
					// A unique key per submission: reusing one would make every
					// request after the first a deduplicated no-op and the run would
					// measure idempotency lookups instead of workflow starts.
					IdempotencyKey: fmt.Sprintf("loadtest-%s-%s-%d", phase, runID, i),
				})
				elapsed := time.Since(started)
				if err != nil {
					errCount.Add(1)
					continue
				}
				mu.Lock()
				ids = append(ids, exec.ID)
				latencies = append(latencies, elapsed)
				mu.Unlock()
			}
		}()
	}

	for i := range opts.executions {
		if ticker != nil {
			select {
			case <-ctx.Done():
				close(work)
				wg.Wait()
				return nil, ctx.Err()
			case <-ticker.C:
			}
		}
		select {
		case work <- i:
		case <-ctx.Done():
			close(work)
			wg.Wait()
			return nil, ctx.Err()
		}
	}
	close(work)
	wg.Wait()

	return &submission{ids: ids, latencies: latencies, errors: int(errCount.Load())}, nil
}

// runID keeps idempotency keys unique across repeated runs against the same
// database, so a second run is not silently deduplicated into the first.
var runID = uuid.NewString()[:8]

func payload(i int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"orderId":"load-%d","customer":"load@example.com","amount":%d.50,"currency":"USD","sku":"widget-blue","quantity":%d}`,
		i, 10+i%90, 1+i%5))
}

type drainResult struct {
	executions []client.ExecutionResponse
	completed  int
	failed     int
	firstStart time.Time
	lastFinish time.Time
	tasks      int
}

// drain waits for every submitted execution to reach a terminal state.
//
// Progress is read with paged list requests scoped to this run's workflow, not one
// GetExecution per execution. The difference is not a micro-optimization: the naive
// version issued a sequential request per pending execution per pass, so watching
// 600 executions cost 600 round trips per pass and the watcher's own latency landed
// inside the measurement window. It made throughput look like it did not improve
// with more workers, because the constant being measured was the load generator.
//
// Each run uses its own workflow name, so a filtered list returns exactly this
// run's executions -- including their timestamps, which removes the separate
// fetch pass entirely.
func drain(ctx context.Context, c *client.Client, workflow string, expected int) (*drainResult, error) {
	const pageSize = 500

	result := &drainResult{}
	lastReport := time.Now()

	for {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("timed out with %d of %d executions still running",
				expected-result.completed-result.failed, expected)
		}

		terminal := make([]client.ExecutionResponse, 0, expected)
		var seen int
		for offset := 0; ; offset += pageSize {
			page, err := c.ListExecutions(ctx, client.ExecutionFilter{
				WorkflowName: workflow,
				Limit:        pageSize,
				Offset:       offset,
			})
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return nil, fmt.Errorf("timed out polling executions: %w", err)
				}
				break
			}
			seen += len(page)
			for _, exec := range page {
				if isTerminal(exec.State) {
					terminal = append(terminal, exec)
				}
			}
			if len(page) < pageSize {
				break
			}
		}

		if len(terminal) >= expected {
			result.executions = terminal
			break
		}

		if time.Since(lastReport) > 5*time.Second {
			fmt.Printf("  %d/%d complete\n", len(terminal), expected)
			lastReport = time.Now()
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out with %d of %d executions still running",
				expected-len(terminal), expected)
		case <-time.After(100 * time.Millisecond):
		}
	}

	for _, exec := range result.executions {
		switch exec.State {
		case domain.WorkflowCompleted:
			result.completed++
		default:
			result.failed++
		}
		result.tasks += len(exec.Tasks)
	}

	// The server's own view of the interval: first acceptance to last terminal
	// state. Reported as a cross-check on the monotonic measurement, not as the
	// throughput denominator.
	for i, exec := range result.executions {
		if i == 0 || exec.CreatedAt.Before(result.firstStart) {
			result.firstStart = exec.CreatedAt
		}
		finish := exec.UpdatedAt
		if exec.CompletedAt != nil {
			finish = *exec.CompletedAt
		}
		if finish.After(result.lastFinish) {
			result.lastFinish = finish
		}
	}
	return result, nil
}

func isTerminal(state domain.WorkflowState) bool {
	switch state {
	case domain.WorkflowCompleted, domain.WorkflowFailed, domain.WorkflowCanceled:
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

type stats struct {
	Count  int     `json:"count"`
	MinMS  float64 `json:"minMs"`
	P50MS  float64 `json:"p50Ms"`
	P95MS  float64 `json:"p95Ms"`
	P99MS  float64 `json:"p99Ms"`
	MaxMS  float64 `json:"maxMs"`
	MeanMS float64 `json:"meanMs"`
}

type report struct {
	Label       string  `json:"label,omitempty"`
	Workflow    string  `json:"workflow"`
	TasksPerWF  int     `json:"tasksPerWorkflow"`
	Requested   int     `json:"requested"`
	Concurrency int     `json:"concurrency"`
	TargetRate  float64 `json:"targetRatePerSec,omitempty"`

	Accepted  int `json:"accepted"`
	Rejected  int `json:"rejected"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`

	// WindowSeconds is the generator's own elapsed time, measured on the monotonic
	// clock. Throughput is derived from this.
	WindowSeconds   float64 `json:"windowSeconds"`
	WorkflowsPerSec float64 `json:"workflowsPerSec"`
	TasksPerSec     float64 `json:"tasksPerSec"`
	SubmitPerSec    float64 `json:"submitAcceptedPerSec"`

	// ServerWindowSeconds is the same interval as the database saw it: first
	// acceptance to last terminal state. Kept as a cross-check rather than as the
	// headline figure, because it is wall-clock and so vulnerable to the host's
	// clock moving underneath the run.
	ServerWindowSeconds float64 `json:"serverWindowSeconds"`
	// ClockSuspect is set when the two clocks disagree enough that the run should
	// not be believed. See buildReport.
	ClockSuspect bool `json:"clockSuspect"`

	SubmitLatency   stats `json:"submitLatency"`
	EndToEndLatency stats `json:"endToEndLatency"`

	GeneratedAt time.Time `json:"generatedAt"`
}

func buildReport(opts options, sub *submission, drained *drainResult, wallDuration time.Duration) *report {
	window := wallDuration.Seconds()
	if window <= 0 {
		window = 0.001
	}
	serverWindow := drained.lastFinish.Sub(drained.firstStart).Seconds()

	// Throughput comes from the generator's monotonic clock; the database's
	// wall-clock view is only a cross-check.
	//
	// The distinction is not academic. A development laptop that suspends mid-run
	// leaves database timestamps spanning the suspend while Go's monotonic clock
	// does not advance through it. Deriving throughput from those timestamps turned
	// a healthy ~30 workflows/sec run into a reported 0.8 -- a number that reads
	// like a scalability finding and was really a closed lid. Comparing the two
	// clocks catches it, so a suspect run gets flagged instead of published.
	clockSuspect := serverWindow > window*2+5

	endToEnd := make([]time.Duration, 0, len(drained.executions))
	for _, exec := range drained.executions {
		finish := exec.UpdatedAt
		if exec.CompletedAt != nil {
			finish = *exec.CompletedAt
		}
		if d := finish.Sub(exec.CreatedAt); d >= 0 {
			endToEnd = append(endToEnd, d)
		}
	}

	tasks := drained.tasks
	if tasks == 0 {
		// The execution response omits tasks on some paths; fall back to the spec.
		tasks = drained.completed * opts.tasks
	}

	return &report{
		Label:       opts.label,
		Workflow:    opts.workflow,
		TasksPerWF:  opts.tasks,
		Requested:   opts.executions,
		Concurrency: opts.concurrency,
		TargetRate:  opts.rate,

		Accepted:  len(sub.ids),
		Rejected:  sub.errors,
		Completed: drained.completed,
		Failed:    drained.failed,

		WindowSeconds:       window,
		WorkflowsPerSec:     float64(drained.completed) / window,
		TasksPerSec:         float64(tasks) / window,
		SubmitPerSec:        float64(len(sub.ids)) / window,
		ServerWindowSeconds: serverWindow,
		ClockSuspect:        clockSuspect,

		SubmitLatency:   summarize(sub.latencies),
		EndToEndLatency: summarize(endToEnd),

		GeneratedAt: time.Now().UTC(),
	}
}

func summarize(samples []time.Duration) stats {
	if len(samples) == 0 {
		return stats{}
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, s := range sorted {
		total += s
	}

	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return stats{
		Count:  len(sorted),
		MinMS:  ms(sorted[0]),
		P50MS:  ms(percentile(sorted, 0.50)),
		P95MS:  ms(percentile(sorted, 0.95)),
		P99MS:  ms(percentile(sorted, 0.99)),
		MaxMS:  ms(sorted[len(sorted)-1]),
		MeanMS: ms(total / time.Duration(len(sorted))),
	}
}

// percentile uses nearest-rank on a sorted slice. Simple and, unlike an
// interpolating variant, it never reports a latency that was not actually observed.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (r *report) print() {
	line := func(format string, args ...any) { fmt.Printf(format+"\n", args...) }

	fmt.Println()
	line("================ Chronos load test ================")
	if r.Label != "" {
		line("  label                 %s", r.Label)
	}
	line("  workflow              %s (%d tasks each)", r.Workflow, r.TasksPerWF)
	line("  submitters            %d", r.Concurrency)
	if r.TargetRate > 0 {
		line("  target rate           %.0f/s", r.TargetRate)
	}
	line("")
	line("  accepted / rejected   %d / %d", r.Accepted, r.Rejected)
	line("  completed / failed    %d / %d", r.Completed, r.Failed)
	line("")
	line("  measurement window    %.2fs (monotonic)", r.WindowSeconds)
	line("  server-side window    %.2fs (database wall clock)", r.ServerWindowSeconds)
	line("  workflows/sec         %.1f", r.WorkflowsPerSec)
	line("  tasks/sec             %.1f", r.TasksPerSec)
	if r.ClockSuspect {
		line("")
		line("  !! CLOCK JUMP DETECTED")
		line("     The database's wall clock advanced %.0fs while this process measured", r.ServerWindowSeconds)
		line("     only %.0fs. The host almost certainly suspended mid-run, which inflates", r.WindowSeconds)
		line("     the end-to-end latencies below. Throughput is still correct, but this run")
		line("     should be repeated before its latency numbers are used.")
	}
	line("")
	printStats("  submit latency  (POST /v1/executions)", r.SubmitLatency)
	printStats("  end-to-end      (accept -> terminal)", r.EndToEndLatency)
	line("===================================================")
}

func printStats(label string, s stats) {
	fmt.Printf("%s\n", label)
	fmt.Printf("      n=%d  p50=%.1fms  p95=%.1fms  p99=%.1fms  max=%.1fms  mean=%.1fms\n",
		s.Count, s.P50MS, s.P95MS, s.P99MS, s.MaxMS, s.MeanMS)
}

func (r *report) writeJSON(path string) error {
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(encoded, '\n'), 0o644)
}
