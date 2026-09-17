package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AakashSaiRaj/chronos/internal/store"
	"github.com/AakashSaiRaj/chronos/internal/telemetry"
)

// ObserverConfig tunes the gauge refresher.
type ObserverConfig struct {
	// Interval is how often the database-derived gauges are refreshed.
	Interval time.Duration
}

func (c ObserverConfig) withDefaults() ObserverConfig {
	if c.Interval <= 0 {
		// Faster than a typical 15s scrape, so a scrape rarely sees a stale value,
		// but slow enough that the aggregate queries are negligible load.
		c.Interval = 5 * time.Second
	}
	return c
}

// Observer refreshes the gauges that can only be answered by querying the
// database: queue depth, backlog age, task and execution counts, worker counts.
//
// A background poller rather than a Prometheus Collector that queries on scrape.
// The on-scrape approach gives fresher numbers, but it puts a database round trip
// on the scrape path, so a slow database turns into scrape timeouts and gaps in
// the very metrics you would be reaching for. Polling keeps scrape latency flat
// and bounded; the cost is that a gauge can be up to one interval stale, which for
// a backlog measured in seconds is immaterial.
//
// Only one replica needs to run this — the gauges describe the whole system, not
// the process — but running it on all of them is harmless: they compute the same
// values from the same tables, and Prometheus deduplicates by target.
type Observer struct {
	store   *store.Store
	metrics *telemetry.Metrics
	cfg     ObserverConfig
	logger  *slog.Logger

	// poolCollector is registered once and reads the pool on scrape. That *is*
	// cheap enough for the scrape path: it is an in-memory struct read with no
	// database involvement.
	//
	// Guarded by a Once rather than a nil check: registration happens on the first
	// refresh, and Refresh is exported for tests to drive directly, so it is not
	// guaranteed to be reached from a single goroutine.
	registerPool  sync.Once
	poolCollector prometheus.Collector
}

// NewObserver constructs an Observer.
func NewObserver(s *store.Store, metrics *telemetry.Metrics, cfg ObserverConfig, logger *slog.Logger) *Observer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Observer{store: s, metrics: metrics, cfg: cfg.withDefaults(), logger: logger}
}

// Run refreshes gauges until ctx is canceled.
func (o *Observer) Run(ctx context.Context) error {
	if o.metrics == nil {
		o.logger.Warn("metrics disabled; queue gauges will not be published")
		return nil
	}

	o.logger.Info("metrics observer started", "interval", o.cfg.Interval)

	ticker := time.NewTicker(o.cfg.Interval)
	defer ticker.Stop()

	for {
		if err := o.Refresh(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Losing gauges is a monitoring outage, not an availability one, so
			// log and keep trying rather than taking the process down.
			o.logger.Warn("refreshing queue gauges failed", "error", err)
		}

		select {
		case <-ctx.Done():
			o.logger.Info("metrics observer stopping")
			return nil
		case <-ticker.C:
		}
	}
}

// Refresh performs one gauge update. Exported so tests can drive it directly.
func (o *Observer) Refresh(ctx context.Context) error {
	if o.metrics == nil {
		return nil
	}

	// Registered here rather than in Run so the pool gauges exist whenever the
	// observer is doing anything at all. Tying it to Run would mean a caller that
	// drives Refresh itself silently gets no pool saturation metrics, which is an
	// asymmetry nobody would expect to have to know about.
	o.registerPoolCollector()

	// Bounded independently of the caller: a hung aggregate query must not wedge
	// the refresh loop.
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	started := time.Now()
	stats, err := o.store.CollectQueueStats(queryCtx)
	o.metrics.ObserveDBQuery("collect_queue_stats", time.Since(started), err)
	if err != nil {
		return err
	}

	// Reset before setting. Without this, a label combination that disappears
	// (an activity with no remaining work, a drained queue) would keep reporting
	// its last value forever — the classic stale-gauge bug that makes a dashboard
	// show a permanent phantom backlog.
	o.metrics.QueueDepth.Reset()
	o.metrics.QueueBackoffDepth.Reset()
	o.metrics.RunningTaskCount.Reset()
	o.metrics.OldestClaimableAge.Reset()
	o.metrics.TasksByState.Reset()
	o.metrics.ExecutionsByState.Reset()
	o.metrics.WorkersRegistered.Reset()

	for key, n := range stats.ClaimableByActivity {
		o.metrics.QueueDepth.WithLabelValues(key.TaskQueue, key.Activity).Set(float64(n))
	}
	for queue, n := range stats.BackoffByQueue {
		o.metrics.QueueBackoffDepth.WithLabelValues(queue).Set(float64(n))
	}
	for queue, n := range stats.RunningByQueue {
		o.metrics.RunningTaskCount.WithLabelValues(queue).Set(float64(n))
	}
	for queue, age := range stats.OldestClaimableAge {
		o.metrics.OldestClaimableAge.WithLabelValues(queue).Set(age.Seconds())
	}
	for state, n := range stats.TasksByState {
		o.metrics.TasksByState.WithLabelValues(string(state)).Set(float64(n))
	}
	for state, n := range stats.ExecutionsByState {
		o.metrics.ExecutionsByState.WithLabelValues(string(state)).Set(float64(n))
	}
	for key, n := range stats.WorkersByQueueState {
		o.metrics.WorkersRegistered.WithLabelValues(key.TaskQueue, key.State).Set(float64(n))
	}
	o.metrics.DeadLetterDepth.Set(float64(stats.DeadLetterDepth))

	return nil
}

// registerPoolCollector publishes connection pool saturation.
//
// Registered as a Collector rather than polled, because reading the pool stats is
// an in-memory operation with no database involvement, so putting it on the scrape
// path costs nothing and gives the freshest possible value.
func (o *Observer) registerPoolCollector() {
	o.registerPool.Do(o.buildAndRegisterPoolCollector)
}

func (o *Observer) buildAndRegisterPoolCollector() {
	descs := map[string]*prometheus.Desc{}
	newDesc := func(name, help string) *prometheus.Desc {
		d := prometheus.NewDesc(
			prometheus.BuildFQName(telemetry.Namespace, "db_pool", name), help, nil, nil)
		descs[name] = d
		return d
	}

	acquired := newDesc("acquired_connections", "Connections currently checked out.")
	idle := newDesc("idle_connections", "Connections open and idle.")
	total := newDesc("total_connections", "Connections open.")
	maximum := newDesc("max_connections",
		"Pool ceiling. Multiplied by the replica count this is the load on the database's max_connections.")
	empty := newDesc("empty_acquires_total",
		"Acquires that had to wait for a free connection. Rising means the pool is the bottleneck, "+
			"which surfaces as latency well before it surfaces as an error.")
	canceled := newDesc("canceled_acquires_total", "Acquires abandoned before a connection was free.")

	o.poolCollector = &poolCollector{
		store:    o.store,
		acquired: acquired, idle: idle, total: total, maximum: maximum,
		empty: empty, canceled: canceled,
	}

	if err := o.metrics.Registry().Register(o.poolCollector); err != nil {
		o.logger.Warn("could not register pool collector", "error", err)
	}
}

type poolCollector struct {
	store                                           *store.Store
	acquired, idle, total, maximum, empty, canceled *prometheus.Desc
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.acquired
	ch <- c.idle
	ch <- c.total
	ch <- c.maximum
	ch <- c.empty
	ch <- c.canceled
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.store.PoolStats()
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(st.Acquired))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(st.Idle))
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(st.Total))
	ch <- prometheus.MustNewConstMetric(c.maximum, prometheus.GaugeValue, float64(st.Max))
	ch <- prometheus.MustNewConstMetric(c.empty, prometheus.CounterValue, float64(st.EmptyAcquires))
	ch <- prometheus.MustNewConstMetric(c.canceled, prometheus.CounterValue, float64(st.Canceled))
}
