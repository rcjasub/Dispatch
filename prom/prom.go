// Package prom exposes gqueue metrics to Prometheus.
//
// It has two halves:
//   - event metrics (counters and histograms) updated by the worker as jobs run,
//     via the gqueue.Metrics interface;
//   - state metrics (queue depths) read from Redis at scrape time by a custom
//     Collector, so they are always current and cost nothing between scrapes.
//
// Usage:
//
//	m, err := prom.New(queue, prometheus.DefaultRegisterer)
//	worker.SetMetrics(m)
//	http.Handle("/metrics", promhttp.Handler())
package prom

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcjasub/gqueue"
)

// Metrics implements gqueue.Metrics.
type Metrics struct {
	started    *prometheus.CounterVec
	finished   *prometheus.CounterVec
	wait       *prometheus.HistogramVec
	processing *prometheus.HistogramVec
}

var _ gqueue.Metrics = (*Metrics)(nil)

// New creates the metrics for one queue and registers them (plus the depth
// collector) on reg. Every series carries a constant label queue=<q.Name>.
func New(q *gqueue.Queue, reg prometheus.Registerer) (*Metrics, error) {
	constLabels := prometheus.Labels{"queue": q.Name}

	m := &Metrics{
		started: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "gqueue_jobs_started_total",
			Help:        "Jobs handed to a handler (after the rate limiter).",
			ConstLabels: constLabels,
		}, []string{"priority"}),

		// One counter with an outcome label instead of three counters: failures
		// are outcome=~"retried|dead_letter", and sum() gives total processed.
		finished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "gqueue_jobs_processed_total",
			Help:        "Handler runs by outcome: completed, retried, dead_letter.",
			ConstLabels: constLabels,
		}, []string{"priority", "outcome"}),

		// Histograms, not Summaries: histogram buckets can be summed across pods,
		// so histogram_quantile() gives a correct cluster-wide p95. Summaries
		// compute quantiles per process and cannot be aggregated.
		wait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "gqueue_job_wait_seconds",
			Help:        "Time from first enqueue to handler start.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.001, 2, 18), // 1ms .. ~131s
		}, []string{"priority"}),

		processing: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "gqueue_job_processing_seconds",
			Help:        "Handler run time.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 16), // 0.5ms .. ~16s
		}, []string{"priority", "outcome"}),
	}

	for _, c := range []prometheus.Collector{m.started, m.finished, m.wait, m.processing, newDepthCollector(q)} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *Metrics) JobStarted(j gqueue.Job, wait time.Duration) {
	p := j.Priority.String()
	m.started.WithLabelValues(p).Inc()
	if wait > 0 {
		m.wait.WithLabelValues(p).Observe(wait.Seconds())
	}
}

func (m *Metrics) JobCompleted(j gqueue.Job, took time.Duration) {
	m.observe(j, "completed", took)
}

func (m *Metrics) JobRetried(j gqueue.Job, took time.Duration) {
	m.observe(j, "retried", took)
}

func (m *Metrics) JobDeadLettered(j gqueue.Job, took time.Duration) {
	m.observe(j, "dead_letter", took)
}

func (m *Metrics) observe(j gqueue.Job, outcome string, took time.Duration) {
	p := j.Priority.String()
	m.finished.WithLabelValues(p, outcome).Inc()
	m.processing.WithLabelValues(p, outcome).Observe(took.Seconds())
}

// depthCollector reads queue depths from Redis each time Prometheus scrapes.
//
// Note for Kubernetes: every worker pod runs this collector against the same
// Redis, so every pod reports the same depth. Aggregate with max(), not sum().
type depthCollector struct {
	q          *gqueue.Queue
	waiting    *prometheus.Desc
	delayed    *prometheus.Desc
	active     *prometheus.Desc
	deadLetter *prometheus.Desc
	up         *prometheus.Desc
}

func newDepthCollector(q *gqueue.Queue) *depthCollector {
	cl := prometheus.Labels{"queue": q.Name}
	return &depthCollector{
		q:          q,
		waiting:    prometheus.NewDesc("gqueue_waiting_jobs", "Jobs waiting in a priority lane.", []string{"priority"}, cl),
		delayed:    prometheus.NewDesc("gqueue_delayed_jobs", "Jobs waiting on a delay or retry backoff.", nil, cl),
		active:     prometheus.NewDesc("gqueue_active_jobs", "Jobs currently being processed.", nil, cl),
		deadLetter: prometheus.NewDesc("gqueue_dead_letter_jobs", "Jobs in the dead-letter list.", nil, cl),
		up:         prometheus.NewDesc("gqueue_redis_up", "1 if the last depth read from Redis succeeded.", nil, cl),
	}
}

func (c *depthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.waiting
	ch <- c.delayed
	ch <- c.active
	ch <- c.deadLetter
	ch <- c.up
}

func (c *depthCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s, err := c.q.Stats(ctx)
	if err != nil {
		// Report Redis as down instead of failing the whole scrape, so the
		// counters and histograms above still reach Prometheus.
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)
	for p, n := range s.Waiting {
		ch <- prometheus.MustNewConstMetric(c.waiting, prometheus.GaugeValue, float64(n), p.String())
	}
	ch <- prometheus.MustNewConstMetric(c.delayed, prometheus.GaugeValue, float64(s.Delayed))
	ch <- prometheus.MustNewConstMetric(c.active, prometheus.GaugeValue, float64(s.Active))
	ch <- prometheus.MustNewConstMetric(c.deadLetter, prometheus.GaugeValue, float64(s.DeadLetter))
}
