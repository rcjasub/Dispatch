package gqueue

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

type ProcessFunc func(j Job, report func(pct int)) (string, error)
type Event func(job Job)

type RateLimitConfig struct {
	MaxTokens   int
	RefillRate  float64
}

type Worker struct {
	queue       *Queue
	handlers    map[string]ProcessFunc
	concurrency int
	onCompleted Event
	onFailed    Event
	waitGroup   sync.WaitGroup
	rateLimit   *RateLimitConfig
	metrics     Metrics
}

func NewWorker(queue *Queue, concurrency int, rt *RateLimitConfig) *Worker {
	return &Worker{
		queue:       queue,
		handlers:    make(map[string]ProcessFunc),
		concurrency: concurrency,
		rateLimit:   rt,
		metrics:     noopMetrics{},
	}
}

// SetMetrics plugs in a metrics sink (e.g. prom.New). Call before Start.
func (w *Worker) SetMetrics(m Metrics) {
	if m == nil {
		m = noopMetrics{}
	}
	w.metrics = m
}

func (w *Worker) Register(name string, fn ProcessFunc) {
	w.handlers[name] = fn
}

func (w *Worker) Start(ctx context.Context) {
	for i := 0; i < w.concurrency; i++ {
		w.waitGroup.Add(1)
		go func() {
			defer w.waitGroup.Done() // defer means "run this when the function returns"
			name := fmt.Sprintf("worker-%d", i)
			for {
				// One atomic step: checks pause, pops by priority, records the job
				// in active-jobs and marks it active (see reliable.go).
				job, ok := w.queue.claim(ctx, name)
				if !ok {
					return
				}
				// Once we own a job, finish it even if shutdown starts (SIGTERM in
				// Kubernetes cancels ctx). With the cancelled ctx every Redis write
				// below would fail, leaving the job stuck in active-jobs until the
				// stalled detector re-ran it: a duplicate on every graceful restart.
				job.StartedAt = time.Now()
				w.processJob(context.WithoutCancel(ctx), job)
			}
		}()
	}
}

func (w *Worker) processJob(ctx context.Context, job Job) {

	if w.rateLimit != nil {
		for !w.queue.Allow(ctx, w.rateLimit.MaxTokens, w.rateLimit.RefillRate) {
			time.Sleep(100 * time.Millisecond)  // pause before checking again
		}
	}

	job.Status = StatusActive
	report := func(pct int) {
		w.queue.client.HSet(ctx, "job:"+job.Id, "progress", pct)
	}
	handler, ok := w.handlers[job.Name]
	if !ok {
		handler = func(j Job, report func(pct int)) (string, error) {
			return "", fmt.Errorf("no handler registered for job type: %s", j.Name)
		}
	}
	started := time.Now()
	var wait time.Duration
	if !job.CreatedAt.IsZero() {
		wait = started.Sub(job.CreatedAt)
	}
	w.metrics.JobStarted(job, wait)

	result, err := handler(job, report)
	took := time.Since(started)

	if err != nil {
		job.Attempts++

		if job.Attempts < job.MaxRetries {
			job.Delay = time.Duration(math.Pow(2, float64(job.Attempts))) * time.Second
			w.queue.client.HSet(ctx, "job:"+job.Id,
				"status", StatusWaiting.String(),
				"attempts", job.Attempts,
			)
			w.queue.Enqueue(ctx, job)
			// Not atomic with the Enqueue above: a crash in between leaves the job
			// both scheduled for retry and in active-jobs, so it can run twice.
			// Only affects jobs that already failed once; acceptable for now.
			w.queue.client.ZRem(ctx, "active-jobs:"+w.queue.Name, job.Id)
			w.metrics.JobRetried(job, took)

		} else {
			job.Status = StatusDeadLetter
			job.Error = err.Error()
			job.FailedAt = time.Now()
			w.queue.client.HSet(ctx, "job:"+job.Id,
				"status", job.Status.String(),
				"failedAt", job.FailedAt.Format(time.RFC3339),
				"error", job.Error,
			)
			w.queue.client.LPush(ctx, "dead-letter:"+w.queue.Name, job.Id)
			w.queue.client.ZRem(ctx, "active-jobs:"+w.queue.Name, job.Id)
			w.queue.client.Publish(ctx, "job:failed", job.Id)
			w.metrics.JobDeadLettered(job, took)
			if w.onFailed != nil {
				w.onFailed(job)
			}
		}
	} else {
		job.Status = StatusCompleted
		job.CompletedAt = time.Now()
		// HSET completed + ZREM active-jobs + PUBLISH as one atomic step (reliable.go).
		w.queue.ack(ctx, job, result)
		w.metrics.JobCompleted(job, took)
		if w.onCompleted != nil {
			w.onCompleted(job)
		}
	}

}

func (w *Worker) OnCompleted(fn Event) {
	w.onCompleted = fn
}

func (w *Worker) OnFailed(fn Event) {
	w.onFailed = fn
}

func (w *Worker) Wait() {
	w.waitGroup.Wait()
}
