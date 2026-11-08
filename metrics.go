package gqueue

import (
	"context"
	"time"
)

// Metrics receives events from a Worker as jobs move through their lifecycle.
// The core package only defines this interface; it does not depend on any
// metrics system. The gqueue/prom package implements it with Prometheus.
//
// Methods are called from worker goroutines concurrently, so implementations
// must be safe for concurrent use.
type Metrics interface {
	// JobStarted fires right before the handler runs (after the rate limiter).
	// wait is time since the job was first enqueued; it is 0 when unknown
	// (e.g. a job requeued by the stalled detector has no CreatedAt).
	JobStarted(job Job, wait time.Duration)
	// JobCompleted fires when the handler returned nil.
	JobCompleted(job Job, took time.Duration)
	// JobRetried fires when the handler failed and the job was scheduled again.
	JobRetried(job Job, took time.Duration)
	// JobDeadLettered fires when the handler failed and retries are exhausted.
	JobDeadLettered(job Job, took time.Duration)
}

// noopMetrics is the default so the worker never has to nil-check.
type noopMetrics struct{}

func (noopMetrics) JobStarted(Job, time.Duration)      {}
func (noopMetrics) JobCompleted(Job, time.Duration)    {}
func (noopMetrics) JobRetried(Job, time.Duration)      {}
func (noopMetrics) JobDeadLettered(Job, time.Duration) {}

// Stats is a point-in-time snapshot of how many jobs sit in each Redis structure.
type Stats struct {
	Waiting    map[JobPriority]int64 // per priority lane (list length)
	Delayed    int64                 // waiting on a delay or retry backoff
	Active     int64                 // being processed (active-jobs sorted set)
	DeadLetter int64
}

// Stats reads queue depths in one round trip using a pipeline.
// Used by the Prometheus collector at scrape time.
func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	pipe := q.client.Pipeline()
	lanes := make([]interface{ Val() int64 }, len(q.Names))
	for i, name := range q.Names {
		lanes[i] = pipe.LLen(ctx, name)
	}
	delayed := pipe.ZCard(ctx, "delayed:"+q.Name)
	active := pipe.ZCard(ctx, "active-jobs:"+q.Name)
	dead := pipe.LLen(ctx, "dead-letter:"+q.Name)
	if _, err := pipe.Exec(ctx); err != nil {
		return Stats{}, err
	}

	s := Stats{
		Waiting:    make(map[JobPriority]int64, len(q.Names)),
		Delayed:    delayed.Val(),
		Active:     active.Val(),
		DeadLetter: dead.Val(),
	}
	for i, l := range lanes {
		s.Waiting[JobPriority(i)] = l.Val()
	}
	return s, nil
}
