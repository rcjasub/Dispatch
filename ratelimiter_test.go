package gqueue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRateLimitAcrossReplicas simulates several worker pods sharing one limit.
// Each "replica" has its own Redis client (its own connection pool, like a
// separate process) and several goroutines hammering Allow at the same time.
//
// The limit holds because the whole read-refill-decrement runs as one Lua
// script, and Redis runs a script atomically: no other command (from any
// client) can interleave between the read of `tokens` and the write. Done in
// Go as GET then SET, two replicas could both read tokens=1 and both proceed.
func TestRateLimitAcrossReplicas(t *testing.T) {
	const (
		replicas   = 4
		goroutines = 8
		maxTokens  = 50
		refillRate = 100.0 // tokens per second
		duration   = 2 * time.Second
	)
	name := fmt.Sprintf("rl-replicas-%d", time.Now().UnixNano())
	ctx := context.Background()
	defer testClient.Del(ctx, "ratelimit:"+name)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(duration)

	for range replicas {
		client := redis.NewClient(&redis.Options{Addr: testClient.Options().Addr})
		defer client.Close()
		q := NewQueue(client, nil, name)

		for range goroutines {
			wg.Go(func() {
				for time.Now().Before(deadline) {
					if q.Allow(ctx, maxTokens, refillRate) {
						allowed.Add(1)
					}
				}
			})
		}
	}
	wg.Wait()

	// Upper bound: the initial full bucket plus everything refilled during the run.
	// Allow fails open on Redis errors, so a broken Redis would also fail this.
	limit := int64(maxTokens + refillRate*duration.Seconds())
	got := allowed.Load()
	t.Logf("%d replicas x %d goroutines: %d allowed in %v (bound %d)", replicas, goroutines, got, duration, limit)
	if got > limit {
		t.Fatalf("rate limit exceeded across replicas: %d allowed, bound %d", got, limit)
	}
	// Lower bound guards against a limiter that is simply stuck at 0.
	if got < limit/2 {
		t.Fatalf("limiter too strict: %d allowed, expected near %d", got, limit)
	}
}
