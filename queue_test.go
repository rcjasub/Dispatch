package gqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"os"
	"testing"
	"time"
)

var testClient *redis.Client

func TestMain(m *testing.M) {
	// Create one shared Redis client for all tests instead of each test making its own.
	// REDIS_ADDR lets tests target a dedicated Redis instead of one shared with other projects.
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	testClient = redis.NewClient(&redis.Options{Addr: addr})

	// m.Run() triggers all TestXxx functions in this package.
	// It only exists here because TestMain gives us m — we must call it ourselves.
	// Returns 0 if all tests passed, 1 if any failed.
	code := m.Run()

	// Tests are done — release the Redis connection.
	testClient.Close()

	// Because we defined TestMain, Go hands exit control to us.
	// Without this, the process would always exit 0 even on failure.
	os.Exit(code)
}

func BenchmarkEnqueue(b *testing.B) {
	q := NewQueue(testClient, []string{"bench:high", "bench:mid", "bench:low"}, "bench")
	ctx := context.Background()
	job := NewJob("1", "bench-job", "payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(ctx, job)
	}
}

func BenchmarkDequeue(b *testing.B) {
	q := NewQueue(testClient, []string{"bench:high", "bench:mid", "bench:low"}, "bench")
	ctx := context.Background()
	job := NewJob("1", "bench-job", "payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q.Enqueue(ctx, job)
		b.StartTimer()
		q.Dequeue(ctx)
	}
}

func TestEnqueue(t *testing.T) {
	q := NewQueue(testClient, []string{"\x01:high", "\x02:mid", "\x03:low"}, "\x01")
	ctx := context.Background()
	job := NewJob("1", "bench-job", "payload")

	q.client.Del(ctx, q.Names...)
	q.Enqueue(ctx, job)

	count := q.client.LLen(ctx, q.Names[1]).Val()
	if count != 1 {
		t.Errorf("expected 1 job in queue, got %d", count)
	}
}

func TestDequeue(t *testing.T) {
	q := NewQueue(testClient, []string{"\x01:high", "\x02:mid", "\x03:low"}, "\x01")
	ctx := context.Background()
	job := NewJob("1", "bench-job", "payload")

	q.client.Del(ctx, q.Names...)
	q.Enqueue(ctx, job)
	q.Dequeue(ctx)

	count := q.client.LLen(ctx, q.Names[1]).Val()
	if count != 0 {
		t.Errorf("expected 0 jobs in queue, got %d", count)
	}
}

func TestRetry(t *testing.T) {
	q := NewQueue(testClient, []string{"\x01:high", "\x02:mid", "\x03:low"}, "\x01")
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)
	q.client.Del(ctx, "delayed:"+q.Name)

	job := NewJob("retry-1", "send-email", "bad@example.com")
	job.MaxRetries = 3

	worker := NewWorker(q, 1, nil)
	worker.Register("send-email", func(j Job, report func(pct int)) (string, error) {
		return "", fmt.Errorf("simulated failure")
	})

	q.Enqueue(ctx, job)
	dequeued, _ := q.Dequeue(ctx)
	worker.processJob(ctx, dequeued)

	delayed := q.client.ZCard(ctx, "delayed:"+q.Name).Val()
	if delayed != 1 {
		t.Errorf("expected job to be re-queued in delayed set, got %d", delayed)
	}
}

func TestDeadLetter(t *testing.T) {
	q := NewQueue(testClient, []string{"\x01:high", "\x02:mid", "\x03:low"}, "\x01")
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)
	q.client.Del(ctx, "delayed:"+q.Name)
	q.client.Del(ctx, "dead-letter:"+q.Name)

	job := NewJob("retry-1", "send-email", "bad@example.com")
	job.MaxRetries = 3

	worker := NewWorker(q, 1, nil)
	worker.Register("send-email", func(j Job, report func(pct int)) (string, error) {
		return "", fmt.Errorf("simulated failure")
	})

	job.Attempts = job.MaxRetries - 1
	q.Enqueue(ctx, job)
	dequeued, _ := q.Dequeue(ctx)
	worker.processJob(ctx, dequeued)

	count := q.client.LLen(ctx, "dead-letter:"+q.Name).Val()
	if count != 1 {
		t.Errorf("expected job in dead-letter, got %d", count)
	}
}

func TestOnCompleted(t *testing.T) {
	q := NewQueue(testClient, []string{"\x01:high", "\x02:mid", "\x03:low"}, "\x01")
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)

	job := NewJob("completed-1", "send-email", "user@example.com")

	worker := NewWorker(q, 1, nil)
	worker.Register("send-email", func(j Job, report func(pct int)) (string, error) {
		return "done", nil
	})

	fired := false
	worker.OnCompleted(func(j Job) {
		fired = true
	})

	q.Enqueue(ctx, job)
	dequeued, _ := q.Dequeue(ctx)
	worker.processJob(ctx, dequeued)

	if !fired {
		t.Error("expected OnCompleted callback to fire, but it did not")
	}
}

func TestOnFailed(t *testing.T) {
	q := NewQueue(testClient, []string{"\x01:high", "\x02:mid", "\x03:low"}, "\x01")
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)
	q.client.Del(ctx, "dead-letter:"+q.Name)

	job := NewJob("failed-1", "send-email", "bad@example.com")
	job.Attempts = job.MaxRetries - 1

	worker := NewWorker(q, 1, nil)
	worker.Register("send-email", func(j Job, report func(pct int)) (string, error) {
		return "", fmt.Errorf("simulated failure")
	})

	fired := false
	worker.OnFailed(func(j Job) {
		fired = true
	})

	q.Enqueue(ctx, job)
	dequeued, _ := q.Dequeue(ctx)
	worker.processJob(ctx, dequeued)

	if !fired {
		t.Error("expected OnFailed callback to fire, but it did not")
	}
}

func TestPriority(t *testing.T) {
	q := NewQueue(testClient, []string{"\x01:high", "\x02:mid", "\x03:low"}, "\x01")
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)

	low := NewJob("prio-low", "task", "payload")
	low.Priority = PriorityLow

	mid := NewJob("prio-mid", "task", "payload")
	mid.Priority = PriorityMid

	high := NewJob("prio-high", "task", "payload")
	high.Priority = PriorityHigh

	q.Enqueue(ctx, low)
	q.Enqueue(ctx, mid)
	q.Enqueue(ctx, high)

	expected := []string{"prio-high", "prio-mid", "prio-low"}
	for _, id := range expected {
		job, _ := q.Dequeue(ctx)
		if job.Id != id {
			t.Errorf("expected job %q, got %q", id, job.Id)
		}
	}
}

func TestStalledJobDetection(t *testing.T) {
	q := NewQueue(testClient, []string{"\x01:high", "\x02:mid", "\x03:low"}, "\x01")
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)
	q.client.Del(ctx, "active-jobs:"+q.Name)

	job := NewJob("stall-1", "send-email", "payload")
	q.Enqueue(ctx, job)

	// Simulate worker picking up the job but crashing before finishing
	dequeued, _ := q.Dequeue(ctx)
	q.client.ZAdd(ctx, "active-jobs:"+q.Name, redis.Z{
		Score:  float64(time.Now().Add(-2 * time.Minute).Unix()),
		Member: dequeued.Id,
	})

	q.DetectStalled(ctx, 1*time.Minute)

	count := q.client.LLen(ctx, q.Names[PriorityMid]).Val()
	if count != 1 {
		t.Errorf("expected stalled job to be requeued, got %d jobs in queue", count)
	}
}

func TestNoHandler(t *testing.T) {
	q := NewQueue(testClient, []string{"\x01:high", "\x02:mid", "\x03:low"}, "\x01")
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)
	q.client.Del(ctx, "delayed:"+q.Name)
	q.client.Del(ctx, "dead-letter:"+q.Name)

	job := NewJob("nohandler-1", "unknown-job-type", "payload")
	job.Attempts = job.MaxRetries - 1

	worker := NewWorker(q, 1, nil)

	q.Enqueue(ctx, job)
	dequeued, _ := q.Dequeue(ctx)
	worker.processJob(ctx, dequeued)

	count := q.client.LLen(ctx, "dead-letter:"+q.Name).Val()
	if count != 1 {
		t.Errorf("expected unhandled job in dead-letter, got %d", count)
	}
}

func TestRepeatableJob(t *testing.T) {
	q := NewQueue(testClient, []string{"\x01:high", "\x02:mid", "\x03:low"}, "\x01")
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)
	q.client.Del(ctx, "repeatable:"+q.Name)

	rj := RepeatableJob{
		Name:     "test-heartbeat",
		JobName:  "send-email",
		Payload:  "user@example.com",
		Priority: PriorityMid,
		Cron:     "@every 1m",
	}

	// Seed with a past timestamp so the scheduler sees it as due immediately.
	data, _ := json.Marshal(rj)
	q.client.ZAdd(ctx, "repeatable:"+q.Name, redis.Z{
		Score:  float64(time.Now().Add(-5 * time.Second).Unix()),
		Member: string(data),
	})

	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	q.tickRepeatable(ctx, parser)

	// One job instance should have been enqueued.
	count := q.client.LLen(ctx, q.Names[PriorityMid]).Val()
	if count != 1 {
		t.Errorf("expected 1 job enqueued, got %d", count)
	}

	// The repeatable definition should have been re-scheduled (still 1 entry in the set).
	remaining := q.client.ZCard(ctx, "repeatable:"+q.Name).Val()
	if remaining != 1 {
		t.Errorf("expected repeatable job to be re-scheduled, got %d entries in set", remaining)
	}
}

func TestRateLimit(t *testing.T) {
	q := NewQueue(testClient, []string{"\x01:high", "\x02:mid", "\x03:low"}, "\x01")
	ctx := context.Background()

    q.client.Del(ctx, "ratelimit:"+q.Name);
	maxTokens := 3
	refillRate := 1.0

	q.Allow(ctx, maxTokens, refillRate)
	q.Allow(ctx, maxTokens, refillRate)
	q.Allow(ctx, maxTokens, refillRate)
	allowed := q.Allow(ctx, maxTokens, refillRate)

	if(allowed) {
		t.Error("expected 4th call to be denied, but it was allowed")
	}
}