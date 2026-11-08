# gqueue

A Redis-backed job queue library for Go. Inspired by BullMQ, built to explore distributed systems concepts like job persistence, fault tolerance, and concurrency control, then load-tested, monitored, and run on Kubernetes to see how it actually behaves.

## Features

- **Priority queues**: high, mid, and low priority lanes; urgent jobs always run first
- **Crash-safe job claiming**: taking a job and recording who holds it is one atomic Lua script, so a worker crash never loses a job
- **Delayed jobs**: schedule a job to run after a duration
- **Repeatable jobs**: cron-style scheduling (`@every 5s`, `@daily`, standard 6-field expressions)
- **Retry with exponential backoff**: failed jobs are retried automatically with increasing delays
- **Dead-letter queue**: jobs that exhaust retries are moved here for inspection and manual retry
- **Stalled job detection**: if a worker dies mid-job, the job is requeued instead of lost
- **Rate limiting**: token bucket in a Redis Lua script, shared by every worker and every replica
- **Metrics**: pluggable `Metrics` interface, with a Prometheus implementation in `gqueue/prom`
- **Job progress and results**: workers report percentage completion; results and timestamps are stored in Redis
- **Pub/sub events**: subscribe to `job:completed` and `job:failed` for real-time updates
- **Pause/resume**: stop a queue without killing workers
- **Graceful shutdown**: on cancel, workers stop taking jobs but finish the ones they hold
- **Safe with many replicas**: the scheduler and stalled detector can run on every pod without duplicating work

## Requirements

- Go 1.25+
- Redis 6.2+

## Installation

```bash
go get github.com/rcjasub/gqueue
```

## Quick Start

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
queue := gqueue.NewQueue(rdb, []string{"myqueue:high", "myqueue:mid", "myqueue:low"}, "myqueue")
worker := gqueue.NewWorker(queue, 3, nil) // 3 concurrent goroutines, no rate limit

worker.Register("send-email", func(job gqueue.Job, report func(pct int)) (string, error) {
    report(50)
    // do work...
    report(100)
    return "done", nil
})

worker.OnCompleted(func(job gqueue.Job) {
    fmt.Println("completed:", job.Id)
})

ctx := context.Background()
go queue.StartScheduler(ctx)                       // delayed jobs and retry backoff
go queue.StartStalledDetector(ctx, 30*time.Second) // recover jobs from crashed workers
worker.Start(ctx)

job := gqueue.NewJob("job-1", "send-email", "user@example.com")
queue.Enqueue(ctx, job)

worker.Wait()
```

## API

### Queue

```go
// Create a queue. Names are the Redis list keys per priority (high→mid→low order).
gqueue.NewQueue(client *redis.Client, names []string, name string) *Queue

// Enqueue a job. Uses Delay field if set, otherwise enqueues immediately.
queue.Enqueue(ctx, job) error

// Dequeue claims the highest-priority job: it is removed from its lane and recorded
// in active-jobs in one atomic step. Waits until a job is available; returns false
// only when ctx is cancelled. The caller owns the job: finish it, or the stalled
// detector will requeue it.
queue.Dequeue(ctx) (Job, bool)

// Delayed job scheduler: run in a goroutine.
go queue.StartScheduler(ctx)

// Repeatable job scheduler: run in a goroutine.
go queue.StartRepeatableScheduler(ctx)

// Register a repeatable (cron) job.
queue.AddRepeatable(ctx, gqueue.RepeatableJob{
    Name:    "unique-key",
    JobName: "send-email",       // must match a registered handler
    Payload: "user@example.com",
    Cron:    "@every 1m",
})

// Dead-letter inspection.
queue.ListDead(ctx) ([]Job, error)
queue.RetryDead(ctx, jobId) error

// Stalled job detection: requeues jobs active longer than the threshold.
// The threshold must be longer than your slowest job.
go queue.StartStalledDetector(ctx, 30*time.Second)

// Queue depths per lane, delayed, active, dead-letter (one pipelined round trip).
queue.Stats(ctx) (gqueue.Stats, error)

// Pause and resume.
queue.Pause(ctx)
queue.Resume(ctx)

// Pub/sub.
pubsub := queue.Subscribe(ctx, "job:completed", "job:failed")
```

### Worker

```go
// Create a worker. Pass *RateLimitConfig to enable rate limiting, nil to disable.
gqueue.NewWorker(queue *Queue, concurrency int, rateLimit *gqueue.RateLimitConfig) *Worker

// Register a handler for a job type.
worker.Register("job-name", func(job gqueue.Job, report func(pct int)) (string, error) {
    report(50)  // report progress 0-100
    return "result", nil
})

// Optional callbacks.
worker.OnCompleted(func(job gqueue.Job) { ... })
worker.OnFailed(func(job gqueue.Job) { ... })

// Optional metrics sink (see Metrics below).
worker.SetMetrics(m)

// Start processing. Non-blocking: spawns goroutines internally.
worker.Start(ctx)

// After ctx is cancelled: wait for in-flight jobs to finish.
worker.Wait()
```

### Rate Limiting

```go
worker := gqueue.NewWorker(queue, 4, &gqueue.RateLimitConfig{
    MaxTokens:  10,  // burst capacity
    RefillRate: 5.0, // tokens added per second
})
```

A token bucket in a Redis Lua script. Redis runs the script atomically and it uses Redis's own clock, so one limit holds across every goroutine, process, and pod sharing the queue. If Redis is unreachable, the limiter lets jobs through (fails open) rather than stopping all work.

### Metrics

```go
import "github.com/rcjasub/gqueue/prom"

m, err := prom.New(queue, prometheus.DefaultRegisterer)
worker.SetMetrics(m)
http.Handle("/metrics", promhttp.Handler())
```

| Metric | Type | Meaning |
|---|---|---|
| `gqueue_waiting_jobs{priority}` | gauge | jobs waiting per lane |
| `gqueue_delayed_jobs`, `gqueue_active_jobs`, `gqueue_dead_letter_jobs` | gauge | backoff backlog, in-flight, dead letters |
| `gqueue_jobs_processed_total{priority,outcome}` | counter | outcome = completed, retried, dead_letter |
| `gqueue_job_wait_seconds{priority}` | histogram | enqueue → handler start |
| `gqueue_job_processing_seconds{priority,outcome}` | histogram | handler run time |

Queue depths are read from Redis at scrape time. With several replicas every pod reports the same depth, so aggregate with `max()`, not `sum()`. Latencies are histograms, so `histogram_quantile()` gives a correct p95 across all pods.

### Job

```go
job := gqueue.NewJob("unique-id", "handler-name", "payload string")

job.Priority  = gqueue.PriorityHigh  // PriorityHigh, PriorityMid, PriorityLow
job.Delay     = 10 * time.Second     // run after 10s
job.MaxRetries = 5                   // default is 3
```

## How It Works

```
Enqueue → Redis list per priority (high / mid / low)
             ↓
Claim (one Lua script): check pause → pop high → mid → low → record in active-jobs
             ↓
Handler runs → success: ack script (mark completed + clear active-jobs + publish)
             → failure: retry with backoff, or dead-letter after max retries
```

**Claiming is atomic.** A worker that crashes leaves its job either still in its lane or recorded in `active-jobs`, where the stalled detector finds it and requeues it. There is no moment where a taken job is untracked. A Lua script can't block, so when every lane is empty, workers poll with a short backoff (2 ms up to 50 ms).

**Delivery is at-least-once.** A job interrupted mid-handler by a crash runs again after the stalled threshold, so handlers should be safe to run twice (idempotent).

Delayed jobs live in a Redis sorted set scored by Unix timestamp. The scheduler polls every second and moves ready jobs into the priority lists. Every replica can run it: a job is only moved by the replica whose `ZREM` succeeds.

## Running it

### Worker binary

`cmd/worker` is a ready-to-deploy worker configured by environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `QUEUE_NAME` | `default` | lanes are `<QUEUE_NAME>:high\|mid\|low` |
| `CONCURRENCY` | `4` | goroutines per process |
| `RATE_LIMIT` | `0` | jobs/sec shared by all replicas (0 = off) |
| `WORK` / `WORK_MODE` | `5ms` / `sleep` | simulated job; `cpu` burns CPU (for CPU autoscaling) |
| `FAIL_RATE` | `0` | fraction of runs that fail (exercises retries) |
| `STALLED_THRESHOLD` | `30s` | when an active job counts as abandoned |
| `TRACK_RUNS` | `false` | count handler runs per job (for duplicate detection) |
| `HTTP_ADDR` | `:2112` | serves `/metrics`, `/healthz`, `/readyz` |

### Load test

`cmd/loadtest` enqueues a mix of priorities (10% high / 30% mid / 60% low) and reports throughput, wait time per lane (p50/p95/max), jobs per second, duplicates, and missing jobs.

```bash
go run ./cmd/loadtest -concurrency 16 -work 5ms                          # throughput, workers in-process
go run ./cmd/loadtest -concurrency 16 -work 5ms -produce-rate 3100        # priority under overload
go run ./cmd/loadtest -concurrency 16 -work 5ms -produce-rate 3100 -fifo  # same load, plain FIFO
go run ./cmd/loadtest -concurrency 16 -rate 100 -jobs 2000                # rate limiter
go run ./cmd/loadtest -concurrency 0 -queue default -jobs 20000           # against remote workers
```

Use `-redis host:port` to point it at a different Redis.

### Local monitoring stack (Docker Compose)

```bash
docker compose -f deploy/local/docker-compose.yml up -d --build
docker compose -f deploy/local/docker-compose.yml up -d --scale worker=4
go run ./cmd/loadtest -redis 127.0.0.1:6380 -concurrency 0 -jobs 20000 -produce-rate 1000
```

Redis is exposed on `127.0.0.1:6380`, Prometheus on http://localhost:9090, and Grafana on http://localhost:3000 with a preloaded "gqueue" dashboard.

### Kubernetes (kind)

```bash
go install sigs.k8s.io/kind@v0.29.0
kind create cluster --name gqueue --config deploy/kind-config.yaml

# metrics-server, needed by the autoscaler
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml
kubectl -n kube-system patch deployment metrics-server --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

docker build -t gqueue-worker:dev .
kind load docker-image gqueue-worker:dev --name gqueue
kubectl apply -f deploy/k8s/

# generate load from inside the cluster (the image also contains /loadtest)
kubectl -n gqueue run loadtest --image=gqueue-worker:dev --image-pull-policy=IfNotPresent \
  --restart=Never --command -- /loadtest -redis redis:6379 -concurrency 0 -jobs 20000
kubectl -n gqueue logs -f loadtest
```

On Windows Git Bash, set `MSYS_NO_PATHCONV=1` first, or `/loadtest` gets rewritten into a Windows path.

`deploy/k8s` contains Redis, the worker Deployment (settings in a ConfigMap), a CPU-based HorizontalPodAutoscaler (1–8 pods), and an in-cluster Prometheus. Delete the autoscaler before scaling by hand: `kubectl -n gqueue delete hpa worker`.

## Results

Measured on one laptop (16 cores, Docker Desktop, kind with 3 nodes) with simulated 5 ms jobs. These show behavior and trends, not production capacity.

| Test | Result |
|---|---|
| Throughput, 8 worker pods | ~5,000 jobs/sec |
| Scaling, 1 → 8 pods | 7.3x (686 → 5,030 jobs/sec) |
| Urgent-job p95 wait vs FIFO, 25% overload | 4.3 ms vs 1.51 s (99% less) |
| Rate limit, cap 100/s across 4 pods | steady 94–106/s |
| Graceful pod delete during load | 0 lost, 0 duplicates |
| 4 × `kill -9` during 150,000 jobs | 0 lost (3 lost before atomic claiming) |
| Autoscaler under a traffic spike | ~30 s to react, 1 → 8 pods, queue drained in ~87 s |

Priority does not make work faster; it changes who waits. In the same test, low-priority p95 wait rose from 1.51 s to 2.12 s.

## Testing

Tests need a Redis instance. They use `localhost:6379` by default; set `REDIS_ADDR` to use another one:

```bash
REDIS_ADDR=127.0.0.1:6380 go test ./...
```

## Known limitations

- **At-least-once delivery**: a job interrupted by a crash runs again, so handlers should be idempotent.
- **Retry and dead-letter steps aren't atomic yet**: a crash there can re-run a job that already failed once.
- **Idle polling**: an idle worker can take up to 50 ms to pick up a new job.
- **Single Redis**: Redis is a single point of failure, and its CPU is the throughput ceiling. The scripts assume one Redis instance, not Redis Cluster.
- **CPU-based autoscaling reacts slowly** to queue growth. Scaling on queue depth (for example with KEDA) would react sooner.
