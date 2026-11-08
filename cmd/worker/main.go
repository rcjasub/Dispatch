// Command worker is the deployable gqueue worker: the binary that goes in the
// Docker image and runs as N replicas in Kubernetes.
//
// All settings come from environment variables so a Kubernetes ConfigMap can
// set them without rebuilding the image:
//
//	REDIS_ADDR         localhost:6379
//	QUEUE_NAME         default         lanes are <QUEUE_NAME>:high|mid|low
//	CONCURRENCY        4               worker goroutines per process (per pod)
//	RATE_LIMIT         0               jobs/sec across ALL replicas (0 = off)
//	WORK               5ms             simulated work per job
//	WORK_MODE          sleep           sleep = waits (I/O-like), cpu = burns CPU (needed for CPU-based HPA)
//	FAIL_RATE          0               fraction of runs that fail, to exercise retries and dead letters
//	STALLED_THRESHOLD  30s             a job active longer than this is presumed orphaned and requeued
//	HTTP_ADDR          :2112           serves /metrics, /healthz, /readyz
//	TRACK_RUNS         false           count handler runs per job (HINCRBY job:<id> runs), so the
//	                                   load test can detect a job that executed twice
package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rcjasub/gqueue"
	"github.com/rcjasub/gqueue/prom"
	"github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := env("REDIS_ADDR", "localhost:6379")
	queueName := env("QUEUE_NAME", "default")
	concurrency := envInt("CONCURRENCY", 4)
	rate := envInt("RATE_LIMIT", 0)
	work := envDuration("WORK", 5*time.Millisecond)
	workMode := env("WORK_MODE", "sleep")
	failRate := envFloat("FAIL_RATE", 0)
	stalled := envDuration("STALLED_THRESHOLD", 30*time.Second)
	httpAddr := env("HTTP_ADDR", ":2112")
	trackRuns := env("TRACK_RUNS", "false") == "true"

	// SIGTERM is what Kubernetes sends when it stops a pod; SIGINT is Ctrl+C.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, PoolSize: concurrency + 10})
	q := gqueue.NewQueue(rdb, []string{queueName + ":high", queueName + ":mid", queueName + ":low"}, queueName)

	var rl *gqueue.RateLimitConfig
	if rate > 0 {
		rl = &gqueue.RateLimitConfig{MaxTokens: rate, RefillRate: float64(rate)}
	}
	w := gqueue.NewWorker(q, concurrency, rl)

	m, err := prom.New(q, prometheus.DefaultRegisterer)
	if err != nil {
		log.Fatalf("metrics: %v", err)
	}
	w.SetMetrics(m)

	w.Register("bench", func(j gqueue.Job, _ func(int)) (string, error) {
		if trackRuns {
			// Pub/sub events only show what was *announced*. This counts what actually
			// *executed*, which is what at-least-once delivery is about.
			rdb.HIncrBy(context.Background(), "job:"+j.Id, "runs", 1)
		}
		if workMode == "cpu" {
			burnCPU(work)
		} else {
			time.Sleep(work)
		}
		if failRate > 0 && rand.Float64() < failRate {
			return "", errors.New("simulated failure")
		}
		return "ok", nil
	})

	srv := &http.Server{Addr: httpAddr, Handler: routes(rdb)}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()

	log.Printf("worker up: queue=%s concurrency=%d rate=%d work=%v mode=%s fail=%.2f redis=%s",
		queueName, concurrency, rate, work, workMode, failRate, redisAddr)

	go q.StartScheduler(ctx)                // moves delayed / retry-backoff jobs back to their lane
	go q.StartStalledDetector(ctx, stalled) // requeues jobs owned by a worker that died
	w.Start(ctx)

	<-ctx.Done()
	log.Printf("shutdown: finishing in-flight jobs")
	w.Wait() // workers stop dequeuing, but jobs already taken run to completion

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = rdb.Close()
	log.Printf("shutdown: done")
}

func routes(rdb *redis.Client) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	// Liveness: the process is running. Deliberately does NOT check Redis:
	// if Redis is down, restarting every worker pod would not fix it.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// Readiness: can this pod do work right now?
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unreachable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// burnCPU keeps one core busy for d. time.Sleep uses no CPU, so a sleeping
// handler would never trigger a CPU-based autoscaler no matter how deep the queue.
func burnCPU(d time.Duration) {
	deadline := time.Now().Add(d)
	buf := []byte("gqueue")
	for time.Now().Before(deadline) {
		sum := sha256.Sum256(buf)
		buf = sum[:]
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v, err := strconv.Atoi(env(key, strconv.Itoa(def)))
	if err != nil {
		log.Fatalf("%s: %v", key, err)
	}
	return v
}

func envFloat(key string, def float64) float64 {
	v, err := strconv.ParseFloat(env(key, fmt.Sprint(def)), 64)
	if err != nil {
		log.Fatalf("%s: %v", key, err)
	}
	return v
}

func envDuration(key string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(env(key, def.String()))
	if err != nil {
		log.Fatalf("%s: %v", key, err)
	}
	return v
}
