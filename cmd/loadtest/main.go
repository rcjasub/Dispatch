// Command loadtest drives gqueue with a synthetic workload and reports the
// baseline numbers we compare against later (metrics, Kubernetes, scaling):
//
//   - throughput (jobs/sec)
//   - wait time (enqueue -> processing start) per priority lane, p50/p95/max
//   - peak jobs per second, to check the rate limiter holds its cap
//   - duplicates and missing jobs, to check nothing is lost or run twice
//
// Two ways to run workers:
//
//	local  (-concurrency N>0): workers run inside this process. Used for the Step 1 baseline.
//	remote (-concurrency 0):   this process only produces; workers are cmd/worker processes
//	                           or Kubernetes pods on the same Redis and -queue. Completions
//	                           are observed via the job:completed / job:failed pub/sub channels.
//
// Examples:
//
//	go run ./cmd/loadtest -concurrency 1
//	go run ./cmd/loadtest -concurrency 16 -work 5ms
//	go run ./cmd/loadtest -concurrency 16 -work 5ms -produce-rate 2500          # priority under steady load
//	go run ./cmd/loadtest -concurrency 16 -work 5ms -produce-rate 2500 -fifo    # same load, no priorities
//	go run ./cmd/loadtest -concurrency 16 -rate 100 -jobs 2000                  # rate limiter cap
//	go run ./cmd/loadtest -concurrency 0 -queue default -jobs 20000             # against cmd/worker or K8s pods
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rcjasub/gqueue"
	"github.com/redis/go-redis/v9"
)

type config struct {
	redisAddr   string
	queue       string
	jobs        int
	concurrency int
	work        time.Duration
	fifo        bool
	rate        int
	produceRate int
	maxRetries  int
	producers   int
	linger      time.Duration
	timeout     time.Duration
	seed        uint64
}

func (c config) remote() bool { return c.concurrency == 0 }

func parseFlags() config {
	var c config
	flag.StringVar(&c.redisAddr, "redis", "localhost:6379", "Redis address")
	flag.StringVar(&c.queue, "queue", "", "queue name (default: unique per run locally, \"default\" in remote mode)")
	flag.IntVar(&c.jobs, "jobs", 10000, "number of jobs to enqueue")
	flag.IntVar(&c.concurrency, "concurrency", 1, "local worker goroutines; 0 = remote workers")
	flag.DurationVar(&c.work, "work", 0, "local mode: simulated work per job (e.g. 5ms); 0 = empty handler")
	flag.BoolVar(&c.fifo, "fifo", false, "put every job in the mid lane (plain FIFO baseline)")
	flag.IntVar(&c.rate, "rate", 0, "local mode: rate limit in jobs/sec (0 = off). Remote workers use RATE_LIMIT.")
	flag.IntVar(&c.produceRate, "produce-rate", 0, "enqueue at this many jobs/sec while workers run (0 = enqueue everything up front)")
	flag.IntVar(&c.maxRetries, "max-retries", 3, "attempts per job before it goes to the dead-letter list")
	flag.IntVar(&c.producers, "producers", 8, "goroutines calling Enqueue (one goroutine tops out near 1 job per 2 round trips)")
	flag.DurationVar(&c.linger, "linger", 0, "after all jobs finish, keep listening this long (e.g. 45s) so late re-runs by the stalled detector are counted")
	flag.DurationVar(&c.timeout, "timeout", 5*time.Minute, "give up after this long")
	flag.Uint64Var(&c.seed, "seed", 42, "random seed for the priority mix")
	flag.Parse()
	return c
}

// recorder collects measurements from many goroutines at once, so every field
// after mu is guarded by it.
type recorder struct {
	start    time.Time
	total    int
	done     chan struct{}
	doneOnce sync.Once

	mu         sync.Mutex
	seen       map[string]int // job id -> times finished; >1 means it ran twice
	failed     int
	duplicates int
	waits      map[gqueue.JobPriority][]time.Duration
	perSecond  map[int]int // second since start -> jobs in that second
}

func newRecorder(total int) *recorder {
	return &recorder{
		total:     total,
		done:      make(chan struct{}),
		seen:      make(map[string]int, total),
		waits:     make(map[gqueue.JobPriority][]time.Duration),
		perSecond: make(map[int]int),
	}
}

func (r *recorder) second(t time.Time) int { return int(t.Sub(r.start) / time.Second) }

// jobStarted (local mode) runs at the top of the handler: after the worker
// dequeued the job and after the rate limiter let it through.
func (r *recorder) jobStarted(j gqueue.Job) {
	now := time.Now()
	r.mu.Lock()
	r.waits[j.Priority] = append(r.waits[j.Priority], now.Sub(j.CreatedAt))
	r.perSecond[r.second(now)]++
	r.mu.Unlock()
}

// jobFinished records a completion or dead-letter. In remote mode we can't see
// handler starts, so per-second counts are taken here instead.
func (r *recorder) jobFinished(id string, failed, countPerSecond bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen[id]++
	if r.seen[id] > 1 {
		r.duplicates++
		return
	}
	if failed {
		r.failed++
	}
	if countPerSecond {
		r.perSecond[r.second(time.Now())]++
	}
	if len(r.seen) == r.total {
		r.doneOnce.Do(func() { close(r.done) })
	}
}

func main() {
	cfg := parseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.redisAddr,
		// Every worker goroutine holds a connection during BRPOP, and the producer
		// needs its own. Make sure the pool is never the bottleneck we measure.
		PoolSize: cfg.concurrency + cfg.producers + 10,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Fprintln(os.Stderr, "cannot reach Redis:", err)
		os.Exit(1)
	}

	// runID makes every job id unique across runs: job hashes are keyed
	// "job:<id>" and are NOT namespaced by queue, so reusing ids would collide.
	runID := fmt.Sprintf("lt%d", time.Now().UnixNano())
	queueName := cfg.queue
	if queueName == "" {
		if cfg.remote() {
			queueName = "default" // must match QUEUE_NAME on the workers
		} else {
			// Fresh lanes and a fresh token bucket ("ratelimit:"+queue) every run.
			queueName = runID
		}
	}
	lanes := []string{queueName + ":high", queueName + ":mid", queueName + ":low"}
	queue := gqueue.NewQueue(rdb, lanes, queueName)
	rec := newRecorder(cfg.jobs)

	var worker *gqueue.Worker
	if cfg.remote() {
		if err := watchRemote(ctx, rdb, runID, rec); err != nil {
			fmt.Fprintln(os.Stderr, "subscribe:", err)
			os.Exit(1)
		}
	} else {
		var rl *gqueue.RateLimitConfig
		if cfg.rate > 0 {
			// Bucket size == rate: allows a one-second burst, then refills at `rate`/sec.
			rl = &gqueue.RateLimitConfig{MaxTokens: cfg.rate, RefillRate: float64(cfg.rate)}
		}
		worker = gqueue.NewWorker(queue, cfg.concurrency, rl)
		worker.Register("bench", func(j gqueue.Job, _ func(int)) (string, error) {
			rec.jobStarted(j)
			if cfg.work > 0 {
				time.Sleep(cfg.work)
			}
			return "", nil
		})
		worker.OnCompleted(func(j gqueue.Job) { rec.jobFinished(j.Id, false, false) })
		worker.OnFailed(func(j gqueue.Job) { rec.jobFinished(j.Id, true, false) })
	}

	rng := rand.New(rand.NewPCG(cfg.seed, cfg.seed))
	makeJob := func(i int) gqueue.Job {
		// NewJob stamps CreatedAt = time.Now(), so build the job right before enqueueing it.
		j := gqueue.NewJob(fmt.Sprintf("%s-%d", runID, i), "bench", "")
		j.Priority = pickPriority(rng, cfg.fifo)
		j.MaxRetries = cfg.maxRetries
		return j
	}
	startWorkers := func() {
		if worker != nil {
			worker.Start(ctx)
		}
	}

	printHeader(cfg, queueName)

	var enqueueDur time.Duration
	if cfg.produceRate == 0 {
		// Prefill: measure pure drain speed. The clock starts once the backlog is in place.
		// (Remote workers start draining immediately, so in remote mode this is
		// "enqueue + drain" unless you scale the workers to 0 first.)
		t := time.Now()
		if cfg.remote() {
			rec.start = t
		}
		jobs, finish := startEnqueuers(ctx, queue, cfg.producers)
		for i := range cfg.jobs {
			jobs <- makeJob(i)
		}
		finish()
		enqueueDur = time.Since(t)
		if !cfg.remote() {
			rec.start = time.Now()
		}
		startWorkers()
	} else {
		// Steady producer: workers run while jobs arrive, which is what makes priority visible.
		rec.start = time.Now()
		startWorkers()
		jobs, finish := startEnqueuers(ctx, queue, cfg.producers)
		go produce(ctx, cfg, makeJob, jobs, finish)
	}

	timedOut := false
	select {
	case <-rec.done:
	case <-time.After(cfg.timeout):
		timedOut = true
	}
	elapsed := time.Since(rec.start)
	if cfg.linger > 0 {
		time.Sleep(cfg.linger) // duplicates arriving now are still counted
	}

	cancel()
	if worker != nil {
		worker.Wait()
	}

	printResults(cfg, rec, elapsed, enqueueDur, timedOut)
	if cfg.remote() {
		reportRuns(rdb, runID, queueName, cfg.jobs)
	}
	cleanup(rdb, runID, queueName, cfg)
}

// watchRemote subscribes to the worker's pub/sub events and feeds the recorder.
// It must be subscribed BEFORE the first enqueue, or early completions are missed:
// pub/sub does not store messages for subscribers that are not listening yet.
func watchRemote(ctx context.Context, rdb *redis.Client, runID string, rec *recorder) error {
	ps := rdb.Subscribe(ctx, "job:completed", "job:failed")
	// Receive waits for the subscription confirmation from Redis.
	if _, err := ps.Receive(ctx); err != nil {
		return err
	}
	// Big buffer: if we fall behind, go-redis would eventually drop messages.
	ch := ps.Channel(redis.WithChannelSize(50000))
	prefix := runID + "-"
	go func() {
		defer ps.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				// The channels are shared by every queue on this Redis; keep only our jobs.
				if strings.HasPrefix(msg.Payload, prefix) {
					rec.jobFinished(msg.Payload, msg.Channel == "job:failed", true)
				}
			}
		}
	}()
	return nil
}

// pickPriority gives a realistic mix: few urgent jobs, most of them background work.
func pickPriority(rng *rand.Rand, fifo bool) gqueue.JobPriority {
	if fifo {
		return gqueue.PriorityMid
	}
	switch n := rng.IntN(100); {
	case n < 10:
		return gqueue.PriorityHigh
	case n < 40:
		return gqueue.PriorityMid
	default:
		return gqueue.PriorityLow
	}
}

// produce enqueues at cfg.produceRate jobs/sec. A ticker can't fire every 0.5ms
// reliably (Windows timers are ~1ms or coarser), so we tick every 10ms and enqueue
// however many jobs are "owed" by now. That keeps the average rate accurate.
//
// Jobs are built here (one goroutine, so the rng needs no lock) and handed to the
// enqueuer pool. The channel is unbuffered, so CreatedAt is stamped right before a
// free enqueuer picks the job up.
func produce(ctx context.Context, cfg config, makeJob func(int) gqueue.Job, jobs chan<- gqueue.Job, finish func()) {
	defer finish()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	start := time.Now()
	sent := 0
	for sent < cfg.jobs {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		owed := int(time.Since(start).Seconds()*float64(cfg.produceRate)) - sent
		for ; owed > 0 && sent < cfg.jobs; owed-- {
			select {
			case jobs <- makeJob(sent):
				sent++
			case <-ctx.Done():
				return
			}
		}
	}
}

// startEnqueuers runs n goroutines that Enqueue whatever arrives on the returned
// channel. finish closes the channel and waits until every job is in Redis.
//
// Why a pool: each Enqueue is 2 sequential round trips (HSET + LPUSH), so one
// goroutine tops out near 1/(2*RTT), ~1000 jobs/s here. That made the first
// priority test fail: the producer was slower than the workers, no backlog
// formed, and every lane showed the same wait.
func startEnqueuers(ctx context.Context, q *gqueue.Queue, n int) (chan<- gqueue.Job, func()) {
	ch := make(chan gqueue.Job)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			for j := range ch {
				if err := q.Enqueue(ctx, j); err != nil && ctx.Err() == nil {
					fmt.Fprintln(os.Stderr, "enqueue:", err)
				}
			}
		})
	}
	var once sync.Once
	return ch, func() { once.Do(func() { close(ch); wg.Wait() }) }
}

func printHeader(cfg config, queueName string) {
	mode := "prefill"
	if cfg.produceRate > 0 {
		mode = fmt.Sprintf("steady producer @ %d jobs/s", cfg.produceRate)
	}
	workers := fmt.Sprintf("local, concurrency=%d, work=%v", cfg.concurrency, cfg.work)
	if cfg.remote() {
		workers = "remote (cmd/worker / Kubernetes)"
	}
	prio := "mixed (10% high / 30% mid / 60% low)"
	if cfg.fifo {
		prio = "FIFO (all mid)"
	}
	limit := "off"
	if cfg.rate > 0 {
		limit = fmt.Sprintf("%d jobs/s", cfg.rate)
	} else if cfg.remote() {
		limit = "set on workers (RATE_LIMIT)"
	}
	fmt.Printf("queue=%s  jobs=%d  mode=%s\nworkers=%s\npriority=%s  rate-limit=%s\n\n",
		queueName, cfg.jobs, mode, workers, prio, limit)
}

func printResults(cfg config, rec *recorder, elapsed, enqueueDur time.Duration, timedOut bool) {
	rec.mu.Lock()
	defer rec.mu.Unlock()

	finished := len(rec.seen)
	if timedOut {
		fmt.Printf("TIMED OUT after %v\n", cfg.timeout)
	}
	if enqueueDur > 0 {
		fmt.Printf("enqueue:     %d jobs in %v (%.0f jobs/s)\n", cfg.jobs, enqueueDur.Round(time.Millisecond),
			float64(cfg.jobs)/enqueueDur.Seconds())
	}
	fmt.Printf("processing:  %d jobs in %v (%.0f jobs/s)\n", finished, elapsed.Round(time.Millisecond),
		float64(finished)/elapsed.Seconds())
	fmt.Printf("integrity:   %d dead-lettered, %d duplicates, %d missing\n\n",
		rec.failed, rec.duplicates, cfg.jobs-finished)

	if !cfg.remote() {
		fmt.Println("wait time (enqueue -> start)")
		fmt.Printf("  %-6s %7s %10s %10s %10s\n", "lane", "jobs", "p50", "p95", "max")
		for _, p := range []gqueue.JobPriority{gqueue.PriorityHigh, gqueue.PriorityMid, gqueue.PriorityLow} {
			w := rec.waits[p]
			if len(w) == 0 {
				continue
			}
			slices.Sort(w)
			fmt.Printf("  %-6s %7d %10v %10v %10v\n", p, len(w),
				percentile(w, 0.50), percentile(w, 0.95), w[len(w)-1].Round(time.Microsecond))
		}
	} else {
		fmt.Println("wait time: see gqueue_job_wait_seconds in Prometheus (handlers run in other processes)")
	}

	if cfg.rate > 0 || cfg.remote() {
		peakSec, peak := 0, 0
		var steady []int // every full second except the first (burst) and last (partial)
		last := int(elapsed / time.Second)
		for s := 0; s <= last; s++ {
			n := rec.perSecond[s]
			if n > peak {
				peakSec, peak = s, n
			}
			if s > 0 && s < last {
				steady = append(steady, n)
			}
		}
		what := "started"
		if cfg.remote() {
			what = "finished"
		}
		fmt.Printf("\njobs %s per second", what)
		if cfg.rate > 0 {
			fmt.Printf(" (cap %d/s)", cfg.rate)
		}
		fmt.Println()
		note := ""
		if cfg.rate > 0 || cfg.remote() {
			note = " (with a rate limit: the bucket starts full, so a burst is expected)"
		}
		fmt.Printf("  first second:    %d%s\n", rec.perSecond[0], note)
		fmt.Printf("  peak second:     %d (second #%d)\n", peak, peakSec)
		if len(steady) > 0 {
			slices.Sort(steady)
			fmt.Printf("  steady max/min:  %d / %d\n", steady[len(steady)-1], steady[0])
		}
	}
}

// reportRuns separates two things pub/sub alone cannot tell apart:
//   - "duplicates" above = a completion event was *announced* twice;
//   - runs > 1 here      = a handler *executed* twice (workers need TRACK_RUNS=true).
//
// It also lists active-jobs entries this run left behind. Every finished job
// should have been ZREM'd, so a leftover means a crash, or a ZREM that failed.
func reportRuns(rdb *redis.Client, runID, queueName string, jobs int) {
	ctx := context.Background()
	pipe := rdb.Pipeline()
	runs := make([]*redis.StringCmd, jobs)
	for i := range jobs {
		runs[i] = pipe.HGet(ctx, fmt.Sprintf("job:%s-%d", runID, i), "runs")
	}
	active := pipe.ZRangeWithScores(ctx, "active-jobs:"+queueName, 0, -1)
	_, _ = pipe.Exec(ctx) // missing fields come back as redis.Nil; checked per command below

	hist := map[int]int{}
	noData := 0
	var multi []string
	for i, c := range runs {
		n, err := c.Int()
		if err != nil {
			noData++
			continue
		}
		hist[n]++
		if n > 1 && len(multi) < 20 {
			multi = append(multi, fmt.Sprintf("%s-%d x%d", runID, i, n))
		}
	}
	fmt.Println("\nhandler executions per job")
	if noData == jobs {
		fmt.Println("  no data: start workers with TRACK_RUNS=true")
	} else {
		twice := 0
		for n, c := range hist {
			if n > 1 {
				twice += c
			}
		}
		fmt.Printf("  ran once: %d   ran 2+ times: %d   no record: %d\n", hist[1], twice, noData)
		for _, m := range multi {
			fmt.Println("   ", m)
		}
	}

	var leaked []string
	for _, z := range active.Val() {
		if id := z.Member.(string); strings.HasPrefix(id, runID+"-") {
			leaked = append(leaked, fmt.Sprintf("%s (age %.0fs)", id, float64(time.Now().Unix())-z.Score))
		}
	}
	fmt.Printf("  left in active-jobs: %d\n", len(leaked))
	for _, l := range leaked {
		fmt.Println("   ", l)
	}
}

// percentile expects a sorted slice. Nearest-rank method: the smallest value
// such that at least p of the samples are <= it.
func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := max(int(math.Ceil(p*float64(len(sorted))))-1, 0)
	return sorted[idx].Round(time.Microsecond)
}

// cleanup removes the per-job hashes so repeated runs don't pile up in Redis.
// Per-queue keys are only removed for the throwaway per-run queue; a shared
// queue (-queue / remote mode) belongs to the running workers.
func cleanup(rdb *redis.Client, runID, queueName string, cfg config) {
	ctx := context.Background()
	pipe := rdb.Pipeline()
	for i := range cfg.jobs {
		pipe.Del(ctx, fmt.Sprintf("job:%s-%d", runID, i))
	}
	if queueName == runID {
		pipe.Del(ctx, runID+":high", runID+":mid", runID+":low",
			"ratelimit:"+runID, "active-jobs:"+runID, "delayed:"+runID, "dead-letter:"+runID)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "cleanup:", err)
	}
}
