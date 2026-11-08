package gqueue

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Reliable claim / ack.
//
// The old dequeue was two separate commands:
//
//	BRPOP lanes          -> Redis removes the job from its lane
//	ZADD active-jobs id  -> the stalled detector can now see it
//
// A worker that crashed between them took the job with it: it was in no lane
// and not in active-jobs, so nothing could ever find it. Crash testing on
// Kubernetes lost 3 of 150,000 jobs exactly this way.
//
// claimScript does pop + record as ONE script. Redis runs a script atomically:
// either both writes happen or neither does, so a crash can only leave a job in
// its lane (not taken yet) or in active-jobs (recoverable by DetectStalled).
//
// Why not BLMOVE (the classic "reliable queue" command)? It blocks on a single
// list, and gqueue has three lanes that must be checked in priority order.
// A script can check all three, but scripts cannot block, so claim() polls
// with a short backoff when every lane is empty.
var claimScript = redis.NewScript(`
-- KEYS[1] paused flag   KEYS[2] active-jobs zset   KEYS[3] poison list
-- KEYS[4..] lanes, highest priority first
-- ARGV[1] worker name   ARGV[2] startedAt (caller's clock, display only)
if redis.call('EXISTS', KEYS[1]) == 1 then
    return -1
end
for i = 4, #KEYS do
    local job = redis.call('RPOP', KEYS[i])
    if job then
        -- Writes made before a Lua error are NOT rolled back, so a decode error
        -- must not abort the script after RPOP: park unreadable jobs instead.
        local ok, decoded = pcall(cjson.decode, job)
        if not ok or type(decoded) ~= 'table' or not decoded['Id'] then
            redis.call('LPUSH', KEYS[3], job)
            return false
        end
        local id = decoded['Id']
        -- Score with Redis's clock so every pod and the stalled detector agree.
        local now = redis.call('TIME')
        redis.call('ZADD', KEYS[2], now[1], id)
        -- Not declared in KEYS (the id is only known after the pop). Fine on a
        -- single Redis; Redis Cluster would need hash tags to keep keys together.
        redis.call('HSET', 'job:' .. id, 'status', 'active', 'startedAt', ARGV[2], 'worker', ARGV[1])
        return job
    end
end
return false
`)

// ackScript marks a job completed, clears it from active-jobs and announces it,
// in one step. Before, a crash between HSET completed and ZREM left a finished
// job in active-jobs, and the stalled detector ran it a second time.
var ackScript = redis.NewScript(`
-- KEYS[1] job hash   KEYS[2] active-jobs zset
-- ARGV[1] id   ARGV[2] completedAt   ARGV[3] result
redis.call('HSET', KEYS[1], 'status', 'completed', 'completedAt', ARGV[2], 'result', ARGV[3])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('PUBLISH', 'job:completed', ARGV[1])
return 1
`)

const (
	minPoll    = 2 * time.Millisecond
	maxPoll    = 50 * time.Millisecond // worst-case pickup delay when the queue was idle
	pausedPoll = 500 * time.Millisecond
	errorPoll  = time.Second
)

// claim takes the highest-priority job and records it as active, atomically.
// It waits until a job is available and returns false only when ctx is done.
func (q *Queue) claim(ctx context.Context, worker string) (Job, bool) {
	keys := append([]string{
		"queue:" + q.Name + ":paused",
		"active-jobs:" + q.Name,
		"poison:" + q.Name,
	}, q.Names...)

	backoff := minPoll
	for {
		res, err := claimScript.Run(ctx, q.client, keys, worker, time.Now().Format(time.RFC3339)).Result()

		var wait time.Duration
		switch {
		case err == nil:
			if s, ok := res.(string); ok {
				var job Job
				if json.Unmarshal([]byte(s), &job) == nil {
					return job, true
				}
			}
			wait = pausedPoll // -1: queue is paused
		case errors.Is(err, redis.Nil):
			wait = backoff // every lane empty
			backoff = min(backoff*2, maxPoll)
		default:
			if ctx.Err() != nil {
				return Job{}, false
			}
			// A Redis hiccup. The old Dequeue returned false here, which made the
			// worker goroutine exit for good: one blip and the worker stopped working.
			wait = errorPoll
		}

		select {
		case <-ctx.Done():
			return Job{}, false
		case <-time.After(wait):
		}
	}
}

// ack records a successful job: completed + removed from active-jobs + published.
func (q *Queue) ack(ctx context.Context, job Job, result string) error {
	return ackScript.Run(ctx, q.client,
		[]string{"job:" + job.Id, "active-jobs:" + q.Name},
		job.Id, job.CompletedAt.Format(time.RFC3339), result,
	).Err()
}
