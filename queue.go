package gqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"strconv"
	"time"
)

type Queue struct {
	client *redis.Client
	Names  []string    // Redis list keys per priority, e.g. ["q:high", "q:mid", "q:low"]
	Name   string      // queue identifier, used for pause key and rate limiter key
}

func NewQueue(client *redis.Client, name []string, n string) *Queue {
	return &Queue{client: client, Names: name, Name: n}
}

// The ctx gets passed to Redis operations so that if the context is cancelled
// (e.g. Ctrl+C), Redis knows to stop too.
func (q *Queue) Enqueue(ctx context.Context, job Job) error {
	jobJSON, err := json.Marshal(job)
	if err != nil {
		return err
	}

	q.client.HSet(ctx, "job:"+job.Id,
		"id", job.Id,
		"name", job.Name,
		"status", job.Status.String(),
		"payload", job.Payload,
		"priority", int(job.Priority),
		"maxRetries", job.MaxRetries,
		"attempts", job.Attempts,
		"createdAt", job.CreatedAt.Format(time.RFC3339),
	)

	if job.Delay > 0 {
		return q.client.ZAdd(ctx, "delayed:"+q.Name, redis.Z{
			Score:  float64(time.Now().Add(job.Delay).Unix()),
			Member: jobJSON,
		}).Err()
	}

	return q.client.LPush(ctx, q.Names[job.Priority], jobJSON).Err()
}

func (q *Queue) StartScheduler(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := fmt.Sprintf("%d", time.Now().Unix())
			jobs, err := q.client.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key:     "delayed:" + q.Name,
				Start:   "0",
				Stop:    now,
				ByScore: true,
			}).Result()
			if err != nil || len(jobs) == 0 {
				continue
			}
			for _, jobJSON := range jobs {
				var job Job
				json.Unmarshal([]byte(jobJSON), &job)
				// Claim first, then push. With several worker pods all running the
				// scheduler, only the one whose ZREM removed the entry moves the job;
				// the old order (push, then remove) let two pods push the same job.
				n, err := q.client.ZRem(ctx, "delayed:"+q.Name, jobJSON).Result()
				if err != nil || n == 0 {
					continue
				}
				q.client.LPush(ctx, q.Names[job.Priority], jobJSON)
			}
		}
	}
}

// Dequeue claims the highest-priority waiting job: it is removed from its lane
// and recorded in active-jobs in one atomic step (see reliable.go). It blocks
// until a job is available and returns false only when ctx is cancelled.
// The caller owns the job: finish it, or the stalled detector will requeue it.
func (q *Queue) Dequeue(ctx context.Context) (Job, bool) {
	return q.claim(ctx, "")
}

func (q *Queue) ListDead(ctx context.Context) ([]Job, error) {
	ids, err := q.client.LRange(ctx, "dead-letter:"+q.Name, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	var jobs []Job
	for _, id := range ids {
		data, err := q.client.HGetAll(ctx, "job:"+id).Result()
		if err != nil || len(data) == 0 {
			continue
		}
		jobs = append(jobs, Job{
			Id:      data["id"],
			Status:  StatusDeadLetter,
			Payload: data["payload"],
			Error:   data["error"],
		})
	}

	return jobs, nil
}

func (q *Queue) DetectStalled(ctx context.Context, threshold time.Duration) {
	// Use Redis's clock: claim scores active jobs with Redis TIME, so comparing
	// against this pod's clock would make the threshold drift with clock skew.
	now, err := q.client.Time(ctx).Result()
	if err != nil {
		return
	}
	cutoff := strconv.FormatInt(now.Add(-threshold).Unix(), 10)
	ids, err := q.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     "active-jobs:" + q.Name,
		Start:   "0",
		Stop:    cutoff,
		ByScore: true,
	}).Result()
	if err != nil || len(ids) == 0 {
		return
	}
	for _, id := range ids {
		data, err := q.client.HGetAll(ctx, "job:"+id).Result()
		if err != nil || len(data) == 0 {
			q.client.ZRem(ctx, "active-jobs:"+q.Name, id)
			continue
		}
		job := Job{
			Id:         data["id"],
			Name:       data["name"],
			Payload:    data["payload"],
			Status:     StatusWaiting,
			MaxRetries: 3,
		}
		if p, err := strconv.Atoi(data["priority"]); err == nil {
			job.Priority = JobPriority(p)
		}
		if r, err := strconv.Atoi(data["maxRetries"]); err == nil {
			job.MaxRetries = r
		}
		if a, err := strconv.Atoi(data["attempts"]); err == nil {
			job.Attempts = a
		}
		// Every worker replica runs this detector, so two pods can both see the
		// same stalled id. ZREM is atomic: only the one that actually removed the
		// id (n == 1) requeues it; the other skips it instead of creating a duplicate.
		if n, err := q.client.ZRem(ctx, "active-jobs:"+q.Name, id).Result(); err != nil || n == 0 {
			continue
		}
		q.Enqueue(ctx, job)
	}
}

func (q *Queue) StartStalledDetector(ctx context.Context, threshold time.Duration) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.DetectStalled(ctx, threshold)
		}
	}
}

func (q *Queue) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return q.client.Subscribe(ctx, channels...)
}

func (q *Queue) Pause(ctx context.Context) error {
	return q.client.Set(ctx, "queue:"+q.Name+":paused", "1", 0).Err()
}

func (q *Queue) Resume(ctx context.Context) error {
	return q.client.Del(ctx, "queue:"+q.Name+":paused").Err()
}

func (q *Queue) IsPaused(ctx context.Context) bool {
	exists, err := q.client.Exists(ctx, "queue:"+q.Name+":paused").Result()
	return err == nil && exists > 0
}

// AddRepeatable registers a repeatable job in Redis, scored by its first next-run time.
func (q *Queue) AddRepeatable(ctx context.Context, rj RepeatableJob) error {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(rj.Cron)
	if err != nil {
		return fmt.Errorf("invalid cron %q: %w", rj.Cron, err)
	}
	next := schedule.Next(time.Now())
	data, err := json.Marshal(rj)
	if err != nil {
		return err
	}
	return q.client.ZAdd(ctx, "repeatable:"+q.Name, redis.Z{
		Score:  float64(next.Unix()),
		Member: string(data),
	}).Err()
}

// tickRepeatable fires all due repeatable jobs and re-schedules them.
func (q *Queue) tickRepeatable(ctx context.Context, parser cron.Parser) {
	now := fmt.Sprintf("%d", time.Now().Unix())
	members, err := q.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     "repeatable:"+q.Name,
		Start:   "0",
		Stop:    now,
		ByScore: true,
	}).Result()
	if err != nil || len(members) == 0 {
		return
	}
	for _, member := range members {
		var rj RepeatableJob
		if err := json.Unmarshal([]byte(member), &rj); err != nil {
			q.client.ZRem(ctx, "repeatable:"+q.Name, member)
			continue
		}
		job := NewJob(fmt.Sprintf("%s-%d", rj.Name, time.Now().UnixNano()), rj.JobName, rj.Payload)
		job.Priority = rj.Priority
		q.Enqueue(ctx, job)

		q.client.ZRem(ctx, "repeatable:"+q.Name, member)
		schedule, err := parser.Parse(rj.Cron)
		if err != nil {
			continue
		}
		next := schedule.Next(time.Now())
		q.client.ZAdd(ctx, "repeatable:"+q.Name, redis.Z{
			Score:  float64(next.Unix()),
			Member: member,
		})
	}
}

func (q *Queue) StartRepeatableScheduler(ctx context.Context) {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.tickRepeatable(ctx, parser)
		}
	}
}

// GetJob fetches a job's current status and details from Redis by its ID.
func (q *Queue) GetJob(ctx context.Context, id string) (Job, error) {
	data, err := q.client.HGetAll(ctx, "job:"+id).Result()
	if err != nil || len(data) == 0 {
		return Job{}, fmt.Errorf("job not found: %s", id)
	}

	job := Job{
		Id:      data["id"],
		Name:    data["name"],
		Payload: data["payload"],
		Error:   data["error"],
	}

	if s, ok := data["status"]; ok {
		job.Status = ParseStatus(s)
	}
	if p, err := strconv.Atoi(data["priority"]); err == nil {
		job.Priority = JobPriority(p)
	}
	if a, err := strconv.Atoi(data["attempts"]); err == nil {
		job.Attempts = a
	}
	if r, err := strconv.Atoi(data["maxRetries"]); err == nil {
		job.MaxRetries = r
	}

	return job, nil
}

func (q *Queue) RetryDead(ctx context.Context, id string) error {
	data, err := q.client.HGetAll(ctx, "job:"+id).Result()
	if err != nil || len(data) == 0 {
		return fmt.Errorf("job not found: %s", id)
	}

	job := Job{
		Id:       data["id"],
		Name:     data["name"],
		Status:   StatusWaiting,
		Payload:  data["payload"],
		Attempts: 0,
	}

	q.client.LRem(ctx, "dead-letter:"+q.Name, 1, id)

	return q.Enqueue(ctx, job)
}

    